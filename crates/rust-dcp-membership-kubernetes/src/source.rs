use std::pin::Pin;

use futures_util::{Stream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Client, runtime::watcher};

use crate::{
    KubernetesMembershipError, PodIdentity, PodState, PodWatchEvent, Result,
    stateful::validate_namespace,
};

/// Boxed, recoverable stream of normalized pod watcher events.
pub type PodWatchStream = Pin<Box<dyn Stream<Item = Result<PodWatchEvent>> + Send + 'static>>;

/// Injectable source for Kubernetes membership watch events.
pub trait PodMembershipSource: Send + Sync {
    /// Starts a fresh event stream.
    fn watch(&self) -> PodWatchStream;
}

/// kube-rs Pod watcher scoped by namespace and an explicit label selector.
#[derive(Clone)]
pub struct KubePodMembershipSource {
    client: Client,
    namespace: String,
    label_selector: String,
}

impl std::fmt::Debug for KubePodMembershipSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KubePodMembershipSource")
            .field("namespace", &self.namespace)
            .field("label_selector", &self.label_selector)
            .finish_non_exhaustive()
    }
}

impl KubePodMembershipSource {
    /// Creates a full-Pod watcher so Ready conditions and deletion timestamps
    /// remain available to the membership state machine.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for an invalid namespace or empty label
    /// selector.
    pub fn new(
        client: Client,
        namespace: impl Into<String>,
        label_selector: impl Into<String>,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let label_selector = label_selector.into();
        validate_namespace(&namespace)?;
        if label_selector.trim().is_empty() {
            return Err(KubernetesMembershipError::Configuration(
                "pod label selector must not be empty".into(),
            ));
        }
        Ok(Self {
            client,
            namespace,
            label_selector,
        })
    }
}

impl PodMembershipSource for KubePodMembershipSource {
    fn watch(&self) -> PodWatchStream {
        let pods = Api::<Pod>::namespaced(self.client.clone(), &self.namespace);
        let config = watcher::Config::default().labels(&self.label_selector);
        Box::pin(watcher::watcher(pods, config).map(|event| match event {
            Ok(watcher::Event::Apply(pod)) => normalize_pod(pod).map(PodWatchEvent::Apply),
            Ok(watcher::Event::Delete(pod)) => {
                normalize_deleted_pod(pod).map(PodWatchEvent::Delete)
            }
            Ok(watcher::Event::Init) => Ok(PodWatchEvent::Init),
            Ok(watcher::Event::InitApply(pod)) => normalize_pod(pod).map(PodWatchEvent::InitApply),
            Ok(watcher::Event::InitDone) => Ok(PodWatchEvent::InitDone),
            Err(error) => Err(KubernetesMembershipError::Watch(error.to_string())),
        }))
    }
}

fn normalize_deleted_pod(pod: Pod) -> Result<PodIdentity> {
    let pod_name = pod.metadata.name.ok_or_else(|| {
        KubernetesMembershipError::WatchSequence("deleted pod omitted metadata.name".into())
    })?;
    let uid = pod.metadata.uid.ok_or_else(|| {
        KubernetesMembershipError::WatchSequence(format!(
            "deleted pod {pod_name:?} omitted metadata.uid"
        ))
    })?;
    PodIdentity::new(pod_name, uid)
}

fn normalize_pod(pod: Pod) -> Result<PodState> {
    let pod_name = pod.metadata.name.ok_or_else(|| {
        KubernetesMembershipError::WatchSequence("watched pod omitted metadata.name".into())
    })?;
    let uid = pod.metadata.uid.ok_or_else(|| {
        KubernetesMembershipError::WatchSequence(format!(
            "watched pod {pod_name:?} omitted metadata.uid"
        ))
    })?;
    let created_at = pod.metadata.creation_timestamp.ok_or_else(|| {
        KubernetesMembershipError::WatchSequence(format!(
            "watched pod {pod_name:?} omitted metadata.creationTimestamp"
        ))
    })?;
    let created_at_millis = u64::try_from(created_at.0.timestamp_millis()).map_err(|error| {
        KubernetesMembershipError::WatchSequence(format!(
            "watched pod {pod_name:?} has a pre-epoch creation timestamp: {error}"
        ))
    })?;
    let terminating = pod.metadata.deletion_timestamp.is_some();
    let ready = pod.status.as_ref().is_some_and(|status| {
        let running = status.phase.as_deref() == Some("Running");
        running
            && status.conditions.as_ref().is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition.type_ == "Ready" && condition.status == "True")
            })
    });
    PodState::new(pod_name, uid, created_at_millis, ready, terminating)
}

#[cfg(test)]
mod tests {
    use k8s_openapi::{
        api::core::v1::{PodCondition, PodStatus},
        apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time},
        chrono::{TimeZone, Utc},
    };

    use super::*;

    fn ready_pod() -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some("consumer-0".into()),
                uid: Some("uid-a".into()),
                creation_timestamp: Some(Time(Utc.timestamp_millis_opt(1_234).single().unwrap())),
                ..ObjectMeta::default()
            },
            status: Some(PodStatus {
                phase: Some("Running".into()),
                conditions: Some(vec![PodCondition {
                    type_: "Ready".into(),
                    status: "True".into(),
                    ..PodCondition::default()
                }]),
                ..PodStatus::default()
            }),
            ..Pod::default()
        }
    }

    #[test]
    fn real_pod_normalization_requires_ready_running_and_nonterminating_state() {
        let normalized = normalize_pod(ready_pod()).unwrap();
        assert!(normalized.is_ready());
        assert!(!normalized.is_terminating());
        assert_eq!(normalized.created_at_millis(), 1_234);

        let mut failed = ready_pod();
        failed.status.as_mut().unwrap().phase = Some("Failed".into());
        assert!(!normalize_pod(failed).unwrap().is_ready());

        let mut pending = ready_pod();
        pending.status.as_mut().unwrap().phase = Some("Pending".into());
        assert!(!normalize_pod(pending).unwrap().is_ready());

        let mut terminating = ready_pod();
        terminating.metadata.deletion_timestamp =
            Some(Time(Utc.timestamp_millis_opt(2_000).single().unwrap()));
        assert!(normalize_pod(terminating).unwrap().is_terminating());
    }

    #[test]
    fn real_pod_normalization_rejects_missing_uid_or_creation_timestamp() {
        let mut missing_uid = ready_pod();
        missing_uid.metadata.uid = None;
        assert!(normalize_pod(missing_uid).is_err());

        let mut missing_timestamp = ready_pod();
        missing_timestamp.metadata.creation_timestamp = None;
        assert!(normalize_pod(missing_timestamp).is_err());
    }

    #[test]
    fn deletion_normalization_only_requires_immutable_identity() {
        let mut deleted = ready_pod();
        deleted.metadata.creation_timestamp = None;
        deleted.status = None;

        let identity = normalize_deleted_pod(deleted).unwrap();
        assert_eq!(identity.pod_name(), "consumer-0");
        assert_eq!(identity.uid(), "uid-a");
    }
}
