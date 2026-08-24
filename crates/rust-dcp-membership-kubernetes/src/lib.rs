//! Kubernetes-backed Tokio membership and vBucket assignment extension.

#![forbid(unsafe_code)]

mod error;
mod model;
mod runtime;
mod source;
mod stateful;

pub use error::{KubernetesMembershipError, Result};
pub use model::{
    KubernetesMembershipSnapshot, PodIdentity, PodMember, PodMembershipState, PodState,
    PodWatchEvent,
};
pub use runtime::{KubernetesMembership, KubernetesMembershipConfig};
pub use source::{KubePodMembershipSource, PodMembershipSource, PodWatchStream};
pub use stateful::{StatefulSetMembership, StatefulSetMembershipConfig};

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::*;

    struct ChannelSource {
        receiver: StdMutex<Option<mpsc::UnboundedReceiver<Result<PodWatchEvent>>>>,
    }

    impl ChannelSource {
        fn new() -> (Arc<Self>, mpsc::UnboundedSender<Result<PodWatchEvent>>) {
            let (sender, receiver) = mpsc::unbounded_channel();
            (
                Arc::new(Self {
                    receiver: StdMutex::new(Some(receiver)),
                }),
                sender,
            )
        }
    }

    impl PodMembershipSource for ChannelSource {
        fn watch(&self) -> PodWatchStream {
            let receiver = self.receiver.lock().unwrap().take().unwrap();
            Box::pin(futures_util::stream::unfold(
                receiver,
                |mut receiver| async move { receiver.recv().await.map(|event| (event, receiver)) },
            ))
        }
    }

    fn pod(name: &str, uid: &str, created_at: u64) -> PodState {
        PodState::new(name, uid, created_at, true, false).unwrap()
    }

    #[test]
    fn stateful_set_ordinal_resolves_a_balanced_assignment() {
        let membership = StatefulSetMembership::resolve(
            StatefulSetMembershipConfig::new("consumer", "consumer-1", 3, 10)
                .unwrap()
                .generation(7),
        )
        .unwrap();

        assert_eq!(membership.member_number(), 2);
        assert_eq!(membership.total_members(), 3);
        assert_eq!(membership.assignment().generation(), 7);
        assert_eq!(
            membership.assignment().vbuckets().collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
    }

    #[test]
    fn stateful_set_rejects_wrong_prefix_noncanonical_or_out_of_range_ordinal() {
        assert!(
            StatefulSetMembershipConfig::new("consumer", "other-1", 3, 10)
                .and_then(StatefulSetMembership::resolve)
                .is_err()
        );
        assert!(
            StatefulSetMembershipConfig::new("consumer", "consumer-01", 3, 10)
                .and_then(StatefulSetMembership::resolve)
                .is_err()
        );
        assert!(
            StatefulSetMembershipConfig::new("consumer", "consumer-3", 3, 10)
                .and_then(StatefulSetMembership::resolve)
                .is_err()
        );
    }

    #[test]
    fn stateful_set_supports_a_nonzero_start_ordinal() {
        let membership = StatefulSetMembership::resolve(
            StatefulSetMembershipConfig::new("consumer", "consumer-6", 3, 10)
                .unwrap()
                .start_ordinal(5),
        )
        .unwrap();

        assert_eq!(membership.member_number(), 2);
        assert_eq!(
            membership.assignment().vbuckets().collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
    }

    #[test]
    fn kubernetes_names_reject_empty_dns_segments_and_namespace_subdomains() {
        assert!(PodIdentity::new("consumer..zero", "uid-a").is_err());
        assert!(
            KubernetesMembershipConfig::new(
                "apps.production",
                "app=consumer",
                PodIdentity::new("consumer-0", "uid-a").unwrap(),
                8,
            )
            .is_err()
        );
    }

    #[test]
    fn watcher_init_is_atomic_and_orders_ready_pods_deterministically() {
        let identity = PodIdentity::new("consumer-1", "uid-b").unwrap();
        let mut state = PodMembershipState::new(identity, 8).unwrap();

        assert!(state.apply(PodWatchEvent::Init).unwrap().is_none());
        assert!(
            state
                .apply(PodWatchEvent::InitApply(pod("consumer-1", "uid-b", 20)))
                .unwrap()
                .is_none()
        );
        assert!(
            state
                .apply(PodWatchEvent::InitApply(pod("consumer-0", "uid-a", 10)))
                .unwrap()
                .is_none()
        );
        let initial = state.apply(PodWatchEvent::InitDone).unwrap().unwrap();

        assert_eq!(initial.assignment().generation(), 1);
        assert_eq!(
            initial
                .members()
                .iter()
                .map(PodMember::pod_name)
                .collect::<Vec<_>>(),
            vec!["consumer-0", "consumer-1"]
        );
        assert_eq!(
            initial.assignment().vbuckets().collect::<Vec<_>>(),
            vec![4, 5, 6, 7]
        );

        assert!(state.apply(PodWatchEvent::Init).unwrap().is_none());
        assert!(
            state
                .apply(PodWatchEvent::InitApply(pod("consumer-1", "uid-b", 20)))
                .unwrap()
                .is_none()
        );
        assert!(
            state
                .apply(PodWatchEvent::InitApply(pod("consumer-0", "uid-a", 10)))
                .unwrap()
                .is_none()
        );
        assert!(state.apply(PodWatchEvent::InitDone).unwrap().is_none());
        assert_eq!(state.generation(), 1);
    }

    #[test]
    fn not_ready_and_terminating_pods_are_excluded_and_stale_delete_is_ignored() {
        let identity = PodIdentity::new("consumer-0", "uid-a").unwrap();
        let mut state = PodMembershipState::new(identity, 8).unwrap();
        state.apply(PodWatchEvent::Init).unwrap();
        state
            .apply(PodWatchEvent::InitApply(pod("consumer-0", "uid-a", 10)))
            .unwrap();
        state.apply(PodWatchEvent::InitDone).unwrap().unwrap();

        let not_ready = PodState::new("consumer-1", "uid-b", 20, false, false).unwrap();
        assert!(
            state
                .apply(PodWatchEvent::Apply(not_ready))
                .unwrap()
                .is_none()
        );
        let terminating = PodState::new("consumer-2", "uid-c", 30, true, true).unwrap();
        assert!(
            state
                .apply(PodWatchEvent::Apply(terminating))
                .unwrap()
                .is_none()
        );

        state
            .apply(PodWatchEvent::Apply(pod("consumer-1", "uid-new", 40)))
            .unwrap()
            .unwrap();
        assert!(
            state
                .apply(PodWatchEvent::Delete(
                    PodIdentity::new("consumer-1", "uid-old").unwrap()
                ))
                .unwrap()
                .is_none()
        );
        assert_eq!(state.generation(), 2);
    }

    #[test]
    fn same_name_new_uid_fences_the_old_local_pod() {
        let identity = PodIdentity::new("consumer-0", "uid-old").unwrap();
        let mut state = PodMembershipState::new(identity, 8).unwrap();
        state.apply(PodWatchEvent::Init).unwrap();
        state
            .apply(PodWatchEvent::InitApply(pod("consumer-0", "uid-old", 10)))
            .unwrap();
        state.apply(PodWatchEvent::InitDone).unwrap().unwrap();

        assert!(matches!(
            state.apply(PodWatchEvent::Apply(pod("consumer-0", "uid-new", 20))),
            Err(KubernetesMembershipError::Fenced { .. })
        ));
    }

    fn runtime_config() -> KubernetesMembershipConfig {
        KubernetesMembershipConfig::new(
            "apps",
            "app=consumer",
            PodIdentity::new("consumer-0", "uid-a").unwrap(),
            8,
        )
        .unwrap()
        .startup_timeout(Duration::from_secs(1))
        .watch_error_backoff(Duration::from_millis(1))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tokio_runtime_publishes_incremental_assignment_changes() {
        let (source, sender) = ChannelSource::new();
        sender.send(Ok(PodWatchEvent::Init)).unwrap();
        sender
            .send(Ok(PodWatchEvent::InitApply(pod("consumer-0", "uid-a", 10))))
            .unwrap();
        sender.send(Ok(PodWatchEvent::InitDone)).unwrap();

        let membership = KubernetesMembership::with_source(runtime_config(), source)
            .await
            .unwrap();
        assert_eq!(membership.snapshot().assignment().generation(), 1);
        let mut updates = membership.subscribe();

        sender
            .send(Err(KubernetesMembershipError::Watch(
                "injected incremental failure".into(),
            )))
            .unwrap();
        sender
            .send(Ok(PodWatchEvent::Apply(pod("consumer-1", "uid-b", 20))))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), updates.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updates.borrow().assignment().generation(), 2);
        assert_eq!(
            updates.borrow().assignment().vbuckets().collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        membership.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn watcher_error_recovers_during_initialization_but_stream_end_is_terminal() {
        let (source, sender) = ChannelSource::new();
        sender
            .send(Err(KubernetesMembershipError::Watch(
                "injected transient failure".into(),
            )))
            .unwrap();
        sender.send(Ok(PodWatchEvent::Init)).unwrap();
        sender
            .send(Ok(PodWatchEvent::InitApply(pod("consumer-0", "uid-a", 10))))
            .unwrap();
        sender.send(Ok(PodWatchEvent::InitDone)).unwrap();

        let membership = KubernetesMembership::with_source(runtime_config(), source)
            .await
            .unwrap();
        let mut updates = membership.subscribe();
        drop(sender);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), updates.changed())
                .await
                .unwrap()
                .is_err()
        );
        assert!(matches!(
            membership.close().await,
            Err(KubernetesMembershipError::WatchEnded)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_uid_replacement_terminates_the_tokio_membership_watch() {
        let (source, sender) = ChannelSource::new();
        sender.send(Ok(PodWatchEvent::Init)).unwrap();
        sender
            .send(Ok(PodWatchEvent::InitApply(pod("consumer-0", "uid-a", 10))))
            .unwrap();
        sender.send(Ok(PodWatchEvent::InitDone)).unwrap();
        let membership = KubernetesMembership::with_source(runtime_config(), source)
            .await
            .unwrap();
        let mut updates = membership.subscribe();

        sender
            .send(Ok(PodWatchEvent::Apply(pod(
                "consumer-0",
                "uid-replacement",
                20,
            ))))
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), updates.changed())
                .await
                .unwrap()
                .is_err()
        );
        assert!(matches!(
            membership.close().await,
            Err(KubernetesMembershipError::Fenced { .. })
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn startup_times_out_when_the_complete_view_never_contains_self() {
        let (source, sender) = ChannelSource::new();
        sender.send(Ok(PodWatchEvent::Init)).unwrap();
        sender.send(Ok(PodWatchEvent::InitDone)).unwrap();
        let config = runtime_config().startup_timeout(Duration::from_millis(20));

        let error = KubernetesMembership::with_source(config, source)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            KubernetesMembershipError::StartupTimeout(_)
        ));
        drop(sender);
    }
}
