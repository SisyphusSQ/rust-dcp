use std::{sync::Arc, time::Duration};

use futures_util::StreamExt;
use kube::Client;
use rust_dcp_core::VBucketAssignment;
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time,
};

use crate::{
    KubePodMembershipSource, KubernetesMembershipError, KubernetesMembershipSnapshot, PodIdentity,
    PodMembershipSource, PodMembershipState, PodWatchStream, Result, stateful::validate_namespace,
};

/// Dynamic pod watcher configuration for one DCP consumer group.
#[derive(Clone, Debug)]
pub struct KubernetesMembershipConfig {
    namespace: String,
    label_selector: String,
    identity: PodIdentity,
    vbucket_count: usize,
    startup_timeout: Duration,
    watch_error_backoff: Duration,
}

impl KubernetesMembershipConfig {
    /// Creates a namespace-scoped Ready-pod membership configuration.
    ///
    /// An explicit label selector and exact pod UID are required to prevent an
    /// accidentally broad watch and to fence same-name pod recreation.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for invalid namespace, selector, or
    /// vBucket bounds.
    pub fn new(
        namespace: impl Into<String>,
        label_selector: impl Into<String>,
        identity: PodIdentity,
        vbucket_count: usize,
    ) -> Result<Self> {
        let config = Self {
            namespace: namespace.into(),
            label_selector: label_selector.into(),
            identity,
            vbucket_count,
            startup_timeout: Duration::from_secs(60),
            watch_error_backoff: Duration::from_secs(1),
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets the deadline for observing the first complete view containing the
    /// exact local pod UID.
    #[must_use]
    pub const fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Sets the delay before polling kube-rs again after a recoverable watcher
    /// error.
    #[must_use]
    pub const fn watch_error_backoff(mut self, backoff: Duration) -> Self {
        self.watch_error_backoff = backoff;
        self
    }

    /// Kubernetes namespace watched by this member.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Explicit Kubernetes label selector defining group membership.
    #[must_use]
    pub fn label_selector(&self) -> &str {
        &self.label_selector
    }

    /// Exact local pod identity.
    #[must_use]
    pub const fn identity(&self) -> &PodIdentity {
        &self.identity
    }

    fn validate(&self) -> Result<()> {
        validate_namespace(&self.namespace)?;
        if self.label_selector.trim().is_empty() {
            return Err(KubernetesMembershipError::Configuration(
                "pod label selector must not be empty".into(),
            ));
        }
        VBucketAssignment::balanced(1, self.vbucket_count, 1, 1)
            .map_err(|error| KubernetesMembershipError::Configuration(error.to_string()))?;
        if self.startup_timeout.is_zero() {
            return Err(KubernetesMembershipError::Configuration(
                "startup timeout must be greater than zero".into(),
            ));
        }
        if self.watch_error_backoff.is_zero() {
            return Err(KubernetesMembershipError::Configuration(
                "watch error backoff must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

/// Running Tokio pod membership watcher.
pub struct KubernetesMembership {
    snapshots: watch::Receiver<KubernetesMembershipSnapshot>,
    cancel: watch::Sender<bool>,
    task: Mutex<TaskState>,
}

impl std::fmt::Debug for KubernetesMembership {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KubernetesMembership")
            .field("snapshot", &*self.snapshots.borrow())
            .finish_non_exhaustive()
    }
}

impl KubernetesMembership {
    /// Infers a kube-rs client from the local environment and waits for the
    /// first complete Ready-pod assignment.
    ///
    /// # Errors
    ///
    /// Returns client inference, startup timeout, watcher, assignment, or local
    /// pod fencing errors.
    pub async fn connect(config: KubernetesMembershipConfig) -> Result<Self> {
        let client = Client::try_default()
            .await
            .map_err(|error| KubernetesMembershipError::Kubernetes(error.to_string()))?;
        Self::with_client(config, client).await
    }

    /// Starts with an application-provided kube-rs client.
    ///
    /// # Errors
    ///
    /// Returns configuration, startup timeout, watcher, assignment, or local
    /// pod fencing errors.
    pub async fn with_client(config: KubernetesMembershipConfig, client: Client) -> Result<Self> {
        let source = Arc::new(KubePodMembershipSource::new(
            client,
            config.namespace(),
            config.label_selector(),
        )?);
        Self::with_source(config, source).await
    }

    /// Starts with an injectable normalized watcher source.
    ///
    /// # Errors
    ///
    /// Returns configuration, startup timeout, watcher, assignment, or local
    /// pod fencing errors.
    pub async fn with_source(
        config: KubernetesMembershipConfig,
        source: Arc<dyn PodMembershipSource>,
    ) -> Result<Self> {
        config.validate()?;
        let mut events = source.watch();
        let mut state = PodMembershipState::new(config.identity.clone(), config.vbucket_count)?;
        let initial = time::timeout(
            config.startup_timeout,
            wait_for_initial_snapshot(&config, &mut events, &mut state),
        )
        .await
        .map_err(|_| KubernetesMembershipError::StartupTimeout(config.startup_timeout))??;

        let (snapshot_tx, snapshots) = watch::channel(initial);
        let (cancel, cancel_rx) = watch::channel(false);
        let task = tokio::spawn(run_membership(
            Arc::new(config),
            events,
            state,
            snapshot_tx,
            cancel_rx,
        ));
        Ok(Self {
            snapshots,
            cancel,
            task: Mutex::new(TaskState {
                handle: Some(task),
                terminal: None,
            }),
        })
    }

    /// Latest complete Ready-pod assignment.
    #[must_use]
    pub fn snapshot(&self) -> KubernetesMembershipSnapshot {
        self.snapshots.borrow().clone()
    }

    /// Subscribes to effective pod-set and assignment changes.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<KubernetesMembershipSnapshot> {
        self.snapshots.clone()
    }

    /// Cancels the watcher and waits for its Tokio task.
    ///
    /// Repeated and concurrent calls are idempotent. A prior terminal failure
    /// is returned again as a task error.
    ///
    /// # Errors
    ///
    /// Returns a watcher, fencing, assignment, or task join error.
    pub async fn close(&self) -> Result<()> {
        let _ = self.cancel.send(true);
        let mut task = self.task.lock().await;
        if let Some(terminal) = &task.terminal {
            return match terminal {
                Ok(()) => Ok(()),
                Err(message) => Err(KubernetesMembershipError::Task(message.clone())),
            };
        }
        let Some(handle) = task.handle.take() else {
            return Ok(());
        };
        let result = match handle.await {
            Ok(result) => result,
            Err(error) => Err(KubernetesMembershipError::Task(error.to_string())),
        };
        task.terminal = Some(match &result {
            Ok(()) => Ok(()),
            Err(error) => Err(error.to_string()),
        });
        result
    }
}

impl Drop for KubernetesMembership {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

struct TaskState {
    handle: Option<JoinHandle<Result<()>>>,
    terminal: Option<std::result::Result<(), String>>,
}

async fn wait_for_initial_snapshot(
    config: &KubernetesMembershipConfig,
    events: &mut PodWatchStream,
    state: &mut PodMembershipState,
) -> Result<KubernetesMembershipSnapshot> {
    loop {
        match events.next().await {
            Some(Ok(event)) => {
                if let Some(snapshot) = state.apply(event)? {
                    return Ok(snapshot);
                }
            }
            Some(Err(error)) if is_recoverable_watch_error(&error) => {
                tracing::warn!(error = %error, "Kubernetes membership watcher is recovering during startup");
                time::sleep(config.watch_error_backoff).await;
            }
            Some(Err(error)) => return Err(error),
            None => return Err(KubernetesMembershipError::WatchEnded),
        }
    }
}

async fn run_membership(
    config: Arc<KubernetesMembershipConfig>,
    mut events: PodWatchStream,
    mut state: PodMembershipState,
    snapshots: watch::Sender<KubernetesMembershipSnapshot>,
    mut cancel: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        let event = tokio::select! {
            biased;
            changed = cancel.changed() => {
                let _ = changed;
                return Ok(());
            }
            event = events.next() => event,
        };
        match event {
            Some(Ok(event)) => {
                if let Some(snapshot) = state.apply(event)? {
                    snapshots.send_replace(snapshot);
                }
            }
            Some(Err(error)) if is_recoverable_watch_error(&error) => {
                tracing::warn!(error = %error, "Kubernetes membership watcher is recovering");
                tokio::select! {
                    biased;
                    changed = cancel.changed() => {
                        let _ = changed;
                        return Ok(());
                    }
                    () = time::sleep(config.watch_error_backoff) => {}
                }
            }
            Some(Err(error)) => return Err(error),
            None => return Err(KubernetesMembershipError::WatchEnded),
        }
    }
}

fn is_recoverable_watch_error(error: &KubernetesMembershipError) -> bool {
    matches!(error, KubernetesMembershipError::Watch(_))
}
