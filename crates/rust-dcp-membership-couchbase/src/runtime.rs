use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use rust_dcp_core::DcpConfig;
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time::{self, Instant, MissedTickBehavior},
};

use crate::{
    CouchbaseKvMembershipStore, CouchbaseMembershipError, CouchbaseRegistryCollection,
    MemberIdentity, MembershipSnapshot, MembershipStore, RegistryDocument, Result,
    StoreWriteResult,
};

const REGISTRY_KEY_PREFIX: &str = "rust-dcp:membership:v1:";

/// Timings, CAS bounds, identity, and partition count for one member process.
#[derive(Clone, Debug)]
pub struct CouchbaseMembershipConfig {
    group: String,
    identity: MemberIdentity,
    vbucket_count: usize,
    heartbeat_interval: Duration,
    monitor_interval: Duration,
    stale_after: Duration,
    rebalance_delay: Duration,
    operation_timeout: Duration,
    max_cas_attempts: NonZeroUsize,
}

impl CouchbaseMembershipConfig {
    /// Creates a configuration with production-oriented lifecycle defaults.
    ///
    /// The caller supplies a unique incarnation value for every process start;
    /// reusing it would defeat stale-process fencing.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for an invalid group, identity, or
    /// vBucket count.
    pub fn new(
        group: impl Into<String>,
        identity: MemberIdentity,
        vbucket_count: usize,
    ) -> Result<Self> {
        let config = Self {
            group: group.into(),
            identity,
            vbucket_count,
            heartbeat_interval: Duration::from_secs(10),
            monitor_interval: Duration::from_secs(30),
            stale_after: Duration::from_secs(70),
            rebalance_delay: Duration::from_secs(30),
            operation_timeout: Duration::from_secs(30),
            max_cas_attempts: NonZeroUsize::new(16).unwrap_or(NonZeroUsize::MIN),
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets the interval between local liveness writes.
    #[must_use]
    pub const fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Sets the interval between stale-member scans.
    #[must_use]
    pub const fn monitor_interval(mut self, interval: Duration) -> Self {
        self.monitor_interval = interval;
        self
    }

    /// Sets the elapsed heartbeat age at which a member can be fenced.
    #[must_use]
    pub const fn stale_after(mut self, duration: Duration) -> Self {
        self.stale_after = duration;
        self
    }

    /// Sets the initial join-settling delay before an assignment is returned.
    #[must_use]
    pub const fn rebalance_delay(mut self, duration: Duration) -> Self {
        self.rebalance_delay = duration;
        self
    }

    /// Sets the deadline around one complete load/mutate/CAS retry loop.
    #[must_use]
    pub const fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    /// Sets the bounded number of registry CAS attempts per lifecycle action.
    #[must_use]
    pub const fn max_cas_attempts(mut self, attempts: NonZeroUsize) -> Self {
        self.max_cas_attempts = attempts;
        self
    }

    /// Logical consumer-group name.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Exact process identity protected by the registry fence.
    #[must_use]
    pub const fn identity(&self) -> &MemberIdentity {
        &self.identity
    }

    /// Shared registry document key.
    #[must_use]
    pub fn registry_key(&self) -> String {
        format!("{REGISTRY_KEY_PREFIX}{}", self.group)
    }

    fn validate(&self) -> Result<()> {
        let key = self.registry_key();
        if self.group.is_empty()
            || !self.group.bytes().all(|byte| byte.is_ascii_graphic())
            || key.len() > 250
        {
            return Err(CouchbaseMembershipError::Configuration(
                "membership group must produce a registry key of at most 250 visible ASCII bytes"
                    .into(),
            ));
        }
        let stale_after_millis = duration_millis(self.stale_after, "stale timeout")?;
        RegistryDocument::new(self.vbucket_count, stale_after_millis)?;
        if self.heartbeat_interval.is_zero() {
            return Err(CouchbaseMembershipError::Configuration(
                "heartbeat interval must be greater than zero".into(),
            ));
        }
        if self.monitor_interval.is_zero() {
            return Err(CouchbaseMembershipError::Configuration(
                "monitor interval must be greater than zero".into(),
            ));
        }
        if self.operation_timeout.is_zero() {
            return Err(CouchbaseMembershipError::Configuration(
                "operation timeout must be greater than zero".into(),
            ));
        }
        if self.stale_after <= self.heartbeat_interval {
            return Err(CouchbaseMembershipError::Configuration(
                "stale timeout must be greater than the heartbeat interval".into(),
            ));
        }
        Ok(())
    }
}

/// Running Tokio membership process backed by a CAS registry document.
pub struct CouchbaseMembership {
    snapshots: watch::Receiver<MembershipSnapshot>,
    cancel: watch::Sender<bool>,
    task: Mutex<TaskState>,
}

impl std::fmt::Debug for CouchbaseMembership {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CouchbaseMembership")
            .field("snapshot", &*self.snapshots.borrow())
            .finish_non_exhaustive()
    }
}

impl CouchbaseMembership {
    /// Connects using the built-in Couchbase KV store in the default
    /// collection.
    ///
    /// # Errors
    ///
    /// Returns configuration, bootstrap, registry, timeout, duplicate-member,
    /// or fencing errors before publishing the first assignment.
    pub async fn connect(config: CouchbaseMembershipConfig, dcp: DcpConfig) -> Result<Self> {
        let store = Arc::new(CouchbaseKvMembershipStore::new(dcp)?);
        Self::with_store(config, store).await
    }

    /// Connects using the built-in Couchbase KV store in a named collection.
    ///
    /// # Errors
    ///
    /// Returns configuration, bootstrap, collection, registry, timeout,
    /// duplicate-member, or fencing errors before the first assignment.
    pub async fn connect_in_collection(
        config: CouchbaseMembershipConfig,
        dcp: DcpConfig,
        collection: CouchbaseRegistryCollection,
    ) -> Result<Self> {
        let store = Arc::new(CouchbaseKvMembershipStore::in_collection(dcp, collection)?);
        Self::with_store(config, store).await
    }

    /// Starts the runtime with an application-provided asynchronous CAS store.
    ///
    /// # Errors
    ///
    /// Returns configuration, store, registry, timeout, duplicate-member, or
    /// fencing errors before the first assignment is available.
    pub async fn with_store(
        config: CouchbaseMembershipConfig,
        store: Arc<dyn MembershipStore>,
    ) -> Result<Self> {
        config.validate()?;
        let config = Arc::new(config);
        let initial = mutate_registry(&config, store.as_ref(), RegistryAction::Register)
            .await?
            .ok_or_else(|| {
                CouchbaseMembershipError::Store(
                    "registration did not produce a membership snapshot".into(),
                )
            })?;
        let (snapshot_tx, snapshots) = watch::channel(initial);
        let (cancel, cancel_rx) = watch::channel(false);
        let task = tokio::spawn(run_membership(
            config.clone(),
            store.clone(),
            snapshot_tx.clone(),
            cancel_rx,
        ));
        let membership = Self {
            snapshots,
            cancel,
            task: Mutex::new(TaskState {
                handle: Some(task),
                terminal: None,
            }),
        };

        if !config.rebalance_delay.is_zero() {
            time::sleep(config.rebalance_delay).await;
        }
        let refreshed =
            match mutate_registry(&config, store.as_ref(), RegistryAction::Register).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    let _ = membership.close().await;
                    return Err(error);
                }
            };
        if let Some(snapshot) = refreshed {
            let assignment_changed = {
                let current = snapshot_tx.borrow();
                current.assignment() != snapshot.assignment()
            };
            if assignment_changed {
                snapshot_tx.send_replace(snapshot);
            }
        }

        let task_finished = membership
            .task
            .lock()
            .await
            .handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished);
        if task_finished {
            membership.close().await?;
        }
        Ok(membership)
    }

    /// Returns the latest assignment observed by this process.
    #[must_use]
    pub fn snapshot(&self) -> MembershipSnapshot {
        self.snapshots.borrow().clone()
    }

    /// Subscribes to effective membership or assignment changes.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<MembershipSnapshot> {
        self.snapshots.clone()
    }

    /// Gracefully removes this exact incarnation and waits for the Tokio task.
    ///
    /// Repeated and concurrent calls are idempotent. A prior terminal failure
    /// is returned again as a task error.
    ///
    /// # Errors
    ///
    /// Returns the background registry, fencing, timeout, or join failure.
    pub async fn close(&self) -> Result<()> {
        let _ = self.cancel.send(true);
        let mut task = self.task.lock().await;
        if let Some(terminal) = &task.terminal {
            return match terminal {
                Ok(()) => Ok(()),
                Err(message) => Err(CouchbaseMembershipError::Task(message.clone())),
            };
        }
        let Some(handle) = task.handle.take() else {
            return Ok(());
        };
        let result = match handle.await {
            Ok(result) => result,
            Err(error) => Err(CouchbaseMembershipError::Task(error.to_string())),
        };
        task.terminal = Some(match &result {
            Ok(()) => Ok(()),
            Err(error) => Err(error.to_string()),
        });
        result
    }
}

impl Drop for CouchbaseMembership {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

struct TaskState {
    handle: Option<JoinHandle<Result<()>>>,
    terminal: Option<std::result::Result<(), String>>,
}

#[derive(Clone, Copy)]
enum RegistryAction {
    Register,
    Heartbeat,
    Monitor,
    Remove,
}

async fn run_membership(
    config: Arc<CouchbaseMembershipConfig>,
    store: Arc<dyn MembershipStore>,
    snapshots: watch::Sender<MembershipSnapshot>,
    mut cancel: watch::Receiver<bool>,
) -> Result<()> {
    let mut heartbeat = time::interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut monitor = time::interval(config.monitor_interval);
    monitor.set_missed_tick_behavior(MissedTickBehavior::Delay);
    monitor.tick().await;
    let mut last_successful_renewal = Instant::now();

    loop {
        let action = tokio::select! {
            biased;
            changed = cancel.changed() => {
                let _ = changed;
                break;
            }
            _ = heartbeat.tick() => RegistryAction::Heartbeat,
            _ = monitor.tick() => RegistryAction::Monitor,
        };
        match mutate_registry(&config, store.as_ref(), action).await {
            Ok(snapshot) => {
                last_successful_renewal = Instant::now();
                if let Some(snapshot) = snapshot {
                    let assignment_changed = {
                        let current = snapshots.borrow();
                        current.assignment() != snapshot.assignment()
                    };
                    if assignment_changed {
                        snapshots.send_replace(snapshot);
                    }
                }
            }
            Err(error) if is_transient_runtime_error(&error) => {
                let idle_for = last_successful_renewal.elapsed();
                if idle_for >= config.stale_after {
                    return Err(CouchbaseMembershipError::LeaseExpired {
                        idle_for,
                        last_error: error.to_string(),
                    });
                }
                tracing::warn!(
                    member = config.identity.member_id(),
                    idle_millis = idle_for.as_millis(),
                    error = %error,
                    "membership renewal failed within the configured stale tolerance"
                );
            }
            Err(error) => return Err(error),
        }
    }

    mutate_registry(&config, store.as_ref(), RegistryAction::Remove).await?;
    Ok(())
}

async fn mutate_registry(
    config: &CouchbaseMembershipConfig,
    store: &dyn MembershipStore,
    action: RegistryAction,
) -> Result<Option<MembershipSnapshot>> {
    time::timeout(
        config.operation_timeout,
        mutate_registry_without_timeout(config, store, action),
    )
    .await
    .map_err(|_| CouchbaseMembershipError::Timeout(config.operation_timeout))?
}

async fn mutate_registry_without_timeout(
    config: &CouchbaseMembershipConfig,
    store: &dyn MembershipStore,
    action: RegistryAction,
) -> Result<Option<MembershipSnapshot>> {
    let key = config.registry_key();
    let stale_after_millis = duration_millis(config.stale_after, "stale timeout")?;
    for _ in 0..config.max_cas_attempts.get() {
        let stored = store.load(&key).await?;
        let mut registry = match &stored {
            Some(stored) => {
                let registry = serde_json::from_slice::<RegistryDocument>(&stored.value)?;
                registry.validate_settings(config.vbucket_count, stale_after_millis)?;
                registry
            }
            None if matches!(action, RegistryAction::Register) => {
                RegistryDocument::new(config.vbucket_count, stale_after_millis)?
            }
            None => {
                return Err(CouchbaseMembershipError::Fenced {
                    member_id: config.identity.member_id().to_owned(),
                });
            }
        };
        let before = registry.clone();
        let now_millis = unix_time_millis()?;
        let snapshot = match action {
            RegistryAction::Register => {
                registry.prune_stale(now_millis)?;
                registry.register(&config.identity, now_millis)?;
                Some(registry.snapshot(&config.identity)?)
            }
            RegistryAction::Heartbeat => {
                registry.heartbeat(&config.identity, now_millis)?;
                Some(registry.snapshot(&config.identity)?)
            }
            RegistryAction::Monitor => {
                registry.heartbeat(&config.identity, now_millis)?;
                registry.prune_stale(now_millis)?;
                Some(registry.snapshot(&config.identity)?)
            }
            RegistryAction::Remove => {
                registry.remove(&config.identity)?;
                None
            }
        };

        if registry == before {
            return Ok(snapshot);
        }
        let value = Bytes::from(serde_json::to_vec(&registry)?);
        let write = match stored {
            Some(stored) => store.replace(&key, value, stored.cas).await?,
            None => store.create(&key, value).await?,
        };
        if write == StoreWriteResult::Stored {
            return Ok(snapshot);
        }
        tokio::task::yield_now().await;
    }
    Err(CouchbaseMembershipError::Store(format!(
        "membership CAS conflicted for all {} attempts",
        config.max_cas_attempts
    )))
}

fn is_transient_runtime_error(error: &CouchbaseMembershipError) -> bool {
    matches!(
        error,
        CouchbaseMembershipError::Dcp(_)
            | CouchbaseMembershipError::Timeout(_)
            | CouchbaseMembershipError::Store(_)
    )
}

fn duration_millis(duration: Duration, kind: &str) -> Result<u64> {
    u64::try_from(duration.as_millis()).map_err(|error| {
        CouchbaseMembershipError::Configuration(format!(
            "{kind} cannot be represented in milliseconds: {error}"
        ))
    })
}

fn unix_time_millis() -> Result<u64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CouchbaseMembershipError::Store(format!("system clock precedes Unix epoch: {error}"))
        })?;
    u64::try_from(elapsed.as_millis()).map_err(|error| {
        CouchbaseMembershipError::Store(format!(
            "Unix timestamp cannot be represented in milliseconds: {error}"
        ))
    })
}
