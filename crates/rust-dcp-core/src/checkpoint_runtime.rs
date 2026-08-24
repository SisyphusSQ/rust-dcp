use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::Stream;
use rust_dcp_protocol::Frame;
use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
    time,
};

use crate::{
    CheckpointConfig, CheckpointMode, CheckpointStore, DcpCheckpoint, DcpError, DcpEvent,
    DcpStreamItem, Result,
};

/// Loads and validates checkpoints within a bounded Tokio operation.
///
/// Missing vBuckets are omitted so the caller can apply its configured
/// earliest/latest initialization policy.
///
/// # Errors
///
/// Returns a timeout, store, bucket-identity, duplicate-vBucket, or checkpoint
/// validation error.
pub async fn load_checkpoints(
    store: &dyn CheckpointStore,
    bucket_uuid: &str,
    vbuckets: &[u16],
    timeout: Duration,
) -> Result<BTreeMap<u16, DcpCheckpoint>> {
    if bucket_uuid.is_empty() {
        return Err(DcpError::InvalidConfiguration(
            "checkpoint bucket UUID must not be empty".into(),
        ));
    }
    if timeout.is_zero() {
        return Err(DcpError::InvalidConfiguration(
            "checkpoint load timeout must be greater than zero".into(),
        ));
    }
    let requested = vbuckets.iter().copied().collect::<BTreeSet<_>>();
    if requested.len() != vbuckets.len() {
        return Err(DcpError::Checkpoint(
            "checkpoint load vBucket list contains duplicates".into(),
        ));
    }
    let checkpoints = time::timeout(timeout, store.load(bucket_uuid, vbuckets))
        .await
        .map_err(|_| DcpError::Timeout(timeout))??;
    for (&vbucket, checkpoint) in &checkpoints {
        if !requested.contains(&vbucket) {
            return Err(DcpError::Checkpoint(format!(
                "checkpoint store returned unrequested vBucket {vbucket}"
            )));
        }
        validate_checkpoint_identity(vbucket, bucket_uuid, checkpoint)?;
    }
    Ok(checkpoints)
}

/// Result of acknowledging one delivered, sequence-bearing event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AckOutcome {
    /// vBucket whose event was acknowledged.
    pub vbucket: u16,
    /// Sequence number of the specific acknowledged event.
    pub acknowledged_seqno: u64,
    /// New contiguous processed checkpoint, if this ACK closed the queue head.
    pub advanced_to: Option<DcpCheckpoint>,
    /// Delivered sequence-bearing events still waiting for contiguous ACKs.
    pub pending_events: usize,
}

/// Single-use application acknowledgement for one delivered event.
pub struct EventAck {
    state: Arc<Mutex<CoordinatorState>>,
    vbucket: u16,
    delivery_id: u64,
    seqno: u64,
}

impl fmt::Debug for EventAck {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventAck")
            .field("vbucket", &self.vbucket)
            .field("delivery_id", &self.delivery_id)
            .field("seqno", &self.seqno)
            .finish_non_exhaustive()
    }
}

impl EventAck {
    /// Marks this event processed and advances through every now-contiguous
    /// acknowledged event.
    ///
    /// This is an in-memory processed ACK only. It never sends DCP network
    /// credit and never performs checkpoint I/O.
    ///
    /// # Errors
    ///
    /// Returns a checkpoint error if the coordinator was poisoned or this
    /// token is obsolete.
    pub fn acknowledge(self) -> Result<AckOutcome> {
        let mut state = lock_state(&self.state)?;
        let partition = state.partitions.get_mut(&self.vbucket).ok_or_else(|| {
            DcpError::Checkpoint(format!(
                "vBucket {} is not tracked by the checkpoint coordinator",
                self.vbucket
            ))
        })?;
        let pending = partition
            .pending
            .iter_mut()
            .find(|pending| pending.delivery_id == self.delivery_id)
            .ok_or_else(|| {
                DcpError::Checkpoint(format!(
                    "checkpoint ACK {} for vBucket {} is obsolete",
                    self.delivery_id, self.vbucket
                ))
            })?;
        pending.acknowledged = true;

        let mut advanced_to = None;
        while partition
            .pending
            .front()
            .is_some_and(|pending| pending.acknowledged)
        {
            let Some(pending) = partition.pending.pop_front() else {
                break;
            };
            partition.processed = pending.checkpoint;
            advanced_to = Some(partition.processed.clone());
        }
        if advanced_to.is_some() {
            partition.generation = partition.generation.checked_add(1).ok_or_else(|| {
                DcpError::Checkpoint(format!(
                    "checkpoint generation overflow for vBucket {}",
                    self.vbucket
                ))
            })?;
        }
        Ok(AckOutcome {
            vbucket: self.vbucket,
            acknowledged_seqno: self.seqno,
            advanced_to,
            pending_events: partition.pending.len(),
        })
    }
}

/// DCP event paired with its optional processed-ACK token.
pub struct TrackedEvent {
    event: DcpEvent,
    ack: Option<EventAck>,
}

impl fmt::Debug for TrackedEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrackedEvent")
            .field("event", &self.event)
            .field("requires_ack", &self.ack.is_some())
            .finish()
    }
}

impl TrackedEvent {
    /// Borrow the delivered DCP event.
    #[must_use]
    pub const fn event(&self) -> &DcpEvent {
        &self.event
    }

    /// Whether this sequence-bearing event must be acknowledged after
    /// successful application processing.
    #[must_use]
    pub const fn requires_ack(&self) -> bool {
        self.ack.is_some()
    }

    /// Splits the event and its optional single-use ACK token.
    #[must_use]
    pub fn into_parts(self) -> (DcpEvent, Option<EventAck>) {
        (self.event, self.ack)
    }

    /// Acknowledges this event when it advances sequence progress.
    /// Snapshot/stream boundary events return `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns an obsolete-token or coordinator-state error.
    pub fn acknowledge(self) -> Result<Option<AckOutcome>> {
        self.ack.map(EventAck::acknowledge).transpose()
    }
}

/// Item emitted by a checkpoint-aware adapter around [`crate::DcpStream`].
#[derive(Debug)]
#[non_exhaustive]
pub enum CheckpointStreamItem {
    /// DCP event with explicit processed-ACK ownership.
    Event(TrackedEvent),
    /// Raw cluster configuration notification.
    TopologyConfig {
        /// Peer that supplied the configuration.
        source: String,
        /// Raw configuration payload.
        payload: Bytes,
    },
    /// Future server request retained for forward compatibility.
    Unknown(Frame),
}

/// Stream adapter that registers every sequence-bearing event before yielding
/// it to the application.
pub struct CheckpointStream<S> {
    inner: S,
    coordinator: CheckpointCoordinator,
}

impl<S> fmt::Debug for CheckpointStream<S>
where
    S: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckpointStream")
            .field("inner", &self.inner)
            .field("coordinator", &self.coordinator)
            .finish()
    }
}

impl<S> CheckpointStream<S> {
    /// Checkpoint coordinator shared with yielded ACK tokens.
    #[must_use]
    pub const fn coordinator(&self) -> &CheckpointCoordinator {
        &self.coordinator
    }

    /// Releases the underlying stream and coordinator.
    #[must_use]
    pub fn into_parts(self) -> (S, CheckpointCoordinator) {
        (self.inner, self.coordinator)
    }
}

impl<S> Stream for CheckpointStream<S>
where
    S: Stream<Item = Result<DcpStreamItem>> + Unpin,
{
    type Item = Result<CheckpointStreamItem>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(context) {
            Poll::Ready(Some(Ok(DcpStreamItem::Event(event)))) => Poll::Ready(Some(
                self.coordinator
                    .track_event(event)
                    .map(CheckpointStreamItem::Event),
            )),
            Poll::Ready(Some(Ok(DcpStreamItem::TopologyConfig { source, payload }))) => {
                Poll::Ready(Some(Ok(CheckpointStreamItem::TopologyConfig {
                    source,
                    payload,
                })))
            }
            Poll::Ready(Some(Ok(DcpStreamItem::Unknown(frame)))) => {
                Poll::Ready(Some(Ok(CheckpointStreamItem::Unknown(frame))))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Result of one manual or automatic durable flush.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointFlushReport {
    /// Dirty vBuckets included in the store call.
    pub attempted: usize,
    /// Checkpoints confirmed persisted by the store call.
    pub persisted: usize,
    /// vBuckets still dirty after reconciling concurrent ACKs.
    pub remaining_dirty: usize,
    /// End-to-end flush duration.
    pub elapsed: Duration,
}

/// Per-vBucket processed/durable status snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionCheckpointStatus {
    /// Last checkpoint confirmed by a successful store call.
    pub durable: DcpCheckpoint,
    /// Highest contiguous application-acknowledged checkpoint.
    pub processed: DcpCheckpoint,
    /// Delivered events still waiting for contiguous processed ACKs.
    pub pending_events: usize,
    /// Whether processed progress is newer than confirmed durable progress.
    pub dirty: bool,
}

/// Coordinator-level checkpoint metrics and last automatic-flush failure.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CheckpointMetrics {
    /// Total store flush attempts containing at least one dirty vBucket.
    pub flush_attempts: u64,
    /// Store flush attempts that failed or timed out.
    pub flush_failures: u64,
    /// Duration of the most recent non-empty attempt.
    pub last_flush_duration: Option<Duration>,
    /// Most recent flush error; cleared after a successful non-empty flush.
    pub last_flush_error: Option<String>,
}

/// Tokio-driven per-vBucket processed and durable checkpoint coordinator.
#[derive(Clone)]
pub struct CheckpointCoordinator {
    inner: Arc<CheckpointInner>,
}

impl fmt::Debug for CheckpointCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckpointCoordinator")
            .field("bucket_uuid", &self.inner.bucket_uuid)
            .field("mode", &self.inner.config.mode)
            .finish_non_exhaustive()
    }
}

impl CheckpointCoordinator {
    /// Creates a coordinator from the effective start checkpoint of every
    /// assigned vBucket and starts the automatic Tokio flush loop when enabled.
    ///
    /// # Errors
    ///
    /// Returns a configuration, bucket-identity, checkpoint, or Tokio runtime
    /// error.
    pub async fn new(
        config: CheckpointConfig,
        store: Arc<dyn CheckpointStore>,
        checkpoints: BTreeMap<u16, DcpCheckpoint>,
    ) -> Result<Self> {
        config.validate()?;
        let bucket_uuid = validate_initial_checkpoints(&checkpoints)?;
        let partitions = checkpoints
            .into_iter()
            .map(|(vbucket, checkpoint)| {
                (
                    vbucket,
                    PartitionState {
                        last_registered_seqno: checkpoint.seqno,
                        durable: checkpoint.clone(),
                        processed: checkpoint,
                        snapshot: None,
                        pending: VecDeque::new(),
                        next_delivery_id: 1,
                        generation: 0,
                        durable_generation: 0,
                    },
                )
            })
            .collect();
        let (cancel, cancel_receiver) = watch::channel(false);
        let inner = Arc::new(CheckpointInner {
            state: Arc::new(Mutex::new(CoordinatorState {
                partitions,
                metrics: CheckpointMetrics::default(),
            })),
            store,
            config,
            bucket_uuid,
            flush_gate: Arc::new(AsyncMutex::new(())),
            cancel,
            scheduler: AsyncMutex::new(None),
        });
        if let CheckpointMode::Automatic { flush_interval } = inner.config.mode {
            let weak = Arc::downgrade(&inner);
            let handle = tokio::spawn(run_scheduler(weak, cancel_receiver, flush_interval));
            *inner.scheduler.lock().await = Some(handle);
        }
        Ok(Self { inner })
    }

    /// Wraps a DCP item stream so event ACKs are registered before delivery.
    #[must_use]
    pub fn wrap<S>(&self, stream: S) -> CheckpointStream<S> {
        CheckpointStream {
            inner: stream,
            coordinator: self.clone(),
        }
    }

    /// Registers one DCP event and attaches a processed-ACK token when it has a
    /// sequence number.
    ///
    /// # Errors
    ///
    /// Rejects unknown vBuckets, missing/invalid snapshots, or non-monotonic
    /// delivery.
    pub fn track_event(&self, event: DcpEvent) -> Result<TrackedEvent> {
        let vbucket = event.vbucket();
        let mut state = lock_state(&self.inner.state)?;
        let partition = state.partitions.get_mut(&vbucket).ok_or_else(|| {
            DcpError::Checkpoint(format!("received event for untracked vBucket {vbucket}"))
        })?;
        if let DcpEvent::SnapshotMarker(marker) = &event {
            if marker.start_seqno > marker.end_seqno || marker.end_seqno < partition.processed.seqno
            {
                return Err(DcpError::Checkpoint(format!(
                    "snapshot {}..={} is invalid after processed seqno {} for vBucket {vbucket}",
                    marker.start_seqno, marker.end_seqno, partition.processed.seqno
                )));
            }
            partition.snapshot = Some((marker.start_seqno, marker.end_seqno));
            return Ok(TrackedEvent { event, ack: None });
        }

        let Some(seqno) = event.seqno() else {
            return Ok(TrackedEvent { event, ack: None });
        };
        let (snapshot_start, snapshot_end) = partition.snapshot.ok_or_else(|| {
            DcpError::Checkpoint(format!(
                "received seqno {seqno} before a snapshot marker for vBucket {vbucket}"
            ))
        })?;
        if seqno <= partition.last_registered_seqno
            || seqno < snapshot_start
            || seqno > snapshot_end
        {
            return Err(DcpError::Checkpoint(format!(
                "seqno {seqno} is non-monotonic or outside snapshot {snapshot_start}..={snapshot_end} for vBucket {vbucket}"
            )));
        }
        let mut checkpoint = partition.pending.back().map_or_else(
            || partition.processed.clone(),
            |pending| pending.checkpoint.clone(),
        );
        checkpoint.seqno = seqno;
        checkpoint.snapshot_start = snapshot_start;
        checkpoint.snapshot_end = snapshot_end;
        if let DcpEvent::SystemEvent(system_event) = &event {
            checkpoint.manifest_uid = Some(system_event.manifest_uid);
        }
        checkpoint.validate()?;

        let delivery_id = partition.next_delivery_id;
        partition.next_delivery_id =
            partition.next_delivery_id.checked_add(1).ok_or_else(|| {
                DcpError::Checkpoint(format!("delivery ID overflow for vBucket {vbucket}"))
            })?;
        partition.last_registered_seqno = seqno;
        partition.pending.push_back(PendingAck {
            delivery_id,
            checkpoint,
            acknowledged: false,
        });
        Ok(TrackedEvent {
            event,
            ack: Some(EventAck {
                state: Arc::clone(&self.inner.state),
                vbucket,
                delivery_id,
                seqno,
            }),
        })
    }

    /// Flushes every dirty, contiguous processed checkpoint through the store.
    ///
    /// # Errors
    ///
    /// Returns a bounded timeout or store error. A failed batch remains dirty.
    pub async fn flush(&self) -> Result<CheckpointFlushReport> {
        self.inner.flush().await
    }

    /// Marks an effective start checkpoint dirty even when its sequence number
    /// equals the last durable in-memory value.
    ///
    /// Use this after applying a missing-checkpoint fallback such as `latest`,
    /// so the captured start position is persisted before a restart can skip
    /// later offline changes.
    ///
    /// # Errors
    ///
    /// Returns an unknown-vBucket, generation-overflow, or poisoned-state
    /// error.
    pub fn mark_dirty(&self, vbucket: u16) -> Result<()> {
        let mut state = lock_state(&self.inner.state)?;
        let partition = state.partitions.get_mut(&vbucket).ok_or_else(|| {
            DcpError::Checkpoint(format!(
                "vBucket {vbucket} is not tracked by the checkpoint coordinator"
            ))
        })?;
        partition.generation = partition.generation.checked_add(1).ok_or_else(|| {
            DcpError::Checkpoint(format!(
                "checkpoint generation overflow for vBucket {vbucket}"
            ))
        })?;
        Ok(())
    }

    /// Stops the automatic loop, waits for it, and performs a final flush.
    ///
    /// # Errors
    ///
    /// Returns a scheduler join, timeout, or store error.
    pub async fn shutdown(&self) -> Result<CheckpointFlushReport> {
        let _ = self.inner.cancel.send(true);
        if let Some(handle) = self.inner.scheduler.lock().await.take() {
            handle.await.map_err(|error| {
                DcpError::Checkpoint(format!("checkpoint scheduler task failed: {error}"))
            })?;
        }
        self.flush().await
    }

    /// Per-vBucket processed/durable state at one instant.
    ///
    /// # Errors
    ///
    /// Returns an error if the coordinator state was poisoned.
    pub fn statuses(&self) -> Result<BTreeMap<u16, PartitionCheckpointStatus>> {
        let state = lock_state(&self.inner.state)?;
        Ok(state
            .partitions
            .iter()
            .map(|(&vbucket, partition)| {
                (
                    vbucket,
                    PartitionCheckpointStatus {
                        durable: partition.durable.clone(),
                        processed: partition.processed.clone(),
                        pending_events: partition.pending.len(),
                        dirty: partition.is_dirty(),
                    },
                )
            })
            .collect())
    }

    /// Returns the contiguous application-processed checkpoint for every
    /// tracked vBucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the coordinator state was poisoned.
    pub fn processed_checkpoints(&self) -> Result<BTreeMap<u16, DcpCheckpoint>> {
        let state = lock_state(&self.inner.state)?;
        Ok(state
            .partitions
            .iter()
            .map(|(&vbucket, partition)| (vbucket, partition.processed.clone()))
            .collect())
    }

    /// Replaces effective stream starts after a connection generation is
    /// reopened and invalidates every outstanding ACK token for those
    /// vBuckets.
    ///
    /// A changed effective checkpoint is marked dirty, including a rollback
    /// to an earlier sequence or a new failover UUID, so a later flush cannot
    /// leave the pre-reopen resume point durable.
    ///
    /// # Errors
    ///
    /// Returns an identity, checkpoint, unknown-vBucket, generation-overflow,
    /// or poisoned-state error. Validation completes before any partition is
    /// changed.
    pub fn rebase_partitions(&self, effective: &BTreeMap<u16, DcpCheckpoint>) -> Result<()> {
        let mut state = lock_state(&self.inner.state)?;
        for (&vbucket, checkpoint) in effective {
            let partition = state.partitions.get(&vbucket).ok_or_else(|| {
                DcpError::Checkpoint(format!("cannot rebase untracked vBucket {vbucket}"))
            })?;
            validate_checkpoint_identity(vbucket, &self.inner.bucket_uuid, checkpoint)?;
            if partition.processed != *checkpoint && partition.generation == u64::MAX {
                return Err(DcpError::Checkpoint(format!(
                    "checkpoint generation overflow for vBucket {vbucket}"
                )));
            }
        }
        for (&vbucket, checkpoint) in effective {
            let partition = state.partitions.get_mut(&vbucket).ok_or_else(|| {
                DcpError::Checkpoint(format!("cannot rebase untracked vBucket {vbucket}"))
            })?;
            if partition.processed != *checkpoint {
                partition.generation += 1;
            }
            partition.processed = checkpoint.clone();
            partition.last_registered_seqno = checkpoint.seqno;
            partition.snapshot = None;
            partition.pending.clear();
        }
        Ok(())
    }

    /// Flush counters and last automatic/manual store error.
    ///
    /// # Errors
    ///
    /// Returns an error if the coordinator state was poisoned.
    pub fn metrics(&self) -> Result<CheckpointMetrics> {
        Ok(lock_state(&self.inner.state)?.metrics.clone())
    }
}

struct CheckpointInner {
    state: Arc<Mutex<CoordinatorState>>,
    store: Arc<dyn CheckpointStore>,
    config: CheckpointConfig,
    bucket_uuid: String,
    flush_gate: Arc<AsyncMutex<()>>,
    cancel: watch::Sender<bool>,
    scheduler: AsyncMutex<Option<JoinHandle<()>>>,
}

impl CheckpointInner {
    async fn flush(&self) -> Result<CheckpointFlushReport> {
        let started = Instant::now();
        let has_dirty = lock_state(&self.state)?
            .partitions
            .values()
            .any(PartitionState::is_dirty);
        if !has_dirty {
            return Ok(CheckpointFlushReport {
                attempted: 0,
                persisted: 0,
                remaining_dirty: 0,
                elapsed: started.elapsed(),
            });
        }

        let deadline = time::Instant::now() + self.config.timeout;
        let Ok(flush_guard) =
            time::timeout_at(deadline, Arc::clone(&self.flush_gate).lock_owned()).await
        else {
            let error = DcpError::Timeout(self.config.timeout);
            self.record_flush_failure(started.elapsed(), &error, true)?;
            return Err(error);
        };
        let dirty = {
            let state = lock_state(&self.state)?;
            state
                .partitions
                .iter()
                .filter(|(_, partition)| partition.is_dirty())
                .map(|(&vbucket, partition)| DirtyCheckpoint {
                    vbucket,
                    checkpoint: partition.processed.clone(),
                    generation: partition.generation,
                })
                .collect::<Vec<_>>()
        };
        if dirty.is_empty() {
            return Ok(CheckpointFlushReport {
                attempted: 0,
                persisted: 0,
                remaining_dirty: 0,
                elapsed: started.elapsed(),
            });
        }
        {
            let mut state = lock_state(&self.state)?;
            state.metrics.flush_attempts = state
                .metrics
                .flush_attempts
                .checked_add(1)
                .ok_or_else(|| DcpError::Checkpoint("flush attempt counter overflow".into()))?;
        }
        let checkpoints = dirty
            .iter()
            .map(|dirty| dirty.checkpoint.clone())
            .collect::<Vec<_>>();
        let store = Arc::clone(&self.store);
        let operation = tokio::spawn(async move {
            let _flush_guard = flush_guard;
            store.save(&checkpoints).await
        });
        let store_result = time::timeout_at(deadline, operation).await;
        let elapsed = started.elapsed();
        let error = match store_result {
            Ok(Ok(Ok(()))) => None,
            Ok(Ok(Err(error))) => Some(error),
            Ok(Err(error)) => Some(DcpError::CheckpointStore(format!(
                "checkpoint store task failed: {error}"
            ))),
            Err(_) => Some(DcpError::Timeout(self.config.timeout)),
        };
        if let Some(error) = error {
            self.record_flush_failure(elapsed, &error, false)?;
            return Err(error);
        }

        let mut state = lock_state(&self.state)?;
        for saved in &dirty {
            let partition = state
                .partitions
                .get_mut(&saved.vbucket)
                .expect("dirty checkpoint came from this map");
            partition.durable = saved.checkpoint.clone();
            partition.durable_generation = saved.generation;
        }
        state.metrics.last_flush_duration = Some(elapsed);
        state.metrics.last_flush_error = None;
        let remaining_dirty = state
            .partitions
            .values()
            .filter(|partition| partition.is_dirty())
            .count();
        Ok(CheckpointFlushReport {
            attempted: dirty.len(),
            persisted: dirty.len(),
            remaining_dirty,
            elapsed,
        })
    }

    fn record_flush_failure(
        &self,
        elapsed: Duration,
        error: &DcpError,
        increment_attempt: bool,
    ) -> Result<()> {
        let mut state = lock_state(&self.state)?;
        if increment_attempt {
            state.metrics.flush_attempts = state
                .metrics
                .flush_attempts
                .checked_add(1)
                .ok_or_else(|| DcpError::Checkpoint("flush attempt counter overflow".into()))?;
        }
        state.metrics.flush_failures = state
            .metrics
            .flush_failures
            .checked_add(1)
            .ok_or_else(|| DcpError::Checkpoint("flush failure counter overflow".into()))?;
        state.metrics.last_flush_duration = Some(elapsed);
        state.metrics.last_flush_error = Some(error.to_string());
        Ok(())
    }
}

impl Drop for CheckpointInner {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

async fn run_scheduler(
    inner: Weak<CheckpointInner>,
    mut cancel: watch::Receiver<bool>,
    flush_interval: Duration,
) {
    let mut ticker = time::interval(flush_interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    return;
                }
            }
            _ = ticker.tick() => {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                let _ = inner.flush().await;
            }
        }
    }
}

#[derive(Debug)]
struct CoordinatorState {
    partitions: BTreeMap<u16, PartitionState>,
    metrics: CheckpointMetrics,
}

#[derive(Debug)]
struct PartitionState {
    durable: DcpCheckpoint,
    processed: DcpCheckpoint,
    snapshot: Option<(u64, u64)>,
    last_registered_seqno: u64,
    pending: VecDeque<PendingAck>,
    next_delivery_id: u64,
    generation: u64,
    durable_generation: u64,
}

impl PartitionState {
    fn is_dirty(&self) -> bool {
        self.generation != self.durable_generation || self.durable != self.processed
    }
}

#[derive(Debug)]
struct PendingAck {
    delivery_id: u64,
    checkpoint: DcpCheckpoint,
    acknowledged: bool,
}

#[derive(Debug)]
struct DirtyCheckpoint {
    vbucket: u16,
    checkpoint: DcpCheckpoint,
    generation: u64,
}

fn validate_initial_checkpoints(checkpoints: &BTreeMap<u16, DcpCheckpoint>) -> Result<String> {
    if checkpoints.is_empty() {
        return Err(DcpError::InvalidConfiguration(
            "checkpoint coordinator requires at least one vBucket".into(),
        ));
    }
    let mut bucket_uuid = None;
    for (&vbucket, checkpoint) in checkpoints {
        checkpoint.validate()?;
        if checkpoint.vbucket != vbucket {
            return Err(DcpError::Checkpoint(format!(
                "checkpoint map key {vbucket} does not match checkpoint vBucket {}",
                checkpoint.vbucket
            )));
        }
        let observed = checkpoint.bucket_uuid.as_deref().ok_or_else(|| {
            DcpError::Checkpoint(format!("vBucket {vbucket} checkpoint has no bucket UUID"))
        })?;
        if observed.is_empty() {
            return Err(DcpError::Checkpoint(format!(
                "vBucket {vbucket} checkpoint has an empty bucket UUID"
            )));
        }
        if let Some(expected) = bucket_uuid
            && expected != observed
        {
            return Err(DcpError::Checkpoint(format!(
                "checkpoint bucket UUID {observed} does not match {expected}"
            )));
        }
        bucket_uuid = Some(observed);
    }
    Ok(bucket_uuid.expect("non-empty map was checked").to_owned())
}

fn validate_checkpoint_identity(
    vbucket: u16,
    bucket_uuid: &str,
    checkpoint: &DcpCheckpoint,
) -> Result<()> {
    if checkpoint.vbucket != vbucket {
        return Err(DcpError::Checkpoint(format!(
            "loaded checkpoint key {vbucket} does not match vBucket {}",
            checkpoint.vbucket
        )));
    }
    if checkpoint.bucket_uuid.as_deref() != Some(bucket_uuid) {
        return Err(DcpError::Checkpoint(format!(
            "vBucket {vbucket} checkpoint does not belong to bucket {bucket_uuid}"
        )));
    }
    checkpoint.validate()
}

fn lock_state(state: &Arc<Mutex<CoordinatorState>>) -> Result<MutexGuard<'_, CoordinatorState>> {
    state
        .lock()
        .map_err(|_| DcpError::Checkpoint("checkpoint coordinator state was poisoned".into()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures_util::{StreamExt, stream};
    use rust_dcp_protocol::{Frame, Opcode};
    use tokio::sync::Semaphore;

    use super::*;
    use crate::{SeqNoAdvanced, SnapshotFlags, SnapshotMarker};

    struct MemoryStore {
        checkpoints: Mutex<BTreeMap<u16, DcpCheckpoint>>,
        block_save: AtomicBool,
        save_started: Semaphore,
        save_finished: Semaphore,
        release_save: Semaphore,
    }

    impl Default for MemoryStore {
        fn default() -> Self {
            Self {
                checkpoints: Mutex::new(BTreeMap::new()),
                block_save: AtomicBool::new(false),
                save_started: Semaphore::new(0),
                save_finished: Semaphore::new(0),
                release_save: Semaphore::new(0),
            }
        }
    }

    impl MemoryStore {
        fn saved(&self, vbucket: u16) -> Option<DcpCheckpoint> {
            self.checkpoints.lock().unwrap().get(&vbucket).cloned()
        }
    }

    impl CheckpointStore for MemoryStore {
        fn load<'a>(
            &'a self,
            bucket_uuid: &'a str,
            vbuckets: &'a [u16],
        ) -> Pin<
            Box<dyn std::future::Future<Output = Result<BTreeMap<u16, DcpCheckpoint>>> + Send + 'a>,
        > {
            Box::pin(async move {
                Ok(self
                    .checkpoints
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(vbucket, checkpoint)| {
                        vbuckets.contains(vbucket)
                            && checkpoint.bucket_uuid.as_deref() == Some(bucket_uuid)
                    })
                    .map(|(&vbucket, checkpoint)| (vbucket, checkpoint.clone()))
                    .collect())
            })
        }

        fn save<'a>(
            &'a self,
            checkpoints: &'a [DcpCheckpoint],
        ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                self.save_started.add_permits(1);
                if self.block_save.load(Ordering::SeqCst) {
                    self.release_save.acquire().await.unwrap().forget();
                }
                let mut saved = self.checkpoints.lock().unwrap();
                for checkpoint in checkpoints {
                    saved.insert(checkpoint.vbucket, checkpoint.clone());
                }
                self.save_finished.add_permits(1);
                Ok(())
            })
        }

        fn clear<'a>(
            &'a self,
            _bucket_uuid: &'a str,
            vbuckets: &'a [u16],
        ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                let mut saved = self.checkpoints.lock().unwrap();
                for vbucket in vbuckets {
                    saved.remove(vbucket);
                }
                Ok(())
            })
        }
    }

    fn manual_config() -> CheckpointConfig {
        CheckpointConfig {
            mode: CheckpointMode::Manual,
            timeout: Duration::from_secs(1),
        }
    }

    fn checkpoint(vbucket: u16) -> DcpCheckpoint {
        DcpCheckpoint {
            bucket_uuid: Some("bucket-id".into()),
            vbucket,
            vbucket_uuid: 0xaaaa,
            seqno: 0,
            snapshot_start: 0,
            snapshot_end: 0,
            manifest_uid: None,
        }
    }

    fn snapshot(vbucket: u16, start: u64, end: u64) -> DcpEvent {
        DcpEvent::SnapshotMarker(SnapshotMarker {
            vbucket,
            start_seqno: start,
            end_seqno: end,
            flags: SnapshotFlags::MEMORY,
            high_completed_seqno: None,
            max_visible_seqno: None,
            purge_seqno: None,
        })
    }

    fn progress(vbucket: u16, seqno: u64) -> DcpEvent {
        DcpEvent::SeqNoAdvanced(SeqNoAdvanced { vbucket, seqno })
    }

    async fn coordinator(
        config: CheckpointConfig,
        store: Arc<MemoryStore>,
    ) -> CheckpointCoordinator {
        CheckpointCoordinator::new(config, store, BTreeMap::from([(7, checkpoint(7))]))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn out_of_order_ack_advances_only_contiguous_delivery() {
        let store = Arc::new(MemoryStore::default());
        let coordinator = coordinator(manual_config(), store.clone()).await;
        assert!(
            coordinator
                .track_event(snapshot(7, 1, 2))
                .unwrap()
                .acknowledge()
                .unwrap()
                .is_none()
        );
        let first = coordinator.track_event(progress(7, 1)).unwrap();
        let second = coordinator.track_event(progress(7, 2)).unwrap();

        let second_outcome = second.acknowledge().unwrap().unwrap();
        assert_eq!(second_outcome.advanced_to, None);
        assert_eq!(coordinator.statuses().unwrap()[&7].processed.seqno, 0);

        let first_outcome = first.acknowledge().unwrap().unwrap();
        assert_eq!(first_outcome.advanced_to.unwrap().seqno, 2);
        assert_eq!(first_outcome.pending_events, 0);
        assert!(coordinator.statuses().unwrap()[&7].dirty);

        let report = coordinator.flush().await.unwrap();
        assert_eq!(report.attempted, 1);
        assert_eq!(report.remaining_dirty, 0);
        assert_eq!(store.saved(7).unwrap().seqno, 2);
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn forced_dirty_persists_an_unchanged_fallback_checkpoint() {
        let store = Arc::new(MemoryStore::default());
        let coordinator = coordinator(manual_config(), store.clone()).await;

        assert!(!coordinator.statuses().unwrap()[&7].dirty);
        assert_eq!(coordinator.flush().await.unwrap().attempted, 0);

        coordinator.mark_dirty(7).unwrap();
        assert!(coordinator.statuses().unwrap()[&7].dirty);
        let report = coordinator.flush().await.unwrap();

        assert_eq!(report.attempted, 1);
        assert_eq!(report.persisted, 1);
        assert_eq!(report.remaining_dirty, 0);
        assert_eq!(store.saved(7), Some(checkpoint(7)));
        assert!(!coordinator.statuses().unwrap()[&7].dirty);
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reconnect_rebases_processed_state_and_invalidates_old_ack_tokens() {
        let store = Arc::new(MemoryStore::default());
        let coordinator = coordinator(manual_config(), store.clone()).await;
        coordinator.track_event(snapshot(7, 1, 2)).unwrap();
        coordinator
            .track_event(progress(7, 1))
            .unwrap()
            .acknowledge()
            .unwrap();
        let stale = coordinator.track_event(progress(7, 2)).unwrap();
        let mut effective = coordinator.processed_checkpoints().unwrap()[&7].clone();
        effective.vbucket_uuid = 0xbbbb;

        coordinator
            .rebase_partitions(&BTreeMap::from([(7, effective.clone())]))
            .unwrap();

        let status = &coordinator.statuses().unwrap()[&7];
        assert_eq!(status.processed, effective);
        assert_eq!(status.pending_events, 0);
        assert!(status.dirty);
        assert!(stale.acknowledge().is_err());

        coordinator.track_event(snapshot(7, 2, 2)).unwrap();
        coordinator
            .track_event(progress(7, 2))
            .unwrap()
            .acknowledge()
            .unwrap();
        coordinator.flush().await.unwrap();
        assert_eq!(store.saved(7).unwrap().vbucket_uuid, 0xbbbb);
        assert_eq!(store.saved(7).unwrap().seqno, 2);
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_ack_during_flush_remains_dirty_for_the_next_flush() {
        let store = Arc::new(MemoryStore::default());
        store.block_save.store(true, Ordering::SeqCst);
        let coordinator = coordinator(manual_config(), store.clone()).await;
        coordinator.track_event(snapshot(7, 1, 2)).unwrap();
        coordinator
            .track_event(progress(7, 1))
            .unwrap()
            .acknowledge()
            .unwrap();

        let flushing = {
            let coordinator = coordinator.clone();
            tokio::spawn(async move { coordinator.flush().await })
        };
        store.save_started.acquire().await.unwrap().forget();
        coordinator
            .track_event(progress(7, 2))
            .unwrap()
            .acknowledge()
            .unwrap();
        store.block_save.store(false, Ordering::SeqCst);
        store.release_save.add_permits(1);

        let report = flushing.await.unwrap().unwrap();
        assert_eq!(report.remaining_dirty, 1);
        let status = &coordinator.statuses().unwrap()[&7];
        assert_eq!(status.durable.seqno, 1);
        assert_eq!(status.processed.seqno, 2);
        assert!(status.dirty);

        coordinator.flush().await.unwrap();
        assert_eq!(store.saved(7).unwrap().seqno, 2);
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn automatic_mode_uses_tokio_scheduler_and_flushes_processed_progress() {
        let store = Arc::new(MemoryStore::default());
        let coordinator = coordinator(
            CheckpointConfig {
                mode: CheckpointMode::Automatic {
                    flush_interval: Duration::from_millis(5),
                },
                timeout: Duration::from_secs(1),
            },
            store.clone(),
        )
        .await;
        coordinator.track_event(snapshot(7, 1, 1)).unwrap();
        coordinator
            .track_event(progress(7, 1))
            .unwrap()
            .acknowledge()
            .unwrap();

        time::timeout(Duration::from_millis(200), async {
            loop {
                if store
                    .saved(7)
                    .is_some_and(|checkpoint| checkpoint.seqno == 1)
                {
                    break;
                }
                time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(coordinator.metrics().unwrap().flush_attempts, 1);
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn store_timeout_keeps_checkpoint_dirty() {
        let store = Arc::new(MemoryStore::default());
        store.block_save.store(true, Ordering::SeqCst);
        let coordinator = coordinator(
            CheckpointConfig {
                mode: CheckpointMode::Manual,
                timeout: Duration::from_millis(10),
            },
            store.clone(),
        )
        .await;
        coordinator.track_event(snapshot(7, 1, 1)).unwrap();
        coordinator
            .track_event(progress(7, 1))
            .unwrap()
            .acknowledge()
            .unwrap();

        assert!(matches!(
            coordinator.flush().await,
            Err(DcpError::Timeout(_))
        ));
        assert!(coordinator.statuses().unwrap()[&7].dirty);
        assert_eq!(coordinator.metrics().unwrap().flush_failures, 1);

        coordinator.track_event(snapshot(7, 2, 2)).unwrap();
        coordinator
            .track_event(progress(7, 2))
            .unwrap()
            .acknowledge()
            .unwrap();
        store.block_save.store(false, Ordering::SeqCst);
        store.release_save.add_permits(1);
        store.save_finished.acquire().await.unwrap().forget();
        coordinator.flush().await.unwrap();
        assert_eq!(store.saved(7).unwrap().seqno, 2);
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stream_adapter_preserves_control_items_and_registers_events() {
        let store = Arc::new(MemoryStore::default());
        let coordinator = coordinator(manual_config(), store).await;
        let source = stream::iter(vec![
            Ok(DcpStreamItem::TopologyConfig {
                source: "node-a".into(),
                payload: Bytes::from_static(b"config"),
            }),
            Ok(DcpStreamItem::Unknown(Frame::request(Opcode(0x63)))),
            Ok(DcpStreamItem::Event(snapshot(7, 1, 1))),
            Ok(DcpStreamItem::Event(progress(7, 1))),
        ]);
        let mut stream = coordinator.wrap(source);

        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            CheckpointStreamItem::TopologyConfig { .. }
        ));
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            CheckpointStreamItem::Unknown(_)
        ));
        let CheckpointStreamItem::Event(marker) = stream.next().await.unwrap().unwrap() else {
            panic!("expected marker");
        };
        assert!(!marker.requires_ack());
        let CheckpointStreamItem::Event(event) = stream.next().await.unwrap().unwrap() else {
            panic!("expected progress event");
        };
        assert!(event.requires_ack());
        event.acknowledge().unwrap();
        assert_eq!(coordinator.statuses().unwrap()[&7].processed.seqno, 1);
        coordinator.shutdown().await.unwrap();
    }
}
