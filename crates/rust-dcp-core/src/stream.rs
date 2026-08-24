use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_util::Stream;
use rust_dcp_protocol::{
    DcpMessage, DcpStreamFlags, Frame, HelloFeature, Opcode, ProtocolError, Status, StreamFilter,
    StreamRequest, StreamRequestResponse, buffer_ack, close_stream, noop_response,
    parse_dcp_message, parse_stream_request_response, snapshot_marker_response, stream_request,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time,
};

use crate::{
    DataType, DcpCheckpoint, DcpConnection, DcpControlFeature, DcpDeletion, DcpError, DcpEvent,
    DcpExpiration, DcpMode, DcpMutation, FailoverEntry, FlowControlConfig, KvConnection,
    OsoSnapshot, OsoSnapshotState, Result, RollbackPolicy, SeqNoAdvanced, SnapshotFlags,
    SnapshotMarker, StartPosition, StreamEnd, StreamEndReason, SystemEvent, SystemEventKind,
    fetch_failover_log,
};

const MAX_ROLLBACK_ATTEMPTS: usize = 16;

/// Resolved request for one vBucket DCP stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VBucketStreamRequest {
    checkpoint: DcpCheckpoint,
    end_seqno: u64,
    flags: DcpStreamFlags,
    filter: Option<StreamFilter>,
}

impl VBucketStreamRequest {
    /// Resolves earliest/latest/checkpoint semantics against a captured high
    /// sequence number and current failover log.
    ///
    /// `high_seqno` is also frozen as the end boundary for finite mode. The
    /// default request is active-only and never opts into ignoring purged
    /// tombstones. For a collection filter, the wire manifest UID is replaced
    /// with the UID observed at this vBucket's checkpoint; a new stream omits
    /// it until a system event has actually been observed.
    ///
    /// # Errors
    ///
    /// Returns a checkpoint, bucket-identity, rollback, or configuration error
    /// when the requested start cannot be represented safely.
    pub fn resolve(
        vbucket: u16,
        bucket_uuid: Option<&str>,
        start: &StartPosition,
        mode: DcpMode,
        high_seqno: u64,
        failover_log: Vec<FailoverEntry>,
        filter: Option<StreamFilter>,
    ) -> Result<Self> {
        let newest = *failover_log.first().ok_or_else(|| {
            DcpError::Topology(format!("vBucket {vbucket} has an empty failover log"))
        })?;
        let explicit_checkpoint = matches!(start, StartPosition::Checkpoint(_));
        let mut checkpoint = match start {
            StartPosition::Earliest => DcpCheckpoint::earliest(vbucket),
            StartPosition::Latest => DcpCheckpoint {
                seqno: high_seqno,
                snapshot_start: high_seqno,
                snapshot_end: high_seqno,
                ..DcpCheckpoint::earliest(vbucket)
            },
            StartPosition::Checkpoint(checkpoint) => checkpoint.clone(),
        };
        if checkpoint.vbucket != vbucket {
            return Err(DcpError::Checkpoint(format!(
                "checkpoint vBucket {} does not match requested vBucket {vbucket}",
                checkpoint.vbucket
            )));
        }
        checkpoint.validate()?;
        if let (Some(expected), Some(observed)) = (bucket_uuid, checkpoint.bucket_uuid.as_deref())
            && expected != observed
        {
            return Err(DcpError::Checkpoint(format!(
                "checkpoint bucket UUID {observed} does not match current bucket {expected}"
            )));
        }
        if checkpoint.bucket_uuid.is_none() {
            checkpoint.bucket_uuid = bucket_uuid.map(str::to_owned);
        }
        if checkpoint.vbucket_uuid == 0 {
            checkpoint.vbucket_uuid = newest.vbucket_uuid;
        }
        if checkpoint.seqno > high_seqno {
            return Err(DcpError::RollbackRequired {
                vbucket,
                requested_seqno: checkpoint.seqno,
                rollback_seqno: high_seqno,
                failover_log,
            });
        }

        let mut flags = DcpStreamFlags::ACTIVE_ONLY;
        if explicit_checkpoint && checkpoint.seqno == 0 {
            flags |= DcpStreamFlags::STRICT_VBUUID;
        }
        let mut filter = filter;
        if let Some(filter) = filter.as_mut() {
            filter.manifest_uid = checkpoint.manifest_uid;
        }
        if filter.as_ref().and_then(|filter| filter.stream_id) == Some(0) {
            return Err(DcpError::InvalidConfiguration(format!(
                "vBucket {vbucket} stream ID must be non-zero"
            )));
        }

        Ok(Self {
            checkpoint,
            end_seqno: match mode {
                DcpMode::Finite => high_seqno,
                DcpMode::Infinite => u64::MAX,
            },
            flags,
            filter,
        })
    }

    /// Adds explicitly requested protocol flags.
    ///
    /// `IGNORE_PURGED_TOMBSTONES` can cause missed deletions and is never added
    /// by [`Self::resolve`].
    #[must_use]
    pub fn with_flags(mut self, flags: DcpStreamFlags) -> Self {
        self.flags |= flags;
        self
    }

    /// Effective starting checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &DcpCheckpoint {
        &self.checkpoint
    }

    /// Frozen finite end or `u64::MAX` for an infinite stream.
    #[must_use]
    pub const fn end_seqno(&self) -> u64 {
        self.end_seqno
    }

    pub(crate) fn with_frozen_end_seqno(mut self, end_seqno: u64) -> Result<Self> {
        if end_seqno < self.checkpoint.seqno {
            return Err(DcpError::Stream {
                vbucket: self.checkpoint.vbucket,
                message: format!(
                    "frozen stream end {end_seqno} is behind effective start {}",
                    self.checkpoint.seqno
                ),
            });
        }
        self.end_seqno = end_seqno;
        Ok(self)
    }

    /// Optional multiplexed stream ID.
    #[must_use]
    pub fn stream_id(&self) -> Option<u16> {
        self.filter.as_ref().and_then(|filter| filter.stream_id)
    }

    fn wire_request(&self) -> StreamRequest {
        StreamRequest {
            vbucket: self.checkpoint.vbucket,
            flags: self.flags,
            vbucket_uuid: self.checkpoint.vbucket_uuid,
            start_seqno: self.checkpoint.seqno,
            end_seqno: self.end_seqno,
            snapshot_start: self.checkpoint.snapshot_start,
            snapshot_end: self.checkpoint.snapshot_end,
            filter: self.filter.clone(),
            opaque: 0,
        }
    }

    fn rewind(&mut self, rollback_seqno: u64, failover_log: &[FailoverEntry]) -> Result<u64> {
        let branch = *failover_log
            .iter()
            .find(|entry| entry.seqno <= rollback_seqno)
            .ok_or_else(|| {
                DcpError::Topology(format!(
                    "failover log has no branch covering rollback seqno {rollback_seqno} for vBucket {}",
                    self.checkpoint.vbucket
                ))
            })?;
        self.checkpoint.vbucket_uuid = branch.vbucket_uuid;
        self.checkpoint.seqno = rollback_seqno;
        self.checkpoint.snapshot_start = rollback_seqno;
        self.checkpoint.snapshot_end = rollback_seqno;
        self.checkpoint.manifest_uid = None;
        if let Some(filter) = self.filter.as_mut() {
            filter.manifest_uid = None;
        }
        self.checkpoint.validate()?;
        Ok(branch.vbucket_uuid)
    }
}

/// Application decision returned by a delegated rollback handler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RollbackAction {
    /// Preserve the default safe behavior and return `RollbackRequired`.
    StopAndReport,
    /// Explicitly authorize rewind and at-least-once replay.
    RewindAndReplay,
}

/// Observable rollback request supplied to a delegated handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RollbackRequest {
    /// vBucket requiring recovery.
    pub vbucket: u16,
    /// Checkpoint rejected by the producer.
    pub checkpoint: DcpCheckpoint,
    /// Server-required rewind sequence number.
    pub rollback_seqno: u64,
    /// Fresh failover log, newest entry first.
    pub failover_log: Vec<FailoverEntry>,
}

/// Asynchronous application hook for delegated rollback policy.
pub trait RollbackHandler: Send + Sync {
    /// Chooses whether an observed rollback may replay downstream effects.
    fn handle(
        &self,
        request: RollbackRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RollbackAction>> + Send + '_>>;
}

/// Rewind that was explicitly applied while opening a stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RollbackApplied {
    /// Affected vBucket.
    pub vbucket: u16,
    /// Rejected sequence number.
    pub requested_seqno: u64,
    /// Effective replay start.
    pub rollback_seqno: u64,
    /// Failover branch selected for the replay.
    pub vbucket_uuid: u64,
}

/// Effective state returned for an opened partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionOpenState {
    effective_checkpoint: DcpCheckpoint,
    /// vBucket identifier.
    pub vbucket: u16,
    /// Effective start sequence number after any explicit rewind.
    pub start_seqno: u64,
    /// Finite end boundary or `u64::MAX`.
    pub end_seqno: u64,
    /// Producer-confirmed current failover branch UUID.
    pub vbucket_uuid: u64,
    /// Optional multiplexed stream ID.
    pub stream_id: Option<u16>,
    /// Opaque token assigned to this stream request and echoed by its events.
    pub opaque: u32,
}

impl PartitionOpenState {
    /// Complete checkpoint actually used by the accepted stream request.
    #[must_use]
    pub const fn checkpoint(&self) -> &DcpCheckpoint {
        &self.effective_checkpoint
    }
}

/// Synchronous result of opening every requested vBucket stream.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StreamOpenReport {
    partitions: BTreeMap<u16, PartitionOpenState>,
    rollbacks: Vec<RollbackApplied>,
}

impl StreamOpenReport {
    /// Opened partition states.
    #[must_use]
    pub const fn partitions(&self) -> &BTreeMap<u16, PartitionOpenState> {
        &self.partitions
    }

    /// Explicit rewinds applied before event delivery began.
    #[must_use]
    pub fn rollbacks(&self) -> &[RollbackApplied] {
        &self.rollbacks
    }
}

/// Item emitted by the asynchronous connection runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DcpStreamItem {
    /// Typed DCP data, snapshot, progress, system, or stream-end event.
    Event(DcpEvent),
    /// Raw CCCP notification for the topology owner to parse and apply.
    TopologyConfig {
        /// Peer that supplied the config.
        source: String,
        /// Raw bucket-config JSON or notification payload.
        payload: Bytes,
    },
    /// Future server request retained instead of silently discarded.
    Unknown(Frame),
}

/// Tokio-owned multi-vBucket DCP event stream for one KV connection.
pub struct DcpStream {
    receiver: mpsc::Receiver<Result<DcpStreamItem>>,
    commands: mpsc::Sender<RuntimeCommand>,
    worker: Option<JoinHandle<()>>,
    open_report: StreamOpenReport,
}

impl fmt::Debug for DcpStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DcpStream")
            .field("open_report", &self.open_report)
            .field(
                "worker_finished",
                &self.worker.as_ref().is_none_or(JoinHandle::is_finished),
            )
            .finish_non_exhaustive()
    }
}

impl DcpStream {
    /// Effective partition and rollback state established before delivery.
    #[must_use]
    pub const fn open_report(&self) -> &StreamOpenReport {
        &self.open_report
    }

    /// Requests closure of one vBucket stream on the connection.
    ///
    /// # Errors
    ///
    /// Returns a protocol/server error, an unknown-vBucket error, or
    /// cancellation if the worker has already stopped.
    pub async fn close_vbucket(&self, vbucket: u16) -> Result<()> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::Close { vbucket, response })
            .await
            .map_err(|_| DcpError::Cancelled)?;
        receiver.await.map_err(|_| DcpError::Cancelled)?
    }

    /// Stops the worker, flushes outstanding network credit, and closes the
    /// underlying connection.
    ///
    /// # Errors
    ///
    /// Returns a flow-control I/O error or worker join failure.
    pub async fn shutdown(mut self) -> Result<()> {
        let (response, receiver) = oneshot::channel();
        let command_result = if self
            .commands
            .send(RuntimeCommand::Shutdown { response })
            .await
            .is_ok()
        {
            receiver.await.map_err(|_| DcpError::Cancelled)?
        } else {
            Ok(())
        };
        if let Some(worker) = self.worker.take() {
            worker.await.map_err(|error| DcpError::Stream {
                vbucket: 0,
                message: format!("DCP worker task failed: {error}"),
            })?;
        }
        command_result
    }
}

impl Stream for DcpStream {
    type Item = Result<DcpStreamItem>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(context)
    }
}

impl Drop for DcpStream {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

/// Opens all requested vBucket streams, applies the configured explicit
/// rollback policy, and starts the Tokio connection worker.
///
/// Network buffer acknowledgement is handled inside the worker when frames
/// enter the bounded delivery queue. It does not mark an event processed and
/// does not advance a durable checkpoint.
///
/// # Errors
///
/// Returns a validation, stream-open, rollback, handler, or Tokio runtime
/// error. No worker is started unless every requested partition opens.
pub async fn open_dcp_stream(
    mut connection: DcpConnection,
    requests: Vec<VBucketStreamRequest>,
    flow_control: FlowControlConfig,
    rollback_policy: RollbackPolicy,
    rollback_handler: Option<Arc<dyn RollbackHandler>>,
) -> Result<DcpStream> {
    flow_control.validate()?;
    validate_requests(&requests)?;
    validate_capabilities(connection.capabilities(), &requests)?;
    if rollback_policy == RollbackPolicy::DelegateToHandler && rollback_handler.is_none() {
        return Err(DcpError::InvalidConfiguration(
            "DelegateToHandler rollback policy requires a rollback handler".into(),
        ));
    }

    let stream_end_on_close = connection
        .capabilities()
        .supports_control(DcpControlFeature::StreamEndOnClose);
    let mut report = StreamOpenReport::default();
    for mut request in requests {
        let (state, rollbacks) = open_partition(
            connection.connection_mut(),
            &mut request,
            rollback_policy,
            rollback_handler.as_deref(),
        )
        .await?;
        report.rollbacks.extend(rollbacks);
        report.partitions.insert(state.vbucket, state);
    }

    let queue_capacity = flow_control.event_queue_capacity.get();
    let (sender, receiver) = mpsc::channel(queue_capacity);
    let (commands, command_receiver) = mpsc::channel(16);
    let runtime_partitions = report
        .partitions
        .values()
        .map(|state| {
            (
                state.vbucket,
                PartitionProgress::new(
                    state.start_seqno,
                    state.end_seqno,
                    state.stream_id,
                    state.opaque,
                ),
            )
        })
        .collect();
    let worker = tokio::spawn(run_worker(
        connection.into_inner(),
        runtime_partitions,
        flow_control,
        stream_end_on_close,
        sender,
        command_receiver,
    ));

    Ok(DcpStream {
        receiver,
        commands,
        worker: Some(worker),
        open_report: report,
    })
}

fn validate_capabilities(
    capabilities: &crate::BootstrapCapabilities,
    requests: &[VBucketStreamRequest],
) -> Result<()> {
    for request in requests {
        if request.filter.is_some() && !capabilities.supports(HelloFeature::Collections) {
            return Err(DcpError::Unsupported(format!(
                "vBucket {} uses a stream filter but the server did not negotiate collections",
                request.checkpoint.vbucket
            )));
        }
        if request.stream_id().is_some()
            && !capabilities.supports_control(DcpControlFeature::StreamId)
        {
            return Err(DcpError::Unsupported(format!(
                "vBucket {} uses a stream ID but enable_stream_id was not accepted",
                request.checkpoint.vbucket
            )));
        }
    }
    Ok(())
}

fn validate_requests(requests: &[VBucketStreamRequest]) -> Result<()> {
    if requests.is_empty() {
        return Err(DcpError::InvalidConfiguration(
            "at least one vBucket stream request is required".into(),
        ));
    }
    let mut vbuckets = BTreeSet::new();
    for request in requests {
        request.checkpoint.validate()?;
        if request.checkpoint.seqno > request.end_seqno {
            return Err(DcpError::InvalidConfiguration(format!(
                "vBucket {} start {} exceeds stream end {}",
                request.checkpoint.vbucket, request.checkpoint.seqno, request.end_seqno
            )));
        }
        if !vbuckets.insert(request.checkpoint.vbucket) {
            return Err(DcpError::InvalidConfiguration(format!(
                "duplicate stream request for vBucket {}",
                request.checkpoint.vbucket
            )));
        }
    }
    Ok(())
}

async fn open_partition(
    connection: &mut KvConnection,
    request: &mut VBucketStreamRequest,
    rollback_policy: RollbackPolicy,
    rollback_handler: Option<&dyn RollbackHandler>,
) -> Result<(PartitionOpenState, Vec<RollbackApplied>)> {
    let mut rollbacks = Vec::new();
    let mut rollback_attempts = 0_usize;
    loop {
        let response = connection
            .request(stream_request(&request.wire_request())?)
            .await?;
        match parse_stream_request_response(&response)? {
            StreamRequestResponse::Opened(entries) => {
                let current = entries.first().ok_or_else(|| {
                    DcpError::Topology(format!(
                        "stream-open response has no failover entries for vBucket {}",
                        request.checkpoint.vbucket
                    ))
                })?;
                let mut effective_checkpoint = request.checkpoint.clone();
                effective_checkpoint.vbucket_uuid = current.vbucket_uuid;
                return Ok((
                    PartitionOpenState {
                        effective_checkpoint,
                        vbucket: request.checkpoint.vbucket,
                        start_seqno: request.checkpoint.seqno,
                        end_seqno: request.end_seqno,
                        vbucket_uuid: current.vbucket_uuid,
                        stream_id: request.stream_id(),
                        opaque: response.opaque,
                    },
                    rollbacks,
                ));
            }
            StreamRequestResponse::Rollback(rollback_seqno) => {
                rollback_attempts += 1;
                if rollback_attempts > MAX_ROLLBACK_ATTEMPTS {
                    return Err(DcpError::Stream {
                        vbucket: request.checkpoint.vbucket,
                        message: format!(
                            "producer exceeded {MAX_ROLLBACK_ATTEMPTS} consecutive rollback responses"
                        ),
                    });
                }
                if rollback_seqno >= request.checkpoint.seqno {
                    return Err(ProtocolError::MalformedDcp(format!(
                        "vBucket {} rollback {} does not precede requested seqno {}",
                        request.checkpoint.vbucket, rollback_seqno, request.checkpoint.seqno
                    ))
                    .into());
                }
                let failover_log =
                    fetch_failover_log(connection, request.checkpoint.vbucket).await?;
                let rollback_request = RollbackRequest {
                    vbucket: request.checkpoint.vbucket,
                    checkpoint: request.checkpoint.clone(),
                    rollback_seqno,
                    failover_log: failover_log.clone(),
                };
                let action = match rollback_policy {
                    RollbackPolicy::StopAndReport => RollbackAction::StopAndReport,
                    RollbackPolicy::RewindAndReplay => RollbackAction::RewindAndReplay,
                    RollbackPolicy::DelegateToHandler => {
                        rollback_handler
                            .expect("validated before opening")
                            .handle(rollback_request.clone())
                            .await?
                    }
                };
                if action == RollbackAction::StopAndReport {
                    return Err(DcpError::RollbackRequired {
                        vbucket: rollback_request.vbucket,
                        requested_seqno: rollback_request.checkpoint.seqno,
                        rollback_seqno,
                        failover_log,
                    });
                }
                let requested_seqno = request.checkpoint.seqno;
                let vbucket_uuid = request.rewind(rollback_seqno, &failover_log)?;
                rollbacks.push(RollbackApplied {
                    vbucket: request.checkpoint.vbucket,
                    requested_seqno,
                    rollback_seqno,
                    vbucket_uuid,
                });
            }
        }
    }
}

#[derive(Debug)]
enum RuntimeCommand {
    Close {
        vbucket: u16,
        response: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<()>>,
    },
}

#[derive(Clone, Copy, Debug)]
struct SnapshotWindow {
    start: u64,
    end: u64,
}

#[derive(Clone, Copy, Debug)]
struct PartitionProgress {
    last_seqno: u64,
    end_seqno: u64,
    stream_id: Option<u16>,
    opaque: u32,
    snapshot: Option<SnapshotWindow>,
}

impl PartitionProgress {
    const fn new(last_seqno: u64, end_seqno: u64, stream_id: Option<u16>, opaque: u32) -> Self {
        Self {
            last_seqno,
            end_seqno,
            stream_id,
            opaque,
            snapshot: None,
        }
    }

    fn observe(&mut self, message: &DcpMessage) -> Result<()> {
        if let DcpMessage::SnapshotMarker(marker) = message {
            if marker.end_seqno < self.last_seqno || marker.end_seqno > self.end_seqno {
                return Err(DcpError::Stream {
                    vbucket: marker.vbucket,
                    message: format!(
                        "snapshot {}..={} is outside delivered {}..={}",
                        marker.start_seqno, marker.end_seqno, self.last_seqno, self.end_seqno
                    ),
                });
            }
            self.snapshot = Some(SnapshotWindow {
                start: marker.start_seqno,
                end: marker.end_seqno,
            });
            return Ok(());
        }
        let Some(seqno) = message_seqno(message) else {
            return Ok(());
        };
        let snapshot = self.snapshot.ok_or_else(|| DcpError::Stream {
            vbucket: message_vbucket(message).unwrap_or_default(),
            message: format!("received seqno {seqno} before a snapshot marker"),
        })?;
        if seqno < snapshot.start || seqno > snapshot.end || seqno > self.end_seqno {
            return Err(DcpError::Stream {
                vbucket: message_vbucket(message).unwrap_or_default(),
                message: format!(
                    "seqno {seqno} is outside snapshot {}..={} or stream end {}",
                    snapshot.start, snapshot.end, self.end_seqno
                ),
            });
        }
        if seqno <= self.last_seqno {
            return Err(DcpError::Stream {
                vbucket: message_vbucket(message).unwrap_or_default(),
                message: format!("non-monotonic seqno {seqno} after {}", self.last_seqno),
            });
        }
        self.last_seqno = seqno;
        Ok(())
    }
}

struct FlowCredit {
    threshold: u64,
    pending: u64,
}

impl FlowCredit {
    fn new(config: &FlowControlConfig) -> Result<Self> {
        let buffer_size = u32::try_from(config.connection_buffer_size).map_err(|error| {
            DcpError::InvalidConfiguration(format!("invalid connection buffer size: {error}"))
        })?;
        let target = f64::from(buffer_size) * f64::from(config.ack_ratio);
        let mut low = 1_u32;
        let mut high = buffer_size;
        while low < high {
            let middle = low + (high - low) / 2;
            if f64::from(middle) < target {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(Self {
            threshold: u64::from(low),
            pending: 0,
        })
    }

    async fn record(&mut self, connection: &mut KvConnection, bytes: usize) -> Result<()> {
        self.pending = self
            .pending
            .checked_add(u64::try_from(bytes).map_err(|error| {
                DcpError::Protocol(ProtocolError::InvalidLength(format!(
                    "wire byte count does not fit u64: {error}"
                )))
            })?)
            .ok_or_else(|| {
                DcpError::Protocol(ProtocolError::InvalidLength(
                    "flow-control byte count overflow".into(),
                ))
            })?;
        if self.pending >= self.threshold {
            self.flush(connection).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, connection: &mut KvConnection) -> Result<()> {
        while self.pending != 0 {
            let amount = self.pending.min(u64::from(u32::MAX));
            connection
                .send_frame(buffer_ack(
                    u32::try_from(amount).expect("amount is bounded by u32::MAX"),
                    0,
                ))
                .await?;
            self.pending -= amount;
        }
        Ok(())
    }
}

async fn run_worker(
    mut connection: KvConnection,
    mut partitions: BTreeMap<u16, PartitionProgress>,
    flow_config: FlowControlConfig,
    stream_end_on_close: bool,
    output: mpsc::Sender<Result<DcpStreamItem>>,
    mut commands: mpsc::Receiver<RuntimeCommand>,
) {
    let mut credit = match FlowCredit::new(&flow_config) {
        Ok(credit) => credit,
        Err(error) => {
            let _ = output.send(Err(error)).await;
            return;
        }
    };
    loop {
        if partitions.is_empty() {
            if let Err(error) = credit.flush(&mut connection).await {
                let _ = output.send(Err(error)).await;
            }
            return;
        }
        let deadline = connection.last_inbound_activity() + flow_config.dead_connection_timeout;
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    let _ = credit.flush(&mut connection).await;
                    return;
                };
                if !handle_command(
                    command,
                    &mut connection,
                    &mut partitions,
                    &mut credit,
                    stream_end_on_close,
                    &output,
                ).await {
                    return;
                }
            }
            received = time::timeout_at(deadline, connection.receive_frame()) => {
                let frame = match received {
                    Ok(Ok(frame)) => frame,
                    Ok(Err(error)) => {
                        let _ = output.send(Err(error)).await;
                        return;
                    }
                    Err(_) => {
                        let _ = output.send(Err(DcpError::DeadConnection {
                            peer: connection.peer().to_owned(),
                            idle_for: flow_config.dead_connection_timeout,
                        })).await;
                        return;
                    }
                };
                if let Err(error) = handle_frame(
                    frame,
                    &mut connection,
                    &mut partitions,
                    &mut credit,
                    &output,
                ).await {
                    let _ = output.send(Err(error)).await;
                    return;
                }
            }
        }
    }
}

async fn handle_command(
    command: RuntimeCommand,
    connection: &mut KvConnection,
    partitions: &mut BTreeMap<u16, PartitionProgress>,
    credit: &mut FlowCredit,
    stream_end_on_close: bool,
    output: &mpsc::Sender<Result<DcpStreamItem>>,
) -> bool {
    match command {
        RuntimeCommand::Shutdown { response } => {
            let result = credit.flush(connection).await;
            let _ = response.send(result);
            false
        }
        RuntimeCommand::Close { vbucket, response } => {
            let Some(progress) = partitions.get(&vbucket).copied() else {
                let _ = response.send(Err(DcpError::Stream {
                    vbucket,
                    message: "cannot close a stream that is not active".into(),
                }));
                return true;
            };
            let result = async {
                let close_response = connection
                    .request(close_stream(vbucket, progress.stream_id, 0))
                    .await?;
                ensure_success_response(
                    &close_response,
                    Opcode::DCP_CLOSE_STREAM,
                    "close DCP stream",
                )?;
                Ok(())
            }
            .await;
            let succeeded = result.is_ok();
            let _ = response.send(result);
            if succeeded && !stream_end_on_close {
                partitions.remove(&vbucket);
                if output
                    .send(Ok(DcpStreamItem::Event(DcpEvent::StreamEnd(StreamEnd {
                        vbucket,
                        reason: StreamEndReason::Closed,
                    }))))
                    .await
                    .is_err()
                {
                    return false;
                }
            }
            true
        }
    }
}

async fn handle_frame(
    frame: Frame,
    connection: &mut KvConnection,
    partitions: &mut BTreeMap<u16, PartitionProgress>,
    credit: &mut FlowCredit,
    output: &mpsc::Sender<Result<DcpStreamItem>>,
) -> Result<()> {
    if frame.magic.is_response() {
        return Err(ProtocolError::MalformedDcp(format!(
            "unexpected response opcode 0x{:02x} in DCP event loop",
            frame.opcode.as_u8()
        ))
        .into());
    }
    if frame.opcode == Opcode::GET_CLUSTER_CONFIG {
        output
            .send(Ok(DcpStreamItem::TopologyConfig {
                source: connection.peer().to_owned(),
                payload: frame.value,
            }))
            .await
            .map_err(|_| DcpError::Cancelled)?;
        return Ok(());
    }

    let wire_size = frame.wire_size();
    let message = parse_dcp_message(&frame)?;
    if let DcpMessage::Noop { opaque } = message {
        connection.send_frame(noop_response(opaque)).await?;
        return Ok(());
    }
    if let DcpMessage::Unknown(unknown) = message {
        let is_flow_controlled = (0x50..=0x65).contains(&unknown.opcode.as_u8());
        output
            .send(Ok(DcpStreamItem::Unknown(unknown)))
            .await
            .map_err(|_| DcpError::Cancelled)?;
        if is_flow_controlled {
            credit.record(connection, wire_size).await?;
        }
        return Ok(());
    }

    let vbucket = message_vbucket(&message).ok_or_else(|| {
        DcpError::Protocol(ProtocolError::MalformedDcp(
            "known DCP event has no vBucket".into(),
        ))
    })?;
    let progress = partitions
        .get_mut(&vbucket)
        .ok_or_else(|| DcpError::Stream {
            vbucket,
            message: "received an event for a stream that is not active".into(),
        })?;
    if frame.opaque != progress.opaque {
        return Err(DcpError::Stream {
            vbucket,
            message: format!(
                "event opaque {} does not match active stream {}",
                frame.opaque, progress.opaque
            ),
        });
    }
    if message_stream_id(&message) != progress.stream_id {
        return Err(DcpError::Stream {
            vbucket,
            message: format!(
                "event stream ID {:?} does not match active stream {:?}",
                message_stream_id(&message),
                progress.stream_id
            ),
        });
    }
    progress.observe(&message)?;
    if matches!(
        &message,
        DcpMessage::SnapshotMarker(marker)
            if marker.flags & SnapshotFlags::ACK.bits() != 0
    ) {
        connection
            .send_frame(snapshot_marker_response(frame.opaque))
            .await?;
    }
    let is_stream_end = matches!(message, DcpMessage::StreamEnd(_));
    let event = convert_event(message)?;
    output
        .send(Ok(DcpStreamItem::Event(event)))
        .await
        .map_err(|_| DcpError::Cancelled)?;
    credit.record(connection, wire_size).await?;
    if is_stream_end {
        partitions.remove(&vbucket);
    }
    Ok(())
}

fn ensure_success_response(response: &Frame, opcode: Opcode, context: &str) -> Result<()> {
    if !response.magic.is_response() || response.opcode != opcode {
        return Err(ProtocolError::MalformedFrame(format!(
            "expected response opcode 0x{:02x}, got magic 0x{:02x} opcode 0x{:02x}",
            opcode.as_u8(),
            response.magic.as_u8(),
            response.opcode.as_u8()
        ))
        .into());
    }
    if response.status == Status::SUCCESS {
        return Ok(());
    }
    Err(DcpError::ServerStatus {
        status: response.status.as_u16(),
        opcode: response.opcode.as_u8(),
        message: if response.value.is_empty() {
            context.to_owned()
        } else {
            format!("{context}: {}", String::from_utf8_lossy(&response.value))
        },
    })
}

fn message_vbucket(message: &DcpMessage) -> Option<u16> {
    match message {
        DcpMessage::Mutation(event) => Some(event.vbucket),
        DcpMessage::Deletion(event) => Some(event.vbucket),
        DcpMessage::Expiration(event) => Some(event.vbucket),
        DcpMessage::SnapshotMarker(event) => Some(event.vbucket),
        DcpMessage::StreamEnd(event) => Some(event.vbucket),
        DcpMessage::SeqNoAdvanced(event) => Some(event.vbucket),
        DcpMessage::SystemEvent(event) => Some(event.vbucket),
        DcpMessage::OsoSnapshot(event) => Some(event.vbucket),
        _ => None,
    }
}

fn message_stream_id(message: &DcpMessage) -> Option<u16> {
    match message {
        DcpMessage::Mutation(event) => event.stream_id,
        DcpMessage::Deletion(event) => event.stream_id,
        DcpMessage::Expiration(event) => event.stream_id,
        DcpMessage::SnapshotMarker(event) => event.stream_id,
        DcpMessage::StreamEnd(event) => event.stream_id,
        DcpMessage::SeqNoAdvanced(event) => event.stream_id,
        DcpMessage::SystemEvent(event) => event.stream_id,
        DcpMessage::OsoSnapshot(event) => event.stream_id,
        _ => None,
    }
}

fn message_seqno(message: &DcpMessage) -> Option<u64> {
    match message {
        DcpMessage::Mutation(event) => Some(event.seqno),
        DcpMessage::Deletion(event) => Some(event.seqno),
        DcpMessage::Expiration(event) => Some(event.seqno),
        DcpMessage::SeqNoAdvanced(event) => Some(event.seqno),
        DcpMessage::SystemEvent(event) => Some(event.seqno),
        _ => None,
    }
}

fn convert_event(message: DcpMessage) -> Result<DcpEvent> {
    Ok(match message {
        DcpMessage::Mutation(event) => DcpEvent::Mutation(convert_mutation(event)),
        DcpMessage::Deletion(event) => DcpEvent::Deletion(convert_deletion(event)),
        DcpMessage::Expiration(event) => DcpEvent::Expiration(convert_expiration(event)),
        DcpMessage::SnapshotMarker(event) => {
            DcpEvent::SnapshotMarker(convert_snapshot_marker(&event))
        }
        DcpMessage::StreamEnd(event) => DcpEvent::StreamEnd(convert_stream_end(&event)),
        DcpMessage::SeqNoAdvanced(event) => DcpEvent::SeqNoAdvanced(SeqNoAdvanced {
            vbucket: event.vbucket,
            seqno: event.seqno,
        }),
        DcpMessage::SystemEvent(event) => DcpEvent::SystemEvent(convert_system_event(event)),
        DcpMessage::OsoSnapshot(event) => DcpEvent::OsoSnapshot(convert_oso_snapshot(&event)),
        DcpMessage::Noop { .. } | DcpMessage::Unknown(_) => {
            return Err(ProtocolError::MalformedDcp(
                "control or unknown message cannot be converted to DcpEvent".into(),
            )
            .into());
        }
        _ => {
            return Err(ProtocolError::MalformedDcp(
                "unsupported future DCP message cannot be converted to DcpEvent".into(),
            )
            .into());
        }
    })
}

fn convert_mutation(event: rust_dcp_protocol::DcpMutation) -> DcpMutation {
    DcpMutation {
        vbucket: event.vbucket,
        seqno: event.seqno,
        rev_seqno: event.rev_seqno,
        flags: event.flags,
        expiry: event.expiry,
        lock_time: event.lock_time,
        cas: event.cas,
        datatype: DataType::from_bits_retain(event.datatype),
        collection_id: event.collection_id,
        collection_name: None,
        key: event.key,
        value: event.value,
    }
}

fn convert_deletion(event: rust_dcp_protocol::DcpDeletion) -> DcpDeletion {
    DcpDeletion {
        vbucket: event.vbucket,
        seqno: event.seqno,
        rev_seqno: event.rev_seqno,
        delete_time: event.delete_time,
        cas: event.cas,
        collection_id: event.collection_id,
        collection_name: None,
        key: event.key,
        value: event.value,
        datatype: DataType::from_bits_retain(event.datatype),
    }
}

fn convert_expiration(event: rust_dcp_protocol::DcpExpiration) -> DcpExpiration {
    DcpExpiration {
        vbucket: event.vbucket,
        seqno: event.seqno,
        rev_seqno: event.rev_seqno,
        delete_time: event.delete_time,
        cas: event.cas,
        collection_id: event.collection_id,
        collection_name: None,
        key: event.key,
        value: event.value,
        datatype: DataType::from_bits_retain(event.datatype),
    }
}

fn convert_snapshot_marker(event: &rust_dcp_protocol::SnapshotMarker) -> SnapshotMarker {
    SnapshotMarker {
        vbucket: event.vbucket,
        start_seqno: event.start_seqno,
        end_seqno: event.end_seqno,
        flags: SnapshotFlags::from_bits_retain(event.flags),
        high_completed_seqno: event.high_completed_seqno,
        max_visible_seqno: event.max_visible_seqno,
        purge_seqno: event.purge_seqno,
    }
}

fn convert_stream_end(event: &rust_dcp_protocol::StreamEnd) -> StreamEnd {
    let reason = match event.reason {
        rust_dcp_protocol::StreamEndReason::Ok => StreamEndReason::Ok,
        rust_dcp_protocol::StreamEndReason::StateChanged => StreamEndReason::StateChanged,
        rust_dcp_protocol::StreamEndReason::Closed => StreamEndReason::Closed,
        rust_dcp_protocol::StreamEndReason::Disconnected => StreamEndReason::Disconnected,
        rust_dcp_protocol::StreamEndReason::TooSlow => StreamEndReason::TooSlow,
        rust_dcp_protocol::StreamEndReason::BackfillFailed => StreamEndReason::BackfillFailed,
        rust_dcp_protocol::StreamEndReason::FilterEmpty => StreamEndReason::FilterEmpty,
        rust_dcp_protocol::StreamEndReason::Unknown(reason) => StreamEndReason::Unknown(reason),
    };
    StreamEnd {
        vbucket: event.vbucket,
        reason,
    }
}

fn convert_system_event(event: rust_dcp_protocol::SystemEvent) -> SystemEvent {
    SystemEvent {
        vbucket: event.vbucket,
        seqno: event.seqno,
        manifest_uid: event.manifest_uid,
        version: event.version,
        key: event.key,
        kind: convert_system_event_kind(event.kind),
    }
}

fn convert_system_event_kind(kind: rust_dcp_protocol::SystemEventKind) -> SystemEventKind {
    match kind {
        rust_dcp_protocol::SystemEventKind::CollectionCreated {
            scope_id,
            collection_id,
            max_ttl,
        } => SystemEventKind::CollectionCreated {
            scope_id,
            collection_id,
            max_ttl,
        },
        rust_dcp_protocol::SystemEventKind::CollectionDropped {
            scope_id,
            collection_id,
        } => SystemEventKind::CollectionDropped {
            scope_id,
            collection_id,
        },
        rust_dcp_protocol::SystemEventKind::CollectionFlushed { collection_id } => {
            SystemEventKind::CollectionFlushed { collection_id }
        }
        rust_dcp_protocol::SystemEventKind::ScopeCreated { scope_id } => {
            SystemEventKind::ScopeCreated { scope_id }
        }
        rust_dcp_protocol::SystemEventKind::ScopeDropped { scope_id } => {
            SystemEventKind::ScopeDropped { scope_id }
        }
        rust_dcp_protocol::SystemEventKind::CollectionChanged {
            collection_id,
            max_ttl,
        } => SystemEventKind::CollectionChanged {
            collection_id,
            max_ttl: Some(max_ttl),
        },
        rust_dcp_protocol::SystemEventKind::Unknown { code, data } => {
            SystemEventKind::Unknown { code, data }
        }
    }
}

fn convert_oso_snapshot(event: &rust_dcp_protocol::OsoSnapshot) -> OsoSnapshot {
    let state = match event.state {
        rust_dcp_protocol::OsoSnapshotState::Begin => OsoSnapshotState::Begin,
        rust_dcp_protocol::OsoSnapshotState::End => OsoSnapshotState::End,
        rust_dcp_protocol::OsoSnapshotState::Unknown(state) => OsoSnapshotState::Unknown(state),
    };
    OsoSnapshot {
        vbucket: event.vbucket,
        state,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, num::NonZeroUsize, time::Duration};

    use bytes::{BufMut, BytesMut};
    use futures_util::{SinkExt, StreamExt};
    use rust_dcp_protocol::{
        DcpMessage, FrameCodec, HelloFeature, OsoSnapshotState as WireOsoState,
        SeqNoAdvanced as WireSeqNoAdvanced, SnapshotMarker as WireSnapshotMarker,
    };
    use tokio::io::duplex;
    use tokio_util::codec::Framed;

    use super::*;
    use crate::{BootstrapCapabilities, SaslMechanism};

    fn flow_config() -> FlowControlConfig {
        FlowControlConfig {
            connection_buffer_size: 128,
            ack_ratio: 0.1,
            event_queue_capacity: NonZeroUsize::new(8).unwrap(),
            noop_interval: Duration::from_secs(1),
            dead_connection_timeout: Duration::from_secs(2),
        }
    }

    fn test_connection<T>(io: T, stream_end_on_close: bool) -> DcpConnection
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let mut controls = BTreeSet::new();
        if stream_end_on_close {
            controls.insert(DcpControlFeature::StreamEndOnClose);
        }
        DcpConnection::from_test_parts(
            KvConnection::from_io(io, "unit-test:11210", Duration::from_secs(1)),
            BootstrapCapabilities {
                hello_features: BTreeSet::<HelloFeature>::new(),
                sasl_mechanism: SaslMechanism::Plain,
                dcp_controls: controls,
            },
        )
    }

    fn failover_log() -> Vec<FailoverEntry> {
        vec![FailoverEntry {
            vbucket_uuid: 0xaaaa,
            seqno: 0,
        }]
    }

    fn request(mode: DcpMode, start: &StartPosition, high_seqno: u64) -> VBucketStreamRequest {
        VBucketStreamRequest::resolve(
            7,
            Some("bucket-id"),
            start,
            mode,
            high_seqno,
            failover_log(),
            None,
        )
        .unwrap()
    }

    fn stream_open_response(request: &Frame, uuid: u64, seqno: u64) -> Frame {
        let mut response = Frame::success_response_to(request);
        let mut value = BytesMut::new();
        value.put_u64(uuid);
        value.put_u64(seqno);
        response.value = value.freeze();
        response
    }

    #[test]
    fn request_resolution_freezes_finite_end_and_keeps_purge_safety() {
        let earliest = request(DcpMode::Finite, &StartPosition::Earliest, 99);
        assert_eq!(earliest.checkpoint().seqno, 0);
        assert_eq!(earliest.checkpoint().vbucket_uuid, 0xaaaa);
        assert_eq!(earliest.end_seqno(), 99);
        assert_eq!(
            earliest.wire_request().flags.bits() & DcpStreamFlags::IGNORE_PURGED_TOMBSTONES.bits(),
            0
        );

        let latest = request(DcpMode::Infinite, &StartPosition::Latest, 99);
        assert_eq!(latest.checkpoint().seqno, 99);
        assert_eq!(latest.checkpoint().snapshot_start, 99);
        assert_eq!(latest.end_seqno(), u64::MAX);
    }

    #[test]
    fn collection_filter_uses_the_manifest_uid_observed_at_the_checkpoint() {
        let checkpoint = DcpCheckpoint {
            bucket_uuid: Some("bucket-id".into()),
            vbucket: 7,
            vbucket_uuid: 0xaaaa,
            seqno: 42,
            snapshot_start: 40,
            snapshot_end: 50,
            manifest_uid: Some(0x17),
        };
        let request = VBucketStreamRequest::resolve(
            7,
            Some("bucket-id"),
            &StartPosition::Checkpoint(checkpoint),
            DcpMode::Infinite,
            99,
            failover_log(),
            Some(StreamFilter {
                collection_ids: vec![8],
                manifest_uid: Some(0x2a),
                ..StreamFilter::default()
            }),
        )
        .unwrap();

        assert_eq!(
            request.wire_request().filter.unwrap().manifest_uid,
            Some(0x17)
        );
    }

    #[test]
    fn new_collection_stream_does_not_claim_an_unobserved_manifest_uid() {
        let request = VBucketStreamRequest::resolve(
            7,
            Some("bucket-id"),
            &StartPosition::Earliest,
            DcpMode::Infinite,
            99,
            failover_log(),
            Some(StreamFilter {
                collection_ids: vec![8],
                manifest_uid: Some(0x2a),
                ..StreamFilter::default()
            }),
        )
        .unwrap();

        assert_eq!(request.wire_request().filter.unwrap().manifest_uid, None);
    }

    #[test]
    fn one_stream_id_can_be_reused_across_distinct_vbuckets() {
        let request = |vbucket| {
            VBucketStreamRequest::resolve(
                vbucket,
                Some("bucket-id"),
                &StartPosition::Earliest,
                DcpMode::Infinite,
                99,
                failover_log(),
                Some(StreamFilter {
                    collection_ids: vec![8],
                    stream_id: Some(7),
                    ..StreamFilter::default()
                }),
            )
            .unwrap()
        };

        assert!(validate_requests(&[request(7), request(8)]).is_ok());
    }

    #[test]
    fn rollback_clears_a_manifest_uid_observed_after_the_rewind_point() {
        let checkpoint = DcpCheckpoint {
            bucket_uuid: Some("bucket-id".into()),
            vbucket: 7,
            vbucket_uuid: 0xaaaa,
            seqno: 42,
            snapshot_start: 40,
            snapshot_end: 50,
            manifest_uid: Some(0x17),
        };
        let mut request = VBucketStreamRequest::resolve(
            7,
            Some("bucket-id"),
            &StartPosition::Checkpoint(checkpoint),
            DcpMode::Infinite,
            99,
            failover_log(),
            Some(StreamFilter {
                collection_ids: vec![8],
                ..StreamFilter::default()
            }),
        )
        .unwrap();

        request.rewind(20, &failover_log()).unwrap();

        assert_eq!(request.checkpoint().manifest_uid, None);
        assert_eq!(request.wire_request().filter.unwrap().manifest_uid, None);
    }

    #[test]
    fn checkpoint_ahead_of_high_seqno_is_an_explicit_rollback() {
        let checkpoint = DcpCheckpoint {
            bucket_uuid: Some("bucket-id".into()),
            vbucket: 7,
            vbucket_uuid: 0xaaaa,
            seqno: 101,
            snapshot_start: 100,
            snapshot_end: 110,
            manifest_uid: None,
        };
        let result = VBucketStreamRequest::resolve(
            7,
            Some("bucket-id"),
            &StartPosition::Checkpoint(checkpoint),
            DcpMode::Finite,
            99,
            failover_log(),
            None,
        );

        assert!(matches!(
            result,
            Err(DcpError::RollbackRequired {
                requested_seqno: 101,
                rollback_seqno: 99,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn stream_filter_requires_negotiated_collections() {
        let (client_io, _server_io) = duplex(4_096);
        let connection = test_connection(client_io, true);
        let filtered = VBucketStreamRequest::resolve(
            7,
            Some("bucket-id"),
            &StartPosition::Earliest,
            DcpMode::Infinite,
            99,
            failover_log(),
            Some(StreamFilter {
                collection_ids: vec![8],
                ..StreamFilter::default()
            }),
        )
        .unwrap();

        let result = open_dcp_stream(
            connection,
            vec![filtered],
            flow_config(),
            RollbackPolicy::StopAndReport,
            None,
        )
        .await;

        assert!(matches!(result, Err(DcpError::Unsupported(_))));
    }

    #[tokio::test]
    async fn flow_credit_waits_until_the_bounded_output_accepts_a_frame() {
        let (client_io, server_io) = duplex(4_096);
        let connection =
            KvConnection::from_io(client_io, "unit-test:11210", Duration::from_secs(1));
        let mut credit = FlowCredit::new(&flow_config()).unwrap();
        let (output, mut receiver) = mpsc::channel(1);
        output
            .send(Ok(DcpStreamItem::Unknown(Frame::request(Opcode::NOOP))))
            .await
            .unwrap();
        let future_event = Frame::request(Opcode(0x63));
        let expected_credit = future_event.wire_size();
        let handler = tokio::spawn(async move {
            let mut connection = connection;
            let mut partitions = BTreeMap::new();
            handle_frame(
                future_event,
                &mut connection,
                &mut partitions,
                &mut credit,
                &output,
            )
            .await
        });
        let mut server = Framed::new(server_io, FrameCodec::default());

        assert!(
            time::timeout(Duration::from_millis(20), server.next())
                .await
                .is_err()
        );
        receiver.recv().await.unwrap().unwrap();
        handler.await.unwrap().unwrap();

        let ack = server.next().await.unwrap().unwrap();
        assert_eq!(ack.opcode, Opcode::DCP_BUFFER_ACK);
        assert_eq!(
            usize::try_from(u32::from_be_bytes(ack.extras[..4].try_into().unwrap())).unwrap(),
            expected_credit
        );
        assert!(matches!(
            receiver.recv().await.unwrap().unwrap(),
            DcpStreamItem::Unknown(frame) if frame.opcode == Opcode(0x63)
        ));
    }

    #[tokio::test]
    async fn worker_emits_events_but_handles_noop_snapshot_ack_and_credit_internally() {
        let (client_io, server_io) = duplex(32 * 1024);
        let connection = test_connection(client_io, true);
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(server_io, FrameCodec::default());
            let open = framed.next().await.unwrap().unwrap();
            assert_eq!(open.opcode, Opcode::DCP_STREAM_REQUEST);
            framed
                .send(stream_open_response(&open, 0xaaaa, 0))
                .await
                .unwrap();

            let mut snapshot = Frame::request(Opcode::DCP_SNAPSHOT_MARKER);
            snapshot.vbucket = 7;
            snapshot.opaque = open.opaque;
            let mut extras = BytesMut::new();
            extras.put_u64(1);
            extras.put_u64(1);
            extras.put_u32(SnapshotFlags::MEMORY.bits() | SnapshotFlags::ACK.bits());
            snapshot.extras = extras.freeze();

            let mut mutation = Frame::request(Opcode::DCP_MUTATION);
            mutation.vbucket = 7;
            mutation.opaque = open.opaque;
            let mut extras = BytesMut::new();
            extras.put_u64(1);
            extras.put_u64(1);
            extras.put_u32(0);
            extras.put_u32(0);
            extras.put_u32(0);
            mutation.extras = extras.freeze();
            mutation.key = Bytes::from_static(b"doc");
            mutation.value = Bytes::from_static(b"value");

            let mut noop = Frame::request(Opcode::DCP_NOOP);
            noop.opaque = 0x1234;

            let mut end = Frame::request(Opcode::DCP_STREAM_END);
            end.vbucket = 7;
            end.opaque = open.opaque;
            end.extras = Bytes::copy_from_slice(&0_u32.to_be_bytes());

            let expected_credit = snapshot.wire_size() + mutation.wire_size() + end.wire_size();
            framed.send(snapshot).await.unwrap();
            framed.send(mutation).await.unwrap();
            framed.send(noop).await.unwrap();
            framed.send(end).await.unwrap();

            let mut saw_noop = false;
            let mut saw_snapshot_ack = false;
            let mut credit = 0_usize;
            while let Some(frame) = framed.next().await.transpose().unwrap() {
                match frame.opcode {
                    Opcode::DCP_NOOP => {
                        assert!(frame.magic.is_response());
                        assert_eq!(frame.opaque, 0x1234);
                        saw_noop = true;
                    }
                    Opcode::DCP_SNAPSHOT_MARKER => {
                        assert!(frame.magic.is_response());
                        saw_snapshot_ack = true;
                    }
                    Opcode::DCP_BUFFER_ACK => {
                        credit += usize::try_from(u32::from_be_bytes(
                            frame.extras[..4].try_into().unwrap(),
                        ))
                        .unwrap();
                    }
                    opcode => panic!("unexpected client opcode {opcode:?}"),
                }
            }
            (saw_noop, saw_snapshot_ack, credit, expected_credit)
        });

        let mut stream = open_dcp_stream(
            connection,
            vec![request(DcpMode::Finite, &StartPosition::Earliest, 1)],
            flow_config(),
            RollbackPolicy::StopAndReport,
            None,
        )
        .await
        .unwrap();
        let mut events = Vec::new();
        while let Some(item) = stream.next().await {
            events.push(item.unwrap());
        }

        assert!(matches!(
            events.as_slice(),
            [
                DcpStreamItem::Event(DcpEvent::SnapshotMarker(_)),
                DcpStreamItem::Event(DcpEvent::Mutation(_)),
                DcpStreamItem::Event(DcpEvent::StreamEnd(_))
            ]
        ));
        let (saw_noop, saw_snapshot_ack, credited, expected) = server.await.unwrap();
        assert!(saw_noop);
        assert!(saw_snapshot_ack);
        assert_eq!(credited, expected);
    }

    #[tokio::test]
    async fn worker_rejects_an_event_from_a_stale_stream_opaque() {
        let (client_io, server_io) = duplex(8 * 1024);
        let connection = test_connection(client_io, true);
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(server_io, FrameCodec::default());
            let open = framed.next().await.unwrap().unwrap();
            framed
                .send(stream_open_response(&open, 0xaaaa, 0))
                .await
                .unwrap();

            let mut snapshot = Frame::request(Opcode::DCP_SNAPSHOT_MARKER);
            snapshot.vbucket = 7;
            snapshot.opaque = open.opaque + 1;
            let mut extras = BytesMut::new();
            extras.put_u64(1);
            extras.put_u64(1);
            extras.put_u32(SnapshotFlags::MEMORY.bits());
            snapshot.extras = extras.freeze();
            framed.send(snapshot).await.unwrap();
        });

        let mut stream = open_dcp_stream(
            connection,
            vec![request(DcpMode::Finite, &StartPosition::Earliest, 1)],
            flow_config(),
            RollbackPolicy::StopAndReport,
            None,
        )
        .await
        .unwrap();

        assert!(matches!(
            stream.next().await.unwrap(),
            Err(DcpError::Stream { message, .. }) if message.contains("opaque")
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stop_policy_returns_typed_rollback_with_fresh_failover_log() {
        let (client_io, server_io) = duplex(16 * 1024);
        let connection = test_connection(client_io, true);
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(server_io, FrameCodec::default());
            let open = framed.next().await.unwrap().unwrap();
            let mut rollback = Frame::response(Opcode::DCP_STREAM_REQUEST, Status::ROLLBACK);
            rollback.opaque = open.opaque;
            rollback.value = Bytes::copy_from_slice(&20_u64.to_be_bytes());
            framed.send(rollback).await.unwrap();

            let failover = framed.next().await.unwrap().unwrap();
            assert_eq!(failover.opcode, Opcode::DCP_GET_FAILOVER_LOG);
            let mut response = Frame::success_response_to(&failover);
            let mut value = BytesMut::new();
            value.put_u64(0xbbbb);
            value.put_u64(10);
            value.put_u64(0xaaaa);
            value.put_u64(0);
            response.value = value.freeze();
            framed.send(response).await.unwrap();
        });
        let checkpoint = DcpCheckpoint {
            bucket_uuid: Some("bucket-id".into()),
            vbucket: 7,
            vbucket_uuid: 0xaaaa,
            seqno: 50,
            snapshot_start: 40,
            snapshot_end: 60,
            manifest_uid: None,
        };
        let request = request(
            DcpMode::Infinite,
            &StartPosition::Checkpoint(checkpoint),
            100,
        );

        let result = open_dcp_stream(
            connection,
            vec![request],
            flow_config(),
            RollbackPolicy::StopAndReport,
            None,
        )
        .await;
        assert!(matches!(
            result,
            Err(DcpError::RollbackRequired {
                requested_seqno: 50,
                rollback_seqno: 20,
                ..
            })
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn explicit_rewind_reopens_on_covering_failover_branch_and_is_reported() {
        let (client_io, server_io) = duplex(16 * 1024);
        let connection = test_connection(client_io, true);
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(server_io, FrameCodec::default());
            let open = framed.next().await.unwrap().unwrap();
            let mut rollback = Frame::response(Opcode::DCP_STREAM_REQUEST, Status::ROLLBACK);
            rollback.opaque = open.opaque;
            rollback.value = Bytes::copy_from_slice(&20_u64.to_be_bytes());
            framed.send(rollback).await.unwrap();

            let failover = framed.next().await.unwrap().unwrap();
            let mut response = Frame::success_response_to(&failover);
            let mut value = BytesMut::new();
            value.put_u64(0xbbbb);
            value.put_u64(10);
            value.put_u64(0xaaaa);
            value.put_u64(0);
            response.value = value.freeze();
            framed.send(response).await.unwrap();

            let reopened = framed.next().await.unwrap().unwrap();
            assert_eq!(
                u64::from_be_bytes(reopened.extras[8..16].try_into().unwrap()),
                20
            );
            assert_eq!(
                u64::from_be_bytes(reopened.extras[24..32].try_into().unwrap()),
                0xbbbb
            );
            framed
                .send(stream_open_response(&reopened, 0xbbbb, 10))
                .await
                .unwrap();
            while framed.next().await.is_some() {}
        });
        let checkpoint = DcpCheckpoint {
            bucket_uuid: Some("bucket-id".into()),
            vbucket: 7,
            vbucket_uuid: 0xaaaa,
            seqno: 50,
            snapshot_start: 40,
            snapshot_end: 60,
            manifest_uid: None,
        };
        let request = request(
            DcpMode::Infinite,
            &StartPosition::Checkpoint(checkpoint),
            100,
        );

        let stream = open_dcp_stream(
            connection,
            vec![request],
            flow_config(),
            RollbackPolicy::RewindAndReplay,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            stream.open_report().rollbacks(),
            &[RollbackApplied {
                vbucket: 7,
                requested_seqno: 50,
                rollback_seqno: 20,
                vbucket_uuid: 0xbbbb,
            }]
        );
        let effective = stream.open_report().partitions()[&7].checkpoint();
        assert_eq!(effective.seqno, 20);
        assert_eq!(effective.snapshot_start, 20);
        assert_eq!(effective.snapshot_end, 20);
        assert_eq!(effective.vbucket_uuid, 0xbbbb);
        assert_eq!(effective.manifest_uid, None);
        stream.shutdown().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn close_synthesizes_stream_end_for_older_server() {
        let (client_io, server_io) = duplex(8 * 1024);
        let connection = test_connection(client_io, false);
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(server_io, FrameCodec::default());
            let open = framed.next().await.unwrap().unwrap();
            framed
                .send(stream_open_response(&open, 0xaaaa, 0))
                .await
                .unwrap();
            let close = framed.next().await.unwrap().unwrap();
            assert_eq!(close.opcode, Opcode::DCP_CLOSE_STREAM);
            framed
                .send(Frame::success_response_to(&close))
                .await
                .unwrap();
        });

        let mut stream = open_dcp_stream(
            connection,
            vec![request(DcpMode::Infinite, &StartPosition::Earliest, 1)],
            flow_config(),
            RollbackPolicy::StopAndReport,
            None,
        )
        .await
        .unwrap();
        stream.close_vbucket(7).await.unwrap();
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            DcpStreamItem::Event(DcpEvent::StreamEnd(StreamEnd {
                reason: StreamEndReason::Closed,
                ..
            }))
        ));
        assert!(stream.next().await.is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn dead_connection_timeout_is_reported_as_liveness_failure() {
        let (client_io, _server_io) = duplex(256);
        let connection =
            KvConnection::from_io(client_io, "silent.test:11210", Duration::from_secs(1));
        let mut config = flow_config();
        config.dead_connection_timeout = Duration::from_millis(20);
        let (output, mut receiver) = mpsc::channel(1);
        let (_commands, command_receiver) = mpsc::channel(1);
        let worker = tokio::spawn(run_worker(
            connection,
            BTreeMap::from([(7, PartitionProgress::new(0, u64::MAX, None, 0))]),
            config,
            true,
            output,
            command_receiver,
        ));

        assert!(matches!(
            receiver.recv().await.unwrap(),
            Err(DcpError::DeadConnection { .. })
        ));
        worker.await.unwrap();
    }

    #[test]
    fn partition_progress_rejects_non_monotonic_sequence_numbers() {
        let mut progress = PartitionProgress::new(0, 10, None, 0);
        progress
            .observe(&DcpMessage::SnapshotMarker(WireSnapshotMarker {
                vbucket: 7,
                stream_id: None,
                start_seqno: 1,
                end_seqno: 10,
                flags: SnapshotFlags::MEMORY.bits(),
                max_visible_seqno: None,
                high_completed_seqno: None,
                timestamp: None,
                purge_seqno: None,
            }))
            .unwrap();
        progress
            .observe(&DcpMessage::SeqNoAdvanced(WireSeqNoAdvanced {
                vbucket: 7,
                stream_id: None,
                seqno: 5,
            }))
            .unwrap();

        assert!(
            progress
                .observe(&DcpMessage::SeqNoAdvanced(WireSeqNoAdvanced {
                    vbucket: 7,
                    stream_id: None,
                    seqno: 4,
                }))
                .is_err()
        );

        let mut history_progress = PartitionProgress::new(0, 10, None, 0);
        history_progress
            .observe(&DcpMessage::SnapshotMarker(WireSnapshotMarker {
                vbucket: 7,
                stream_id: None,
                start_seqno: 1,
                end_seqno: 10,
                flags: SnapshotFlags::HISTORY.bits() | SnapshotFlags::MAY_CONTAIN_DUPLICATES.bits(),
                max_visible_seqno: None,
                high_completed_seqno: None,
                timestamp: None,
                purge_seqno: None,
            }))
            .unwrap();
        history_progress
            .observe(&DcpMessage::SeqNoAdvanced(WireSeqNoAdvanced {
                vbucket: 7,
                stream_id: None,
                seqno: 5,
            }))
            .unwrap();
        assert!(
            history_progress
                .observe(&DcpMessage::SeqNoAdvanced(WireSeqNoAdvanced {
                    vbucket: 7,
                    stream_id: None,
                    seqno: 5,
                }))
                .is_err()
        );
    }

    #[test]
    fn oso_event_conversion_remains_visible_without_enabling_oso() {
        let event = convert_event(DcpMessage::OsoSnapshot(rust_dcp_protocol::OsoSnapshot {
            vbucket: 7,
            stream_id: None,
            state: WireOsoState::Begin,
        }))
        .unwrap();
        assert!(matches!(
            event,
            DcpEvent::OsoSnapshot(OsoSnapshot {
                state: OsoSnapshotState::Begin,
                ..
            })
        ));
    }
}
