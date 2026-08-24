use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{Stream, StreamExt, stream::SelectAll};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, watch},
    task::JoinHandle,
    time,
};

use crate::rollback_mitigation::{RollbackMitigator, mitigation_position, spawn_tokio_mitigator};
use crate::{
    AckOutcome, AssignmentMode, CheckpointCoordinator, CheckpointFlushReport, CheckpointMetrics,
    CheckpointStore, ClusterTopology, CollectionRegistry, CollectionRegistryStatus,
    CollectionSelection, DcpCheckpoint, DcpConfig, DcpError, DcpEvent, DcpHealth, DcpMetrics,
    DcpMode, DcpStream, DcpStreamItem, FailoverEntry, PartitionCheckpointStatus, Result,
    RollbackAction, RollbackHandler, RollbackPolicy, RollbackRequest, StartPosition, StreamFilter,
    TopologyState, TrackedEvent, VBucketAssignment, VBucketStreamRequest, bootstrap_connection,
    discover_topology, fetch_failover_log, fetch_selection_high_seqnos, load_checkpoints,
    open_dcp_stream, resolve_collection_selection,
};

const CLIENT_NAME: &str = concat!("rust-dcp/", env!("CARGO_PKG_VERSION"));
const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_millis(100);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

type ClientFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>;

trait ManagedNodeStream: Stream<Item = Result<DcpStreamItem>> + Send + Unpin {
    fn shutdown(self: Box<Self>) -> ClientFuture<()>;
}

impl ManagedNodeStream for DcpStream {
    fn shutdown(self: Box<Self>) -> ClientFuture<()> {
        Box::pin(async move { (*self).shutdown().await })
    }
}

struct OpenGenerationRequest {
    config: Arc<DcpConfig>,
    topology: ClusterTopology,
    starts: BTreeMap<u16, StartPosition>,
    frozen_end_seqnos: Option<BTreeMap<u16, u64>>,
    stream_id: Option<u16>,
    rollback_handler: Option<Arc<dyn RollbackHandler>>,
}

struct OpenedGeneration {
    streams: Vec<Box<dyn ManagedNodeStream>>,
    effective_checkpoints: BTreeMap<u16, DcpCheckpoint>,
    end_seqnos: BTreeMap<u16, u64>,
    registry: CollectionRegistry,
    rollback_count: usize,
    mitigation: Option<RollbackMitigator>,
}

trait ClientBackend: Send + Sync {
    fn discover(&self, config: Arc<DcpConfig>) -> ClientFuture<ClusterTopology>;
    fn open_generation(&self, request: OpenGenerationRequest) -> ClientFuture<OpenedGeneration>;
}

#[derive(Debug, Default)]
struct TokioClientBackend;

impl ClientBackend for TokioClientBackend {
    fn discover(&self, config: Arc<DcpConfig>) -> ClientFuture<ClusterTopology> {
        Box::pin(async move {
            let mut connection =
                bootstrap_connection(&config, CLIENT_NAME, &next_connection_name("metadata"))
                    .await?;
            discover_topology(
                connection.connection_mut(),
                &config.bucket,
                config.tls.enabled,
                &config.network,
            )
            .await
        })
    }

    fn open_generation(&self, request: OpenGenerationRequest) -> ClientFuture<OpenedGeneration> {
        Box::pin(open_tokio_generation(request))
    }
}

fn next_connection_name(role: &str) -> String {
    let id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    format!("rust-dcp-{role}-{id}")
}

#[tracing::instrument(
    name = "rust_dcp.open_generation",
    skip(request),
    fields(bucket = %request.config.bucket, vbuckets = request.starts.len()),
    err
)]
async fn open_tokio_generation(request: OpenGenerationRequest) -> Result<OpenedGeneration> {
    if request.starts.is_empty() {
        return Err(DcpError::InvalidConfiguration(
            "a DCP subscription must own at least one vBucket".into(),
        ));
    }
    let grouped = request
        .topology
        .active_vbuckets_by_node(request.starts.keys().copied())?;
    let mut streams: Vec<Box<dyn ManagedNodeStream>> = Vec::with_capacity(grouped.len());
    let mut effective_checkpoints = BTreeMap::new();
    let mut end_seqnos = BTreeMap::new();
    let mut rollback_count = 0_usize;
    let mut selection = None;

    for (node, vbuckets) in grouped {
        let opened = open_tokio_node(&request, &node, &vbuckets, selection.as_ref()).await;
        let (stream, node_selection, preflight_rollbacks) = match opened {
            Ok(opened) => opened,
            Err(error) => {
                shutdown_stream_vec(streams).await;
                return Err(error);
            }
        };
        if selection.is_none() {
            selection = Some(node_selection);
        }
        let Some(node_rollbacks) =
            preflight_rollbacks.checked_add(stream.open_report().rollbacks().len())
        else {
            streams.push(Box::new(stream));
            shutdown_stream_vec(streams).await;
            return Err(DcpError::Stream {
                vbucket: 0,
                message: "rollback counter overflow".into(),
            });
        };
        let Some(next_rollback_count) = rollback_count.checked_add(node_rollbacks) else {
            streams.push(Box::new(stream));
            shutdown_stream_vec(streams).await;
            return Err(DcpError::Stream {
                vbucket: 0,
                message: "rollback counter overflow".into(),
            });
        };
        rollback_count = next_rollback_count;
        let mut duplicate = None;
        for (&vbucket, state) in stream.open_report().partitions() {
            if effective_checkpoints
                .insert(vbucket, state.checkpoint().clone())
                .is_some()
                || end_seqnos.insert(vbucket, state.end_seqno).is_some()
            {
                duplicate = Some(vbucket);
                break;
            }
        }
        streams.push(Box::new(stream));
        if let Some(vbucket) = duplicate {
            shutdown_stream_vec(streams).await;
            return Err(DcpError::Topology(format!(
                "vBucket {vbucket} was opened on more than one node"
            )));
        }
    }

    let selection = selection.ok_or_else(|| {
        DcpError::Topology("the active topology produced no node-level DCP streams".into())
    })?;
    if let Err(error) = validate_opened_partitions(
        &request.starts,
        &effective_checkpoints,
        &end_seqnos,
        request.frozen_end_seqnos.as_ref(),
        streams.len(),
    ) {
        shutdown_stream_vec(streams).await;
        return Err(error);
    }
    let mitigation =
        match spawn_tokio_mitigator(&request.config, &request.topology, &effective_checkpoints) {
            Ok(mitigation) => mitigation,
            Err(error) => {
                shutdown_stream_vec(streams).await;
                return Err(error);
            }
        };
    Ok(OpenedGeneration {
        streams,
        effective_checkpoints,
        end_seqnos,
        registry: CollectionRegistry::new(selection),
        rollback_count,
        mitigation,
    })
}

#[tracing::instrument(
    name = "rust_dcp.open_node",
    skip(request, vbuckets, existing_selection),
    fields(node = %node, vbuckets = vbuckets.len()),
    err
)]
async fn open_tokio_node(
    request: &OpenGenerationRequest,
    node: &crate::NodeId,
    vbuckets: &[u16],
    existing_selection: Option<&CollectionSelection>,
) -> Result<(DcpStream, CollectionSelection, usize)> {
    let endpoint = request
        .topology
        .endpoints()
        .get(node)
        .ok_or_else(|| DcpError::Topology(format!("node {node} has no KV endpoint")))?;
    let mut node_config = (*request.config).clone();
    node_config.seeds = vec![endpoint.address().parse()?];
    let mut connection = bootstrap_connection(
        &node_config,
        CLIENT_NAME,
        &next_connection_name(node.as_str()),
    )
    .await?;
    let capabilities = connection.capabilities().clone();
    let selection = match existing_selection {
        Some(selection) => selection.clone(),
        None => {
            resolve_collection_selection(
                connection.connection_mut(),
                &capabilities,
                &request.config.collections,
                request.stream_id,
            )
            .await?
        }
    };
    let high_seqnos = fetch_selection_high_seqnos(connection.connection_mut(), &selection).await?;
    let bucket_uuid = bucket_identity(&request.topology);
    let mut stream_requests = Vec::with_capacity(vbuckets.len());
    let mut preflight_rollbacks = 0_usize;
    for &vbucket in vbuckets {
        let high_seqno = high_seqnos.get(&vbucket).copied().ok_or_else(|| {
            DcpError::Topology(format!(
                "node {node} omitted active high seqno for assigned vBucket {vbucket}"
            ))
        })?;
        let failover_log = fetch_failover_log(connection.connection_mut(), vbucket).await?;
        let start = request.starts.get(&vbucket).ok_or_else(|| {
            DcpError::Topology(format!("missing start position for vBucket {vbucket}"))
        })?;
        let frozen_end_seqno = match &request.frozen_end_seqnos {
            Some(end_seqnos) => Some(*end_seqnos.get(&vbucket).ok_or_else(|| {
                DcpError::Topology(format!("missing frozen end seqno for vBucket {vbucket}"))
            })?),
            None => None,
        };
        let (stream_request, rollbacks) = resolve_vbucket_request(
            vbucket,
            bucket_uuid,
            start,
            request.config.mode,
            high_seqno,
            failover_log,
            selection.stream_filter().cloned(),
            request.config.rollback_policy,
            request.rollback_handler.as_deref(),
            frozen_end_seqno,
        )
        .await?;
        preflight_rollbacks =
            preflight_rollbacks
                .checked_add(rollbacks)
                .ok_or_else(|| DcpError::Stream {
                    vbucket,
                    message: "preflight rollback counter overflow".into(),
                })?;
        stream_requests.push(stream_request);
    }
    let stream = open_dcp_stream(
        connection,
        stream_requests,
        request.config.flow_control.clone(),
        request.config.rollback_policy,
        request.rollback_handler.clone(),
    )
    .await?;
    Ok((stream, selection, preflight_rollbacks))
}

#[allow(clippy::too_many_arguments)]
async fn resolve_vbucket_request(
    vbucket: u16,
    bucket_uuid: &str,
    start: &StartPosition,
    mode: DcpMode,
    high_seqno: u64,
    failover_log: Vec<FailoverEntry>,
    filter: Option<StreamFilter>,
    rollback_policy: RollbackPolicy,
    rollback_handler: Option<&dyn RollbackHandler>,
    frozen_end_seqno: Option<u64>,
) -> Result<(VBucketStreamRequest, usize)> {
    let initial = VBucketStreamRequest::resolve(
        vbucket,
        Some(bucket_uuid),
        start,
        mode,
        high_seqno,
        failover_log.clone(),
        filter.clone(),
    );
    let (requested_seqno, rollback_seqno, observed_log) = match initial {
        Ok(request) => {
            return Ok((apply_frozen_end_seqno(request, frozen_end_seqno)?, 0));
        }
        Err(DcpError::RollbackRequired {
            requested_seqno,
            rollback_seqno,
            failover_log,
            ..
        }) => (requested_seqno, rollback_seqno, failover_log),
        Err(error) => return Err(error),
    };
    let StartPosition::Checkpoint(checkpoint) = start else {
        return Err(DcpError::RollbackRequired {
            vbucket,
            requested_seqno,
            rollback_seqno,
            failover_log: observed_log,
        });
    };
    let mut rejected = checkpoint.clone();
    if rejected.bucket_uuid.is_none() {
        rejected.bucket_uuid = Some(bucket_uuid.to_owned());
    }
    if rejected.vbucket_uuid == 0 {
        rejected.vbucket_uuid = observed_log.first().map_or(0, |entry| entry.vbucket_uuid);
    }
    let request = RollbackRequest {
        vbucket,
        checkpoint: rejected.clone(),
        rollback_seqno,
        failover_log: observed_log.clone(),
    };
    let action = match rollback_policy {
        RollbackPolicy::StopAndReport => RollbackAction::StopAndReport,
        RollbackPolicy::RewindAndReplay => RollbackAction::RewindAndReplay,
        RollbackPolicy::DelegateToHandler => {
            rollback_handler
                .ok_or_else(|| {
                    DcpError::InvalidConfiguration(
                        "DelegateToHandler rollback policy requires a rollback handler".into(),
                    )
                })?
                .handle(request)
                .await?
        }
    };
    if action == RollbackAction::StopAndReport {
        return Err(DcpError::RollbackRequired {
            vbucket,
            requested_seqno,
            rollback_seqno,
            failover_log: observed_log,
        });
    }
    let branch = observed_log
        .iter()
        .find(|entry| entry.seqno <= rollback_seqno)
        .ok_or_else(|| {
            DcpError::Topology(format!(
                "failover log has no branch covering rollback seqno {rollback_seqno} for vBucket {vbucket}"
            ))
        })?;
    rejected.vbucket_uuid = branch.vbucket_uuid;
    rejected.seqno = rollback_seqno;
    rejected.snapshot_start = rollback_seqno;
    rejected.snapshot_end = rollback_seqno;
    rejected.manifest_uid = None;
    rejected.validate()?;
    let request = VBucketStreamRequest::resolve(
        vbucket,
        Some(bucket_uuid),
        &StartPosition::Checkpoint(rejected),
        mode,
        high_seqno,
        observed_log,
        filter,
    )?;
    Ok((apply_frozen_end_seqno(request, frozen_end_seqno)?, 1))
}

fn apply_frozen_end_seqno(
    request: VBucketStreamRequest,
    frozen_end_seqno: Option<u64>,
) -> Result<VBucketStreamRequest> {
    match frozen_end_seqno {
        Some(end_seqno) => request.with_frozen_end_seqno(end_seqno),
        None => Ok(request),
    }
}

fn bucket_identity(topology: &ClusterTopology) -> &str {
    topology.bucket_uuid().unwrap_or_else(|| topology.bucket())
}

fn validate_opened_partitions(
    starts: &BTreeMap<u16, StartPosition>,
    effective: &BTreeMap<u16, DcpCheckpoint>,
    end_seqnos: &BTreeMap<u16, u64>,
    frozen_end_seqnos: Option<&BTreeMap<u16, u64>>,
    stream_count: usize,
) -> Result<()> {
    let requested = starts.keys().copied().collect::<BTreeSet<_>>();
    let opened = effective.keys().copied().collect::<BTreeSet<_>>();
    if requested != opened {
        return Err(DcpError::Topology(format!(
            "opened vBuckets {opened:?} do not match requested vBuckets {requested:?}"
        )));
    }
    let ends = end_seqnos.keys().copied().collect::<BTreeSet<_>>();
    if requested != ends {
        return Err(DcpError::Topology(format!(
            "opened end-seqno vBuckets {ends:?} do not match requested vBuckets {requested:?}"
        )));
    }
    if let Some(frozen) = frozen_end_seqnos
        && frozen != end_seqnos
    {
        return Err(DcpError::Stream {
            vbucket: 0,
            message: format!(
                "reopened finite endpoints {end_seqnos:?} do not match frozen endpoints {frozen:?}"
            ),
        });
    }
    for (&vbucket, checkpoint) in effective {
        if end_seqnos[&vbucket] < checkpoint.seqno {
            return Err(DcpError::Stream {
                vbucket,
                message: format!(
                    "stream end {} is behind effective start {}",
                    end_seqnos[&vbucket], checkpoint.seqno
                ),
            });
        }
    }
    if stream_count == 0 {
        return Err(DcpError::Topology(
            "a DCP generation opened no node connections".into(),
        ));
    }
    Ok(())
}

async fn shutdown_stream_vec(streams: Vec<Box<dyn ManagedNodeStream>>) {
    for stream in streams {
        if let Err(error) = stream.shutdown().await {
            tracing::warn!(%error, "failed to shut down a partially opened DCP stream");
        }
    }
}

async fn close_mitigation(mitigation: Option<RollbackMitigator>) -> Result<()> {
    match mitigation {
        Some(mitigation) => mitigation.close().await,
        None => Ok(()),
    }
}

/// Subscription ownership, persistence, and optional stream-ID settings.
pub struct DcpSubscriptionSpec {
    assignment: AssignmentMode,
    checkpoint_store: Arc<dyn CheckpointStore>,
    stream_id: Option<u16>,
    rollback_handler: Option<Arc<dyn RollbackHandler>>,
}

impl fmt::Debug for DcpSubscriptionSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DcpSubscriptionSpec")
            .field("assignment", &self.assignment)
            .field("stream_id", &self.stream_id)
            .field("has_rollback_handler", &self.rollback_handler.is_some())
            .finish_non_exhaustive()
    }
}

impl DcpSubscriptionSpec {
    /// Creates a subscription that owns every vBucket in the current topology.
    #[must_use]
    pub fn standalone(checkpoint_store: Arc<dyn CheckpointStore>) -> Self {
        Self {
            assignment: AssignmentMode::Standalone,
            checkpoint_store,
            stream_id: None,
            rollback_handler: None,
        }
    }

    /// Creates a subscription fenced by an externally managed assignment.
    #[must_use]
    pub fn external(
        checkpoint_store: Arc<dyn CheckpointStore>,
        assignment: VBucketAssignment,
    ) -> Self {
        Self {
            assignment: AssignmentMode::External(assignment),
            checkpoint_store,
            stream_id: None,
            rollback_handler: None,
        }
    }

    /// Adds an optional DCP stream identifier negotiated with the server.
    #[must_use]
    pub const fn stream_id(mut self, stream_id: Option<u16>) -> Self {
        self.stream_id = stream_id;
        self
    }

    /// Adds the application callback required by delegated rollback policy.
    #[must_use]
    pub fn rollback_handler(mut self, handler: Arc<dyn RollbackHandler>) -> Self {
        self.rollback_handler = Some(handler);
        self
    }

    /// Current assignment mode.
    #[must_use]
    pub const fn assignment(&self) -> &AssignmentMode {
        &self.assignment
    }

    /// Configured optional DCP stream identifier.
    #[must_use]
    pub const fn stream_id_value(&self) -> Option<u16> {
        self.stream_id
    }
}

/// One typed DCP event with explicit application-processing ownership.
pub struct DcpDelivery {
    tracked: TrackedEvent,
    metrics: DcpMetrics,
    connection_generation: u64,
    assignment_generation: u64,
}

impl fmt::Debug for DcpDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DcpDelivery")
            .field("tracked", &self.tracked)
            .field("connection_generation", &self.connection_generation)
            .field("assignment_generation", &self.assignment_generation)
            .finish_non_exhaustive()
    }
}

impl DcpDelivery {
    /// Delivered event.
    #[must_use]
    pub const fn event(&self) -> &DcpEvent {
        self.tracked.event()
    }

    /// Local connection generation that produced this delivery.
    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    /// External assignment fence, or zero for standalone ownership.
    #[must_use]
    pub const fn assignment_generation(&self) -> u64 {
        self.assignment_generation
    }

    /// Marks this delivery processed without sending network credit or forcing
    /// durable checkpoint I/O.
    ///
    /// # Errors
    ///
    /// Returns an obsolete-ACK or checkpoint-state error.
    #[tracing::instrument(
        name = "rust_dcp.mark_processed",
        skip(self),
        fields(
            vbucket = self.event().vbucket(),
            connection_generation = self.connection_generation,
            assignment_generation = self.assignment_generation
        ),
        err
    )]
    pub async fn mark_processed(self) -> Result<Option<AckOutcome>> {
        let outcome = self.tracked.acknowledge()?;
        self.metrics.record_processed();
        Ok(outcome)
    }
}

/// Tokio-driven asynchronous DCP subscription.
pub struct DcpSubscription {
    receiver: mpsc::Receiver<ManagedOutput>,
    current_generation: Arc<AtomicU64>,
    coordinator: CheckpointCoordinator,
    registry: Arc<Mutex<CollectionRegistry>>,
    metrics: DcpMetrics,
    health: DcpHealth,
    cancel: watch::Sender<bool>,
    control: Arc<SubscriptionControl>,
}

impl fmt::Debug for DcpSubscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DcpSubscription")
            .finish_non_exhaustive()
    }
}

impl Stream for DcpSubscription {
    type Item = Result<DcpDelivery>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.receiver.poll_recv(context) {
                Poll::Ready(Some(output))
                    if output.generation != self.current_generation.load(Ordering::Acquire) =>
                {
                    self.metrics.record_stale_generation_drop();
                }
                Poll::Ready(Some(output)) => {
                    if let Ok(delivery) = &output.item {
                        self.metrics.record_delivery(delivery.event());
                        tracing::trace!(
                            vbucket = delivery.event().vbucket(),
                            connection_generation = delivery.connection_generation(),
                            assignment_generation = delivery.assignment_generation(),
                            "yielding a DCP delivery to the application"
                        );
                    }
                    return Poll::Ready(Some(output.item));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl DcpSubscription {
    /// Flushes contiguous processed checkpoints without changing DCP network credit.
    ///
    /// # Errors
    ///
    /// Returns a bounded checkpoint-store or coordinator error.
    #[tracing::instrument(name = "rust_dcp.checkpoint_flush", skip(self), err)]
    pub async fn flush(&self) -> Result<CheckpointFlushReport> {
        self.coordinator.flush().await
    }

    /// Per-vBucket processed and durable positions.
    ///
    /// # Errors
    ///
    /// Returns a poisoned coordinator-state error.
    pub fn checkpoint_statuses(&self) -> Result<BTreeMap<u16, PartitionCheckpointStatus>> {
        self.coordinator.statuses()
    }

    /// Checkpoint flush counters and the most recent automatic-flush failure.
    ///
    /// # Errors
    ///
    /// Returns a poisoned coordinator-state error.
    pub fn checkpoint_metrics(&self) -> Result<CheckpointMetrics> {
        self.coordinator.metrics()
    }

    /// Current collection-manifest mapping freshness.
    ///
    /// # Errors
    ///
    /// Returns a poisoned registry or collection-state error.
    pub fn collection_status(&self) -> Result<CollectionRegistryStatus> {
        lock_registry(&self.registry)?.status()
    }

    /// Cloneable client-level metrics handle.
    #[must_use]
    pub fn metrics(&self) -> DcpMetrics {
        self.metrics.clone()
    }

    /// Cloneable client health handle.
    #[must_use]
    pub fn health(&self) -> DcpHealth {
        self.health.clone()
    }

    /// Stops all node streams and performs the final bounded checkpoint flush.
    ///
    /// # Errors
    ///
    /// Returns a stream-shutdown, task-join, or checkpoint-flush error.
    #[tracing::instrument(name = "rust_dcp.subscription_close", skip(self), err)]
    pub async fn close(&self) -> Result<()> {
        let _ = self.cancel.send(true);
        self.control.wait().await
    }
}

impl Drop for DcpSubscription {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

/// Cloneable owner of cluster topology, health checks, and one DCP subscription.
#[derive(Clone)]
pub struct DcpClient {
    inner: Arc<ClientInner>,
}

impl fmt::Debug for DcpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DcpClient")
            .field("bucket", &self.inner.config.bucket)
            .field("closed", &self.inner.closed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl DcpClient {
    /// Connects and discovers the initial Couchbase bucket topology.
    ///
    /// # Errors
    ///
    /// Returns configuration, authentication, transport, or topology errors.
    #[tracing::instrument(
        name = "rust_dcp.connect",
        skip(config),
        fields(bucket = %config.bucket),
        err
    )]
    pub async fn connect(config: DcpConfig) -> Result<Self> {
        Self::connect_with_backend(config, Arc::new(TokioClientBackend)).await
    }

    async fn connect_with_backend(
        config: DcpConfig,
        backend: Arc<dyn ClientBackend>,
    ) -> Result<Self> {
        config.validate()?;
        let config = Arc::new(config);
        let metrics = DcpMetrics::default();
        let health = DcpHealth::default();
        metrics.record_bootstrap_attempt();
        let topology = match backend.discover(Arc::clone(&config)).await {
            Ok(topology) => {
                metrics.record_bootstrap_success();
                topology
            }
            Err(error) => {
                metrics.record_bootstrap_failure();
                health.record_failure(SystemTime::now(), error.to_string());
                return Err(error);
            }
        };
        let topology_state = Arc::new(Mutex::new(TopologyState::new(topology.clone())));
        let snapshot = TopologySnapshot {
            generation: 1,
            topology,
        };
        let (topology_sender, _) = watch::channel(snapshot);
        let (cancel, cancel_receiver) = watch::channel(false);
        health.record_success(SystemTime::now(), 0, 1);
        let inner = Arc::new(ClientInner {
            config: Arc::clone(&config),
            backend: Arc::clone(&backend),
            topology_state: Arc::clone(&topology_state),
            topology_sender: topology_sender.clone(),
            metrics: metrics.clone(),
            health: health.clone(),
            cancel,
            lifecycle: AsyncMutex::new(()),
            health_task: AsyncMutex::new(None),
            subscription: AsyncMutex::new(None),
            active_subscription: Arc::new(AtomicBool::new(false)),
            closed: AtomicBool::new(false),
        });
        if config.health_check.enabled {
            let task = tokio::spawn(run_health_checks(HealthRuntime {
                config,
                backend,
                topology_state,
                topology_sender,
                metrics,
                health,
                cancel: cancel_receiver,
            }));
            *inner.health_task.lock().await = Some(task);
        }
        Ok(Self { inner })
    }

    /// Starts the client's single active subscription.
    ///
    /// # Errors
    ///
    /// Returns assignment, checkpoint-load, collection, stream-open, rollback,
    /// or lifecycle errors. A client deliberately owns at most one active
    /// subscription so connection gauges and shutdown have one authority.
    #[tracing::instrument(
        name = "rust_dcp.subscribe",
        skip(self, spec),
        fields(
            bucket = %self.inner.config.bucket,
            assignment = ?spec.assignment,
            stream_id = ?spec.stream_id
        ),
        err
    )]
    pub async fn subscribe(&self, spec: DcpSubscriptionSpec) -> Result<DcpSubscription> {
        self.validate_subscription_spec(&spec)?;
        let mut reservation =
            ActiveReservation::acquire(Arc::clone(&self.inner.active_subscription))?;
        let prepared = self.prepare_subscription(&spec).await?;
        let _lifecycle = self.inner.lifecycle.lock().await;
        if self.inner.closed.load(Ordering::Acquire) {
            prepared.discard().await;
            return Err(DcpError::Cancelled);
        }
        let subscription = self.activate_subscription(spec, prepared).await;
        reservation.commit();
        Ok(subscription)
    }

    fn validate_subscription_spec(&self, spec: &DcpSubscriptionSpec) -> Result<()> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(DcpError::Cancelled);
        }
        if spec.stream_id == Some(0) {
            return Err(DcpError::InvalidConfiguration(
                "DCP stream ID must be non-zero".into(),
            ));
        }
        if self.inner.config.rollback_policy == RollbackPolicy::DelegateToHandler
            && spec.rollback_handler.is_none()
        {
            return Err(DcpError::InvalidConfiguration(
                "DelegateToHandler rollback policy requires a rollback handler".into(),
            ));
        }
        Ok(())
    }

    async fn prepare_subscription(
        &self,
        spec: &DcpSubscriptionSpec,
    ) -> Result<PreparedSubscription> {
        let topology_snapshot = self.inner.topology_sender.borrow().clone();
        let (vbuckets, assignment_generation) =
            resolve_assignment(&spec.assignment, &topology_snapshot.topology)?;
        let bucket_uuid = bucket_identity(&topology_snapshot.topology).to_owned();
        let loaded = load_checkpoints(
            spec.checkpoint_store.as_ref(),
            &bucket_uuid,
            &vbuckets,
            self.inner.config.checkpoint.timeout,
        )
        .await?;
        let starts = resolve_initial_starts(&vbuckets, &loaded, &self.inner.config.start_from)?;
        let request = OpenGenerationRequest {
            config: Arc::clone(&self.inner.config),
            topology: topology_snapshot.topology.clone(),
            starts: starts.clone(),
            frozen_end_seqnos: None,
            stream_id: spec.stream_id,
            rollback_handler: spec.rollback_handler.clone(),
        };
        let opened = self.inner.backend.open_generation(request).await?;
        if let Err(error) = validate_opened_partitions(
            &starts,
            &opened.effective_checkpoints,
            &opened.end_seqnos,
            None,
            opened.streams.len(),
        ) {
            shutdown_stream_vec(opened.streams).await;
            let _ = close_mitigation(opened.mitigation).await;
            return Err(error);
        }
        let OpenedGeneration {
            streams,
            effective_checkpoints,
            end_seqnos,
            registry,
            rollback_count,
            mitigation,
        } = opened;
        let coordinator = match CheckpointCoordinator::new(
            self.inner.config.checkpoint.clone(),
            Arc::clone(&spec.checkpoint_store),
            effective_checkpoints.clone(),
        )
        .await
        {
            Ok(coordinator) => coordinator,
            Err(error) => {
                shutdown_stream_vec(streams).await;
                let _ = close_mitigation(mitigation).await;
                return Err(error);
            }
        };
        if let Err(error) =
            mark_changed_initial_positions(&coordinator, &loaded, &effective_checkpoints)
        {
            shutdown_stream_vec(streams).await;
            let _ = close_mitigation(mitigation).await;
            let _ = coordinator.shutdown().await;
            return Err(error);
        }
        Ok(PreparedSubscription {
            topology_snapshot,
            vbuckets,
            assignment_generation,
            streams,
            coordinator,
            end_seqnos,
            registry,
            rollback_count,
            mitigation,
        })
    }

    async fn activate_subscription(
        &self,
        spec: DcpSubscriptionSpec,
        prepared: PreparedSubscription,
    ) -> DcpSubscription {
        let PreparedSubscription {
            topology_snapshot,
            vbuckets,
            assignment_generation,
            streams,
            coordinator,
            end_seqnos,
            registry,
            rollback_count,
            mitigation,
        } = prepared;
        let registry = Arc::new(Mutex::new(registry));
        let current_generation = Arc::new(AtomicU64::new(1));
        let (sender, receiver) =
            mpsc::channel(self.inner.config.flow_control.event_queue_capacity.get());
        let (cancel, cancel_receiver) = watch::channel(false);
        self.inner
            .metrics
            .set_assigned_vbuckets(u64::try_from(vbuckets.len()).unwrap_or(u64::MAX));
        self.inner
            .metrics
            .set_active_connections(u64::try_from(streams.len()).unwrap_or(u64::MAX));
        self.inner.metrics.record_rollbacks(rollback_count);
        self.inner.health.record_success(
            SystemTime::now(),
            u64::try_from(streams.len()).unwrap_or(u64::MAX),
            topology_snapshot.generation,
        );
        let runtime = SubscriptionRuntime {
            config: Arc::clone(&self.inner.config),
            backend: Arc::clone(&self.inner.backend),
            topology_state: Arc::clone(&self.inner.topology_state),
            topology_sender: self.inner.topology_sender.clone(),
            topology_receiver: self.inner.topology_sender.subscribe(),
            topology_snapshot,
            stream_id: spec.stream_id,
            rollback_handler: spec.rollback_handler,
            assignment_generation,
            coordinator: coordinator.clone(),
            frozen_end_seqnos: end_seqnos,
            registry: Arc::clone(&registry),
            metrics: self.inner.metrics.clone(),
            health: self.inner.health.clone(),
            current_generation: Arc::clone(&current_generation),
            sender,
            global_cancel: self.inner.cancel.subscribe(),
            local_cancel: cancel_receiver,
            active_subscription: Arc::clone(&self.inner.active_subscription),
            mitigation,
        };
        let (completion_sender, completion_receiver) = watch::channel(None);
        let join = tokio::spawn(async move {
            let result = run_subscription(runtime, streams).await;
            let completion = match &result {
                Ok(()) => SubscriptionCompletion::Succeeded,
                Err(error) => SubscriptionCompletion::Failed(Arc::from(error.to_string())),
            };
            completion_sender.send_replace(Some(completion));
            result
        });
        let control = Arc::new(SubscriptionControl {
            join: AsyncMutex::new(Some(join)),
            completion: completion_receiver,
        });
        *self.inner.subscription.lock().await = Some(Arc::clone(&control));
        DcpSubscription {
            receiver,
            current_generation,
            coordinator,
            registry,
            metrics: self.inner.metrics.clone(),
            health: self.inner.health.clone(),
            cancel,
            control,
        }
    }

    /// Cloneable lock-free lifecycle and delivery metrics.
    #[must_use]
    pub fn metrics(&self) -> DcpMetrics {
        self.inner.metrics.clone()
    }

    /// Cloneable health snapshot handle.
    #[must_use]
    pub fn health(&self) -> DcpHealth {
        self.inner.health.clone()
    }

    /// Current accepted cluster topology.
    ///
    /// # Errors
    ///
    /// Returns a poisoned topology-state error.
    pub fn topology(&self) -> Result<ClusterTopology> {
        Ok(lock_topology(&self.inner.topology_state)?
            .topology()
            .clone())
    }

    /// Cancels health checks and the active subscription, then waits for final
    /// stream shutdown and checkpoint persistence.
    ///
    /// # Errors
    ///
    /// Returns a subscription, checkpoint, health-task, or task-join error.
    #[tracing::instrument(
        name = "rust_dcp.client_close",
        skip(self),
        fields(bucket = %self.inner.config.bucket),
        err
    )]
    pub async fn close(&self) -> Result<()> {
        let subscription = {
            let _lifecycle = self.inner.lifecycle.lock().await;
            self.inner.closed.store(true, Ordering::Release);
            let _ = self.inner.cancel.send(true);
            self.inner.subscription.lock().await.clone()
        };
        let mut first_error = None;
        if let Some(subscription) = subscription {
            if let Err(error) = subscription.wait().await {
                first_error = Some(error);
            }
        }
        if let Some(task) = self.inner.health_task.lock().await.take() {
            if let Err(error) = task.await {
                let error = DcpError::Topology(format!("health-check task failed: {error}"));
                if first_error.is_some() {
                    tracing::warn!(%error, "health task also failed while closing the DCP client");
                } else {
                    first_error = Some(error);
                }
            }
        }
        self.inner.metrics.set_active_connections(0);
        self.inner.metrics.set_assigned_vbuckets(0);
        self.inner.health.record_stopped(SystemTime::now());
        first_error.map_or(Ok(()), Err)
    }
}

struct ManagedOutput {
    generation: u64,
    item: Result<DcpDelivery>,
}

#[derive(Clone)]
struct TopologySnapshot {
    generation: u64,
    topology: ClusterTopology,
}

struct SubscriptionControl {
    join: AsyncMutex<Option<JoinHandle<Result<()>>>>,
    completion: watch::Receiver<Option<SubscriptionCompletion>>,
}

impl SubscriptionControl {
    async fn wait(&self) -> Result<()> {
        if let Some(join) = self.join.lock().await.take() {
            return join.await.map_err(|error| {
                DcpError::Topology(format!("subscription task failed: {error}"))
            })?;
        }
        let mut completion = self.completion.clone();
        loop {
            if let Some(result) = completion.borrow().clone() {
                return result.into_result();
            }
            completion.changed().await.map_err(|_| {
                DcpError::Topology("subscription task stopped without a completion result".into())
            })?;
        }
    }
}

#[derive(Clone, Debug)]
enum SubscriptionCompletion {
    Succeeded,
    Failed(Arc<str>),
}

impl SubscriptionCompletion {
    fn into_result(self) -> Result<()> {
        match self {
            Self::Succeeded => Ok(()),
            Self::Failed(message) => Err(DcpError::Stream {
                vbucket: 0,
                message: message.to_string(),
            }),
        }
    }
}

struct ClientInner {
    config: Arc<DcpConfig>,
    backend: Arc<dyn ClientBackend>,
    topology_state: Arc<Mutex<TopologyState>>,
    topology_sender: watch::Sender<TopologySnapshot>,
    metrics: DcpMetrics,
    health: DcpHealth,
    cancel: watch::Sender<bool>,
    lifecycle: AsyncMutex<()>,
    health_task: AsyncMutex<Option<JoinHandle<()>>>,
    subscription: AsyncMutex<Option<Arc<SubscriptionControl>>>,
    active_subscription: Arc<AtomicBool>,
    closed: AtomicBool,
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        self.health.record_stopped(SystemTime::now());
    }
}

struct ActiveReservation {
    active: Arc<AtomicBool>,
    committed: bool,
}

impl ActiveReservation {
    fn acquire(active: Arc<AtomicBool>) -> Result<Self> {
        active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                DcpError::InvalidConfiguration(
                    "this DCP client already has an active subscription".into(),
                )
            })?;
        Ok(Self {
            active,
            committed: false,
        })
    }

    const fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for ActiveReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.active.store(false, Ordering::Release);
        }
    }
}

struct HealthRuntime {
    config: Arc<DcpConfig>,
    backend: Arc<dyn ClientBackend>,
    topology_state: Arc<Mutex<TopologyState>>,
    topology_sender: watch::Sender<TopologySnapshot>,
    metrics: DcpMetrics,
    health: DcpHealth,
    cancel: watch::Receiver<bool>,
}

struct PreparedSubscription {
    topology_snapshot: TopologySnapshot,
    vbuckets: Vec<u16>,
    assignment_generation: u64,
    streams: Vec<Box<dyn ManagedNodeStream>>,
    coordinator: CheckpointCoordinator,
    end_seqnos: BTreeMap<u16, u64>,
    registry: CollectionRegistry,
    rollback_count: usize,
    mitigation: Option<RollbackMitigator>,
}

impl PreparedSubscription {
    async fn discard(self) {
        shutdown_stream_vec(self.streams).await;
        let _ = close_mitigation(self.mitigation).await;
    }
}

struct SubscriptionRuntime {
    config: Arc<DcpConfig>,
    backend: Arc<dyn ClientBackend>,
    topology_state: Arc<Mutex<TopologyState>>,
    topology_sender: watch::Sender<TopologySnapshot>,
    topology_receiver: watch::Receiver<TopologySnapshot>,
    topology_snapshot: TopologySnapshot,
    stream_id: Option<u16>,
    rollback_handler: Option<Arc<dyn RollbackHandler>>,
    assignment_generation: u64,
    coordinator: CheckpointCoordinator,
    frozen_end_seqnos: BTreeMap<u16, u64>,
    registry: Arc<Mutex<CollectionRegistry>>,
    metrics: DcpMetrics,
    health: DcpHealth,
    current_generation: Arc<AtomicU64>,
    sender: mpsc::Sender<ManagedOutput>,
    global_cancel: watch::Receiver<bool>,
    local_cancel: watch::Receiver<bool>,
    active_subscription: Arc<AtomicBool>,
    mitigation: Option<RollbackMitigator>,
}

fn resolve_assignment(
    mode: &AssignmentMode,
    topology: &ClusterTopology,
) -> Result<(Vec<u16>, u64)> {
    let (vbuckets, generation) = match mode {
        AssignmentMode::Standalone => {
            let vbuckets = (0..topology.num_vbuckets())
                .map(|vbucket| {
                    u16::try_from(vbucket).map_err(|error| {
                        DcpError::Topology(format!("invalid vBucket identifier: {error}"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            (vbuckets, 0)
        }
        AssignmentMode::External(assignment) => (
            assignment.vbuckets().collect::<Vec<_>>(),
            assignment.generation(),
        ),
    };
    if vbuckets.is_empty() {
        return Err(DcpError::InvalidConfiguration(
            "a DCP subscription must own at least one vBucket".into(),
        ));
    }
    for &vbucket in &vbuckets {
        topology.active_node(vbucket)?;
    }
    Ok((vbuckets, generation))
}

fn resolve_initial_starts(
    vbuckets: &[u16],
    loaded: &BTreeMap<u16, DcpCheckpoint>,
    fallback: &StartPosition,
) -> Result<BTreeMap<u16, StartPosition>> {
    vbuckets
        .iter()
        .map(|&vbucket| {
            let start = match loaded.get(&vbucket) {
                Some(checkpoint) => StartPosition::Checkpoint(checkpoint.clone()),
                None => match fallback {
                    StartPosition::Earliest => StartPosition::Earliest,
                    StartPosition::Latest => StartPosition::Latest,
                    StartPosition::Checkpoint(checkpoint) if checkpoint.vbucket == vbucket => {
                        StartPosition::Checkpoint(checkpoint.clone())
                    }
                    StartPosition::Checkpoint(checkpoint) => {
                        return Err(DcpError::InvalidConfiguration(format!(
                            "fallback checkpoint for vBucket {} cannot initialize missing vBucket {vbucket}",
                            checkpoint.vbucket
                        )));
                    }
                },
            };
            Ok((vbucket, start))
        })
        .collect()
}

fn mark_changed_initial_positions(
    coordinator: &CheckpointCoordinator,
    loaded: &BTreeMap<u16, DcpCheckpoint>,
    effective: &BTreeMap<u16, DcpCheckpoint>,
) -> Result<()> {
    for (&vbucket, checkpoint) in effective {
        let should_persist = match loaded.get(&vbucket) {
            Some(previous) => previous != checkpoint,
            None => true,
        };
        if should_persist {
            coordinator.mark_dirty(vbucket)?;
        }
    }
    Ok(())
}

fn lock_topology(state: &Arc<Mutex<TopologyState>>) -> Result<MutexGuard<'_, TopologyState>> {
    state
        .lock()
        .map_err(|_| DcpError::Topology("topology state was poisoned".into()))
}

fn lock_registry(
    registry: &Arc<Mutex<CollectionRegistry>>,
) -> Result<MutexGuard<'_, CollectionRegistry>> {
    registry
        .lock()
        .map_err(|_| DcpError::Collection("subscription collection registry was poisoned".into()))
}

fn replace_registry(
    registry: &Arc<Mutex<CollectionRegistry>>,
    replacement: CollectionRegistry,
) -> Result<()> {
    *lock_registry(registry)? = replacement;
    Ok(())
}

fn apply_topology_candidate(
    state: &Arc<Mutex<TopologyState>>,
    sender: &watch::Sender<TopologySnapshot>,
    metrics: &DcpMetrics,
    candidate: ClusterTopology,
) -> Result<Option<TopologySnapshot>> {
    let mut state = lock_topology(state)?;
    let Some(change) = state.apply(candidate)? else {
        return Ok(None);
    };
    let snapshot = TopologySnapshot {
        generation: change.generation(),
        topology: state.topology().clone(),
    };
    drop(state);
    metrics.record_topology_update();
    sender.send_replace(snapshot.clone());
    tracing::info!(
        topology_generation = snapshot.generation,
        "accepted a newer Couchbase bucket topology"
    );
    Ok(Some(snapshot))
}

async fn run_health_checks(mut runtime: HealthRuntime) {
    loop {
        tokio::select! {
            changed = runtime.cancel.changed() => {
                if changed.is_err() || *runtime.cancel.borrow() {
                    return;
                }
            }
            () = time::sleep(runtime.config.health_check.interval) => {}
        }
        if *runtime.cancel.borrow() {
            return;
        }
        runtime.metrics.record_health_check();
        let checked_at = SystemTime::now();
        let probe = time::timeout(
            runtime.config.health_check.timeout,
            runtime.backend.discover(Arc::clone(&runtime.config)),
        )
        .await;
        let result = match probe {
            Ok(result) => result.and_then(|candidate| {
                apply_topology_candidate(
                    &runtime.topology_state,
                    &runtime.topology_sender,
                    &runtime.metrics,
                    candidate,
                )
                .map(|_| ())
            }),
            Err(_) => Err(DcpError::Timeout(runtime.config.health_check.timeout)),
        };
        match result {
            Ok(()) => {
                let generation = runtime.topology_sender.borrow().generation;
                runtime.health.record_success(
                    checked_at,
                    runtime.metrics.snapshot().active_connections,
                    generation,
                );
            }
            Err(error) => {
                runtime.metrics.record_health_failure();
                runtime.metrics.record_bootstrap_failure();
                runtime.health.record_failure(checked_at, error.to_string());
                tracing::warn!(%error, "Couchbase DCP health probe failed");
            }
        }
    }
}

enum GenerationAction {
    Stop,
    Complete,
    Reopen(TopologySnapshot),
    Reconnect,
    Fatal(DcpError),
}

enum OutputDisposition {
    Sent,
    Stop,
    Reopen(TopologySnapshot),
}

enum ReopenDisposition {
    Opened(Vec<Box<dyn ManagedNodeStream>>),
    Stop,
    Fatal(DcpError),
}

struct ActiveSubscriptionGuard {
    active: Arc<AtomicBool>,
    metrics: DcpMetrics,
}

impl Drop for ActiveSubscriptionGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        self.metrics.set_active_connections(0);
        self.metrics.set_assigned_vbuckets(0);
    }
}

async fn run_subscription(
    mut runtime: SubscriptionRuntime,
    streams: Vec<Box<dyn ManagedNodeStream>>,
) -> Result<()> {
    let _active_guard = ActiveSubscriptionGuard {
        active: Arc::clone(&runtime.active_subscription),
        metrics: runtime.metrics.clone(),
    };
    let mut streams = streams.into_iter().collect::<SelectAll<_>>();
    let mut terminal_error = None;
    loop {
        let action = drive_generation(&mut runtime, &mut streams).await;
        let stream_shutdown = shutdown_selected_streams(streams).await;
        let mitigation_shutdown = close_mitigation(runtime.mitigation.take()).await;
        let shutdown_result = combine_generation_shutdown(stream_shutdown, mitigation_shutdown);
        runtime.metrics.set_active_connections(0);
        match action {
            GenerationAction::Stop | GenerationAction::Complete => {
                if let Err(error) = shutdown_result {
                    runtime.metrics.record_stream_error();
                    runtime
                        .health
                        .record_failure(SystemTime::now(), error.to_string());
                    terminal_error = Some(error);
                }
                break;
            }
            GenerationAction::Fatal(error) => {
                if let Err(shutdown_error) = shutdown_result {
                    tracing::warn!(
                        %shutdown_error,
                        "node-stream shutdown also failed after a terminal DCP error"
                    );
                }
                runtime.metrics.record_stream_error();
                runtime
                    .health
                    .record_failure(SystemTime::now(), error.to_string());
                terminal_error = Some(error);
                break;
            }
            GenerationAction::Reconnect => {
                record_reopen_shutdown_failure(&runtime, shutdown_result);
                let snapshot = runtime.topology_sender.borrow().clone();
                match reopen_generation(&mut runtime, snapshot).await {
                    ReopenDisposition::Opened(opened) => {
                        streams = opened.into_iter().collect();
                    }
                    ReopenDisposition::Stop => break,
                    ReopenDisposition::Fatal(error) => {
                        terminal_error = Some(error);
                        break;
                    }
                }
            }
            GenerationAction::Reopen(snapshot) => {
                record_reopen_shutdown_failure(&runtime, shutdown_result);
                match reopen_generation(&mut runtime, snapshot).await {
                    ReopenDisposition::Opened(opened) => {
                        streams = opened.into_iter().collect();
                    }
                    ReopenDisposition::Stop => break,
                    ReopenDisposition::Fatal(error) => {
                        terminal_error = Some(error);
                        break;
                    }
                }
            }
        }
    }

    finish_subscription(&mut runtime, terminal_error).await
}

async fn finish_subscription(
    runtime: &mut SubscriptionRuntime,
    terminal_error: Option<DcpError>,
) -> Result<()> {
    let mut return_error = None;
    if let Some(error) = terminal_error {
        if *runtime.global_cancel.borrow() || *runtime.local_cancel.borrow() {
            return_error = Some(error);
        } else {
            let message = error.to_string();
            let generation = runtime.current_generation.load(Ordering::Acquire);
            let output = ManagedOutput {
                generation,
                item: Err(error),
            };
            tokio::select! {
                biased;
                changed = runtime.global_cancel.changed() => {
                    let _ = changed;
                }
                changed = runtime.local_cancel.changed() => {
                    let _ = changed;
                }
                result = runtime.sender.send(output) => {
                    let _ = result;
                }
            }
            return_error = Some(DcpError::Stream {
                vbucket: 0,
                message,
            });
        }
    }
    let checkpoint_result = runtime.coordinator.shutdown().await;
    if return_error.is_none() && checkpoint_result.is_ok() {
        runtime.health.record_success(
            SystemTime::now(),
            0,
            runtime.topology_sender.borrow().generation,
        );
    } else if let Err(error) = &checkpoint_result {
        runtime
            .health
            .record_failure(SystemTime::now(), error.to_string());
    }
    match (return_error, checkpoint_result) {
        (Some(error), Err(checkpoint_error)) => {
            tracing::warn!(
                %checkpoint_error,
                "final checkpoint flush also failed after a terminal subscription error"
            );
            Err(error)
        }
        (Some(error), Ok(_)) => Err(error),
        (None, result) => result.map(|_| ()),
    }
}

fn record_reopen_shutdown_failure(runtime: &SubscriptionRuntime, result: Result<()>) {
    if let Err(error) = result {
        runtime.metrics.record_stream_error();
        runtime
            .health
            .record_failure(SystemTime::now(), error.to_string());
        tracing::warn!(%error, "old DCP generation failed during shutdown; reopening anyway");
    }
}

async fn drive_generation(
    runtime: &mut SubscriptionRuntime,
    streams: &mut SelectAll<Box<dyn ManagedNodeStream>>,
) -> GenerationAction {
    loop {
        if *runtime.global_cancel.borrow() || *runtime.local_cancel.borrow() {
            return GenerationAction::Stop;
        }
        let latest = runtime.topology_receiver.borrow().clone();
        if latest.generation > runtime.topology_snapshot.generation {
            return GenerationAction::Reopen(latest);
        }
        tokio::select! {
            biased;
            changed = runtime.global_cancel.changed() => {
                if changed.is_err() || *runtime.global_cancel.borrow() {
                    return GenerationAction::Stop;
                }
            }
            changed = runtime.local_cancel.changed() => {
                if changed.is_err() || *runtime.local_cancel.borrow() {
                    return GenerationAction::Stop;
                }
            }
            changed = runtime.topology_receiver.changed() => {
                if changed.is_ok() {
                    let snapshot = runtime.topology_receiver.borrow().clone();
                    if snapshot.generation > runtime.topology_snapshot.generation {
                        return GenerationAction::Reopen(snapshot);
                    }
                }
            }
            item = streams.next() => {
                let Some(item) = item else {
                    return if runtime.config.mode == DcpMode::Finite {
                        GenerationAction::Complete
                    } else {
                        GenerationAction::Reconnect
                    };
                };
                if let Some(action) = handle_stream_item(runtime, item).await {
                    return action;
                }
            }
        }
    }
}

async fn handle_stream_item(
    runtime: &mut SubscriptionRuntime,
    item: Result<DcpStreamItem>,
) -> Option<GenerationAction> {
    let output = match item {
        Ok(DcpStreamItem::Event(event)) => {
            if let Some(action) = wait_for_mitigation(runtime, &event).await {
                return Some(action);
            }
            if document_is_before_listener_cutoff(&runtime.config, &event) {
                let vbucket = event.vbucket();
                let seqno = event.seqno();
                let tracked = match runtime.coordinator.track_event(event) {
                    Ok(tracked) => tracked,
                    Err(error) => return Some(GenerationAction::Fatal(error)),
                };
                if let Err(error) = tracked.acknowledge() {
                    return Some(GenerationAction::Fatal(error));
                }
                tracing::trace!(
                    vbucket,
                    ?seqno,
                    ?runtime.config.listener.skip_until,
                    "skipped DCP document event before listener cutoff"
                );
                return None;
            }
            let registry = match lock_registry(&runtime.registry) {
                Ok(registry) => registry.clone(),
                Err(error) => return Some(GenerationAction::Fatal(error)),
            };
            let event = match registry.decorate(event) {
                Ok(event) => event,
                Err(error) => return Some(GenerationAction::Fatal(error)),
            };
            let tracked = match runtime.coordinator.track_event(event) {
                Ok(tracked) => tracked,
                Err(error) => return Some(GenerationAction::Fatal(error)),
            };
            let generation = runtime.current_generation.load(Ordering::Acquire);
            ManagedOutput {
                generation,
                item: Ok(DcpDelivery {
                    tracked,
                    metrics: runtime.metrics.clone(),
                    connection_generation: generation,
                    assignment_generation: runtime.assignment_generation,
                }),
            }
        }
        Ok(DcpStreamItem::Unknown(_)) => {
            runtime.metrics.record_unknown_frame();
            tracing::debug!("observed an unknown future DCP frame on the low-level stream");
            return None;
        }
        Ok(DcpStreamItem::TopologyConfig { source, payload }) => {
            let candidate = match ClusterTopology::from_json(
                &payload,
                &source,
                runtime.config.tls.enabled,
                &runtime.config.network,
            ) {
                Ok(candidate) => candidate,
                Err(error) => return Some(GenerationAction::Fatal(error)),
            };
            return match apply_topology_candidate(
                &runtime.topology_state,
                &runtime.topology_sender,
                &runtime.metrics,
                candidate,
            ) {
                Ok(Some(snapshot)) => Some(GenerationAction::Reopen(snapshot)),
                Ok(None) => None,
                Err(error) => Some(GenerationAction::Fatal(error)),
            };
        }
        Err(error) => {
            runtime.metrics.record_stream_error();
            runtime
                .health
                .record_failure(SystemTime::now(), error.to_string());
            return Some(if is_retryable(&error) {
                GenerationAction::Reconnect
            } else {
                GenerationAction::Fatal(error)
            });
        }
    };
    match send_output(runtime, output).await {
        OutputDisposition::Sent => None,
        OutputDisposition::Stop => Some(GenerationAction::Stop),
        OutputDisposition::Reopen(snapshot) => Some(GenerationAction::Reopen(snapshot)),
    }
}

fn document_is_before_listener_cutoff(config: &DcpConfig, event: &DcpEvent) -> bool {
    const NANOS_PER_SECOND: u64 = 1_000_000_000;

    let Some(skip_until) = config.listener.skip_until else {
        return false;
    };
    let cas = match event {
        DcpEvent::Mutation(event) => event.cas,
        DcpEvent::Deletion(event) => event.cas,
        DcpEvent::Expiration(event) => event.cas,
        DcpEvent::SnapshotMarker(_)
        | DcpEvent::StreamEnd(_)
        | DcpEvent::SeqNoAdvanced(_)
        | DcpEvent::SystemEvent(_)
        | DcpEvent::OsoSnapshot(_) => return false,
    };
    let Ok(cutoff_from_epoch) = skip_until.duration_since(UNIX_EPOCH) else {
        return false;
    };
    let event_time = Duration::from_secs(cas / NANOS_PER_SECOND);
    cutoff_from_epoch > event_time
}

async fn wait_for_mitigation(
    runtime: &mut SubscriptionRuntime,
    event: &DcpEvent,
) -> Option<GenerationAction> {
    let (vbucket, seqno) = mitigation_position(event)?;
    let mitigation = runtime.mitigation.as_mut()?;
    if *runtime.global_cancel.borrow() || *runtime.local_cancel.borrow() {
        return Some(GenerationAction::Stop);
    }
    let latest = runtime.topology_receiver.borrow().clone();
    if latest.generation > runtime.topology_snapshot.generation {
        return Some(GenerationAction::Reopen(latest));
    }

    let mut global_cancel = runtime.global_cancel.clone();
    let mut local_cancel = runtime.local_cancel.clone();
    let mut topology = runtime.topology_receiver.clone();
    let wait = mitigation.wait_until_safe(vbucket, seqno);
    tokio::select! {
        biased;
        changed = global_cancel.changed() => {
            let _ = changed;
            Some(GenerationAction::Stop)
        }
        changed = local_cancel.changed() => {
            let _ = changed;
            Some(GenerationAction::Stop)
        }
        changed = topology.changed() => {
            if changed.is_ok() {
                let snapshot = topology.borrow().clone();
                if snapshot.generation > runtime.topology_snapshot.generation {
                    return Some(GenerationAction::Reopen(snapshot));
                }
            }
            Some(GenerationAction::Reconnect)
        }
        result = wait => match result {
            Ok(delayed) => {
                if delayed {
                    runtime.metrics.record_rollback_mitigation_delay();
                    tracing::debug!(vbucket, seqno, "rollback mitigation released a persisted delivery");
                }
                None
            }
            Err(error) => {
                runtime.metrics.record_rollback_mitigation_failure();
                runtime.health.record_failure(SystemTime::now(), error.to_string());
                Some(GenerationAction::Fatal(error))
            }
        },
    }
}

async fn send_output(
    runtime: &mut SubscriptionRuntime,
    output: ManagedOutput,
) -> OutputDisposition {
    loop {
        tokio::select! {
            result = runtime.sender.reserve() => {
                return match result {
                    Ok(permit) => {
                        permit.send(output);
                        OutputDisposition::Sent
                    }
                    Err(_) => OutputDisposition::Stop,
                };
            }
            changed = runtime.global_cancel.changed() => {
                if changed.is_err() || *runtime.global_cancel.borrow() {
                    return OutputDisposition::Stop;
                }
            }
            changed = runtime.local_cancel.changed() => {
                if changed.is_err() || *runtime.local_cancel.borrow() {
                    return OutputDisposition::Stop;
                }
            }
            changed = runtime.topology_receiver.changed() => {
                if changed.is_ok() {
                    let snapshot = runtime.topology_receiver.borrow().clone();
                    if snapshot.generation > runtime.topology_snapshot.generation {
                        return OutputDisposition::Reopen(snapshot);
                    }
                }
            }
        }
    }
}

async fn reopen_generation(
    runtime: &mut SubscriptionRuntime,
    mut snapshot: TopologySnapshot,
) -> ReopenDisposition {
    let observed = runtime.topology_receiver.borrow_and_update().clone();
    if observed.generation > snapshot.generation {
        snapshot = observed;
    }
    let generation = match next_connection_generation(&runtime.current_generation) {
        Ok(generation) => generation,
        Err(error) => return ReopenDisposition::Fatal(error),
    };
    runtime.metrics.record_reconnect();
    let mut backoff = INITIAL_RECONNECT_BACKOFF;
    loop {
        if *runtime.global_cancel.borrow() || *runtime.local_cancel.borrow() {
            return ReopenDisposition::Stop;
        }
        let checkpoints = match runtime.coordinator.processed_checkpoints() {
            Ok(checkpoints) => checkpoints,
            Err(error) => return ReopenDisposition::Fatal(error),
        };
        let starts = checkpoints
            .iter()
            .map(|(&vbucket, checkpoint)| (vbucket, StartPosition::Checkpoint(checkpoint.clone())))
            .collect::<BTreeMap<_, _>>();
        let request = OpenGenerationRequest {
            config: Arc::clone(&runtime.config),
            topology: snapshot.topology.clone(),
            starts: starts.clone(),
            frozen_end_seqnos: Some(runtime.frozen_end_seqnos.clone()),
            stream_id: runtime.stream_id,
            rollback_handler: runtime.rollback_handler.clone(),
        };
        let attempt = runtime.backend.open_generation(request);
        let result = tokio::select! {
            result = attempt => Some(result),
            changed = runtime.global_cancel.changed() => {
                if changed.is_err() || *runtime.global_cancel.borrow() {
                    return ReopenDisposition::Stop;
                }
                None
            }
            changed = runtime.local_cancel.changed() => {
                if changed.is_err() || *runtime.local_cancel.borrow() {
                    return ReopenDisposition::Stop;
                }
                None
            }
            changed = runtime.topology_receiver.changed() => {
                if changed.is_ok() {
                    let candidate = runtime.topology_receiver.borrow().clone();
                    if candidate.generation > snapshot.generation {
                        snapshot = candidate;
                    }
                }
                None
            }
        };
        let Some(result) = result else {
            continue;
        };
        match result {
            Ok(opened) => {
                return accept_reopened_generation(runtime, snapshot, &starts, opened, generation)
                    .await;
            }
            Err(error) if is_retryable(&error) => {
                runtime.metrics.record_stream_error();
                runtime
                    .health
                    .record_failure(SystemTime::now(), error.to_string());
                tracing::warn!(%error, ?backoff, "DCP generation reopen failed; retrying");
            }
            Err(error) => return ReopenDisposition::Fatal(error),
        }
        tokio::select! {
            () = time::sleep(backoff) => {}
            changed = runtime.global_cancel.changed() => {
                if changed.is_err() || *runtime.global_cancel.borrow() {
                    return ReopenDisposition::Stop;
                }
            }
            changed = runtime.local_cancel.changed() => {
                if changed.is_err() || *runtime.local_cancel.borrow() {
                    return ReopenDisposition::Stop;
                }
            }
            changed = runtime.topology_receiver.changed() => {
                if changed.is_ok() {
                    let candidate = runtime.topology_receiver.borrow().clone();
                    if candidate.generation > snapshot.generation {
                        snapshot = candidate;
                    }
                }
            }
        }
        backoff = backoff.saturating_mul(2).min(MAX_RECONNECT_BACKOFF);
    }
}

fn next_connection_generation(generation: &AtomicU64) -> Result<u64> {
    generation
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map(|previous| previous + 1)
        .map_err(|_| DcpError::Stream {
            vbucket: 0,
            message: "connection generation overflow".into(),
        })
}

async fn accept_reopened_generation(
    runtime: &mut SubscriptionRuntime,
    snapshot: TopologySnapshot,
    starts: &BTreeMap<u16, StartPosition>,
    opened: OpenedGeneration,
    generation: u64,
) -> ReopenDisposition {
    if let Err(error) = validate_opened_partitions(
        starts,
        &opened.effective_checkpoints,
        &opened.end_seqnos,
        Some(&runtime.frozen_end_seqnos),
        opened.streams.len(),
    ) {
        shutdown_stream_vec(opened.streams).await;
        let _ = close_mitigation(opened.mitigation).await;
        return ReopenDisposition::Fatal(error);
    }
    if let Err(error) = runtime
        .coordinator
        .rebase_partitions(&opened.effective_checkpoints)
    {
        shutdown_stream_vec(opened.streams).await;
        let _ = close_mitigation(opened.mitigation).await;
        return ReopenDisposition::Fatal(error);
    }
    if let Err(error) = replace_registry(&runtime.registry, opened.registry) {
        shutdown_stream_vec(opened.streams).await;
        let _ = close_mitigation(opened.mitigation).await;
        return ReopenDisposition::Fatal(error);
    }
    let connection_count = u64::try_from(opened.streams.len()).unwrap_or(u64::MAX);
    runtime.metrics.record_rollbacks(opened.rollback_count);
    runtime.metrics.set_active_connections(connection_count);
    runtime
        .health
        .record_success(SystemTime::now(), connection_count, snapshot.generation);
    runtime.topology_snapshot = snapshot;
    runtime.mitigation = opened.mitigation;
    tracing::info!(
        connection_generation = generation,
        active_connections = opened.streams.len(),
        "opened a new DCP connection generation"
    );
    ReopenDisposition::Opened(opened.streams)
}

fn combine_generation_shutdown(streams: Result<()>, mitigation: Result<()>) -> Result<()> {
    match (streams, mitigation) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(mitigation_error)) => {
            tracing::warn!(%mitigation_error, "rollback mitigation also failed during generation shutdown");
            Err(error)
        }
    }
}

async fn shutdown_selected_streams(streams: SelectAll<Box<dyn ManagedNodeStream>>) -> Result<()> {
    let mut first_error = None;
    for stream in streams {
        if let Err(error) = stream.shutdown().await {
            tracing::warn!(%error, "failed to shut down a DCP node stream");
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn is_retryable(error: &DcpError) -> bool {
    matches!(
        error,
        DcpError::Io(_)
            | DcpError::Timeout(_)
            | DcpError::DeadConnection { .. }
            | DcpError::Topology(_)
            | DcpError::Stream { .. }
            | DcpError::Cancelled
    )
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        io,
        num::NonZeroUsize,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::Bytes;
    use futures_util::StreamExt;
    use tokio::sync::mpsc;

    use super::*;
    use crate::rollback_mitigation::{
        MitigationSource, MitigationSourceFuture, ObservationBatch, ObservationOutcome,
        RollbackMitigator,
    };
    use crate::{
        CheckpointConfig, CheckpointMode, CheckpointStoreFuture, ClusterTopology, CollectionFilter,
        CollectionManifest, CollectionRegistry, CollectionRegistryStatus, Credentials,
        DcpCheckpoint, DcpStreamItem, HealthCheckConfig, SeqNoAdvanced, SnapshotFlags,
        SnapshotMarker, StartPosition, TopologyNetwork,
    };

    const TOPOLOGY: &str = r#"{
        "rev": 7,
        "revEpoch": 2,
        "name": "travel",
        "uuid": "bucket-uuid",
        "nodeLocator": "vbucket",
        "nodesExt": [{
          "hostname": "127.0.0.1",
          "nodeUUID": "node-a",
          "services": {"kv": 11210, "kvSSL": 11207}
        }],
        "vBucketServerMap": {
          "hashAlgorithm": "CRC",
          "numReplicas": 0,
          "serverList": ["127.0.0.1:11210"],
          "vBucketMap": [[0]]
        }
      }"#;

    #[derive(Default)]
    struct MemoryStore {
        checkpoints: Mutex<BTreeMap<u16, DcpCheckpoint>>,
        save_calls: AtomicUsize,
    }

    #[derive(Default)]
    struct ManualMitigationSource {
        observations: Mutex<ObservationBatch>,
    }

    impl ManualMitigationSource {
        fn set_persisted(&self, vbucket: u16, vbucket_uuid: u64, persisted_seqno: u64) {
            self.observations.lock().unwrap().insert(
                vbucket,
                ObservationOutcome::Persisted {
                    vbucket_uuid,
                    persisted_seqno,
                },
            );
        }

        fn set_branch_changed(&self, vbucket: u16, observed_vbucket_uuid: u64) {
            self.observations.lock().unwrap().insert(
                vbucket,
                ObservationOutcome::BranchChanged {
                    observed_vbucket_uuid,
                },
            );
        }
    }

    impl MitigationSource for ManualMitigationSource {
        fn observe(&self) -> MitigationSourceFuture<'_> {
            let observations = self.observations.lock().unwrap().clone();
            Box::pin(async move { observations })
        }
    }

    impl CheckpointStore for MemoryStore {
        fn load<'a>(
            &'a self,
            bucket_uuid: &'a str,
            vbuckets: &'a [u16],
        ) -> CheckpointStoreFuture<'a, BTreeMap<u16, DcpCheckpoint>> {
            Box::pin(async move {
                let requested = vbuckets
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>();
                Ok(self
                    .checkpoints
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(vbucket, checkpoint)| {
                        requested.contains(vbucket)
                            && checkpoint.bucket_uuid.as_deref() == Some(bucket_uuid)
                    })
                    .map(|(&vbucket, checkpoint)| (vbucket, checkpoint.clone()))
                    .collect())
            })
        }

        fn save<'a>(&'a self, checkpoints: &'a [DcpCheckpoint]) -> CheckpointStoreFuture<'a, ()> {
            Box::pin(async move {
                let mut stored = self.checkpoints.lock().unwrap();
                for checkpoint in checkpoints {
                    stored.insert(checkpoint.vbucket, checkpoint.clone());
                }
                self.save_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }

        fn clear<'a>(
            &'a self,
            _bucket_uuid: &'a str,
            vbuckets: &'a [u16],
        ) -> CheckpointStoreFuture<'a, ()> {
            Box::pin(async move {
                let mut stored = self.checkpoints.lock().unwrap();
                for vbucket in vbuckets {
                    stored.remove(vbucket);
                }
                Ok(())
            })
        }
    }

    struct FakeStream {
        receiver: mpsc::UnboundedReceiver<Result<DcpStreamItem>>,
        shutdowns: Arc<AtomicUsize>,
        shutdown_error: bool,
        shutdown_gate: Option<Arc<tokio::sync::Notify>>,
    }

    impl Stream for FakeStream {
        type Item = Result<DcpStreamItem>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.receiver.poll_recv(context)
        }
    }

    impl ManagedNodeStream for FakeStream {
        fn shutdown(self: Box<Self>) -> ClientFuture<()> {
            Box::pin(async move {
                self.shutdowns.fetch_add(1, Ordering::SeqCst);
                if let Some(gate) = &self.shutdown_gate {
                    gate.notified().await;
                }
                if self.shutdown_error {
                    Err(DcpError::Io(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "fake shutdown failed",
                    )))
                } else {
                    Ok(())
                }
            })
        }
    }

    struct FakeBackend {
        discoveries: Mutex<VecDeque<Result<ClusterTopology>>>,
        generations: Mutex<VecDeque<Result<OpenedGeneration>>>,
        requests: Mutex<Vec<OpenGenerationRequest>>,
        open_notify: tokio::sync::Notify,
        discovery_calls: AtomicUsize,
        discovery_notify: tokio::sync::Notify,
        open_gate: Option<Arc<tokio::sync::Notify>>,
    }

    impl FakeBackend {
        fn new(
            discoveries: impl IntoIterator<Item = Result<ClusterTopology>>,
            generations: impl IntoIterator<Item = Result<OpenedGeneration>>,
        ) -> Self {
            Self {
                discoveries: Mutex::new(discoveries.into_iter().collect()),
                generations: Mutex::new(generations.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
                open_notify: tokio::sync::Notify::new(),
                discovery_calls: AtomicUsize::new(0),
                discovery_notify: tokio::sync::Notify::new(),
                open_gate: None,
            }
        }

        fn with_open_gate(mut self, gate: Arc<tokio::sync::Notify>) -> Self {
            self.open_gate = Some(gate);
            self
        }

        async fn wait_for_open_calls(&self, count: usize) {
            loop {
                if self.requests.lock().unwrap().len() >= count {
                    return;
                }
                self.open_notify.notified().await;
            }
        }

        async fn wait_for_discovery_calls(&self, count: usize) {
            loop {
                if self.discovery_calls.load(Ordering::SeqCst) >= count {
                    return;
                }
                self.discovery_notify.notified().await;
            }
        }
    }

    impl ClientBackend for FakeBackend {
        fn discover(&self, _config: Arc<DcpConfig>) -> ClientFuture<ClusterTopology> {
            self.discovery_calls.fetch_add(1, Ordering::SeqCst);
            self.discovery_notify.notify_waiters();
            let result = self
                .discoveries
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err(DcpError::Topology("no fake discovery".into())));
            Box::pin(async move { result })
        }

        fn open_generation(
            &self,
            request: OpenGenerationRequest,
        ) -> ClientFuture<OpenedGeneration> {
            self.requests.lock().unwrap().push(request);
            self.open_notify.notify_waiters();
            let result = self
                .generations
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err(DcpError::Topology("no fake generation".into())));
            let gate = self.open_gate.clone();
            Box::pin(async move {
                if let Some(gate) = gate {
                    gate.notified().await;
                }
                result
            })
        }
    }

    fn topology(revision: u64) -> ClusterTopology {
        ClusterTopology::from_json(
            TOPOLOGY
                .replace("\"rev\": 7", &format!("\"rev\": {revision}"))
                .as_bytes(),
            "127.0.0.1:11210",
            false,
            &TopologyNetwork::Default,
        )
        .unwrap()
    }

    fn config(start_from: StartPosition) -> DcpConfig {
        let mut config = DcpConfig::builder(Credentials::new("alice", "secret"), "travel")
            .seed("127.0.0.1:11210")
            .unwrap()
            .start_from(start_from)
            .checkpoint(CheckpointConfig {
                mode: CheckpointMode::Manual,
                timeout: Duration::from_secs(1),
            })
            .build()
            .unwrap();
        config.health_check = HealthCheckConfig {
            enabled: false,
            interval: Duration::from_secs(60),
            timeout: Duration::from_secs(1),
        };
        config
    }

    fn checkpoint(seqno: u64, vbucket_uuid: u64) -> DcpCheckpoint {
        DcpCheckpoint {
            bucket_uuid: Some("bucket-uuid".into()),
            vbucket: 0,
            vbucket_uuid,
            seqno,
            snapshot_start: seqno,
            snapshot_end: seqno,
            manifest_uid: None,
        }
    }

    fn registry() -> CollectionRegistry {
        let manifest = CollectionManifest::parse(
            br#"{"uid":"7","scopes":[{"uid":"0","name":"_default","collections":[{"uid":"0","name":"_default"}]}]}"#,
        )
        .unwrap();
        CollectionRegistry::new(
            manifest
                .resolve(&CollectionFilter::default(), None)
                .unwrap()
                .into(),
        )
    }

    fn opened_generation(
        effective: DcpCheckpoint,
    ) -> (
        OpenedGeneration,
        mpsc::UnboundedSender<Result<DcpStreamItem>>,
        Arc<AtomicUsize>,
    ) {
        opened_generation_with_end(effective, u64::MAX)
    }

    fn opened_generation_with_end(
        effective: DcpCheckpoint,
        end_seqno: u64,
    ) -> (
        OpenedGeneration,
        mpsc::UnboundedSender<Result<DcpStreamItem>>,
        Arc<AtomicUsize>,
    ) {
        opened_generation_with_shutdown(effective, end_seqno, false, None)
    }

    fn opened_generation_with_shutdown_error(
        effective: DcpCheckpoint,
        shutdown_error: bool,
    ) -> (
        OpenedGeneration,
        mpsc::UnboundedSender<Result<DcpStreamItem>>,
        Arc<AtomicUsize>,
    ) {
        opened_generation_with_shutdown(effective, u64::MAX, shutdown_error, None)
    }

    fn opened_generation_with_shutdown(
        effective: DcpCheckpoint,
        end_seqno: u64,
        shutdown_error: bool,
        shutdown_gate: Option<Arc<tokio::sync::Notify>>,
    ) -> (
        OpenedGeneration,
        mpsc::UnboundedSender<Result<DcpStreamItem>>,
        Arc<AtomicUsize>,
    ) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let shutdowns = Arc::new(AtomicUsize::new(0));
        (
            OpenedGeneration {
                streams: vec![Box::new(FakeStream {
                    receiver,
                    shutdowns: Arc::clone(&shutdowns),
                    shutdown_error,
                    shutdown_gate,
                })],
                effective_checkpoints: BTreeMap::from([(0, effective)]),
                end_seqnos: BTreeMap::from([(0, end_seqno)]),
                registry: registry(),
                rollback_count: 0,
                mitigation: None,
            },
            sender,
            shutdowns,
        )
    }

    fn snapshot(vbucket: u16, seqno: u64) -> DcpStreamItem {
        snapshot_range(vbucket, seqno, seqno)
    }

    fn snapshot_range(vbucket: u16, start_seqno: u64, end_seqno: u64) -> DcpStreamItem {
        DcpStreamItem::Event(DcpEvent::SnapshotMarker(SnapshotMarker {
            vbucket,
            start_seqno,
            end_seqno,
            flags: SnapshotFlags::MEMORY,
            high_completed_seqno: None,
            max_visible_seqno: None,
            purge_seqno: None,
        }))
    }

    fn mutation(vbucket: u16, seqno: u64, cas: u64) -> DcpStreamItem {
        DcpStreamItem::Event(DcpEvent::Mutation(crate::DcpMutation {
            vbucket,
            seqno,
            rev_seqno: 1,
            flags: 0,
            expiry: 0,
            lock_time: 0,
            cas,
            datatype: crate::DataType::default(),
            collection_id: Some(0),
            collection_name: None,
            key: Bytes::from_static(b"mutation"),
            value: Bytes::new(),
        }))
    }

    fn deletion(vbucket: u16, seqno: u64, cas: u64) -> DcpStreamItem {
        DcpStreamItem::Event(DcpEvent::Deletion(crate::DcpDeletion {
            vbucket,
            seqno,
            rev_seqno: 2,
            delete_time: None,
            cas,
            collection_id: Some(0),
            collection_name: None,
            key: Bytes::from_static(b"deletion"),
            value: Bytes::new(),
            datatype: crate::DataType::default(),
        }))
    }

    fn expiration(vbucket: u16, seqno: u64, cas: u64) -> DcpStreamItem {
        DcpStreamItem::Event(DcpEvent::Expiration(crate::DcpExpiration {
            vbucket,
            seqno,
            rev_seqno: 3,
            delete_time: None,
            cas,
            collection_id: Some(0),
            collection_name: None,
            key: Bytes::from_static(b"expiration"),
            value: Bytes::new(),
            datatype: crate::DataType::default(),
        }))
    }

    fn advanced(vbucket: u16, seqno: u64) -> DcpStreamItem {
        DcpStreamItem::Event(DcpEvent::SeqNoAdvanced(SeqNoAdvanced { vbucket, seqno }))
    }

    async fn next_delivery(subscription: &mut DcpSubscription) -> DcpDelivery {
        tokio::time::timeout(Duration::from_secs(1), subscription.next())
            .await
            .expect("subscription item timed out")
            .expect("subscription ended")
            .expect("subscription failed")
    }

    #[tokio::test]
    async fn subscribe_tracks_processing_and_persists_a_missing_latest_start() {
        let (generation, sender, shutdowns) = opened_generation(checkpoint(42, 11));
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(generation)]));
        let client = DcpClient::connect_with_backend(
            config(StartPosition::Latest),
            Arc::clone(&backend) as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();
        let store = Arc::new(MemoryStore::default());
        let mut subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(
                Arc::clone(&store) as Arc<dyn CheckpointStore>
            ))
            .await
            .unwrap();

        assert!(matches!(
            backend.requests.lock().unwrap()[0].starts[&0],
            StartPosition::Latest
        ));
        let report = subscription.flush().await.unwrap();
        assert_eq!((report.attempted, report.persisted), (1, 1));
        assert_eq!(store.checkpoints.lock().unwrap()[&0].seqno, 42);

        sender.send(Ok(snapshot(0, 43))).unwrap();
        sender.send(Ok(advanced(0, 43))).unwrap();
        let marker = next_delivery(&mut subscription).await;
        assert!(matches!(marker.event(), DcpEvent::SnapshotMarker(_)));
        assert_eq!(marker.mark_processed().await.unwrap(), None);
        let progress = next_delivery(&mut subscription).await;
        assert_eq!(progress.connection_generation(), 1);
        assert_eq!(progress.assignment_generation(), 0);
        assert_eq!(
            progress
                .mark_processed()
                .await
                .unwrap()
                .unwrap()
                .advanced_to
                .unwrap()
                .seqno,
            43
        );
        subscription.flush().await.unwrap();
        assert_eq!(store.checkpoints.lock().unwrap()[&0].seqno, 43);
        assert_eq!(client.metrics().snapshot().delivered_events, 2);
        assert_eq!(client.metrics().snapshot().processed_events, 2);
        assert_eq!(
            subscription.collection_status().unwrap(),
            CollectionRegistryStatus {
                manifest_uid: Some(7),
                stale: false,
            }
        );

        subscription.close().await.unwrap();
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn listener_skip_until_filters_documents_and_advances_checkpoint_progress() {
        let (generation, sender, _) = opened_generation(checkpoint(0, 11));
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(generation)]));
        let mut dcp_config = config(StartPosition::Earliest);
        dcp_config.listener.skip_until = Some(std::time::UNIX_EPOCH + Duration::from_secs(20));
        let client = DcpClient::connect_with_backend(dcp_config, backend as Arc<dyn ClientBackend>)
            .await
            .unwrap();
        let store = Arc::new(MemoryStore::default());
        let mut subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(
                Arc::clone(&store) as Arc<dyn CheckpointStore>
            ))
            .await
            .unwrap();

        sender.send(Ok(snapshot_range(0, 1, 4))).unwrap();
        sender.send(Ok(mutation(0, 1, 19_999_999_999))).unwrap();
        sender.send(Ok(deletion(0, 2, 19_000_000_000))).unwrap();
        sender.send(Ok(expiration(0, 3, 19_000_000_000))).unwrap();
        sender.send(Ok(mutation(0, 4, 20_000_000_000))).unwrap();

        let marker = next_delivery(&mut subscription).await;
        assert!(matches!(marker.event(), DcpEvent::SnapshotMarker(_)));
        let boundary = next_delivery(&mut subscription).await;
        assert!(matches!(
            boundary.event(),
            DcpEvent::Mutation(event) if event.seqno == 4
        ));
        let before_ack = subscription.checkpoint_statuses().unwrap();
        assert_eq!(before_ack[&0].processed.seqno, 3);
        assert_eq!(before_ack[&0].pending_events, 1);

        boundary.mark_processed().await.unwrap();
        subscription.flush().await.unwrap();
        assert_eq!(store.checkpoints.lock().unwrap()[&0].seqno, 4);
        assert_eq!(client.metrics().snapshot().delivered_events, 2);
        assert_eq!(client.metrics().snapshot().processed_events, 1);

        subscription.close().await.unwrap();
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn subscription_does_not_deliver_before_rollback_mitigation_is_persisted() {
        let source = Arc::new(ManualMitigationSource::default());
        source.set_persisted(0, 11, 0);
        let mitigation = RollbackMitigator::spawn(
            crate::RollbackMitigationConfig {
                enabled: true,
                poll_interval: Duration::from_millis(1),
                request_timeout: Duration::from_millis(10),
                maximum_stall: Duration::from_secs(1),
            },
            source.clone(),
            BTreeMap::from([(0, 11)]),
        )
        .unwrap();
        let (mut generation, sender, _shutdowns) = opened_generation(checkpoint(0, 11));
        generation.mitigation = Some(mitigation);
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(generation)]));
        let client = DcpClient::connect_with_backend(
            config(StartPosition::Earliest),
            backend as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();
        let mut subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(Arc::new(
                MemoryStore::default(),
            )))
            .await
            .unwrap();

        sender.send(Ok(snapshot(0, 1))).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), subscription.next())
                .await
                .is_err()
        );
        source.set_persisted(0, 11, 1);
        let delivery = next_delivery(&mut subscription).await;
        assert!(matches!(delivery.event(), DcpEvent::SnapshotMarker(_)));
        assert_eq!(client.metrics().snapshot().rollback_mitigation_delays, 1);

        subscription.close().await.unwrap();
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn subscription_close_cancels_a_rollback_mitigation_wait() {
        let source = Arc::new(ManualMitigationSource::default());
        source.set_persisted(0, 11, 0);
        let mitigation = RollbackMitigator::spawn(
            crate::RollbackMitigationConfig {
                enabled: true,
                poll_interval: Duration::from_millis(1),
                request_timeout: Duration::from_millis(10),
                maximum_stall: Duration::from_secs(10),
            },
            source,
            BTreeMap::from([(0, 11)]),
        )
        .unwrap();
        let (mut generation, sender, shutdowns) = opened_generation(checkpoint(0, 11));
        generation.mitigation = Some(mitigation);
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(generation)]));
        let client = DcpClient::connect_with_backend(
            config(StartPosition::Earliest),
            backend as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();
        let mut subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(Arc::new(
                MemoryStore::default(),
            )))
            .await
            .unwrap();

        sender.send(Ok(snapshot(0, 1))).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), subscription.next())
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_millis(250), subscription.close())
            .await
            .expect("subscription close must cancel mitigation before maximum_stall")
            .unwrap();

        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(client.metrics().snapshot().rollback_mitigation_failures, 0);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn rollback_mitigation_stall_is_reported_instead_of_reconnecting_forever() {
        let source = Arc::new(ManualMitigationSource::default());
        source.set_branch_changed(0, 22);
        let mitigation = RollbackMitigator::spawn(
            crate::RollbackMitigationConfig {
                enabled: true,
                poll_interval: Duration::from_millis(1),
                request_timeout: Duration::from_millis(5),
                maximum_stall: Duration::from_millis(30),
            },
            source,
            BTreeMap::from([(0, 11)]),
        )
        .unwrap();
        let (mut generation, sender, _shutdowns) = opened_generation(checkpoint(0, 11));
        generation.mitigation = Some(mitigation);
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(generation)]));
        let client = DcpClient::connect_with_backend(
            config(StartPosition::Earliest),
            backend as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();
        let mut subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(Arc::new(
                MemoryStore::default(),
            )))
            .await
            .unwrap();

        sender.send(Ok(snapshot(0, 1))).unwrap();
        let error = tokio::time::timeout(Duration::from_millis(250), subscription.next())
            .await
            .expect("bounded mitigation must surface an error")
            .expect("the subscription must yield the mitigation error")
            .unwrap_err();

        assert!(matches!(error, DcpError::RollbackMitigation { .. }));
        assert_eq!(client.metrics().snapshot().rollback_mitigation_failures, 1);
        let _ = client.close().await;
    }

    #[tokio::test]
    async fn missing_latest_start_at_zero_is_persisted_before_shutdown() {
        let (generation, _sender, _shutdowns) = opened_generation(checkpoint(0, 11));
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(generation)]));
        let client = DcpClient::connect_with_backend(
            config(StartPosition::Latest),
            backend as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();
        let store = Arc::new(MemoryStore::default());
        let subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(
                Arc::clone(&store) as Arc<dyn CheckpointStore>
            ))
            .await
            .unwrap();

        subscription.close().await.unwrap();

        assert_eq!(store.save_calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.checkpoints.lock().unwrap()[&0].seqno, 0);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn topology_change_reopens_from_processed_checkpoint_and_drops_stale_deliveries() {
        let (first, first_sender, first_shutdowns) = opened_generation(checkpoint(0, 11));
        let (second, second_sender, _second_shutdowns) = opened_generation(checkpoint(1, 22));
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(first), Ok(second)]));
        let client = DcpClient::connect_with_backend(
            config(StartPosition::Earliest),
            Arc::clone(&backend) as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();
        let store = Arc::new(MemoryStore::default());
        let mut subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(store))
            .await
            .unwrap();

        first_sender.send(Ok(snapshot(0, 1))).unwrap();
        first_sender.send(Ok(advanced(0, 1))).unwrap();
        next_delivery(&mut subscription)
            .await
            .mark_processed()
            .await
            .unwrap();
        next_delivery(&mut subscription)
            .await
            .mark_processed()
            .await
            .unwrap();

        first_sender.send(Ok(snapshot(0, 2))).unwrap();
        first_sender.send(Ok(advanced(0, 2))).unwrap();
        first_sender
            .send(Ok(DcpStreamItem::TopologyConfig {
                source: "127.0.0.1:11210".into(),
                payload: TOPOLOGY.replace("\"rev\": 7", "\"rev\": 8").into(),
            }))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), backend.wait_for_open_calls(2))
            .await
            .unwrap();

        let resume_seqno = {
            let requests = backend.requests.lock().unwrap();
            let StartPosition::Checkpoint(resume) = &requests[1].starts[&0] else {
                panic!("reopen must use a processed checkpoint");
            };
            resume.seqno
        };
        assert_eq!(resume_seqno, 1);
        assert_eq!(first_shutdowns.load(Ordering::SeqCst), 1);

        second_sender.send(Ok(snapshot(0, 2))).unwrap();
        second_sender.send(Ok(advanced(0, 2))).unwrap();
        let marker = next_delivery(&mut subscription).await;
        assert_eq!(marker.connection_generation(), 2);
        marker.mark_processed().await.unwrap();
        let progress = next_delivery(&mut subscription).await;
        assert_eq!(progress.connection_generation(), 2);
        progress.mark_processed().await.unwrap();
        assert_eq!(client.metrics().snapshot().stale_generation_drops, 2);
        assert_eq!(client.metrics().snapshot().reconnects, 1);

        subscription.close().await.unwrap();
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_initial_discovery_is_observable() {
        let backend = Arc::new(FakeBackend::new(
            [Err(DcpError::Io(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "offline",
            )))],
            [],
        ));

        let error = DcpClient::connect_with_backend(
            config(StartPosition::Earliest),
            backend as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, DcpError::Io(_)));
    }

    #[tokio::test]
    async fn one_client_fences_a_single_subscription_and_releases_it_after_close() {
        let (first, _first_sender, _first_shutdowns) = opened_generation(checkpoint(0, 11));
        let (second, second_sender, _second_shutdowns) = opened_generation(checkpoint(0, 11));
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(first), Ok(second)]));
        let client = DcpClient::connect_with_backend(
            config(StartPosition::Earliest),
            Arc::clone(&backend) as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();
        let store: Arc<dyn CheckpointStore> = Arc::new(MemoryStore::default());
        let first_subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(Arc::clone(&store)))
            .await
            .unwrap();

        let error = client
            .subscribe(DcpSubscriptionSpec::standalone(Arc::clone(&store)))
            .await
            .unwrap_err();
        assert!(matches!(error, DcpError::InvalidConfiguration(_)));
        first_subscription.close().await.unwrap();

        let mut second_subscription = client
            .subscribe(DcpSubscriptionSpec::external(
                store,
                VBucketAssignment::new(9, [0]),
            ))
            .await
            .unwrap();
        second_sender.send(Ok(snapshot(0, 1))).unwrap();
        let delivery = next_delivery(&mut second_subscription).await;
        assert_eq!(delivery.assignment_generation(), 9);
        delivery.mark_processed().await.unwrap();
        second_subscription.close().await.unwrap();
        assert_eq!(backend.requests.lock().unwrap().len(), 2);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn finite_subscription_completes_when_all_node_streams_end() {
        let (generation, sender, _shutdowns) = opened_generation(checkpoint(0, 11));
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(generation)]));
        let mut finite_config = config(StartPosition::Earliest);
        finite_config.mode = DcpMode::Finite;
        let client =
            DcpClient::connect_with_backend(finite_config, backend as Arc<dyn ClientBackend>)
                .await
                .unwrap();
        let mut subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(Arc::new(
                MemoryStore::default(),
            )))
            .await
            .unwrap();

        drop(sender);
        let end = tokio::time::timeout(Duration::from_secs(1), subscription.next())
            .await
            .unwrap();
        assert!(end.is_none());
        subscription.close().await.unwrap();
        assert_eq!(client.metrics().snapshot().active_connections, 0);
        assert_eq!(client.metrics().snapshot().assigned_vbuckets, 0);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn finite_reconnect_preserves_the_initial_high_seqno_boundary() {
        let (first, first_sender, _first_shutdowns) =
            opened_generation_with_end(checkpoint(0, 11), 5);
        let (second, _second_sender, _second_shutdowns) =
            opened_generation_with_end(checkpoint(0, 11), 5);
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(first), Ok(second)]));
        let mut finite_config = config(StartPosition::Earliest);
        finite_config.mode = DcpMode::Finite;
        let client = DcpClient::connect_with_backend(
            finite_config,
            Arc::clone(&backend) as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();
        let subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(Arc::new(
                MemoryStore::default(),
            )))
            .await
            .unwrap();

        first_sender
            .send(Err(DcpError::DeadConnection {
                peer: "fake-node".into(),
                idle_for: Duration::from_secs(1),
            }))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), backend.wait_for_open_calls(2))
            .await
            .unwrap();

        {
            let requests = backend.requests.lock().unwrap();
            assert_eq!(
                requests[1]
                    .frozen_end_seqnos
                    .as_ref()
                    .and_then(|ends| ends.get(&0)),
                Some(&5)
            );
        }
        subscription.close().await.unwrap();
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn periodic_health_probe_accepts_new_topology_and_close_marks_stopped() {
        let backend = Arc::new(FakeBackend::new([Ok(topology(7)), Ok(topology(8))], []));
        let mut health_config = config(StartPosition::Earliest);
        health_config.health_check = HealthCheckConfig {
            enabled: true,
            interval: Duration::from_millis(20),
            timeout: Duration::from_secs(1),
        };
        let client = DcpClient::connect_with_backend(
            health_config,
            Arc::clone(&backend) as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(1), backend.wait_for_discovery_calls(2))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.metrics().snapshot().topology_updates == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(client.topology().unwrap().revision().revision(), 8);
        assert_eq!(client.health().snapshot().topology_generation, 2);
        assert_eq!(
            client.health().snapshot().status,
            crate::DcpHealthStatus::Healthy
        );

        client.close().await.unwrap();
        assert_eq!(
            client.health().snapshot().status,
            crate::DcpHealthStatus::Stopped
        );
    }

    #[tokio::test]
    async fn preflight_high_seqno_rollback_requires_explicit_replay_policy() {
        let start = StartPosition::Checkpoint(checkpoint(100, 99));
        let failover_log = vec![
            crate::FailoverEntry {
                vbucket_uuid: 22,
                seqno: 50,
            },
            crate::FailoverEntry {
                vbucket_uuid: 11,
                seqno: 0,
            },
        ];

        let error = resolve_vbucket_request(
            0,
            "bucket-uuid",
            &start,
            DcpMode::Infinite,
            80,
            failover_log.clone(),
            None,
            RollbackPolicy::StopAndReport,
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            DcpError::RollbackRequired {
                requested_seqno: 100,
                rollback_seqno: 80,
                ..
            }
        ));

        let (request, rollbacks) = resolve_vbucket_request(
            0,
            "bucket-uuid",
            &start,
            DcpMode::Infinite,
            80,
            failover_log,
            None,
            RollbackPolicy::RewindAndReplay,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(rollbacks, 1);
        assert_eq!(request.checkpoint().seqno, 80);
        assert_eq!(request.checkpoint().snapshot_start, 80);
        assert_eq!(request.checkpoint().snapshot_end, 80);
        assert_eq!(request.checkpoint().vbucket_uuid, 22);
        assert_eq!(request.checkpoint().manifest_uid, None);
    }

    #[tokio::test]
    async fn subscription_close_surfaces_node_shutdown_failure() {
        let (generation, _sender, shutdowns) =
            opened_generation_with_shutdown_error(checkpoint(0, 11), true);
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(generation)]));
        let client = DcpClient::connect_with_backend(
            config(StartPosition::Earliest),
            backend as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();
        let subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(Arc::new(
                MemoryStore::default(),
            )))
            .await
            .unwrap();

        let error = subscription.close().await.unwrap_err();
        assert!(matches!(error, DcpError::Io(_)));
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
        assert!(client.close().await.is_err());
        assert_eq!(
            client.health().snapshot().status,
            crate::DcpHealthStatus::Stopped
        );
    }

    #[tokio::test]
    async fn terminal_error_does_not_block_close_when_delivery_queue_is_full() {
        let (generation, sender, _shutdowns) = opened_generation(checkpoint(0, 11));
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(generation)]));
        let mut bounded_config = config(StartPosition::Earliest);
        bounded_config.flow_control.event_queue_capacity = NonZeroUsize::new(1).unwrap();
        let client =
            DcpClient::connect_with_backend(bounded_config, backend as Arc<dyn ClientBackend>)
                .await
                .unwrap();
        let subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(Arc::new(
                MemoryStore::default(),
            )))
            .await
            .unwrap();

        sender.send(Ok(snapshot(0, 1))).unwrap();
        sender
            .send(Err(DcpError::Collection("terminal fake error".into())))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.metrics().snapshot().stream_errors == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let error = tokio::time::timeout(Duration::from_secs(1), subscription.close())
            .await
            .expect("subscription close must not wait for queue capacity")
            .unwrap_err();
        assert!(error.to_string().contains("terminal fake error"));
        assert!(client.close().await.is_err());
    }

    #[tokio::test]
    async fn close_racing_with_subscribe_never_activates_after_client_shutdown() {
        let (generation, _sender, shutdowns) = opened_generation(checkpoint(0, 11));
        let gate = Arc::new(tokio::sync::Notify::new());
        let backend = Arc::new(
            FakeBackend::new([Ok(topology(7))], [Ok(generation)]).with_open_gate(Arc::clone(&gate)),
        );
        let client = DcpClient::connect_with_backend(
            config(StartPosition::Earliest),
            Arc::clone(&backend) as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();
        let subscribing_client = client.clone();
        let subscribe = tokio::spawn(async move {
            subscribing_client
                .subscribe(DcpSubscriptionSpec::standalone(Arc::new(
                    MemoryStore::default(),
                )))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), backend.wait_for_open_calls(1))
            .await
            .unwrap();

        client.close().await.unwrap();
        gate.notify_one();
        let error = subscribe.await.unwrap().unwrap_err();

        assert!(matches!(error, DcpError::Cancelled));
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(
            client.health().snapshot().status,
            crate::DcpHealthStatus::Stopped
        );
    }

    #[tokio::test]
    async fn concurrent_close_callers_both_wait_for_subscription_shutdown() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let (generation, _sender, shutdowns) = opened_generation_with_shutdown(
            checkpoint(0, 11),
            u64::MAX,
            false,
            Some(Arc::clone(&gate)),
        );
        let backend = Arc::new(FakeBackend::new([Ok(topology(7))], [Ok(generation)]));
        let client = DcpClient::connect_with_backend(
            config(StartPosition::Earliest),
            backend as Arc<dyn ClientBackend>,
        )
        .await
        .unwrap();
        let subscription = client
            .subscribe(DcpSubscriptionSpec::standalone(Arc::new(
                MemoryStore::default(),
            )))
            .await
            .unwrap();
        let closing_client = client.clone();
        let subscription_close = tokio::spawn(async move { subscription.close().await });
        let client_close = tokio::spawn(async move { closing_client.close().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while shutdowns.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        tokio::task::yield_now().await;
        assert!(!subscription_close.is_finished());
        assert!(!client_close.is_finished());
        gate.notify_one();
        subscription_close.await.unwrap().unwrap();
        client_close.await.unwrap().unwrap();
    }
}
