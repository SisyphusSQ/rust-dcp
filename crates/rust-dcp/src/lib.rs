//! An asynchronous, embeddable Couchbase DCP client.
//!
//! The public API keeps network buffer credit, application processing, and
//! durable checkpoint persistence as separate lifecycle operations.

#![forbid(unsafe_code)]

/// Core public models used to configure and consume DCP streams.
pub mod core {
    pub use rust_dcp_core::*;
}

/// Low-level wire protocol primitives.
pub mod protocol {
    pub use rust_dcp_protocol::*;
}

pub use rust_dcp_core::{
    AckOutcome, AssignmentMode, BootstrapCapabilities, CheckpointConfig, CheckpointCoordinator,
    CheckpointFlushReport, CheckpointMetrics, CheckpointMode, CheckpointStore,
    CheckpointStoreFuture, CheckpointStream, CheckpointStreamItem, ClusterTopology,
    CollectionFilter, CollectionManifest, CollectionRegistry, CollectionRegistryStatus,
    CollectionSelection, CollectionStream, CouchbaseCheckpointCollection,
    CouchbaseCheckpointCollectionSpec, CouchbaseCheckpointStore, CouchbaseKvCheckpointCollection,
    Credentials, DataType, DcpCheckpoint, DcpClient, DcpConfig, DcpConfigBuilder, DcpConnection,
    DcpControlFeature, DcpDeletion, DcpDelivery, DcpError, DcpEvent, DcpExpiration, DcpHealth,
    DcpHealthSnapshot, DcpHealthStatus, DcpMetrics, DcpMetricsSnapshot, DcpMode, DcpMutation,
    DcpPriority, DcpStream, DcpStreamFlags, DcpStreamItem, DcpSubscription, DcpSubscriptionSpec,
    EventAck, FailoverEntry, FileCheckpointStore, FlowControlConfig, HealthCheckConfig,
    ListenerConfig, ManifestCollection, ManifestScope, NoopCheckpointStore, OsoSnapshot,
    OsoSnapshotState, PartitionCheckpointStatus, PartitionOpenState, ReadOnlyCheckpointStore,
    ResolvedCollectionFilter, Result, RollbackAction, RollbackApplied, RollbackHandler,
    RollbackMitigationConfig, RollbackPolicy, RollbackRequest, SeedAddress, SeqNoAdvanced,
    SnapshotFlags, SnapshotMarker, StartPosition, StreamEnd, StreamEndReason, StreamFilter,
    StreamOpenReport, SystemEvent, SystemEventKind, TlsConfig, TopologyNetwork, TrackedEvent,
    VBucketAssignment, VBucketStreamRequest, bootstrap_connection, fetch_collection_manifest,
    fetch_selection_high_seqnos, load_checkpoints, open_dcp_stream, resolve_collection_id,
    resolve_collection_selection,
};

#[cfg(feature = "prometheus")]
pub use rust_dcp_prometheus::DcpPrometheusCollector;
