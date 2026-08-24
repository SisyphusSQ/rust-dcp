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
    CheckpointStoreFuture, CheckpointStream, CheckpointStreamItem, CollectionFilter,
    CouchbaseCheckpointCollection, CouchbaseCheckpointStore, Credentials, DataType, DcpCheckpoint,
    DcpConfig, DcpConfigBuilder, DcpConnection, DcpControlFeature, DcpDeletion, DcpError, DcpEvent,
    DcpExpiration, DcpMode, DcpMutation, DcpPriority, DcpStream, DcpStreamFlags, DcpStreamItem,
    EventAck, FailoverEntry, FileCheckpointStore, FlowControlConfig, HealthCheckConfig,
    OsoSnapshot, OsoSnapshotState, PartitionCheckpointStatus, PartitionOpenState, RollbackAction,
    RollbackApplied, RollbackHandler, RollbackPolicy, RollbackRequest, SeedAddress, SeqNoAdvanced,
    SnapshotFlags, SnapshotMarker, StartPosition, StreamEnd, StreamEndReason, StreamFilter,
    StreamOpenReport, SystemEvent, SystemEventKind, TlsConfig, TrackedEvent, VBucketAssignment,
    VBucketStreamRequest, bootstrap_connection, load_checkpoints, open_dcp_stream,
};
