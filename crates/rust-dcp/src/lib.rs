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
    AssignmentMode, BootstrapCapabilities, CheckpointConfig, CheckpointMode, CollectionFilter,
    Credentials, DataType, DcpCheckpoint, DcpConfig, DcpConfigBuilder, DcpConnection,
    DcpControlFeature, DcpDeletion, DcpError, DcpEvent, DcpExpiration, DcpMode, DcpMutation,
    DcpPriority, DcpStream, DcpStreamFlags, DcpStreamItem, FailoverEntry, FlowControlConfig,
    HealthCheckConfig, OsoSnapshot, OsoSnapshotState, PartitionOpenState, RollbackAction,
    RollbackApplied, RollbackHandler, RollbackPolicy, RollbackRequest, SeedAddress, SeqNoAdvanced,
    SnapshotFlags, SnapshotMarker, StartPosition, StreamEnd, StreamEndReason, StreamFilter,
    StreamOpenReport, SystemEvent, SystemEventKind, TlsConfig, VBucketAssignment,
    VBucketStreamRequest, bootstrap_connection, open_dcp_stream,
};
