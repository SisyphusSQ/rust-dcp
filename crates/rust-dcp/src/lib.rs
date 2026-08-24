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
    AssignmentMode, CheckpointConfig, CheckpointMode, CollectionFilter, Credentials, DataType,
    DcpCheckpoint, DcpConfig, DcpConfigBuilder, DcpDeletion, DcpError, DcpEvent, DcpExpiration,
    DcpMode, DcpMutation, DcpPriority, FailoverEntry, FlowControlConfig, HealthCheckConfig,
    OsoSnapshot, OsoSnapshotState, RollbackPolicy, SeedAddress, SeqNoAdvanced, SnapshotFlags,
    SnapshotMarker, StartPosition, StreamEnd, StreamEndReason, SystemEvent, SystemEventKind,
    TlsConfig, VBucketAssignment,
};
