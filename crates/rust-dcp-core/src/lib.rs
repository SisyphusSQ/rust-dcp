//! Core types and runtime building blocks for `rust-dcp`.

#![forbid(unsafe_code)]

mod assignment;
mod checkpoint;
mod config;
mod error;
mod event;

pub use assignment::{AssignmentMode, VBucketAssignment};
pub use checkpoint::{DcpCheckpoint, FailoverEntry};
pub use config::{
    CheckpointConfig, CheckpointMode, CollectionFilter, Credentials, DcpConfig, DcpConfigBuilder,
    DcpMode, DcpPriority, FlowControlConfig, HealthCheckConfig, RollbackPolicy, SeedAddress,
    StartPosition, TlsConfig,
};
pub use error::{DcpError, Result};
pub use event::{
    DataType, DcpDeletion, DcpEvent, DcpExpiration, DcpMutation, OsoSnapshot, OsoSnapshotState,
    SeqNoAdvanced, SnapshotFlags, SnapshotMarker, StreamEnd, StreamEndReason, SystemEvent,
    SystemEventKind,
};
