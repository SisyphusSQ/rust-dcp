//! Core types and runtime building blocks for `rust-dcp`.

#![forbid(unsafe_code)]

mod assignment;
mod auth;
mod bootstrap;
mod checkpoint;
mod config;
mod error;
mod event;
mod transport;

pub use assignment::{AssignmentMode, VBucketAssignment};
pub use auth::{SaslMechanism, ScramAlgorithm};
pub use bootstrap::{
    BootstrapCapabilities, DcpConnection, DcpControlFeature, bootstrap_connection,
    bootstrap_on_connection,
};
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
pub use transport::{AsyncIo, BoxedIo, KvConnection};
