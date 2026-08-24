//! Core types and runtime building blocks for `rust-dcp`.

#![forbid(unsafe_code)]

mod assignment;
mod auth;
mod bootstrap;
mod checkpoint;
mod checkpoint_runtime;
mod checkpoint_store;
mod config;
mod error;
mod event;
mod stream;
mod topology;
mod transport;

pub use assignment::{AssignmentMode, VBucketAssignment};
pub use auth::{SaslMechanism, ScramAlgorithm};
pub use bootstrap::{
    BootstrapCapabilities, DcpConnection, DcpControlFeature, bootstrap_connection,
    bootstrap_on_connection,
};
pub use checkpoint::{DcpCheckpoint, FailoverEntry};
pub use checkpoint_runtime::{
    AckOutcome, CheckpointCoordinator, CheckpointFlushReport, CheckpointMetrics, CheckpointStream,
    CheckpointStreamItem, EventAck, PartitionCheckpointStatus, TrackedEvent, load_checkpoints,
};
pub use checkpoint_store::{
    CheckpointStore, CheckpointStoreFuture, CouchbaseCheckpointCollection,
    CouchbaseCheckpointStore, FileCheckpointStore,
};
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
pub use rust_dcp_protocol::{DcpStreamFlags, StreamFilter};
pub use stream::{
    DcpStream, DcpStreamItem, PartitionOpenState, RollbackAction, RollbackApplied, RollbackHandler,
    RollbackRequest, StreamOpenReport, VBucketStreamRequest, open_dcp_stream,
};
pub use topology::{
    ClusterTopology, KvEndpoint, NodeId, TopologyChange, TopologyNetwork, TopologyRevision,
    TopologyState, discover_topology, fetch_active_high_seqnos, fetch_failover_log,
};
pub use transport::{AsyncIo, BoxedIo, KvConnection};
