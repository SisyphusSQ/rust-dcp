use std::{io, time::Duration};

use thiserror::Error;

use crate::FailoverEntry;
use rust_dcp_protocol::ProtocolError;

/// Result type used throughout `rust-dcp`.
pub type Result<T, E = DcpError> = std::result::Result<T, E>;

/// Errors surfaced by the DCP client.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DcpError {
    /// User-supplied configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// A wire frame or protocol message is invalid.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// An I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// TLS setup or validation failed.
    #[error("TLS error: {0}")]
    Tls(String),

    /// Authentication was rejected or could not be completed.
    #[error("authentication failed: {0}")]
    Authentication(String),

    /// The server returned a non-success protocol status.
    #[error("server returned status 0x{status:04x} for opcode 0x{opcode:02x}: {message}")]
    ServerStatus {
        /// Memcached status code.
        status: u16,
        /// Opcode associated with the response.
        opcode: u8,
        /// Human-readable context.
        message: String,
    },

    /// A cluster topology could not be discovered or applied.
    #[error("topology error: {0}")]
    Topology(String),

    /// The requested operation is unsupported by the connected server.
    #[error("unsupported capability: {0}")]
    Unsupported(String),

    /// Collection manifest, filter, or event state is inconsistent.
    #[error("collection state error: {0}")]
    Collection(String),

    /// A checkpoint could not be validated or persisted.
    #[error("checkpoint error: {0}")]
    Checkpoint(String),

    /// A checkpoint store operation failed.
    #[error("checkpoint store error: {0}")]
    CheckpointStore(String),

    /// The server requires the partition to roll back to an earlier history.
    #[error("vBucket {vbucket} requires rollback from seqno {requested_seqno} to {rollback_seqno}")]
    RollbackRequired {
        /// Affected vBucket.
        vbucket: u16,
        /// Sequence number requested by the client.
        requested_seqno: u64,
        /// Sequence number required by the server.
        rollback_seqno: u64,
        /// Current server failover log, newest entry first.
        failover_log: Vec<FailoverEntry>,
    },

    /// Work from an obsolete assignment generation was observed.
    #[error("stale assignment generation {observed}; current generation is {current}")]
    StaleGeneration {
        /// Generation attached to the operation.
        observed: u64,
        /// Current assignment generation.
        current: u64,
    },

    /// An operation exceeded its configured timeout.
    #[error("operation timed out after {0:?}")]
    Timeout(Duration),

    /// No inbound frame arrived within the configured liveness window.
    #[error("KV peer {peer} was silent for {idle_for:?}")]
    DeadConnection {
        /// Endpoint whose connection stopped making inbound progress.
        peer: String,
        /// Configured maximum idle duration.
        idle_for: Duration,
    },

    /// The subscription or client was cancelled.
    #[error("operation cancelled")]
    Cancelled,

    /// A stream ended unexpectedly.
    #[error("stream error for vBucket {vbucket}: {message}")]
    Stream {
        /// Affected vBucket.
        vbucket: u16,
        /// Human-readable reason.
        message: String,
    },
}
