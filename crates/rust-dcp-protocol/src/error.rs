use std::io;

use thiserror::Error;

/// Result returned by wire-protocol operations.
pub type Result<T, E = ProtocolError> = std::result::Result<T, E>;

/// Memcached binary or DCP protocol failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// An I/O error surfaced through a codec consumer.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// The header contains an unsupported magic byte.
    #[error("unsupported Memcached magic 0x{0:02x}")]
    InvalidMagic(u8),

    /// A length field exceeds the configured or representable limit.
    #[error("invalid frame length: {0}")]
    InvalidLength(String),

    /// A frame is structurally inconsistent.
    #[error("malformed frame: {0}")]
    MalformedFrame(String),

    /// A DCP message has invalid extras or value data.
    #[error("malformed DCP message: {0}")]
    MalformedDcp(String),

    /// A collection-ID ULEB128 prefix is invalid.
    #[error("invalid collection ID: {0}")]
    InvalidCollectionId(String),

    /// A request could not be encoded.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// A server response has an unexpected opcode or status.
    #[error("server status 0x{status:04x} for opcode 0x{opcode:02x}: {message}")]
    ServerStatus {
        /// Memcached status code.
        status: u16,
        /// Response opcode.
        opcode: u8,
        /// Server payload or local context.
        message: String,
    },

    /// JSON serialization for a stream filter failed.
    #[error("stream filter serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}
