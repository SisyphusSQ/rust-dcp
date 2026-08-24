use serde::{Deserialize, Serialize};

use crate::{ProtocolError, Result};

/// Memcached binary protocol magic byte.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Magic {
    /// Client request using the classic header.
    Request,
    /// Server response using the classic header.
    Response,
    /// Server-initiated request using the classic header.
    ServerRequest,
    /// Client or DCP producer request with flexible framing extras.
    AltRequest,
    /// Server response with flexible framing extras.
    AltResponse,
}

impl Magic {
    /// Wire byte for this magic variant.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Request => 0x80,
            Self::Response => 0x81,
            Self::ServerRequest => 0x82,
            Self::AltRequest => 0x08,
            Self::AltResponse => 0x18,
        }
    }

    /// Whether the status/vBucket header field represents a vBucket.
    #[must_use]
    pub const fn is_request(self) -> bool {
        matches!(self, Self::Request | Self::AltRequest | Self::ServerRequest)
    }

    /// Whether this is a response packet.
    #[must_use]
    pub const fn is_response(self) -> bool {
        matches!(self, Self::Response | Self::AltResponse)
    }

    /// Whether this variant uses flexible framing extras.
    #[must_use]
    pub const fn is_alt(self) -> bool {
        matches!(self, Self::AltRequest | Self::AltResponse)
    }
}

impl TryFrom<u8> for Magic {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x80 => Ok(Self::Request),
            0x81 => Ok(Self::Response),
            0x82 => Ok(Self::ServerRequest),
            0x08 => Ok(Self::AltRequest),
            0x18 => Ok(Self::AltResponse),
            other => Err(ProtocolError::InvalidMagic(other)),
        }
    }
}

/// Memcached command opcode.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Opcode(pub u8);

impl Opcode {
    /// Read a document.
    pub const GET: Self = Self(0x00);
    /// Write a document.
    pub const SET: Self = Self(0x01);
    /// Delete a document.
    pub const DELETE: Self = Self(0x04);
    /// Basic NOOP command.
    pub const NOOP: Self = Self(0x0a);
    /// Negotiate connection features.
    pub const HELLO: Self = Self(0x1f);
    /// List SASL mechanisms.
    pub const SASL_LIST_MECHS: Self = Self(0x20);
    /// Begin SASL authentication.
    pub const SASL_AUTH: Self = Self(0x21);
    /// Continue SASL authentication.
    pub const SASL_STEP: Self = Self(0x22);
    /// Return vBucket high sequence numbers.
    pub const GET_ALL_VB_SEQNOS: Self = Self(0x48);
    /// Open a DCP producer/consumer connection.
    pub const DCP_OPEN: Self = Self(0x50);
    /// Close a DCP stream.
    pub const DCP_CLOSE_STREAM: Self = Self(0x52);
    /// Open one vBucket DCP stream.
    pub const DCP_STREAM_REQUEST: Self = Self(0x53);
    /// Read one vBucket failover log.
    pub const DCP_GET_FAILOVER_LOG: Self = Self(0x54);
    /// DCP stream-end event.
    pub const DCP_STREAM_END: Self = Self(0x55);
    /// DCP snapshot marker event.
    pub const DCP_SNAPSHOT_MARKER: Self = Self(0x56);
    /// DCP mutation event.
    pub const DCP_MUTATION: Self = Self(0x57);
    /// DCP deletion event.
    pub const DCP_DELETION: Self = Self(0x58);
    /// DCP expiration event.
    pub const DCP_EXPIRATION: Self = Self(0x59);
    /// DCP NOOP request.
    pub const DCP_NOOP: Self = Self(0x5c);
    /// Return DCP flow-control credit.
    pub const DCP_BUFFER_ACK: Self = Self(0x5d);
    /// Configure a DCP connection.
    pub const DCP_CONTROL: Self = Self(0x5e);
    /// Collection/scope system event.
    pub const DCP_SYSTEM_EVENT: Self = Self(0x5f);
    /// Filtered stream sequence-number advancement.
    pub const DCP_SEQNO_ADVANCED: Self = Self(0x64);
    /// Out-of-sequence snapshot marker.
    pub const DCP_OSO_SNAPSHOT: Self = Self(0x65);
    /// Select a bucket on the current connection.
    pub const SELECT_BUCKET: Self = Self(0x89);
    /// Read the cluster configuration.
    pub const GET_CLUSTER_CONFIG: Self = Self(0xb5);
    /// Read the collection manifest.
    pub const COLLECTIONS_GET_MANIFEST: Self = Self(0xba);
    /// Resolve a scope/collection identifier.
    pub const COLLECTIONS_GET_ID: Self = Self(0xbb);

    /// Raw opcode byte.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self.0
    }

    /// Whether keys for this opcode carry a collection-ID ULEB128 prefix.
    #[must_use]
    pub const fn is_collection_encoded(self) -> bool {
        matches!(
            self.0,
            0x00..=0x06
                | 0x0e..=0x0f
                | 0x1c..=0x1d
                | 0x57..=0x59
                | 0x83
                | 0x94..=0x95
                | 0xa0
                | 0xa2
                | 0xa8
                | 0xc5..=0xd2
        )
    }
}

/// Memcached response status.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Status(pub u16);

impl Status {
    /// Successful response.
    pub const SUCCESS: Self = Self(0x00);
    /// Request arguments are invalid for this server or command.
    pub const INVALID_ARGUMENTS: Self = Self(0x04);
    /// Request was sent to a non-owner node.
    pub const NOT_MY_VBUCKET: Self = Self(0x07);
    /// Authentication failed.
    pub const AUTH_ERROR: Self = Self(0x20);
    /// Multi-step authentication must continue.
    pub const AUTH_CONTINUE: Self = Self(0x21);
    /// DCP stream must roll back.
    pub const ROLLBACK: Self = Self(0x23);
    /// Server does not know the command.
    pub const UNKNOWN_COMMAND: Self = Self(0x81);
    /// Command is known but unsupported.
    pub const NOT_SUPPORTED: Self = Self(0x83);
    /// Temporary failure.
    pub const TEMPORARY_FAILURE: Self = Self(0x86);
    /// Collection identifier is unknown.
    pub const COLLECTION_UNKNOWN: Self = Self(0x88);
    /// Scope identifier is unknown.
    pub const SCOPE_UNKNOWN: Self = Self(0x8c);

    /// Raw status value.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    /// Whether the operation completed successfully.
    #[must_use]
    pub const fn is_success(self) -> bool {
        self.0 == 0
    }
}

/// One vBucket failover-log entry, newest first on the wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FailoverEntry {
    /// UUID identifying the history branch.
    pub vbucket_uuid: u64,
    /// First sequence number belonging to this branch.
    pub seqno: u64,
}
