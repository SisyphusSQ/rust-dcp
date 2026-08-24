use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Memcached datatype flags attached to a value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DataType(u8);

impl DataType {
    /// JSON value bit.
    pub const JSON: Self = Self(0x01);
    /// Snappy-compressed value bit.
    pub const SNAPPY: Self = Self(0x02);
    /// XATTR prefix bit.
    pub const XATTR: Self = Self(0x04);

    /// Preserves every bit supplied by the server.
    #[must_use]
    pub const fn from_bits_retain(bits: u8) -> Self {
        Self(bits)
    }

    /// Raw datatype byte.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Tests whether all bits in `flag` are set.
    #[must_use]
    pub const fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 == flag.0
    }
}

/// Snapshot marker flags.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SnapshotFlags(u32);

impl SnapshotFlags {
    /// Snapshot contains in-memory items.
    pub const MEMORY: Self = Self(0x01);
    /// Snapshot contains on-disk items.
    pub const DISK: Self = Self(0x02);
    /// Checkpoint boundary marker.
    pub const CHECKPOINT: Self = Self(0x04);
    /// Snapshot acknowledges completion.
    pub const ACK: Self = Self(0x08);
    /// Snapshot may contain historical values.
    pub const HISTORY: Self = Self(0x10);
    /// Snapshot can contain duplicate sequence numbers.
    pub const MAY_CONTAIN_DUPLICATES: Self = Self(0x20);

    /// Preserves every bit supplied by the server.
    #[must_use]
    pub const fn from_bits_retain(bits: u32) -> Self {
        Self(bits)
    }

    /// Raw flags.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Tests whether all bits in `flag` are set.
    #[must_use]
    pub const fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 == flag.0
    }
}

/// Document mutation event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcpMutation {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Sequence number in the vBucket history.
    pub seqno: u64,
    /// Document revision sequence number.
    pub rev_seqno: u64,
    /// Document flags.
    pub flags: u32,
    /// Expiration epoch seconds.
    pub expiry: u32,
    /// Lock time supplied by the server.
    pub lock_time: u32,
    /// CAS value.
    pub cas: u64,
    /// Value datatype flags.
    pub datatype: DataType,
    /// Collection ID when collections are enabled.
    pub collection_id: Option<u32>,
    /// Resolved collection name when available.
    pub collection_name: Option<String>,
    /// Document key without collection-ID prefix.
    pub key: Bytes,
    /// Raw document bytes, including XATTR framing when indicated by datatype.
    pub value: Bytes,
}

impl DcpMutation {
    /// Mirrors go-dcp's `IsCreated` behavior.
    #[must_use]
    pub const fn is_created(&self) -> bool {
        self.rev_seqno == 1
    }
}

/// Document deletion event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcpDeletion {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Sequence number in the vBucket history.
    pub seqno: u64,
    /// Document revision sequence number.
    pub rev_seqno: u64,
    /// Deletion epoch seconds when supplied by the server.
    pub delete_time: Option<u32>,
    /// CAS value.
    pub cas: u64,
    /// Collection ID when collections are enabled.
    pub collection_id: Option<u32>,
    /// Resolved collection name when available.
    pub collection_name: Option<String>,
    /// Document key without collection-ID prefix.
    pub key: Bytes,
    /// Optional tombstone value (for example, XATTR data).
    pub value: Bytes,
    /// Tombstone datatype flags.
    pub datatype: DataType,
}

/// Document expiration event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcpExpiration {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Sequence number in the vBucket history.
    pub seqno: u64,
    /// Document revision sequence number.
    pub rev_seqno: u64,
    /// Deletion epoch seconds when supplied by the server.
    pub delete_time: Option<u32>,
    /// CAS value.
    pub cas: u64,
    /// Collection ID when collections are enabled.
    pub collection_id: Option<u32>,
    /// Resolved collection name when available.
    pub collection_name: Option<String>,
    /// Document key without collection-ID prefix.
    pub key: Bytes,
    /// Optional tombstone value.
    pub value: Bytes,
    /// Tombstone datatype flags.
    pub datatype: DataType,
}

/// Snapshot boundary for one vBucket stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotMarker {
    /// vBucket identifier.
    pub vbucket: u16,
    /// First sequence number in the snapshot.
    pub start_seqno: u64,
    /// Last sequence number in the snapshot.
    pub end_seqno: u64,
    /// Marker flags.
    pub flags: SnapshotFlags,
    /// Highest completed prepare sequence number.
    pub high_completed_seqno: Option<u64>,
    /// Highest sequence number visible to readers.
    pub max_visible_seqno: Option<u64>,
    /// Purge sequence number at marker creation.
    pub purge_seqno: Option<u64>,
}

/// Reason encoded in a DCP stream-end event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StreamEndReason {
    /// Finite stream reached its requested end.
    Ok,
    /// vBucket ownership changed.
    StateChanged,
    /// Client closed the stream.
    Closed,
    /// Stream disconnected from its producer.
    Disconnected,
    /// Stream was too slow for the producer.
    TooSlow,
    /// Backfill could not be completed.
    BackfillFailed,
    /// Server-side collection filter became empty.
    FilterEmpty,
    /// Unrecognized future reason.
    Unknown(u32),
}

/// End of one vBucket stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamEnd {
    /// vBucket identifier.
    pub vbucket: u16,
    /// End reason.
    pub reason: StreamEndReason,
}

/// Progress jump emitted for filtered collections.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeqNoAdvanced {
    /// vBucket identifier.
    pub vbucket: u16,
    /// New contiguous sequence number.
    pub seqno: u64,
}

/// System-event payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SystemEventKind {
    /// A collection was created.
    CollectionCreated {
        /// Scope identifier.
        scope_id: u32,
        /// Collection identifier.
        collection_id: u32,
        /// Optional maximum TTL seconds.
        max_ttl: Option<u32>,
    },
    /// A collection was removed.
    CollectionDropped {
        /// Scope identifier.
        scope_id: u32,
        /// Collection identifier.
        collection_id: u32,
    },
    /// A collection was flushed.
    CollectionFlushed {
        /// Collection identifier.
        collection_id: u32,
    },
    /// A scope was created.
    ScopeCreated {
        /// Scope identifier.
        scope_id: u32,
    },
    /// A scope was removed.
    ScopeDropped {
        /// Scope identifier.
        scope_id: u32,
    },
    /// Collection properties changed.
    CollectionChanged {
        /// Scope identifier.
        scope_id: u32,
        /// Collection identifier.
        collection_id: u32,
        /// Optional maximum TTL seconds.
        max_ttl: Option<u32>,
    },
    /// A future system-event type not yet understood by this SDK.
    Unknown {
        /// Raw event code.
        code: u32,
        /// Raw event-data bytes.
        data: Bytes,
    },
}

/// Collection or scope manifest event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemEvent {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Sequence number in the vBucket history.
    pub seqno: u64,
    /// Manifest UID.
    pub manifest_uid: u64,
    /// Protocol event version.
    pub version: u8,
    /// Collection or scope name supplied in the event key.
    pub key: Bytes,
    /// Typed event payload.
    pub kind: SystemEventKind,
}

/// OSO snapshot state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OsoSnapshotState {
    /// Start of an out-of-sequence snapshot.
    Begin,
    /// End of an out-of-sequence snapshot.
    End,
    /// Future state value.
    Unknown(u32),
}

/// Out-of-sequence snapshot marker.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OsoSnapshot {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Marker state.
    pub state: OsoSnapshotState,
}

/// Public event stream returned by `rust-dcp`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type", content = "event")]
#[non_exhaustive]
pub enum DcpEvent {
    /// Document mutation.
    Mutation(DcpMutation),
    /// Explicit document deletion.
    Deletion(DcpDeletion),
    /// Expiration-driven document deletion.
    Expiration(DcpExpiration),
    /// Snapshot boundary.
    SnapshotMarker(SnapshotMarker),
    /// Stream termination.
    StreamEnd(StreamEnd),
    /// Filtered progress advancement.
    SeqNoAdvanced(SeqNoAdvanced),
    /// Collection/scope manifest change.
    SystemEvent(SystemEvent),
    /// Out-of-sequence snapshot boundary.
    OsoSnapshot(OsoSnapshot),
}

impl DcpEvent {
    /// vBucket to which the event belongs.
    #[must_use]
    pub const fn vbucket(&self) -> u16 {
        match self {
            Self::Mutation(event) => event.vbucket,
            Self::Deletion(event) => event.vbucket,
            Self::Expiration(event) => event.vbucket,
            Self::SnapshotMarker(event) => event.vbucket,
            Self::StreamEnd(event) => event.vbucket,
            Self::SeqNoAdvanced(event) => event.vbucket,
            Self::SystemEvent(event) => event.vbucket,
            Self::OsoSnapshot(event) => event.vbucket,
        }
    }

    /// Sequence number that can advance ordered progress, if present.
    #[must_use]
    pub const fn seqno(&self) -> Option<u64> {
        match self {
            Self::Mutation(event) => Some(event.seqno),
            Self::Deletion(event) => Some(event.seqno),
            Self::Expiration(event) => Some(event.seqno),
            Self::SeqNoAdvanced(event) => Some(event.seqno),
            Self::SystemEvent(event) => Some(event.seqno),
            Self::SnapshotMarker(_) | Self::StreamEnd(_) | Self::OsoSnapshot(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seqno_advanced_is_visible_to_progress_chain() {
        let event = DcpEvent::SeqNoAdvanced(SeqNoAdvanced {
            vbucket: 12,
            seqno: 99,
        });

        assert_eq!(event.vbucket(), 12);
        assert_eq!(event.seqno(), Some(99));
    }

    #[test]
    fn datatype_preserves_future_bits() {
        let datatype = DataType::from_bits_retain(0x83);

        assert!(datatype.contains(DataType::JSON));
        assert!(datatype.contains(DataType::SNAPPY));
        assert_eq!(datatype.bits(), 0x83);
    }

    #[test]
    fn mutation_creation_matches_revision_one() {
        let mutation = DcpMutation {
            vbucket: 1,
            seqno: 2,
            rev_seqno: 1,
            flags: 0,
            expiry: 0,
            lock_time: 0,
            cas: 0,
            datatype: DataType::default(),
            collection_id: None,
            collection_name: None,
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
        };

        assert!(mutation.is_created());
    }
}
