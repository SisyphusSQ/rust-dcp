use bytes::Bytes;

use crate::{Frame, Opcode, ProtocolError, Result};

/// Raw DCP mutation decoded from a producer frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DcpMutation {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Optional multiplexed stream ID.
    pub stream_id: Option<u16>,
    /// vBucket sequence number.
    pub seqno: u64,
    /// Document revision number.
    pub rev_seqno: u64,
    /// Document flags.
    pub flags: u32,
    /// Expiration epoch seconds.
    pub expiry: u32,
    /// Document lock time.
    pub lock_time: u32,
    /// CAS value.
    pub cas: u64,
    /// Datatype byte.
    pub datatype: u8,
    /// Collection ID decoded from the key.
    pub collection_id: Option<u32>,
    /// Document key.
    pub key: Bytes,
    /// Raw document value.
    pub value: Bytes,
}

/// Raw DCP deletion decoded from a producer frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DcpDeletion {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Optional multiplexed stream ID.
    pub stream_id: Option<u16>,
    /// vBucket sequence number.
    pub seqno: u64,
    /// Document revision number.
    pub rev_seqno: u64,
    /// Delete epoch seconds in v2 frames.
    pub delete_time: Option<u32>,
    /// CAS value.
    pub cas: u64,
    /// Datatype byte.
    pub datatype: u8,
    /// Collection ID decoded from the key.
    pub collection_id: Option<u32>,
    /// Document key.
    pub key: Bytes,
    /// Optional tombstone/XATTR value.
    pub value: Bytes,
}

/// Raw DCP expiration decoded from a producer frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DcpExpiration {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Optional multiplexed stream ID.
    pub stream_id: Option<u16>,
    /// vBucket sequence number.
    pub seqno: u64,
    /// Document revision number.
    pub rev_seqno: u64,
    /// Delete epoch seconds when supplied.
    pub delete_time: Option<u32>,
    /// CAS value.
    pub cas: u64,
    /// Datatype byte.
    pub datatype: u8,
    /// Collection ID decoded from the key.
    pub collection_id: Option<u32>,
    /// Document key.
    pub key: Bytes,
    /// Optional tombstone/XATTR value.
    pub value: Bytes,
}

/// Raw snapshot boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotMarker {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Optional multiplexed stream ID.
    pub stream_id: Option<u16>,
    /// First sequence number in the snapshot.
    pub start_seqno: u64,
    /// Last sequence number in the snapshot.
    pub end_seqno: u64,
    /// Snapshot flags.
    pub flags: u32,
    /// Highest sequence number visible to readers.
    pub max_visible_seqno: Option<u64>,
    /// Highest completed prepare sequence number.
    pub high_completed_seqno: Option<u64>,
    /// Snapshot timestamp in v2.1+ markers.
    pub timestamp: Option<u64>,
    /// Purge sequence number in newer marker versions.
    pub purge_seqno: Option<u64>,
}

/// Producer reason for ending a DCP stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamEndReason {
    /// Requested stream range completed.
    Ok,
    /// Client closed the stream.
    Closed,
    /// vBucket state or ownership changed.
    StateChanged,
    /// Producer disconnected the stream.
    Disconnected,
    /// Consumer was too slow.
    TooSlow,
    /// Disk backfill failed.
    BackfillFailed,
    /// Server-side collection filter became empty.
    FilterEmpty,
    /// Future reason code.
    Unknown(u32),
}

/// Raw DCP stream-end event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamEnd {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Optional multiplexed stream ID.
    pub stream_id: Option<u16>,
    /// Producer reason.
    pub reason: StreamEndReason,
}

/// Raw DCP sequence-number advancement event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeqNoAdvanced {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Optional multiplexed stream ID.
    pub stream_id: Option<u16>,
    /// New contiguous sequence number.
    pub seqno: u64,
}

/// Raw collection/scope event payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemEventKind {
    /// Collection creation.
    CollectionCreated {
        /// Scope identifier.
        scope_id: u32,
        /// Collection identifier.
        collection_id: u32,
        /// Optional maximum TTL seconds.
        max_ttl: Option<u32>,
    },
    /// Collection deletion.
    CollectionDropped {
        /// Scope identifier.
        scope_id: u32,
        /// Collection identifier.
        collection_id: u32,
    },
    /// Collection flush.
    CollectionFlushed {
        /// Collection identifier.
        collection_id: u32,
    },
    /// Scope creation.
    ScopeCreated {
        /// Scope identifier.
        scope_id: u32,
    },
    /// Scope deletion.
    ScopeDropped {
        /// Scope identifier.
        scope_id: u32,
    },
    /// Collection property update.
    CollectionChanged {
        /// Collection identifier.
        collection_id: u32,
        /// Maximum TTL seconds.
        max_ttl: u32,
    },
    /// Future event code with its raw data retained.
    Unknown {
        /// Raw event code.
        code: u32,
        /// Raw event value.
        data: Bytes,
    },
}

/// Raw DCP system event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemEvent {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Optional multiplexed stream ID.
    pub stream_id: Option<u16>,
    /// vBucket sequence number.
    pub seqno: u64,
    /// Event protocol version.
    pub version: u8,
    /// Manifest UID.
    pub manifest_uid: u64,
    /// Collection or scope name.
    pub key: Bytes,
    /// Typed event data.
    pub kind: SystemEventKind,
}

/// OSO snapshot boundary state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OsoSnapshotState {
    /// Start of an OSO snapshot.
    Begin,
    /// End of an OSO snapshot.
    End,
    /// Future state code.
    Unknown(u32),
}

/// Raw OSO snapshot marker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OsoSnapshot {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Optional multiplexed stream ID.
    pub stream_id: Option<u16>,
    /// Begin/end state.
    pub state: OsoSnapshotState,
}

/// Producer-side DCP frame decoded into a typed message.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DcpMessage {
    /// Mutation event.
    Mutation(DcpMutation),
    /// Explicit deletion event.
    Deletion(DcpDeletion),
    /// Expiration event.
    Expiration(DcpExpiration),
    /// Snapshot marker.
    SnapshotMarker(SnapshotMarker),
    /// Stream end.
    StreamEnd(StreamEnd),
    /// Filtered progress advancement.
    SeqNoAdvanced(SeqNoAdvanced),
    /// Collection/scope manifest event.
    SystemEvent(SystemEvent),
    /// Out-of-sequence snapshot boundary.
    OsoSnapshot(OsoSnapshot),
    /// Producer NOOP that requires a response carrying the same opaque.
    Noop {
        /// Correlation token to echo.
        opaque: u32,
    },
    /// Unrecognized frame retained for forward compatibility.
    Unknown(Frame),
}

/// Parses a producer frame into a typed DCP message.
///
/// # Errors
///
/// Returns [`ProtocolError::MalformedDcp`] when a known DCP opcode has
/// truncated or inconsistent extras/value fields, or when the packet is a
/// response rather than a producer request.
pub fn parse_dcp_message(frame: &Frame) -> Result<DcpMessage> {
    if !frame.magic.is_request() {
        return Err(ProtocolError::MalformedDcp(
            "DCP event must use request or server-request magic".into(),
        ));
    }
    let stream_id = frame.stream_id();
    match frame.opcode {
        Opcode::DCP_MUTATION => parse_mutation(frame, stream_id),
        Opcode::DCP_DELETION => parse_deletion(frame, stream_id),
        Opcode::DCP_EXPIRATION => parse_expiration(frame, stream_id),
        Opcode::DCP_SNAPSHOT_MARKER => parse_snapshot_marker(frame, stream_id),
        Opcode::DCP_STREAM_END => {
            require_len("stream-end extras", &frame.extras, 4)?;
            let raw = read_u32(&frame.extras, 0);
            let reason = match raw {
                0 => StreamEndReason::Ok,
                1 => StreamEndReason::Closed,
                2 => StreamEndReason::StateChanged,
                3 => StreamEndReason::Disconnected,
                4 => StreamEndReason::TooSlow,
                5 => StreamEndReason::BackfillFailed,
                7 => StreamEndReason::FilterEmpty,
                other => StreamEndReason::Unknown(other),
            };
            Ok(DcpMessage::StreamEnd(StreamEnd {
                vbucket: frame.vbucket,
                stream_id,
                reason,
            }))
        }
        Opcode::DCP_SEQNO_ADVANCED => {
            require_len("seqno-advanced extras", &frame.extras, 8)?;
            Ok(DcpMessage::SeqNoAdvanced(SeqNoAdvanced {
                vbucket: frame.vbucket,
                stream_id,
                seqno: read_u64(&frame.extras, 0),
            }))
        }
        Opcode::DCP_SYSTEM_EVENT => parse_system_event(frame, stream_id),
        Opcode::DCP_OSO_SNAPSHOT => {
            require_len("OSO snapshot extras", &frame.extras, 4)?;
            let raw = read_u32(&frame.extras, 0);
            let state = match raw {
                0 => OsoSnapshotState::Begin,
                1 => OsoSnapshotState::End,
                other => OsoSnapshotState::Unknown(other),
            };
            Ok(DcpMessage::OsoSnapshot(OsoSnapshot {
                vbucket: frame.vbucket,
                stream_id,
                state,
            }))
        }
        Opcode::DCP_NOOP => Ok(DcpMessage::Noop {
            opaque: frame.opaque,
        }),
        _ => Ok(DcpMessage::Unknown(frame.clone())),
    }
}

fn parse_mutation(frame: &Frame, stream_id: Option<u16>) -> Result<DcpMessage> {
    require_len("mutation extras", &frame.extras, 28)?;
    Ok(DcpMessage::Mutation(DcpMutation {
        vbucket: frame.vbucket,
        stream_id,
        seqno: read_u64(&frame.extras, 0),
        rev_seqno: read_u64(&frame.extras, 8),
        flags: read_u32(&frame.extras, 16),
        expiry: read_u32(&frame.extras, 20),
        lock_time: read_u32(&frame.extras, 24),
        cas: frame.cas,
        datatype: frame.datatype,
        collection_id: frame.collection_id,
        key: frame.key.clone(),
        value: frame.value.clone(),
    }))
}

fn parse_deletion(frame: &Frame, stream_id: Option<u16>) -> Result<DcpMessage> {
    require_len("deletion extras", &frame.extras, 16)?;
    Ok(DcpMessage::Deletion(DcpDeletion {
        vbucket: frame.vbucket,
        stream_id,
        seqno: read_u64(&frame.extras, 0),
        rev_seqno: read_u64(&frame.extras, 8),
        delete_time: (frame.extras.len() >= 20).then(|| read_u32(&frame.extras, 16)),
        cas: frame.cas,
        datatype: frame.datatype,
        collection_id: frame.collection_id,
        key: frame.key.clone(),
        value: frame.value.clone(),
    }))
}

fn parse_expiration(frame: &Frame, stream_id: Option<u16>) -> Result<DcpMessage> {
    require_len("expiration extras", &frame.extras, 16)?;
    Ok(DcpMessage::Expiration(DcpExpiration {
        vbucket: frame.vbucket,
        stream_id,
        seqno: read_u64(&frame.extras, 0),
        rev_seqno: read_u64(&frame.extras, 8),
        delete_time: (frame.extras.len() >= 20).then(|| read_u32(&frame.extras, 16)),
        cas: frame.cas,
        datatype: frame.datatype,
        collection_id: frame.collection_id,
        key: frame.key.clone(),
        value: frame.value.clone(),
    }))
}

fn parse_snapshot_marker(frame: &Frame, stream_id: Option<u16>) -> Result<DcpMessage> {
    let marker = if frame.extras.len() == 20 {
        SnapshotMarker {
            vbucket: frame.vbucket,
            stream_id,
            start_seqno: read_u64(&frame.extras, 0),
            end_seqno: read_u64(&frame.extras, 8),
            flags: read_u32(&frame.extras, 16),
            max_visible_seqno: None,
            high_completed_seqno: None,
            timestamp: None,
            purge_seqno: None,
        }
    } else if frame.extras.len() == 1 {
        require_len("v2 snapshot marker value", &frame.value, 36)?;
        let version = frame.extras[0];
        if version >= 1 {
            require_len("v2.1 snapshot marker value", &frame.value, 44)?;
        }
        if version >= 2 {
            require_len("v2.2 snapshot marker value", &frame.value, 52)?;
        }
        SnapshotMarker {
            vbucket: frame.vbucket,
            stream_id,
            start_seqno: read_u64(&frame.value, 0),
            end_seqno: read_u64(&frame.value, 8),
            flags: read_u32(&frame.value, 16),
            max_visible_seqno: Some(read_u64(&frame.value, 20)),
            high_completed_seqno: Some(read_u64(&frame.value, 28)),
            timestamp: (version >= 1).then(|| read_u64(&frame.value, 36)),
            purge_seqno: (version >= 2).then(|| read_u64(&frame.value, 44)),
        }
    } else {
        return Err(ProtocolError::MalformedDcp(format!(
            "snapshot marker extras must be 20 bytes (v1) or 1 byte (v2), got {}",
            frame.extras.len()
        )));
    };
    if marker.start_seqno > marker.end_seqno {
        return Err(ProtocolError::MalformedDcp(format!(
            "snapshot start {} exceeds end {}",
            marker.start_seqno, marker.end_seqno
        )));
    }
    Ok(DcpMessage::SnapshotMarker(marker))
}

fn parse_system_event(frame: &Frame, stream_id: Option<u16>) -> Result<DcpMessage> {
    require_len("system-event extras", &frame.extras, 13)?;
    let seqno = read_u64(&frame.extras, 0);
    let event_code = read_u32(&frame.extras, 8);
    let version = frame.extras[12];
    require_len("system-event value", &frame.value, 8)?;
    let manifest_uid = read_u64(&frame.value, 0);
    let kind = match event_code {
        0 => {
            require_len("collection-create value", &frame.value, 16)?;
            if version >= 1 {
                require_len("collection-create v1 value", &frame.value, 20)?;
            }
            SystemEventKind::CollectionCreated {
                scope_id: read_u32(&frame.value, 8),
                collection_id: read_u32(&frame.value, 12),
                max_ttl: (version >= 1).then(|| read_u32(&frame.value, 16)),
            }
        }
        1 => {
            require_len("collection-drop value", &frame.value, 16)?;
            SystemEventKind::CollectionDropped {
                scope_id: read_u32(&frame.value, 8),
                collection_id: read_u32(&frame.value, 12),
            }
        }
        2 => {
            require_len("collection-flush value", &frame.value, 12)?;
            SystemEventKind::CollectionFlushed {
                collection_id: read_u32(&frame.value, 8),
            }
        }
        3 => {
            require_len("scope-create value", &frame.value, 12)?;
            SystemEventKind::ScopeCreated {
                scope_id: read_u32(&frame.value, 8),
            }
        }
        4 => {
            require_len("scope-drop value", &frame.value, 12)?;
            SystemEventKind::ScopeDropped {
                scope_id: read_u32(&frame.value, 8),
            }
        }
        5 => {
            require_len("collection-change value", &frame.value, 16)?;
            SystemEventKind::CollectionChanged {
                collection_id: read_u32(&frame.value, 8),
                max_ttl: read_u32(&frame.value, 12),
            }
        }
        code => SystemEventKind::Unknown {
            code,
            data: frame.value.clone(),
        },
    };
    Ok(DcpMessage::SystemEvent(SystemEvent {
        vbucket: frame.vbucket,
        stream_id,
        seqno,
        version,
        manifest_uid,
        key: frame.key.clone(),
        kind,
    }))
}

fn require_len(label: &str, bytes: &[u8], minimum: usize) -> Result<()> {
    if bytes.len() < minimum {
        return Err(ProtocolError::MalformedDcp(format!(
            "{label} requires at least {minimum} bytes, got {}",
            bytes.len()
        )));
    }
    Ok(())
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("caller validated message length"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("caller validated message length"),
    )
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use super::*;

    #[test]
    fn mutation_decodes_all_fixed_fields() {
        let mut extras = BytesMut::new();
        extras.put_u64(100);
        extras.put_u64(7);
        extras.put_u32(0xdead_beef);
        extras.put_u32(3600);
        extras.put_u32(12);
        let mut frame = Frame::request(Opcode::DCP_MUTATION);
        frame.vbucket = 9;
        frame.cas = 88;
        frame.datatype = 0x03;
        frame.collection_id = Some(4);
        frame.extras = extras.freeze();
        frame.key = Bytes::from_static(b"key");
        frame.value = Bytes::from_static(b"value");

        let DcpMessage::Mutation(event) = parse_dcp_message(&frame).unwrap() else {
            panic!("expected mutation");
        };
        assert_eq!(event.vbucket, 9);
        assert_eq!(event.seqno, 100);
        assert_eq!(event.rev_seqno, 7);
        assert_eq!(event.flags, 0xdead_beef);
        assert_eq!(event.expiry, 3600);
        assert_eq!(event.lock_time, 12);
        assert_eq!(event.collection_id, Some(4));
    }

    #[test]
    fn snapshot_v1_and_v2_are_both_supported() {
        let mut v1_extras = BytesMut::new();
        v1_extras.put_u64(5);
        v1_extras.put_u64(10);
        v1_extras.put_u32(3);
        let mut v1 = Frame::request(Opcode::DCP_SNAPSHOT_MARKER);
        v1.extras = v1_extras.freeze();
        let DcpMessage::SnapshotMarker(v1_marker) = parse_dcp_message(&v1).unwrap() else {
            panic!("expected marker");
        };
        assert_eq!(v1_marker.start_seqno, 5);
        assert_eq!(v1_marker.max_visible_seqno, None);

        let mut value = BytesMut::new();
        value.put_u64(6);
        value.put_u64(12);
        value.put_u32(0x10);
        value.put_u64(11);
        value.put_u64(9);
        value.put_u64(1234);
        let mut v2 = Frame::request(Opcode::DCP_SNAPSHOT_MARKER);
        v2.extras = Bytes::from_static(&[1]);
        v2.value = value.freeze();
        let DcpMessage::SnapshotMarker(v2_marker) = parse_dcp_message(&v2).unwrap() else {
            panic!("expected marker");
        };
        assert_eq!(v2_marker.max_visible_seqno, Some(11));
        assert_eq!(v2_marker.high_completed_seqno, Some(9));
        assert_eq!(v2_marker.timestamp, Some(1234));
    }

    #[test]
    fn collection_create_system_event_decodes_manifest_and_ttl() {
        let mut extras = BytesMut::new();
        extras.put_u64(44);
        extras.put_u32(0);
        extras.put_u8(1);
        let mut value = BytesMut::new();
        value.put_u64(0xaa);
        value.put_u32(8);
        value.put_u32(9);
        value.put_u32(3600);
        let mut frame = Frame::request(Opcode::DCP_SYSTEM_EVENT);
        frame.vbucket = 3;
        frame.extras = extras.freeze();
        frame.key = Bytes::from_static(b"airline");
        frame.value = value.freeze();

        let DcpMessage::SystemEvent(event) = parse_dcp_message(&frame).unwrap() else {
            panic!("expected system event");
        };
        assert_eq!(event.manifest_uid, 0xaa);
        assert_eq!(
            event.kind,
            SystemEventKind::CollectionCreated {
                scope_id: 8,
                collection_id: 9,
                max_ttl: Some(3600)
            }
        );
    }

    #[test]
    fn truncated_known_event_is_rejected_without_panicking() {
        let mut frame = Frame::request(Opcode::DCP_DELETION);
        frame.extras = Bytes::from_static(&[0; 8]);

        assert!(parse_dcp_message(&frame).is_err());
    }

    #[test]
    fn noop_exposes_opaque_for_reply() {
        let mut frame = Frame::request(Opcode::DCP_NOOP);
        frame.opaque = 0x1234_5678;

        assert_eq!(
            parse_dcp_message(&frame).unwrap(),
            DcpMessage::Noop {
                opaque: 0x1234_5678
            }
        );
    }
}
