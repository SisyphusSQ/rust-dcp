use std::{
    collections::BTreeSet,
    ops::{BitOr, BitOrAssign},
};

use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;

use crate::{FailoverEntry, Frame, FramingExtra, Opcode, ProtocolError, Result, Status};

/// Features advertised through the Memcached `HELLO` command.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u16)]
pub enum HelloFeature {
    /// Datatype byte support.
    Datatype = 0x01,
    /// TLS support.
    Tls = 0x02,
    /// TCP no-delay support.
    TcpNoDelay = 0x03,
    /// Mutation sequence numbers.
    SeqNo = 0x04,
    /// XATTR values.
    Xattr = 0x06,
    /// Extended errors.
    ExtendedErrors = 0x07,
    /// Bucket selection command.
    SelectBucket = 0x08,
    /// Snappy compression.
    Snappy = 0x0a,
    /// JSON datatype.
    Json = 0x0b,
    /// Duplex/server-request support.
    Duplex = 0x0c,
    /// Cluster-map notifications.
    ClusterMapNotifications = 0x0d,
    /// Flexible framing extras.
    AltRequest = 0x10,
    /// Collections and scopes.
    Collections = 0x12,
    /// Snappy-compressed configs and documents.
    SnappyEverywhere = 0x13,
    /// Point-in-time-recovery snapshots.
    Pitr = 0x16,
}

impl HelloFeature {
    /// Raw feature code.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// Converts a supported wire value into a typed feature.
    #[must_use]
    pub const fn from_u16(value: u16) -> Option<Self> {
        match value {
            0x01 => Some(Self::Datatype),
            0x02 => Some(Self::Tls),
            0x03 => Some(Self::TcpNoDelay),
            0x04 => Some(Self::SeqNo),
            0x06 => Some(Self::Xattr),
            0x07 => Some(Self::ExtendedErrors),
            0x08 => Some(Self::SelectBucket),
            0x0a => Some(Self::Snappy),
            0x0b => Some(Self::Json),
            0x0c => Some(Self::Duplex),
            0x0d => Some(Self::ClusterMapNotifications),
            0x10 => Some(Self::AltRequest),
            0x12 => Some(Self::Collections),
            0x13 => Some(Self::SnappyEverywhere),
            0x16 => Some(Self::Pitr),
            _ => None,
        }
    }
}

/// Flags sent while opening a DCP connection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DcpOpenFlags(u32);

impl DcpOpenFlags {
    /// Ask the server to act as the producer.
    pub const PRODUCER: Self = Self(0x01);
    /// Open a notification-only connection.
    pub const NOTIFIER: Self = Self(0x02);
    /// Include document XATTRs.
    pub const INCLUDE_XATTRS: Self = Self(0x04);
    /// Omit document values.
    pub const NO_VALUE: Self = Self(0x08);
    /// Include tombstone delete times.
    pub const INCLUDE_DELETE_TIMES: Self = Self(0x20);
    /// Request PITR/history snapshots.
    pub const PITR: Self = Self(0x80);

    /// Raw flag bits.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }
}

impl BitOr for DcpOpenFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for DcpOpenFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// Flags sent with one vBucket stream request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DcpStreamFlags(u32);

impl DcpStreamFlags {
    /// Transfer vBucket ownership to the consumer.
    pub const TAKEOVER: Self = Self(0x01);
    /// Read only on-disk items.
    pub const DISK_ONLY: Self = Self(0x02);
    /// Use the latest sequence number as the stream boundary.
    pub const LATEST: Self = Self(0x04);
    /// Deprecated per-stream no-value mode.
    pub const NO_VALUE: Self = Self(0x08);
    /// Connect only to an active vBucket.
    pub const ACTIVE_ONLY: Self = Self(0x10);
    /// Require an exact vBucket UUID match.
    pub const STRICT_VBUUID: Self = Self(0x20);
    /// Ask the producer to start at its current high sequence number.
    pub const FROM_LATEST: Self = Self(0x40);
    /// Allow gaps caused only by already-purged tombstones.
    pub const IGNORE_PURGED_TOMBSTONES: Self = Self(0x80);
    /// Transfer eligible resident values before regular streaming.
    pub const CACHE_TRANSFER: Self = Self(0x100);

    /// Raw flag bits.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }
}

impl BitOr for DcpStreamFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for DcpStreamFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// vBucket state filter for `GET_ALL_VB_SEQNOS`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum VBucketState {
    /// Active vBuckets.
    Active = 0x01,
    /// Replica vBuckets.
    Replica = 0x02,
    /// Pending vBuckets.
    Pending = 0x03,
    /// Dead vBuckets.
    Dead = 0x04,
}

/// Optional DCP stream filter encoded as JSON.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StreamFilter {
    /// Select an entire scope when no collection IDs are supplied.
    pub scope_id: Option<u32>,
    /// Select one or more collection IDs.
    pub collection_ids: Vec<u32>,
    /// Manifest UID used to guard collection recreation.
    pub manifest_uid: Option<u64>,
    /// DCP stream ID for multiplexed streams.
    pub stream_id: Option<u16>,
}

impl StreamFilter {
    fn encode(&self) -> Result<Bytes> {
        #[derive(Serialize)]
        struct WireFilter {
            #[serde(skip_serializing_if = "Option::is_none")]
            uid: Option<String>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            collections: Vec<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            scope: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            sid: Option<u16>,
        }

        if self.scope_id.is_some() && !self.collection_ids.is_empty() {
            return Err(ProtocolError::InvalidRequest(
                "stream filter cannot select both a scope and collection IDs".into(),
            ));
        }
        if self
            .collection_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != self.collection_ids.len()
        {
            return Err(ProtocolError::InvalidRequest(
                "stream filter collection IDs must be unique".into(),
            ));
        }
        if self.stream_id == Some(0) {
            return Err(ProtocolError::InvalidRequest(
                "DCP stream ID must be non-zero".into(),
            ));
        }

        let wire = WireFilter {
            uid: self.manifest_uid.map(|value| format!("{value:x}")),
            collections: self
                .collection_ids
                .iter()
                .map(|value| format!("{value:x}"))
                .collect(),
            scope: if self.collection_ids.is_empty() {
                self.scope_id.map(|value| format!("{value:x}"))
            } else {
                None
            },
            sid: self.stream_id,
        };
        Ok(Bytes::from(serde_json::to_vec(&wire)?))
    }
}

/// Parameters for one DCP stream request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamRequest {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Stream flags.
    pub flags: DcpStreamFlags,
    /// Failover branch UUID.
    pub vbucket_uuid: u64,
    /// First sequence number requested.
    pub start_seqno: u64,
    /// Last sequence number requested (`u64::MAX` for infinite mode).
    pub end_seqno: u64,
    /// Snapshot start stored in the checkpoint.
    pub snapshot_start: u64,
    /// Snapshot end stored in the checkpoint.
    pub snapshot_end: u64,
    /// Optional collection, manifest, and stream-ID filter.
    pub filter: Option<StreamFilter>,
    /// Correlation token.
    pub opaque: u32,
}

/// Parsed response to a DCP stream request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamRequestResponse {
    /// Stream opened; response value contains the failover log.
    Opened(Vec<FailoverEntry>),
    /// Stream must be reopened from this sequence number.
    Rollback(u64),
}

/// One vBucket high sequence number.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VBucketSeqNo {
    /// vBucket identifier.
    pub vbucket: u16,
    /// Current high sequence number.
    pub seqno: u64,
}

/// Identifiers returned by `COLLECTIONS_GET_ID`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectionId {
    /// Manifest containing the resolved collection.
    pub manifest_uid: u64,
    /// Collection identifier used by DCP filters and key prefixes.
    pub collection_id: u32,
}

/// Builds a `HELLO` feature-negotiation request.
#[must_use]
pub fn hello(client_name: &str, features: &[HelloFeature], opaque: u32) -> Frame {
    let mut value = BytesMut::with_capacity(features.len() * 2);
    for feature in features {
        value.put_u16(feature.as_u16());
    }
    let mut frame = Frame::request(Opcode::HELLO);
    frame.key = Bytes::copy_from_slice(client_name.as_bytes());
    frame.value = value.freeze();
    frame.opaque = opaque;
    frame
}

/// Builds a SASL mechanism-list request.
#[must_use]
pub fn sasl_list_mechanisms(opaque: u32) -> Frame {
    let mut frame = Frame::request(Opcode::SASL_LIST_MECHS);
    frame.opaque = opaque;
    frame
}

/// Builds the first SASL authentication request.
#[must_use]
pub fn sasl_auth(mechanism: &str, payload: impl Into<Bytes>, opaque: u32) -> Frame {
    let mut frame = Frame::request(Opcode::SASL_AUTH);
    frame.key = Bytes::copy_from_slice(mechanism.as_bytes());
    frame.value = payload.into();
    frame.opaque = opaque;
    frame
}

/// Builds a subsequent SASL authentication step.
#[must_use]
pub fn sasl_step(mechanism: &str, payload: impl Into<Bytes>, opaque: u32) -> Frame {
    let mut frame = Frame::request(Opcode::SASL_STEP);
    frame.key = Bytes::copy_from_slice(mechanism.as_bytes());
    frame.value = payload.into();
    frame.opaque = opaque;
    frame
}

/// Builds a bucket-selection request.
#[must_use]
pub fn select_bucket(bucket: &str, opaque: u32) -> Frame {
    let mut frame = Frame::request(Opcode::SELECT_BUCKET);
    frame.key = Bytes::copy_from_slice(bucket.as_bytes());
    frame.opaque = opaque;
    frame
}

/// Builds a cluster-configuration request.
#[must_use]
pub fn get_cluster_config(opaque: u32) -> Frame {
    let mut frame = Frame::request(Opcode::GET_CLUSTER_CONFIG);
    frame.opaque = opaque;
    frame
}

/// Builds a request for the current collection manifest.
#[must_use]
pub fn get_collection_manifest(opaque: u32) -> Frame {
    let mut frame = Frame::request(Opcode::COLLECTIONS_GET_MANIFEST);
    frame.opaque = opaque;
    frame
}

/// Builds a request that resolves one `scope.collection` name.
///
/// # Errors
///
/// Returns a protocol error when the name cannot be represented by this wire
/// command.
pub fn get_collection_id(scope: &str, collection: &str, opaque: u32) -> Result<Frame> {
    validate_collection_name("scope", scope)?;
    validate_collection_name("collection", collection)?;
    let mut frame = Frame::request(Opcode::COLLECTIONS_GET_ID);
    frame.value = Bytes::from(format!("{scope}.{collection}"));
    frame.opaque = opaque;
    Ok(frame)
}

/// Builds a DCP connection-open request.
#[must_use]
pub fn dcp_open(name: &str, flags: DcpOpenFlags, opaque: u32) -> Frame {
    let mut extras = BytesMut::with_capacity(8);
    extras.put_u32(0);
    extras.put_u32((flags | DcpOpenFlags::PRODUCER).bits());
    let mut frame = Frame::request(Opcode::DCP_OPEN);
    frame.key = Bytes::copy_from_slice(name.as_bytes());
    frame.extras = extras.freeze();
    frame.opaque = opaque;
    frame
}

/// Builds one DCP control request.
#[must_use]
pub fn dcp_control(key: &str, value: &str, opaque: u32) -> Frame {
    let mut frame = Frame::request(Opcode::DCP_CONTROL);
    frame.key = Bytes::copy_from_slice(key.as_bytes());
    frame.value = Bytes::copy_from_slice(value.as_bytes());
    frame.opaque = opaque;
    frame
}

/// Builds a request for high sequence numbers.
#[must_use]
pub fn get_vbucket_seqnos(state: VBucketState, collection_id: Option<u32>, opaque: u32) -> Frame {
    let mut extras = BytesMut::with_capacity(if collection_id.is_some() { 8 } else { 4 });
    extras.put_u32(state as u32);
    if let Some(collection_id) = collection_id {
        extras.put_u32(collection_id);
    }
    let mut frame = Frame::request(Opcode::GET_ALL_VB_SEQNOS);
    frame.extras = extras.freeze();
    frame.opaque = opaque;
    frame
}

/// Builds a failover-log request for one vBucket.
#[must_use]
pub fn get_failover_log(vbucket: u16, opaque: u32) -> Frame {
    let mut frame = Frame::request(Opcode::DCP_GET_FAILOVER_LOG);
    frame.vbucket = vbucket;
    frame.opaque = opaque;
    frame
}

/// Builds one vBucket stream request.
///
/// # Errors
///
/// Returns [`ProtocolError::InvalidRequest`] for inconsistent sequence or
/// snapshot bounds, or a JSON error if the optional filter cannot serialize.
pub fn stream_request(request: &StreamRequest) -> Result<Frame> {
    if request.start_seqno > request.end_seqno {
        return Err(ProtocolError::InvalidRequest(format!(
            "stream start {} exceeds end {}",
            request.start_seqno, request.end_seqno
        )));
    }
    if request.snapshot_start > request.snapshot_end {
        return Err(ProtocolError::InvalidRequest(format!(
            "snapshot start {} exceeds end {}",
            request.snapshot_start, request.snapshot_end
        )));
    }
    if request.start_seqno > request.snapshot_end && request.snapshot_end != 0 {
        return Err(ProtocolError::InvalidRequest(
            "stream start lies beyond checkpoint snapshot".into(),
        ));
    }

    let mut extras = BytesMut::with_capacity(48);
    extras.put_u32(request.flags.bits());
    extras.put_u32(0);
    extras.put_u64(request.start_seqno);
    extras.put_u64(request.end_seqno);
    extras.put_u64(request.vbucket_uuid);
    extras.put_u64(request.snapshot_start);
    extras.put_u64(request.snapshot_end);
    let mut frame = Frame::request(Opcode::DCP_STREAM_REQUEST);
    frame.vbucket = request.vbucket;
    frame.opaque = request.opaque;
    frame.extras = extras.freeze();
    if let Some(filter) = &request.filter {
        frame.value = filter.encode()?;
    }
    Ok(frame)
}

/// Builds a DCP stream-close request.
#[must_use]
pub fn close_stream(vbucket: u16, stream_id: Option<u16>, opaque: u32) -> Frame {
    let mut frame = Frame::request(Opcode::DCP_CLOSE_STREAM);
    frame.vbucket = vbucket;
    frame.opaque = opaque;
    if let Some(stream_id) = stream_id {
        frame
            .framing_extras
            .push(FramingExtra::stream_id(stream_id));
    }
    frame
}

/// Builds a DCP buffer-acknowledgement request.
#[must_use]
pub fn buffer_ack(bytes: u32, opaque: u32) -> Frame {
    let mut extras = BytesMut::with_capacity(4);
    extras.put_u32(bytes);
    let mut frame = Frame::request(Opcode::DCP_BUFFER_ACK);
    frame.extras = extras.freeze();
    frame.opaque = opaque;
    frame
}

/// Builds the response to a producer's DCP NOOP request.
#[must_use]
pub fn noop_response(opaque: u32) -> Frame {
    let mut frame = Frame::response(Opcode::DCP_NOOP, Status::SUCCESS);
    frame.opaque = opaque;
    frame
}

/// Builds a success response for a snapshot marker carrying the ACK flag.
#[must_use]
pub fn snapshot_marker_response(opaque: u32) -> Frame {
    let mut frame = Frame::response(Opcode::DCP_SNAPSHOT_MARKER, Status::SUCCESS);
    frame.opaque = opaque;
    frame
}

/// Parses a successful failover-log response.
///
/// # Errors
///
/// Returns a protocol error for the wrong opcode, a non-success status, or a
/// value whose length is not a multiple of 16.
pub fn parse_failover_log(frame: &Frame) -> Result<Vec<FailoverEntry>> {
    ensure_response(frame, Opcode::DCP_GET_FAILOVER_LOG)?;
    parse_failover_entries(&frame.value)
}

/// Parses a DCP stream-request response, including rollback status.
///
/// # Errors
///
/// Returns a protocol error for the wrong opcode, malformed payload, or an
/// unexpected non-success status.
pub fn parse_stream_request_response(frame: &Frame) -> Result<StreamRequestResponse> {
    ensure_response_opcode(frame, Opcode::DCP_STREAM_REQUEST)?;
    if frame.status == Status::ROLLBACK {
        if frame.value.len() != 8 {
            return Err(ProtocolError::MalformedDcp(format!(
                "rollback response must contain 8 bytes, got {}",
                frame.value.len()
            )));
        }
        return Ok(StreamRequestResponse::Rollback(u64::from_be_bytes([
            frame.value[0],
            frame.value[1],
            frame.value[2],
            frame.value[3],
            frame.value[4],
            frame.value[5],
            frame.value[6],
            frame.value[7],
        ])));
    }
    ensure_success(frame)?;
    Ok(StreamRequestResponse::Opened(parse_failover_entries(
        &frame.value,
    )?))
}

/// Parses a successful high-sequence-number response.
///
/// # Errors
///
/// Returns a protocol error for the wrong opcode, a non-success status, or a
/// value whose length is not a multiple of 10.
pub fn parse_vbucket_seqnos(frame: &Frame) -> Result<Vec<VBucketSeqNo>> {
    ensure_response(frame, Opcode::GET_ALL_VB_SEQNOS)?;
    if frame.value.len() % 10 != 0 {
        return Err(ProtocolError::MalformedDcp(format!(
            "vBucket seqno response length {} is not divisible by 10",
            frame.value.len()
        )));
    }
    Ok(frame
        .value
        .chunks_exact(10)
        .map(|chunk| VBucketSeqNo {
            vbucket: u16::from_be_bytes([chunk[0], chunk[1]]),
            seqno: u64::from_be_bytes([
                chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7], chunk[8], chunk[9],
            ]),
        })
        .collect())
}

/// Parses a successful collection-ID response.
///
/// # Errors
///
/// Returns a protocol error for the wrong opcode, a non-success status, or
/// response extras whose length is not exactly 12 bytes.
pub fn parse_collection_id(frame: &Frame) -> Result<CollectionId> {
    ensure_response(frame, Opcode::COLLECTIONS_GET_ID)?;
    if frame.extras.len() != 12 {
        return Err(ProtocolError::MalformedFrame(format!(
            "collection-ID response extras must contain 12 bytes, got {}",
            frame.extras.len()
        )));
    }
    let manifest_uid = frame
        .extras
        .get(..8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_be_bytes)
        .ok_or_else(|| {
            ProtocolError::MalformedFrame("collection-ID manifest UID is malformed".into())
        })?;
    let collection_id = frame
        .extras
        .get(8..12)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_be_bytes)
        .ok_or_else(|| {
            ProtocolError::MalformedFrame("collection-ID numeric ID is malformed".into())
        })?;
    Ok(CollectionId {
        manifest_uid,
        collection_id,
    })
}

/// Returns the JSON payload from a successful collection-manifest response.
///
/// # Errors
///
/// Returns a protocol error for the wrong opcode or a non-success status.
pub fn parse_collection_manifest(frame: &Frame) -> Result<&[u8]> {
    ensure_response(frame, Opcode::COLLECTIONS_GET_MANIFEST)?;
    Ok(&frame.value)
}

fn parse_failover_entries(value: &[u8]) -> Result<Vec<FailoverEntry>> {
    if value.len() % 16 != 0 {
        return Err(ProtocolError::MalformedDcp(format!(
            "failover log length {} is not divisible by 16",
            value.len()
        )));
    }
    Ok(value
        .chunks_exact(16)
        .map(|chunk| FailoverEntry {
            vbucket_uuid: u64::from_be_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
            ]),
            seqno: u64::from_be_bytes([
                chunk[8], chunk[9], chunk[10], chunk[11], chunk[12], chunk[13], chunk[14],
                chunk[15],
            ]),
        })
        .collect())
}

fn validate_collection_name(kind: &str, name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 251 {
        return Err(ProtocolError::InvalidRequest(format!(
            "{kind} name must contain between 1 and 251 ASCII characters"
        )));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'%'))
    {
        return Err(ProtocolError::InvalidRequest(format!(
            "{kind} name contains a character unsupported by Couchbase collections"
        )));
    }
    Ok(())
}

fn ensure_response(frame: &Frame, opcode: Opcode) -> Result<()> {
    ensure_response_opcode(frame, opcode)?;
    ensure_success(frame)
}

fn ensure_response_opcode(frame: &Frame, opcode: Opcode) -> Result<()> {
    if !frame.magic.is_response() || frame.opcode != opcode {
        return Err(ProtocolError::MalformedFrame(format!(
            "expected response opcode 0x{:02x}, got {:?} opcode 0x{:02x}",
            opcode.as_u8(),
            frame.magic,
            frame.opcode.as_u8()
        )));
    }
    Ok(())
}

fn ensure_success(frame: &Frame) -> Result<()> {
    if frame.status.is_success() {
        return Ok(());
    }
    Err(ProtocolError::ServerStatus {
        status: frame.status.as_u16(),
        opcode: frame.opcode.as_u8(),
        message: String::from_utf8_lossy(&frame.value).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Magic;

    #[test]
    fn open_connection_sets_producer_and_requested_flags() {
        let frame = dcp_open("rust-dcp", DcpOpenFlags::INCLUDE_XATTRS, 7);

        assert_eq!(&frame.extras[..4], &[0, 0, 0, 0]);
        assert_eq!(
            u32::from_be_bytes(frame.extras[4..8].try_into().unwrap()),
            0x05
        );
        assert_eq!(frame.opaque, 7);
    }

    #[test]
    fn stream_request_has_exact_wire_extras_and_hex_filter() {
        let frame = stream_request(&StreamRequest {
            vbucket: 12,
            flags: DcpStreamFlags::ACTIVE_ONLY | DcpStreamFlags::STRICT_VBUUID,
            vbucket_uuid: 77,
            start_seqno: 10,
            end_seqno: 99,
            snapshot_start: 8,
            snapshot_end: 20,
            filter: Some(StreamFilter {
                collection_ids: vec![0x8, 0xcafe],
                manifest_uid: Some(0xff),
                stream_id: Some(3),
                ..StreamFilter::default()
            }),
            opaque: 42,
        })
        .expect("valid request");

        assert_eq!(frame.extras.len(), 48);
        assert_eq!(
            u32::from_be_bytes(frame.extras[..4].try_into().unwrap()),
            0x30
        );
        assert_eq!(
            u64::from_be_bytes(frame.extras[8..16].try_into().unwrap()),
            10
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&frame.value).unwrap(),
            serde_json::json!({"uid":"ff","collections":["8","cafe"],"sid":3})
        );
    }

    #[test]
    fn stream_request_rejects_inverted_bounds() {
        let result = stream_request(&StreamRequest {
            vbucket: 0,
            flags: DcpStreamFlags::default(),
            vbucket_uuid: 0,
            start_seqno: 2,
            end_seqno: 1,
            snapshot_start: 0,
            snapshot_end: 0,
            filter: None,
            opaque: 0,
        });

        assert!(result.is_err());
    }

    #[test]
    fn stream_request_rejects_ambiguous_collection_filters() {
        let request_with = |filter| StreamRequest {
            vbucket: 0,
            flags: DcpStreamFlags::default(),
            vbucket_uuid: 0,
            start_seqno: 0,
            end_seqno: u64::MAX,
            snapshot_start: 0,
            snapshot_end: 0,
            filter: Some(filter),
            opaque: 0,
        };

        assert!(
            stream_request(&request_with(StreamFilter {
                scope_id: Some(8),
                collection_ids: vec![9],
                ..StreamFilter::default()
            }))
            .is_err()
        );
        assert!(
            stream_request(&request_with(StreamFilter {
                collection_ids: vec![9, 9],
                ..StreamFilter::default()
            }))
            .is_err()
        );
        assert!(
            stream_request(&request_with(StreamFilter {
                stream_id: Some(0),
                ..StreamFilter::default()
            }))
            .is_err()
        );
    }

    #[test]
    fn rollback_response_is_not_treated_as_generic_failure() {
        let mut frame = Frame::response(Opcode::DCP_STREAM_REQUEST, Status::ROLLBACK);
        frame.value = Bytes::copy_from_slice(&123_u64.to_be_bytes());

        assert_eq!(
            parse_stream_request_response(&frame).expect("rollback response"),
            StreamRequestResponse::Rollback(123)
        );
    }

    #[test]
    fn failover_log_and_vbucket_seqnos_parse_big_endian_values() {
        let mut failover = Frame::response(Opcode::DCP_GET_FAILOVER_LOG, Status::SUCCESS);
        let mut failover_value = BytesMut::new();
        failover_value.put_u64(77);
        failover_value.put_u64(12);
        failover.value = failover_value.freeze();
        assert_eq!(
            parse_failover_log(&failover).unwrap(),
            vec![FailoverEntry {
                vbucket_uuid: 77,
                seqno: 12
            }]
        );

        let mut seqnos = Frame::response(Opcode::GET_ALL_VB_SEQNOS, Status::SUCCESS);
        let mut value = BytesMut::new();
        value.put_u16(9);
        value.put_u64(999);
        seqnos.value = value.freeze();
        assert_eq!(
            parse_vbucket_seqnos(&seqnos).unwrap(),
            vec![VBucketSeqNo {
                vbucket: 9,
                seqno: 999
            }]
        );
    }

    #[test]
    fn collection_metadata_requests_match_the_memcached_wire_contract() {
        let manifest = get_collection_manifest(17);
        assert_eq!(manifest.opcode, Opcode::COLLECTIONS_GET_MANIFEST);
        assert!(manifest.key.is_empty());
        assert!(manifest.value.is_empty());
        assert_eq!(manifest.opaque, 17);

        let collection = get_collection_id("inventory", "airline", 18).unwrap();
        assert_eq!(collection.opcode, Opcode::COLLECTIONS_GET_ID);
        assert!(collection.key.is_empty());
        assert_eq!(&collection.value[..], b"inventory.airline");
        assert_eq!(collection.opaque, 18);
    }

    #[test]
    fn collection_id_request_rejects_unrepresentable_names() {
        assert!(get_collection_id("_default", "_default", 0).is_ok());
        assert!(get_collection_id("", "airline", 0).is_err());
        assert!(get_collection_id("inventory", "air.line", 0).is_err());
        assert!(get_collection_id("inventory", "white space", 0).is_err());
        assert!(get_collection_id("inventory", &"a".repeat(252), 0).is_err());
    }

    #[test]
    fn collection_id_response_requires_exact_big_endian_extras() {
        let mut response = Frame::response(Opcode::COLLECTIONS_GET_ID, Status::SUCCESS);
        let mut extras = BytesMut::new();
        extras.put_u64(0xfeed_beef);
        extras.put_u32(0xcafe);
        response.extras = extras.freeze();

        assert_eq!(
            parse_collection_id(&response).unwrap(),
            CollectionId {
                manifest_uid: 0xfeed_beef,
                collection_id: 0xcafe,
            }
        );

        response.extras = Bytes::from_static(&[0; 11]);
        assert!(parse_collection_id(&response).is_err());
    }

    #[test]
    fn noop_response_preserves_producer_opaque() {
        let frame = noop_response(0xdead_beef);
        assert_eq!(frame.magic, Magic::Response);
        assert_eq!(frame.opcode, Opcode::DCP_NOOP);
        assert_eq!(frame.opaque, 0xdead_beef);
    }

    #[test]
    fn snapshot_marker_response_preserves_producer_opaque() {
        let frame = snapshot_marker_response(0x1234_5678);
        assert_eq!(frame.magic, Magic::Response);
        assert_eq!(frame.opcode, Opcode::DCP_SNAPSHOT_MARKER);
        assert_eq!(frame.status, Status::SUCCESS);
        assert_eq!(frame.opaque, 0x1234_5678);
    }

    #[test]
    fn stream_flags_match_memcached_protocol_values() {
        assert_eq!(DcpStreamFlags::TAKEOVER.bits(), 0x01);
        assert_eq!(DcpStreamFlags::DISK_ONLY.bits(), 0x02);
        assert_eq!(DcpStreamFlags::LATEST.bits(), 0x04);
        assert_eq!(DcpStreamFlags::NO_VALUE.bits(), 0x08);
        assert_eq!(DcpStreamFlags::ACTIVE_ONLY.bits(), 0x10);
        assert_eq!(DcpStreamFlags::STRICT_VBUUID.bits(), 0x20);
        assert_eq!(DcpStreamFlags::FROM_LATEST.bits(), 0x40);
        assert_eq!(DcpStreamFlags::IGNORE_PURGED_TOMBSTONES.bits(), 0x80);
        assert_eq!(DcpStreamFlags::CACHE_TRANSFER.bits(), 0x100);
    }
}
