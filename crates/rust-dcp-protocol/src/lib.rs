//! Wire-level primitives for the Couchbase DCP protocol.
//!
//! This crate deliberately has no networking or checkpoint responsibilities.

#![forbid(unsafe_code)]

mod codec;
mod command;
mod dcp;
mod error;
mod frame;
mod types;
mod uleb128;

pub use codec::FrameCodec;
pub use command::{
    CollectionId, DcpOpenFlags, DcpStreamFlags, DocumentStoreMode, DocumentStoreRequest,
    HelloFeature, StreamFilter, StreamRequest, StreamRequestResponse, VBucketSeqNo, VBucketState,
    buffer_ack, close_stream, dcp_control, dcp_open, delete_document, get_cluster_config,
    get_collection_id, get_collection_manifest, get_document, get_failover_log, get_vbucket_seqnos,
    hello, noop_response, parse_collection_id, parse_collection_manifest, parse_failover_log,
    parse_stream_request_response, parse_vbucket_seqnos, sasl_auth, sasl_list_mechanisms,
    sasl_step, select_bucket, snapshot_marker_response, store_document, stream_request,
};
pub use dcp::{
    DcpDeletion, DcpExpiration, DcpMessage, DcpMutation, OsoSnapshot, OsoSnapshotState,
    SeqNoAdvanced, SnapshotMarker, StreamEnd, StreamEndReason, SystemEvent, SystemEventKind,
    parse_dcp_message,
};
pub use error::{ProtocolError, Result};
pub use frame::{Frame, FramingExtra};
pub use types::{FailoverEntry, Magic, Opcode, Status};
pub use uleb128::{decode_uleb128_u32, encode_uleb128_u32};

/// Size of a classic Memcached binary protocol header.
pub const HEADER_LEN: usize = 24;
