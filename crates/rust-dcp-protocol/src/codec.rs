use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    Frame, FramingExtra, HEADER_LEN, Magic, ProtocolError, Result, Status, decode_uleb128_u32,
    encode_uleb128_u32,
};

const DEFAULT_MAX_FRAME_SIZE: usize = 32 * 1024 * 1024;

/// Incremental codec for classic and flexible Memcached binary frames.
#[derive(Clone, Debug)]
pub struct FrameCodec {
    max_frame_size: usize,
    collections_enabled: bool,
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            collections_enabled: false,
        }
    }
}

impl FrameCodec {
    /// Creates a codec with an explicit maximum total frame size.
    #[must_use]
    pub const fn new(max_frame_size: usize) -> Self {
        Self {
            max_frame_size,
            collections_enabled: false,
        }
    }

    /// Enables or disables collection-ID key prefixes.
    #[must_use]
    pub const fn with_collections(mut self, enabled: bool) -> Self {
        self.collections_enabled = enabled;
        self
    }

    /// Current collection-prefix mode.
    #[must_use]
    pub const fn collections_enabled(&self) -> bool {
        self.collections_enabled
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = ProtocolError;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>> {
        if source.len() < HEADER_LEN {
            return Ok(None);
        }

        let magic = Magic::try_from(source[0])?;
        let body_len = usize::try_from(u32::from_be_bytes([
            source[8], source[9], source[10], source[11],
        ]))
        .map_err(|_| ProtocolError::InvalidLength("body length does not fit usize".into()))?;
        let total_len = HEADER_LEN
            .checked_add(body_len)
            .ok_or_else(|| ProtocolError::InvalidLength("frame length overflows usize".into()))?;
        if total_len > self.max_frame_size {
            return Err(ProtocolError::InvalidLength(format!(
                "frame size {total_len} exceeds configured maximum {}",
                self.max_frame_size
            )));
        }
        if source.len() < total_len {
            source.reserve(total_len - source.len());
            return Ok(None);
        }

        let packet = source.split_to(total_len).freeze();
        let (framing_len, key_len) = if magic.is_alt() {
            (usize::from(packet[2]), usize::from(packet[3]))
        } else {
            (0, usize::from(u16::from_be_bytes([packet[2], packet[3]])))
        };
        let extras_len = usize::from(packet[4]);
        let fields_len = framing_len
            .checked_add(extras_len)
            .and_then(|length| length.checked_add(key_len))
            .ok_or_else(|| ProtocolError::InvalidLength("body fields overflow usize".into()))?;
        if fields_len > body_len {
            return Err(ProtocolError::MalformedFrame(format!(
                "framing ({framing_len}) + extras ({extras_len}) + key ({key_len}) exceeds body ({body_len})"
            )));
        }

        let body = packet.slice(HEADER_LEN..);
        let framing_extras = decode_framing_extras(body.slice(..framing_len))?;
        let extras_start = framing_len;
        let key_start = extras_start + extras_len;
        let value_start = key_start + key_len;
        let extras = body.slice(extras_start..key_start);
        let mut key = body.slice(key_start..value_start);
        let value = body.slice(value_start..);
        let mut collection_id = None;
        let opcode = crate::Opcode(packet[1]);
        if self.collections_enabled && opcode.is_collection_encoded() && !key.is_empty() {
            let (decoded, consumed) = decode_uleb128_u32(&key)?;
            key = key.slice(consumed..);
            collection_id = Some(decoded);
        }

        let status_or_vbucket = u16::from_be_bytes([packet[6], packet[7]]);
        let mut frame = Frame {
            magic,
            opcode,
            datatype: packet[5],
            vbucket: if magic.is_request() {
                status_or_vbucket
            } else {
                0
            },
            status: if magic.is_response() {
                Status(status_or_vbucket)
            } else {
                Status::SUCCESS
            },
            opaque: u32::from_be_bytes([packet[12], packet[13], packet[14], packet[15]]),
            cas: u64::from_be_bytes([
                packet[16], packet[17], packet[18], packet[19], packet[20], packet[21], packet[22],
                packet[23],
            ]),
            framing_extras,
            extras,
            key,
            value,
            collection_id,
            received_wire_size: None,
        };
        frame.mark_received(total_len);
        Ok(Some(frame))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = ProtocolError;

    fn encode(&mut self, frame: Frame, destination: &mut BytesMut) -> Result<()> {
        let framing = encode_framing_extras(&frame.framing_extras)?;
        let magic = encoded_magic(frame.magic, !framing.is_empty())?;
        if frame.extras.len() > usize::from(u8::MAX) {
            return Err(ProtocolError::InvalidLength(format!(
                "extras length {} exceeds 255",
                frame.extras.len()
            )));
        }

        let mut key = BytesMut::new();
        if self.collections_enabled && frame.opcode.is_collection_encoded() {
            encode_uleb128_u32(frame.collection_id.unwrap_or(0), &mut key);
        } else if frame
            .collection_id
            .is_some_and(|collection_id| collection_id != 0)
        {
            return Err(ProtocolError::InvalidRequest(
                "collection ID supplied for a frame that cannot encode it".into(),
            ));
        }
        key.extend_from_slice(&frame.key);

        if magic.is_alt() {
            if key.len() > usize::from(u8::MAX) {
                return Err(ProtocolError::InvalidLength(format!(
                    "flexible-frame key length {} exceeds 255",
                    key.len()
                )));
            }
            if framing.len() > usize::from(u8::MAX) {
                return Err(ProtocolError::InvalidLength(format!(
                    "framing extras length {} exceeds 255",
                    framing.len()
                )));
            }
        } else if key.len() > usize::from(u16::MAX) {
            return Err(ProtocolError::InvalidLength(format!(
                "key length {} exceeds 65535",
                key.len()
            )));
        }

        if frame.magic.is_request() && !frame.status.is_success() {
            return Err(ProtocolError::InvalidRequest(
                "request frame cannot carry a response status".into(),
            ));
        }
        if frame.magic.is_response() && frame.vbucket != 0 {
            return Err(ProtocolError::InvalidRequest(
                "response frame cannot carry a vBucket".into(),
            ));
        }

        let body_len = framing
            .len()
            .checked_add(frame.extras.len())
            .and_then(|length| length.checked_add(key.len()))
            .and_then(|length| length.checked_add(frame.value.len()))
            .ok_or_else(|| ProtocolError::InvalidLength("body length overflows usize".into()))?;
        let total_len = HEADER_LEN
            .checked_add(body_len)
            .ok_or_else(|| ProtocolError::InvalidLength("frame length overflows usize".into()))?;
        if total_len > self.max_frame_size {
            return Err(ProtocolError::InvalidLength(format!(
                "frame size {total_len} exceeds configured maximum {}",
                self.max_frame_size
            )));
        }
        let body_len_u32 = u32::try_from(body_len)
            .map_err(|_| ProtocolError::InvalidLength("body length exceeds u32::MAX".into()))?;
        let extras_len_u8 = u8::try_from(frame.extras.len())
            .map_err(|_| ProtocolError::InvalidLength("extras length exceeds u8::MAX".into()))?;
        let framing_len_u8 = u8::try_from(framing.len()).map_err(|_| {
            ProtocolError::InvalidLength("framing extras length exceeds u8::MAX".into())
        })?;

        destination.reserve(total_len);
        destination.put_u8(magic.as_u8());
        destination.put_u8(frame.opcode.as_u8());
        if magic.is_alt() {
            destination.put_u8(framing_len_u8);
            destination.put_u8(u8::try_from(key.len()).map_err(|_| {
                ProtocolError::InvalidLength("flexible-frame key length exceeds u8::MAX".into())
            })?);
        } else {
            destination.put_u16(u16::try_from(key.len()).map_err(|_| {
                ProtocolError::InvalidLength("classic-frame key length exceeds u16::MAX".into())
            })?);
        }
        destination.put_u8(extras_len_u8);
        destination.put_u8(frame.datatype);
        if frame.magic.is_response() {
            destination.put_u16(frame.status.as_u16());
        } else {
            destination.put_u16(frame.vbucket);
        }
        destination.put_u32(body_len_u32);
        destination.put_u32(frame.opaque);
        destination.put_u64(frame.cas);
        destination.extend_from_slice(&framing);
        destination.extend_from_slice(&frame.extras);
        destination.extend_from_slice(&key);
        destination.extend_from_slice(&frame.value);
        Ok(())
    }
}

fn encoded_magic(magic: Magic, has_framing: bool) -> Result<Magic> {
    if !has_framing {
        return Ok(magic);
    }
    match magic {
        Magic::Request | Magic::AltRequest => Ok(Magic::AltRequest),
        Magic::Response | Magic::AltResponse => Ok(Magic::AltResponse),
        Magic::ServerRequest => Err(ProtocolError::InvalidRequest(
            "server-request frames cannot carry flexible framing extras".into(),
        )),
    }
}

fn encode_framing_extras(extras: &[FramingExtra]) -> Result<BytesMut> {
    let capacity = extras
        .iter()
        .map(|extra| extra.encoded_header_len() + extra.data.len())
        .sum();
    let mut destination = BytesMut::with_capacity(capacity);
    for extra in extras {
        if extra.id > 270 || extra.data.len() > 270 {
            return Err(ProtocolError::InvalidLength(
                "framing-extra identifier and length must not exceed 270".into(),
            ));
        }
        let id_nibble = u8::try_from(extra.id.min(15)).map_err(|_| {
            ProtocolError::InvalidLength("framing-extra id nibble exceeds u8".into())
        })?;
        let len_nibble = u8::try_from(extra.data.len().min(15)).map_err(|_| {
            ProtocolError::InvalidLength("framing-extra length nibble exceeds u8".into())
        })?;
        destination.put_u8((id_nibble << 4) | len_nibble);
        if extra.id >= 15 {
            destination.put_u8(u8::try_from(extra.id - 15).map_err(|_| {
                ProtocolError::InvalidLength("framing-extra id extension exceeds u8".into())
            })?);
        }
        if extra.data.len() >= 15 {
            destination.put_u8(u8::try_from(extra.data.len() - 15).map_err(|_| {
                ProtocolError::InvalidLength("framing-extra length extension exceeds u8".into())
            })?);
        }
        destination.extend_from_slice(&extra.data);
    }
    Ok(destination)
}

fn decode_framing_extras(mut source: Bytes) -> Result<Vec<FramingExtra>> {
    let mut extras = Vec::new();
    while !source.is_empty() {
        let header = source[0];
        source = source.slice(1..);
        let mut id = u16::from(header >> 4);
        let mut length = usize::from(header & 0x0f);
        if id == 15 {
            let extension = source.first().copied().ok_or_else(|| {
                ProtocolError::MalformedFrame("truncated framing-extra id extension".into())
            })?;
            source = source.slice(1..);
            id += u16::from(extension);
        }
        if length == 15 {
            let extension = source.first().copied().ok_or_else(|| {
                ProtocolError::MalformedFrame("truncated framing-extra length extension".into())
            })?;
            source = source.slice(1..);
            length += usize::from(extension);
        }
        if source.len() < length {
            return Err(ProtocolError::MalformedFrame(format!(
                "framing-extra declares {length} data bytes but only {} remain",
                source.len()
            )));
        }
        let data = source.slice(..length);
        source = source.slice(length..);
        extras.push(FramingExtra { id, data });
    }
    Ok(extras)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Opcode, Status};

    fn round_trip(codec: &mut FrameCodec, frame: Frame) -> Frame {
        let mut bytes = BytesMut::new();
        codec.encode(frame, &mut bytes).expect("encode frame");
        codec
            .decode(&mut bytes)
            .expect("decode frame")
            .expect("complete frame")
    }

    #[test]
    fn classic_frame_round_trips() {
        let mut frame = Frame::request(Opcode::DCP_CONTROL);
        frame.vbucket = 17;
        frame.opaque = 0x8765_4321;
        frame.cas = 0x7654_3210_7654_3210;
        frame.key = Bytes::from_static(b"enable_noop");
        frame.value = Bytes::from_static(b"true");
        let decoded = round_trip(&mut FrameCodec::default(), frame.clone());

        assert_eq!(decoded.magic, frame.magic);
        assert_eq!(decoded.opcode, frame.opcode);
        assert_eq!(decoded.vbucket, 17);
        assert_eq!(decoded.key, frame.key);
        assert_eq!(decoded.value, frame.value);
        assert_eq!(decoded.wire_size(), HEADER_LEN + 15);
    }

    #[test]
    fn flexible_stream_id_frame_round_trips() {
        let mut frame = Frame::request(Opcode::DCP_CLOSE_STREAM);
        frame.framing_extras.push(FramingExtra::stream_id(0xe1f8));
        let decoded = round_trip(&mut FrameCodec::default(), frame);

        assert_eq!(decoded.magic, Magic::AltRequest);
        assert_eq!(decoded.stream_id(), Some(0xe1f8));
    }

    #[test]
    fn collection_prefix_round_trips_and_is_not_exposed_in_key() {
        let mut frame = Frame::request(Opcode::DCP_MUTATION);
        frame.collection_id = Some(0xcafe_f00d);
        frame.key = Bytes::from_static(b"document");
        let decoded = round_trip(&mut FrameCodec::default().with_collections(true), frame);

        assert_eq!(decoded.collection_id, Some(0xcafe_f00d));
        assert_eq!(decoded.key, Bytes::from_static(b"document"));
    }

    #[test]
    fn decoder_waits_for_complete_body() {
        let mut bytes = BytesMut::from(
            &[
                0x81, 0x0a, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,
            ][..],
        );

        assert!(
            FrameCodec::default()
                .decode(&mut bytes)
                .expect("decode")
                .is_none()
        );
        assert_eq!(bytes.len(), HEADER_LEN);
    }

    #[test]
    fn decoder_rejects_field_lengths_beyond_body() {
        let mut bytes = BytesMut::from(
            &[
                0x81, 0x0a, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ][..],
        );

        assert!(FrameCodec::default().decode(&mut bytes).is_err());
    }

    #[test]
    fn response_status_round_trips() {
        let frame = Frame::response(Opcode::SASL_AUTH, Status::AUTH_CONTINUE);
        let decoded = round_trip(&mut FrameCodec::default(), frame);

        assert_eq!(decoded.status, Status::AUTH_CONTINUE);
        assert_eq!(decoded.vbucket, 0);
    }
}
