use bytes::Bytes;

use crate::{Magic, Opcode, ProtocolError, Result, Status};

/// One flexible framing-extra entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FramingExtra {
    /// Frame identifier, including future extended identifiers.
    pub id: u16,
    /// Raw frame data.
    pub data: Bytes,
}

impl FramingExtra {
    /// Creates a framing extra after validating representable limits.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidLength`] when the frame identifier or
    /// data needs more than the single extension byte allowed by the protocol.
    pub fn new(id: u16, data: impl Into<Bytes>) -> Result<Self> {
        let data = data.into();
        if id > 270 {
            return Err(ProtocolError::InvalidLength(format!(
                "framing-extra id {id} exceeds 270"
            )));
        }
        if data.len() > 270 {
            return Err(ProtocolError::InvalidLength(format!(
                "framing-extra data length {} exceeds 270",
                data.len()
            )));
        }
        Ok(Self { id, data })
    }

    /// Creates the DCP stream-ID frame (request frame type 2).
    #[must_use]
    pub fn stream_id(stream_id: u16) -> Self {
        Self {
            id: 2,
            data: Bytes::copy_from_slice(&stream_id.to_be_bytes()),
        }
    }

    pub(crate) fn encoded_header_len(&self) -> usize {
        1 + usize::from(self.id >= 15) + usize::from(self.data.len() >= 15)
    }
}

/// A decoded Memcached binary protocol packet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    /// Header magic and direction.
    pub magic: Magic,
    /// Command opcode.
    pub opcode: Opcode,
    /// Datatype byte.
    pub datatype: u8,
    /// vBucket field for request packets.
    pub vbucket: u16,
    /// Status field for response packets.
    pub status: Status,
    /// Correlation token.
    pub opaque: u32,
    /// Compare-and-swap token.
    pub cas: u64,
    /// Flexible framing extras.
    pub framing_extras: Vec<FramingExtra>,
    /// Command extras.
    pub extras: Bytes,
    /// Key without collection-ID prefix.
    pub key: Bytes,
    /// Value bytes.
    pub value: Bytes,
    /// Collection ID decoded from the key prefix.
    pub collection_id: Option<u32>,
    pub(crate) received_wire_size: Option<usize>,
}

impl Frame {
    /// Creates an empty classic request frame.
    #[must_use]
    pub fn request(opcode: Opcode) -> Self {
        Self {
            magic: Magic::Request,
            opcode,
            datatype: 0,
            vbucket: 0,
            status: Status::SUCCESS,
            opaque: 0,
            cas: 0,
            framing_extras: Vec::new(),
            extras: Bytes::new(),
            key: Bytes::new(),
            value: Bytes::new(),
            collection_id: None,
            received_wire_size: None,
        }
    }

    /// Creates an empty classic response frame.
    #[must_use]
    pub fn response(opcode: Opcode, status: Status) -> Self {
        Self {
            magic: Magic::Response,
            status,
            ..Self::request(opcode)
        }
    }

    /// Creates a success response that retains a request's opcode and opaque.
    #[must_use]
    pub fn success_response_to(request: &Self) -> Self {
        let mut response = Self::response(request.opcode, Status::SUCCESS);
        response.opaque = request.opaque;
        response
    }

    /// Returns the stream ID carried in framing extras, if present and valid.
    #[must_use]
    pub fn stream_id(&self) -> Option<u16> {
        self.framing_extras
            .iter()
            .rev()
            .find(|extra| extra.id == 2 && extra.data.len() == 2)
            .map(|extra| u16::from_be_bytes([extra.data[0], extra.data[1]]))
    }

    /// Exact received wire size, or an estimate for a locally constructed frame.
    #[must_use]
    pub fn wire_size(&self) -> usize {
        self.received_wire_size.unwrap_or_else(|| {
            crate::HEADER_LEN
                + self
                    .framing_extras
                    .iter()
                    .map(|extra| extra.encoded_header_len() + extra.data.len())
                    .sum::<usize>()
                + self.extras.len()
                + self.key.len()
                + self.value.len()
        })
    }

    pub(crate) fn mark_received(&mut self, wire_size: usize) {
        self.received_wire_size = Some(wire_size);
    }
}
