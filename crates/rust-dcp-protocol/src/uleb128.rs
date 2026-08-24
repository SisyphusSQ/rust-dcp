use bytes::{BufMut, BytesMut};

use crate::{ProtocolError, Result};

/// Appends a 32-bit unsigned LEB128 value.
pub fn encode_uleb128_u32(mut value: u32, destination: &mut BytesMut) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        destination.put_u8(byte);
        if byte & 0x80 == 0 {
            break;
        }
    }
}

/// Decodes one 32-bit unsigned LEB128 value and its encoded length.
///
/// # Errors
///
/// Returns [`ProtocolError::InvalidCollectionId`] for truncated, overlong, or
/// overflowing input.
pub fn decode_uleb128_u32(source: &[u8]) -> Result<(u32, usize)> {
    if source.is_empty() {
        return Err(ProtocolError::InvalidCollectionId(
            "no bytes supplied".into(),
        ));
    }

    let mut value = 0_u64;
    for (index, byte) in source.iter().copied().enumerate() {
        if index >= 5 {
            return Err(ProtocolError::InvalidCollectionId(
                "encoded value exceeds five bytes".into(),
            ));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            if value > u64::from(u32::MAX) {
                return Err(ProtocolError::InvalidCollectionId(
                    "encoded value exceeds u32".into(),
                ));
            }
            let decoded = u32::try_from(value).map_err(|_| {
                ProtocolError::InvalidCollectionId("encoded value exceeds u32".into())
            })?;
            return Ok((decoded, index + 1));
        }
    }

    Err(ProtocolError::InvalidCollectionId(
        "encoded value is truncated".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uleb128_round_trips_boundary_values() {
        for expected in [0, 1, 0x7f, 0x80, 0x555, 0xffff, 0xcafe_f00d, u32::MAX] {
            let mut bytes = BytesMut::new();
            encode_uleb128_u32(expected, &mut bytes);
            let (actual, consumed) = decode_uleb128_u32(&bytes).expect("valid encoding");

            assert_eq!(actual, expected);
            assert_eq!(consumed, bytes.len());
        }
    }

    #[test]
    fn uleb128_rejects_truncated_and_overflowing_values() {
        assert!(decode_uleb128_u32(&[0x80]).is_err());
        assert!(decode_uleb128_u32(&[0xff, 0xff, 0xff, 0xff, 0x10]).is_err());
        assert!(decode_uleb128_u32(&[0x80, 0x80, 0x80, 0x80, 0x80, 0]).is_err());
    }
}
