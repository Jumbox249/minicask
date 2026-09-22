//! The on-disk record format.
//!
//! ```text
//! byte  0 ..  4   crc32 of everything after it (little endian)
//! byte  4 .. 12   timestamp, milliseconds since the unix epoch
//! byte 12 .. 16   key length
//! byte 16 .. 20   value length
//! byte 20 .. 21   flags: bit 0 set means "this key was deleted"
//! byte 21 ..      key bytes, then value bytes
//! ```
//!
//! Everything is fixed width and little endian, so a reader only ever needs
//! one 21-byte read to know how far the record extends.

use crate::crc::crc32_parts;
use crate::error::{Error, Result};
use std::time::{SystemTime, UNIX_EPOCH};

pub const HEADER_LEN: usize = 21;
pub const FLAG_TOMBSTONE: u8 = 0b0000_0001;

/// The fixed-width part of a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub crc: u32,
    pub tstamp: u64,
    pub key_len: u32,
    pub value_len: u32,
    pub flags: u8,
}

impl Header {
    /// Total bytes this record occupies on disk.
    pub fn record_len(&self) -> u64 {
        HEADER_LEN as u64 + self.key_len as u64 + self.value_len as u64
    }

    pub fn is_tombstone(&self) -> bool {
        self.flags & FLAG_TOMBSTONE != 0
    }

    pub fn decode(buf: &[u8; HEADER_LEN]) -> Self {
        Header {
            crc: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            tstamp: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
            key_len: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            value_len: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
            flags: buf[20],
        }
    }

    fn encode(&self) -> [u8; HEADER_LEN] {
        let mut buf = [0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(&self.crc.to_le_bytes());
        buf[4..12].copy_from_slice(&self.tstamp.to_le_bytes());
        buf[12..16].copy_from_slice(&self.key_len.to_le_bytes());
        buf[16..20].copy_from_slice(&self.value_len.to_le_bytes());
        buf[20] = self.flags;
        buf
    }

    /// Recompute the checksum over the header tail plus the payload and
    /// compare it with the stored one.
    pub fn verify(&self, key: &[u8], value: &[u8]) -> bool {
        let header = self.encode();
        self.crc == crc32_parts(&[&header[4..], key, value])
    }
}

/// Serialise one record, ready to be appended to the active log file.
pub fn encode(key: &[u8], value: Option<&[u8]>, tstamp: u64) -> Result<Vec<u8>> {
    if key.is_empty() {
        return Err(Error::InvalidKey("keys may not be empty"));
    }
    if key.len() > u32::MAX as usize {
        return Err(Error::InvalidKey("key exceeds the u32 length prefix"));
    }
    let payload = value.unwrap_or(&[]);
    if payload.len() > u32::MAX as usize {
        return Err(Error::ValueTooLarge(payload.len()));
    }

    let mut header = Header {
        crc: 0,
        tstamp,
        key_len: key.len() as u32,
        value_len: payload.len() as u32,
        flags: if value.is_none() { FLAG_TOMBSTONE } else { 0 },
    };
    // The checksum covers the header from byte 4 onwards, so fill it in on a
    // draft encoding first and then rewrite the first four bytes.
    let draft = header.encode();
    header.crc = crc32_parts(&[&draft[4..], key, payload]);

    let mut out = Vec::with_capacity(HEADER_LEN + key.len() + payload.len());
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(key);
    out.extend_from_slice(payload);
    Ok(out)
}

/// Milliseconds since the unix epoch, saturating at zero if the clock is
/// somehow behind it.
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let bytes = encode(b"colour", Some(b"green"), 42).unwrap();
        assert_eq!(bytes.len(), HEADER_LEN + 6 + 5);

        let header = Header::decode(bytes[..HEADER_LEN].try_into().unwrap());
        assert_eq!(header.tstamp, 42);
        assert_eq!(header.key_len, 6);
        assert_eq!(header.value_len, 5);
        assert!(!header.is_tombstone());
        assert!(header.verify(b"colour", b"green"));
        assert_eq!(header.record_len(), bytes.len() as u64);
    }

    #[test]
    fn tombstones_carry_no_value() {
        let bytes = encode(b"colour", None, 7).unwrap();
        let header = Header::decode(bytes[..HEADER_LEN].try_into().unwrap());
        assert!(header.is_tombstone());
        assert_eq!(header.value_len, 0);
        assert!(header.verify(b"colour", b""));
    }

    #[test]
    fn a_single_flipped_bit_fails_verification() {
        let mut bytes = encode(b"colour", Some(b"green"), 42).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0b0000_0001;
        let header = Header::decode(bytes[..HEADER_LEN].try_into().unwrap());
        assert!(!header.verify(b"colour", &bytes[HEADER_LEN + 6..]));
    }

    #[test]
    fn empty_keys_are_rejected() {
        assert!(matches!(
            encode(b"", Some(b"x"), 0),
            Err(Error::InvalidKey(_))
        ));
    }
}
