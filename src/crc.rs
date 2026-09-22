//! CRC-32 (IEEE 802.3, the same polynomial zlib and gzip use).
//!
//! Hand-rolled so the crate keeps its zero-dependency promise. The table is
//! built at compile time, so this costs nothing at startup.

const POLY: u32 = 0xEDB8_8320;

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static TABLE: [u32; 256] = build_table();

/// Checksum of several buffers, as if they were concatenated. Records are
/// written as header + key + value, and this avoids joining them first.
pub fn crc32_parts(parts: &[&[u8]]) -> u32 {
    let mut crc = 0xFFFF_FFFF;
    for part in parts {
        crc = update(crc, part);
    }
    crc ^ 0xFFFF_FFFF
}

fn update(mut crc: u32, data: &[u8]) -> u32 {
    for &byte in data {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = TABLE[idx] ^ (crc >> 8);
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crc32(data: &[u8]) -> u32 {
        crc32_parts(&[data])
    }

    #[test]
    fn known_vectors() {
        // Values cross-checked against zlib's crc32.
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"a"), 0xE8B7_BE43);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(
            crc32(b"The quick brown fox jumps over the lazy dog"),
            0x414F_A339
        );
    }

    #[test]
    fn parts_match_concatenated() {
        let joined = crc32(b"hello world");
        assert_eq!(crc32_parts(&[b"hello ", b"world"]), joined);
        assert_eq!(crc32_parts(&[b"h", b"ello wor", b"ld"]), joined);
    }
}
