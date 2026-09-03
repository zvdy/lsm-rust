//! CRC-32 (IEEE 802.3) checksums used to detect corrupted on-disk data.
//!
//! The engine stores a checksum alongside every SSTable block and write-ahead
//! log record. Verifying it on read turns silent corruption — a bit flip, a
//! partially written block, a truncated file — into a clean error instead of
//! plausible-looking garbage handed back to the caller.
//!
//! This is the standard reflected CRC-32 (polynomial `0xEDB88320`), the same
//! one used by zlib, gzip, and PNG, implemented here with a compile-time
//! lookup table so the crate stays dependency free.

/// Reflected CRC-32 polynomial (IEEE 802.3 / zlib).
const POLYNOMIAL: u32 = 0xEDB8_8320;

/// Byte-wise lookup table, built at compile time.
const TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLYNOMIAL
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

/// Compute the CRC-32 of `data`.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLE[index];
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_known_vectors() {
        // The canonical CRC-32 check value, plus a few widely published ones.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"a"), 0xE8B7_BE43);
        assert_eq!(crc32(b"abc"), 0x3524_41C2);
        assert_eq!(
            crc32(b"The quick brown fox jumps over the lazy dog"),
            0x414F_A339
        );
    }

    #[test]
    fn detects_single_bit_flips() {
        let data = b"the quick brown fox".to_vec();
        let expected = crc32(&data);
        for byte in 0..data.len() {
            for bit in 0..8 {
                let mut corrupted = data.clone();
                corrupted[byte] ^= 1 << bit;
                assert_ne!(crc32(&corrupted), expected, "missed flip at {byte}:{bit}");
            }
        }
    }

    #[test]
    fn detects_truncation_and_extension() {
        let data = b"a block of entries".to_vec();
        let expected = crc32(&data);
        assert_ne!(crc32(&data[..data.len() - 1]), expected);
        let mut extended = data.clone();
        extended.push(0);
        assert_ne!(crc32(&extended), expected);
    }
}
