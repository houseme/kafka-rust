//! ZSTD compression codec for Kafka.
//!
//! This module provides ZSTD compression and decompression using the
//! [`zstd`](https://crates.io/crates/zstd) crate. The ZSTD compression
//! type corresponds to attribute value `4` in the Kafka message protocol
//! (the lower 3 bits of the `attributes` field).
//!
//! # Performance Characteristics
//!
//! ZSTD offers an excellent balance between compression ratio and speed:
//! - **Compression speed**: Fast, comparable to LZ4 at default level
//! - **Decompression speed**: Very fast, typically 1+ GB/s
//! - **Compression ratio**: Better than GZIP at comparable speeds

use crate::error::{Error, Result};

/// Compresses the given byte slice using ZSTD.
///
/// Uses the default compression level (level 3), which provides a good
/// balance between compression ratio and speed for typical Kafka workloads.
///
/// # Errors
///
/// Returns [`Error::InvalidZstd`] if the underlying encoder encounters
/// an error during compression.
pub fn compress(src: &[u8]) -> Result<Vec<u8>> {
    zstd::encode_all(src, 0).map_err(|e| Error::InvalidZstd(e.to_string()))
}

/// Decompresses ZSTD-compressed data back to the original bytes.
///
/// # Errors
///
/// Returns [`Error::InvalidZstd`] if the input is not valid ZSTD data
/// or if the data is corrupted.
pub fn uncompress(src: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(src).map_err(|e| Error::InvalidZstd(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{compress, uncompress};
    use crate::error::Result;

    #[test]
    fn test_compress_and_uncompress_roundtrip() {
        let msg = b"This is a test message for ZSTD compression";
        let compressed = compress(msg).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(msg.to_vec(), decompressed);
    }

    #[test]
    fn test_uncompress_corrupted_data() {
        let data = b"Hello, ZSTD compression!";
        let mut compressed = compress(data).unwrap();
        if compressed.len() > 4 {
            compressed[4] ^= 0xFF;
        }
        let result: Result<Vec<u8>> = uncompress(&compressed);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_input() {
        let empty = b"";
        let compressed = compress(empty).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_large_input_compression_ratio() {
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let compressed = compress(&data).unwrap();
        assert!(compressed.len() < data.len());
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_repetitive_data() {
        let data = "aaaaaa".repeat(10_000).into_bytes();
        let compressed = compress(&data).unwrap();
        assert!(compressed.len() < data.len() / 2);
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_all_byte_values() {
        let data: Vec<u8> = (0u8..=255).collect();
        let compressed = compress(&data).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_single_byte() {
        let data = b"X";
        let compressed = compress(data).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data.to_vec(), decompressed);
    }

    #[test]
    fn test_non_repetitive_data() {
        let data: Vec<u8> = (0u32..1_000)
            .flat_map(|i| i.to_le_bytes())
            .collect();
        let compressed = compress(&data).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_zstd_compression_attribute_value() {
        use crate::compression::Compression;
        assert_eq!(Compression::ZSTD as i8, 4);
    }

    #[test]
    fn test_invalid_data() {
        let result: Result<Vec<u8>> = uncompress(b"not valid zstd data at all");
        assert!(result.is_err());
    }
}
