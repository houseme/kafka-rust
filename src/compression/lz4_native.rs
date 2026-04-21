//! LZ4 compression codec for Kafka (native lz4 crate).
//!
//! This module provides LZ4 compression and decompression using the
//! [`lz4`](https://crates.io/crates/lz4) crate with its **frame** format.
//! This is the native C-based LZ4 implementation, which is compatible with
//! `kafka-protocol`'s LZ4 handling.
//!
//! # Kafka Compatibility
//!
//! Kafka brokers use the LZ4 frame format as defined by the
//! [LZ4 Frame Format Specification](https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md).
//! The `lz4` crate's `EncoderBuilder`/`DecoderBuilder` produce and consume
//! this format, ensuring compatibility with Kafka's Java client.

use std::io::{Read, Write};

use crate::error::{Error, Result};

/// Compresses the given byte slice using the LZ4 frame format.
///
/// # Errors
///
/// Returns [`Error::InvalidLz4Native`] if the underlying encoder encounters
/// an error during compression.
pub fn compress(src: &[u8]) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut encoder = lz4::EncoderBuilder::new()
        .auto_flush(true)
        .build(&mut buffer)
        .map_err(|e| Error::InvalidLz4Native(e.to_string()))?;
    encoder
        .write_all(src)
        .map_err(|e| Error::InvalidLz4Native(e.to_string()))?;
    let (_output, result) = encoder.finish();
    result.map_err(|e| Error::InvalidLz4Native(e.to_string()))?;
    Ok(buffer)
}

/// Decompresses LZ4 frame-formatted data back to the original bytes.
///
/// # Errors
///
/// Returns [`Error::InvalidLz4Native`] if the input is not a valid LZ4 frame
/// or if the frame data is corrupted.
pub fn uncompress(src: &[u8]) -> Result<Vec<u8>> {
    let mut decoder =
        lz4::Decoder::new(src).map_err(|e| Error::InvalidLz4Native(e.to_string()))?;
    let mut buffer = Vec::new();
    decoder
        .read_to_end(&mut buffer)
        .map_err(|e| Error::InvalidLz4Native(e.to_string()))?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::{compress, uncompress};
    use crate::error::Result;

    #[test]
    fn test_compress_and_uncompress_roundtrip() {
        let msg = b"This is a test message for LZ4 compression";
        let compressed = compress(msg).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(msg.to_vec(), decompressed);
    }

    #[test]
    fn test_uncompress_corrupted_data() {
        let data = b"Hello, LZ4 compression!";
        let mut compressed = compress(data).unwrap();
        // Corrupt the frame header (FLG byte) to ensure an error
        if compressed.len() > 5 {
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
    fn test_frame_header_magic() {
        let data = b"verify lz4 frame header";
        let compressed = compress(data).unwrap();
        assert!(compressed.len() >= 4);
        assert_eq!(&compressed[0..4], &[0x04, 0x22, 0x4D, 0x18]);
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
            .flat_map(u32::to_le_bytes)
            .collect();
        let compressed = compress(&data).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_invalid_frame_content() {
        let mut invalid = vec![0x04, 0x22, 0x4D, 0x18];
        invalid.extend_from_slice(&[0x40, 0x00, 0x00, 0x00]);
        invalid.extend_from_slice(&[0x7F, 0xFF, 0xFF, 0xFF]);
        let result: Result<Vec<u8>> = uncompress(&invalid);
        assert!(result.is_err());
    }
}
