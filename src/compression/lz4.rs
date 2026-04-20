//! LZ4 compression codec for Kafka.
//!
//! This module provides LZ4 compression and decompression using the
//! [`lz4_flex`](https://crates.io/crates/lz4_flex) crate with its **frame**
//! format. The LZ4 compression type corresponds to attribute value `3` in the
//! Kafka message protocol (the lower 3 bits of the `attributes` field).
//!
//! # Kafka Compatibility
//!
//! Kafka brokers use the LZ4 frame format as defined by the
//! [LZ4 Frame Format Specification](https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md).
//! The `lz4_flex` crate's frame encoder/decoder is compatible with this format,
//! allowing seamless interoperability with Kafka's Java client and other
//! LZ4-enabled Kafka producers/consumers.
//!
//! # Usage
//!
//! This module is feature-gated behind the `lz4` Cargo feature (enabled by
//! default). When the feature is active, `Compression::LZ4` becomes available
//! in [`crate::compression::Compression`].
//!
//! ```ignore
//! use rustfs_kafka::compression::Compression;
//! client.set_compression(Compression::LZ4);
//! ```
//!
//! # Performance Characteristics
//!
//! LZ4 offers a favorable trade-off between compression speed and ratio:
//! - **Compression speed**: Very fast, typically 400+ MB/s
//! - **Decompression speed**: Extremely fast, typically 2+ GB/s
//! - **Compression ratio**: Lower than GZIP but significantly faster
//!
//! LZ4 is well-suited for scenarios where low CPU overhead and high throughput
//! are prioritized over maximum compression ratio.

use std::io::{Read, Write};

use lz4_flex::frame::{FrameDecoder, FrameEncoder};

use crate::error::{Error, Result};

/// Compresses the given byte slice using the LZ4 frame format.
///
/// The output includes the LZ4 frame header, compressed block data, and
/// an optional content-size field. The frame format ensures compatibility
/// with Kafka's LZ4 decompression implementation.
///
/// # Errors
///
/// Returns [`Error::InvalidLz4`] if the underlying encoder encounters
/// an I/O error during compression.
pub fn compress(src: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = FrameEncoder::new(Vec::new());
    encoder
        .write_all(src)
        .map_err(|e| Error::InvalidLz4(e.to_string()))?;
    encoder
        .finish()
        .map_err(|e| Error::InvalidLz4(e.to_string()))
}

/// Decompresses LZ4 frame-formatted data back to the original bytes.
///
/// The input must be a valid LZ4 frame (as produced by [`compress`]
/// or by Kafka's Java LZ4 codec). The function reads the entire frame
/// header and all compressed blocks, then returns the reassembled
/// uncompressed payload.
///
/// # Errors
///
/// Returns [`Error::InvalidLz4`] if the input is not a valid LZ4 frame
/// or if the frame data is corrupted.
pub fn uncompress(src: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = FrameDecoder::new(src);
    let mut buffer = Vec::new();
    decoder
        .read_to_end(&mut buffer)
        .map_err(|e| Error::InvalidLz4(e.to_string()))?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::{compress, uncompress};
    use crate::error::Result;

    /// Round-trip test: compress then decompress should yield the original data.
    #[test]
    fn test_compress_and_uncompress_roundtrip() {
        let msg = b"This is a test message for LZ4 compression";
        let compressed = compress(msg).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(msg.to_vec(), decompressed);
    }

    /// Corrupted frame data should produce an error on decompression.
    #[test]
    fn test_uncompress_corrupted_data() {
        let data = b"Hello, LZ4 compression!";
        let mut compressed = compress(data).unwrap();
        // corrupt the frame content (skip the header bytes)
        if compressed.len() > 7 {
            compressed[7] ^= 0xFF;
        }
        let result: Result<Vec<u8>> = uncompress(&compressed);
        assert!(result.is_err());
    }

    /// Empty input should produce a valid (minimal) frame that decompresses
    /// to an empty slice.
    #[test]
    fn test_empty_input() {
        let empty = b"";
        let compressed = compress(empty).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    /// Large input with repetitive patterns should compress to a smaller size.
    #[test]
    fn test_large_input_compression_ratio() {
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let compressed = compress(&data).unwrap();
        assert!(compressed.len() < data.len());
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    /// Highly repetitive data should achieve a good compression ratio.
    #[test]
    fn test_repetitive_data() {
        let data = "aaaaaa".repeat(10_000).into_bytes();
        let compressed = compress(&data).unwrap();
        // highly repetitive data should compress to well under 50% of original
        assert!(compressed.len() < data.len() / 2);
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    /// Binary data with all byte values should round-trip correctly.
    #[test]
    fn test_all_byte_values() {
        let data: Vec<u8> = (0u8..=255).collect();
        let compressed = compress(&data).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    /// Compressed output should always start with the LZ4 frame magic number
    /// (0x184D2204 in little-endian).
    #[test]
    fn test_frame_header_magic() {
        let data = b"verify lz4 frame header";
        let compressed = compress(data).unwrap();
        // lz4 frame magic: 0x04, 0x22, 0x4D, 0x18
        assert!(compressed.len() >= 4);
        assert_eq!(&compressed[0..4], &[0x04, 0x22, 0x4D, 0x18]);
    }

    /// Very short input (single byte) should still round-trip correctly.
    #[test]
    fn test_single_byte() {
        let data = b"X";
        let compressed = compress(data).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data.to_vec(), decompressed);
    }

    /// Random-ish data (non-repetitive) should still compress and decompress
    /// correctly, though the compressed size may be larger than the input.
    #[test]
    fn test_non_repetitive_data() {
        let data: Vec<u8> = (0u32..1_000)
            .flat_map(|i| i.to_le_bytes())
            .collect();
        let compressed = compress(&data).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    /// Verify the Kafka protocol attribute value for LZ4 is 3.
    #[test]
    fn test_lz4_compression_attribute_value() {
        use crate::compression::Compression;
        assert_eq!(Compression::LZ4 as i8, 3);
    }

    /// A truncated frame (valid magic + corrupted block length) should fail
    /// on decompression.
    #[test]
    fn test_invalid_frame_content() {
        // Valid LZ4 frame magic followed by a block with an impossibly large
        // declared size, which will cause a read error during decompression.
        let mut invalid = vec![0x04, 0x22, 0x4D, 0x18];
        // frame descriptor: block max size (4 bytes) + block checksum flag
        invalid.extend_from_slice(&[0x40, 0x00, 0x00, 0x00]);
        // block with huge declared size
        invalid.extend_from_slice(&[0x7F, 0xFF, 0xFF, 0xFF]);
        let result: Result<Vec<u8>> = uncompress(&invalid);
        assert!(result.is_err());
    }
}
