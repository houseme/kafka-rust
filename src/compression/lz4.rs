use std::io::{Read, Write};

use lz4_flex::frame::{FrameDecoder, FrameEncoder};

use crate::error::{Error, Result};

pub fn compress(src: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = FrameEncoder::new(Vec::new());
    encoder
        .write_all(src)
        .map_err(|e| Error::InvalidLz4(e.to_string()))?;
    encoder
        .finish()
        .map_err(|e| Error::InvalidLz4(e.to_string()))
}

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

    #[test]
    fn test_compress_and_uncompress() {
        let msg = b"This is a test message for LZ4 compression";
        let compressed = compress(msg).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(msg.to_vec(), decompressed);
    }

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

    #[test]
    fn test_empty_input() {
        let empty = b"";
        let compressed = compress(empty).unwrap();
        let decompressed = uncompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_large_input() {
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let compressed = compress(&data).unwrap();
        assert!(compressed.len() < data.len());
        let decompressed = uncompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }
}
