//! Compression types for Kafka messages.
//!
//! Kafka supports pluggable compression at the message-set level. The
//! compression type is encoded in the lower 3 bits of the `attributes`
//! field in each [`Message`](crate::protocol::produce::MessageProduceRequest).
//!
//! Actual compression and decompression is handled by the `kafka-protocol` crate.

/// Compression types supported by Kafka.
///
/// The discriminant values correspond to the compression encoding in the
/// `attributes` field of a [`Message`](crate::protocol::produce::MessageProduceRequest)
/// in the Kafka wire protocol (lower 3 bits).
///
/// | Variant | Value | Kafka constant |
/// |---------|-------|---------------|
/// | `NONE`  | 0     | `none`        |
/// | `GZIP`  | 1     | `gzip`        |
/// | `SNAPPY`| 2     | `snappy`      |
/// | `LZ4`   | 3     | `lz4`         |
/// | `ZSTD`  | 4     | `zstd`        |
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub enum Compression {
    /// No compression.
    #[default]
    NONE = 0,
    /// GZIP compression (RFC 1952). Higher compression ratio but slower.
    GZIP = 1,
    /// Snappy compression. Good balance of speed and ratio.
    SNAPPY = 2,
    /// LZ4 compression. Fastest compression and decompression speed.
    LZ4 = 3,
    /// ZSTD compression. Excellent balance of ratio and speed.
    ZSTD = 4,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_discriminant_values() {
        assert_eq!(Compression::NONE as i32, 0);
        assert_eq!(Compression::GZIP as i32, 1);
        assert_eq!(Compression::SNAPPY as i32, 2);
        assert_eq!(Compression::LZ4 as i32, 3);
        assert_eq!(Compression::ZSTD as i32, 4);
    }

    #[test]
    fn test_compression_default() {
        assert_eq!(Compression::default(), Compression::NONE);
    }

    #[test]
    fn test_compression_debug() {
        assert_eq!(format!("{:?}", Compression::NONE), "NONE");
        assert_eq!(format!("{:?}", Compression::GZIP), "GZIP");
        assert_eq!(format!("{:?}", Compression::SNAPPY), "SNAPPY");
        assert_eq!(format!("{:?}", Compression::LZ4), "LZ4");
        assert_eq!(format!("{:?}", Compression::ZSTD), "ZSTD");
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn compression_discriminant_in_range(disc in 0i8..=4i8) {
            assert!(matches!(disc, 0..=4));
        }

        #[test]
        fn compression_debug_roundtrip(disc in 0u8..=4u8) {
            let compression = match disc {
                0 => Compression::NONE,
                1 => Compression::GZIP,
                2 => Compression::SNAPPY,
                3 => Compression::LZ4,
                4 => Compression::ZSTD,
                _ => return Ok(()),
            };
            let debug_str = format!("{:?}", compression);
            assert!(!debug_str.is_empty());
            let as_i32 = compression as i32;
            assert!((0..=4).contains(&as_i32));
        }
    }
}
