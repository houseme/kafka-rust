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
#[derive(Debug, Copy, Clone, Default)]
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
