//! Compression codecs for Kafka messages.
//!
//! Kafka supports pluggable compression at the message-set level. The
//! compression type is encoded in the lower 3 bits of the `attributes`
//! field in each [`Message`](crate::protocol::produce::MessageProduceRequest).
//!
//! This crate provides the following codecs, each gated behind a Cargo feature:
//!
//! | Codec  | Attribute | Feature  | Crate      |
//! |--------|-----------|----------|------------|
//! | None   | `0`       | —        | —          |
//! | GZIP   | `1`       | `gzip`   | `flate2`   |
//! | Snappy | `2`       | `snappy` | `snap`     |
//! | LZ4    | `3`       | `lz4`    | `lz4_flex` |
//!
//! All features except `gzip` are enabled by default. To trim dependencies,
//! disable the default features and re-enable only what you need:
//!
//! ```toml
//! [dependencies]
//! rustfs-kafka = { version = "0.20", default-features = false, features = ["gzip"] }
//! ```

#[cfg(feature = "gzip")]
pub mod gzip;

#[cfg(feature = "lz4")]
pub mod lz4;

#[cfg(feature = "snappy")]
pub mod snappy;

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
#[derive(Debug, Copy, Clone, Default)]
pub enum Compression {
    /// No compression.
    #[default]
    NONE = 0,
    /// GZIP compression (RFC 1952). Higher compression ratio but slower.
    #[cfg(feature = "gzip")]
    GZIP = 1,
    /// Snappy compression. Good balance of speed and ratio.
    #[cfg(feature = "snappy")]
    SNAPPY = 2,
    /// LZ4 compression. Fastest compression and decompression speed.
    #[cfg(feature = "lz4")]
    LZ4 = 3,
}
