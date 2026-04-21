//! Protocol adapter layer bridging `kafka-protocol` crate types to our internal types.
//!
//! This module provides conversion functions between our internal request/response
//! types and the `kafka_protocol` crate's generated types. During migration, both
//! the old `protocol` module and this module coexist.

pub mod api_versions;
pub mod consumer;
pub mod fetch;
pub mod metadata;
pub mod offset;
pub mod produce;

use crate::compression::Compression;

pub const API_VERSION_PRODUCE: i16 = 3;
pub const API_VERSION_FETCH: i16 = 4;
pub const API_VERSION_METADATA: i16 = 1;
pub const API_VERSION_LIST_OFFSETS: i16 = 1;
pub const API_VERSION_OFFSET_COMMIT: i16 = 2;
pub const API_VERSION_OFFSET_FETCH: i16 = 2;
pub const API_VERSION_FIND_COORDINATOR: i16 = 1;

/// Map our `Compression` to `kafka_protocol::records::Compression`.
pub fn to_kp_compression(c: Compression) -> kafka_protocol::records::Compression {
    match c {
        Compression::NONE => kafka_protocol::records::Compression::None,
        #[cfg(feature = "gzip")]
        Compression::GZIP => kafka_protocol::records::Compression::Gzip,
        #[cfg(feature = "snappy")]
        Compression::SNAPPY => kafka_protocol::records::Compression::Snappy,
        #[cfg(feature = "lz4")]
        Compression::LZ4 => kafka_protocol::records::Compression::Lz4,
        #[cfg(feature = "zstd")]
        Compression::ZSTD => kafka_protocol::records::Compression::Zstd,
    }
}
