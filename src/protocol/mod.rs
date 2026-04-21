//! Kafka protocol layer bridging `kafka-protocol` crate types to internal types.
//!
//! This module provides request builders, response converters, and internal
//! data types for all supported Kafka APIs.

pub mod api_versions;
pub mod consumer;
pub mod fetch;
pub mod metadata;
pub mod offset;
pub mod produce;

// Re-export key types for convenience

use crate::compression::Compression;
use crate::error::{Error, KafkaCode, Result};
use std::mem;
use std::time::Duration;

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
        Compression::GZIP => kafka_protocol::records::Compression::Gzip,
        Compression::SNAPPY => kafka_protocol::records::Compression::Snappy,
        Compression::LZ4 => kafka_protocol::records::Compression::Lz4,
        Compression::ZSTD => kafka_protocol::records::Compression::Zstd,
    }
}

// --------------------------------------------------------------------
// Shared data types (moved from old protocol module)
// --------------------------------------------------------------------

#[derive(Default, Debug, Clone)]
pub struct HeaderResponse {
    pub correlation: i32,
}

// --------------------------------------------------------------------

impl KafkaCode {
    pub(crate) fn from_protocol(n: i16) -> Option<KafkaCode> {
        if n == 0 {
            return None;
        }
        if n >= KafkaCode::OffsetOutOfRange as i16 && n <= KafkaCode::UnsupportedVersion as i16 {
            return Some(unsafe { mem::transmute(n as i8) });
        }
        Some(KafkaCode::Unknown)
    }
}

#[test]
fn test_kafka_code_from_protocol() {
    macro_rules! assert_kafka_code {
        ($kcode:path, $n:expr) => {
            assert_eq!(KafkaCode::from_protocol($n), Some($kcode));
        };
    }

    assert!(KafkaCode::from_protocol(0).is_none());
    assert_kafka_code!(KafkaCode::OffsetOutOfRange, KafkaCode::OffsetOutOfRange as i16);
    assert_kafka_code!(KafkaCode::IllegalGeneration, KafkaCode::IllegalGeneration as i16);
    assert_kafka_code!(KafkaCode::UnsupportedVersion, KafkaCode::UnsupportedVersion as i16);
    assert_kafka_code!(KafkaCode::Unknown, KafkaCode::Unknown as i16);
    assert_kafka_code!(KafkaCode::Unknown, i16::MAX);
    assert_kafka_code!(KafkaCode::Unknown, i16::MIN);
    assert_kafka_code!(KafkaCode::Unknown, -100);
    assert_kafka_code!(KafkaCode::Unknown, 100);
}

impl Error {
    pub(crate) fn from_protocol(n: i16) -> Option<Error> {
        KafkaCode::from_protocol(n).map(Error::Kafka)
    }
}

// --------------------------------------------------------------------

pub fn to_millis_i32(d: Duration) -> Result<i32> {
    let m = d
        .as_secs()
        .saturating_mul(1_000)
        .saturating_add(u64::from(d.subsec_millis()));
    if m > i32::MAX as u64 {
        Err(Error::InvalidDuration)
    } else {
        Ok(m as i32)
    }
}

#[test]
fn test_to_millis_i32() {
    fn assert_invalid(d: Duration) {
        match to_millis_i32(d) {
            Err(Error::InvalidDuration) => {}
            other => panic!("Expected Err(InvalidDuration) but got {other:?}"),
        }
    }
    fn assert_valid(d: Duration, expected_millis: i32) {
        let r = to_millis_i32(d);
        match r {
            Ok(m) => assert_eq!(expected_millis, m),
            Err(e) => panic!("Expected Ok({expected_millis}) but got Err({e:?})"),
        }
    }
    assert_valid(Duration::from_millis(1_234), 1_234);
    assert_valid(Duration::new(540, 123_456_789), 540_123);
    assert_invalid(Duration::from_millis(u64::MAX));
    assert_invalid(Duration::from_millis(u64::from(u32::MAX)));
    assert_invalid(Duration::from_millis(i32::MAX as u64 + 1));
    assert_valid(Duration::from_millis(i32::MAX as u64 - 1), i32::MAX - 1);
}
