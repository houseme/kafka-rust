use std::mem;
use std::time::Duration;

use crate::error::{Error, KafkaCode, Result};

pub mod consumer;
pub mod list_offset;
pub mod metadata;
pub mod offset;
pub mod produce;

pub use self::consumer::{
    GroupCoordinatorResponse, OffsetCommitResponse, OffsetFetchResponse,
    PartitionOffsetCommitResponse, PartitionOffsetFetchResponse,
    TopicPartitionOffsetCommitResponse, TopicPartitionOffsetFetchResponse,
};
pub use self::metadata::{BrokerMetadata, MetadataResponse, PartitionMetadata, TopicMetadata};
pub use self::offset::{OffsetResponse, PartitionOffsetResponse, TopicPartitionOffsetResponse};
pub use self::produce::{PartitionProduceResponse, ProduceResponse, TopicPartitionProduceResponse};

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

#[derive(Default, Debug, Clone)]
pub struct HeaderResponse {
    pub correlation: i32,
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
