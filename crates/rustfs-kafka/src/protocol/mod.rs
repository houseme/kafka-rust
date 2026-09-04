//! Kafka protocol layer bridging `kafka-protocol` crate types to internal types.
//!
//! This module provides request builders, response converters, and internal
//! data types for all supported Kafka APIs.

pub mod admin;
pub mod api_keys;
pub mod api_versions;
pub mod consumer;
pub mod create_topics;
pub mod delete_topics;
pub mod fetch;
pub mod group;
pub mod init_producer_id;
pub mod metadata;
pub mod offset;
pub mod produce;
pub mod share_consumer;
pub mod telemetry;
pub mod transaction;

// Re-export key types for convenience

use crate::compression::Compression;
use crate::error::{Error, Result};
#[cfg(test)]
use crate::error::{KafkaCode, ProtocolError};
use std::time::Duration;

pub const API_VERSION_PRODUCE: i16 = 9;
pub const API_VERSION_FETCH: i16 = 12;
pub const API_VERSION_METADATA: i16 = 1;
pub const API_VERSION_LIST_OFFSETS: i16 = 1;
pub const API_VERSION_OFFSET_COMMIT: i16 = 2;
pub const API_VERSION_OFFSET_FETCH: i16 = 2;
pub const API_VERSION_FIND_COORDINATOR: i16 = 3;
pub const API_VERSION_DELETE_RECORDS: i16 = 2;
pub const API_VERSION_OFFSET_FOR_LEADER_EPOCH: i16 = 4;
pub const API_VERSION_DESCRIBE_GROUPS: i16 = 6;
pub const API_VERSION_LIST_GROUPS: i16 = 5;
pub const API_VERSION_DESCRIBE_ACLS: i16 = 3;
pub const API_VERSION_CREATE_ACLS: i16 = 3;
pub const API_VERSION_DELETE_ACLS: i16 = 3;
pub const API_VERSION_DESCRIBE_CLUSTER: i16 = 2;
pub const API_VERSION_DESCRIBE_CONFIGS: i16 = 4;
pub const API_VERSION_INCREMENTAL_ALTER_CONFIGS: i16 = 1;
pub const API_VERSION_DESCRIBE_LOG_DIRS: i16 = 4;
pub const API_VERSION_DESCRIBE_DELEGATION_TOKEN: i16 = 3;
pub const API_VERSION_DELETE_GROUPS: i16 = 2;
pub const API_VERSION_CREATE_PARTITIONS: i16 = 3;
pub const API_VERSION_ELECT_LEADERS: i16 = 2;
pub const API_VERSION_ALTER_PARTITION_REASSIGNMENTS: i16 = 1;
pub const API_VERSION_LIST_PARTITION_REASSIGNMENTS: i16 = 0;
pub const API_VERSION_OFFSET_DELETE: i16 = 0;
pub const API_VERSION_DESCRIBE_CLIENT_QUOTAS: i16 = 1;
pub const API_VERSION_ALTER_CLIENT_QUOTAS: i16 = 1;
pub const API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS: i16 = 0;
pub const API_VERSION_DESCRIBE_QUORUM: i16 = 2;
pub const API_VERSION_CONSUMER_GROUP_DESCRIBE: i16 = 1;
pub const API_VERSION_LIST_CONFIG_RESOURCES: i16 = 1;
pub const API_VERSION_DESCRIBE_TOPIC_PARTITIONS: i16 = 0;
pub const API_VERSION_SHARE_GROUP_DESCRIBE: i16 = 1;
pub const API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS: i16 = 0;
pub const API_VERSION_DESCRIBE_PRODUCERS: i16 = 0;
pub const API_VERSION_LIST_TRANSACTIONS: i16 = 2;
pub const API_VERSION_DESCRIBE_TRANSACTIONS: i16 = 0;
pub const API_VERSION_ALTER_CONFIGS: i16 = 2;
pub const API_VERSION_ALTER_REPLICA_LOG_DIRS: i16 = 2;
pub const API_VERSION_CREATE_DELEGATION_TOKEN: i16 = 3;
pub const API_VERSION_RENEW_DELEGATION_TOKEN: i16 = 2;
pub const API_VERSION_EXPIRE_DELEGATION_TOKEN: i16 = 2;
pub const API_VERSION_ALTER_USER_SCRAM_CREDENTIALS: i16 = 0;
pub const API_VERSION_ADD_OFFSETS_TO_TXN: i16 = 4;
pub const API_VERSION_TXN_OFFSET_COMMIT: i16 = 5;
pub const API_VERSION_WRITE_TXN_MARKERS: i16 = 1;
pub const API_VERSION_ALTER_SHARE_GROUP_OFFSETS: i16 = 0;
pub const API_VERSION_DELETE_SHARE_GROUP_OFFSETS: i16 = 0;
pub const API_VERSION_UPDATE_FEATURES: i16 = 2;
pub const API_VERSION_VOTE: i16 = 2;
pub const API_VERSION_BEGIN_QUORUM_EPOCH: i16 = 1;
pub const API_VERSION_END_QUORUM_EPOCH: i16 = 1;
pub const API_VERSION_ALTER_PARTITION: i16 = 3;
pub const API_VERSION_ENVELOPE: i16 = 0;
pub const API_VERSION_FETCH_SNAPSHOT: i16 = 1;
pub const API_VERSION_BROKER_REGISTRATION: i16 = 4;
pub const API_VERSION_BROKER_HEARTBEAT: i16 = 1;
pub const API_VERSION_UNREGISTER_BROKER: i16 = 0;
pub const API_VERSION_ALLOCATE_PRODUCER_IDS: i16 = 0;
pub const API_VERSION_CONTROLLER_REGISTRATION: i16 = 0;
pub const API_VERSION_ASSIGN_REPLICAS_TO_DIRS: i16 = 0;
pub const API_VERSION_ADD_RAFT_VOTER: i16 = 0;
pub const API_VERSION_REMOVE_RAFT_VOTER: i16 = 0;
pub const API_VERSION_UPDATE_RAFT_VOTER: i16 = 0;
pub const API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS: i16 = 0;
pub const API_VERSION_PUSH_TELEMETRY: i16 = 0;
pub const API_VERSION_CONSUMER_GROUP_HEARTBEAT: i16 = 1;
pub const API_VERSION_SHARE_GROUP_HEARTBEAT: i16 = 1;
pub const API_VERSION_SHARE_FETCH: i16 = 1;
pub const API_VERSION_SHARE_ACKNOWLEDGE: i16 = 1;
pub const API_VERSION_INITIALIZE_SHARE_GROUP_STATE: i16 = 0;
pub const API_VERSION_READ_SHARE_GROUP_STATE: i16 = 0;
pub const API_VERSION_WRITE_SHARE_GROUP_STATE: i16 = 0;
pub const API_VERSION_DELETE_SHARE_GROUP_STATE: i16 = 0;
pub const API_VERSION_READ_SHARE_GROUP_STATE_SUMMARY: i16 = 0;

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

#[test]
fn test_kafka_code_from_protocol() {
    macro_rules! assert_kafka_code {
        ($kcode:path, $n:expr) => {
            assert_eq!(KafkaCode::from_protocol($n), Some($kcode));
        };
    }

    assert!(KafkaCode::from_protocol(0).is_none());
    assert_kafka_code!(
        KafkaCode::OffsetOutOfRange,
        KafkaCode::OffsetOutOfRange as i16
    );
    assert_kafka_code!(
        KafkaCode::IllegalGeneration,
        KafkaCode::IllegalGeneration as i16
    );
    assert_kafka_code!(
        KafkaCode::UnsupportedVersion,
        KafkaCode::UnsupportedVersion as i16
    );
    assert_kafka_code!(KafkaCode::Unknown, KafkaCode::Unknown as i16);
    assert_kafka_code!(KafkaCode::Unknown, i16::MAX);
    assert_kafka_code!(KafkaCode::Unknown, i16::MIN);
    assert_kafka_code!(KafkaCode::Unknown, -100);
    assert_kafka_code!(KafkaCode::Unknown, 100);
}

// --------------------------------------------------------------------

pub fn to_millis_i32(d: Duration) -> Result<i32> {
    let m = d
        .as_secs()
        .saturating_mul(1_000)
        .saturating_add(u64::from(d.subsec_millis()));
    if m > i32::MAX as u64 {
        Err(Error::invalid_duration())
    } else {
        i32::try_from(m).map_err(|_| Error::invalid_duration())
    }
}

pub fn to_millis_i64(d: Duration) -> Result<i64> {
    let m = d
        .as_secs()
        .saturating_mul(1_000)
        .saturating_add(u64::from(d.subsec_millis()));
    i64::try_from(m).map_err(|_| Error::invalid_duration())
}

pub(crate) fn usize_to_i16(value: usize) -> Result<i16> {
    i16::try_from(value).map_err(|_| Error::codec())
}

pub(crate) fn usize_to_i32(value: usize) -> Result<i32> {
    i32::try_from(value).map_err(|_| Error::codec())
}

pub(crate) fn non_negative_i16_to_usize(value: i16) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::codec())
}

pub(crate) fn non_negative_i32_to_usize(value: i32) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::codec())
}

pub(crate) fn non_negative_i32_to_u64(value: i32) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::codec())
}

#[test]
fn test_to_millis_i32() {
    fn assert_invalid(d: Duration) {
        match to_millis_i32(d) {
            Err(Error::Protocol(ProtocolError::InvalidDuration)) => {}
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
    assert_valid(Duration::from_secs(1), 1_000);
    assert_valid(Duration::ZERO, 0);
}

#[test]
fn test_to_millis_i64() {
    fn assert_invalid(d: Duration) {
        match to_millis_i64(d) {
            Err(Error::Protocol(ProtocolError::InvalidDuration)) => {}
            other => panic!("Expected Err(InvalidDuration) but got {other:?}"),
        }
    }
    fn assert_valid(d: Duration, expected_millis: i64) {
        let r = to_millis_i64(d);
        match r {
            Ok(m) => assert_eq!(expected_millis, m),
            Err(e) => panic!("Expected Ok({expected_millis}) but got Err({e:?})"),
        }
    }

    assert_valid(Duration::from_millis(1_234), 1_234);
    assert_valid(Duration::new(540, 123_456_789), 540_123);
    assert_valid(Duration::from_millis(i64::MAX as u64), i64::MAX);
    assert_invalid(Duration::from_millis(i64::MAX as u64 + 1));
    assert_invalid(Duration::from_millis(u64::MAX));
    assert_valid(Duration::ZERO, 0);
}

#[test]
fn test_to_kp_compression() {
    use kafka_protocol::records::Compression as KpCompression;

    assert_eq!(to_kp_compression(Compression::NONE), KpCompression::None);
    assert_eq!(to_kp_compression(Compression::GZIP), KpCompression::Gzip);
    assert_eq!(
        to_kp_compression(Compression::SNAPPY),
        KpCompression::Snappy
    );
    assert_eq!(to_kp_compression(Compression::LZ4), KpCompression::Lz4);
    assert_eq!(to_kp_compression(Compression::ZSTD), KpCompression::Zstd);
}

#[test]
fn test_api_version_constants_are_positive() {
    let versions = [
        API_VERSION_PRODUCE,
        API_VERSION_FETCH,
        API_VERSION_METADATA,
        API_VERSION_LIST_OFFSETS,
        API_VERSION_OFFSET_COMMIT,
        API_VERSION_OFFSET_FETCH,
        API_VERSION_FIND_COORDINATOR,
        API_VERSION_DELETE_RECORDS,
        API_VERSION_OFFSET_FOR_LEADER_EPOCH,
        API_VERSION_DESCRIBE_GROUPS,
        API_VERSION_LIST_GROUPS,
        API_VERSION_DESCRIBE_ACLS,
        API_VERSION_CREATE_ACLS,
        API_VERSION_DELETE_ACLS,
        API_VERSION_DESCRIBE_CLUSTER,
        API_VERSION_DESCRIBE_CONFIGS,
        API_VERSION_INCREMENTAL_ALTER_CONFIGS,
        API_VERSION_DESCRIBE_LOG_DIRS,
        API_VERSION_DESCRIBE_DELEGATION_TOKEN,
        API_VERSION_DELETE_GROUPS,
        API_VERSION_CREATE_PARTITIONS,
        API_VERSION_ELECT_LEADERS,
        API_VERSION_ALTER_PARTITION_REASSIGNMENTS,
        API_VERSION_LIST_PARTITION_REASSIGNMENTS,
        API_VERSION_OFFSET_DELETE,
        API_VERSION_DESCRIBE_CLIENT_QUOTAS,
        API_VERSION_ALTER_CLIENT_QUOTAS,
        API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
        API_VERSION_DESCRIBE_QUORUM,
        API_VERSION_CONSUMER_GROUP_DESCRIBE,
        API_VERSION_LIST_CONFIG_RESOURCES,
        API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
        API_VERSION_SHARE_GROUP_DESCRIBE,
        API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
        API_VERSION_DESCRIBE_PRODUCERS,
        API_VERSION_LIST_TRANSACTIONS,
        API_VERSION_DESCRIBE_TRANSACTIONS,
        API_VERSION_ALTER_CONFIGS,
        API_VERSION_ALTER_REPLICA_LOG_DIRS,
        API_VERSION_CREATE_DELEGATION_TOKEN,
        API_VERSION_RENEW_DELEGATION_TOKEN,
        API_VERSION_EXPIRE_DELEGATION_TOKEN,
        API_VERSION_ALTER_USER_SCRAM_CREDENTIALS,
        API_VERSION_ADD_OFFSETS_TO_TXN,
        API_VERSION_TXN_OFFSET_COMMIT,
        API_VERSION_UPDATE_FEATURES,
        API_VERSION_UNREGISTER_BROKER,
        API_VERSION_ASSIGN_REPLICAS_TO_DIRS,
        API_VERSION_ADD_RAFT_VOTER,
        API_VERSION_REMOVE_RAFT_VOTER,
        API_VERSION_UPDATE_RAFT_VOTER,
        API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS,
        API_VERSION_PUSH_TELEMETRY,
        API_VERSION_CONSUMER_GROUP_HEARTBEAT,
        API_VERSION_SHARE_GROUP_HEARTBEAT,
        API_VERSION_SHARE_FETCH,
        API_VERSION_SHARE_ACKNOWLEDGE,
        API_VERSION_ALTER_SHARE_GROUP_OFFSETS,
        API_VERSION_DELETE_SHARE_GROUP_OFFSETS,
    ];
    assert!(versions.iter().all(|v| *v >= 0));
    assert_eq!(API_VERSION_LIST_PARTITION_REASSIGNMENTS, 0);
    assert_eq!(API_VERSION_OFFSET_DELETE, 0);
    assert_eq!(API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS, 0);
    assert_eq!(API_VERSION_DESCRIBE_TOPIC_PARTITIONS, 0);
    assert_eq!(API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS, 0);
    assert_eq!(API_VERSION_DESCRIBE_PRODUCERS, 0);
    assert_eq!(API_VERSION_DESCRIBE_TRANSACTIONS, 0);
    assert_eq!(API_VERSION_ALTER_USER_SCRAM_CREDENTIALS, 0);
    assert_eq!(API_VERSION_ADD_OFFSETS_TO_TXN, 4);
    assert_eq!(API_VERSION_TXN_OFFSET_COMMIT, 5);
    assert_eq!(API_VERSION_UPDATE_FEATURES, 2);
    assert_eq!(API_VERSION_UNREGISTER_BROKER, 0);
    assert_eq!(API_VERSION_ASSIGN_REPLICAS_TO_DIRS, 0);
    assert_eq!(API_VERSION_ADD_RAFT_VOTER, 0);
    assert_eq!(API_VERSION_REMOVE_RAFT_VOTER, 0);
    assert_eq!(API_VERSION_UPDATE_RAFT_VOTER, 0);
    assert_eq!(API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS, 0);
    assert_eq!(API_VERSION_PUSH_TELEMETRY, 0);
    assert_eq!(API_VERSION_CONSUMER_GROUP_HEARTBEAT, 1);
    assert_eq!(API_VERSION_SHARE_GROUP_HEARTBEAT, 1);
    assert_eq!(API_VERSION_SHARE_FETCH, 1);
    assert_eq!(API_VERSION_SHARE_ACKNOWLEDGE, 1);
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn to_kp_compression_roundtrip(disc in 0u8..=4u8) {
            use crate::compression::Compression;
            let compression = match disc {
                0 => Compression::NONE,
                1 => Compression::GZIP,
                2 => Compression::SNAPPY,
                3 => Compression::LZ4,
                4 => Compression::ZSTD,
                _ => return Ok(()),
            };
            let kp = to_kp_compression(compression);
            let _ = format!("{kp:?}");
        }

        #[test]
        fn to_millis_i32_valid_range(millis in 0u64..=2_147_483_647u64) {
            let d = Duration::from_millis(millis);
            assert!(to_millis_i32(d).is_ok());
        }

        #[test]
        fn to_millis_i64_accepts_u64_values_in_i64_range(millis in 0u64..=i64::MAX as u64) {
            let d = Duration::from_millis(millis);
            prop_assert_eq!(to_millis_i64(d)?, i64::try_from(millis)?);
        }
    }
}
