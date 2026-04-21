use std::time::Duration;

use crate::compression::Compression;

#[cfg(not(feature = "producer_timestamp"))]
use crate::protocol::produce::ProducerTimestamp;

use super::GroupOffsetStorage;

pub(crate) const DEFAULT_CONNECTION_RW_TIMEOUT_SECS: u64 = 120;

pub const DEFAULT_COMPRESSION: Compression = Compression::NONE;

pub const DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS: u64 = 100;

pub const DEFAULT_FETCH_MIN_BYTES: i32 = 4096;

pub const DEFAULT_FETCH_MAX_BYTES_PER_PARTITION: i32 = 32 * 1024;

pub const DEFAULT_FETCH_CRC_VALIDATION: bool = true;

pub const DEFAULT_GROUP_OFFSET_STORAGE: Option<GroupOffsetStorage> = None;

pub const DEFAULT_RETRY_BACKOFF_TIME_MILLIS: u64 = 100;

pub const DEFAULT_RETRY_MAX_ATTEMPTS: u32 = 120_000 / DEFAULT_RETRY_BACKOFF_TIME_MILLIS as u32;

pub const DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS: u64 = 540_000;

pub(crate) const DEFAULT_PRODUCER_TIMESTAMP: Option<ProducerTimestamp> = None;

#[derive(Debug)]
pub(crate) struct ClientConfig {
    pub client_id: String,
    pub hosts: Vec<String>,
    pub compression: Compression,
    pub fetch_max_wait_time: i32,
    pub fetch_min_bytes: i32,
    pub fetch_max_bytes_per_partition: i32,
    pub fetch_crc_validation: bool,
    pub offset_storage: Option<GroupOffsetStorage>,
    pub retry_backoff_time: Duration,
    pub retry_max_attempts: u32,
    #[allow(unused)]
    pub producer_timestamp: Option<ProducerTimestamp>,
}
