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

/// Fetch-related configuration.
#[derive(Debug, Clone)]
pub struct FetchConfig {
    pub max_wait_time: i32,
    pub min_bytes: i32,
    pub max_bytes_per_partition: i32,
    pub crc_validation: bool,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            max_wait_time: DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS as i32,
            min_bytes: DEFAULT_FETCH_MIN_BYTES,
            max_bytes_per_partition: DEFAULT_FETCH_MAX_BYTES_PER_PARTITION,
            crc_validation: DEFAULT_FETCH_CRC_VALIDATION,
        }
    }
}

/// Retry-related configuration.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub backoff: Duration,
    pub max_attempts: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            backoff: Duration::from_millis(DEFAULT_RETRY_BACKOFF_TIME_MILLIS),
            max_attempts: DEFAULT_RETRY_MAX_ATTEMPTS,
        }
    }
}

/// Connection-related configuration.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub rw_timeout: Duration,
    pub idle_timeout: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            rw_timeout: Duration::from_secs(DEFAULT_CONNECTION_RW_TIMEOUT_SECS),
            idle_timeout: Duration::from_millis(DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS),
        }
    }
}

/// Internal configuration for the Kafka client.
#[derive(Debug)]
pub(crate) struct ClientConfig {
    pub client_id: String,
    pub hosts: Vec<String>,
    pub compression: Compression,
    pub fetch: FetchConfig,
    pub retry: RetryConfig,
    pub connection: ConnectionConfig,
    pub offset_storage: Option<GroupOffsetStorage>,
    #[allow(unused)]
    pub producer_timestamp: Option<ProducerTimestamp>,
}

impl ClientConfig {
    #[allow(dead_code)]
    pub(crate) fn retry_backoff_time(&self) -> Duration {
        self.retry.backoff
    }

    pub(crate) fn retry_max_attempts(&self) -> u32 {
        self.retry.max_attempts
    }

    pub(crate) fn fetch_max_wait_time(&self) -> i32 {
        self.fetch.max_wait_time
    }

    pub(crate) fn fetch_min_bytes(&self) -> i32 {
        self.fetch.min_bytes
    }

    pub(crate) fn fetch_max_bytes_per_partition(&self) -> i32 {
        self.fetch.max_bytes_per_partition
    }

    #[allow(dead_code)]
    pub(crate) fn fetch_crc_validation(&self) -> bool {
        self.fetch.crc_validation
    }

    #[allow(dead_code)]
    pub(crate) fn connection_idle_timeout(&self) -> Duration {
        self.connection.idle_timeout
    }

    #[allow(dead_code)]
    pub(crate) fn connection_rw_timeout(&self) -> Duration {
        self.connection.rw_timeout
    }
}
