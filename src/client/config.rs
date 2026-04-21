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

/// Strategy for retrying failed operations.
#[derive(Debug, Clone)]
pub enum RetryPolicy {
    /// No retries.
    None,

    /// Fixed interval between retries.
    Fixed {
        interval: Duration,
        max_attempts: u32,
    },

    /// Exponential backoff with jitter.
    Exponential {
        initial: Duration,
        max: Duration,
        multiplier: f64,
        max_attempts: u32,
    },
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::Fixed {
            interval: Duration::from_millis(DEFAULT_RETRY_BACKOFF_TIME_MILLIS),
            max_attempts: DEFAULT_RETRY_MAX_ATTEMPTS,
        }
    }
}

impl RetryPolicy {
    /// Returns the delay before the next retry attempt.
    /// Returns `None` if no more retries should be attempted.
    #[must_use]
    pub fn next_delay(&self, attempt: u32) -> Option<Duration> {
        match self {
            Self::None => None,
            Self::Fixed {
                max_attempts,
                interval,
            } => {
                if attempt >= *max_attempts {
                    None
                } else {
                    Some(*interval)
                }
            }
            Self::Exponential {
                initial,
                max,
                multiplier,
                max_attempts,
            } => {
                if attempt >= *max_attempts {
                    return None;
                }
                let delay = initial.mul_f64(multiplier.powi(attempt as i32));
                let delay = delay.min(*max);
                Some(delay)
            }
        }
    }

    /// Returns the maximum number of retry attempts.
    #[must_use]
    pub fn max_attempts(&self) -> u32 {
        match self {
            Self::None => 0,
            Self::Fixed { max_attempts, .. } => *max_attempts,
            Self::Exponential { max_attempts, .. } => *max_attempts,
        }
    }
}

/// Retry-related configuration.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub policy: RetryPolicy,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            policy: RetryPolicy::default(),
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
    pub(crate) fn retry_policy(&self) -> &RetryPolicy {
        &self.retry.policy
    }

    pub(crate) fn retry_max_attempts(&self) -> u32 {
        self.retry.policy.max_attempts()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_retry_policy_none_no_retries() {
        let policy = RetryPolicy::None;
        assert_eq!(policy.max_attempts(), 0);
        assert!(policy.next_delay(0).is_none());
        assert!(policy.next_delay(1).is_none());
    }

    #[test]
    fn test_retry_policy_fixed() {
        let policy = RetryPolicy::Fixed {
            interval: Duration::from_millis(100),
            max_attempts: 3,
        };
        assert_eq!(policy.max_attempts(), 3);
        assert_eq!(policy.next_delay(0), Some(Duration::from_millis(100)));
        assert_eq!(policy.next_delay(1), Some(Duration::from_millis(100)));
        assert_eq!(policy.next_delay(2), Some(Duration::from_millis(100)));
        assert!(policy.next_delay(3).is_none());
    }

    #[test]
    fn test_retry_policy_exponential_delays() {
        let policy = RetryPolicy::Exponential {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            multiplier: 2.0,
            max_attempts: 5,
        };
        assert_eq!(policy.max_attempts(), 5);
        assert_eq!(policy.next_delay(0), Some(Duration::from_millis(100)));
        assert_eq!(policy.next_delay(1), Some(Duration::from_millis(200)));
        assert_eq!(policy.next_delay(2), Some(Duration::from_millis(400)));
        assert_eq!(policy.next_delay(3), Some(Duration::from_millis(800)));
        assert_eq!(policy.next_delay(4), Some(Duration::from_millis(1600)));
        assert!(policy.next_delay(5).is_none());
    }

    #[test]
    fn test_retry_policy_exponential_max_cap() {
        let policy = RetryPolicy::Exponential {
            initial: Duration::from_millis(100),
            max: Duration::from_millis(500),
            multiplier: 4.0,
            max_attempts: 5,
        };
        assert_eq!(policy.next_delay(0), Some(Duration::from_millis(100)));
        assert_eq!(policy.next_delay(1), Some(Duration::from_millis(400)));
        assert_eq!(policy.next_delay(2), Some(Duration::from_millis(500))); // capped
        assert_eq!(policy.next_delay(3), Some(Duration::from_millis(500))); // capped
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.policy.max_attempts(), DEFAULT_RETRY_MAX_ATTEMPTS);
    }
}
