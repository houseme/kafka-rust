use std::time::Duration;

use crate::compression::Compression;
use crate::protocol::produce::ProducerTimestamp;

use super::GroupOffsetStorage;

/// Default read/write timeout (seconds) for connection operations.
pub(crate) const DEFAULT_CONNECTION_RW_TIMEOUT_SECS: u64 = 120;

/// Default compression type for produced messages.
pub const DEFAULT_COMPRESSION: Compression = Compression::NONE;

/// Default maximum wait time (in milliseconds) for fetch requests.
pub const DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS: u64 = 100;

/// Default minimum number of bytes to accumulate before returning fetch results.
pub const DEFAULT_FETCH_MIN_BYTES: i32 = 4096;

/// Default maximum bytes to fetch per partition.
pub const DEFAULT_FETCH_MAX_BYTES_PER_PARTITION: i32 = 32 * 1024;

/// Whether CRC validation for fetched messages is enabled by default.
pub const DEFAULT_FETCH_CRC_VALIDATION: bool = true;

/// Default storage for group offsets (None = not configured).
pub const DEFAULT_GROUP_OFFSET_STORAGE: Option<GroupOffsetStorage> = None;

/// Default backoff time (milliseconds) used for retries.
pub const DEFAULT_RETRY_BACKOFF_TIME_MILLIS: u64 = 100;

/// Default maximum number of retry attempts.
pub const DEFAULT_RETRY_MAX_ATTEMPTS: u32 = 120_000 / 100;

/// Default idle timeout (milliseconds) for pooled connections.
pub const DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS: u64 = 540_000;

/// Default producer timestamp configuration (None = disabled).
pub(crate) const DEFAULT_PRODUCER_TIMESTAMP: Option<ProducerTimestamp> = None;

/// Fetch-related configuration.
#[derive(Debug, Clone)]
pub struct FetchConfig {
    /// Maximum wait time (ms) for fetch requests.
    pub max_wait_time: i32,
    /// Minimum number of bytes to accumulate before returning a fetch response.
    pub min_bytes: i32,
    /// Maximum number of bytes to fetch per partition.
    pub max_bytes_per_partition: i32,
    /// Enable CRC validation for fetched message sets.
    pub crc_validation: bool,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            max_wait_time: 100,
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
        /// Fixed interval between retry attempts.
        interval: Duration,
        /// Maximum number of retry attempts for the fixed policy.
        max_attempts: u32,
    },

    /// Exponential backoff with jitter.
    Exponential {
        /// Initial backoff duration.
        initial: Duration,
        /// Maximum backoff duration (cap).
        max: Duration,
        /// Multiplier applied at each retry step.
        multiplier: f64,
        /// Maximum number of retry attempts for the exponential policy.
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
                let exp = i32::try_from(attempt).unwrap_or(i32::MAX);
                let delay = initial.mul_f64(multiplier.powi(exp));
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
            Self::Fixed { max_attempts, .. } | Self::Exponential { max_attempts, .. } => {
                *max_attempts
            }
        }
    }
}

/// Retry-related configuration.
#[derive(Debug, Clone, Default)]
pub struct RetryConfig {
    /// Retry policy configuration.
    pub policy: RetryPolicy,
}

/// Connection-related configuration.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Read/write timeout for connections.
    pub rw_timeout: Duration,
    /// Idle timeout for pooled connections.
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
