use std::time::Duration;

use crate::client::state::ClientState;
use crate::client::{ClientConfig, GroupOffsetStorage, KafkaClient};
use crate::client::config::RetryPolicy;
use crate::compression::Compression;
use crate::protocol::api_versions::ApiVersionCache;

#[cfg(feature = "security")]
use crate::network::SecurityConfig;

use crate::network::Connections;

/// Builder for constructing [`KafkaClient`] instances.
///
/// Provides a fluent API for configuring all aspects of the client
/// before construction. This is the recommended way to create clients
/// with non-default settings.
///
/// # Example
///
/// ```no_run
/// use rustfs_kafka::client::KafkaClient;
///
/// let client = KafkaClient::builder()
///     .with_hosts(vec!["localhost:9092".to_owned()])
///     .with_client_id("my-app".to_owned())
///     .build();
/// ```
#[must_use]
pub struct KafkaClientBuilder {
    hosts: Vec<String>,
    client_id: String,
    #[cfg(feature = "security")]
    security: Option<SecurityConfig>,
    compression: Compression,
    fetch_max_wait_time_millis: u64,
    fetch_min_bytes: i32,
    fetch_max_bytes_per_partition: i32,
    fetch_crc_validation: bool,
    offset_storage: Option<GroupOffsetStorage>,
    retry_policy: RetryPolicy,
    producer_timestamp: Option<crate::protocol::produce::ProducerTimestamp>,
    conn_rw_timeout_secs: u64,
    conn_idle_timeout_millis: u64,
}

impl KafkaClientBuilder {
    pub(crate) fn new() -> Self {
        Self {
            hosts: Vec::new(),
            client_id: String::new(),
            #[cfg(feature = "security")]
            security: None,
            compression: super::DEFAULT_COMPRESSION,
            fetch_max_wait_time_millis: super::DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS,
            fetch_min_bytes: super::DEFAULT_FETCH_MIN_BYTES,
            fetch_max_bytes_per_partition: super::DEFAULT_FETCH_MAX_BYTES_PER_PARTITION,
            fetch_crc_validation: super::DEFAULT_FETCH_CRC_VALIDATION,
            offset_storage: super::DEFAULT_GROUP_OFFSET_STORAGE,
            retry_policy: RetryPolicy::default(),
            producer_timestamp: super::DEFAULT_PRODUCER_TIMESTAMP,
            conn_rw_timeout_secs: super::DEFAULT_CONNECTION_RW_TIMEOUT_SECS,
            conn_idle_timeout_millis: super::DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS,
        }
    }

    /// Sets the list of Kafka broker hosts to connect to.
    ///
    /// Each host should be in the form `"hostname:port"`.
    pub fn with_hosts(mut self, hosts: Vec<String>) -> Self {
        self.hosts = hosts;
        self
    }

    /// Sets the client ID sent with every request to Kafka brokers.
    pub fn with_client_id(mut self, id: String) -> Self {
        self.client_id = id;
        self
    }

    /// Sets the TLS security configuration.
    #[cfg(feature = "security")]
    pub fn with_security(mut self, config: SecurityConfig) -> Self {
        self.security = Some(config);
        self
    }

    /// Sets the compression algorithm to use when producing messages.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the maximum time (in milliseconds) to wait for sufficient
    /// data when fetching messages.
    pub fn with_fetch_max_wait_time(mut self, millis: u64) -> Self {
        self.fetch_max_wait_time_millis = millis;
        self
    }

    /// Sets the minimum number of bytes to wait for when fetching messages.
    pub fn with_fetch_min_bytes(mut self, bytes: i32) -> Self {
        self.fetch_min_bytes = bytes;
        self
    }

    /// Sets the maximum bytes to fetch per partition.
    pub fn with_fetch_max_bytes_per_partition(mut self, bytes: i32) -> Self {
        self.fetch_max_bytes_per_partition = bytes;
        self
    }

    /// Enables or disables CRC validation on fetched messages.
    pub fn with_fetch_crc_validation(mut self, validate: bool) -> Self {
        self.fetch_crc_validation = validate;
        self
    }

    /// Sets the group offset storage type.
    pub fn with_group_offset_storage(mut self, storage: Option<GroupOffsetStorage>) -> Self {
        self.offset_storage = storage;
        self
    }

    /// Sets the retry policy.
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Sets the retry backoff time (in milliseconds).
    /// Only affects `RetryPolicy::Fixed` or `RetryPolicy::Exponential`.
    pub fn with_retry_backoff_time(mut self, millis: u64) -> Self {
        let backoff = Duration::from_millis(millis);
        match &mut self.retry_policy {
            RetryPolicy::Exponential { initial, .. } => *initial = backoff,
            RetryPolicy::Fixed { interval, .. } => *interval = backoff,
            RetryPolicy::None => {}
        }
        self
    }

    /// Sets the maximum number of retry attempts.
    pub fn with_retry_max_attempts(mut self, attempts: u32) -> Self {
        match &mut self.retry_policy {
            RetryPolicy::Exponential { max_attempts, .. } => *max_attempts = attempts,
            RetryPolicy::Fixed { max_attempts, .. } => *max_attempts = attempts,
            RetryPolicy::None => {}
        }
        self
    }

    /// Sets the producer timestamp mode.
    #[cfg(feature = "producer_timestamp")]
    pub fn with_producer_timestamp(
        mut self,
        timestamp: Option<crate::protocol::produce::ProducerTimestamp>,
    ) -> Self {
        self.producer_timestamp = timestamp;
        self
    }

    /// Sets the connection read/write timeout (in seconds).
    pub fn with_conn_rw_timeout(mut self, secs: u64) -> Self {
        self.conn_rw_timeout_secs = secs;
        self
    }

    /// Sets the connection idle timeout (in milliseconds).
    pub fn with_conn_idle_timeout(mut self, millis: u64) -> Self {
        self.conn_idle_timeout_millis = millis;
        self
    }

    /// Builds the [`KafkaClient`] with the configured settings.
    ///
    /// # Panics
    ///
    /// Panics if `fetch_max_wait_time_millis` exceeds `i32::MAX` milliseconds.
    #[must_use]
    pub fn build(self) -> KafkaClient {
        let config = ClientConfig {
            client_id: self.client_id,
            hosts: self.hosts,
            compression: self.compression,
            fetch: super::config::FetchConfig {
                max_wait_time: crate::protocol::to_millis_i32(Duration::from_millis(
                    self.fetch_max_wait_time_millis,
                ))
                .expect("invalid fetch-max-wait-time"),
                min_bytes: self.fetch_min_bytes,
                max_bytes_per_partition: self.fetch_max_bytes_per_partition,
                crc_validation: self.fetch_crc_validation,
            },
            retry: super::config::RetryConfig {
                policy: self.retry_policy,
            },
            connection: super::config::ConnectionConfig {
                rw_timeout: Duration::from_secs(self.conn_rw_timeout_secs),
                idle_timeout: Duration::from_millis(self.conn_idle_timeout_millis),
            },
            offset_storage: self.offset_storage,
            producer_timestamp: self.producer_timestamp,
        };

        let rw_timeout = if self.conn_rw_timeout_secs == 0 {
            None
        } else {
            Some(config.connection.rw_timeout)
        };

        #[cfg(not(feature = "security"))]
        let conn_pool = Connections::new(rw_timeout, config.connection.idle_timeout);

        #[cfg(feature = "security")]
        let conn_pool = match self.security {
            Some(security) => Connections::new_with_security(rw_timeout, config.connection.idle_timeout, Some(security)),
            None => Connections::new(rw_timeout, config.connection.idle_timeout),
        };

        KafkaClient {
            config,
            conn_pool,
            state: ClientState::new(),
            api_versions: ApiVersionCache::new(),
        }
    }
}

impl Default for KafkaClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
