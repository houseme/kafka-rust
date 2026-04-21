//! Kafka Client - A mid-level abstraction for a kafka cluster
//! allowing building higher level constructs.
//!
//! The entry point into this module is `KafkaClient` obtained by a
//! call to `KafkaClient::new()`.

// pub re-export
pub use crate::compression::Compression;
#[cfg(feature = "producer_timestamp")]
pub use crate::protocol::produce::ProducerTimestamp;
pub use crate::utils::PartitionOffset;
use crate::utils::TimestampedPartitionOffset;
use std;
use std::collections::hash_map;
use std::collections::hash_map::HashMap;
use std::iter::Iterator;
use std::mem;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, trace};

#[cfg(not(feature = "producer_timestamp"))]
use crate::protocol::produce::ProducerTimestamp;

#[cfg(feature = "security")]
pub use self::network::SecurityConfig;

use crate::error::{Error, KafkaCode, Result};
use crate::protocol;

use crate::client_internals::KafkaClientInternals;

pub mod metadata;
pub(crate) mod network;
mod state;

/// Owned fetch response types from the kafka-protocol adapter.
/// These types own their data (no lifetimes) and are returned by
/// `KafkaClient::fetch_messages_kp`.
pub mod fetch_kp {
    pub use crate::protocol::fetch::{
        OwnedData, OwnedFetchResponse, OwnedMessage, OwnedPartition, OwnedTopic,
    };
}

pub mod fetch {
    pub use crate::protocol::fetch::OwnedFetchResponse as Response;
    pub use crate::protocol::fetch::{OwnedData, OwnedMessage, OwnedPartition, OwnedTopic};
}

const DEFAULT_CONNECTION_RW_TIMEOUT_SECS: u64 = 120;

fn default_conn_rw_timeout() -> Option<Duration> {
    match DEFAULT_CONNECTION_RW_TIMEOUT_SECS {
        0 => None,
        n => Some(Duration::from_secs(n)),
    }
}

/// The default value for `KafkaClient::set_compression(..)`
pub const DEFAULT_COMPRESSION: Compression = Compression::NONE;

/// The default value for `KafkaClient::set_fetch_max_wait_time(..)`
pub const DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS: u64 = 100;

/// The default value for `KafkaClient::set_fetch_min_bytes(..)`
pub const DEFAULT_FETCH_MIN_BYTES: i32 = 4096;

/// The default value for `KafkaClient::set_fetch_max_bytes(..)`
pub const DEFAULT_FETCH_MAX_BYTES_PER_PARTITION: i32 = 32 * 1024;

/// The default value for `KafkaClient::set_fetch_crc_validation(..)`
pub const DEFAULT_FETCH_CRC_VALIDATION: bool = true;

/// The default value for `KafkaClient::set_group_offset_storage(..)`
pub const DEFAULT_GROUP_OFFSET_STORAGE: Option<GroupOffsetStorage> = None;

/// The default value for `KafkaClient::set_retry_backoff_time(..)`
pub const DEFAULT_RETRY_BACKOFF_TIME_MILLIS: u64 = 100;

/// The default value for `KafkaClient::set_retry_max_attempts(..)`
// the default value: re-attempt a repeatable operation for
// approximetaly up to two minutes
pub const DEFAULT_RETRY_MAX_ATTEMPTS: u32 = 120_000 / DEFAULT_RETRY_BACKOFF_TIME_MILLIS as u32;

/// The default value for `KafkaClient::set_connection_idle_timeout(..)`
pub const DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS: u64 = 540_000;

/// The default value for `KafkaClient::set_producer_timestamp(..)`
pub(crate) const DEFAULT_PRODUCER_TIMESTAMP: Option<ProducerTimestamp> = None;

/// Client struct keeping track of brokers and topic metadata.
///
/// Implements methods described by the [Kafka Protocol](http://kafka.apache.org/protocol.html).
///
/// You will have to load metadata before making any other request.
#[derive(Debug)]
pub struct KafkaClient {
    // ~ this kafka client configuration
    config: ClientConfig,

    // ~ a pool of re-usable connections to kafka brokers
    conn_pool: network::Connections,

    // ~ the current state of this client
    state: state::ClientState,

    // ~ negotiated API versions per broker
    api_versions: crate::protocol::api_versions::ApiVersionCache,
}

#[derive(Debug)]
struct ClientConfig {
    client_id: String,
    hosts: Vec<String>,
    // ~ compression to use when sending messages
    compression: Compression,
    // ~ these are the defaults when fetching messages for details
    // refer to the kafka wire protocol
    fetch_max_wait_time: i32,
    fetch_min_bytes: i32,
    fetch_max_bytes_per_partition: i32,
    fetch_crc_validation: bool,
    // ~ the version of the API to use for the corresponding kafka
    // calls; note that this might have an effect on the storage type
    // kafka will then use (zookeeper or __consumer_offsets).  it is
    // important to use version for both of them which target the same
    // storage type.
    // offset_fetch_version: protocol::OffsetFetchVersion,
    // offset_commit_version: protocol::OffsetCommitVersion,
    offset_storage: Option<GroupOffsetStorage>,
    // ~ the duration to wait before retrying a failed
    // operation like refreshing group coordinators; this avoids
    // operation retries in a tight loop.
    retry_backoff_time: Duration,
    // ~ the number of repeated retry attempts; prevents endless
    // repetition of a retry attempt
    retry_max_attempts: u32,
    // ~ producer's message timestamp option CreateTime/LogAppendTime
    #[allow(unused)]
    producer_timestamp: Option<ProducerTimestamp>,
}

// --------------------------------------------------------------------

/// Possible values when querying a topic's offset.
/// See `KafkaClient::fetch_offsets`.
#[derive(Debug, Copy, Clone)]
pub enum FetchOffset {
    /// Receive the earliest available offset.
    Earliest,
    /// Receive the latest offset.
    Latest,
    /// Used to ask for all messages before a certain time (ms); unix
    /// timestamp in milliseconds.
    /// See [Writing a Driver for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/Writing+a+Driver+for+Kafka#WritingaDriverforKafka-Offsets)
    ByTime(i64),
}

impl FetchOffset {
    fn to_kafka_value(self) -> i64 {
        match self {
            FetchOffset::Earliest => -2,
            FetchOffset::Latest => -1,
            FetchOffset::ByTime(n) => n,
        }
    }
}

// --------------------------------------------------------------------

/// Defines the available storage types to utilize when fetching or
/// committing group offsets.  See also `KafkaClient::set_group_offset_storage`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum GroupOffsetStorage {
    /// Zookeeper based storage (available as of kafka 0.8.1)
    Zookeeper,
    /// Kafka based storage (available as of Kafka 0.8.2). This is the
    /// preferred method for groups to store their offsets at.
    Kafka,
}


/// Data point identifying a topic partition to fetch a group's offset
/// for.  See `KafkaClient::fetch_group_offsets`.
#[derive(Debug)]
pub struct FetchGroupOffset<'a> {
    /// The topic to fetch the group offset for
    pub topic: &'a str,
    /// The partition to fetch the group offset for
    pub partition: i32,
}

impl<'a> FetchGroupOffset<'a> {
    #[inline]
    #[must_use]
    pub fn new(topic: &'a str, partition: i32) -> Self {
        FetchGroupOffset { topic, partition }
    }
}

impl<'a> AsRef<FetchGroupOffset<'a>> for FetchGroupOffset<'a> {
    fn as_ref(&self) -> &Self {
        self
    }
}

// --------------------------------------------------------------------

/// Data point identifying a particular topic partition offset to be
/// committed.
/// See `KafkaClient::commit_offsets`.
#[derive(Debug)]
pub struct CommitOffset<'a> {
    /// The offset to be committed
    pub offset: i64,
    /// The topic to commit the offset for
    pub topic: &'a str,
    /// The partition to commit the offset for
    pub partition: i32,
}

impl<'a> CommitOffset<'a> {
    #[must_use]
    pub fn new(topic: &'a str, partition: i32, offset: i64) -> Self {
        CommitOffset {
            offset,
            topic,
            partition,
        }
    }
}

impl<'a> AsRef<CommitOffset<'a>> for CommitOffset<'a> {
    fn as_ref(&self) -> &Self {
        self
    }
}

// --------------------------------------------------------------------

/// Possible choices on acknowledgement requirements when
/// producing/sending messages to Kafka. See
/// `KafkaClient::produce_messages`.
#[derive(Debug, Copy, Clone)]
pub enum RequiredAcks {
    /// Indicates to the receiving Kafka broker not to acknowledge
    /// messages sent to it at all. Sending messages with this
    /// acknowledgement requirement translates into a fire-and-forget
    /// scenario which - of course - is very fast but not reliable.
    None = 0,
    /// Requires the receiving Kafka broker to wait until the sent
    /// messages are written to local disk.  Such messages can be
    /// regarded as acknowledged by one broker in the cluster.
    One = 1,
    /// Requires the sent messages to be acknowledged by all in-sync
    /// replicas of the targeted topic partitions.
    All = -1,
}

// --------------------------------------------------------------------

/// Message data to be sent/produced to a particular topic partition.
/// See `KafkaClient::produce_messages` and `Producer::send`.
#[derive(Debug)]
pub struct ProduceMessage<'a, 'b> {
    /// The "key" data of this message.
    pub key: Option<&'b [u8]>,

    /// The "value" data of this message.
    pub value: Option<&'b [u8]>,

    /// The topic to produce this message to.
    pub topic: &'a str,

    /// The partition (of the corresponding topic) to produce this
    /// message to.
    pub partition: i32,
}

impl<'a, 'b> AsRef<ProduceMessage<'a, 'b>> for ProduceMessage<'a, 'b> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<'a, 'b> ProduceMessage<'a, 'b> {
    /// A convenient constructor method to create a new produce
    /// message with all attributes specified.
    #[must_use]
    pub fn new(
        topic: &'a str,
        partition: i32,
        key: Option<&'b [u8]>,
        value: Option<&'b [u8]>,
    ) -> Self {
        ProduceMessage {
            key,
            value,
            topic,
            partition,
        }
    }
}

// --------------------------------------------------------------------

/// Partition related request data for fetching messages.
/// See `KafkaClient::fetch_messages`.
#[derive(Debug)]
pub struct FetchPartition<'a> {
    /// The topic to fetch messages from.
    pub topic: &'a str,

    /// The offset as of which to fetch messages.
    pub offset: i64,

    /// The partition to fetch messages from.
    pub partition: i32,

    /// Specifies the max. amount of data to fetch (for this
    /// partition.)  This implicitly defines the biggest message the
    /// client can accept.  If this value is too small, no messages
    /// can be delivered.  Setting this size should be in sync with
    /// the producers to the partition.
    ///
    /// Zero or negative values are treated as "unspecified".
    pub max_bytes: i32,
}

impl<'a> FetchPartition<'a> {
    /// Creates a new "fetch messages" request structure with an
    /// unspecified `max_bytes`.
    #[must_use]
    pub fn new(topic: &'a str, partition: i32, offset: i64) -> Self {
        FetchPartition {
            topic,
            partition,
            offset,
            max_bytes: -1,
        }
    }

    /// Sets the `max_bytes` value for the "fetch messages" request.
    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: i32) -> Self {
        self.max_bytes = max_bytes;
        self
    }
}

impl<'a> AsRef<FetchPartition<'a>> for FetchPartition<'a> {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// A confirmation of messages sent back by the Kafka broker
/// to confirm delivery of producer messages.
#[derive(Debug)]
pub struct ProduceConfirm {
    /// The topic the messages were sent to.
    pub topic: String,

    /// The list of individual confirmations for each offset and partition.
    pub partition_confirms: Vec<ProducePartitionConfirm>,
}

/// A confirmation of messages sent back by the Kafka broker
/// to confirm delivery of producer messages for a particular topic.
#[derive(Debug)]
pub struct ProducePartitionConfirm {
    /// The offset assigned to the first message in the message set appended
    /// to this partition, or an error if one occurred.
    pub offset: std::result::Result<i64, KafkaCode>,

    /// The partition to which the message(s) were appended.
    pub partition: i32,
}

// --------------------------------------------------------------------

impl KafkaClient {
    /// Creates a new instance of `KafkaClient`. Before being able to
    /// successfully use the new client, you'll have to load metadata.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = rustfs_kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// ```
    #[must_use]
    pub fn new(hosts: Vec<String>) -> KafkaClient {
        KafkaClient {
            config: ClientConfig {
                client_id: String::new(),
                hosts,
                compression: DEFAULT_COMPRESSION,
                fetch_max_wait_time: protocol::to_millis_i32(Duration::from_millis(
                    DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS,
                ))
                .expect("invalid default-fetch-max-time-millis"),
                fetch_min_bytes: DEFAULT_FETCH_MIN_BYTES,
                fetch_max_bytes_per_partition: DEFAULT_FETCH_MAX_BYTES_PER_PARTITION,
                fetch_crc_validation: DEFAULT_FETCH_CRC_VALIDATION,
                offset_storage: DEFAULT_GROUP_OFFSET_STORAGE,
                retry_backoff_time: Duration::from_millis(DEFAULT_RETRY_BACKOFF_TIME_MILLIS),
                retry_max_attempts: DEFAULT_RETRY_MAX_ATTEMPTS,
                producer_timestamp: DEFAULT_PRODUCER_TIMESTAMP,
            },
            conn_pool: network::Connections::new(
                default_conn_rw_timeout(),
                Duration::from_millis(DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS),
            ),
            state: state::ClientState::new(),
            api_versions: crate::protocol::api_versions::ApiVersionCache::new(),
        }
    }

    /// Creates a new secure instance of `KafkaClient`. Before being able to
    /// successfully use the new client, you'll have to load metadata.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustfs_kafka::client::{KafkaClient, SecurityConfig};
    ///
    /// let mut client = KafkaClient::new_secure(
    ///     vec!["localhost:9093".to_owned()],
    ///     SecurityConfig::new()
    ///         .with_ca_cert("ca.pem".to_owned())
    ///         .with_client_cert("client.crt".to_owned(), "client.key".to_owned())
    /// );
    /// client.load_metadata_all().unwrap();
    /// ```
    /// See also `SecurityConfig#with_hostname_verification` to disable hostname verification.
    ///
    /// See also `KafkaClient::load_metadatata_all` and
    /// `KafkaClient::load_metadata` methods, as well as
    /// [Kafka's documentation](https://kafka.apache.org/documentation.html#security_ssl).
    #[cfg(feature = "security")]
    #[must_use]
    pub fn new_secure(hosts: Vec<String>, security: SecurityConfig) -> KafkaClient {
        KafkaClient {
            config: ClientConfig {
                client_id: String::new(),
                hosts,
                compression: DEFAULT_COMPRESSION,
                fetch_max_wait_time: protocol::to_millis_i32(Duration::from_millis(
                    DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS,
                ))
                .expect("invalid default-fetch-max-time-millis"),
                fetch_min_bytes: DEFAULT_FETCH_MIN_BYTES,
                fetch_max_bytes_per_partition: DEFAULT_FETCH_MAX_BYTES_PER_PARTITION,
                fetch_crc_validation: DEFAULT_FETCH_CRC_VALIDATION,
                offset_storage: DEFAULT_GROUP_OFFSET_STORAGE,
                retry_backoff_time: Duration::from_millis(DEFAULT_RETRY_BACKOFF_TIME_MILLIS),
                retry_max_attempts: DEFAULT_RETRY_MAX_ATTEMPTS,
                producer_timestamp: DEFAULT_PRODUCER_TIMESTAMP,
            },
            conn_pool: network::Connections::new_with_security(
                default_conn_rw_timeout(),
                Duration::from_millis(DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS),
                Some(security),
            ),
            state: state::ClientState::new(),
            api_versions: crate::protocol::api_versions::ApiVersionCache::new(),
        }
    }

    /// Exposes the hosts used for discovery of the target kafka
    /// cluster.  This set of hosts corresponds to the values supplied
    /// to `KafkaClient::new`.
    #[inline]
    #[must_use]
    pub fn hosts(&self) -> &[String] {
        &self.config.hosts
    }

    /// Sets the `client_id` to be sent along every request to the
    /// remote Kafka brokers.  By default, this value is the empty
    /// string.
    ///
    /// Kafka brokers write out this client id to their
    /// request/response trace log - if configured appropriately.
    pub fn set_client_id(&mut self, client_id: String) {
        self.config.client_id = client_id;
    }

    /// Retrieves the current `KafkaClient::set_client_id` setting.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.config.client_id
    }

    /// Sets the compression algorithm to use when sending out messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use rustfs_kafka::client::{Compression, KafkaClient};
    ///
    /// let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// client.set_compression(Compression::NONE);
    /// ```
    #[inline]
    pub fn set_compression(&mut self, compression: Compression) {
        self.config.compression = compression;
    }

    /// Retrieves the current `KafkaClient::set_compression` setting.
    #[inline]
    #[must_use]
    pub fn compression(&self) -> Compression {
        self.config.compression
    }

    /// Sets the maximum time in milliseconds to wait for insufficient
    /// data to become available when fetching messages.
    ///
    /// See also `KafkaClient::set_fetch_min_bytes(..)` and
    /// `KafkaClient::set_fetch_max_bytes_per_partition(..)`.
    #[inline]
    pub fn set_fetch_max_wait_time(&mut self, max_wait_time: Duration) -> Result<()> {
        self.config.fetch_max_wait_time = protocol::to_millis_i32(max_wait_time)?;
        Ok(())
    }

    /// Retrieves the current `KafkaClient::set_fetch_max_wait_time`
    /// setting.
    #[inline]
    #[must_use]
    pub fn fetch_max_wait_time(&self) -> Duration {
        Duration::from_millis(self.config.fetch_max_wait_time as u64)
    }

    /// Sets the minimum number of bytes of available data to wait for
    /// as long as specified by `KafkaClient::set_fetch_max_wait_time`
    /// when fetching messages.
    ///
    /// By setting higher values in combination with the timeout the
    /// consumer can tune for throughput and trade a little additional
    /// latency for reading only large chunks of data (e.g. setting
    /// `MaxWaitTime` to 100 ms and setting `MinBytes` to 64k would allow
    /// the server to wait up to 100ms to try to accumulate 64k of
    /// data before responding).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use rustfs_kafka::client::{KafkaClient, FetchPartition};
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// client.set_fetch_max_wait_time(Duration::from_millis(100));
    /// client.set_fetch_min_bytes(64 * 1024);
    /// let r = client.fetch_messages(&[FetchPartition::new("my-topic", 0, 0)]);
    /// ```
    ///
    /// See also `KafkaClient::set_fetch_max_wait_time(..)` and
    /// `KafkaClient::set_fetch_max_bytes_per_partition(..)`.
    #[inline]
    pub fn set_fetch_min_bytes(&mut self, min_bytes: i32) {
        self.config.fetch_min_bytes = min_bytes;
    }

    /// Retrieves the current `KafkaClient::set_fetch_min_bytes`
    /// setting.
    #[inline]
    #[must_use]
    pub fn fetch_min_bytes(&self) -> i32 {
        self.config.fetch_min_bytes
    }

    /// Sets the default maximum number of bytes to obtain from _a
    /// single kafka partition_ when fetching messages.
    ///
    /// This basically determines the maximum message size this client
    /// will be able to fetch.  If a topic partition contains a
    /// message larger than this specified number of bytes, the server
    /// will not deliver it.
    ///
    /// Note that this setting is related to a single partition.  The
    /// overall potential data size in a fetch messages response will
    /// thus be determined by the number of partitions in the fetch
    /// messages request times this "max bytes per partitions."
    ///
    /// This client will use this setting by default for all queried
    /// partitions, however, `fetch_messages` does allow you to
    /// override this setting for a particular partition being
    /// queried.
    ///
    /// See also `KafkaClient::set_fetch_max_wait_time`,
    /// `KafkaClient::set_fetch_min_bytes`, and `KafkaClient::fetch_messages`.
    #[inline]
    pub fn set_fetch_max_bytes_per_partition(&mut self, max_bytes: i32) {
        self.config.fetch_max_bytes_per_partition = max_bytes;
    }

    /// Retrieves the current
    /// `KafkaClient::set_fetch_max_bytes_per_partition` setting.
    #[inline]
    #[must_use]
    pub fn fetch_max_bytes_per_partition(&self) -> i32 {
        self.config.fetch_max_bytes_per_partition
    }

    /// Specifies whether the to perform CRC validation on fetched
    /// messages.
    ///
    /// This ensures detection of on-the-wire or on-disk corruption to
    /// fetched messages.  This check adds some overhead, so it may be
    /// disabled in cases seeking extreme performance.
    #[inline]
    pub fn set_fetch_crc_validation(&mut self, validate_crc: bool) {
        self.config.fetch_crc_validation = validate_crc;
    }

    /// Retrieves the current `KafkaClient::set_fetch_crc_validation`
    /// setting.
    #[inline]
    #[must_use]
    pub fn fetch_crc_validation(&self) -> bool {
        self.config.fetch_crc_validation
    }

    /// Specifies the group offset storage to address when fetching or
    /// committing group offsets.
    ///
    /// In addition to Zookeeper, Kafka 0.8.2 brokers or later offer a
    /// more performant (and scalable) way to manage group offset
    /// directly by itself. Note that the remote storages are separate
    /// and independent on each other. Hence, you typically want
    /// consistently hard-code your choice in your program.
    ///
    /// Unless you have a 0.8.1 broker or want to participate in a
    /// group which is already based on Zookeeper, you generally want
    /// to choose `GroupOffsetStorage::Kafka` here.
    ///
    /// See also `KafkaClient::fetch_group_offsets` and
    /// `KafkaClient::commit_offsets`.
    #[inline]
    pub fn set_group_offset_storage(&mut self, storage: Option<GroupOffsetStorage>) {
        self.config.offset_storage = storage;
    }

    /// Retrieves the current `KafkaClient::set_group_offset_storage`
    /// settings.
    #[must_use]
    pub fn group_offset_storage(&self) -> Option<GroupOffsetStorage> {
        self.config.offset_storage
    }

    /// Specifies the time to wait before retrying a failed,
    /// repeatable operation against Kafka.  This avoids retrying such
    /// operations in a tight loop.
    #[inline]
    pub fn set_retry_backoff_time(&mut self, time: Duration) {
        self.config.retry_backoff_time = time;
    }

    /// Retrieves the current `KafkaClient::set_retry_backoff_time`
    /// setting.
    #[must_use]
    pub fn retry_backoff_time(&self) -> Duration {
        self.config.retry_backoff_time
    }

    /// Specifies the upper limit of retry attempts for failed,
    /// repeatable operations against kafka.  This avoids retrying
    /// them forever.
    #[inline]
    pub fn set_retry_max_attempts(&mut self, attempts: u32) {
        self.config.retry_max_attempts = attempts;
    }

    /// Retrieves the current `KafkaClient::set_retry_max_attempts`
    /// setting.
    #[inline]
    #[must_use]
    pub fn retry_max_attempts(&self) -> u32 {
        self.config.retry_max_attempts
    }

    /// Specifies the timeout after which idle connections will
    /// transparently be closed/re-established by `KafkaClient`.
    ///
    /// To be effective this value must be smaller than the [remote
    /// broker's `connections.max.idle.ms`
    /// setting](https://kafka.apache.org/documentation.html#brokerconfigs).
    #[inline]
    pub fn set_connection_idle_timeout(&mut self, timeout: Duration) {
        self.conn_pool.set_idle_timeout(timeout);
    }

    /// Retrieves the current
    /// `KafkaClient::set_connection_idle_timeout` setting.
    #[inline]
    #[must_use]
    pub fn connection_idle_timeout(&self) -> Duration {
        self.conn_pool.idle_timeout()
    }

    #[cfg(feature = "producer_timestamp")]
    /// Sets the compression algorithm to use when sending out messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use rustfs_kafka::client::{Compression, KafkaClient};
    ///
    /// let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// client.set_producer_timestamp(Timestamp::CreateTime);
    /// ```
    #[inline]
    pub fn set_producer_timestamp(&mut self, producer_timestamp: Option<ProducerTimestamp>) {
        self.config.producer_timestamp = producer_timestamp;
    }

    #[cfg(feature = "producer_timestamp")]
    /// Retrieves the current `KafkaClient::producer_timestamp` setting.
    #[inline]
    #[must_use]
    pub fn producer_timestamp(&self) -> Option<ProducerTimestamp> {
        self.config.producer_timestamp
    }

    /// Provides a view onto the currently loaded metadata of known .
    ///
    /// # Examples
    /// ```no_run
    /// use rustfs_kafka::client::KafkaClient;
    /// use rustfs_kafka::client::metadata::Broker;
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// for topic in client.topics() {
    ///   for partition in topic.partitions() {
    ///     println!("{} #{} => {}", topic.name(), partition.id(),
    ///              partition.leader()
    ///                       .map(Broker::host)
    ///                       .unwrap_or("no-leader!"));
    ///   }
    /// }
    /// ```
    #[inline]
    #[must_use]
    pub fn topics(&self) -> metadata::Topics<'_> {
        metadata::Topics::new(self)
    }

    /// Resets and loads metadata for all topics from the underlying
    /// brokers.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = rustfs_kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// for topic in client.topics().names() {
    ///   println!("topic: {}", topic);
    /// }
    /// ```
    ///
    /// Returns the metadata for all loaded topics underlying this
    /// client.
    #[inline]
    pub fn load_metadata_all(&mut self) -> Result<()> {
        self.reset_metadata();
        self.load_metadata_kp::<&str>(&[])
    }

    /// Reloads metadata for a list of supplied topics.
    ///
    /// Note: if any of the specified topics does not exist yet on the
    /// underlying brokers and these have the [configuration for "auto
    /// create topics"
    /// enabled](https://kafka.apache.org/documentation.html#configuration),
    /// the remote kafka instance will create the yet missing topics
    /// on the fly as a result of explicitly loading their metadata.
    /// This is in contrast to other methods of this `KafkaClient`
    /// which will silently filter out requests to
    /// not-yet-loaded/not-yet-known topics and, thus, not cause
    /// topics to be automatically created.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = rustfs_kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let _ = client.load_metadata(&["my-topic"]).unwrap();
    /// ```
    ///
    /// Returns the metadata for _all_ loaded topics underlying this
    /// client (this might be more topics than specified right to this
    /// method call.)
    #[inline]
    pub fn load_metadata<T: AsRef<str>>(&mut self, topics: &[T]) -> Result<()> {
        self.load_metadata_kp(topics)
    }

    /// Reloads metadata using the kafka-protocol adapter (v1 protocol).
    /// This is the new path that will eventually replace `load_metadata`.
    pub fn load_metadata_kp<T: AsRef<str>>(&mut self, topics: &[T]) -> Result<()> {
        let resp = self.fetch_metadata_kp(topics)?;
        self.state.update_metadata(resp);
        Ok(())
    }

    /// Clears metadata stored in the client.  You must load metadata
    /// after this call if you want to use the client.
    #[inline]
    pub fn reset_metadata(&mut self) {
        self.state.clear_metadata();
    }

    /// Fetches metadata using the kafka-protocol adapter (protocol version 1).
    fn fetch_metadata_kp<T: AsRef<str>>(
        &mut self,
        topics: &[T],
    ) -> Result<protocol::metadata::MetadataResponseData> {
        let correlation = self.state.next_correlation_id();
        let now = Instant::now();
        let topic_strs: Vec<&str> = topics.iter().map(|t| t.as_ref()).collect();

        for host in &self.config.hosts {
            debug!("fetch_metadata_kp: requesting metadata from {}", host);
            match self.conn_pool.get_conn(host, now) {
                Ok(conn) => {
                    // Negotiate API versions on first connection to each broker
                    if !self.api_versions.contains(host) {
                        let av_correlation = self.state.next_correlation_id();
                        match crate::protocol::api_versions::fetch_api_versions(
                            conn, av_correlation, &self.config.client_id,
                        ) {
                            Ok(versions) => {
                                self.api_versions.insert(host.clone(), versions);
                            }
                            Err(e) => debug!(
                                "fetch_metadata_kp: API version negotiation failed for {}: {}",
                                host, e
                            ),
                        }
                    }

                    let (header, request) =
                        crate::protocol::metadata::build_metadata_request(correlation, &self.config.client_id, Some(&topic_strs));
                    match __kp_send_request(conn, &header, &request, crate::protocol::API_VERSION_METADATA) {
                        Ok(()) => {
                            match __kp_get_response::<kafka_protocol::messages::MetadataResponse>(conn, crate::protocol::API_VERSION_METADATA) {
                                Ok(kp_resp) => {
                                    return Ok(crate::protocol::metadata::convert_metadata_response(kp_resp, correlation));
                                }
                                Err(e) => debug!(
                                    "fetch_metadata_kp: failed to decode metadata from {}: {}",
                                    host, e
                                ),
                            }
                        }
                        Err(e) => debug!(
                            "fetch_metadata_kp: failed to request metadata from {}: {}",
                            host, e
                        ),
                    }
                }
                Err(e) => {
                    debug!("fetch_metadata_kp: failed to connect to {}: {}", host, e);
                }
            }
        }
        Err(Error::NoHostReachable)
    }

    /// Fetch offsets for a list of topics
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustfs_kafka::client::KafkaClient;
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// let topics: Vec<String> = client.topics().names().map(ToOwned::to_owned).collect();
    /// let offsets = client.fetch_offsets(&topics, rustfs_kafka::client::FetchOffset::Latest).unwrap();
    /// ```
    ///
    /// Returns a mapping of topic name to `PartitionOffset`s for each
    /// currently available partition of the corresponding topic.
    pub fn fetch_offsets<T: AsRef<str>>(
        &mut self,
        topics: &[T],
        offset: FetchOffset,
    ) -> Result<HashMap<String, Vec<PartitionOffset>>> {
        self.fetch_offsets_kp(topics, offset)
    }

    /// Fetch offsets for a list of topics
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustfs_kafka::client::{KafkaClient, FetchOffset};
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// let topics = vec!["test-topic".to_string()];
    /// let offsets = client.list_offsets(&topics, FetchOffset::ByTime(1698425676797));
    /// ```
    ///
    /// Returns a mapping of topic name to `TimestampedPartitionOffset`s.
    /// Each entry in the vector represents the timestamp, and the corresponding offset,
    /// that Kafka finds to be the first message with timestamp *later* than the passed in
    /// `FetchOffset` parameter.
    /// example: Ok({"test-topic": [`TimestampedPartitionOffset` { offset: 20, partition: 0, time: 1698425676798 } ]
    /// Note that the message might not be exactly at the given timestamp.
    pub fn list_offsets<T: AsRef<str>>(
        &mut self,
        topics: &[T],
        offset: FetchOffset,
    ) -> Result<HashMap<String, Vec<TimestampedPartitionOffset>>> {
        self.fetch_offsets_kp(topics, offset)
            .map(|m| {
                m.into_iter()
                    .map(|(topic, offsets)| {
                        let timestamped = offsets
                            .into_iter()
                            .map(|po| TimestampedPartitionOffset {
                                offset: po.offset,
                                partition: po.partition,
                                time: 0, // legacy method does not return real timestamps via _kp path
                            })
                            .collect();
                        (topic, timestamped)
                    })
                    .collect()
            })
    }

    /// Takes ownership back from the given `HashMap` Entry.
    fn get_key_from_entry<'a, K: 'a, V: 'a>(entry: hash_map::Entry<'a, K, V>) -> K {
        match entry {
            hash_map::Entry::Occupied(e) => e.remove_entry().0,
            hash_map::Entry::Vacant(e) => e.into_key(),
        }
    }

    /// Fetch offset for a single topic.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustfs_kafka::client::{KafkaClient, FetchOffset};
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// let offsets = client.fetch_topic_offsets("my-topic", FetchOffset::Latest).unwrap();
    /// ```
    ///
    /// Returns a vector of the offset data for each available partition.
    /// See also `KafkaClient::fetch_offsets`.
    pub fn fetch_topic_offsets<T: AsRef<str>>(
        &mut self,
        topic: T,
        offset: FetchOffset,
    ) -> Result<Vec<PartitionOffset>> {
        let topic = topic.as_ref();

        let mut m = self.fetch_offsets(&[topic], offset)?;
        let offs = m.remove(topic).unwrap_or_default();
        if offs.is_empty() {
            Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition))
        } else {
            Ok(offs)
        }
    }

    /// Fetch messages from Kafka (multiple topic, partitions).
    ///
    /// It takes a vector specifying the topic partitions and their
    /// offsets as of which to fetch messages.  Additionally, the
    /// default "max fetch size per partition" can be explicitly
    /// overridden if it is "defined" - this is, if `max_bytes` is
    /// greater than zero.
    ///
    /// Returns owned responses (no lifetimes).
    ///
    /// Note: before using this method consider using
    /// `rustfs_kafka::consumer::Consumer` instead which provides an easier
    /// to use API for the regular use-case of fetching messages from
    /// Kafka.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use rustfs_kafka::client::{KafkaClient, FetchPartition};
    ///
    /// let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// let reqs = &[FetchPartition::new("my-topic", 0, 0),
    ///              FetchPartition::new("my-topic-2", 0, 0).with_max_bytes(1024*1024)];
    /// let resps = client.fetch_messages(reqs).unwrap();
    /// ```
    /// See also `rustfs_kafka::consumer`.
    /// See also `KafkaClient::set_fetch_max_bytes_per_partition`.
    pub fn fetch_messages<'a, I, J>(
        &mut self,
        input: I,
    ) -> Result<Vec<fetch_kp::OwnedFetchResponse>>
    where
        J: AsRef<FetchPartition<'a>>,
        I: IntoIterator<Item = J>,
    {
        self.fetch_messages_kp(input)
    }

    /// Fetch messages from a single kafka partition.
    ///
    /// See `KafkaClient::fetch_messages`.
    pub fn fetch_messages_for_partition(
        &mut self,
        req: &FetchPartition<'_>,
    ) -> Result<Vec<fetch_kp::OwnedFetchResponse>> {
        self.fetch_messages_kp([req])
    }

    /// Fetch messages using the kafka-protocol adapter (protocol version 4).
    /// Returns owned responses without lifetimes.
    ///
    /// See `KafkaClient::fetch_messages` for parameter details.
    pub fn fetch_messages_kp<'a, I, J>(
        &mut self,
        input: I,
    ) -> Result<Vec<fetch_kp::OwnedFetchResponse>>
    where
        J: AsRef<FetchPartition<'a>>,
        I: IntoIterator<Item = J>,
    {
        let state = &mut self.state;
        let config = &self.config;
        let correlation = state.next_correlation_id();

        let mut broker_partitions: HashMap<&str, Vec<(&str, i32, i64, i32)>> = HashMap::new();
        for inp in input {
            let inp = inp.as_ref();
            if let Some(broker) = state.find_broker(inp.topic, inp.partition) {
                broker_partitions.entry(broker).or_default().push((
                    inp.topic,
                    inp.partition,
                    inp.offset,
                    if inp.max_bytes > 0 {
                        inp.max_bytes
                    } else {
                        config.fetch_max_bytes_per_partition
                    },
                ));
            }
        }

        __fetch_messages_kp(
            &mut self.conn_pool,
            correlation,
            &config.client_id,
            config.fetch_max_wait_time,
            config.fetch_min_bytes,
            broker_partitions,
        )
    }

    /// Send a message to Kafka
    ///
    /// `required_acks` - indicates how many acknowledgements the
    /// servers should receive before responding to the request
    ///
    /// `ack_timeout` - provides a maximum time in milliseconds the
    /// server can await the receipt of the number of acknowledgements
    /// in `required_acks`
    ///
    /// `input` - the set of `ProduceMessage`s to send
    ///
    /// Note: Unlike the higher-level `Producer` API, this method will
    /// *not* automatically determine the partition to deliver the
    /// message to.  It will strictly try to send the message to the
    /// specified partition.
    ///
    /// Note: Trying to send messages to non-existing topics or
    /// non-existing partitions will result in an error.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use rustfs_kafka::client::{KafkaClient, ProduceMessage, RequiredAcks};
    ///
    /// let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// let req = vec![ProduceMessage::new("my-topic", 0, None, Some("a".as_bytes())),
    ///                ProduceMessage::new("my-topic-2", 0, None, Some("b".as_bytes()))];
    /// let resp = client.produce_messages(RequiredAcks::One, Duration::from_millis(100), req);
    /// println!("{:?}", resp);
    /// ```
    ///
    /// The return value will contain a vector of topic, partition,
    /// offset and error if any OR error:Error.
    // XXX rework signaling an error; note that we need to either return the
    // messages which kafka failed to accept or otherwise tell the client about them
    pub fn produce_messages<'a, 'b, I, J>(
        &mut self,
        acks: RequiredAcks,
        ack_timeout: Duration,
        messages: I,
    ) -> Result<Vec<ProduceConfirm>>
    where
        J: AsRef<ProduceMessage<'a, 'b>>,
        I: IntoIterator<Item = J>,
    {
        self.produce_messages_kp(acks, ack_timeout, messages)
    }

    /// Produces messages using the kafka-protocol adapter (protocol version 3).
    /// This is the new path that will eventually replace `produce_messages`.
    pub fn produce_messages_kp<'a, 'b, I, J>(
        &mut self,
        acks: RequiredAcks,
        ack_timeout: Duration,
        messages: I,
    ) -> Result<Vec<ProduceConfirm>>
    where
        J: AsRef<ProduceMessage<'a, 'b>>,
        I: IntoIterator<Item = J>,
    {
        self.internal_produce_messages_kp(acks as i16, protocol::to_millis_i32(ack_timeout)?, messages)
    }

    fn internal_produce_messages_kp<'a, 'b, I, J>(
        &mut self,
        required_acks: i16,
        ack_timeout_ms: i32,
        messages: I,
    ) -> Result<Vec<ProduceConfirm>>
    where
        J: AsRef<ProduceMessage<'a, 'b>>,
        I: IntoIterator<Item = J>,
    {
        let state = &mut self.state;
        let correlation = state.next_correlation_id();
        let config = &self.config;

        // ~ map topic and partition to the corresponding brokers
        let mut broker_msgs: HashMap<&str, Vec<(&str, i32, Option<&'b [u8]>, Option<&'b [u8]>)>> =
            HashMap::new();
        for msg in messages {
            let msg = msg.as_ref();
            match state.find_broker(msg.topic, msg.partition) {
                None => return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)),
                Some(broker) => {
                    broker_msgs
                        .entry(broker)
                        .or_default()
                        .push((msg.topic, msg.partition, msg.key, msg.value));
                }
            }
        }

        __produce_messages_kp(
            &mut self.conn_pool,
            correlation,
            &config.client_id,
            required_acks,
            ack_timeout_ms,
            config.compression,
            broker_msgs,
            required_acks == 0,
        )
    }

    /// Commit offset for a topic partitions on behalf of a consumer group.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustfs_kafka::client::{KafkaClient, CommitOffset};
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// client.commit_offsets("my-group",
    ///     &[CommitOffset::new("my-topic", 0, 100),
    ///       CommitOffset::new("my-topic", 1, 99)])
    ///    .unwrap();
    /// ```
    ///
    /// In this example, we commit the offset 100 for the topic
    /// partition "my-topic:0" and 99 for the topic partition
    /// "my-topic:1".  Once successfully committed, these can then be
    /// retrieved using `fetch_group_offsets` even from another
    /// process or at much later point in time to resume comusing the
    /// topic partitions as of these offsets.
    pub fn commit_offsets<'a, J, I>(&mut self, group: &str, offsets: I) -> Result<()>
    where
        J: AsRef<CommitOffset<'a>>,
        I: IntoIterator<Item = J>,
    {
        self.commit_offsets_kp(group, offsets)
    }

    /// Commit offset of a particular topic partition on behalf of a
    /// consumer group.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustfs_kafka::client::KafkaClient;
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// client.commit_offset("my-group", "my-topic", 0, 100).unwrap();
    /// ```
    ///
    /// See also `KafkaClient::commit_offsets`.
    pub fn commit_offset(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        self.commit_offset_kp(group, topic, partition, offset)
    }

    /// Fetch offset for a specified list of topic partitions of a consumer group
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustfs_kafka::client::{KafkaClient, FetchGroupOffset};
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    ///
    /// let offsets =
    ///      client.fetch_group_offsets("my-group",
    ///             &[FetchGroupOffset::new("my-topic", 0),
    ///               FetchGroupOffset::new("my-topic", 1)])
    ///             .unwrap();
    /// ```
    ///
    /// See also `KafkaClient::fetch_group_topic_offset`.
    pub fn fetch_group_offsets<'a, J, I>(
        &mut self,
        group: &str,
        partitions: I,
    ) -> Result<HashMap<String, Vec<PartitionOffset>>>
    where
        J: AsRef<FetchGroupOffset<'a>>,
        I: IntoIterator<Item = J>,
    {
        self.fetch_group_offsets_kp(group, partitions)
    }

    /// Fetch offset for all partitions of a particular topic of a consumer group
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustfs_kafka::client::KafkaClient;
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// let offsets = client.fetch_group_topic_offset("my-group", "my-topic").unwrap();
    /// ```
    pub fn fetch_group_topic_offset(
        &mut self,
        group: &str,
        topic: &str,
    ) -> Result<Vec<PartitionOffset>> {
        self.fetch_group_topic_offset_kp(group, topic)
    }

    // =====================================================================
    // kafka-protocol adapter methods (protocol2)
    // =====================================================================

    /// Fetch offsets using the kafka-protocol adapter (ListOffsets v1).
    pub fn fetch_offsets_kp<T: AsRef<str>>(
        &mut self,
        topics: &[T],
        offset: FetchOffset,
    ) -> Result<HashMap<String, Vec<PartitionOffset>>> {
        let time = offset.to_kafka_value();
        let n_topics = topics.len();
        let state = &mut self.state;
        let correlation = state.next_correlation_id();
        let config = &self.config;

        let mut broker_partitions: HashMap<&str, Vec<(&str, i32, i64)>> = HashMap::new();
        for topic in topics {
            let topic = topic.as_ref();
            if let Some(ps) = state.partitions_for(topic) {
                for (id, host) in ps
                    .iter()
                    .filter_map(|(id, p)| p.broker(state).map(|b| (id, b.host())))
                {
                    broker_partitions
                        .entry(host)
                        .or_default()
                        .push((topic, id, time));
                }
            }
        }

        let now = Instant::now();
        let mut res: HashMap<String, Vec<PartitionOffset>> = HashMap::with_capacity(n_topics);
        for (host, partitions) in broker_partitions {
            let conn = self.conn_pool.get_conn(host, now)
                .map_err(|e| e.with_broker_context(host, "ListOffsets"))?;
            let (header, request) = crate::protocol::offset::build_list_offsets_request(
                correlation, &config.client_id, &partitions,
            );
            __kp_send_request(conn, &header, &request, crate::protocol::API_VERSION_LIST_OFFSETS)
                .map_err(|e| e.with_broker_context(host, "ListOffsets"))?;
            let kp_resp = __kp_get_response::<kafka_protocol::messages::ListOffsetsResponse>(
                conn,
                crate::protocol::API_VERSION_LIST_OFFSETS,
            ).map_err(|e| e.with_broker_context(host, "ListOffsets"))?;
            let our_resp = crate::protocol::offset::convert_list_offsets_response(kp_resp, correlation);

            for tp in our_resp.topic_partitions {
                let mut entry = res.entry(tp.topic);
                let mut new_resp_offsets = None;
                let mut err = None;
                {
                    let resp_offsets = match entry {
                        hash_map::Entry::Occupied(ref mut e) => e.get_mut(),
                        hash_map::Entry::Vacant(_) => {
                            new_resp_offsets.get_or_insert(Vec::with_capacity(tp.partitions.len()))
                        }
                    };
                    for p in tp.partitions {
                        let partition_offset = match p.to_offset() {
                            Ok(po) => po,
                            Err(code) => {
                                err = Some((p.partition, code));
                                break;
                            }
                        };
                        resp_offsets.push(partition_offset);
                    }
                }
                if let Some((partition, code)) = err {
                    let topic = KafkaClient::get_key_from_entry(entry);
                    return Err(Error::TopicPartitionError {
                        topic_name: topic,
                        partition_id: partition,
                        error_code: code,
                    });
                }
                if let hash_map::Entry::Vacant(e) = entry {
                    e.insert(new_resp_offsets.unwrap());
                }
            }
        }

        Ok(res)
    }

    /// Commit offsets using the kafka-protocol adapter (OffsetCommit v2).
    pub fn commit_offsets_kp<'a, J, I>(&mut self, group: &str, offsets: I) -> Result<()>
    where
        J: AsRef<CommitOffset<'a>>,
        I: IntoIterator<Item = J>,
    {
        let correlation_id = self.state.next_correlation_id();
        let config = &self.config;
        let mut offset_vec: Vec<(&str, i32, i64, Option<&str>)> = Vec::new();
        for o in offsets {
            let o = o.as_ref();
            if self.state.contains_topic_partition(o.topic, o.partition) {
                offset_vec.push((o.topic, o.partition, o.offset, None));
            } else {
                return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
            }
        }
        if offset_vec.is_empty() {
            debug!("commit_offsets_kp: no offsets provided");
            Ok(())
        } else {
            __commit_offsets_kp(
                &offset_vec,
                group,
                correlation_id,
                &config.client_id,
                &mut self.state,
                &mut self.conn_pool,
                config,
            )
        }
    }

    /// Commit a single offset using the kafka-protocol adapter.
    pub fn commit_offset_kp(&mut self, group: &str, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.commit_offsets_kp(group, &[CommitOffset::new(topic, partition, offset)])
    }

    /// Fetch group offsets using the kafka-protocol adapter (OffsetFetch v2).
    pub fn fetch_group_offsets_kp<'a, J, I>(
        &mut self,
        group: &str,
        partitions: I,
    ) -> Result<HashMap<String, Vec<PartitionOffset>>>
    where
        J: AsRef<FetchGroupOffset<'a>>,
        I: IntoIterator<Item = J>,
    {
        let correlation_id = self.state.next_correlation_id();
        let config = &self.config;
        let mut partition_vec: Vec<(&str, i32)> = Vec::new();
        for p in partitions {
            let p = p.as_ref();
            if self.state.contains_topic_partition(p.topic, p.partition) {
                partition_vec.push((p.topic, p.partition));
            } else {
                return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
            }
        }
        __fetch_group_offsets_kp(
            &partition_vec,
            group,
            correlation_id,
            &config.client_id,
            &mut self.state,
            &mut self.conn_pool,
            config,
        )
    }

    /// Fetch group topic offset using the kafka-protocol adapter.
    pub fn fetch_group_topic_offset_kp(
        &mut self,
        group: &str,
        topic: &str,
    ) -> Result<Vec<PartitionOffset>> {
        let correlation_id = self.state.next_correlation_id();
        let config = &self.config;
        let mut partition_vec: Vec<(&str, i32)> = Vec::new();
        match self.state.partitions_for(topic) {
            None => return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)),
            Some(tp) => {
                for (id, _) in tp {
                    partition_vec.push((topic, id));
                }
            }
        }
        __fetch_group_offsets_kp(
            &partition_vec,
            group,
            correlation_id,
            &config.client_id,
            &mut self.state,
            &mut self.conn_pool,
            config,
        )
        .map(|mut m| m.remove(topic).unwrap_or_default())
    }
}

impl KafkaClientInternals for KafkaClient {
    fn internal_produce_messages<'a, 'b, I, J>(
        &mut self,
        required_acks: i16,
        ack_timeout: i32,
        messages: I,
    ) -> Result<Vec<ProduceConfirm>>
    where
        J: AsRef<ProduceMessage<'a, 'b>>,
        I: IntoIterator<Item = J>,
    {
        self.internal_produce_messages_kp(required_acks, ack_timeout, messages)
    }
}

fn __fetch_messages_kp(
    conn_pool: &mut network::Connections,
    correlation_id: i32,
    client_id: &str,
    max_wait_ms: i32,
    min_bytes: i32,
    broker_partitions: HashMap<&str, Vec<(&str, i32, i64, i32)>>,
) -> Result<Vec<crate::protocol::fetch::OwnedFetchResponse>> {
    let now = Instant::now();
    let mut res = Vec::with_capacity(broker_partitions.len());
    for (host, partitions) in broker_partitions {
        let conn = conn_pool.get_conn(host, now)
            .map_err(|e| e.with_broker_context(host, "Fetch"))?;
        let (header, request) = crate::protocol::fetch::build_fetch_request(
            correlation_id,
            client_id,
            -1,
            max_wait_ms,
            min_bytes,
            0x7fffffff, // max_bytes for the whole fetch request
            &partitions,
        );
        __kp_send_request(conn, &header, &request, crate::protocol::API_VERSION_FETCH)
            .map_err(|e| e.with_broker_context(host, "Fetch"))?;
        let kp_resp = __kp_get_response::<kafka_protocol::messages::FetchResponse>(
            conn,
            crate::protocol::API_VERSION_FETCH,
        ).map_err(|e| e.with_broker_context(host, "Fetch"))?;
        let owned = crate::protocol::fetch::convert_fetch_response(kp_resp, correlation_id)?;
        res.push(owned);
    }
    Ok(res)
}

/// ~ carries out the given produce requests and returns the response
#[allow(clippy::similar_names)]
fn __produce_messages_kp(
    conn_pool: &mut network::Connections,
    correlation_id: i32,
    client_id: &str,
    required_acks: i16,
    ack_timeout_ms: i32,
    compression: Compression,
    broker_msgs: HashMap<&str, Vec<(&str, i32, Option<&[u8]>, Option<&[u8]>)>>,
    no_acks: bool,
) -> Result<Vec<ProduceConfirm>> {
    let now = Instant::now();
    if no_acks {
        for (host, msgs) in broker_msgs {
            let conn = conn_pool.get_conn(host, now)
                .map_err(|e| e.with_broker_context(host, "Produce"))?;
            let (header, request) = crate::protocol::produce::build_produce_request(
                correlation_id,
                client_id,
                required_acks,
                ack_timeout_ms,
                compression,
                &msgs,
            );
            __kp_send_request(conn, &header, &request, crate::protocol::API_VERSION_PRODUCE)
                .map_err(|e| e.with_broker_context(host, "Produce"))?;
        }
        Ok(vec![])
    } else {
        let mut res: Vec<ProduceConfirm> = vec![];
        for (host, msgs) in broker_msgs {
            let conn = conn_pool.get_conn(host, now)
                .map_err(|e| e.with_broker_context(host, "Produce"))?;
            let (header, request) = crate::protocol::produce::build_produce_request(
                correlation_id,
                client_id,
                required_acks,
                ack_timeout_ms,
                compression,
                &msgs,
            );
            __kp_send_request(conn, &header, &request, crate::protocol::API_VERSION_PRODUCE)
                .map_err(|e| e.with_broker_context(host, "Produce"))?;
            let kp_resp = __kp_get_response::<kafka_protocol::messages::ProduceResponse>(
                conn,
                crate::protocol::API_VERSION_PRODUCE,
            ).map_err(|e| e.with_broker_context(host, "Produce"))?;
            let our_resp = crate::protocol::produce::convert_produce_response(kp_resp, correlation_id);
            for tpo in our_resp.get_response() {
                res.push(tpo);
            }
        }
        Ok(res)
    }
}

fn __get_response_size(conn: &mut network::KafkaConnection) -> Result<i32> {
    let mut buf = [0u8; 4];
    conn.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

// ---------------------------------------------------------------------------
// kafka-protocol transport functions (Phase 3+)
// These use Encodable/Decodable traits from kafka_protocol with BytesMut/Bytes.

fn __kp_send_request(
    conn: &mut network::KafkaConnection,
    header: &kafka_protocol::messages::RequestHeader,
    body: &impl kafka_protocol::protocol::Encodable,
    api_version: i16,
) -> Result<()> {
    use bytes::BytesMut;
    use kafka_protocol::protocol::Encodable;

    let mut header_buf = BytesMut::new();
    header.encode(&mut header_buf, api_version).map_err(|_| Error::CodecError)?;

    let mut body_buf = BytesMut::new();
    body.encode(&mut body_buf, api_version).map_err(|_| Error::CodecError)?;

    let total_len = (header_buf.len() + body_buf.len()) as i32;
    let mut out = BytesMut::with_capacity(4 + total_len as usize);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body_buf);

    trace!("__kp_send_request: sending {} bytes", out.len());
    conn.send(&out)?;
    Ok(())
}

fn __kp_get_response<R: kafka_protocol::protocol::Decodable>(
    conn: &mut network::KafkaConnection,
    api_version: i16,
) -> Result<R> {
    use bytes::Bytes;
    use kafka_protocol::messages::ResponseHeader;
    use kafka_protocol::protocol::Decodable;

    let size = __get_response_size(conn)?;
    let resp_bytes = conn.read_exact_alloc(size as u64)?;

    let mut bytes = Bytes::from(resp_bytes);
    let _resp_header = ResponseHeader::decode(&mut bytes, api_version).map_err(|_| Error::CodecError)?;

    R::decode(&mut bytes, api_version).map_err(|_| Error::CodecError)
}

/// Suspends the calling thread for the configured "retry" time. This
/// method should be called _only_ as part of a retry attempt.
fn __retry_sleep(cfg: &ClientConfig) {
    thread::sleep(cfg.retry_backoff_time);
}

// =====================================================================
// kafka-protocol adapter free functions (protocol2)
// =====================================================================

fn __get_group_coordinator_kp<'a>(
    group: &str,
    state: &'a mut state::ClientState,
    conn_pool: &mut network::Connections,
    config: &ClientConfig,
    now: Instant,
) -> Result<&'a str> {
    if let Some(host) = state.group_coordinator(group) {
        return Ok(unsafe { mem::transmute(host) });
    }
    let correlation_id = state.next_correlation_id();
    let (header, request) =
        crate::protocol::consumer::build_find_coordinator_request(correlation_id, &config.client_id, group);
    let mut attempt = 1;
    loop {
        let conn = conn_pool.get_conn_any(now).expect("available connection");
        debug!(
            "get_group_coordinator_kp: asking for coordinator of '{}' on: {:?}",
            group, conn
        );
        __kp_send_request(conn, &header, &request, crate::protocol::API_VERSION_FIND_COORDINATOR)
            .map_err(|e| e.with_broker_context("any", "FindCoordinator"))?;
        let kp_resp = __kp_get_response::<kafka_protocol::messages::FindCoordinatorResponse>(
            conn,
            crate::protocol::API_VERSION_FIND_COORDINATOR,
        ).map_err(|e| e.with_broker_context("any", "FindCoordinator"))?;
        let r = crate::protocol::consumer::convert_find_coordinator_response(kp_resp, correlation_id);
        let retry_code = match r.error {
            0 => {
                let gc = protocol::consumer::GroupCoordinatorResponse {
                    header: protocol::HeaderResponse { correlation: correlation_id },
                    error: r.error,
                    broker_id: r.broker_id,
                    port: r.port,
                    host: r.host,
                };
                return Ok(state.set_group_coordinator(group, &gc));
            }
            e if KafkaCode::from_protocol(e) == Some(KafkaCode::GroupCoordinatorNotAvailable) => e,
            e => {
                if let Some(code) = KafkaCode::from_protocol(e) {
                    return Err(Error::Kafka(code));
                }
                return Err(Error::Kafka(KafkaCode::Unknown));
            }
        };
        if attempt < config.retry_max_attempts {
            debug!(
                "get_group_coordinator_kp: will retry request (c: {}) due to: {:?}",
                correlation_id, retry_code
            );
            attempt += 1;
            __retry_sleep(config);
        } else {
            return Err(Error::Kafka(
                KafkaCode::from_protocol(retry_code).unwrap_or(KafkaCode::Unknown),
            ));
        }
    }
}

fn __commit_offsets_kp(
    offsets: &[(&str, i32, i64, Option<&str>)],
    group: &str,
    correlation_id: i32,
    client_id: &str,
    state: &mut state::ClientState,
    conn_pool: &mut network::Connections,
    config: &ClientConfig,
) -> Result<()> {
    let mut attempt = 1;
    loop {
        let now = Instant::now();
        let host = __get_group_coordinator_kp(group, state, conn_pool, config, now)?;
        debug!("commit_offsets_kp: sending request to: {}", host);

        let conn = conn_pool.get_conn(host, now)
            .map_err(|e| e.with_broker_context(host, "OffsetCommit"))?;
        let (header, request) = crate::protocol::consumer::build_offset_commit_request(
            correlation_id,
            client_id,
            group,
            -1, // generation_id
            "", // member_id
            -1, // retention_time_ms
            offsets,
        );
        __kp_send_request(conn, &header, &request, crate::protocol::API_VERSION_OFFSET_COMMIT)
            .map_err(|e| e.with_broker_context(host, "OffsetCommit"))?;
        let kp_resp = __kp_get_response::<kafka_protocol::messages::OffsetCommitResponse>(
            conn,
            crate::protocol::API_VERSION_OFFSET_COMMIT,
        ).map_err(|e| e.with_broker_context(host, "OffsetCommit"))?;
        let our_resp = crate::protocol::consumer::convert_offset_commit_response(kp_resp, correlation_id);

        let mut retry_code = None;
        'rproc: for tp in &our_resp.topic_partitions {
            for p in &tp.partitions {
                match KafkaCode::from_protocol(p.error) {
                    None => {}
                    Some(e @ KafkaCode::GroupLoadInProgress) => {
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Some(e @ KafkaCode::NotCoordinatorForGroup) => {
                        debug!("commit_offsets_kp: resetting group coordinator for '{}'", group);
                        state.remove_group_coordinator(group);
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Some(code) => return Err(Error::Kafka(code)),
                }
            }
        }
        match retry_code {
            Some(e) => {
                if attempt < config.retry_max_attempts {
                    debug!(
                        "commit_offsets_kp: will retry request (c: {}) due to: {:?}",
                        correlation_id, e
                    );
                    attempt += 1;
                    __retry_sleep(config);
                } else {
                    return Err(Error::Kafka(e));
                }
            }
            None => return Ok(()),
        }
    }
}

fn __fetch_group_offsets_kp(
    partitions: &[(&str, i32)],
    group: &str,
    correlation_id: i32,
    client_id: &str,
    state: &mut state::ClientState,
    conn_pool: &mut network::Connections,
    config: &ClientConfig,
) -> Result<HashMap<String, Vec<PartitionOffset>>> {
    let mut attempt = 1;
    loop {
        let now = Instant::now();
        let host = __get_group_coordinator_kp(group, state, conn_pool, config, now)?;
        debug!("fetch_group_offsets_kp: sending request to: {}", host);

        let conn = conn_pool.get_conn(host, now)
            .map_err(|e| e.with_broker_context(host, "OffsetFetch"))?;
        let (header, request) = crate::protocol::consumer::build_offset_fetch_request(
            correlation_id,
            client_id,
            group,
            partitions,
        );
        __kp_send_request(conn, &header, &request, crate::protocol::API_VERSION_OFFSET_FETCH)
            .map_err(|e| e.with_broker_context(host, "OffsetFetch"))?;
        let kp_resp = __kp_get_response::<kafka_protocol::messages::OffsetFetchResponse>(
            conn,
            crate::protocol::API_VERSION_OFFSET_FETCH,
        ).map_err(|e| e.with_broker_context(host, "OffsetFetch"))?;
        let our_resp = crate::protocol::consumer::convert_offset_fetch_response(kp_resp, correlation_id);

        let mut retry_code = None;
        let mut topic_map = HashMap::with_capacity(our_resp.topic_partitions.len());

        'rproc: for tp in our_resp.topic_partitions {
            let mut partition_offsets = Vec::with_capacity(tp.partitions.len());
            for p in tp.partitions {
                match KafkaCode::from_protocol(p.error) {
                    None => {
                        partition_offsets.push(PartitionOffset {
                            offset: p.offset,
                            partition: p.partition,
                        });
                    }
                    Some(e @ KafkaCode::GroupLoadInProgress) => {
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Some(e @ KafkaCode::NotCoordinatorForGroup) => {
                        debug!("fetch_group_offsets_kp: resetting group coordinator for '{}'", group);
                        state.remove_group_coordinator(group);
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Some(e) => return Err(Error::Kafka(e)),
                }
            }
            topic_map.insert(tp.topic, partition_offsets);
        }

        match retry_code {
            Some(e) => {
                if attempt < config.retry_max_attempts {
                    debug!(
                        "fetch_group_offsets_kp: will retry request (c: {}) due to: {:?}",
                        correlation_id, e
                    );
                    attempt += 1;
                    __retry_sleep(config);
                } else {
                    return Err(Error::Kafka(e));
                }
            }
            None => return Ok(topic_map),
        }
    }
}
