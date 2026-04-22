//! Kafka Client - A mid-level abstraction for a kafka cluster
//! allowing building higher level constructs.
//!
//! The entry point into this module is `KafkaClient` obtained by a
//! call to `KafkaClient::new()`.
//!
//! `KafkaClient` is a synchronous, general-purpose Kafka client supporting:
//!
//! - **Message production** via `produce_messages()`
//! - **Message consumption** via `fetch_messages()`
//! - **Metadata queries** via `load_metadata_all()` / `load_metadata()`
//! - **Offset management** via `fetch_offsets()` / `commit_offsets()`
//! - **Topic management** via `create_topics()` / `delete_topics()`
//!
//! # Examples
//!
//! ```no_run
//! use rustfs_kafka::client::KafkaClient;
//!
//! let mut client = KafkaClient::builder()
//!     .with_hosts(vec!["localhost:9092".to_owned()])
//!     .with_client_id("my-app".to_owned())
//!     .build();
//! client.load_metadata_all().unwrap();
//! ```
//!
//! # Security
//!
//! Use `KafkaClient::new_secure()` or `KafkaClient::builder().with_security()`
//! for TLS-encrypted connections:
//!
//! ```no_run
//! # #[cfg(feature = "security")]
//! # {
//! use rustfs_kafka::client::{KafkaClient, SecurityConfig};
//!
//! let mut client = KafkaClient::new_secure(
//!     vec!["localhost:9093".to_owned()],
//!     SecurityConfig::new()
//!         .with_ca_cert("ca.pem".to_owned()),
//! );
//! client.load_metadata_all().unwrap();
//! # }
//! ```

// pub re-exports
pub use crate::compression::Compression;
pub use crate::protocol::create_topics::{CreateTopicsResponseData, TopicConfig, TopicResult};
pub use crate::protocol::delete_topics::{DeleteTopicResult, DeleteTopicsResponseData};
#[cfg(feature = "producer_timestamp")]
pub use crate::protocol::produce::ProducerTimestamp;
pub use crate::utils::PartitionOffset;
use crate::utils::TimestampedPartitionOffset;
use std::collections::hash_map::HashMap;
use std::time::Duration;

#[cfg(feature = "security")]
pub use crate::network::SecurityConfig;

use crate::error::{Error, KafkaCode, Result};
use crate::protocol;

pub mod builder;
pub mod config;
pub(crate) mod fetch_ops;
mod internals;
pub mod metadata;
pub(crate) mod metadata_ops;
pub(crate) mod offset_ops;
pub(crate) mod produce_ops;
mod state;
pub(crate) mod transport;

use crate::network;

#[allow(clippy::wildcard_imports)]
pub use config::*;
pub(crate) use internals::KafkaClientInternals;

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

use config::ClientConfig;

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
    /// messages sent to it at all.
    None = 0,
    /// Requires the receiving Kafka broker to wait until the sent
    /// messages are written to local disk.
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
    /// Optional headers for this message.
    pub headers: &'b [(String, bytes::Bytes)],
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
            headers: &[],
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
    /// partition.)
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

/// Client struct keeping track of brokers and topic metadata.
///
/// Implements methods described by the [Kafka Protocol](http://kafka.apache.org/protocol.html).
///
/// You will have to load metadata before making any other request.
#[derive(Debug)]
pub struct KafkaClient {
    config: ClientConfig,
    conn_pool: network::Connections,
    state: state::ClientState,
    api_versions: crate::protocol::api_versions::ApiVersionCache,
}

impl KafkaClient {
    /// Creates a new `KafkaClientBuilder` with default settings.
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
    pub fn builder() -> builder::KafkaClientBuilder {
        builder::KafkaClientBuilder::new()
    }

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
        Self::builder().with_hosts(hosts).build()
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
    #[cfg(feature = "security")]
    #[must_use]
    pub fn new_secure(hosts: Vec<String>, security: SecurityConfig) -> KafkaClient {
        Self::builder()
            .with_hosts(hosts)
            .with_security(security)
            .build()
    }

    #[inline]
    #[must_use]
    pub fn hosts(&self) -> &[String] {
        &self.config.hosts
    }

    pub fn set_client_id(&mut self, client_id: String) {
        self.config.client_id = client_id;
    }

    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.config.client_id
    }

    #[inline]
    pub fn set_compression(&mut self, compression: Compression) {
        self.config.compression = compression;
    }

    #[inline]
    #[must_use]
    pub fn compression(&self) -> Compression {
        self.config.compression
    }

    #[inline]
    /// Sets the max wait time used by fetch requests.
    ///
    /// # Errors
    ///
    /// Returns an error if `max_wait_time` cannot be represented in protocol milliseconds.
    pub fn set_fetch_max_wait_time(&mut self, max_wait_time: Duration) -> Result<()> {
        self.config.fetch.max_wait_time = protocol::to_millis_i32(max_wait_time)?;
        Ok(())
    }

    #[inline]
    #[must_use]
    pub fn fetch_max_wait_time(&self) -> Duration {
        let millis = u64::try_from(self.config.fetch.max_wait_time).unwrap_or_default();
        Duration::from_millis(millis)
    }

    #[inline]
    pub fn set_fetch_min_bytes(&mut self, min_bytes: i32) {
        self.config.fetch.min_bytes = min_bytes;
    }

    #[inline]
    #[must_use]
    pub fn fetch_min_bytes(&self) -> i32 {
        self.config.fetch.min_bytes
    }

    #[inline]
    pub fn set_fetch_max_bytes_per_partition(&mut self, max_bytes: i32) {
        self.config.fetch.max_bytes_per_partition = max_bytes;
    }

    #[inline]
    #[must_use]
    pub fn fetch_max_bytes_per_partition(&self) -> i32 {
        self.config.fetch.max_bytes_per_partition
    }

    #[inline]
    pub fn set_fetch_crc_validation(&mut self, validate_crc: bool) {
        self.config.fetch.crc_validation = validate_crc;
    }

    #[inline]
    #[must_use]
    pub fn fetch_crc_validation(&self) -> bool {
        self.config.fetch.crc_validation
    }

    #[inline]
    pub fn set_group_offset_storage(&mut self, storage: Option<GroupOffsetStorage>) {
        self.config.offset_storage = storage;
    }

    #[must_use]
    pub fn group_offset_storage(&self) -> Option<GroupOffsetStorage> {
        self.config.offset_storage
    }

    #[inline]
    pub fn set_retry_backoff_time(&mut self, time: Duration) {
        match &mut self.config.retry.policy {
            config::RetryPolicy::Exponential { initial, .. } => *initial = time,
            config::RetryPolicy::Fixed { interval, .. } => *interval = time,
            config::RetryPolicy::None => {}
        }
    }

    #[inline]
    #[must_use]
    pub fn retry_max_attempts(&self) -> u32 {
        self.config.retry.policy.max_attempts()
    }

    #[inline]
    pub fn set_connection_idle_timeout(&mut self, timeout: Duration) {
        self.conn_pool.set_idle_timeout(timeout);
    }

    #[inline]
    #[must_use]
    pub fn connection_idle_timeout(&self) -> Duration {
        self.conn_pool.idle_timeout()
    }

    #[cfg(feature = "producer_timestamp")]
    #[inline]
    pub fn set_producer_timestamp(&mut self, producer_timestamp: Option<ProducerTimestamp>) {
        self.config.producer_timestamp = producer_timestamp;
    }

    #[cfg(feature = "producer_timestamp")]
    #[inline]
    #[must_use]
    pub fn producer_timestamp(&self) -> Option<ProducerTimestamp> {
        self.config.producer_timestamp
    }

    #[inline]
    #[must_use]
    pub fn topics(&self) -> metadata::Topics<'_> {
        metadata::Topics::new(self)
    }

    // -- metadata operations (delegated to metadata_ops.rs) --

    /// Resets and loads metadata for all topics from the underlying brokers.
    ///
    /// # Errors
    ///
    /// Returns an error if no broker is reachable or metadata loading fails.
    #[inline]
    pub fn load_metadata_all(&mut self) -> Result<()> {
        metadata_ops::load_metadata_all(self)
    }

    /// Reloads metadata for a list of supplied topics.
    ///
    /// # Errors
    ///
    /// Returns an error if no broker is reachable or metadata loading fails.
    #[inline]
    pub fn load_metadata<T: AsRef<str>>(&mut self, topics: &[T]) -> Result<()> {
        metadata_ops::load_metadata(self, topics)
    }

    /// Reloads metadata using the kafka-protocol adapter (v1 protocol).
    ///
    /// # Errors
    ///
    /// Returns an error if no broker is reachable or metadata loading fails.
    pub fn load_metadata_kp<T: AsRef<str>>(&mut self, topics: &[T]) -> Result<()> {
        metadata_ops::load_metadata_kp(self, topics)
    }

    /// Clears metadata stored in the client.
    #[inline]
    pub fn reset_metadata(&mut self) {
        metadata_ops::reset_metadata(self);
    }

    /// Fetch offsets for a list of topics.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata or list-offset requests fail.
    pub fn fetch_offsets<T: AsRef<str>>(
        &mut self,
        topics: &[T],
        offset: FetchOffset,
    ) -> Result<HashMap<String, Vec<PartitionOffset>>> {
        metadata_ops::fetch_offsets(self, topics, offset)
    }

    /// Fetch offsets for a list of topics with timestamps.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata or list-offset requests fail.
    pub fn list_offsets<T: AsRef<str>>(
        &mut self,
        topics: &[T],
        offset: FetchOffset,
    ) -> Result<HashMap<String, Vec<TimestampedPartitionOffset>>> {
        metadata_ops::list_offsets(self, topics, offset)
    }

    /// Fetch offset for a single topic.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata or list-offset requests fail.
    pub fn fetch_topic_offsets<T: AsRef<str>>(
        &mut self,
        topic: T,
        offset: FetchOffset,
    ) -> Result<Vec<PartitionOffset>> {
        metadata_ops::fetch_topic_offsets(self, topic, offset)
    }

    /// Fetch offsets using the kafka-protocol adapter (`ListOffsets` v1).
    ///
    /// # Errors
    ///
    /// Returns an error if metadata or list-offset requests fail.
    pub fn fetch_offsets_kp<T: AsRef<str>>(
        &mut self,
        topics: &[T],
        offset: FetchOffset,
    ) -> Result<HashMap<String, Vec<PartitionOffset>>> {
        metadata_ops::fetch_offsets_kp(self, topics, offset)
    }

    // -- topic administration --

    /// Creates one or more topics.
    ///
    /// The request is attempted against configured brokers until one succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if timeout conversion fails, brokers are unreachable, or topic creation fails.
    pub fn create_topics(
        &mut self,
        topics: &[TopicConfig],
        timeout: Duration,
    ) -> Result<CreateTopicsResponseData> {
        let correlation_id = self.state.next_correlation_id();
        let timeout_ms = protocol::to_millis_i32(timeout)?;
        let now = std::time::Instant::now();
        let hosts = self.config.hosts.clone();
        let mut last_err: Option<Error> = None;

        for host in hosts {
            let conn = match self.conn_pool.get_conn(&host, now) {
                Ok(conn) => conn,
                Err(e) => {
                    last_err = Some(e.with_broker_context(&host, "CreateTopics"));
                    continue;
                }
            };

            match crate::protocol::create_topics::fetch_create_topics(
                conn,
                correlation_id,
                &self.config.client_id,
                topics,
                timeout_ms,
            ) {
                Ok(resp) => return Ok(resp),
                Err(e) => last_err = Some(e.with_broker_context(&host, "CreateTopics")),
            }
        }

        Err(last_err.unwrap_or_else(Error::no_host_reachable))
    }

    /// Deletes one or more topics by name.
    ///
    /// The request is attempted against configured brokers until one succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if timeout conversion fails, brokers are unreachable, or topic deletion fails.
    pub fn delete_topics(
        &mut self,
        topic_names: &[&str],
        timeout: Duration,
    ) -> Result<DeleteTopicsResponseData> {
        let correlation_id = self.state.next_correlation_id();
        let timeout_ms = protocol::to_millis_i32(timeout)?;
        let now = std::time::Instant::now();
        let hosts = self.config.hosts.clone();
        let mut last_err: Option<Error> = None;

        for host in hosts {
            let conn = match self.conn_pool.get_conn(&host, now) {
                Ok(conn) => conn,
                Err(e) => {
                    last_err = Some(e.with_broker_context(&host, "DeleteTopics"));
                    continue;
                }
            };

            match crate::protocol::delete_topics::fetch_delete_topics(
                conn,
                correlation_id,
                &self.config.client_id,
                topic_names,
                timeout_ms,
            ) {
                Ok(resp) => return Ok(resp),
                Err(e) => last_err = Some(e.with_broker_context(&host, "DeleteTopics")),
            }
        }

        Err(last_err.unwrap_or_else(Error::no_host_reachable))
    }

    // -- fetch operations (delegated to fetch_ops.rs) --

    /// Fetch messages from Kafka (multiple topic, partitions).
    ///
    /// # Errors
    ///
    /// Returns an error if metadata lookup, fetch request construction, or broker I/O fails.
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
    /// # Errors
    ///
    /// Returns an error if metadata lookup, fetch request construction, or broker I/O fails.
    pub fn fetch_messages_for_partition(
        &mut self,
        req: &FetchPartition<'_>,
    ) -> Result<Vec<fetch_kp::OwnedFetchResponse>> {
        self.fetch_messages_kp([req])
    }

    /// Fetch messages using the kafka-protocol adapter (protocol version 4).
    ///
    /// # Errors
    ///
    /// Returns an error if metadata lookup, fetch request construction, or broker I/O fails.
    pub fn fetch_messages_kp<'a, I, J>(
        &mut self,
        input: I,
    ) -> Result<Vec<fetch_kp::OwnedFetchResponse>>
    where
        J: AsRef<FetchPartition<'a>>,
        I: IntoIterator<Item = J>,
    {
        let correlation = self.state.next_correlation_id();
        fetch_ops::fetch_messages_kp(
            &mut self.conn_pool,
            &mut self.state,
            &self.config,
            correlation,
            input,
        )
    }

    // -- produce operations (delegated to produce_ops.rs) --

    /// Send a message to Kafka.
    ///
    /// # Errors
    ///
    /// Returns an error if partitioning, request serialization, or broker produce calls fail.
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
    ///
    /// # Errors
    ///
    /// Returns an error if partitioning, request serialization, or broker produce calls fail.
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
        produce_ops::internal_produce_messages_kp(
            &mut self.conn_pool,
            &mut self.state,
            &self.config,
            acks,
            ack_timeout,
            messages,
        )
    }

    // -- offset operations (delegated to offset_ops.rs) --

    /// Commit offset for a topic partitions on behalf of a consumer group.
    ///
    /// # Errors
    ///
    /// Returns an error if offset commit request building or broker communication fails.
    pub fn commit_offsets<'a, J, I>(&mut self, group: &str, offsets: I) -> Result<()>
    where
        J: AsRef<CommitOffset<'a>>,
        I: IntoIterator<Item = J>,
    {
        self.commit_offsets_kp(group, offsets)
    }

    /// Commit offset of a particular topic partition on behalf of a consumer group.
    ///
    /// # Errors
    ///
    /// Returns an error if offset commit request building or broker communication fails.
    pub fn commit_offset(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        self.commit_offset_kp(group, topic, partition, offset)
    }

    /// Fetch offset for a specified list of topic partitions of a consumer group.
    ///
    /// # Errors
    ///
    /// Returns an error if offset fetch request building or broker communication fails.
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

    /// Fetch offset for all partitions of a particular topic of a consumer group.
    ///
    /// # Errors
    ///
    /// Returns an error if offset fetch request building or broker communication fails.
    pub fn fetch_group_topic_offset(
        &mut self,
        group: &str,
        topic: &str,
    ) -> Result<Vec<PartitionOffset>> {
        self.fetch_group_topic_offset_kp(group, topic)
    }

    /// Commit offsets using the kafka-protocol adapter (`OffsetCommit` v2).
    ///
    /// # Errors
    ///
    /// Returns an error if offset commit request building or broker communication fails.
    pub fn commit_offsets_kp<'a, J, I>(&mut self, group: &str, offsets: I) -> Result<()>
    where
        J: AsRef<CommitOffset<'a>>,
        I: IntoIterator<Item = J>,
    {
        let correlation_id = self.state.next_correlation_id();
        offset_ops::commit_offsets_kp(
            offsets,
            group,
            correlation_id,
            &self.config.client_id,
            &mut self.state,
            &mut self.conn_pool,
            &self.config,
        )
    }

    /// Commit a single offset using the kafka-protocol adapter.
    ///
    /// # Errors
    ///
    /// Returns an error if offset commit request building or broker communication fails.
    pub fn commit_offset_kp(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        self.commit_offsets_kp(group, &[CommitOffset::new(topic, partition, offset)])
    }

    /// Fetch group offsets using the kafka-protocol adapter (`OffsetFetch` v2).
    ///
    /// # Errors
    ///
    /// Returns an error if offset fetch request building or broker communication fails.
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
        offset_ops::fetch_group_offsets_kp(
            partitions,
            group,
            correlation_id,
            &self.config.client_id,
            &mut self.state,
            &mut self.conn_pool,
            &self.config,
        )
    }

    /// Fetch group topic offset using the kafka-protocol adapter.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic is unknown or offset fetch broker calls fail.
    pub fn fetch_group_topic_offset_kp(
        &mut self,
        group: &str,
        topic: &str,
    ) -> Result<Vec<PartitionOffset>> {
        let correlation_id = self.state.next_correlation_id();
        let mut partition_vec: Vec<FetchGroupOffset<'_>> = Vec::new();
        match self.state.partitions_for(topic) {
            None => return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)),
            Some(tp) => {
                for (id, _) in tp {
                    partition_vec.push(FetchGroupOffset::new(topic, id));
                }
            }
        }
        offset_ops::fetch_group_offsets_kp(
            partition_vec,
            group,
            correlation_id,
            &self.config.client_id,
            &mut self.state,
            &mut self.conn_pool,
            &self.config,
        )
        .map(|mut m| m.remove(topic).unwrap_or_default())
    }

    /// Returns the host of the group coordinator for the given group, if known.
    #[must_use]
    pub fn group_coordinator_host(&self, group: &str) -> Option<String> {
        self.state
            .group_coordinator(group)
            .map(std::borrow::ToOwned::to_owned)
    }

    /// Gets the next correlation ID for request tracking.
    pub fn next_correlation_id(&mut self) -> i32 {
        self.state.next_correlation_id()
    }

    /// Gets a mutable connection to the specified host.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no reachable connection for the given host.
    pub fn get_conn_mut(&mut self, host: &str) -> Result<&mut network::KafkaConnection> {
        self.conn_pool.get_conn(host, std::time::Instant::now())
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
        let acks = match required_acks {
            0 => RequiredAcks::None,
            1 => RequiredAcks::One,
            -1 => RequiredAcks::All,
            _ => RequiredAcks::None,
        };
        produce_ops::internal_produce_messages_kp(
            &mut self.conn_pool,
            &mut self.state,
            &self.config,
            acks,
            Duration::from_millis(u64::try_from(ack_timeout).unwrap_or_default()),
            messages,
        )
    }
}
