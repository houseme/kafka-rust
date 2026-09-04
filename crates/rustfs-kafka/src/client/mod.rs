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
//! - **Cluster, ACL, config, token, quota, SCRAM credential, broker storage, producer, transaction, API version, and group inspection** via
//!   `describe_cluster()` / `describe_acls()` / `describe_configs()` / `describe_log_dirs()` /
//!   `describe_delegation_tokens()` / `describe_client_quotas()` / `describe_user_scram_credentials()` /
//!   `describe_quorum()` / `list_config_resources()` / `describe_topic_partitions()` / `describe_producers()` /
//!   `list_transactions()` / `fetch_api_versions()` / `list_groups()` / `describe_groups()` / `describe_consumer_groups()`
//!   / `describe_share_groups()` / `describe_share_group_offsets()` / `assign_replicas_to_dirs()` /
//!   `add_raft_voter()` / `remove_raft_voter()` / `update_raft_voter()`
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

use crate::utils::TimestampedPartitionOffset;
use std::collections::hash_map::HashMap;
use std::time::Duration;

use crate::error::{Error, KafkaCode, Result};
use crate::protocol;

/// Builder utilities for constructing `KafkaClient` instances.
pub mod builder;

mod admin_ops;
/// Configuration types and defaults for the Kafka client.
pub mod config;
pub(crate) mod fetch_ops;
mod internals;
pub mod metadata;
pub(crate) mod metadata_ops;
pub(crate) mod offset_ops;
pub(crate) mod produce_ops;
mod raw;
mod reexports;
mod state;
pub(crate) mod transport;
mod types;

use crate::network;

#[allow(clippy::wildcard_imports)]
pub use config::*;
pub(crate) use internals::KafkaClientInternals;
pub use reexports::*;
pub use types::*;

/// Owned fetch response types from the kafka-protocol adapter.
/// These types own their data (no lifetimes) and are returned by
/// `KafkaClient::fetch_messages_kp`.
pub mod fetch_kp {
    pub use crate::protocol::fetch::{
        OwnedData, OwnedFetchResponse, OwnedMessage, OwnedPartition, OwnedTopic,
    };
}

/// Types for fetch responses adapted from the `kafka-protocol` crate.
pub mod fetch {
    pub use crate::protocol::fetch::OwnedFetchResponse as Response;
    pub use crate::protocol::fetch::{OwnedData, OwnedMessage, OwnedPartition, OwnedTopic};
}

use config::ClientConfig;
use config::RetryPolicy::{Exponential, Fixed};

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
    api_versions: protocol::api_versions::ApiVersionCache,
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

    /// Returns the configured list of Kafka broker hosts (host:port).
    #[inline]
    #[must_use]
    pub fn hosts(&self) -> &[String] {
        &self.config.hosts
    }

    /// Set the client identifier string reported to Kafka brokers.
    ///
    /// The `client_id` is mainly used for server-side logging and metrics.
    pub fn set_client_id(&mut self, client_id: String) {
        self.config.client_id = client_id;
    }

    /// Returns the configured client identifier.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.config.client_id
    }

    /// Set the compression codec used for message production.
    ///
    /// This affects how produced messages are encoded before being sent to brokers.
    #[inline]
    pub fn set_compression(&mut self, compression: Compression) {
        self.config.compression = compression;
    }

    /// Returns the currently configured compression codec for produced messages.
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

    /// Returns the configured maximum wait time for fetch requests.
    #[inline]
    #[must_use]
    pub fn fetch_max_wait_time(&self) -> Duration {
        let millis = u64::try_from(self.config.fetch.max_wait_time).unwrap_or_default();
        Duration::from_millis(millis)
    }

    #[inline]
    /// Set the minimum number of bytes the broker should accumulate
    /// before returning data for fetch requests.
    pub fn set_fetch_min_bytes(&mut self, min_bytes: i32) {
        self.config.fetch.min_bytes = min_bytes;
    }

    /// Returns the configured minimum number of bytes for fetch requests.
    #[inline]
    #[must_use]
    pub fn fetch_min_bytes(&self) -> i32 {
        self.config.fetch.min_bytes
    }

    #[inline]
    /// Set the maximum number of bytes to fetch per partition.
    pub fn set_fetch_max_bytes_per_partition(&mut self, max_bytes: i32) {
        self.config.fetch.max_bytes_per_partition = max_bytes;
    }

    /// Returns the configured maximum bytes to fetch per partition.
    #[inline]
    #[must_use]
    pub fn fetch_max_bytes_per_partition(&self) -> i32 {
        self.config.fetch.max_bytes_per_partition
    }

    #[inline]
    /// Enable or disable CRC validation for fetched message data.
    pub fn set_fetch_crc_validation(&mut self, validate_crc: bool) {
        self.config.fetch.crc_validation = validate_crc;
    }

    /// Returns whether CRC validation of fetched messages is enabled.
    #[inline]
    #[must_use]
    pub fn fetch_crc_validation(&self) -> bool {
        self.config.fetch.crc_validation
    }

    #[inline]
    /// Configure where consumer group offsets should be stored (Kafka or Zookeeper).
    pub fn set_group_offset_storage(&mut self, storage: Option<GroupOffsetStorage>) {
        self.config.offset_storage = storage;
    }

    /// Returns the currently configured storage for consumer group offsets.
    #[must_use]
    pub fn group_offset_storage(&self) -> Option<GroupOffsetStorage> {
        self.config.offset_storage
    }

    #[inline]
    /// Set the initial backoff/delay used by the retry policy.
    pub fn set_retry_backoff_time(&mut self, time: Duration) {
        match &mut self.config.retry.policy {
            Exponential { initial, .. } => *initial = time,
            Fixed { interval, .. } => *interval = time,
            RetryPolicy::None => {}
        }
    }

    /// Returns the configured maximum number of retry attempts.
    #[inline]
    #[must_use]
    pub fn retry_max_attempts(&self) -> u32 {
        self.config.retry.policy.max_attempts()
    }

    #[inline]
    /// Set the idle timeout for pooled connections.
    ///
    /// Connections idle for longer than this value may be closed.
    pub fn set_connection_idle_timeout(&mut self, timeout: Duration) {
        self.conn_pool.set_idle_timeout(timeout);
    }

    /// Returns the configured connection idle timeout for pooled connections.
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

    /// Returns a view of the currently loaded topic metadata.
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

    // -- admin request helper --

    /// Generic helper for admin API requests that iterate over configured brokers.
    ///
    /// Builds a request via `build`, sends it to each broker in order, and
    /// converts the first successful response via `convert`. Returns the last
    /// error if all brokers fail.
    fn try_admin_request<Req, Resp, T, FBuild, FConvert>(
        &mut self,
        operation_name: &'static str,
        api_version: i16,
        build: FBuild,
        convert: FConvert,
    ) -> Result<T>
    where
        Req: kafka_protocol::protocol::Encodable + kafka_protocol::protocol::HeaderVersion,
        Resp: kafka_protocol::protocol::Decodable + kafka_protocol::protocol::HeaderVersion,
        FBuild: Fn(i32, &str) -> (kafka_protocol::messages::RequestHeader, Req),
        FConvert: Fn(Resp) -> T,
    {
        let correlation_id = self.state.next_correlation_id();
        let now = std::time::Instant::now();
        let hosts = self.config.hosts.clone();
        let mut last_err: Option<Error> = None;

        for host in hosts {
            let conn = match self.conn_pool.get_conn(&host, now) {
                Ok(conn) => conn,
                Err(e) => {
                    last_err = Some(e.with_broker_context(&host, operation_name));
                    continue;
                }
            };

            let (header, request) = build(correlation_id, &self.config.client_id);
            match transport::kp_send_request(conn, &header, &request, api_version)
                .and_then(|()| transport::kp_get_response::<Resp>(conn, api_version))
            {
                Ok(resp) => return Ok(convert(resp)),
                Err(e) => last_err = Some(e.with_broker_context(&host, operation_name)),
            }
        }

        Err(last_err.unwrap_or_else(Error::no_host_reachable))
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
        let timeout_ms = protocol::to_millis_i32(timeout)?;
        self.try_admin_request(
            "CreateTopics",
            protocol::create_topics::API_VERSION_CREATE_TOPICS,
            |correlation_id, client_id| {
                protocol::create_topics::build_create_topics_protocol_request(
                    correlation_id,
                    client_id,
                    topics,
                    timeout_ms,
                )
            },
            protocol::create_topics::convert_create_topics_response,
        )
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
        let timeout_ms = protocol::to_millis_i32(timeout)?;
        self.try_admin_request(
            "DeleteTopics",
            protocol::delete_topics::API_VERSION_DELETE_TOPICS,
            |correlation_id, client_id| {
                protocol::delete_topics::build_delete_topics_protocol_request(
                    correlation_id,
                    client_id,
                    topic_names,
                    timeout_ms,
                )
            },
            protocol::delete_topics::convert_delete_topics_response,
        )
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

    /// Deletes committed offsets for a consumer group.
    ///
    /// Kafka requires each topic to include the partitions whose committed offsets should be
    /// removed.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn delete_group_offsets(
        &mut self,
        group: &str,
        topics: &[TopicPartitionFilter],
    ) -> Result<OffsetDeleteResponseData> {
        self.try_admin_request(
            "OffsetDelete",
            protocol::API_VERSION_OFFSET_DELETE,
            |cid, cid_str| {
                protocol::admin::build_offset_delete_request(cid, cid_str, group, topics)
            },
            protocol::admin::convert_offset_delete_response,
        )
    }

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
        self.state.group_coordinator(group).map(ToOwned::to_owned)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ConnectionError, ProtocolError};

    fn assert_no_host<T>(result: Result<T>) {
        match result {
            Err(Error::Connection(ConnectionError::NoHostReachable)) => {}
            Err(err) => panic!("expected no host reachable, got {err}"),
            Ok(_) => panic!("expected no host reachable, got success"),
        }
    }

    #[test]
    #[allow(deprecated)]
    fn config_and_storage_mutation_apis_surface_no_host() {
        let mut client = KafkaClient::new(vec![]);
        assert_no_host(
            client.incremental_alter_configs(
                &IncrementalAlterConfigsOptions::new([IncrementalAlterConfigsResource::topic(
                    "topic-a",
                    [IncrementalAlterConfig::set("retention.ms", "60000")],
                )])
                .with_validate_only(true),
            ),
        );
        assert_no_host(
            client.alter_configs(
                &AlterConfigsOptions::new([AlterConfigsResource::topic(
                    "topic-a",
                    [AlterConfigsEntry::new("retention.ms", "60000")],
                )])
                .with_validate_only(true),
            ),
        );
        assert_no_host(client.alter_replica_log_dirs(&[AlterReplicaLogDir::new(
            "/kafka-logs-2",
            vec![AlterReplicaLogDirTopic::new("topic-a", [0, 1])],
        )]));
    }

    #[test]
    fn partition_and_quota_mutation_apis_surface_no_host() {
        let mut client = KafkaClient::new(vec![]);
        let topic_partitions = [TopicPartitionFilter::new("topic-a", [0, 1])];

        assert_no_host(client.create_partitions(&[CreatePartitionsTopicSpec::new("topic-a", 3)]));
        assert_no_host(client.delete_records(
            &[DeleteRecordsTopicSpec::new(
                "topic-a",
                [DeleteRecordsPartitionSpec::new(0, 42)],
            )],
            Duration::from_secs(5),
        ));
        assert_no_host(client.alter_partition_reassignments(
            &AlterPartitionReassignmentsOptions::new([PartitionReassignmentTopicSpec::new(
                "topic-a",
                [PartitionReassignmentSpec::new(0, [1, 2])],
            )]),
        ));
        assert_no_host(client.elect_preferred_leaders(&topic_partitions, Duration::from_secs(5)));
        assert_no_host(
            client.offsets_for_leader_epochs(&[LeaderEpochTopicRequest::new(
                "topic-a",
                [LeaderEpochPartitionRequest::new(0, -1, 7)],
            )]),
        );
        assert_no_host(
            client.alter_client_quotas(
                &AlterClientQuotasOptions::new([ClientQuotaAlteration::new(
                    [ClientQuotaEntitySpec::named("user", "alice")],
                    [ClientQuotaAlterationOp::set("producer_byte_rate", 1024.5)],
                )])
                .with_validate_only(true),
            ),
        );
        assert_no_host(client.add_offsets_to_txn("txn-a", 42, 3, "group-a"));
    }

    #[test]
    fn transaction_and_security_mutation_apis_surface_no_host() {
        let mut client = KafkaClient::new(vec![]);

        assert_no_host(client.txn_offset_commit(
            "txn-b",
            "group-b",
            43,
            4,
            &[TxnOffsetCommitTopicPartition {
                topic: "topic-a".to_owned(),
                partition: 0,
                offset: 10,
                leader_epoch: None,
                metadata: None,
            }],
        ));
        assert_no_host(
            client.create_delegation_token(
                &CreateDelegationTokenOptions::new()
                    .with_renewer(KafkaPrincipal::user("bob"))
                    .with_max_lifetime_ms(60_000),
            ),
        );
        assert_no_host(client.renew_delegation_token(b"hmac", Duration::from_secs(3600)));
        assert_no_host(client.expire_delegation_token(b"hmac", Duration::from_secs(60)));
        assert_no_host(
            client.alter_user_scram_credentials(
                &AlterUserScramCredentialsOptions::new()
                    .with_deletion(ScramCredentialDeletion::new(
                        "old-user",
                        SCRAM_MECHANISM_SHA_256,
                    ))
                    .with_upsertion(ScramCredentialUpsertion::new(
                        "new-user",
                        SCRAM_MECHANISM_SHA_512,
                        4096,
                        bytes::Bytes::from_static(b"salt"),
                        bytes::Bytes::from_static(b"salted-password"),
                    )),
            ),
        );
    }

    #[test]
    fn feature_and_share_mutation_apis_surface_no_host() {
        let mut client = KafkaClient::new(vec![]);
        let directory_id = uuid::Uuid::from_u128(1);
        let topic_id = uuid::Uuid::from_u128(2);

        assert_no_host(client.get_telemetry_subscriptions(directory_id));
        assert_no_host(client.push_telemetry(&PushTelemetryOptions::new(
            directory_id,
            1,
            bytes::Bytes::from_static(b"metrics"),
        )));
        assert_no_host(
            client.consumer_group_heartbeat(&ConsumerGroupHeartbeatOptions::new(
                "consumer-group",
                "member-a",
            )),
        );
        assert_no_host(
            client
                .share_group_heartbeat(&ShareGroupHeartbeatOptions::new("share-group", "member-a")),
        );
        let share_topic = ShareFetchTopic::new(
            topic_id,
            [ShareFetchPartition::new(
                0,
                [ShareAcknowledgementBatch::new(
                    0,
                    0,
                    [SHARE_ACK_TYPE_ACCEPT],
                )],
            )],
        );
        assert_no_host(client.share_fetch(
            &ShareFetchOptions::new("share-group", "member-a").with_topics([share_topic]),
        ));
        assert_no_host(client.share_acknowledge(
            &ShareAcknowledgeOptions::new("share-group", "member-a").with_topics([
                ShareAcknowledgeTopic::new(
                    topic_id,
                    [ShareAcknowledgePartition::new(
                        0,
                        [ShareAcknowledgementBatch::new(
                            0,
                            0,
                            [SHARE_ACK_TYPE_ACCEPT],
                        )],
                    )],
                ),
            ]),
        ));
        assert_no_host(client.update_features(&[FeatureUpdate::upgrade("kraft.version", 3)], true));
        assert_no_host(client.unregister_broker(42));
        assert_no_host(
            client.assign_replicas_to_dirs(&AssignReplicasToDirsOptions::new(
                1,
                10,
                [ReplicaDirectoryAssignment::new(
                    directory_id,
                    [ReplicaDirectoryTopicAssignment::new(topic_id, [0])],
                )],
            )),
        );
        assert_no_host(client.add_raft_voter(&AddRaftVoterOptions::new(
            2,
            directory_id,
            [RaftVoterListener::new("CONTROLLER", "controller-2", 9093)],
        )));
        assert_no_host(client.remove_raft_voter(&RemoveRaftVoterOptions::new(2, directory_id)));
        assert_no_host(client.update_raft_voter(&UpdateRaftVoterOptions::new(
            2,
            directory_id,
            [RaftVoterListener::new("CONTROLLER", "controller-2", 9093)],
            RaftVersionFeature::new(1, 3),
        )));
        assert_no_host(client.alter_share_group_offsets(
            "my-group",
            &[AlterShareGroupOffsetTopic::new(
                "topic-a",
                [AlterShareGroupOffsetPartition::new(0, 42)],
            )],
        ));
        assert_no_host(client.delete_share_group_offsets(
            "my-group",
            &[DeleteShareGroupOffsetTopic::new("topic-a")],
        ));
    }

    #[test]
    fn timeout_validated_before_admin_request_routing() {
        let mut client = KafkaClient::new(vec![]);
        let too_large = Duration::from_millis(i32::MAX as u64 + 1);

        assert!(matches!(
            client.delete_records(
                &[DeleteRecordsTopicSpec::new(
                    "topic-a",
                    [DeleteRecordsPartitionSpec::new(0, 42)],
                )],
                too_large,
            ),
            Err(Error::Protocol(ProtocolError::InvalidDuration))
        ));
        assert!(matches!(
            client.elect_unclean_leaders(&[TopicPartitionFilter::new("topic-a", [0])], too_large),
            Err(Error::Protocol(ProtocolError::InvalidDuration))
        ));

        let too_large_i64 = Duration::from_secs((i64::MAX as u64 / 1_000) + 1);
        assert!(matches!(
            client.renew_delegation_token(b"hmac", too_large_i64),
            Err(Error::Protocol(ProtocolError::InvalidDuration))
        ));
        assert!(matches!(
            client.expire_delegation_token(b"hmac", too_large_i64),
            Err(Error::Protocol(ProtocolError::InvalidDuration))
        ));
    }
}
