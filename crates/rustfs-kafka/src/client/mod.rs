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

// pub re-exports
pub use crate::compression::Compression;
pub use crate::protocol::admin::{
    ACL_OPERATION_ALL, ACL_OPERATION_ALTER, ACL_OPERATION_ALTER_CONFIGS, ACL_OPERATION_ANY,
    ACL_OPERATION_CLUSTER_ACTION, ACL_OPERATION_CREATE, ACL_OPERATION_CREATE_TOKENS,
    ACL_OPERATION_DELETE, ACL_OPERATION_DESCRIBE, ACL_OPERATION_DESCRIBE_CONFIGS,
    ACL_OPERATION_DESCRIBE_TOKENS, ACL_OPERATION_IDEMPOTENT_WRITE, ACL_OPERATION_READ,
    ACL_OPERATION_WRITE, ACL_PATTERN_TYPE_ANY, ACL_PATTERN_TYPE_LITERAL, ACL_PATTERN_TYPE_MATCH,
    ACL_PATTERN_TYPE_PREFIXED, ACL_PERMISSION_TYPE_ALLOW, ACL_PERMISSION_TYPE_ANY,
    ACL_PERMISSION_TYPE_DENY, ACL_RESOURCE_TYPE_ANY, ACL_RESOURCE_TYPE_CLUSTER,
    ACL_RESOURCE_TYPE_DELEGATION_TOKEN, ACL_RESOURCE_TYPE_GROUP, ACL_RESOURCE_TYPE_TOPIC,
    ACL_RESOURCE_TYPE_TRANSACTIONAL_ID, ACL_RESOURCE_TYPE_USER, AclBinding, AclDescription,
    AclResource, ActiveProducer, AddOffsetsToTxnResponseData, AddRaftVoterOptions,
    AlterClientQuotaEntryResult, AlterClientQuotasOptions, AlterClientQuotasResponseData,
    AlterConfigsEntry, AlterConfigsOptions, AlterConfigsResource, AlterConfigsResourceResult,
    AlterConfigsResponseData, AlterPartitionReassignmentsOptions,
    AlterPartitionReassignmentsPartitionResult, AlterPartitionReassignmentsResponseData,
    AlterPartitionReassignmentsTopicResult, AlterReplicaLogDir, AlterReplicaLogDirPartitionResult,
    AlterReplicaLogDirTopic, AlterReplicaLogDirTopicResult, AlterReplicaLogDirsResponseData,
    AlterShareGroupOffsetPartition, AlterShareGroupOffsetPartitionResult,
    AlterShareGroupOffsetTopic, AlterShareGroupOffsetTopicResult,
    AlterShareGroupOffsetsResponseData, AlterUserScramCredentialResult,
    AlterUserScramCredentialsOptions, AlterUserScramCredentialsResponseData,
    AssignReplicasToDirsOptions, AssignReplicasToDirsResponseData,
    CLIENT_QUOTA_MATCH_ANY_SPECIFIED, CLIENT_QUOTA_MATCH_DEFAULT, CLIENT_QUOTA_MATCH_EXACT,
    CONFIG_OPERATION_APPEND, CONFIG_OPERATION_DELETE, CONFIG_OPERATION_SET,
    CONFIG_OPERATION_SUBTRACT, CONFIG_RESOURCE_TYPE_BROKER, CONFIG_RESOURCE_TYPE_BROKER_LOGGER,
    CONFIG_RESOURCE_TYPE_TOPIC, ClientQuotaAlteration, ClientQuotaAlterationOp, ClientQuotaEntity,
    ClientQuotaEntityFilter, ClientQuotaEntitySpec, ClientQuotaEntry, ClientQuotaValue,
    ClusterBroker, ConfigEntry, ConfigResource, ConfigSynonym, ConsumerGroupAssignment,
    ConsumerGroupDescribeResponseData, ConsumerGroupDescription, ConsumerGroupMemberDescription,
    ConsumerGroupTopicPartitions, CreateAclResult, CreateAclsResponseData,
    CreateDelegationTokenOptions, CreateDelegationTokenResponseData, CreatePartitionsOptions,
    CreatePartitionsResponseData, CreatePartitionsTopicResult, CreatePartitionsTopicSpec,
    DelegationTokenDescription, DeleteAclsFilterResult, DeleteAclsResponseData,
    DeleteGroupsResponseData, DeleteRecordsPartitionResult, DeleteRecordsPartitionSpec,
    DeleteRecordsResponseData, DeleteRecordsTopicResult, DeleteRecordsTopicSpec,
    DeleteShareGroupOffsetTopic, DeleteShareGroupOffsetTopicResult,
    DeleteShareGroupOffsetsResponseData, DeletedAcl, DeletedGroup, DescribeAclsFilter,
    DescribeAclsResponseData, DescribeClientQuotasOptions, DescribeClientQuotasResponseData,
    DescribeClusterResponseData, DescribeConfigsResponseData, DescribeConfigsResult,
    DescribeDelegationTokenResponseData, DescribeGroupsResponseData, DescribeLogDirsResponseData,
    DescribeProducersResponseData, DescribeQuorumResponseData,
    DescribeShareGroupOffsetsResponseData, DescribeTopicPartitionsOptions,
    DescribeTopicPartitionsResponseData, DescribeTransactionsResponseData,
    DescribeUserScramCredentialsResponseData, DescribedGroup, DescribedGroupMember,
    DescribedTopicPartition, DescribedTopicPartitionsTopic, DescribedTransaction,
    ELECTION_TYPE_PREFERRED, ELECTION_TYPE_UNCLEAN, ElectLeadersOptions,
    ElectLeadersPartitionResult, ElectLeadersResponseData, ElectLeadersTopicResult,
    ExpireDelegationTokenResponseData, FEATURE_UPGRADE_TYPE_SAFE_DOWNGRADE,
    FEATURE_UPGRADE_TYPE_UNSAFE_DOWNGRADE, FEATURE_UPGRADE_TYPE_UPGRADE, FeatureUpdate,
    IncrementalAlterConfig, IncrementalAlterConfigsOptions, IncrementalAlterConfigsResource,
    IncrementalAlterConfigsResourceResult, IncrementalAlterConfigsResponseData, KafkaPrincipal,
    LeaderEpochPartitionOffset, LeaderEpochPartitionRequest, LeaderEpochTopicOffsets,
    LeaderEpochTopicRequest, ListConfigResourcesResponseData, ListGroupsResponseData,
    ListPartitionReassignmentsResponseData, ListTransactionsOptions, ListTransactionsResponseData,
    ListedConfigResource, ListedGroup, ListedTransaction, LogDirDescription, LogDirPartition,
    LogDirTopic, OffsetDeletePartitionResult, OffsetDeleteResponseData, OffsetDeleteTopicResult,
    OffsetForLeaderEpochResponseData, PartitionReassignment, PartitionReassignmentSpec,
    PartitionReassignmentTopicSpec, ProducerPartition, ProducerTopic, QuorumListener, QuorumNode,
    QuorumPartition, QuorumReplicaState, QuorumTopic, RaftVersionFeature, RaftVoterCurrentLeader,
    RaftVoterListener, RaftVoterResponseData, RemoveRaftVoterOptions,
    RenewDelegationTokenResponseData, ReplicaDirectoryAssignment, ReplicaDirectoryAssignmentResult,
    ReplicaDirectoryPartitionResult, ReplicaDirectoryTopicAssignment, ReplicaDirectoryTopicResult,
    SCRAM_MECHANISM_SHA_256, SCRAM_MECHANISM_SHA_512, ScramCredentialDeletion, ScramCredentialInfo,
    ScramCredentialUpsertion, ShareGroupAssignment, ShareGroupDescribeResponseData,
    ShareGroupDescription, ShareGroupMemberDescription, ShareGroupOffsetGroup,
    ShareGroupOffsetPartition, ShareGroupOffsetRequest, ShareGroupOffsetTopic,
    ShareGroupTopicPartitions, TopicPartitionFilter, TopicPartitionsCursor, TopicReassignment,
    TransactionTopic, TxnOffsetCommitPartitionResult, TxnOffsetCommitResponseData,
    TxnOffsetCommitTopicPartition, TxnOffsetCommitTopicResult, UnregisterBrokerResponseData,
    UpdateFeaturesResponseData, UpdateFeaturesResult, UpdateRaftVoterOptions,
    UpdateRaftVoterResponseData, UserScramCredentialsDescription,
};
pub use crate::protocol::api_versions::{
    ApiVersionCache, ApiVersions, ApiVersionsResponseData, BrokerApiVersion, api_key,
};
pub use crate::protocol::create_topics::{CreateTopicsResponseData, TopicConfig, TopicResult};
pub use crate::protocol::delete_topics::{DeleteTopicResult, DeleteTopicsResponseData};
#[cfg(feature = "producer_timestamp")]
pub use crate::protocol::produce::ProducerTimestamp;
pub use crate::protocol::share_consumer::{
    ConsumerGroupHeartbeatOptions, ConsumerGroupHeartbeatResponseData, ForgottenShareFetchTopic,
    HeartbeatAssignment, HeartbeatTopicPartitions, SHARE_ACK_TYPE_ACCEPT, SHARE_ACK_TYPE_GAP,
    SHARE_ACK_TYPE_REJECT, SHARE_ACK_TYPE_RELEASE, ShareAcknowledgeOptions,
    ShareAcknowledgePartition, ShareAcknowledgePartitionResponse, ShareAcknowledgeResponseData,
    ShareAcknowledgeTopic, ShareAcknowledgeTopicResponse, ShareAcknowledgeTopicResponseData,
    ShareAcknowledgementBatch, ShareAcquiredRecords, ShareAssignment, ShareFetchOptions,
    ShareFetchPartition, ShareFetchPartitionResponse, ShareFetchResponseData, ShareFetchTopic,
    ShareFetchTopicResponse, ShareGroupHeartbeatOptions, ShareGroupHeartbeatResponseData,
    ShareHeartbeatResponseData, ShareLeader, ShareNodeEndpoint, ShareTopicPartitions,
};
pub use crate::protocol::telemetry::{
    GetTelemetrySubscriptionsOptions, GetTelemetrySubscriptionsResponseData, PushTelemetryOptions,
    PushTelemetryResponseData, TELEMETRY_COMPRESSION_GZIP, TELEMETRY_COMPRESSION_LZ4,
    TELEMETRY_COMPRESSION_NONE, TELEMETRY_COMPRESSION_SNAPPY, TELEMETRY_COMPRESSION_ZSTD,
    TelemetrySubscriptionsResponseData,
};
pub use crate::utils::PartitionOffset;
use crate::utils::TimestampedPartitionOffset;
use std::collections::hash_map::HashMap;
use std::time::Duration;

#[cfg(feature = "security")]
pub use crate::network::{SaslConfig, SecurityConfig};
#[cfg(feature = "security")]
pub use crate::tls::TlsConfig;

use crate::error::{Error, KafkaCode, Result};
use crate::protocol;

/// Builder utilities for constructing `KafkaClient` instances.
pub mod builder;

/// Configuration types and defaults for the Kafka client.
pub mod config;
pub(crate) mod fetch_ops;
mod internals;
pub mod metadata;
pub(crate) mod metadata_ops;
pub(crate) mod offset_ops;
pub(crate) mod produce_ops;
mod raw;
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

/// Types for fetch responses adapted from the `kafka-protocol` crate.
pub mod fetch {
    pub use crate::protocol::fetch::OwnedFetchResponse as Response;
    pub use crate::protocol::fetch::{OwnedData, OwnedMessage, OwnedPartition, OwnedTopic};
}

use config::ClientConfig;
use config::RetryPolicy::{Exponential, Fixed};
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
    /// Create a new `FetchGroupOffset` which identifies a topic partition
    /// to query a group's offset for.
    ///
    /// The returned value borrows the provided `topic` string slice.
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
    /// Construct a `CommitOffset` for the given topic partition and offset.
    ///
    /// This is a convenience constructor used when committing consumer
    /// offsets on behalf of a group.
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

            match protocol::create_topics::fetch_create_topics(
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

            match protocol::delete_topics::fetch_delete_topics(
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

    /// Fetches the Kafka API version ranges advertised by a broker.
    ///
    /// The request is attempted against configured brokers until one succeeds. On success,
    /// the client's internal version cache is refreshed for the responding broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn fetch_api_versions(&mut self) -> Result<ApiVersionsResponseData> {
        let correlation_id = self.state.next_correlation_id();
        let now = std::time::Instant::now();
        let hosts = self.config.hosts.clone();
        let mut last_err: Option<Error> = None;

        for host in hosts {
            let conn = match self.conn_pool.get_conn(&host, now) {
                Ok(conn) => conn,
                Err(e) => {
                    last_err = Some(e.with_broker_context(&host, "ApiVersions"));
                    continue;
                }
            };

            match protocol::api_versions::fetch_api_versions_data(
                conn,
                correlation_id,
                &self.config.client_id,
            ) {
                Ok(resp) => {
                    self.api_versions.insert(
                        host,
                        protocol::api_versions::BrokerApiVersions::from_api_versions(
                            &resp.api_keys,
                        ),
                    );
                    return Ok(resp);
                }
                Err(e) => last_err = Some(e.with_broker_context(&host, "ApiVersions")),
            }
        }

        Err(last_err.unwrap_or_else(Error::no_host_reachable))
    }

    /// Fetches broker-side client telemetry subscription settings.
    ///
    /// The returned subscription ID, compression choices, interval, and metric
    /// filters are intended to drive subsequent `push_telemetry` calls.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn get_telemetry_subscriptions(
        &mut self,
        client_instance_id: uuid::Uuid,
    ) -> Result<TelemetrySubscriptionsResponseData> {
        self.try_admin_request(
            "GetTelemetrySubscriptions",
            protocol::API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS,
            |correlation_id, client_id| {
                protocol::telemetry::build_get_telemetry_subscriptions_request(
                    correlation_id,
                    client_id,
                    GetTelemetrySubscriptionsOptions::for_client_instance(client_instance_id),
                )
            },
            protocol::telemetry::convert_get_telemetry_subscriptions_response,
        )
    }

    /// Pushes an encoded client telemetry payload to a broker.
    ///
    /// This low-level API does not encode metrics itself; callers should pass a
    /// payload that matches the broker's telemetry subscription requirements.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn push_telemetry(
        &mut self,
        options: &PushTelemetryOptions,
    ) -> Result<PushTelemetryResponseData> {
        self.try_admin_request(
            "PushTelemetry",
            protocol::API_VERSION_PUSH_TELEMETRY,
            |correlation_id, client_id| {
                protocol::telemetry::build_push_telemetry_request(
                    correlation_id,
                    client_id,
                    options,
                )
            },
            protocol::telemetry::convert_push_telemetry_response,
        )
    }

    /// Sends a low-level modern consumer-group heartbeat.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn consumer_group_heartbeat(
        &mut self,
        options: &ConsumerGroupHeartbeatOptions,
    ) -> Result<ConsumerGroupHeartbeatResponseData> {
        self.try_admin_request(
            "ConsumerGroupHeartbeat",
            protocol::API_VERSION_CONSUMER_GROUP_HEARTBEAT,
            |correlation_id, client_id| {
                protocol::share_consumer::build_consumer_group_heartbeat_request(
                    correlation_id,
                    client_id,
                    options,
                )
            },
            protocol::share_consumer::convert_consumer_group_heartbeat_response,
        )
    }

    /// Sends a low-level share-group heartbeat.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn share_group_heartbeat(
        &mut self,
        options: &ShareGroupHeartbeatOptions,
    ) -> Result<ShareGroupHeartbeatResponseData> {
        self.try_admin_request(
            "ShareGroupHeartbeat",
            protocol::API_VERSION_SHARE_GROUP_HEARTBEAT,
            |correlation_id, client_id| {
                protocol::share_consumer::build_share_group_heartbeat_request(
                    correlation_id,
                    client_id,
                    options,
                )
            },
            protocol::share_consumer::convert_share_group_heartbeat_response,
        )
    }

    /// Sends a low-level share fetch request.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn share_fetch(&mut self, options: &ShareFetchOptions) -> Result<ShareFetchResponseData> {
        self.try_admin_request(
            "ShareFetch",
            protocol::API_VERSION_SHARE_FETCH,
            |correlation_id, client_id| {
                protocol::share_consumer::build_share_fetch_request(
                    correlation_id,
                    client_id,
                    options,
                )
            },
            protocol::share_consumer::convert_share_fetch_response,
        )
    }

    /// Sends a low-level share acknowledgement request.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn share_acknowledge(
        &mut self,
        options: &ShareAcknowledgeOptions,
    ) -> Result<ShareAcknowledgeResponseData> {
        self.try_admin_request(
            "ShareAcknowledge",
            protocol::API_VERSION_SHARE_ACKNOWLEDGE,
            |correlation_id, client_id| {
                protocol::share_consumer::build_share_acknowledge_request(
                    correlation_id,
                    client_id,
                    options,
                )
            },
            protocol::share_consumer::convert_share_acknowledge_response,
        )
    }

    /// Describes the Kafka cluster, including cluster ID, controller ID, and brokers.
    ///
    /// The request is attempted against configured brokers until one succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_cluster(&mut self) -> Result<DescribeClusterResponseData> {
        self.describe_cluster_with_options(false, false)
    }

    /// Describes the Kafka cluster with optional authorized-operation and fenced-broker fields.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_cluster_with_options(
        &mut self,
        include_authorized_operations: bool,
        include_fenced_brokers: bool,
    ) -> Result<DescribeClusterResponseData> {
        self.try_admin_request(
            "DescribeCluster",
            protocol::API_VERSION_DESCRIBE_CLUSTER,
            |cid, cid_str| {
                protocol::admin::build_describe_cluster_request(
                    cid,
                    cid_str,
                    include_authorized_operations,
                    include_fenced_brokers,
                )
            },
            protocol::admin::convert_describe_cluster_response,
        )
    }

    /// Describes ACLs visible to the contacted broker.
    ///
    /// By default this matches all ACL resources, operations, and permission types.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_acls(&mut self) -> Result<DescribeAclsResponseData> {
        self.describe_acls_with_filter(&DescribeAclsFilter::default())
    }

    /// Describes ACLs using Kafka resource, principal, host, operation, or permission filters.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_acls_with_filter(
        &mut self,
        filter: &DescribeAclsFilter,
    ) -> Result<DescribeAclsResponseData> {
        self.try_admin_request(
            "DescribeAcls",
            protocol::API_VERSION_DESCRIBE_ACLS,
            |cid, cid_str| protocol::admin::build_describe_acls_request(cid, cid_str, filter),
            protocol::admin::convert_describe_acls_response,
        )
    }

    /// Creates ACL bindings on the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn create_acls(&mut self, bindings: &[AclBinding]) -> Result<CreateAclsResponseData> {
        self.try_admin_request(
            "CreateAcls",
            protocol::API_VERSION_CREATE_ACLS,
            |cid, cid_str| protocol::admin::build_create_acls_request(cid, cid_str, bindings),
            protocol::admin::convert_create_acls_response,
        )
    }

    /// Deletes ACLs matching the supplied filters.
    ///
    /// Each filter may match multiple ACL bindings.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn delete_acls(
        &mut self,
        filters: &[DescribeAclsFilter],
    ) -> Result<DeleteAclsResponseData> {
        self.try_admin_request(
            "DeleteAcls",
            protocol::API_VERSION_DELETE_ACLS,
            |cid, cid_str| protocol::admin::build_delete_acls_request(cid, cid_str, filters),
            protocol::admin::convert_delete_acls_response,
        )
    }

    /// Describes Kafka topic, broker, or broker logger configs.
    ///
    /// By default this fetches all config keys without synonyms or documentation.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_configs(
        &mut self,
        resources: &[ConfigResource],
    ) -> Result<DescribeConfigsResponseData> {
        self.describe_configs_with_options(resources, false, false)
    }

    /// Describes Kafka configs with optional synonyms and broker documentation.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_configs_with_options(
        &mut self,
        resources: &[ConfigResource],
        include_synonyms: bool,
        include_documentation: bool,
    ) -> Result<DescribeConfigsResponseData> {
        self.try_admin_request(
            "DescribeConfigs",
            protocol::API_VERSION_DESCRIBE_CONFIGS,
            |cid, cid_str| {
                protocol::admin::build_describe_configs_request(
                    cid,
                    cid_str,
                    resources,
                    include_synonyms,
                    include_documentation,
                )
            },
            protocol::admin::convert_describe_configs_response,
        )
    }

    /// Applies incremental config changes to Kafka topic, broker, or broker logger resources.
    ///
    /// Prefer this API over Kafka's legacy whole-resource `AlterConfigs` protocol.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn incremental_alter_configs(
        &mut self,
        options: &IncrementalAlterConfigsOptions,
    ) -> Result<IncrementalAlterConfigsResponseData> {
        self.try_admin_request(
            "IncrementalAlterConfigs",
            protocol::API_VERSION_INCREMENTAL_ALTER_CONFIGS,
            |cid, cid_str| {
                protocol::admin::build_incremental_alter_configs_request(cid, cid_str, options)
            },
            protocol::admin::convert_incremental_alter_configs_response,
        )
    }

    /// Alters broker or topic configs with Kafka's legacy whole-resource `AlterConfigs` API.
    ///
    /// Prefer [`incremental_alter_configs`](Self::incremental_alter_configs) for new code.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    #[deprecated(
        since = "1.2.0",
        note = "use incremental_alter_configs for incremental config mutation"
    )]
    pub fn alter_configs(
        &mut self,
        options: &AlterConfigsOptions,
    ) -> Result<AlterConfigsResponseData> {
        self.try_admin_request(
            "AlterConfigs",
            protocol::API_VERSION_ALTER_CONFIGS,
            |cid, cid_str| protocol::admin::build_alter_configs_request(cid, cid_str, options),
            protocol::admin::convert_alter_configs_response,
        )
    }

    /// Moves selected replicas to broker log directories.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn alter_replica_log_dirs(
        &mut self,
        dirs: &[AlterReplicaLogDir],
    ) -> Result<AlterReplicaLogDirsResponseData> {
        self.try_admin_request(
            "AlterReplicaLogDirs",
            protocol::API_VERSION_ALTER_REPLICA_LOG_DIRS,
            |cid, cid_str| {
                protocol::admin::build_alter_replica_log_dirs_request(cid, cid_str, dirs)
            },
            protocol::admin::convert_alter_replica_log_dirs_response,
        )
    }

    /// Describes all delegation tokens visible to the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_delegation_tokens(&mut self) -> Result<DescribeDelegationTokenResponseData> {
        self.describe_delegation_tokens_with_owners(None)
    }

    /// Describes delegation tokens owned by selected principals.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_delegation_tokens_for(
        &mut self,
        owners: &[KafkaPrincipal],
    ) -> Result<DescribeDelegationTokenResponseData> {
        self.describe_delegation_tokens_with_owners(Some(owners))
    }

    fn describe_delegation_tokens_with_owners(
        &mut self,
        owners: Option<&[KafkaPrincipal]>,
    ) -> Result<DescribeDelegationTokenResponseData> {
        self.try_admin_request(
            "DescribeDelegationToken",
            protocol::API_VERSION_DESCRIBE_DELEGATION_TOKEN,
            |cid, cid_str| {
                protocol::admin::build_describe_delegation_token_request(cid, cid_str, owners)
            },
            protocol::admin::convert_describe_delegation_token_response,
        )
    }

    /// Creates a Kafka delegation token.
    ///
    /// The returned HMAC is sensitive credential material and can be passed to
    /// [`renew_delegation_token`](Self::renew_delegation_token) or
    /// [`expire_delegation_token`](Self::expire_delegation_token).
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn create_delegation_token(
        &mut self,
        options: &CreateDelegationTokenOptions,
    ) -> Result<CreateDelegationTokenResponseData> {
        self.try_admin_request(
            "CreateDelegationToken",
            protocol::API_VERSION_CREATE_DELEGATION_TOKEN,
            |cid, cid_str| {
                protocol::admin::build_create_delegation_token_request(cid, cid_str, options)
            },
            protocol::admin::convert_create_delegation_token_response,
        )
    }

    /// Renews a Kafka delegation token by HMAC.
    ///
    /// # Errors
    ///
    /// Returns an error if the duration cannot fit Kafka's millisecond field,
    /// brokers are unreachable, or the broker response cannot be decoded.
    pub fn renew_delegation_token(
        &mut self,
        hmac: &[u8],
        renew_period: Duration,
    ) -> Result<RenewDelegationTokenResponseData> {
        let renew_period_ms = protocol::to_millis_i64(renew_period)?;
        let hmac_bytes = bytes::Bytes::copy_from_slice(hmac);
        self.try_admin_request(
            "RenewDelegationToken",
            protocol::API_VERSION_RENEW_DELEGATION_TOKEN,
            |cid, cid_str| {
                protocol::admin::build_renew_delegation_token_request(
                    cid,
                    cid_str,
                    hmac_bytes.clone(),
                    renew_period_ms,
                )
            },
            |resp| protocol::admin::convert_renew_delegation_token_response(&resp),
        )
    }

    /// Expires a Kafka delegation token by HMAC.
    ///
    /// # Errors
    ///
    /// Returns an error if the duration cannot fit Kafka's millisecond field,
    /// brokers are unreachable, or the broker response cannot be decoded.
    pub fn expire_delegation_token(
        &mut self,
        hmac: &[u8],
        expiry_period: Duration,
    ) -> Result<ExpireDelegationTokenResponseData> {
        let expiry_period_ms = protocol::to_millis_i64(expiry_period)?;
        let hmac_bytes = bytes::Bytes::copy_from_slice(hmac);
        self.try_admin_request(
            "ExpireDelegationToken",
            protocol::API_VERSION_EXPIRE_DELEGATION_TOKEN,
            |cid, cid_str| {
                protocol::admin::build_expire_delegation_token_request(
                    cid,
                    cid_str,
                    hmac_bytes.clone(),
                    expiry_period_ms,
                )
            },
            |resp| protocol::admin::convert_expire_delegation_token_response(&resp),
        )
    }

    /// Describes all broker log directories visible to the contacted broker.
    ///
    /// This returns per-log-dir topic and partition storage details, including the
    /// volume capacity fields exposed by Kafka's latest `DescribeLogDirs` version.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_log_dirs(&mut self) -> Result<DescribeLogDirsResponseData> {
        self.describe_log_dirs_with_filter(None)
    }

    /// Describes broker log directories for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_log_dirs_for(
        &mut self,
        topics: &[TopicPartitionFilter],
    ) -> Result<DescribeLogDirsResponseData> {
        self.describe_log_dirs_with_filter(Some(topics))
    }

    fn describe_log_dirs_with_filter(
        &mut self,
        topics: Option<&[TopicPartitionFilter]>,
    ) -> Result<DescribeLogDirsResponseData> {
        self.try_admin_request(
            "DescribeLogDirs",
            protocol::API_VERSION_DESCRIBE_LOG_DIRS,
            |cid, cid_str| protocol::admin::build_describe_log_dirs_request(cid, cid_str, topics),
            protocol::admin::convert_describe_log_dirs_response,
        )
    }

    /// Deletes records before the supplied offsets for selected topic partitions.
    ///
    /// Kafka keeps the partitions and offsets; this advances the partition low watermark
    /// so records before each requested offset are no longer readable.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout is too large, brokers are unreachable, or the
    /// broker response cannot be decoded.
    pub fn delete_records(
        &mut self,
        topics: &[DeleteRecordsTopicSpec],
        timeout: Duration,
    ) -> Result<DeleteRecordsResponseData> {
        let timeout_ms = protocol::to_millis_i32(timeout)?;
        self.try_admin_request(
            "DeleteRecords",
            protocol::API_VERSION_DELETE_RECORDS,
            |cid, cid_str| {
                protocol::admin::build_delete_records_request(cid, cid_str, topics, timeout_ms)
            },
            protocol::admin::convert_delete_records_response,
        )
    }

    /// Lists all ongoing partition reassignments visible to the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout is too large, brokers are unreachable, or the
    /// broker response cannot be decoded.
    pub fn list_partition_reassignments(
        &mut self,
        timeout: Duration,
    ) -> Result<ListPartitionReassignmentsResponseData> {
        self.list_partition_reassignments_with_filter(None, timeout)
    }

    /// Lists ongoing partition reassignments for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout is too large, brokers are unreachable, or the
    /// broker response cannot be decoded.
    pub fn list_partition_reassignments_for(
        &mut self,
        topics: &[TopicPartitionFilter],
        timeout: Duration,
    ) -> Result<ListPartitionReassignmentsResponseData> {
        self.list_partition_reassignments_with_filter(Some(topics), timeout)
    }

    fn list_partition_reassignments_with_filter(
        &mut self,
        topics: Option<&[TopicPartitionFilter]>,
        timeout: Duration,
    ) -> Result<ListPartitionReassignmentsResponseData> {
        let timeout_ms = protocol::to_millis_i32(timeout)?;
        self.try_admin_request(
            "ListPartitionReassignments",
            protocol::API_VERSION_LIST_PARTITION_REASSIGNMENTS,
            |cid, cid_str| {
                protocol::admin::build_list_partition_reassignments_request(
                    cid, cid_str, topics, timeout_ms,
                )
            },
            protocol::admin::convert_list_partition_reassignments_response,
        )
    }

    /// Alters or cancels partition reassignments.
    ///
    /// Use `PartitionReassignmentSpec::new` to assign a new replica set and
    /// `PartitionReassignmentSpec::cancel` to cancel an active reassignment.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn alter_partition_reassignments(
        &mut self,
        options: &AlterPartitionReassignmentsOptions,
    ) -> Result<AlterPartitionReassignmentsResponseData> {
        self.try_admin_request(
            "AlterPartitionReassignments",
            protocol::API_VERSION_ALTER_PARTITION_REASSIGNMENTS,
            |cid, cid_str| {
                protocol::admin::build_alter_partition_reassignments_request(cid, cid_str, options)
            },
            protocol::admin::convert_alter_partition_reassignments_response,
        )
    }

    /// Describes `KRaft` quorum state for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_quorum(
        &mut self,
        topics: &[TopicPartitionFilter],
    ) -> Result<DescribeQuorumResponseData> {
        self.try_admin_request(
            "DescribeQuorum",
            protocol::API_VERSION_DESCRIBE_QUORUM,
            |cid, cid_str| protocol::admin::build_describe_quorum_request(cid, cid_str, topics),
            protocol::admin::convert_describe_quorum_response,
        )
    }

    /// Updates finalized `KRaft` feature levels.
    ///
    /// This is a cluster-wide metadata mutation. Prefer calling with
    /// `validate_only = true` before applying feature upgrades or downgrades.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn update_features(
        &mut self,
        feature_updates: &[FeatureUpdate],
        validate_only: bool,
    ) -> Result<UpdateFeaturesResponseData> {
        self.try_admin_request(
            "UpdateFeatures",
            protocol::API_VERSION_UPDATE_FEATURES,
            |cid, cid_str| {
                protocol::admin::build_update_features_request(
                    cid,
                    cid_str,
                    feature_updates,
                    validate_only,
                )
            },
            protocol::admin::convert_update_features_response,
        )
    }

    /// Unregisters a broker from the `KRaft` cluster metadata.
    ///
    /// This is a destructive cluster lifecycle operation. Call it only after
    /// the broker has been intentionally removed from service.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn unregister_broker(&mut self, broker_id: i32) -> Result<UnregisterBrokerResponseData> {
        self.try_admin_request(
            "UnregisterBroker",
            protocol::API_VERSION_UNREGISTER_BROKER,
            |cid, cid_str| {
                protocol::admin::build_unregister_broker_request(cid, cid_str, broker_id)
            },
            protocol::admin::convert_unregister_broker_response,
        )
    }

    /// Assigns topic replicas to broker log directory IDs.
    ///
    /// This is a broker storage administration API intended for explicit JBOD
    /// directory placement workflows.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn assign_replicas_to_dirs(
        &mut self,
        options: &AssignReplicasToDirsOptions,
    ) -> Result<AssignReplicasToDirsResponseData> {
        self.try_admin_request(
            "AssignReplicasToDirs",
            protocol::API_VERSION_ASSIGN_REPLICAS_TO_DIRS,
            |cid, cid_str| {
                protocol::admin::build_assign_replicas_to_dirs_request(cid, cid_str, options)
            },
            protocol::admin::convert_assign_replicas_to_dirs_response,
        )
    }

    /// Adds a voter to the `KRaft` controller quorum.
    ///
    /// This is an explicit `KRaft` quorum administration API. Prefer verifying
    /// the target controller listener and directory ID before calling it.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn add_raft_voter(
        &mut self,
        options: &AddRaftVoterOptions,
    ) -> Result<RaftVoterResponseData> {
        self.try_admin_request(
            "AddRaftVoter",
            protocol::API_VERSION_ADD_RAFT_VOTER,
            |cid, cid_str| protocol::admin::build_add_raft_voter_request(cid, cid_str, options),
            protocol::admin::convert_add_raft_voter_response,
        )
    }

    /// Removes a voter from the `KRaft` controller quorum.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn remove_raft_voter(
        &mut self,
        options: &RemoveRaftVoterOptions,
    ) -> Result<RaftVoterResponseData> {
        self.try_admin_request(
            "RemoveRaftVoter",
            protocol::API_VERSION_REMOVE_RAFT_VOTER,
            |cid, cid_str| protocol::admin::build_remove_raft_voter_request(cid, cid_str, options),
            protocol::admin::convert_remove_raft_voter_response,
        )
    }

    /// Updates a voter in the `KRaft` controller quorum.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn update_raft_voter(
        &mut self,
        options: &UpdateRaftVoterOptions,
    ) -> Result<UpdateRaftVoterResponseData> {
        self.try_admin_request(
            "UpdateRaftVoter",
            protocol::API_VERSION_UPDATE_RAFT_VOTER,
            |cid, cid_str| protocol::admin::build_update_raft_voter_request(cid, cid_str, options),
            protocol::admin::convert_update_raft_voter_response,
        )
    }

    /// Elects leaders using the supplied Kafka election type and partition scope.
    ///
    /// Use `ElectLeadersOptions::all_partitions` to ask the broker to elect leaders
    /// for all eligible partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn elect_leaders(
        &mut self,
        options: &ElectLeadersOptions,
    ) -> Result<ElectLeadersResponseData> {
        self.try_admin_request(
            "ElectLeaders",
            protocol::API_VERSION_ELECT_LEADERS,
            |cid, cid_str| protocol::admin::build_elect_leaders_request(cid, cid_str, options),
            protocol::admin::convert_elect_leaders_response,
        )
    }

    /// Elects preferred leaders for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout is too large, brokers are unreachable, or the
    /// broker response cannot be decoded.
    pub fn elect_preferred_leaders(
        &mut self,
        topics: &[TopicPartitionFilter],
        timeout: Duration,
    ) -> Result<ElectLeadersResponseData> {
        let options = ElectLeadersOptions::new(ELECTION_TYPE_PREFERRED, topics.iter().cloned())
            .with_timeout_ms(protocol::to_millis_i32(timeout)?);
        self.elect_leaders(&options)
    }

    /// Elects unclean leaders for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout is too large, brokers are unreachable, or the
    /// broker response cannot be decoded.
    pub fn elect_unclean_leaders(
        &mut self,
        topics: &[TopicPartitionFilter],
        timeout: Duration,
    ) -> Result<ElectLeadersResponseData> {
        let options = ElectLeadersOptions::new(ELECTION_TYPE_UNCLEAN, topics.iter().cloned())
            .with_timeout_ms(protocol::to_millis_i32(timeout)?);
        self.elect_leaders(&options)
    }

    /// Lists config resources for the broker's default supported resource types.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_config_resources(&mut self) -> Result<ListConfigResourcesResponseData> {
        self.list_config_resources_for(&[])
    }

    /// Lists config resources for selected Kafka config resource types.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_config_resources_for(
        &mut self,
        resource_types: &[i8],
    ) -> Result<ListConfigResourcesResponseData> {
        self.try_admin_request(
            "ListConfigResources",
            protocol::API_VERSION_LIST_CONFIG_RESOURCES,
            |cid, cid_str| {
                protocol::admin::build_list_config_resources_request(cid, cid_str, resource_types)
            },
            protocol::admin::convert_list_config_resources_response,
        )
    }

    /// Expands partition counts for one or more topics.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn create_partitions(
        &mut self,
        topics: &[CreatePartitionsTopicSpec],
    ) -> Result<CreatePartitionsResponseData> {
        let options = CreatePartitionsOptions::new(topics.iter().cloned());
        self.create_partitions_with_options(&options)
    }

    /// Expands partition counts using timeout, validation, and assignment options.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn create_partitions_with_options(
        &mut self,
        options: &CreatePartitionsOptions,
    ) -> Result<CreatePartitionsResponseData> {
        self.try_admin_request(
            "CreatePartitions",
            protocol::API_VERSION_CREATE_PARTITIONS,
            |cid, cid_str| protocol::admin::build_create_partitions_request(cid, cid_str, options),
            protocol::admin::convert_create_partitions_response,
        )
    }

    /// Describes topic and partition metadata for one response page.
    ///
    /// Empty `topics` returns all topics visible to the broker, subject to `response_partition_limit`.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_topic_partitions(
        &mut self,
        topics: &[&str],
        response_partition_limit: i32,
    ) -> Result<DescribeTopicPartitionsResponseData> {
        let options = DescribeTopicPartitionsOptions::new(response_partition_limit)
            .with_topics(topics.iter().copied());
        self.describe_topic_partitions_with_options(&options)
    }

    /// Describes topic and partition metadata using Kafka pagination options.
    ///
    /// Use `DescribeTopicPartitionsResponseData::next_cursor` to request subsequent pages.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_topic_partitions_with_options(
        &mut self,
        options: &DescribeTopicPartitionsOptions,
    ) -> Result<DescribeTopicPartitionsResponseData> {
        self.try_admin_request(
            "DescribeTopicPartitions",
            protocol::API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
            |cid, cid_str| {
                protocol::admin::build_describe_topic_partitions_request(cid, cid_str, options)
            },
            protocol::admin::convert_describe_topic_partitions_response,
        )
    }

    /// Describes all client quota entities visible to the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_client_quotas(&mut self) -> Result<DescribeClientQuotasResponseData> {
        self.describe_client_quotas_with_options(&DescribeClientQuotasOptions::default())
    }

    /// Describes client quota entities using Kafka entity filters.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_client_quotas_with_options(
        &mut self,
        options: &DescribeClientQuotasOptions,
    ) -> Result<DescribeClientQuotasResponseData> {
        self.try_admin_request(
            "DescribeClientQuotas",
            protocol::API_VERSION_DESCRIBE_CLIENT_QUOTAS,
            |cid, cid_str| {
                protocol::admin::build_describe_client_quotas_request(cid, cid_str, options)
            },
            protocol::admin::convert_describe_client_quotas_response,
        )
    }

    /// Applies client quota changes for one or more quota entities.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn alter_client_quotas(
        &mut self,
        options: &AlterClientQuotasOptions,
    ) -> Result<AlterClientQuotasResponseData> {
        self.try_admin_request(
            "AlterClientQuotas",
            protocol::API_VERSION_ALTER_CLIENT_QUOTAS,
            |cid, cid_str| {
                protocol::admin::build_alter_client_quotas_request(cid, cid_str, options)
            },
            protocol::admin::convert_alter_client_quotas_response,
        )
    }

    /// Describes SCRAM credential metadata for all visible users.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_user_scram_credentials(
        &mut self,
    ) -> Result<DescribeUserScramCredentialsResponseData> {
        self.describe_user_scram_credentials_with_filter(None)
    }

    /// Describes SCRAM credential metadata for selected users.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_user_scram_credentials_for(
        &mut self,
        users: &[&str],
    ) -> Result<DescribeUserScramCredentialsResponseData> {
        self.describe_user_scram_credentials_with_filter(Some(users))
    }

    fn describe_user_scram_credentials_with_filter(
        &mut self,
        users: Option<&[&str]>,
    ) -> Result<DescribeUserScramCredentialsResponseData> {
        self.try_admin_request(
            "DescribeUserScramCredentials",
            protocol::API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
            |cid, cid_str| {
                protocol::admin::build_describe_user_scram_credentials_request(cid, cid_str, users)
            },
            protocol::admin::convert_describe_user_scram_credentials_response,
        )
    }

    /// Alters SCRAM credentials for Kafka users.
    ///
    /// Upsertions require precomputed `salt` and `salted_password` bytes for the selected SCRAM
    /// mechanism. This mirrors Kafka's protocol and avoids guessing password derivation policy in
    /// the client.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn alter_user_scram_credentials(
        &mut self,
        options: &AlterUserScramCredentialsOptions,
    ) -> Result<AlterUserScramCredentialsResponseData> {
        self.try_admin_request(
            "AlterUserScramCredentials",
            protocol::API_VERSION_ALTER_USER_SCRAM_CREDENTIALS,
            |cid, cid_str| {
                protocol::admin::build_alter_user_scram_credentials_request(cid, cid_str, options)
            },
            protocol::admin::convert_alter_user_scram_credentials_response,
        )
    }

    /// Describes active producers for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_producers(
        &mut self,
        topics: &[TopicPartitionFilter],
    ) -> Result<DescribeProducersResponseData> {
        self.try_admin_request(
            "DescribeProducers",
            protocol::API_VERSION_DESCRIBE_PRODUCERS,
            |cid, cid_str| protocol::admin::build_describe_producers_request(cid, cid_str, topics),
            protocol::admin::convert_describe_producers_response,
        )
    }

    /// Looks up end offsets for specific topic-partition leader epochs.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn offsets_for_leader_epochs(
        &mut self,
        topics: &[LeaderEpochTopicRequest],
    ) -> Result<OffsetForLeaderEpochResponseData> {
        self.try_admin_request(
            "OffsetForLeaderEpoch",
            protocol::API_VERSION_OFFSET_FOR_LEADER_EPOCH,
            |cid, cid_str| {
                protocol::admin::build_offset_for_leader_epoch_request(cid, cid_str, topics)
            },
            protocol::admin::convert_offset_for_leader_epoch_response,
        )
    }

    /// Lists transactions visible to the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_transactions(&mut self) -> Result<ListTransactionsResponseData> {
        self.list_transactions_with_options(&ListTransactionsOptions::default())
    }

    /// Lists transactions using Kafka state, producer ID, duration, or ID pattern filters.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_transactions_with_options(
        &mut self,
        options: &ListTransactionsOptions,
    ) -> Result<ListTransactionsResponseData> {
        self.try_admin_request(
            "ListTransactions",
            protocol::API_VERSION_LIST_TRANSACTIONS,
            |cid, cid_str| protocol::admin::build_list_transactions_request(cid, cid_str, options),
            protocol::admin::convert_list_transactions_response,
        )
    }

    /// Describes detailed transaction state for the supplied transactional IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_transactions(
        &mut self,
        transactional_ids: &[&str],
    ) -> Result<DescribeTransactionsResponseData> {
        self.try_admin_request(
            "DescribeTransactions",
            protocol::API_VERSION_DESCRIBE_TRANSACTIONS,
            |cid, cid_str| {
                protocol::admin::build_describe_transactions_request(
                    cid,
                    cid_str,
                    transactional_ids,
                )
            },
            protocol::admin::convert_describe_transactions_response,
        )
    }

    /// Adds offsets for a consumer group to the current transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn add_offsets_to_txn(
        &mut self,
        txn_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<AddOffsetsToTxnResponseData> {
        self.try_admin_request(
            "AddOffsetsToTxn",
            protocol::API_VERSION_ADD_OFFSETS_TO_TXN,
            |cid, cid_str| {
                protocol::admin::build_add_offsets_to_txn_request(
                    cid,
                    cid_str,
                    txn_id,
                    producer_id,
                    producer_epoch,
                    group_id,
                )
            },
            |resp| protocol::admin::convert_add_offsets_to_txn_response(&resp),
        )
    }

    /// Commits consumer offsets as part of a transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn txn_offset_commit(
        &mut self,
        txn_id: &str,
        group_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        offsets: &[TxnOffsetCommitTopicPartition],
    ) -> Result<TxnOffsetCommitResponseData> {
        self.try_admin_request(
            "TxnOffsetCommit",
            protocol::API_VERSION_TXN_OFFSET_COMMIT,
            |cid, cid_str| {
                protocol::admin::build_txn_offset_commit_request(
                    cid,
                    cid_str,
                    txn_id,
                    group_id,
                    producer_id,
                    producer_epoch,
                    offsets,
                )
            },
            protocol::admin::convert_txn_offset_commit_response,
        )
    }

    /// Describes modern consumer group state for the supplied group IDs.
    ///
    /// This uses Kafka's `ConsumerGroupDescribe` API and returns structured member
    /// subscription and assignment data.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_consumer_groups(
        &mut self,
        groups: &[&str],
    ) -> Result<ConsumerGroupDescribeResponseData> {
        self.describe_consumer_groups_with_options(groups, false)
    }

    /// Describes modern consumer group state with optional authorized-operation fields.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_consumer_groups_with_options(
        &mut self,
        groups: &[&str],
        include_authorized_operations: bool,
    ) -> Result<ConsumerGroupDescribeResponseData> {
        self.try_admin_request(
            "ConsumerGroupDescribe",
            protocol::API_VERSION_CONSUMER_GROUP_DESCRIBE,
            |cid, cid_str| {
                protocol::admin::build_consumer_group_describe_request(
                    cid,
                    cid_str,
                    groups,
                    include_authorized_operations,
                )
            },
            protocol::admin::convert_consumer_group_describe_response,
        )
    }

    /// Describes Kafka share group state for the supplied group IDs.
    ///
    /// This uses Kafka's `ShareGroupDescribe` API and returns structured member
    /// subscription and assignment data.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_share_groups(
        &mut self,
        groups: &[&str],
    ) -> Result<ShareGroupDescribeResponseData> {
        self.describe_share_groups_with_options(groups, false)
    }

    /// Describes Kafka share group state with optional authorized-operation fields.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_share_groups_with_options(
        &mut self,
        groups: &[&str],
        include_authorized_operations: bool,
    ) -> Result<ShareGroupDescribeResponseData> {
        self.try_admin_request(
            "ShareGroupDescribe",
            protocol::API_VERSION_SHARE_GROUP_DESCRIBE,
            |cid, cid_str| {
                protocol::admin::build_share_group_describe_request(
                    cid,
                    cid_str,
                    groups,
                    include_authorized_operations,
                )
            },
            protocol::admin::convert_share_group_describe_response,
        )
    }

    /// Describes all visible share-partition offsets for the supplied share group IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_share_group_offsets(
        &mut self,
        groups: &[&str],
    ) -> Result<DescribeShareGroupOffsetsResponseData> {
        let requests: Vec<_> = groups
            .iter()
            .map(|group| ShareGroupOffsetRequest::all_partitions(*group))
            .collect();
        self.describe_share_group_offsets_with_options(&requests)
    }

    /// Describes share-partition offsets using per-group topic partition filters.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_share_group_offsets_with_options(
        &mut self,
        groups: &[ShareGroupOffsetRequest],
    ) -> Result<DescribeShareGroupOffsetsResponseData> {
        self.try_admin_request(
            "DescribeShareGroupOffsets",
            protocol::API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
            |cid, cid_str| {
                protocol::admin::build_describe_share_group_offsets_request(cid, cid_str, groups)
            },
            protocol::admin::convert_describe_share_group_offsets_response,
        )
    }

    /// Alters start offsets for partitions in a Kafka share group.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn alter_share_group_offsets(
        &mut self,
        group_id: &str,
        topics: &[AlterShareGroupOffsetTopic],
    ) -> Result<AlterShareGroupOffsetsResponseData> {
        self.try_admin_request(
            "AlterShareGroupOffsets",
            protocol::API_VERSION_ALTER_SHARE_GROUP_OFFSETS,
            |cid, cid_str| {
                protocol::admin::build_alter_share_group_offsets_request(
                    cid, cid_str, group_id, topics,
                )
            },
            protocol::admin::convert_alter_share_group_offsets_response,
        )
    }

    /// Deletes stored offsets for topics in a Kafka share group.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn delete_share_group_offsets(
        &mut self,
        group_id: &str,
        topics: &[DeleteShareGroupOffsetTopic],
    ) -> Result<DeleteShareGroupOffsetsResponseData> {
        self.try_admin_request(
            "DeleteShareGroupOffsets",
            protocol::API_VERSION_DELETE_SHARE_GROUP_OFFSETS,
            |cid, cid_str| {
                protocol::admin::build_delete_share_group_offsets_request(
                    cid, cid_str, group_id, topics,
                )
            },
            protocol::admin::convert_delete_share_group_offsets_response,
        )
    }

    /// Lists consumer groups known to the contacted broker.
    ///
    /// The request is attempted against configured brokers until one succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_groups(&mut self) -> Result<ListGroupsResponseData> {
        self.list_groups_with_filters(&[], &[])
    }

    /// Lists consumer groups filtered by group state and group type.
    ///
    /// Empty filters return all groups visible to the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_groups_with_filters(
        &mut self,
        states_filter: &[&str],
        types_filter: &[&str],
    ) -> Result<ListGroupsResponseData> {
        self.try_admin_request(
            "ListGroups",
            protocol::API_VERSION_LIST_GROUPS,
            |cid, cid_str| {
                protocol::admin::build_list_groups_request(
                    cid,
                    cid_str,
                    states_filter,
                    types_filter,
                )
            },
            protocol::admin::convert_list_groups_response,
        )
    }

    /// Deletes the supplied consumer groups.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn delete_groups(&mut self, groups: &[&str]) -> Result<DeleteGroupsResponseData> {
        self.try_admin_request(
            "DeleteGroups",
            protocol::API_VERSION_DELETE_GROUPS,
            |cid, cid_str| protocol::admin::build_delete_groups_request(cid, cid_str, groups),
            protocol::admin::convert_delete_groups_response,
        )
    }

    /// Describes the supplied consumer groups.
    ///
    /// The request is attempted against configured brokers until one succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_groups(&mut self, groups: &[&str]) -> Result<DescribeGroupsResponseData> {
        self.describe_groups_with_options(groups, false)
    }

    /// Describes the supplied consumer groups with optional authorized-operation fields.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_groups_with_options(
        &mut self,
        groups: &[&str],
        include_authorized_operations: bool,
    ) -> Result<DescribeGroupsResponseData> {
        self.try_admin_request(
            "DescribeGroups",
            protocol::API_VERSION_DESCRIBE_GROUPS,
            |cid, cid_str| {
                protocol::admin::build_describe_groups_request(
                    cid,
                    cid_str,
                    groups,
                    include_authorized_operations,
                )
            },
            protocol::admin::convert_describe_groups_response,
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
