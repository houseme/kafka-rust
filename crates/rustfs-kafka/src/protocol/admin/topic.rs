#![allow(clippy::wildcard_imports)]
//! Topic administration helpers.

use kafka_protocol::messages::{
    ApiKey, CreatePartitionsRequest, CreatePartitionsResponse, DeleteRecordsRequest,
    DeleteRecordsResponse, DescribeProducersRequest, DescribeProducersResponse,
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, OffsetDeleteRequest,
    OffsetDeleteResponse, OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{
    API_VERSION_CREATE_PARTITIONS, API_VERSION_DELETE_RECORDS, API_VERSION_DESCRIBE_PRODUCERS,
    API_VERSION_DESCRIBE_TOPIC_PARTITIONS, API_VERSION_OFFSET_DELETE,
    API_VERSION_OFFSET_FOR_LEADER_EPOCH,
};
use super::{group_id, request_header};

/// Preferred replica leader election.
pub const ELECTION_TYPE_PREFERRED: i8 = 0;
/// Unclean leader election.
pub const ELECTION_TYPE_UNCLEAN: i8 = 1;

/// A topic plus a partition list used by read-only diagnostic APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionFilter {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to inspect.
    pub partitions: Vec<i32>,
}

impl TopicPartitionFilter {
    /// Create a topic/partition filter.
    #[must_use]
    pub fn new<I>(topic: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// Partition count expansion for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsTopicSpec {
    /// Topic name.
    pub topic: String,
    /// Desired total partition count after expansion.
    pub count: i32,
    /// Optional explicit replica assignments for the newly created partitions.
    pub assignments: Option<Vec<Vec<i32>>>,
}

impl CreatePartitionsTopicSpec {
    /// Create a partition expansion spec without explicit broker assignments.
    #[must_use]
    pub fn new(topic: impl Into<String>, count: i32) -> Self {
        Self {
            topic: topic.into(),
            count,
            assignments: None,
        }
    }

    /// Attach explicit broker assignments for the new partitions.
    #[must_use]
    pub fn with_assignments<I, J>(mut self, assignments: I) -> Self
    where
        I: IntoIterator<Item = J>,
        J: IntoIterator<Item = i32>,
    {
        self.assignments = Some(
            assignments
                .into_iter()
                .map(|assignment| assignment.into_iter().collect())
                .collect(),
        );
        self
    }
}

/// Options for a `CreatePartitions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsOptions {
    /// Topic partition expansions to request.
    pub topics: Vec<CreatePartitionsTopicSpec>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// Validate the request without applying it.
    pub validate_only: bool,
}

impl CreatePartitionsOptions {
    /// Create options with the supplied topic partition expansions.
    #[must_use]
    pub fn new<I>(topics: I) -> Self
    where
        I: IntoIterator<Item = CreatePartitionsTopicSpec>,
    {
        Self {
            topics: topics.into_iter().collect(),
            timeout_ms: 60_000,
            validate_only: false,
        }
    }

    /// Set the broker-side timeout in milliseconds.
    #[must_use]
    pub fn with_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Validate the request without applying it.
    #[must_use]
    pub fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }
}

/// Result of one topic in a `CreatePartitions` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsTopicResult {
    /// Topic name.
    pub name: String,
    /// Per-topic broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Parsed response from a `CreatePartitions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-topic partition creation results returned by the broker.
    pub results: Vec<CreatePartitionsTopicResult>,
}

/// A partition and high-watermark offset used by `DeleteRecords`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsPartitionSpec {
    /// Partition index.
    pub partition_index: i32,
    /// Delete records before this offset.
    pub offset: i64,
}

impl DeleteRecordsPartitionSpec {
    /// Create a delete-records partition spec.
    #[must_use]
    pub fn new(partition_index: i32, offset: i64) -> Self {
        Self {
            partition_index,
            offset,
        }
    }
}

/// Per-topic delete-records request spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsTopicSpec {
    /// Topic name.
    pub topic: String,
    /// Partition offsets to truncate to.
    pub partitions: Vec<DeleteRecordsPartitionSpec>,
}

impl DeleteRecordsTopicSpec {
    /// Create a delete-records topic spec.
    #[must_use]
    pub fn new<I>(topic: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = DeleteRecordsPartitionSpec>,
    {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// Per-partition result returned by `DeleteRecords`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Partition low watermark after deletion.
    pub low_watermark: i64,
    /// Per-partition broker error code.
    pub error_code: i16,
}

/// Per-topic result returned by `DeleteRecords`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsTopicResult {
    /// Topic name.
    pub name: String,
    /// Partition-level deletion results.
    pub partitions: Vec<DeleteRecordsPartitionResult>,
}

/// Parsed response from a `DeleteRecords` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Topic-level deletion results returned by the broker.
    pub topics: Vec<DeleteRecordsTopicResult>,
}

/// Options for an `ElectLeaders` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersOptions {
    /// Raw Kafka election type.
    pub election_type: i8,
    /// Topic partitions to elect leaders for, or `None` for all eligible partitions.
    pub topic_partitions: Option<Vec<TopicPartitionFilter>>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
}

impl ElectLeadersOptions {
    /// Create options for the supplied partitions.
    #[must_use]
    pub fn new<I>(election_type: i8, topic_partitions: I) -> Self
    where
        I: IntoIterator<Item = TopicPartitionFilter>,
    {
        Self {
            election_type,
            topic_partitions: Some(topic_partitions.into_iter().collect()),
            timeout_ms: 60_000,
        }
    }

    /// Create options that ask the broker to elect leaders for all eligible partitions.
    #[must_use]
    pub fn all_partitions(election_type: i8) -> Self {
        Self {
            election_type,
            topic_partitions: None,
            timeout_ms: 60_000,
        }
    }

    /// Set the broker-side timeout in milliseconds.
    #[must_use]
    pub fn with_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

/// Per-partition leader election result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersPartitionResult {
    /// Partition index.
    pub partition_id: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Per-topic leader election result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersTopicResult {
    /// Topic name.
    pub topic: String,
    /// Partition-level election results.
    pub partition_results: Vec<ElectLeadersPartitionResult>,
}

/// Parsed response from an `ElectLeaders` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Topic-level election results returned by the broker.
    pub replica_election_results: Vec<ElectLeadersTopicResult>,
}

/// Partition request for `OffsetForLeaderEpoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderEpochPartitionRequest {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch known by the caller, or Kafka's `-1` sentinel.
    pub current_leader_epoch: i32,
    /// Leader epoch whose end offset should be looked up.
    pub leader_epoch: i32,
}

impl LeaderEpochPartitionRequest {
    /// Create a leader-epoch offset lookup partition request.
    #[must_use]
    pub fn new(partition: i32, current_leader_epoch: i32, leader_epoch: i32) -> Self {
        Self {
            partition,
            current_leader_epoch,
            leader_epoch,
        }
    }
}

/// Per-topic request for `OffsetForLeaderEpoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderEpochTopicRequest {
    /// Topic name.
    pub topic: String,
    /// Partition epoch lookups for this topic.
    pub partitions: Vec<LeaderEpochPartitionRequest>,
}

impl LeaderEpochTopicRequest {
    /// Create a leader-epoch offset lookup topic request.
    #[must_use]
    pub fn new<I>(topic: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = LeaderEpochPartitionRequest>,
    {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// Per-partition offset returned by `OffsetForLeaderEpoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderEpochPartitionOffset {
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Partition index.
    pub partition: i32,
    /// Leader epoch of the returned end offset.
    pub leader_epoch: i32,
    /// End offset for the requested leader epoch.
    pub end_offset: i64,
}

/// Per-topic result returned by `OffsetForLeaderEpoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderEpochTopicOffsets {
    /// Topic name.
    pub topic: String,
    /// Partition offsets for this topic.
    pub partitions: Vec<LeaderEpochPartitionOffset>,
}

/// Parsed response from an `OffsetForLeaderEpoch` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetForLeaderEpochResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Topic-level leader-epoch offsets returned by the broker.
    pub topics: Vec<LeaderEpochTopicOffsets>,
}

/// Per-partition result returned by `OffsetDelete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeletePartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
}

/// Per-topic result returned by `OffsetDelete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteTopicResult {
    /// Topic name.
    pub name: String,
    /// Partition-level offset deletion results.
    pub partitions: Vec<OffsetDeletePartitionResult>,
}

/// Parsed response from an `OffsetDelete` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteResponseData {
    /// Top-level broker error code.
    pub error_code: i16,
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Topic-level offset deletion results returned by the broker.
    pub topics: Vec<OffsetDeleteTopicResult>,
}

/// Cursor used to page through `DescribeTopicPartitions` results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionsCursor {
    /// Topic name where the next page should start.
    pub topic_name: String,
    /// Partition index where the next page should start.
    pub partition_index: i32,
}

impl TopicPartitionsCursor {
    /// Create a topic-partitions pagination cursor.
    #[must_use]
    pub fn new(topic_name: impl Into<String>, partition_index: i32) -> Self {
        Self {
            topic_name: topic_name.into(),
            partition_index,
        }
    }
}

/// Options for a `DescribeTopicPartitions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsOptions {
    /// Topic names to describe. Empty lets the broker return all visible topics.
    pub topics: Vec<String>,
    /// Maximum number of partitions to include in one response page.
    pub response_partition_limit: i32,
    /// Optional cursor returned by a previous page.
    pub cursor: Option<TopicPartitionsCursor>,
}

impl DescribeTopicPartitionsOptions {
    /// Create options with a response partition limit and no topic filter.
    #[must_use]
    pub fn new(response_partition_limit: i32) -> Self {
        Self {
            topics: Vec::new(),
            response_partition_limit,
            cursor: None,
        }
    }

    /// Restrict the request to selected topic names.
    #[must_use]
    pub fn with_topics<I, S>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.topics = topics.into_iter().map(Into::into).collect();
        self
    }

    /// Continue from a broker-supplied pagination cursor.
    #[must_use]
    pub fn with_cursor(mut self, cursor: TopicPartitionsCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }
}

/// Partition metadata returned by `DescribeTopicPartitions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTopicPartition {
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Partition index.
    pub partition_index: i32,
    /// Current leader broker ID, or Kafka's sentinel when unknown.
    pub leader_id: i32,
    /// Current leader epoch.
    pub leader_epoch: i32,
    /// Replicas hosting this partition.
    pub replica_nodes: Vec<i32>,
    /// Replicas currently in sync with the leader.
    pub isr_nodes: Vec<i32>,
    /// Eligible leader replicas when returned by the broker.
    pub eligible_leader_replicas: Option<Vec<i32>>,
    /// Last known eligible leader replicas when returned by the broker.
    pub last_known_elr: Option<Vec<i32>>,
    /// Replicas currently offline.
    pub offline_replicas: Vec<i32>,
}

/// Topic metadata returned by `DescribeTopicPartitions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTopicPartitionsTopic {
    /// Per-topic broker error code.
    pub error_code: i16,
    /// Topic name, omitted by Kafka for some error responses.
    pub name: Option<String>,
    /// Topic UUID as a string.
    pub topic_id: String,
    /// Whether Kafka marks the topic as internal.
    pub is_internal: bool,
    /// Partition metadata returned for this topic.
    pub partitions: Vec<DescribedTopicPartition>,
    /// Authorized operations bitfield, or Kafka's sentinel when not requested.
    pub topic_authorized_operations: i32,
}

/// Parsed response from a `DescribeTopicPartitions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Topic partition metadata returned by the broker.
    pub topics: Vec<DescribedTopicPartitionsTopic>,
    /// Cursor for the next page, or `None` when the response is complete.
    pub next_cursor: Option<TopicPartitionsCursor>,
}

/// State for one active producer returned by `DescribeProducers`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveProducer {
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i32,
    /// Last sequence number sent by the producer.
    pub last_sequence: i32,
    /// Last timestamp sent by the producer.
    pub last_timestamp: i64,
    /// Current epoch of the producer group coordinator.
    pub coordinator_epoch: i32,
    /// Current transaction start offset, or Kafka's sentinel when absent.
    pub current_txn_start_offset: i64,
}

/// Producer state for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional per-partition broker error message.
    pub error_message: Option<String>,
    /// Active producers returned for the partition.
    pub active_producers: Vec<ActiveProducer>,
}

/// Producer state for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerTopic {
    /// Topic name.
    pub name: String,
    /// Partition producer states.
    pub partitions: Vec<ProducerPartition>,
}

/// Parsed response from a `DescribeProducers` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Topics returned by the broker.
    pub topics: Vec<ProducerTopic>,
}

pub fn build_delete_records_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[DeleteRecordsTopicSpec],
    timeout_ms: i32,
) -> (RequestHeader, DeleteRecordsRequest) {
    use kafka_protocol::messages::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DeleteRecords,
        API_VERSION_DELETE_RECORDS,
    );
    let topics = topics
        .iter()
        .map(|topic| {
            DeleteRecordsTopic::default()
                .with_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            DeleteRecordsPartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_offset(partition.offset)
                        })
                        .collect(),
                )
        })
        .collect();
    let request = DeleteRecordsRequest::default()
        .with_topics(topics)
        .with_timeout_ms(timeout_ms);

    (header, request)
}

/// Build a `ListPartitionReassignments` request.
pub fn build_create_partitions_request(
    correlation_id: i32,
    client_id: &str,
    options: &CreatePartitionsOptions,
) -> (RequestHeader, CreatePartitionsRequest) {
    use kafka_protocol::messages::create_partitions_request::{
        CreatePartitionsAssignment, CreatePartitionsTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::CreatePartitions,
        API_VERSION_CREATE_PARTITIONS,
    );
    let topics = options
        .topics
        .iter()
        .map(|topic| {
            CreatePartitionsTopic::default()
                .with_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_count(topic.count)
                .with_assignments(topic.assignments.as_ref().map(|assignments| {
                    assignments
                        .iter()
                        .map(|assignment| {
                            CreatePartitionsAssignment::default().with_broker_ids(
                                assignment.iter().copied().map(Into::into).collect(),
                            )
                        })
                        .collect()
                }))
        })
        .collect();
    let request = CreatePartitionsRequest::default()
        .with_topics(topics)
        .with_timeout_ms(options.timeout_ms)
        .with_validate_only(options.validate_only);

    (header, request)
}

/// Build a `DescribeTopicPartitions` request.
pub fn build_describe_topic_partitions_request(
    correlation_id: i32,
    client_id: &str,
    options: &DescribeTopicPartitionsOptions,
) -> (RequestHeader, DescribeTopicPartitionsRequest) {
    use kafka_protocol::messages::describe_topic_partitions_request::{
        Cursor as KpCursor, TopicRequest,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeTopicPartitions,
        API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
    );
    let topics = options
        .topics
        .iter()
        .map(|topic| TopicRequest::default().with_name(StrBytes::from_string(topic.clone()).into()))
        .collect();
    let cursor = options.cursor.as_ref().map(|cursor| {
        KpCursor::default()
            .with_topic_name(StrBytes::from_string(cursor.topic_name.clone()).into())
            .with_partition_index(cursor.partition_index)
    });
    let request = DescribeTopicPartitionsRequest::default()
        .with_topics(topics)
        .with_response_partition_limit(options.response_partition_limit)
        .with_cursor(cursor);

    (header, request)
}

/// Build a `DescribeShareGroupOffsets` request.
pub fn build_describe_producers_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[TopicPartitionFilter],
) -> (RequestHeader, DescribeProducersRequest) {
    use kafka_protocol::messages::describe_producers_request::TopicRequest;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeProducers,
        API_VERSION_DESCRIBE_PRODUCERS,
    );
    let topics = topics
        .iter()
        .map(|topic| {
            TopicRequest::default()
                .with_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_partition_indexes(topic.partitions.clone())
        })
        .collect();
    let request = DescribeProducersRequest::default().with_topics(topics);

    (header, request)
}

/// Build an `OffsetForLeaderEpoch` request.
pub fn build_offset_for_leader_epoch_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[LeaderEpochTopicRequest],
) -> (RequestHeader, OffsetForLeaderEpochRequest) {
    use kafka_protocol::messages::offset_for_leader_epoch_request::{
        OffsetForLeaderPartition, OffsetForLeaderTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::OffsetForLeaderEpoch,
        API_VERSION_OFFSET_FOR_LEADER_EPOCH,
    );
    let topics = topics
        .iter()
        .map(|topic| {
            OffsetForLeaderTopic::default()
                .with_topic(StrBytes::from_string(topic.topic.clone()).into())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            OffsetForLeaderPartition::default()
                                .with_partition(partition.partition)
                                .with_current_leader_epoch(partition.current_leader_epoch)
                                .with_leader_epoch(partition.leader_epoch)
                        })
                        .collect(),
                )
        })
        .collect();
    let request = OffsetForLeaderEpochRequest::default()
        .with_replica_id((-1).into())
        .with_topics(topics);

    (header, request)
}

/// Build an `OffsetDelete` request for deleting committed group offsets.
pub fn build_offset_delete_request(
    correlation_id: i32,
    client_id: &str,
    group: &str,
    topics: &[TopicPartitionFilter],
) -> (RequestHeader, OffsetDeleteRequest) {
    use kafka_protocol::messages::offset_delete_request::{
        OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::OffsetDelete,
        API_VERSION_OFFSET_DELETE,
    );
    let topics = topics
        .iter()
        .map(|topic| {
            OffsetDeleteRequestTopic::default()
                .with_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .copied()
                        .map(|partition| {
                            OffsetDeleteRequestPartition::default().with_partition_index(partition)
                        })
                        .collect(),
                )
        })
        .collect();
    let request = OffsetDeleteRequest::default()
        .with_group_id(group_id(group))
        .with_topics(topics);

    (header, request)
}

/// Build a `ListTransactions` request.
pub fn convert_delete_records_response(
    response: DeleteRecordsResponse,
) -> DeleteRecordsResponseData {
    DeleteRecordsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        topics: response
            .topics
            .into_iter()
            .map(|topic| DeleteRecordsTopicResult {
                name: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| DeleteRecordsPartitionResult {
                        partition_index: partition.partition_index,
                        low_watermark: partition.low_watermark,
                        error_code: partition.error_code,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeDelegationTokenResponse` into the crate's public shape.
pub fn convert_create_partitions_response(
    response: CreatePartitionsResponse,
) -> CreatePartitionsResponseData {
    CreatePartitionsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        results: response
            .results
            .into_iter()
            .map(|result| CreatePartitionsTopicResult {
                name: result.name.to_string(),
                error_code: result.error_code,
                error_message: result.error_message.map(|message| message.to_string()),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeTopicPartitionsResponse` into the crate's public shape.
pub fn convert_describe_topic_partitions_response(
    response: DescribeTopicPartitionsResponse,
) -> DescribeTopicPartitionsResponseData {
    DescribeTopicPartitionsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        topics: response
            .topics
            .into_iter()
            .map(|topic| DescribedTopicPartitionsTopic {
                error_code: topic.error_code,
                name: topic.name.map(|name| name.to_string()),
                topic_id: topic.topic_id.to_string(),
                is_internal: topic.is_internal,
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| DescribedTopicPartition {
                        error_code: partition.error_code,
                        partition_index: partition.partition_index,
                        leader_id: i32::from(partition.leader_id),
                        leader_epoch: partition.leader_epoch,
                        replica_nodes: broker_ids_to_i32s(partition.replica_nodes),
                        isr_nodes: broker_ids_to_i32s(partition.isr_nodes),
                        eligible_leader_replicas: partition
                            .eligible_leader_replicas
                            .map(broker_ids_to_i32s),
                        last_known_elr: partition.last_known_elr.map(broker_ids_to_i32s),
                        offline_replicas: broker_ids_to_i32s(partition.offline_replicas),
                    })
                    .collect(),
                topic_authorized_operations: topic.topic_authorized_operations,
            })
            .collect(),
        next_cursor: response.next_cursor.map(|cursor| TopicPartitionsCursor {
            topic_name: cursor.topic_name.to_string(),
            partition_index: cursor.partition_index,
        }),
    }
}

fn broker_ids_to_i32s(ids: Vec<kafka_protocol::messages::BrokerId>) -> Vec<i32> {
    ids.into_iter().map(i32::from).collect()
}

/// Convert a generated `DescribeShareGroupOffsetsResponse` into the crate's public shape.
pub fn convert_describe_producers_response(
    response: DescribeProducersResponse,
) -> DescribeProducersResponseData {
    DescribeProducersResponseData {
        throttle_time_ms: response.throttle_time_ms,
        topics: response
            .topics
            .into_iter()
            .map(|topic| ProducerTopic {
                name: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| ProducerPartition {
                        partition_index: partition.partition_index,
                        error_code: partition.error_code,
                        error_message: partition.error_message.map(|message| message.to_string()),
                        active_producers: partition
                            .active_producers
                            .into_iter()
                            .map(|producer| ActiveProducer {
                                producer_id: i64::from(producer.producer_id),
                                producer_epoch: producer.producer_epoch,
                                last_sequence: producer.last_sequence,
                                last_timestamp: producer.last_timestamp,
                                coordinator_epoch: producer.coordinator_epoch,
                                current_txn_start_offset: producer.current_txn_start_offset,
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `OffsetForLeaderEpochResponse` into the crate's public shape.
pub fn convert_offset_for_leader_epoch_response(
    response: OffsetForLeaderEpochResponse,
) -> OffsetForLeaderEpochResponseData {
    OffsetForLeaderEpochResponseData {
        throttle_time_ms: response.throttle_time_ms,
        topics: response
            .topics
            .into_iter()
            .map(|topic| LeaderEpochTopicOffsets {
                topic: topic.topic.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| LeaderEpochPartitionOffset {
                        error_code: partition.error_code,
                        partition: partition.partition,
                        leader_epoch: partition.leader_epoch,
                        end_offset: partition.end_offset,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `OffsetDeleteResponse` into the crate's public shape.
pub fn convert_offset_delete_response(response: OffsetDeleteResponse) -> OffsetDeleteResponseData {
    OffsetDeleteResponseData {
        error_code: response.error_code,
        throttle_time_ms: response.throttle_time_ms,
        topics: response
            .topics
            .into_iter()
            .map(|topic| OffsetDeleteTopicResult {
                name: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| OffsetDeletePartitionResult {
                        partition_index: partition.partition_index,
                        error_code: partition.error_code,
                    })
                    .collect(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult as KpCreatePartitionsTopicResult;
    use kafka_protocol::messages::delete_records_response::{
        DeleteRecordsPartitionResult as KpDeleteRecordsPartitionResult,
        DeleteRecordsTopicResult as KpDeleteRecordsTopicResult,
    };
    use kafka_protocol::messages::describe_producers_response::{
        PartitionResponse as KpProducerPartition, ProducerState as KpProducerState,
        TopicResponse as KpProducerTopic,
    };
    use kafka_protocol::messages::describe_topic_partitions_response::{
        Cursor as KpTopicPartitionsResponseCursor,
        DescribeTopicPartitionsResponsePartition as KpDescribedTopicPartition,
        DescribeTopicPartitionsResponseTopic as KpDescribedTopicPartitionsTopic,
    };
    use kafka_protocol::messages::offset_delete_response::{
        OffsetDeleteResponsePartition as KpOffsetDeletePartition,
        OffsetDeleteResponseTopic as KpOffsetDeleteTopic,
    };
    use kafka_protocol::messages::offset_for_leader_epoch_response::{
        EpochEndOffset as KpEpochEndOffset,
        OffsetForLeaderTopicResult as KpOffsetForLeaderTopicResult,
    };
    use kafka_protocol::messages::{ApiKey, BrokerId, ProducerId};

    #[test]
    fn delete_records_request_preserves_partition_offsets() {
        let topics = [DeleteRecordsTopicSpec::new(
            "topic-a",
            [
                DeleteRecordsPartitionSpec::new(0, 42),
                DeleteRecordsPartitionSpec::new(2, 99),
            ],
        )];
        let (header, request) = build_delete_records_request(17, "client-k", &topics, 5_000);

        assert_eq!(header.request_api_key, ApiKey::DeleteRecords as i16);
        assert_eq!(header.request_api_version, API_VERSION_DELETE_RECORDS);
        assert_eq!(request.timeout_ms, 5_000);
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
        assert_eq!(request.topics[0].partitions[0].offset, 42);
        assert_eq!(request.topics[0].partitions[1].partition_index, 2);
        assert_eq!(request.topics[0].partitions[1].offset, 99);
    }
    #[test]
    fn create_partitions_request_preserves_options_and_assignments() {
        let options = CreatePartitionsOptions::new([
            CreatePartitionsTopicSpec::new("topic-a", 6).with_assignments([[1, 2], [2, 3]]),
            CreatePartitionsTopicSpec::new("topic-b", 3),
        ])
        .with_timeout_ms(8_000)
        .with_validate_only(true);
        let (header, request) = build_create_partitions_request(24, "client-p", &options);

        assert_eq!(header.request_api_key, ApiKey::CreatePartitions as i16);
        assert_eq!(header.request_api_version, API_VERSION_CREATE_PARTITIONS);
        assert_eq!(request.timeout_ms, 8_000);
        assert!(request.validate_only);
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[0].count, 6);
        let assignments = request.topics[0].assignments.as_ref().unwrap();
        assert_eq!(
            assignments[0]
                .broker_ids
                .iter()
                .copied()
                .map(i32::from)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(request.topics[1].name.to_string(), "topic-b");
        assert!(request.topics[1].assignments.is_none());
    }

    #[test]
    fn offset_for_leader_epoch_request_preserves_epoch_fields() {
        let topics = [LeaderEpochTopicRequest::new(
            "topic-a",
            [LeaderEpochPartitionRequest::new(0, -1, 7)],
        )];
        let (header, request) = build_offset_for_leader_epoch_request(25, "client-q", &topics);

        assert_eq!(header.request_api_key, ApiKey::OffsetForLeaderEpoch as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_OFFSET_FOR_LEADER_EPOCH
        );
        assert_eq!(i32::from(request.replica_id), -1);
        assert_eq!(request.topics[0].topic.to_string(), "topic-a");
        let partition = &request.topics[0].partitions[0];
        assert_eq!(partition.partition, 0);
        assert_eq!(partition.current_leader_epoch, -1);
        assert_eq!(partition.leader_epoch, 7);
    }
    #[test]
    fn describe_topic_partitions_request_accepts_topics_limit_and_cursor() {
        let options = DescribeTopicPartitionsOptions::new(250)
            .with_topics(["topic-a", "topic-b"])
            .with_cursor(TopicPartitionsCursor::new("topic-a", 3));
        let (header, request) = build_describe_topic_partitions_request(22, "client-q", &options);

        assert_eq!(
            header.request_api_key,
            ApiKey::DescribeTopicPartitions as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_DESCRIBE_TOPIC_PARTITIONS
        );
        assert_eq!(request.response_partition_limit, 250);
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[1].name.to_string(), "topic-b");
        let cursor = request.cursor.unwrap();
        assert_eq!(cursor.topic_name.to_string(), "topic-a");
        assert_eq!(cursor.partition_index, 3);
    }
    #[test]
    fn describe_producers_request_uses_topic_partition_filters() {
        let filter = [TopicPartitionFilter::new("topic-a", [0, 1])];
        let (header, request) = build_describe_producers_request(26, "client-u", &filter);

        assert_eq!(header.request_api_key, ApiKey::DescribeProducers as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_PRODUCERS);
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[0].partition_indexes, vec![0, 1]);
    }

    #[test]
    fn offset_delete_request_uses_topic_partition_filters() {
        let filters = [
            TopicPartitionFilter::new("topic-a", [0, 2]),
            TopicPartitionFilter::new("topic-b", [1]),
        ];
        let (header, request) = build_offset_delete_request(27, "client-v", "group-a", &filters);

        assert_eq!(header.request_api_key, ApiKey::OffsetDelete as i16);
        assert_eq!(header.request_api_version, API_VERSION_OFFSET_DELETE);
        assert_eq!(request.group_id.to_string(), "group-a");
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
        assert_eq!(request.topics[0].partitions[1].partition_index, 2);
        assert_eq!(request.topics[1].name.to_string(), "topic-b");
        assert_eq!(request.topics[1].partitions[0].partition_index, 1);
    }
    #[test]
    fn convert_delete_records_response_preserves_low_watermarks() {
        let response = DeleteRecordsResponse::default()
            .with_throttle_time_ms(18)
            .with_topics(vec![
                KpDeleteRecordsTopicResult::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpDeleteRecordsPartitionResult::default()
                            .with_partition_index(0)
                            .with_low_watermark(42)
                            .with_error_code(0),
                    ]),
            ]);

        let converted = convert_delete_records_response(response);

        assert_eq!(converted.throttle_time_ms, 18);
        assert_eq!(converted.topics[0].name, "topic-a");
        assert_eq!(converted.topics[0].partitions[0].partition_index, 0);
        assert_eq!(converted.topics[0].partitions[0].low_watermark, 42);
        assert_eq!(converted.topics[0].partitions[0].error_code, 0);
    }
    #[test]
    fn convert_create_partitions_response_preserves_topic_results() {
        let response = CreatePartitionsResponse::default()
            .with_throttle_time_ms(21)
            .with_results(vec![
                KpCreatePartitionsTopicResult::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok"))),
            ]);

        let converted = convert_create_partitions_response(response);

        assert_eq!(converted.throttle_time_ms, 21);
        assert_eq!(converted.results[0].name, "topic-a");
        assert_eq!(converted.results[0].error_code, 0);
        assert_eq!(converted.results[0].error_message, Some("ok".to_owned()));
    }

    #[test]
    fn convert_describe_topic_partitions_response_preserves_page_state() {
        let response = DescribeTopicPartitionsResponse::default()
            .with_throttle_time_ms(25)
            .with_topics(vec![
                KpDescribedTopicPartitionsTopic::default()
                    .with_error_code(0)
                    .with_name(Some(StrBytes::from_static_str("topic-a").into()))
                    .with_is_internal(false)
                    .with_partitions(vec![
                        KpDescribedTopicPartition::default()
                            .with_error_code(0)
                            .with_partition_index(1)
                            .with_leader_id(BrokerId::from(2))
                            .with_leader_epoch(7)
                            .with_replica_nodes(vec![BrokerId::from(1), BrokerId::from(2)])
                            .with_isr_nodes(vec![BrokerId::from(2)])
                            .with_eligible_leader_replicas(Some(vec![BrokerId::from(2)]))
                            .with_last_known_elr(Some(vec![BrokerId::from(1)]))
                            .with_offline_replicas(vec![BrokerId::from(3)]),
                    ])
                    .with_topic_authorized_operations(654),
            ])
            .with_next_cursor(Some(
                KpTopicPartitionsResponseCursor::default()
                    .with_topic_name(StrBytes::from_static_str("topic-a").into())
                    .with_partition_index(2),
            ));

        let converted = convert_describe_topic_partitions_response(response);

        assert_eq!(converted.throttle_time_ms, 25);
        assert_eq!(
            converted.next_cursor,
            Some(TopicPartitionsCursor::new("topic-a", 2))
        );
        let topic = &converted.topics[0];
        assert_eq!(topic.name, Some("topic-a".to_owned()));
        assert_eq!(topic.topic_id, "00000000-0000-0000-0000-000000000000");
        assert!(!topic.is_internal);
        assert_eq!(topic.topic_authorized_operations, 654);
        let partition = &topic.partitions[0];
        assert_eq!(partition.partition_index, 1);
        assert_eq!(partition.leader_id, 2);
        assert_eq!(partition.leader_epoch, 7);
        assert_eq!(partition.replica_nodes, vec![1, 2]);
        assert_eq!(partition.isr_nodes, vec![2]);
        assert_eq!(partition.eligible_leader_replicas, Some(vec![2]));
        assert_eq!(partition.last_known_elr, Some(vec![1]));
        assert_eq!(partition.offline_replicas, vec![3]);
    }
    #[test]
    fn convert_offset_delete_response_preserves_partition_results() {
        let response = OffsetDeleteResponse::default()
            .with_error_code(0)
            .with_throttle_time_ms(28)
            .with_topics(vec![
                KpOffsetDeleteTopic::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpOffsetDeletePartition::default()
                            .with_partition_index(0)
                            .with_error_code(0),
                        KpOffsetDeletePartition::default()
                            .with_partition_index(2)
                            .with_error_code(15),
                    ]),
            ]);

        let converted = convert_offset_delete_response(response);

        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.throttle_time_ms, 28);
        assert_eq!(converted.topics[0].name, "topic-a");
        assert_eq!(
            converted.topics[0].partitions,
            vec![
                OffsetDeletePartitionResult {
                    partition_index: 0,
                    error_code: 0,
                },
                OffsetDeletePartitionResult {
                    partition_index: 2,
                    error_code: 15,
                },
            ]
        );
    }
    #[test]
    fn convert_describe_producers_response_preserves_active_producers() {
        let response = DescribeProducersResponse::default()
            .with_throttle_time_ms(21)
            .with_topics(vec![
                KpProducerTopic::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpProducerPartition::default()
                            .with_partition_index(0)
                            .with_error_code(0)
                            .with_error_message(Some(StrBytes::from_static_str("ok")))
                            .with_active_producers(vec![
                                KpProducerState::default()
                                    .with_producer_id(ProducerId::from(42))
                                    .with_producer_epoch(2)
                                    .with_last_sequence(12)
                                    .with_last_timestamp(1_700_000)
                                    .with_coordinator_epoch(3)
                                    .with_current_txn_start_offset(99),
                            ]),
                    ]),
            ]);

        let converted = convert_describe_producers_response(response);

        assert_eq!(converted.throttle_time_ms, 21);
        assert_eq!(converted.topics[0].name, "topic-a");
        assert_eq!(
            converted.topics[0].partitions[0].error_message,
            Some("ok".to_owned())
        );
        assert_eq!(
            converted.topics[0].partitions[0].active_producers[0].producer_id,
            42
        );
        assert_eq!(
            converted.topics[0].partitions[0].active_producers[0].current_txn_start_offset,
            99
        );
    }

    #[test]
    fn convert_offset_for_leader_epoch_response_preserves_epoch_offsets() {
        let response = OffsetForLeaderEpochResponse::default()
            .with_throttle_time_ms(22)
            .with_topics(vec![
                KpOffsetForLeaderTopicResult::default()
                    .with_topic(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpEpochEndOffset::default()
                            .with_error_code(0)
                            .with_partition(0)
                            .with_leader_epoch(7)
                            .with_end_offset(420),
                    ]),
            ]);

        let converted = convert_offset_for_leader_epoch_response(response);

        assert_eq!(converted.throttle_time_ms, 22);
        assert_eq!(converted.topics[0].topic, "topic-a");
        assert_eq!(converted.topics[0].partitions[0].partition, 0);
        assert_eq!(converted.topics[0].partitions[0].leader_epoch, 7);
        assert_eq!(converted.topics[0].partitions[0].end_offset, 420);
        assert_eq!(converted.topics[0].partitions[0].error_code, 0);
    }
}
