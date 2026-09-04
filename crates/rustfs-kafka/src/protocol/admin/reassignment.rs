#![allow(clippy::wildcard_imports)]
//! Partition reassignment administration helpers.

use kafka_protocol::messages::{
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, ApiKey,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{
    API_VERSION_ALTER_PARTITION_REASSIGNMENTS, API_VERSION_LIST_PARTITION_REASSIGNMENTS,
};
use super::request_header;
use super::*;

/// A partition reassignment entry for `AlterPartitionReassignments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignmentSpec {
    /// Partition index.
    pub partition_index: i32,
    /// New replica broker IDs, or `None` to cancel an active reassignment.
    pub replicas: Option<Vec<i32>>,
}

impl PartitionReassignmentSpec {
    /// Create a partition reassignment.
    #[must_use]
    pub fn new<I>(partition_index: i32, replicas: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        Self {
            partition_index,
            replicas: Some(replicas.into_iter().collect()),
        }
    }

    /// Create a partition reassignment cancellation.
    #[must_use]
    pub fn cancel(partition_index: i32) -> Self {
        Self {
            partition_index,
            replicas: None,
        }
    }
}

/// Per-topic reassignment spec for `AlterPartitionReassignments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignmentTopicSpec {
    /// Topic name.
    pub topic: String,
    /// Partition reassignments for this topic.
    pub partitions: Vec<PartitionReassignmentSpec>,
}

impl PartitionReassignmentTopicSpec {
    /// Create a topic reassignment spec.
    #[must_use]
    pub fn new<I>(topic: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = PartitionReassignmentSpec>,
    {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// Options for an `AlterPartitionReassignments` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsOptions {
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// Whether replication-factor changes are allowed.
    pub allow_replication_factor_change: bool,
    /// Topic reassignments to alter.
    pub topics: Vec<PartitionReassignmentTopicSpec>,
}

impl AlterPartitionReassignmentsOptions {
    /// Create options with the supplied topic reassignments.
    #[must_use]
    pub fn new<I>(topics: I) -> Self
    where
        I: IntoIterator<Item = PartitionReassignmentTopicSpec>,
    {
        Self {
            timeout_ms: 60_000,
            allow_replication_factor_change: true,
            topics: topics.into_iter().collect(),
        }
    }

    /// Set the broker-side timeout in milliseconds.
    #[must_use]
    pub fn with_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Control whether replication-factor changes are allowed.
    #[must_use]
    pub fn with_allow_replication_factor_change(mut self, allow: bool) -> Self {
        self.allow_replication_factor_change = allow;
        self
    }
}

/// Per-partition result returned by `AlterPartitionReassignments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Per-topic result returned by `AlterPartitionReassignments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsTopicResult {
    /// Topic name.
    pub name: String,
    /// Partition-level reassignment results.
    pub partitions: Vec<AlterPartitionReassignmentsPartitionResult>,
}

/// Parsed response from an `AlterPartitionReassignments` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Whether replication-factor changes were allowed.
    pub allow_replication_factor_change: bool,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional top-level broker error message.
    pub error_message: Option<String>,
    /// Topic-level reassignment results returned by the broker.
    pub responses: Vec<AlterPartitionReassignmentsTopicResult>,
}

/// Ongoing partition reassignment details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignment {
    /// Partition index.
    pub partition_index: i32,
    /// Current replica set.
    pub replicas: Vec<i32>,
    /// Replicas currently being added.
    pub adding_replicas: Vec<i32>,
    /// Replicas currently being removed.
    pub removing_replicas: Vec<i32>,
}

/// Ongoing reassignments for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicReassignment {
    /// Topic name.
    pub name: String,
    /// Ongoing partition reassignments.
    pub partitions: Vec<PartitionReassignment>,
}

/// Parsed response from a `ListPartitionReassignments` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional top-level broker error message.
    pub error_message: Option<String>,
    /// Ongoing reassignments returned by the broker.
    pub topics: Vec<TopicReassignment>,
}

pub fn build_list_partition_reassignments_request(
    correlation_id: i32,
    client_id: &str,
    topics: Option<&[TopicPartitionFilter]>,
    timeout_ms: i32,
) -> (RequestHeader, ListPartitionReassignmentsRequest) {
    use kafka_protocol::messages::list_partition_reassignments_request::ListPartitionReassignmentsTopics;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ListPartitionReassignments,
        API_VERSION_LIST_PARTITION_REASSIGNMENTS,
    );
    let topics = topics.map(|topics| {
        topics
            .iter()
            .map(|topic| {
                ListPartitionReassignmentsTopics::default()
                    .with_name(StrBytes::from_string(topic.topic.clone()).into())
                    .with_partition_indexes(topic.partitions.clone())
            })
            .collect()
    });
    let request = ListPartitionReassignmentsRequest::default()
        .with_timeout_ms(timeout_ms)
        .with_topics(topics);

    (header, request)
}

/// Build an `AlterPartitionReassignments` request.
pub fn build_alter_partition_reassignments_request(
    correlation_id: i32,
    client_id: &str,
    options: &AlterPartitionReassignmentsOptions,
) -> (RequestHeader, AlterPartitionReassignmentsRequest) {
    use kafka_protocol::messages::alter_partition_reassignments_request::{
        ReassignablePartition, ReassignableTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::AlterPartitionReassignments,
        API_VERSION_ALTER_PARTITION_REASSIGNMENTS,
    );
    let topics = options
        .topics
        .iter()
        .map(|topic| {
            ReassignableTopic::default()
                .with_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            ReassignablePartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_replicas(partition.replicas.as_ref().map(|replicas| {
                                    replicas.iter().copied().map(Into::into).collect()
                                }))
                        })
                        .collect(),
                )
        })
        .collect();
    let request = AlterPartitionReassignmentsRequest::default()
        .with_timeout_ms(options.timeout_ms)
        .with_allow_replication_factor_change(options.allow_replication_factor_change)
        .with_topics(topics);

    (header, request)
}

/// Build a `DescribeQuorum` request.
pub fn convert_list_partition_reassignments_response(
    response: ListPartitionReassignmentsResponse,
) -> ListPartitionReassignmentsResponseData {
    ListPartitionReassignmentsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        topics: response
            .topics
            .into_iter()
            .map(|topic| TopicReassignment {
                name: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| PartitionReassignment {
                        partition_index: partition.partition_index,
                        replicas: partition.replicas.into_iter().map(i32::from).collect(),
                        adding_replicas: partition
                            .adding_replicas
                            .into_iter()
                            .map(i32::from)
                            .collect(),
                        removing_replicas: partition
                            .removing_replicas
                            .into_iter()
                            .map(i32::from)
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `AlterPartitionReassignmentsResponse` into the crate's public shape.
pub fn convert_alter_partition_reassignments_response(
    response: AlterPartitionReassignmentsResponse,
) -> AlterPartitionReassignmentsResponseData {
    AlterPartitionReassignmentsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        allow_replication_factor_change: response.allow_replication_factor_change,
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        responses: response
            .responses
            .into_iter()
            .map(|topic| AlterPartitionReassignmentsTopicResult {
                name: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| AlterPartitionReassignmentsPartitionResult {
                        partition_index: partition.partition_index,
                        error_code: partition.error_code,
                        error_message: partition.error_message.map(|message| message.to_string()),
                    })
                    .collect(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::messages::BrokerId;
    use kafka_protocol::messages::alter_partition_reassignments_response::{
        ReassignablePartitionResponse as KpReassignablePartitionResponse,
        ReassignableTopicResponse as KpReassignableTopicResponse,
    };
    use kafka_protocol::messages::list_partition_reassignments_response::{
        OngoingPartitionReassignment as KpOngoingPartitionReassignment,
        OngoingTopicReassignment as KpOngoingTopicReassignment,
    };
    use kafka_protocol::protocol::StrBytes;

    #[test]
    fn list_partition_reassignments_request_accepts_timeout_and_filter() {
        let filter = [TopicPartitionFilter::new("topic-a", [1])];
        let (header, request) =
            build_list_partition_reassignments_request(16, "client-k", Some(&filter), 5000);

        assert_eq!(
            header.request_api_key,
            ApiKey::ListPartitionReassignments as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_LIST_PARTITION_REASSIGNMENTS
        );
        assert_eq!(request.timeout_ms, 5000);
        let topic = &request.topics.as_ref().unwrap()[0];
        assert_eq!(topic.name.to_string(), "topic-a");
        assert_eq!(topic.partition_indexes, vec![1]);
    }
    #[test]
    fn alter_partition_reassignments_request_preserves_replicas_and_cancellations() {
        let options =
            AlterPartitionReassignmentsOptions::new([PartitionReassignmentTopicSpec::new(
                "topic-a",
                [
                    PartitionReassignmentSpec::new(0, [1, 2, 3]),
                    PartitionReassignmentSpec::cancel(1),
                ],
            )])
            .with_timeout_ms(6_000)
            .with_allow_replication_factor_change(false);
        let (header, request) =
            build_alter_partition_reassignments_request(18, "client-l", &options);

        assert_eq!(
            header.request_api_key,
            ApiKey::AlterPartitionReassignments as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_ALTER_PARTITION_REASSIGNMENTS
        );
        assert_eq!(request.timeout_ms, 6_000);
        assert!(!request.allow_replication_factor_change);
        let topic = &request.topics[0];
        assert_eq!(topic.name.to_string(), "topic-a");
        assert_eq!(topic.partitions[0].partition_index, 0);
        assert_eq!(
            topic.partitions[0]
                .replicas
                .as_ref()
                .unwrap()
                .iter()
                .copied()
                .map(i32::from)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(topic.partitions[1].partition_index, 1);
        assert!(topic.partitions[1].replicas.is_none());
    }
    #[test]
    fn convert_list_partition_reassignments_response_preserves_replica_sets() {
        let response = ListPartitionReassignmentsResponse::default()
            .with_throttle_time_ms(17)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_topics(vec![
                KpOngoingTopicReassignment::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpOngoingPartitionReassignment::default()
                            .with_partition_index(0)
                            .with_replicas(vec![BrokerId::from(1), BrokerId::from(2)])
                            .with_adding_replicas(vec![BrokerId::from(3)])
                            .with_removing_replicas(vec![BrokerId::from(1)]),
                    ]),
            ]);

        let converted = convert_list_partition_reassignments_response(response);

        assert_eq!(converted.throttle_time_ms, 17);
        assert_eq!(converted.error_message, Some("ok".to_owned()));
        assert_eq!(converted.topics[0].name, "topic-a");
        assert_eq!(converted.topics[0].partitions[0].replicas, vec![1, 2]);
        assert_eq!(converted.topics[0].partitions[0].adding_replicas, vec![3]);
        assert_eq!(converted.topics[0].partitions[0].removing_replicas, vec![1]);
    }
    #[test]
    fn convert_alter_partition_reassignments_response_preserves_nested_errors() {
        let response = AlterPartitionReassignmentsResponse::default()
            .with_throttle_time_ms(19)
            .with_allow_replication_factor_change(false)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_responses(vec![
                KpReassignableTopicResponse::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpReassignablePartitionResponse::default()
                            .with_partition_index(1)
                            .with_error_code(15)
                            .with_error_message(Some(StrBytes::from_static_str("denied"))),
                    ]),
            ]);

        let converted = convert_alter_partition_reassignments_response(response);

        assert_eq!(converted.throttle_time_ms, 19);
        assert!(!converted.allow_replication_factor_change);
        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.error_message, Some("ok".to_owned()));
        assert_eq!(converted.responses[0].name, "topic-a");
        assert_eq!(converted.responses[0].partitions[0].partition_index, 1);
        assert_eq!(converted.responses[0].partitions[0].error_code, 15);
        assert_eq!(
            converted.responses[0].partitions[0].error_message,
            Some("denied".to_owned())
        );
    }
}
