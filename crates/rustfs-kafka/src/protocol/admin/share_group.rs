#![allow(clippy::wildcard_imports)]
//! Share group offset administration helpers.

use kafka_protocol::messages::{
    AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsResponse, ApiKey,
    DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsResponse,
    DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{
    API_VERSION_ALTER_SHARE_GROUP_OFFSETS, API_VERSION_DELETE_SHARE_GROUP_OFFSETS,
    API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
};
use super::*;

/// Topic partitions in a share group assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupTopicPartitions {
    /// Topic UUID as a string.
    pub topic_id: String,
    /// Topic name.
    pub topic_name: String,
    /// Assigned partition indexes.
    pub partitions: Vec<i32>,
}

/// Assignment returned by `ShareGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupAssignment {
    /// Topic partitions in the assignment.
    pub topic_partitions: Vec<ShareGroupTopicPartitions>,
}

/// Member state returned by `ShareGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupMemberDescription {
    /// Member ID assigned by the share group coordinator.
    pub member_id: String,
    /// Rack ID reported by the member, when configured.
    pub rack_id: Option<String>,
    /// Current member epoch.
    pub member_epoch: i32,
    /// Client ID reported by the member.
    pub client_id: String,
    /// Client host reported by the broker.
    pub client_host: String,
    /// Subscribed topic names.
    pub subscribed_topic_names: Vec<String>,
    /// Current assignment.
    pub assignment: ShareGroupAssignment,
}

/// Share group state returned by `ShareGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupDescription {
    /// Per-group broker error code.
    pub error_code: i16,
    /// Optional per-group broker error message.
    pub error_message: Option<String>,
    /// Group ID.
    pub group_id: String,
    /// Current group state.
    pub group_state: String,
    /// Current group epoch.
    pub group_epoch: i32,
    /// Current assignment epoch.
    pub assignment_epoch: i32,
    /// Selected assignor name.
    pub assignor_name: String,
    /// Members in the group.
    pub members: Vec<ShareGroupMemberDescription>,
    /// Authorized operations bitfield, or Kafka's sentinel when not requested.
    pub authorized_operations: i32,
}

/// Parsed response from a `ShareGroupDescribe` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupDescribeResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Described share groups returned by the broker.
    pub groups: Vec<ShareGroupDescription>,
}

/// One group included in a `DescribeShareGroupOffsets` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffsetRequest {
    /// Share group ID.
    pub group_id: String,
    /// Topic partitions to query, or `None` to describe all share-partition offsets for the group.
    pub topics: Option<Vec<TopicPartitionFilter>>,
}

impl ShareGroupOffsetRequest {
    /// Describe all share-partition offsets for a group.
    #[must_use]
    pub fn all_partitions(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            topics: None,
        }
    }

    /// Describe selected share-partition offsets for a group.
    #[must_use]
    pub fn with_topics<I>(group_id: impl Into<String>, topics: I) -> Self
    where
        I: IntoIterator<Item = TopicPartitionFilter>,
    {
        Self {
            group_id: group_id.into(),
            topics: Some(topics.into_iter().collect()),
        }
    }
}

/// Share group offset state for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffsetPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Share-partition start offset.
    pub start_offset: i64,
    /// Partition leader epoch.
    pub leader_epoch: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional per-partition broker error message.
    pub error_message: Option<String>,
}

/// Share group offset state for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffsetTopic {
    /// Topic name.
    pub topic_name: String,
    /// Topic UUID as a string.
    pub topic_id: String,
    /// Partition offset states.
    pub partitions: Vec<ShareGroupOffsetPartition>,
}

/// Share group offset state for one group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffsetGroup {
    /// Share group ID.
    pub group_id: String,
    /// Topic offset states returned by the broker.
    pub topics: Vec<ShareGroupOffsetTopic>,
    /// Per-group broker error code.
    pub error_code: i16,
    /// Optional per-group broker error message.
    pub error_message: Option<String>,
}

/// Parsed response from a `DescribeShareGroupOffsets` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-group share offset states returned by the broker.
    pub groups: Vec<ShareGroupOffsetGroup>,
}

/// A partition offset to alter for a share group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetPartition {
    /// Partition index.
    pub partition_index: i32,
    /// The new offset value.
    pub offset: i64,
}

impl AlterShareGroupOffsetPartition {
    /// Create a partition offset spec.
    #[must_use]
    pub fn new(partition_index: i32, offset: i64) -> Self {
        Self {
            partition_index,
            offset,
        }
    }
}

/// A topic with partition offsets for `AlterShareGroupOffsets`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetTopic {
    /// Topic name.
    pub topic_name: String,
    /// Partition offsets to alter.
    pub partitions: Vec<AlterShareGroupOffsetPartition>,
}

impl AlterShareGroupOffsetTopic {
    /// Create a topic with partition offsets.
    #[must_use]
    pub fn new<I>(topic_name: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = AlterShareGroupOffsetPartition>,
    {
        Self {
            topic_name: topic_name.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// A partition result in `AlterShareGroupOffsets` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// A topic result in `AlterShareGroupOffsets` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetTopicResult {
    /// Topic name.
    pub topic_name: String,
    /// Per-partition results.
    pub partitions: Vec<AlterShareGroupOffsetPartitionResult>,
}

/// Parsed response from an `AlterShareGroupOffsets` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: i16,
    /// Optional top-level error message.
    pub error_message: Option<String>,
    /// Per-topic results.
    pub responses: Vec<AlterShareGroupOffsetTopicResult>,
}

/// A topic whose share-group offsets are deleted by `DeleteShareGroupOffsets`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetTopic {
    /// Topic name.
    pub topic_name: String,
}

impl DeleteShareGroupOffsetTopic {
    /// Create a topic whose share-group offsets should be deleted.
    #[must_use]
    pub fn new(topic_name: impl Into<String>) -> Self {
        Self {
            topic_name: topic_name.into(),
        }
    }
}

/// A topic result in `DeleteShareGroupOffsets` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetTopicResult {
    /// Topic name.
    pub topic_name: String,
    /// Per-topic broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Parsed response from a `DeleteShareGroupOffsets` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: i16,
    /// Optional top-level error message.
    pub error_message: Option<String>,
    /// Per-topic results.
    pub responses: Vec<DeleteShareGroupOffsetTopicResult>,
}

pub fn build_describe_share_group_offsets_request(
    correlation_id: i32,
    client_id: &str,
    groups: &[ShareGroupOffsetRequest],
) -> (RequestHeader, DescribeShareGroupOffsetsRequest) {
    use kafka_protocol::messages::describe_share_group_offsets_request::{
        DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeShareGroupOffsets,
        API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
    );
    let groups = groups
        .iter()
        .map(|group| {
            let topics = group.topics.as_ref().map(|topics| {
                topics
                    .iter()
                    .map(|topic| {
                        DescribeShareGroupOffsetsRequestTopic::default()
                            .with_topic_name(StrBytes::from_string(topic.topic.clone()).into())
                            .with_partitions(topic.partitions.clone())
                    })
                    .collect()
            });
            DescribeShareGroupOffsetsRequestGroup::default()
                .with_group_id(group_id(&group.group_id))
                .with_topics(topics)
        })
        .collect();
    let request = DescribeShareGroupOffsetsRequest::default().with_groups(groups);

    (header, request)
}

/// Build a `DescribeClientQuotas` request.
pub fn build_alter_share_group_offsets_request(
    correlation_id: i32,
    client_id: &str,
    group_id_str: &str,
    topics: &[AlterShareGroupOffsetTopic],
) -> (RequestHeader, AlterShareGroupOffsetsRequest) {
    use kafka_protocol::messages::alter_share_group_offsets_request::{
        AlterShareGroupOffsetsRequestPartition, AlterShareGroupOffsetsRequestTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::AlterShareGroupOffsets,
        API_VERSION_ALTER_SHARE_GROUP_OFFSETS,
    );
    let topic_list: Vec<AlterShareGroupOffsetsRequestTopic> = topics
        .iter()
        .map(|topic| {
            AlterShareGroupOffsetsRequestTopic::default()
                .with_topic_name(StrBytes::from_string(topic.topic_name.clone()).into())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|p| {
                            AlterShareGroupOffsetsRequestPartition::default()
                                .with_partition_index(p.partition_index)
                                .with_start_offset(p.offset)
                        })
                        .collect(),
                )
        })
        .collect();
    let request = AlterShareGroupOffsetsRequest::default()
        .with_group_id(group_id(group_id_str))
        .with_topics(topic_list);

    (header, request)
}

/// Build a `DeleteShareGroupOffsets` request.
pub fn build_delete_share_group_offsets_request(
    correlation_id: i32,
    client_id: &str,
    group_id_str: &str,
    topics: &[DeleteShareGroupOffsetTopic],
) -> (RequestHeader, DeleteShareGroupOffsetsRequest) {
    use kafka_protocol::messages::delete_share_group_offsets_request::DeleteShareGroupOffsetsRequestTopic;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DeleteShareGroupOffsets,
        API_VERSION_DELETE_SHARE_GROUP_OFFSETS,
    );
    let topic_list: Vec<DeleteShareGroupOffsetsRequestTopic> = topics
        .iter()
        .map(|topic| {
            DeleteShareGroupOffsetsRequestTopic::default()
                .with_topic_name(StrBytes::from_string(topic.topic_name.clone()).into())
        })
        .collect();
    let request = DeleteShareGroupOffsetsRequest::default()
        .with_group_id(group_id(group_id_str))
        .with_topics(topic_list);

    (header, request)
}

/// Convert a generated `DescribeClusterResponse` into the crate's public shape.
pub fn convert_describe_share_group_offsets_response(
    response: DescribeShareGroupOffsetsResponse,
) -> DescribeShareGroupOffsetsResponseData {
    DescribeShareGroupOffsetsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        groups: response
            .groups
            .into_iter()
            .map(|group| ShareGroupOffsetGroup {
                group_id: group.group_id.to_string(),
                topics: group
                    .topics
                    .into_iter()
                    .map(|topic| ShareGroupOffsetTopic {
                        topic_name: topic.topic_name.to_string(),
                        topic_id: topic.topic_id.to_string(),
                        partitions: topic
                            .partitions
                            .into_iter()
                            .map(|partition| ShareGroupOffsetPartition {
                                partition_index: partition.partition_index,
                                start_offset: partition.start_offset,
                                leader_epoch: partition.leader_epoch,
                                error_code: partition.error_code,
                                error_message: partition
                                    .error_message
                                    .map(|message| message.to_string()),
                            })
                            .collect(),
                    })
                    .collect(),
                error_code: group.error_code,
                error_message: group.error_message.map(|message| message.to_string()),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeClientQuotasResponse` into the crate's public shape.
pub fn convert_alter_share_group_offsets_response(
    response: AlterShareGroupOffsetsResponse,
) -> AlterShareGroupOffsetsResponseData {
    AlterShareGroupOffsetsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|m| m.to_string()),
        responses: response
            .responses
            .into_iter()
            .map(|topic| AlterShareGroupOffsetTopicResult {
                topic_name: topic.topic_name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|p| AlterShareGroupOffsetPartitionResult {
                        partition_index: p.partition_index,
                        error_code: p.error_code,
                        error_message: p.error_message.map(|m| m.to_string()),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `DeleteShareGroupOffsetsResponse` into the crate's public shape.
pub fn convert_delete_share_group_offsets_response(
    response: DeleteShareGroupOffsetsResponse,
) -> DeleteShareGroupOffsetsResponseData {
    DeleteShareGroupOffsetsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|m| m.to_string()),
        responses: response
            .responses
            .into_iter()
            .map(|topic| DeleteShareGroupOffsetTopicResult {
                topic_name: topic.topic_name.to_string(),
                error_code: topic.error_code,
                error_message: topic.error_message.map(|m| m.to_string()),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::messages::describe_share_group_offsets_response::{
        DescribeShareGroupOffsetsResponseGroup as KpShareGroupOffsetGroup,
        DescribeShareGroupOffsetsResponsePartition as KpShareGroupOffsetPartition,
        DescribeShareGroupOffsetsResponseTopic as KpShareGroupOffsetTopic,
    };
    use kafka_protocol::protocol::StrBytes;

    #[test]
    fn describe_share_group_offsets_request_distinguishes_all_and_filtered_topics() {
        let groups = [
            ShareGroupOffsetRequest::all_partitions("share-a"),
            ShareGroupOffsetRequest::with_topics(
                "share-b",
                [TopicPartitionFilter::new("topic-a", [0, 2])],
            ),
        ];
        let (header, request) = build_describe_share_group_offsets_request(23, "client-o", &groups);

        assert_eq!(
            header.request_api_key,
            ApiKey::DescribeShareGroupOffsets as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS
        );
        assert_eq!(request.groups[0].group_id.to_string(), "share-a");
        assert!(request.groups[0].topics.is_none());
        assert_eq!(request.groups[1].group_id.to_string(), "share-b");
        let topics = request.groups[1].topics.as_ref().unwrap();
        assert_eq!(topics[0].topic_name.to_string(), "topic-a");
        assert_eq!(topics[0].partitions, vec![0, 2]);
    }
    #[test]
    fn convert_describe_share_group_offsets_response_preserves_offsets() {
        let response = DescribeShareGroupOffsetsResponse::default()
            .with_throttle_time_ms(27)
            .with_groups(vec![
                KpShareGroupOffsetGroup::default()
                    .with_group_id(group_id("share-a"))
                    .with_topics(vec![
                        KpShareGroupOffsetTopic::default()
                            .with_topic_name(StrBytes::from_static_str("topic-a").into())
                            .with_partitions(vec![
                                KpShareGroupOffsetPartition::default()
                                    .with_partition_index(1)
                                    .with_start_offset(42)
                                    .with_leader_epoch(7)
                                    .with_error_code(0)
                                    .with_error_message(Some(StrBytes::from_static_str("ok"))),
                            ]),
                    ])
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok"))),
            ]);

        let converted = convert_describe_share_group_offsets_response(response);

        assert_eq!(converted.throttle_time_ms, 27);
        assert_eq!(converted.groups[0].group_id, "share-a");
        assert_eq!(converted.groups[0].error_message, Some("ok".to_owned()));
        let topic = &converted.groups[0].topics[0];
        assert_eq!(topic.topic_name, "topic-a");
        assert_eq!(topic.topic_id, "00000000-0000-0000-0000-000000000000");
        let partition = &topic.partitions[0];
        assert_eq!(partition.partition_index, 1);
        assert_eq!(partition.start_offset, 42);
        assert_eq!(partition.leader_epoch, 7);
        assert_eq!(partition.error_code, 0);
        assert_eq!(partition.error_message, Some("ok".to_owned()));
    }
    #[test]
    fn alter_share_group_offsets_request_includes_group_and_topics() {
        let topics = vec![AlterShareGroupOffsetTopic::new(
            "topic-a",
            [
                AlterShareGroupOffsetPartition::new(0, 42),
                AlterShareGroupOffsetPartition::new(1, 99),
            ],
        )];
        let (header, request) =
            build_alter_share_group_offsets_request(10, "test-client", "my-group", &topics);

        assert_eq!(
            header.request_api_key,
            ApiKey::AlterShareGroupOffsets as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_ALTER_SHARE_GROUP_OFFSETS
        );
        assert_eq!(request.group_id.as_str(), "my-group");
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].topic_name.as_str(), "topic-a");
        assert_eq!(request.topics[0].partitions.len(), 2);
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
        assert_eq!(request.topics[0].partitions[0].start_offset, 42);
        assert_eq!(request.topics[0].partitions[1].partition_index, 1);
        assert_eq!(request.topics[0].partitions[1].start_offset, 99);
    }

    #[test]
    fn delete_share_group_offsets_request_includes_group_and_topics() {
        let topics = vec![DeleteShareGroupOffsetTopic::new("topic-a")];
        let (header, request) =
            build_delete_share_group_offsets_request(11, "test-client", "my-group", &topics);

        assert_eq!(
            header.request_api_key,
            ApiKey::DeleteShareGroupOffsets as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_DELETE_SHARE_GROUP_OFFSETS
        );
        assert_eq!(request.group_id.as_str(), "my-group");
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].topic_name.as_str(), "topic-a");
    }

    #[test]
    fn alter_share_group_offsets_response_maps_all_fields() {
        use kafka_protocol::messages::alter_share_group_offsets_response::{
            AlterShareGroupOffsetsResponsePartition as KpAlterSharePartition,
            AlterShareGroupOffsetsResponseTopic as KpAlterShareTopic,
        };

        let response = AlterShareGroupOffsetsResponse::default()
            .with_throttle_time_ms(50)
            .with_error_code(0)
            .with_error_message(None)
            .with_responses(vec![
                KpAlterShareTopic::default()
                    .with_topic_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpAlterSharePartition::default()
                            .with_partition_index(0)
                            .with_error_code(0)
                            .with_error_message(None),
                        KpAlterSharePartition::default()
                            .with_partition_index(1)
                            .with_error_code(42)
                            .with_error_message(Some(StrBytes::from_static_str("some error"))),
                    ]),
            ]);

        let converted = convert_alter_share_group_offsets_response(response);

        assert_eq!(converted.throttle_time_ms, 50);
        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.error_message, None);
        assert_eq!(converted.responses.len(), 1);
        assert_eq!(converted.responses[0].topic_name, "topic-a");
        assert_eq!(converted.responses[0].partitions.len(), 2);
        assert_eq!(converted.responses[0].partitions[0].partition_index, 0);
        assert_eq!(converted.responses[0].partitions[0].error_code, 0);
        assert_eq!(converted.responses[0].partitions[0].error_message, None);
        assert_eq!(converted.responses[0].partitions[1].partition_index, 1);
        assert_eq!(converted.responses[0].partitions[1].error_code, 42);
        assert_eq!(
            converted.responses[0].partitions[1].error_message,
            Some("some error".to_owned())
        );
    }

    #[test]
    fn delete_share_group_offsets_response_maps_all_fields() {
        use kafka_protocol::messages::delete_share_group_offsets_response::DeleteShareGroupOffsetsResponseTopic as KpDeleteShareTopic;

        let response = DeleteShareGroupOffsetsResponse::default()
            .with_throttle_time_ms(75)
            .with_error_code(0)
            .with_error_message(None)
            .with_responses(vec![
                KpDeleteShareTopic::default()
                    .with_topic_name(StrBytes::from_static_str("topic-a").into())
                    .with_error_code(0)
                    .with_error_message(None),
                KpDeleteShareTopic::default()
                    .with_topic_name(StrBytes::from_static_str("topic-b").into())
                    .with_error_code(3)
                    .with_error_message(Some(StrBytes::from_static_str("unknown topic"))),
            ]);

        let converted = convert_delete_share_group_offsets_response(response);

        assert_eq!(converted.throttle_time_ms, 75);
        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.error_message, None);
        assert_eq!(converted.responses.len(), 2);
        assert_eq!(converted.responses[0].topic_name, "topic-a");
        assert_eq!(converted.responses[0].error_code, 0);
        assert_eq!(converted.responses[0].error_message, None);
        assert_eq!(converted.responses[1].topic_name, "topic-b");
        assert_eq!(converted.responses[1].error_code, 3);
        assert_eq!(
            converted.responses[1].error_message,
            Some("unknown topic".to_owned())
        );
    }
}
