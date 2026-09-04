//! ShareAcknowledge types, request builders, and response converters.

use kafka_protocol::messages::{
    ApiKey, RequestHeader, ShareAcknowledgeRequest, ShareAcknowledgeResponse,
    share_acknowledge_request as sa_req, share_acknowledge_response as sa_resp,
};
use uuid::Uuid;

use super::fetch::ShareFetchTopic;
use super::{
    API_VERSION_SHARE_ACKNOWLEDGE, ShareAcknowledgementBatch, ShareLeader, ShareNodeEndpoint,
    optional_group_id, optional_str_bytes, optional_string, request_header,
};

/// One partition in a `ShareAcknowledge` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Acknowledgement batches.
    pub acknowledgement_batches: Vec<ShareAcknowledgementBatch>,
}

impl ShareAcknowledgePartition {
    /// Create a share acknowledgement partition.
    #[must_use]
    pub fn new<I>(partition_index: i32, acknowledgement_batches: I) -> Self
    where
        I: IntoIterator<Item = ShareAcknowledgementBatch>,
    {
        Self {
            partition_index,
            acknowledgement_batches: acknowledgement_batches.into_iter().collect(),
        }
    }
}

/// One topic in a `ShareAcknowledge` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeTopic {
    /// Topic UUID.
    pub topic_id: Uuid,
    /// Partitions containing records to acknowledge.
    pub partitions: Vec<ShareAcknowledgePartition>,
}

impl ShareAcknowledgeTopic {
    /// Create a share acknowledgement topic.
    #[must_use]
    pub fn new<I>(topic_id: Uuid, partitions: I) -> Self
    where
        I: IntoIterator<Item = ShareAcknowledgePartition>,
    {
        Self {
            topic_id,
            partitions: partitions.into_iter().collect(),
        }
    }
}

impl From<ShareFetchTopic> for ShareAcknowledgeTopic {
    fn from(topic: ShareFetchTopic) -> Self {
        Self {
            topic_id: topic.topic_id,
            partitions: topic
                .partitions
                .into_iter()
                .map(|partition| ShareAcknowledgePartition {
                    partition_index: partition.partition_index,
                    acknowledgement_batches: partition.acknowledgement_batches,
                })
                .collect(),
        }
    }
}

/// Options for `ShareAcknowledge`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeOptions {
    /// Share group ID.
    pub group_id: Option<String>,
    /// Share group member ID.
    pub member_id: Option<String>,
    /// Current share session epoch.
    pub share_session_epoch: i32,
    /// Topics containing acknowledgements.
    pub topics: Vec<ShareAcknowledgeTopic>,
}

impl ShareAcknowledgeOptions {
    /// Create a share acknowledgement request.
    #[must_use]
    pub fn new(group_id: impl Into<String>, member_id: impl Into<String>) -> Self {
        Self {
            group_id: Some(group_id.into()),
            member_id: Some(member_id.into()),
            share_session_epoch: 0,
            topics: Vec::new(),
        }
    }

    /// Set topics to acknowledge.
    #[must_use]
    pub fn with_topics<I, T>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<ShareAcknowledgeTopic>,
    {
        self.topics = topics.into_iter().map(Into::into).collect();
        self
    }
}

/// One partition in a `ShareAcknowledge` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgePartitionResponse {
    /// Partition index.
    pub partition_index: i32,
    /// Error code.
    pub error_code: i16,
    /// Optional error message.
    pub error_message: Option<String>,
    /// Current leader.
    pub current_leader: ShareLeader,
}

/// One topic in a `ShareAcknowledge` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeTopicResponse {
    /// Topic UUID.
    pub topic_id: Uuid,
    /// Partition responses.
    pub partitions: Vec<ShareAcknowledgePartitionResponse>,
}

/// Backwards-friendly alias for one `ShareAcknowledge` topic response.
#[allow(dead_code)]
pub type ShareAcknowledgeTopicResponseData = ShareAcknowledgeTopicResponse;

/// Parsed response from `ShareAcknowledge`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level response error code.
    pub error_code: i16,
    /// Optional top-level error message.
    pub error_message: Option<String>,
    /// Topic responses.
    pub responses: Vec<ShareAcknowledgeTopicResponse>,
    /// Current leader endpoints.
    pub node_endpoints: Vec<ShareNodeEndpoint>,
}

/// Build a `ShareAcknowledge` request.
pub fn build_share_acknowledge_request(
    correlation_id: i32,
    client_id: &str,
    options: &ShareAcknowledgeOptions,
) -> (RequestHeader, ShareAcknowledgeRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ShareAcknowledge,
        API_VERSION_SHARE_ACKNOWLEDGE,
    );
    let request = ShareAcknowledgeRequest::default()
        .with_group_id(optional_group_id(options.group_id.clone()))
        .with_member_id(optional_str_bytes(options.member_id.clone()))
        .with_share_session_epoch(options.share_session_epoch)
        .with_topics(to_share_acknowledge_topics(&options.topics));

    (header, request)
}

/// Convert a generated `ShareAcknowledgeResponse`.
#[must_use]
pub fn convert_share_acknowledge_response(
    response: ShareAcknowledgeResponse,
) -> ShareAcknowledgeResponseData {
    ShareAcknowledgeResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: optional_string(response.error_message),
        responses: response
            .responses
            .into_iter()
            .map(|topic| ShareAcknowledgeTopicResponse {
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(convert_share_acknowledge_partition_response)
                    .collect(),
            })
            .collect(),
        node_endpoints: response
            .node_endpoints
            .into_iter()
            .map(convert_share_acknowledge_node_endpoint)
            .collect(),
    }
}

fn to_share_acknowledgement_batch_request(
    batch: &ShareAcknowledgementBatch,
) -> sa_req::AcknowledgementBatch {
    sa_req::AcknowledgementBatch::default()
        .with_first_offset(batch.first_offset)
        .with_last_offset(batch.last_offset)
        .with_acknowledge_types(batch.acknowledge_types.clone())
}

fn to_share_acknowledge_topics(topics: &[ShareAcknowledgeTopic]) -> Vec<sa_req::AcknowledgeTopic> {
    topics
        .iter()
        .map(|topic| {
            sa_req::AcknowledgeTopic::default()
                .with_topic_id(topic.topic_id)
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            sa_req::AcknowledgePartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_acknowledgement_batches(
                                    partition
                                        .acknowledgement_batches
                                        .iter()
                                        .map(to_share_acknowledgement_batch_request)
                                        .collect(),
                                )
                        })
                        .collect(),
                )
        })
        .collect()
}

fn convert_share_acknowledge_partition_response(
    partition: sa_resp::PartitionData,
) -> ShareAcknowledgePartitionResponse {
    ShareAcknowledgePartitionResponse {
        partition_index: partition.partition_index,
        error_code: partition.error_code,
        error_message: optional_string(partition.error_message),
        current_leader: ShareLeader {
            leader_id: partition.current_leader.leader_id,
            leader_epoch: partition.current_leader.leader_epoch,
        },
    }
}

fn convert_share_acknowledge_node_endpoint(
    endpoint: sa_resp::NodeEndpoint,
) -> ShareNodeEndpoint {
    ShareNodeEndpoint {
        node_id: *endpoint.node_id,
        host: endpoint.host.to_string(),
        port: endpoint.port,
        rack: optional_string(endpoint.rack),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::str_bytes;
    use kafka_protocol::messages::BrokerId;

    use super::super::SHARE_ACK_TYPE_REJECT;

    #[test]
    fn share_acknowledge_request_maps_topics_and_batches() {
        let topic_id = Uuid::from_u128(4);
        let mut options = ShareAcknowledgeOptions::new("share-a", "member-a").with_topics([
            ShareAcknowledgeTopic::new(
                topic_id,
                [ShareAcknowledgePartition::new(
                    1,
                    [ShareAcknowledgementBatch::new(
                        2,
                        3,
                        [SHARE_ACK_TYPE_REJECT],
                    )],
                )],
            ),
        ]);
        options.share_session_epoch = 8;

        let (header, request) = build_share_acknowledge_request(14, "client-d", &options);

        assert_eq!(header.request_api_key, ApiKey::ShareAcknowledge as i16);
        assert_eq!(header.request_api_version, API_VERSION_SHARE_ACKNOWLEDGE);
        assert_eq!(request.share_session_epoch, 8);
        assert_eq!(request.topics[0].topic_id, topic_id);
        assert_eq!(request.topics[0].partitions[0].partition_index, 1);
        assert_eq!(
            request.topics[0].partitions[0].acknowledgement_batches[0].acknowledge_types,
            vec![SHARE_ACK_TYPE_REJECT]
        );
    }

    #[test]
    fn share_acknowledge_response_maps_partitions_and_endpoints() {
        let topic_id = Uuid::from_u128(7);
        let response = ShareAcknowledgeResponse::default()
            .with_throttle_time_ms(2)
            .with_error_code(3)
            .with_error_message(Some(str_bytes("top".to_owned())))
            .with_responses(vec![
                sa_resp::ShareAcknowledgeTopicResponse::default()
                    .with_topic_id(topic_id)
                    .with_partitions(vec![
                        sa_resp::PartitionData::default()
                            .with_partition_index(4)
                            .with_error_code(5)
                            .with_error_message(Some(str_bytes("partition".to_owned())))
                            .with_current_leader(
                                sa_resp::LeaderIdAndEpoch::default()
                                    .with_leader_id(6)
                                    .with_leader_epoch(7),
                            ),
                    ]),
            ])
            .with_node_endpoints(vec![
                sa_resp::NodeEndpoint::default()
                    .with_node_id(BrokerId(6))
                    .with_host(str_bytes("broker-a".to_owned()))
                    .with_port(9093),
            ]);

        let converted = convert_share_acknowledge_response(response);

        assert_eq!(converted.throttle_time_ms, 2);
        assert_eq!(converted.error_code, 3);
        assert_eq!(converted.error_message.as_deref(), Some("top"));
        assert_eq!(converted.responses[0].topic_id, topic_id);
        assert_eq!(
            converted.responses[0].partitions[0],
            ShareAcknowledgePartitionResponse {
                partition_index: 4,
                error_code: 5,
                error_message: Some("partition".to_owned()),
                current_leader: ShareLeader {
                    leader_id: 6,
                    leader_epoch: 7,
                },
            }
        );
        assert_eq!(
            converted.node_endpoints,
            vec![ShareNodeEndpoint {
                node_id: 6,
                host: "broker-a".to_owned(),
                port: 9093,
                rack: None,
            }]
        );
    }
}
