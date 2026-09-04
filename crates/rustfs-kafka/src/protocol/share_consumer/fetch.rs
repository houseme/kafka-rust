//! ShareFetch types, request builders, and response converters.

use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, RequestHeader, ShareFetchRequest, ShareFetchResponse, share_fetch_request as sf_req,
    share_fetch_response as sf_resp,
};
use uuid::Uuid;

use super::{
    API_VERSION_SHARE_FETCH, ShareAcknowledgementBatch, ShareLeader, ShareNodeEndpoint,
    optional_group_id, optional_str_bytes, optional_string, request_header,
};

/// One partition in a `ShareFetch` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Acknowledgement batches to send with this fetch.
    pub acknowledgement_batches: Vec<ShareAcknowledgementBatch>,
}

impl ShareFetchPartition {
    /// Create a share fetch partition.
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

/// One topic in a `ShareFetch` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchTopic {
    /// Topic UUID.
    pub topic_id: Uuid,
    /// Partitions to fetch from or acknowledge.
    pub partitions: Vec<ShareFetchPartition>,
}

impl ShareFetchTopic {
    /// Create a share fetch topic.
    #[must_use]
    pub fn new<I>(topic_id: Uuid, partitions: I) -> Self
    where
        I: IntoIterator<Item = ShareFetchPartition>,
    {
        Self {
            topic_id,
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// One forgotten topic entry in a `ShareFetch` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgottenShareFetchTopic {
    /// Topic UUID.
    pub topic_id: Uuid,
    /// Partition indexes to remove from the share session.
    pub partitions: Vec<i32>,
}

impl ForgottenShareFetchTopic {
    /// Create a forgotten share topic entry.
    #[must_use]
    pub fn new<I>(topic_id: Uuid, partitions: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        Self {
            topic_id,
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// Options for `ShareFetch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchOptions {
    /// Share group ID.
    pub group_id: Option<String>,
    /// Share group member ID.
    pub member_id: Option<String>,
    /// Current share session epoch.
    pub share_session_epoch: i32,
    /// Maximum wait in milliseconds.
    pub max_wait_ms: i32,
    /// Minimum response bytes.
    pub min_bytes: i32,
    /// Maximum response bytes.
    pub max_bytes: i32,
    /// Maximum records to fetch.
    pub max_records: i32,
    /// Optimal acquired-record/acknowledgement batch size.
    pub batch_size: i32,
    /// Topics to fetch from.
    pub topics: Vec<ShareFetchTopic>,
    /// Topics to remove from this share session.
    pub forgotten_topics_data: Vec<ForgottenShareFetchTopic>,
}

impl ShareFetchOptions {
    /// Create a share fetch request.
    #[must_use]
    pub fn new(group_id: impl Into<String>, member_id: impl Into<String>) -> Self {
        Self {
            group_id: Some(group_id.into()),
            member_id: Some(member_id.into()),
            share_session_epoch: 0,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            max_records: 500,
            batch_size: 1,
            topics: Vec::new(),
            forgotten_topics_data: Vec::new(),
        }
    }

    /// Set topics to fetch.
    #[must_use]
    pub fn with_topics<I>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = ShareFetchTopic>,
    {
        self.topics = topics.into_iter().collect();
        self
    }

    /// Set topics to remove from the share session.
    #[must_use]
    pub fn with_forgotten_topics_data<I>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = ForgottenShareFetchTopic>,
    {
        self.forgotten_topics_data = topics.into_iter().collect();
        self
    }
}

/// Acquired record range returned by `ShareFetch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcquiredRecords {
    /// First acquired offset.
    pub first_offset: i64,
    /// Last acquired offset.
    pub last_offset: i64,
    /// Delivery count for this range.
    pub delivery_count: i16,
}

/// One partition in a `ShareFetch` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchPartitionResponse {
    /// Partition index.
    pub partition_index: i32,
    /// Fetch error code.
    pub error_code: i16,
    /// Optional fetch error message.
    pub error_message: Option<String>,
    /// Acknowledge error code.
    pub acknowledge_error_code: i16,
    /// Optional acknowledge error message.
    pub acknowledge_error_message: Option<String>,
    /// Current leader.
    pub current_leader: ShareLeader,
    /// Raw record batch bytes.
    pub records: Option<Bytes>,
    /// Acquired record ranges.
    pub acquired_records: Vec<ShareAcquiredRecords>,
}

/// One topic in a `ShareFetch` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchTopicResponse {
    /// Topic UUID.
    pub topic_id: Uuid,
    /// Partition responses.
    pub partitions: Vec<ShareFetchPartitionResponse>,
}

/// Parsed response from `ShareFetch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level response error code.
    pub error_code: i16,
    /// Optional top-level error message.
    pub error_message: Option<String>,
    /// Acquisition lock timeout in milliseconds.
    pub acquisition_lock_timeout_ms: i32,
    /// Topic responses.
    pub responses: Vec<ShareFetchTopicResponse>,
    /// Current leader endpoints.
    pub node_endpoints: Vec<ShareNodeEndpoint>,
}

/// Build a `ShareFetch` request.
pub fn build_share_fetch_request(
    correlation_id: i32,
    client_id: &str,
    options: &ShareFetchOptions,
) -> (RequestHeader, ShareFetchRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ShareFetch,
        API_VERSION_SHARE_FETCH,
    );
    let request = ShareFetchRequest::default()
        .with_group_id(optional_group_id(options.group_id.clone()))
        .with_member_id(optional_str_bytes(options.member_id.clone()))
        .with_share_session_epoch(options.share_session_epoch)
        .with_max_wait_ms(options.max_wait_ms)
        .with_min_bytes(options.min_bytes)
        .with_max_bytes(options.max_bytes)
        .with_max_records(options.max_records)
        .with_batch_size(options.batch_size)
        .with_topics(to_share_fetch_topics(&options.topics))
        .with_forgotten_topics_data(to_forgotten_share_fetch_topics(
            &options.forgotten_topics_data,
        ));

    (header, request)
}

/// Convert a generated `ShareFetchResponse`.
#[must_use]
pub fn convert_share_fetch_response(response: ShareFetchResponse) -> ShareFetchResponseData {
    ShareFetchResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: optional_string(response.error_message),
        acquisition_lock_timeout_ms: response.acquisition_lock_timeout_ms,
        responses: response
            .responses
            .into_iter()
            .map(|topic| ShareFetchTopicResponse {
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(convert_share_fetch_partition_response)
                    .collect(),
            })
            .collect(),
        node_endpoints: response
            .node_endpoints
            .into_iter()
            .map(convert_share_fetch_node_endpoint)
            .collect(),
    }
}

fn to_share_acknowledgement_batch(
    batch: &ShareAcknowledgementBatch,
) -> sf_req::AcknowledgementBatch {
    sf_req::AcknowledgementBatch::default()
        .with_first_offset(batch.first_offset)
        .with_last_offset(batch.last_offset)
        .with_acknowledge_types(batch.acknowledge_types.clone())
}

fn to_share_fetch_topics(topics: &[ShareFetchTopic]) -> Vec<sf_req::FetchTopic> {
    topics
        .iter()
        .map(|topic| {
            sf_req::FetchTopic::default()
                .with_topic_id(topic.topic_id)
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            sf_req::FetchPartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_acknowledgement_batches(
                                    partition
                                        .acknowledgement_batches
                                        .iter()
                                        .map(to_share_acknowledgement_batch)
                                        .collect(),
                                )
                        })
                        .collect(),
                )
        })
        .collect()
}

fn to_forgotten_share_fetch_topics(
    topics: &[ForgottenShareFetchTopic],
) -> Vec<sf_req::ForgottenTopic> {
    topics
        .iter()
        .map(|topic| {
            sf_req::ForgottenTopic::default()
                .with_topic_id(topic.topic_id)
                .with_partitions(topic.partitions.clone())
        })
        .collect()
}

fn convert_share_fetch_partition_response(
    partition: sf_resp::PartitionData,
) -> ShareFetchPartitionResponse {
    ShareFetchPartitionResponse {
        partition_index: partition.partition_index,
        error_code: partition.error_code,
        error_message: optional_string(partition.error_message),
        acknowledge_error_code: partition.acknowledge_error_code,
        acknowledge_error_message: optional_string(partition.acknowledge_error_message),
        current_leader: ShareLeader {
            leader_id: partition.current_leader.leader_id,
            leader_epoch: partition.current_leader.leader_epoch,
        },
        records: partition.records,
        acquired_records: partition
            .acquired_records
            .into_iter()
            .map(|records| ShareAcquiredRecords {
                first_offset: records.first_offset,
                last_offset: records.last_offset,
                delivery_count: records.delivery_count,
            })
            .collect(),
    }
}

fn convert_share_fetch_node_endpoint(endpoint: sf_resp::NodeEndpoint) -> ShareNodeEndpoint {
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

    use super::super::{SHARE_ACK_TYPE_ACCEPT, SHARE_ACK_TYPE_RELEASE};

    #[test]
    fn share_fetch_request_maps_topics_and_acknowledgements() {
        let topic_id = Uuid::from_u128(2);
        let forgotten_topic_id = Uuid::from_u128(3);
        let mut options = ShareFetchOptions::new("share-a", "member-a");
        options.share_session_epoch = 4;
        options.max_wait_ms = 1_000;
        options.min_bytes = 2;
        options.max_bytes = 4_096;
        options.max_records = 10;
        options.batch_size = 5;
        options.topics = vec![ShareFetchTopic::new(
            topic_id,
            [ShareFetchPartition::new(
                7,
                [ShareAcknowledgementBatch::new(
                    12,
                    14,
                    [SHARE_ACK_TYPE_ACCEPT, SHARE_ACK_TYPE_RELEASE],
                )],
            )],
        )];
        options.forgotten_topics_data =
            vec![ForgottenShareFetchTopic::new(forgotten_topic_id, [9])];

        let (header, request) = build_share_fetch_request(13, "client-c", &options);

        assert_eq!(header.request_api_key, ApiKey::ShareFetch as i16);
        assert_eq!(header.request_api_version, API_VERSION_SHARE_FETCH);
        assert_eq!(request.share_session_epoch, 4);
        assert_eq!(request.max_wait_ms, 1_000);
        assert_eq!(request.min_bytes, 2);
        assert_eq!(request.max_bytes, 4_096);
        assert_eq!(request.max_records, 10);
        assert_eq!(request.batch_size, 5);
        assert_eq!(request.topics[0].topic_id, topic_id);
        assert_eq!(request.topics[0].partitions[0].partition_index, 7);
        assert_eq!(
            request.topics[0].partitions[0].acknowledgement_batches[0].acknowledge_types,
            vec![SHARE_ACK_TYPE_ACCEPT, SHARE_ACK_TYPE_RELEASE]
        );
        assert_eq!(
            request.forgotten_topics_data[0].topic_id,
            forgotten_topic_id
        );
        assert_eq!(request.forgotten_topics_data[0].partitions, vec![9]);
    }

    #[test]
    fn share_fetch_response_maps_records_and_endpoints() {
        let topic_id = Uuid::from_u128(6);
        let response = ShareFetchResponse::default()
            .with_throttle_time_ms(7)
            .with_error_code(1)
            .with_error_message(Some(str_bytes("top-error".to_owned())))
            .with_acquisition_lock_timeout_ms(30_000)
            .with_responses(vec![
                sf_resp::ShareFetchableTopicResponse::default()
                    .with_topic_id(topic_id)
                    .with_partitions(vec![
                        sf_resp::PartitionData::default()
                            .with_partition_index(3)
                            .with_error_code(4)
                            .with_error_message(Some(str_bytes("fetch-error".to_owned())))
                            .with_acknowledge_error_code(5)
                            .with_acknowledge_error_message(Some(str_bytes("ack-error".to_owned())))
                            .with_current_leader(
                                sf_resp::LeaderIdAndEpoch::default()
                                    .with_leader_id(9)
                                    .with_leader_epoch(2),
                            )
                            .with_records(Some(Bytes::from_static(b"records")))
                            .with_acquired_records(vec![
                                sf_resp::AcquiredRecords::default()
                                    .with_first_offset(11)
                                    .with_last_offset(12)
                                    .with_delivery_count(2),
                            ]),
                    ]),
            ])
            .with_node_endpoints(vec![
                sf_resp::NodeEndpoint::default()
                    .with_node_id(BrokerId(9))
                    .with_host(str_bytes("broker".to_owned()))
                    .with_port(9092)
                    .with_rack(Some(str_bytes("rack".to_owned()))),
            ]);

        let converted = convert_share_fetch_response(response);

        assert_eq!(converted.throttle_time_ms, 7);
        assert_eq!(converted.error_message.as_deref(), Some("top-error"));
        assert_eq!(converted.acquisition_lock_timeout_ms, 30_000);
        assert_eq!(converted.responses[0].topic_id, topic_id);
        let partition = &converted.responses[0].partitions[0];
        assert_eq!(partition.partition_index, 3);
        assert_eq!(partition.error_message.as_deref(), Some("fetch-error"));
        assert_eq!(
            partition.acknowledge_error_message.as_deref(),
            Some("ack-error")
        );
        assert_eq!(
            partition.current_leader,
            ShareLeader {
                leader_id: 9,
                leader_epoch: 2,
            }
        );
        assert_eq!(partition.records, Some(Bytes::from_static(b"records")));
        assert_eq!(
            partition.acquired_records,
            vec![ShareAcquiredRecords {
                first_offset: 11,
                last_offset: 12,
                delivery_count: 2,
            }]
        );
        assert_eq!(
            converted.node_endpoints,
            vec![ShareNodeEndpoint {
                node_id: 9,
                host: "broker".to_owned(),
                port: 9092,
                rack: Some("rack".to_owned()),
            }]
        );
    }
}
