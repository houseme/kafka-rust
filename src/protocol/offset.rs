use kafka_protocol::messages::{ApiKey, BrokerId, ListOffsetsRequest, ListOffsetsResponse, RequestHeader, TopicName};
use kafka_protocol::protocol::StrBytes;

use kafka_protocol::messages::list_offsets_request::ListOffsetsTopic;
use kafka_protocol::messages::list_offsets_request::ListOffsetsPartition;

use super::{API_VERSION_LIST_OFFSETS, HeaderResponse};
use crate::error::KafkaCode;
use crate::utils::PartitionOffset;

pub fn build_list_offsets_request(
    correlation_id: i32,
    client_id: &str,
    partitions: &[(&str, i32, i64)],
) -> (RequestHeader, ListOffsetsRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::ListOffsets as i16)
        .with_request_api_version(API_VERSION_LIST_OFFSETS)
        .with_correlation_id(correlation_id);

    let mut topic_map: std::collections::HashMap<&str, Vec<ListOffsetsPartition>> =
        std::collections::HashMap::new();

    for (topic, partition, timestamp) in partitions {
        topic_map.entry(topic).or_default().push(
            ListOffsetsPartition::default()
                .with_partition_index(*partition)
                .with_timestamp(*timestamp),
        );
    }

    let topics: Vec<ListOffsetsTopic> = topic_map
        .into_iter()
        .map(|(name, partitions)| {
            ListOffsetsTopic::default()
                .with_name(TopicName::from(StrBytes::from_string(name.to_string())))
                .with_partitions(partitions)
        })
        .collect();

    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_isolation_level(0)
        .with_topics(topics);

    (header, request)
}

pub fn convert_list_offsets_response(
    kp_resp: ListOffsetsResponse,
    correlation_id: i32,
) -> OffsetResponseData {
    OffsetResponseData {
        header: HeaderResponse { correlation: correlation_id },
        topic_partitions: kp_resp
            .topics
            .into_iter()
            .map(|t| TopicPartitionOffsetResponse {
                topic: t.name.to_string(),
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| PartitionOffsetResponse {
                        partition: p.partition_index,
                        error: p.error_code,
                        offset: vec![p.offset],
                    })
                    .collect(),
            })
            .collect(),
    }
}

// --------------------------------------------------------------------
// Data types (moved from old protocol/offset.rs)
// --------------------------------------------------------------------

#[derive(Default, Debug)]
#[allow(dead_code)]
pub struct OffsetResponseData {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetResponse>,
}

#[derive(Default, Debug)]
pub struct TopicPartitionOffsetResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetResponse>,
}

#[derive(Default, Debug)]
pub struct PartitionOffsetResponse {
    pub partition: i32,
    pub error: i16,
    pub offset: Vec<i64>,
}

impl PartitionOffsetResponse {
    pub fn to_offset(&self) -> std::result::Result<PartitionOffset, KafkaCode> {
        if let Some(code) = KafkaCode::from_protocol(self.error) {
            Err(code)
        } else {
            let offset = self.offset.first().copied().unwrap_or(-1);
            Ok(PartitionOffset {
                partition: self.partition,
                offset,
            })
        }
    }
}
