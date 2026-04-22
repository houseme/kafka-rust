use kafka_protocol::messages::{
    ApiKey, FindCoordinatorRequest, FindCoordinatorResponse, GroupId, OffsetCommitRequest,
    OffsetFetchRequest, RequestHeader, TopicName,
};
use kafka_protocol::protocol::StrBytes;

use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;

use super::{
    API_VERSION_FIND_COORDINATOR, API_VERSION_OFFSET_COMMIT, API_VERSION_OFFSET_FETCH,
    HeaderResponse,
};

// -- FindCoordinator --

pub fn build_find_coordinator_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
) -> (RequestHeader, FindCoordinatorRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::FindCoordinator as i16)
        .with_request_api_version(API_VERSION_FIND_COORDINATOR)
        .with_correlation_id(correlation_id);

    let request = FindCoordinatorRequest::default()
        .with_key(StrBytes::from_string(group_id.to_string()))
        .with_key_type(0);

    (header, request)
}

pub fn convert_find_coordinator_response(
    kp_resp: &FindCoordinatorResponse,
    correlation_id: i32,
) -> GroupCoordinatorResponse {
    let (error, node_id, host, port) = if let Some(c) = kp_resp.coordinators.first() {
        (
            c.error_code,
            i32::from(c.node_id),
            c.host.to_string(),
            c.port,
        )
    } else {
        (
            kp_resp.error_code,
            i32::from(kp_resp.node_id),
            kp_resp.host.to_string(),
            kp_resp.port,
        )
    };

    GroupCoordinatorResponse {
        header: HeaderResponse {
            correlation: correlation_id,
        },
        error,
        broker_id: node_id,
        port,
        host,
    }
}

// -- OffsetCommit --

pub fn build_offset_commit_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    retention_time_ms: i64,
    offsets: &[(&str, i32, i64, Option<&str>)],
) -> (RequestHeader, OffsetCommitRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::OffsetCommit as i16)
        .with_request_api_version(API_VERSION_OFFSET_COMMIT)
        .with_correlation_id(correlation_id);

    let mut topic_map: std::collections::HashMap<&str, Vec<OffsetCommitRequestPartition>> =
        std::collections::HashMap::new();

    for (topic, partition, offset, metadata) in offsets {
        topic_map.entry(topic).or_default().push(
            OffsetCommitRequestPartition::default()
                .with_partition_index(*partition)
                .with_committed_offset(*offset)
                .with_committed_metadata(metadata.map(|m| StrBytes::from_string(m.to_string()))),
        );
    }

    let topics: Vec<OffsetCommitRequestTopic> = topic_map
        .into_iter()
        .map(|(name, partitions)| {
            OffsetCommitRequestTopic::default()
                .with_name(TopicName::from(StrBytes::from_string(name.to_string())))
                .with_partitions(partitions)
        })
        .collect();

    let request = OffsetCommitRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_string(group_id.to_string())))
        .with_generation_id_or_member_epoch(generation_id)
        .with_member_id(StrBytes::from_string(member_id.to_string()))
        .with_retention_time_ms(retention_time_ms)
        .with_topics(topics);

    (header, request)
}

pub fn convert_offset_commit_response(
    kp_resp: kafka_protocol::messages::OffsetCommitResponse,
    correlation_id: i32,
) -> OffsetCommitResponse {
    OffsetCommitResponse {
        header: HeaderResponse {
            correlation: correlation_id,
        },
        topic_partitions: kp_resp
            .topics
            .into_iter()
            .map(|t| TopicPartitionOffsetCommitResponse {
                topic: t.name.to_string(),
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| PartitionOffsetCommitResponse {
                        partition: p.partition_index,
                        error: p.error_code,
                    })
                    .collect(),
            })
            .collect(),
    }
}

// -- OffsetFetch --

pub fn build_offset_fetch_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    partitions: &[(&str, i32)],
) -> (RequestHeader, OffsetFetchRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::OffsetFetch as i16)
        .with_request_api_version(API_VERSION_OFFSET_FETCH)
        .with_correlation_id(correlation_id);

    let mut topic_map: std::collections::HashMap<&str, Vec<i32>> = std::collections::HashMap::new();

    for (topic, partition) in partitions {
        topic_map.entry(topic).or_default().push(*partition);
    }

    let topics: Vec<OffsetFetchRequestTopic> = topic_map
        .into_iter()
        .map(|(name, partition_indexes)| {
            OffsetFetchRequestTopic::default()
                .with_name(TopicName::from(StrBytes::from_string(name.to_string())))
                .with_partition_indexes(partition_indexes)
        })
        .collect();

    let request = OffsetFetchRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_string(group_id.to_string())))
        .with_topics(Some(topics));

    (header, request)
}

pub fn convert_offset_fetch_response(
    kp_resp: kafka_protocol::messages::OffsetFetchResponse,
    correlation_id: i32,
) -> OffsetFetchResponse {
    OffsetFetchResponse {
        header: HeaderResponse {
            correlation: correlation_id,
        },
        topic_partitions: kp_resp
            .topics
            .into_iter()
            .map(|t| TopicPartitionOffsetFetchResponse {
                topic: t.name.to_string(),
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| PartitionOffsetFetchResponse {
                        partition: p.partition_index,
                        offset: p.committed_offset,
                        metadata: p.metadata.map(|m| m.to_string()).unwrap_or_default(),
                        error: p.error_code,
                    })
                    .collect(),
            })
            .collect(),
    }
}

// --------------------------------------------------------------------
// Data types (moved from old protocol/consumer.rs)
// --------------------------------------------------------------------

use crate::error::{Error, KafkaCode, Result};
use crate::utils::PartitionOffset;

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct GroupCoordinatorResponse {
    pub header: HeaderResponse,
    pub error: i16,
    pub broker_id: i32,
    pub port: i32,
    pub host: String,
}

impl GroupCoordinatorResponse {
    pub fn into_result(self) -> Result<Self> {
        match Error::from_protocol(self.error) {
            Some(e) => Err(e),
            None => Ok(self),
        }
    }
}

#[derive(Default, Debug)]
#[allow(dead_code)]
pub struct OffsetFetchResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetFetchResponse>,
}

#[derive(Default, Debug)]
#[allow(dead_code)]
pub struct TopicPartitionOffsetFetchResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetFetchResponse>,
}

#[derive(Default, Debug)]
#[allow(dead_code)]
pub struct PartitionOffsetFetchResponse {
    pub partition: i32,
    pub offset: i64,
    pub metadata: String,
    pub error: i16,
}

#[allow(dead_code)]
impl PartitionOffsetFetchResponse {
    pub fn get_offsets(&self) -> Result<PartitionOffset> {
        match Error::from_protocol(self.error) {
            Some(Error::Kafka(KafkaCode::UnknownTopicOrPartition)) => Ok(PartitionOffset {
                partition: self.partition,
                offset: -1,
            }),
            Some(e) => Err(e),
            None => Ok(PartitionOffset {
                partition: self.partition,
                offset: self.offset,
            }),
        }
    }
}

#[derive(Default, Debug)]
#[allow(dead_code)]
pub struct OffsetCommitResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetCommitResponse>,
}

#[derive(Default, Debug)]
#[allow(dead_code)]
pub struct TopicPartitionOffsetCommitResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetCommitResponse>,
}

#[derive(Default, Debug)]
#[allow(dead_code)]
pub struct PartitionOffsetCommitResponse {
    pub partition: i32,
    pub error: i16,
}

#[allow(dead_code)]
impl PartitionOffsetCommitResponse {
    pub fn to_error(&self) -> Option<KafkaCode> {
        KafkaCode::from_protocol(self.error)
    }
}
