use kafka_protocol::messages::{ApiKey, MetadataRequest, MetadataResponse, RequestHeader, TopicName};
use kafka_protocol::protocol::StrBytes;

// Re-exports of sub-types from kafka_protocol for convenience
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

use super::API_VERSION_METADATA;
use crate::protocol::{
    BrokerMetadata, HeaderResponse, MetadataResponse as OurMetadataResponse, PartitionMetadata,
    TopicMetadata,
};

pub fn build_metadata_request(
    correlation_id: i32,
    client_id: &str,
    topics: Option<&[&str]>,
) -> (RequestHeader, MetadataRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::Metadata as i16)
        .with_request_api_version(API_VERSION_METADATA)
        .with_correlation_id(correlation_id);

    let request = MetadataRequest::default().with_topics(topics.map(|ts| {
        ts.iter()
            .map(|t| {
                MetadataRequestTopic::default()
                    .with_name(Some(TopicName::from(StrBytes::from_string(
                        t.to_string(),
                    ))))
            })
            .collect()
    }));

    (header, request)
}

pub fn convert_metadata_response(
    kp_resp: MetadataResponse,
    correlation_id: i32,
) -> OurMetadataResponse {
    OurMetadataResponse {
        header: HeaderResponse { correlation: correlation_id },
        brokers: kp_resp
            .brokers
            .into_iter()
            .map(|b| BrokerMetadata {
                node_id: i32::from(b.node_id),
                host: b.host.to_string(),
                port: b.port,
            })
            .collect(),
        topics: kp_resp
            .topics
            .into_iter()
            .map(|t| TopicMetadata {
                error: t.error_code,
                topic: t
                    .name
                    .map(|n| n.to_string())
                    .unwrap_or_default(),
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| PartitionMetadata {
                        error: p.error_code,
                        id: p.partition_index,
                        leader: i32::from(p.leader_id),
                        replicas: p.replica_nodes.into_iter().map(|n| i32::from(n)).collect(),
                        isr: p.isr_nodes.into_iter().map(|n| i32::from(n)).collect(),
                    })
                    .collect(),
            })
            .collect(),
    }
}
