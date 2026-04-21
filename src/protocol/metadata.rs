use kafka_protocol::messages::{ApiKey, MetadataRequest, MetadataResponse, RequestHeader, TopicName};
use kafka_protocol::protocol::StrBytes;

use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

use super::{API_VERSION_METADATA, HeaderResponse};

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
) -> MetadataResponseData {
    MetadataResponseData {
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

// --------------------------------------------------------------------
// Data types (moved from old protocol/metadata.rs)
// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct MetadataResponseData {
    pub header: HeaderResponse,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Default, Debug)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Default, Debug)]
pub struct TopicMetadata {
    pub error: i16,
    pub topic: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Default, Debug)]
pub struct PartitionMetadata {
    pub error: i16,
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}
