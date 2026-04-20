use bytes::Bytes;
use kafka_protocol::messages::{ApiKey, BrokerId, FetchRequest, FetchResponse, RequestHeader, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::RecordBatchDecoder;

use super::API_VERSION_FETCH;
use crate::error::{Error, KafkaCode, Result};
use std::sync::Arc;

// Re-exports of sub-types from kafka_protocol for convenience
use kafka_protocol::messages::fetch_request::FetchPartition as KpFetchPartition;
use kafka_protocol::messages::fetch_request::FetchTopic as KpFetchTopic;
use kafka_protocol::messages::fetch_response::PartitionData as KpPartitionData;

// ---------------------------------------------------------------------------
// Owned fetch response types (no lifetimes) for the protocol2 adapter.
// These mirror the structure of the legacy protocol::fetch types but own
// all their data (String instead of &str, Bytes/Vec<u8> instead of &[u8]).

/// Owned version of `protocol::fetch::Message` with no lifetimes.
#[derive(Debug, Clone)]
pub struct OwnedMessage {
    pub offset: i64,
    pub key: Bytes,
    pub value: Bytes,
}

/// Owned version of `protocol::fetch::Data` with no lifetimes.
#[derive(Debug)]
pub struct OwnedData {
    pub highwatermark_offset: i64,
    pub messages: Vec<OwnedMessage>,
}

/// Owned version of `protocol::fetch::Partition` with no lifetimes.
#[derive(Debug)]
pub struct OwnedPartition {
    pub partition: i32,
    pub data: std::result::Result<OwnedData, Arc<Error>>,
}

/// Owned version of `protocol::fetch::Topic` with no lifetimes.
#[derive(Debug)]
pub struct OwnedTopic {
    pub topic: String,
    pub partitions: Vec<OwnedPartition>,
}

/// Owned version of `protocol::fetch::Response` with no lifetimes.
#[derive(Debug)]
pub struct OwnedFetchResponse {
    pub correlation_id: i32,
    pub topics: Vec<OwnedTopic>,
}

impl OwnedPartition {
    pub fn data(&self) -> std::result::Result<&OwnedData, &Arc<Error>> {
        self.data.as_ref()
    }
}

// ---------------------------------------------------------------------------
// Build functions

pub fn build_fetch_request(
    correlation_id: i32,
    client_id: &str,
    replica_id: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    partitions: &[(&str, i32, i64, i32)],
) -> (RequestHeader, FetchRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::Fetch as i16)
        .with_request_api_version(API_VERSION_FETCH)
        .with_correlation_id(correlation_id);

    let mut topic_map: std::collections::HashMap<&str, Vec<KpFetchPartition>> =
        std::collections::HashMap::new();

    for (topic, partition, offset, partition_max_bytes) in partitions {
        topic_map.entry(topic).or_default().push(
            KpFetchPartition::default()
                .with_partition(*partition)
                .with_fetch_offset(*offset)
                .with_partition_max_bytes(*partition_max_bytes),
        );
    }

    let topics: Vec<KpFetchTopic> = topic_map
        .into_iter()
        .map(|(topic_name, fetch_partitions)| {
            KpFetchTopic::default()
                .with_topic(TopicName::from(StrBytes::from_string(topic_name.to_string())))
                .with_partitions(fetch_partitions)
        })
        .collect();

    let request = FetchRequest::default()
        .with_replica_id(BrokerId::from(replica_id))
        .with_max_wait_ms(max_wait_ms)
        .with_min_bytes(min_bytes)
        .with_max_bytes(max_bytes)
        .with_isolation_level(0)
        .with_topics(topics);

    (header, request)
}

pub fn convert_fetch_response(
    kp_resp: FetchResponse,
    correlation_id: i32,
) -> Result<OwnedFetchResponse> {
    let topics = kp_resp
        .responses
        .into_iter()
        .map(|t| {
            let topic_name = t.topic.to_string();
            let partitions: Vec<OwnedPartition> = t
                .partitions
                .into_iter()
                .map(|p: KpPartitionData| {
                    let data = if p.error_code != 0 {
                        Err(Arc::new(Error::TopicPartitionError {
                            topic_name: topic_name.clone(),
                            partition_id: p.partition_index,
                            error_code: KafkaCode::from_protocol(p.error_code)
                                .unwrap_or(KafkaCode::Unknown),
                        }))
                    } else {
                        decode_partition_records(p.records, p.high_watermark)
                    };
                    OwnedPartition {
                        partition: p.partition_index,
                        data,
                    }
                })
                .collect();
            OwnedTopic {
                topic: topic_name,
                partitions,
            }
        })
        .collect();

    Ok(OwnedFetchResponse {
        correlation_id,
        topics,
    })
}

fn decode_partition_records(
    records: Option<Bytes>,
    high_watermark: i64,
) -> std::result::Result<OwnedData, Arc<Error>> {
    let Some(mut records_bytes) = records else {
        return Ok(OwnedData {
            highwatermark_offset: high_watermark,
            messages: vec![],
        });
    };

    let record_set = match RecordBatchDecoder::decode(&mut records_bytes) {
        Ok(rs) => rs,
        Err(_) => {
            return Err(Arc::new(Error::CodecError));
        }
    };

    let mut messages = Vec::new();
    for record in &record_set.records {
        messages.push(OwnedMessage {
            offset: record.offset,
            key: record.key.as_ref().cloned().unwrap_or_default(),
            value: record.value.as_ref().cloned().unwrap_or_default(),
        });
    }

    Ok(OwnedData {
        highwatermark_offset: high_watermark,
        messages,
    })
}
