use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FetchResponse, RequestHeader, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::RecordBatchDecoder;

use super::API_VERSION_FETCH;
use crate::error::{Error, KafkaCode};
use std::sync::Arc;

// Re-exports of sub-types from kafka_protocol for convenience
use kafka_protocol::messages::fetch_request::FetchPartition as KpFetchPartition;
use kafka_protocol::messages::fetch_request::FetchTopic as KpFetchTopic;
use kafka_protocol::messages::fetch_response::PartitionData as KpPartitionData;

// ---------------------------------------------------------------------------
// Owned fetch response types (no lifetimes) for the protocol adapter.
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

#[tracing::instrument(skip(partitions), fields(correlation_id = correlation_id))]
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
                .with_topic(TopicName::from(StrBytes::from_string(
                    topic_name.to_string(),
                )))
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

pub fn convert_fetch_response(kp_resp: FetchResponse, correlation_id: i32) -> OwnedFetchResponse {
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

    OwnedFetchResponse {
        correlation_id,
        topics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_message_construction_and_field_access() {
        let msg = OwnedMessage {
            offset: 42,
            key: Bytes::from_static(b"my-key"),
            value: Bytes::from_static(b"my-value"),
        };
        assert_eq!(msg.offset, 42);
        assert_eq!(&*msg.key, b"my-key");
        assert_eq!(&*msg.value, b"my-value");
    }

    #[test]
    fn owned_message_with_empty_key_value() {
        let msg = OwnedMessage {
            offset: 0,
            key: Bytes::new(),
            value: Bytes::new(),
        };
        assert_eq!(msg.offset, 0);
        assert!(msg.key.is_empty());
        assert!(msg.value.is_empty());
    }

    #[test]
    fn owned_data_construction() {
        let data = OwnedData {
            highwatermark_offset: 100,
            messages: vec![
                OwnedMessage {
                    offset: 0,
                    key: Bytes::new(),
                    value: Bytes::from_static(b"a"),
                },
                OwnedMessage {
                    offset: 1,
                    key: Bytes::new(),
                    value: Bytes::from_static(b"b"),
                },
            ],
        };
        assert_eq!(data.highwatermark_offset, 100);
        assert_eq!(data.messages.len(), 2);
        assert_eq!(data.messages[0].offset, 0);
        assert_eq!(data.messages[1].offset, 1);
    }

    #[test]
    fn owned_partition_ok_with_empty_messages() {
        let partition = OwnedPartition {
            partition: 0,
            data: Ok(OwnedData {
                highwatermark_offset: 50,
                messages: vec![],
            }),
        };
        assert_eq!(partition.partition, 0);
        let data = partition.data().unwrap();
        assert_eq!(data.highwatermark_offset, 50);
        assert!(data.messages.is_empty());
    }

    #[test]
    fn owned_partition_ok_with_messages() {
        let partition = OwnedPartition {
            partition: 3,
            data: Ok(OwnedData {
                highwatermark_offset: 200,
                messages: vec![OwnedMessage {
                    offset: 10,
                    key: Bytes::new(),
                    value: Bytes::from_static(b"hello"),
                }],
            }),
        };
        assert_eq!(partition.partition, 3);
        let data = partition.data().unwrap();
        assert_eq!(data.messages.len(), 1);
        assert_eq!(&*data.messages[0].value, b"hello");
    }

    #[test]
    fn owned_partition_err() {
        let err = Arc::new(Error::codec());
        let partition = OwnedPartition {
            partition: 1,
            data: Err(err.clone()),
        };
        assert!(partition.data().is_err());
    }

    #[test]
    fn owned_topic_construction() {
        let topic = OwnedTopic {
            topic: "test-topic".to_string(),
            partitions: vec![
                OwnedPartition {
                    partition: 0,
                    data: Ok(OwnedData {
                        highwatermark_offset: 10,
                        messages: vec![],
                    }),
                },
                OwnedPartition {
                    partition: 1,
                    data: Ok(OwnedData {
                        highwatermark_offset: 20,
                        messages: vec![],
                    }),
                },
            ],
        };
        assert_eq!(topic.topic, "test-topic");
        assert_eq!(topic.partitions.len(), 2);
        assert_eq!(topic.partitions[0].partition, 0);
        assert_eq!(topic.partitions[1].partition, 1);
    }

    #[test]
    fn owned_fetch_response_construction() {
        let resp = OwnedFetchResponse {
            correlation_id: 42,
            topics: vec![OwnedTopic {
                topic: "orders".to_string(),
                partitions: vec![OwnedPartition {
                    partition: 0,
                    data: Ok(OwnedData {
                        highwatermark_offset: 5,
                        messages: vec![],
                    }),
                }],
            }],
        };
        assert_eq!(resp.correlation_id, 42);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].topic, "orders");
    }
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

    let raw_records = records_bytes.clone();
    let record_set = match RecordBatchDecoder::decode(&mut records_bytes) {
        Ok(rs) => rs,
        Err(_) => {
            let messages = decode_records_safe(raw_records.as_ref()).map_err(Arc::new)?;
            return Ok(OwnedData {
                highwatermark_offset: high_watermark,
                messages,
            });
        }
    };

    let mut messages = Vec::new();
    for record in &record_set.records {
        messages.push(OwnedMessage {
            offset: record.offset,
            key: record.key.clone().unwrap_or_default(),
            value: record.value.clone().unwrap_or_default(),
        });
    }

    Ok(OwnedData {
        highwatermark_offset: high_watermark,
        messages,
    })
}

/// Safe entry point for fuzzing fetch response data — catches all panics.
pub(crate) fn decode_records_safe(
    records: &[u8],
) -> Result<Vec<OwnedMessage>, crate::error::Error> {
    const MAX_INPUT_SIZE: usize = 1_048_576;
    if records.len() > MAX_INPUT_SIZE {
        return Err(crate::error::Error::codec());
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut buf = Bytes::from(records.to_vec());
        match RecordBatchDecoder::decode(&mut buf) {
            Ok(record_set) => {
                let messages: Vec<OwnedMessage> = record_set
                    .records
                    .iter()
                    .map(|r| OwnedMessage {
                        offset: r.offset,
                        key: r.key.clone().unwrap_or_default(),
                        value: r.value.clone().unwrap_or_default(),
                    })
                    .collect();
                Ok(messages)
            }
            Err(_) => Err(crate::error::Error::codec()),
        }
    }));
    match result {
        Ok(r) => r,
        Err(_) => Err(crate::error::Error::codec()),
    }
}
