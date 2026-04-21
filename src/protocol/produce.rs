use kafka_protocol::messages::{ApiKey, ProduceRequest, ProduceResponse, RequestHeader, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};

use super::{API_VERSION_PRODUCE, HeaderResponse, to_kp_compression};
use crate::compression::Compression;
use crate::error::KafkaCode;
use crate::producer::{ProduceConfirm, ProducePartitionConfirm};

/// A message to produce: (topic, partition, key, value, headers).
pub type ProduceMessageRef<'a> = (
    &'a str,
    i32,
    Option<&'a [u8]>,
    Option<&'a [u8]>,
    &'a [(String, Vec<u8>)],
);

#[tracing::instrument(skip(messages), fields(correlation_id = correlation_id))]
pub fn build_produce_request(
    correlation_id: i32,
    client_id: &str,
    required_acks: i16,
    timeout_ms: i32,
    compression: Compression,
    messages: &[ProduceMessageRef<'_>],
) -> (RequestHeader, ProduceRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::Produce as i16)
        .with_request_api_version(API_VERSION_PRODUCE)
        .with_correlation_id(correlation_id);

    let mut topic_map: std::collections::HashMap<
        &str,
        std::collections::HashMap<i32, Vec<Record>>,
    > = std::collections::HashMap::new();

    for (topic, partition, key, value, headers) in messages {
        let kp_headers: indexmap::IndexMap<
            kafka_protocol::protocol::StrBytes,
            Option<bytes::Bytes>,
        > = headers
            .iter()
            .map(|(k, v)| {
                (
                    kafka_protocol::protocol::StrBytes::from(k.clone()),
                    Some(bytes::Bytes::copy_from_slice(v)),
                )
            })
            .collect();

        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: 0,
            producer_epoch: 0,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: 0,
            timestamp: 0,
            key: key.map(bytes::Bytes::copy_from_slice),
            value: value.map(bytes::Bytes::copy_from_slice),
            headers: kp_headers,
        };
        topic_map
            .entry(topic)
            .or_default()
            .entry(*partition)
            .or_default()
            .push(record);
    }

    let topic_data: Vec<kafka_protocol::messages::produce_request::TopicProduceData> = topic_map
        .into_iter()
        .map(|(topic_name, partitions)| {
            let partition_data: Vec<
                kafka_protocol::messages::produce_request::PartitionProduceData,
            > = partitions
                .into_iter()
                .map(|(partition_idx, records)| {
                    let mut buf = bytes::BytesMut::new();
                    let options = RecordEncodeOptions {
                        version: 2,
                        compression: to_kp_compression(compression),
                    };
                    RecordBatchEncoder::encode(&mut buf, &records, &options)
                        .expect("failed to encode record batch");

                    kafka_protocol::messages::produce_request::PartitionProduceData::default()
                        .with_index(partition_idx)
                        .with_records(Some(buf.freeze()))
                })
                .collect();

            kafka_protocol::messages::produce_request::TopicProduceData::default()
                .with_name(TopicName::from(StrBytes::from_string(
                    topic_name.to_string(),
                )))
                .with_partition_data(partition_data)
        })
        .collect();

    let request = ProduceRequest::default()
        .with_transactional_id(None)
        .with_acks(required_acks)
        .with_timeout_ms(timeout_ms)
        .with_topic_data(topic_data);

    (header, request)
}

pub fn convert_produce_response(
    kp_resp: ProduceResponse,
    correlation_id: i32,
) -> ProduceResponseData {
    ProduceResponseData {
        header: HeaderResponse {
            correlation: correlation_id,
        },
        topic_partitions: kp_resp
            .responses
            .into_iter()
            .map(|t| TopicPartitionProduceResponse {
                topic: t.name.to_string(),
                partitions: t
                    .partition_responses
                    .into_iter()
                    .map(|p| PartitionProduceResponse {
                        partition: p.index,
                        error: p.error_code,
                        offset: p.base_offset,
                    })
                    .collect(),
            })
            .collect(),
    }
}

// --------------------------------------------------------------------
// Data types (moved from old protocol/produce.rs)
// --------------------------------------------------------------------

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum ProducerTimestamp {
    CreateTime = 0,
    LogAppendTime = 8,
}

#[derive(Default, Debug, Clone)]
#[allow(dead_code)]
pub struct ProduceResponseData {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionProduceResponse>,
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionProduceResponse {
    pub topic: String,
    pub partitions: Vec<PartitionProduceResponse>,
}

#[derive(Default, Debug, Clone)]
pub struct PartitionProduceResponse {
    pub partition: i32,
    pub error: i16,
    pub offset: i64,
}

impl ProduceResponseData {
    pub fn get_response(self) -> Vec<ProduceConfirm> {
        self.topic_partitions
            .into_iter()
            .map(TopicPartitionProduceResponse::get_response)
            .collect()
    }
}

impl TopicPartitionProduceResponse {
    pub fn get_response(self) -> ProduceConfirm {
        let Self { topic, partitions } = self;
        let partition_confirms = partitions
            .iter()
            .map(PartitionProduceResponse::get_response)
            .collect();
        ProduceConfirm {
            topic,
            partition_confirms,
        }
    }
}

impl PartitionProduceResponse {
    pub fn get_response(&self) -> ProducePartitionConfirm {
        ProducePartitionConfirm {
            partition: self.partition,
            offset: match KafkaCode::from_protocol(self.error) {
                None => Ok(self.offset),
                Some(code) => Err(code),
            },
        }
    }
}
