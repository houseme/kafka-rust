use kafka_protocol::messages::{ApiKey, ProduceRequest, ProduceResponse, RequestHeader, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};

use super::{API_VERSION_PRODUCE, HeaderResponse, to_kp_compression};
use crate::compression::Compression;
use crate::error::{Error, KafkaCode, Result};
use crate::producer::{ProduceConfirm, ProducePartitionConfirm};

/// A message to produce: (topic, partition, key, value, headers).
pub type ProduceMessageRef<'a> = (
    &'a str,
    i32,
    Option<&'a [u8]>,
    Option<&'a [u8]>,
    &'a [(String, bytes::Bytes)],
);

#[tracing::instrument(skip(messages), fields(correlation_id = correlation_id))]
pub fn build_produce_request(
    correlation_id: i32,
    client_id: &str,
    required_acks: i16,
    timeout_ms: i32,
    compression: Compression,
    messages: &[ProduceMessageRef<'_>],
) -> Result<(RequestHeader, ProduceRequest)> {
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
        let records = topic_map
            .entry(topic)
            .or_default()
            .entry(*partition)
            .or_default();
        let offset = super::usize_to_i32(records.len())?;
        let kp_headers: indexmap::IndexMap<StrBytes, Option<bytes::Bytes>> = headers
            .iter()
            .map(|(k, v)| (StrBytes::from_string(k.clone()), Some(v.clone())))
            .collect();

        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i64::from(offset),
            // The encoder groups records with the same offset - sequence. This
            // keeps one batch per partition while retaining base_sequence = -1
            // for a non-idempotent producer.
            sequence: offset - 1,
            timestamp: 0,
            key: key.map(bytes::Bytes::copy_from_slice),
            value: value.map(bytes::Bytes::copy_from_slice),
            headers: kp_headers,
        };
        records.push(record);
    }

    let topic_data: Vec<kafka_protocol::messages::produce_request::TopicProduceData> =
        topic_map
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
                    RecordBatchEncoder::encode(&mut buf, &records, &options).map_err(|err| {
                        let message = err.to_string();
                        map_record_encode_error(&message)
                    })?;

                    Ok(kafka_protocol::messages::produce_request::PartitionProduceData::default()
                        .with_index(partition_idx)
                        .with_records(Some(buf.freeze())))
                })
                .collect::<Result<_>>()?;

                Ok(
                    kafka_protocol::messages::produce_request::TopicProduceData::default()
                        .with_name(TopicName::from(StrBytes::from_string(
                            topic_name.to_owned(),
                        )))
                        .with_partition_data(partition_data),
                )
            })
            .collect::<Result<_>>()?;

    let request = ProduceRequest::default()
        .with_transactional_id(None)
        .with_acks(required_acks)
        .with_timeout_ms(timeout_ms)
        .with_topic_data(topic_data);

    Ok((header, request))
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

fn map_record_encode_error(message: &str) -> Error {
    if is_disabled_compression_feature_error(message) {
        Error::unsupported_compression()
    } else {
        Error::codec()
    }
}

fn is_disabled_compression_feature_error(message: &str) -> bool {
    message.contains("Support for") && message.contains("not enabled as a cargo feature")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(feature = "gzip"))]
    use crate::error::ProtocolError;

    fn one_message() -> [ProduceMessageRef<'static>; 1] {
        [("topic-a", 0, None, Some(&b"value"[..]), &[])]
    }

    #[test]
    fn produce_batches_have_contiguous_offsets_and_non_idempotent_sequences() {
        let messages: [ProduceMessageRef<'_>; 5] = [
            ("topic-a", 0, None, Some(b"first"), &[]),
            ("topic-b", 0, None, Some(b"other-topic"), &[]),
            ("topic-a", 1, None, Some(b"other-partition"), &[]),
            ("topic-a", 0, None, Some(b"second"), &[]),
            ("topic-a", 0, None, Some(b"third"), &[]),
        ];
        let (_, request) =
            build_produce_request(1, "client-a", 1, 30_000, Compression::NONE, &messages).unwrap();

        assert_eq!(request.topic_data.len(), 2);
        for topic in request.topic_data {
            for partition in topic.partition_data {
                let expected_values: &[&[u8]] = match (topic.name.as_str(), partition.index) {
                    ("topic-a", 0) => &[b"first", b"second", b"third"],
                    ("topic-a", 1) => &[b"other-partition"],
                    ("topic-b", 0) => &[b"other-topic"],
                    other => panic!("unexpected topic/partition: {other:?}"),
                };
                let records = partition.records.unwrap();
                let batch_info = kafka_protocol::records::RecordBatchDecoder::decode_batch_info(
                    &mut records.clone(),
                )
                .unwrap();
                assert_eq!(batch_info.len(), 1, "one batch per partition");
                assert_eq!(batch_info[0].min_offset, 0);
                assert_eq!(batch_info[0].base_sequence, -1);
                assert_eq!(
                    usize::try_from(batch_info[0].record_count).unwrap(),
                    expected_values.len()
                );
                // The broker requires lastOffsetDelta + 1 == recordsCount.
                let last_offset_delta = i32::from_be_bytes(records[23..27].try_into().unwrap());
                assert_eq!(last_offset_delta + 1, batch_info[0].record_count);
                let decoded =
                    kafka_protocol::records::RecordBatchDecoder::decode_all(&mut records.clone())
                        .unwrap();
                assert_eq!(decoded.len(), 1);
                for (index, record) in decoded[0].records.iter().enumerate() {
                    assert_eq!(record.offset, i64::try_from(index).unwrap());
                    assert_eq!(record.value.as_deref().unwrap(), expected_values[index]);
                }
            }
        }
    }

    #[cfg(not(feature = "gzip"))]
    #[test]
    fn build_produce_request_returns_error_when_codec_feature_is_disabled() {
        let err =
            build_produce_request(1, "client-a", 1, 30_000, Compression::GZIP, &one_message())
                .expect_err("disabled gzip support should return an error");

        assert!(matches!(
            err,
            Error::Protocol(ProtocolError::UnsupportedCompression)
        ));
    }

    #[cfg(feature = "compression")]
    #[test]
    fn build_produce_request_supports_enabled_compression_codecs() {
        for compression in [
            Compression::GZIP,
            Compression::SNAPPY,
            Compression::LZ4,
            Compression::ZSTD,
        ] {
            build_produce_request(1, "client-a", 1, 30_000, compression, &one_message())
                .unwrap_or_else(|err| panic!("{compression:?} should encode successfully: {err}"));
        }
    }
}
