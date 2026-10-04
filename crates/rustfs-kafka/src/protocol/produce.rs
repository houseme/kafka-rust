use kafka_protocol::messages::{ApiKey, ProduceRequest, ProduceResponse, RequestHeader, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};

use super::{API_VERSION_PRODUCE, to_kp_compression};
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

/// Identity and sequence for one transactional record. Kept private because
/// the high-level producer owns sequence and transaction lifecycle state.
#[derive(Clone, Copy)]
pub(crate) struct TransactionContext<'a> {
    pub(crate) transactional_id: &'a str,
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    pub(crate) sequence: i32,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct ProduceRequestOptions<'a> {
    pub(crate) timestamp: i64,
    pub(crate) transaction: Option<TransactionContext<'a>>,
}

#[tracing::instrument(skip(messages), fields(correlation_id = correlation_id))]
pub fn build_produce_request(
    correlation_id: i32,
    client_id: &str,
    required_acks: i16,
    timeout_ms: i32,
    compression: Compression,
    messages: &[ProduceMessageRef<'_>],
) -> Result<(RequestHeader, ProduceRequest)> {
    build_produce_request_with_options(
        correlation_id,
        client_id,
        required_acks,
        timeout_ms,
        compression,
        messages,
        ProduceRequestOptions::default(),
    )
}

pub(crate) fn build_transactional_produce_request(
    correlation_id: i32,
    client_id: &str,
    timeout_ms: i32,
    compression: Compression,
    message: ProduceMessageRef<'_>,
    context: TransactionContext<'_>,
    timestamp: i64,
) -> Result<(RequestHeader, ProduceRequest)> {
    if context.producer_id < 0 || context.producer_epoch < 0 || context.sequence < 0 {
        return Err(Error::Config(
            "invalid transactional producer identity or sequence".into(),
        ));
    }
    build_produce_request_with_options(
        correlation_id,
        client_id,
        -1,
        timeout_ms,
        compression,
        &[message],
        ProduceRequestOptions {
            timestamp,
            transaction: Some(context),
        },
    )
}

pub(crate) fn build_produce_request_with_options(
    correlation_id: i32,
    client_id: &str,
    required_acks: i16,
    timeout_ms: i32,
    compression: Compression,
    messages: &[ProduceMessageRef<'_>],
    options: ProduceRequestOptions<'_>,
) -> Result<(RequestHeader, ProduceRequest)> {
    let context = options.transaction;
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::Produce as i16)
        .with_request_api_version(API_VERSION_PRODUCE)
        .with_correlation_id(correlation_id);

    let mut topic_map: std::collections::HashMap<
        &str,
        std::collections::HashMap<i32, Vec<Record>>,
    > = std::collections::HashMap::new();

    for run in messages.chunk_by(|first, second| first.0 == second.0 && first.1 == second.1) {
        let (topic, partition, ..) = run[0];
        let records = topic_map
            .entry(topic)
            .or_default()
            .entry(partition)
            .or_default();
        for (_, _, key, value, headers) in run {
            let offset = super::usize_to_i32(records.len())?;
            let kp_headers: indexmap::IndexMap<StrBytes, Option<bytes::Bytes>> = headers
                .iter()
                .map(|(k, v)| (StrBytes::from_string(k.clone()), Some(v.clone())))
                .collect();

            let record = Record {
                transactional: context.is_some(),
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: context.map_or(-1, |context| context.producer_id),
                producer_epoch: context.map_or(-1, |context| context.producer_epoch),
                timestamp_type: TimestampType::Creation,
                offset: i64::from(offset),
                // The encoder groups records with the same offset - sequence. This
                // keeps one batch per partition while retaining base_sequence = -1
                // for a non-idempotent producer.
                sequence: context.map_or(offset - 1, |context| context.sequence),
                timestamp: options.timestamp,
                key: key.map(bytes::Bytes::copy_from_slice),
                value: value.map(bytes::Bytes::copy_from_slice),
                headers: kp_headers,
            };
            records.push(record);
        }
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
        .with_transactional_id(context.map(|context| {
            kafka_protocol::messages::TransactionalId(StrBytes::from_string(
                context.transactional_id.to_owned(),
            ))
        }))
        .with_acks(required_acks)
        .with_timeout_ms(timeout_ms)
        .with_topic_data(topic_data);

    Ok((header, request))
}

pub(crate) fn into_produce_confirmations(
    response: ProduceResponse,
) -> impl Iterator<Item = ProduceConfirm> {
    response.responses.into_iter().map(|topic| ProduceConfirm {
        topic: topic.name.to_string(),
        partition_confirms: topic
            .partition_responses
            .into_iter()
            .map(|partition| {
                partition_confirmation(partition.index, partition.error_code, partition.base_offset)
            })
            .collect(),
    })
}

// --------------------------------------------------------------------
// Data types (moved from old protocol/produce.rs)
// --------------------------------------------------------------------

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum ProducerTimestamp {
    /// Sample the current Unix time in milliseconds once per produce call.
    CreateTime = 0,
    /// Broker/topic policy, rejected as a client-side produce mode.
    /// Configure `message.timestamp.type=LogAppendTime` on the topic instead.
    LogAppendTime = 8,
}

fn partition_confirmation(partition: i32, error: i16, offset: i64) -> ProducePartitionConfirm {
    ProducePartitionConfirm {
        partition,
        offset: match KafkaCode::from_protocol(error) {
            None if offset >= 0 => Ok(offset),
            code => Err(code.unwrap_or(KafkaCode::Unknown)),
        },
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
                    assert_eq!(record.timestamp, 0);
                    assert_eq!(record.timestamp_type, TimestampType::Creation);
                }
            }
        }
    }

    #[test]
    fn sampled_timestamp_is_encoded_for_every_partition_and_codec() {
        let timestamp = 1_700_000_000_123;
        let messages: [ProduceMessageRef<'_>; 3] = [
            ("topic-a", 0, None, Some(b"first"), &[]),
            ("topic-a", 1, None, Some(b"other-partition"), &[]),
            ("topic-a", 0, None, Some(b"second"), &[]),
        ];
        for compression in enabled_codecs() {
            let (_, request) = build_produce_request_with_options(
                1,
                "client-a",
                1,
                30_000,
                compression,
                &messages,
                ProduceRequestOptions {
                    timestamp,
                    ..Default::default()
                },
            )
            .unwrap();
            for topic in request.topic_data {
                for partition in topic.partition_data {
                    let batches = kafka_protocol::records::RecordBatchDecoder::decode_all(
                        &mut partition.records.unwrap(),
                    )
                    .unwrap();
                    assert_eq!(batches.len(), 1);
                    for record in &batches[0].records {
                        assert_eq!(record.timestamp, timestamp);
                        assert_eq!(record.timestamp_type, TimestampType::Creation);
                    }
                }
            }
        }
    }

    #[test]
    fn sampled_timestamp_preserves_transactional_identity_and_headers() {
        let headers = [(
            "header".to_owned(),
            bytes::Bytes::from_static(b"header-value"),
        )];
        let timestamp = 1_700_000_000_123;
        for compression in enabled_codecs() {
            let (_, request) = build_transactional_produce_request(
                1,
                "client-a",
                30_000,
                compression,
                ("topic-a", 0, Some(b"key"), Some(b"value"), &headers),
                TransactionContext {
                    transactional_id: "transaction-id",
                    producer_id: 42,
                    producer_epoch: 3,
                    sequence: 7,
                },
                timestamp,
            )
            .unwrap();
            assert_eq!(
                request.transactional_id.as_ref().unwrap().as_str(),
                "transaction-id"
            );
            assert_eq!(request.acks, -1);
            let mut bytes = request.topic_data[0].partition_data[0]
                .records
                .clone()
                .unwrap();
            let batches =
                kafka_protocol::records::RecordBatchDecoder::decode_all(&mut bytes).unwrap();
            let record = &batches[0].records[0];
            assert!(record.transactional);
            assert_eq!(record.producer_id, 42);
            assert_eq!(record.producer_epoch, 3);
            assert_eq!(record.sequence, 7);
            assert_eq!(record.timestamp, timestamp);
            assert_eq!(record.timestamp_type, TimestampType::Creation);
            assert_eq!(
                record.headers[&StrBytes::from_static_str("header")].as_deref(),
                Some(&b"header-value"[..])
            );
            assert_eq!(record.key.as_deref(), Some(&b"key"[..]));
            assert_eq!(record.value.as_deref(), Some(&b"value"[..]));
        }
    }

    fn enabled_codecs() -> impl Iterator<Item = Compression> {
        [
            Compression::NONE,
            #[cfg(feature = "gzip")]
            Compression::GZIP,
            #[cfg(feature = "snappy")]
            Compression::SNAPPY,
            #[cfg(feature = "lz4")]
            Compression::LZ4,
            #[cfg(feature = "zstd")]
            Compression::ZSTD,
        ]
        .into_iter()
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

    #[test]
    fn contiguous_target_runs_preserve_record_order_and_unique_headers_for_all_codecs() {
        let headers = [
            ("first".to_owned(), bytes::Bytes::from_static(b"one")),
            ("empty".to_owned(), bytes::Bytes::new()),
            ("last".to_owned(), bytes::Bytes::from_static(b"three")),
        ];
        let messages: [ProduceMessageRef<'_>; 6] = [
            ("a", 0, Some(b""), Some(b"first"), &headers),
            ("a", 0, Some(b""), Some(b"second"), &headers),
            ("a", 1, None, Some(b"other-partition"), &headers),
            ("b", 0, None, Some(b"other-topic"), &headers),
            ("a", 0, Some(b""), Some(b"third"), &headers),
            ("a", 0, Some(b""), Some(b"fourth"), &headers),
        ];
        for compression in enabled_codecs() {
            let (_, request) =
                build_produce_request(1, "client", 1, 30_000, compression, &messages).unwrap();
            for topic in request.topic_data {
                for partition in topic.partition_data {
                    let batches = kafka_protocol::records::RecordBatchDecoder::decode_all(
                        &mut partition.records.unwrap(),
                    )
                    .unwrap();
                    assert_eq!(batches.len(), 1);
                    let expected: &[&[u8]] = match (topic.name.as_str(), partition.index) {
                        ("a", 0) => &[b"first", b"second", b"third", b"fourth"],
                        ("a", 1) => &[b"other-partition"],
                        ("b", 0) => &[b"other-topic"],
                        _ => unreachable!(),
                    };
                    assert_eq!(batches[0].records.len(), expected.len());
                    for (index, record) in batches[0].records.iter().enumerate() {
                        assert_eq!(record.offset, i64::try_from(index).unwrap());
                        assert_eq!(record.sequence, i32::try_from(index).unwrap() - 1);
                        assert_eq!(record.value.as_deref(), Some(expected[index]));
                        assert_eq!(
                            record
                                .headers
                                .keys()
                                .map(StrBytes::as_str)
                                .collect::<Vec<_>>(),
                            vec!["first", "empty", "last"]
                        );
                        assert_eq!(
                            record.headers[&StrBytes::from_static_str("empty")].as_deref(),
                            Some(&b""[..])
                        );
                        if topic.name.as_str() == "a" && partition.index == 0 {
                            assert_eq!(record.key.as_deref(), Some(&b""[..]));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn confirmation_iterator_preserves_response_order_and_rejects_negative_offsets() {
        use kafka_protocol::messages::produce_response::{
            PartitionProduceResponse as GeneratedPartition, TopicProduceResponse as GeneratedTopic,
        };
        let response = ProduceResponse::default().with_responses(vec![
            GeneratedTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("a")))
                .with_partition_responses(vec![
                    GeneratedPartition::default()
                        .with_index(0)
                        .with_base_offset(42),
                    GeneratedPartition::default()
                        .with_index(1)
                        .with_error_code(KafkaCode::NotLeaderForPartition as i16)
                        .with_base_offset(-1),
                    GeneratedPartition::default()
                        .with_index(2)
                        .with_error_code(i16::MAX)
                        .with_base_offset(-1),
                    GeneratedPartition::default()
                        .with_index(3)
                        .with_base_offset(-1),
                ]),
            GeneratedTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("empty"))),
            GeneratedTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("a")))
                .with_partition_responses(vec![
                    GeneratedPartition::default()
                        .with_index(0)
                        .with_base_offset(42),
                ]),
        ]);
        let actual: Vec<_> = into_produce_confirmations(response)
            .map(|confirm| {
                (
                    confirm.topic,
                    confirm
                        .partition_confirms
                        .into_iter()
                        .map(|partition| (partition.partition, partition.offset))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(
            actual,
            vec![
                (
                    "a".to_owned(),
                    vec![
                        (0, Ok(42)),
                        (1, Err(KafkaCode::NotLeaderForPartition)),
                        (2, Err(KafkaCode::Unknown)),
                        (3, Err(KafkaCode::Unknown)),
                    ]
                ),
                ("empty".to_owned(), vec![]),
                ("a".to_owned(), vec![(0, Ok(42))]),
            ]
        );
    }
}
