use bytes::{Buf, Bytes};
use kafka_protocol::compression::{self as compression, Decompressor};
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FetchResponse, RequestHeader, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Compression as RecordCompression, RecordBatchDecoder};

use super::API_VERSION_FETCH;
use crate::error::{Error, KafkaCode};
use std::sync::Arc;

// Re-exports of sub-types from kafka_protocol for convenience
use kafka_protocol::messages::fetch_request::FetchPartition as KpFetchPartition;
use kafka_protocol::messages::fetch_request::FetchTopic as KpFetchTopic;
#[cfg(test)]
use kafka_protocol::messages::fetch_response::PartitionData as KpPartitionData;

// ---------------------------------------------------------------------------
// Owned fetch response types (no lifetimes) for the protocol adapter.
// These mirror the structure of the legacy protocol::fetch types but own
// all their data (String instead of &str, Bytes/Vec<u8> instead of &[u8]).

/// Owned version of `protocol::fetch::Message` with no lifetimes.
#[derive(Debug, Clone)]
pub struct OwnedMessage {
    /// The message offset within the partition.
    pub offset: i64,
    /// The message key bytes.
    pub key: Bytes,
    /// The message value bytes.
    pub value: Bytes,
}

/// Owned version of `protocol::fetch::Data` with no lifetimes.
#[derive(Debug)]
pub struct OwnedData {
    /// The high watermark offset at the time of the fetch.
    pub highwatermark_offset: i64,
    /// Messages decoded from the fetched records.
    pub messages: Vec<OwnedMessage>,
}

/// Owned version of `protocol::fetch::Partition` with no lifetimes.
#[derive(Debug)]
pub struct OwnedPartition {
    /// Partition id.
    pub partition: i32,
    /// Decoded partition data or a decoding error.
    pub data: Result<OwnedData, Arc<Error>>,
    /// High watermark offset for this partition (always available, even on errors).
    pub highwatermark: i64,
}

/// Owned version of `protocol::fetch::Topic` with no lifetimes.
#[derive(Debug)]
pub struct OwnedTopic {
    /// Topic name.
    pub topic: String,
    /// Partition-level data for this topic.
    pub partitions: Vec<OwnedPartition>,
}

/// Owned version of `protocol::fetch::Response` with no lifetimes.
#[derive(Debug)]
pub struct OwnedFetchResponse {
    /// Correlation id matching the request.
    pub correlation_id: i32,
    /// Topics included in this fetch response.
    pub topics: Vec<OwnedTopic>,
}

/// Verified next fetch offsets, aligned with an owned response's topic and
/// partition positions. Offsets come from fully validated batch headers,
/// including batches with no application records.
#[derive(Debug)]
pub struct FetchProgress {
    next_offsets: Vec<Vec<Option<i64>>>,
}

impl FetchProgress {
    /// Returns the verified next offset for the partition at these positions.
    ///
    /// `None` means the partition contained no batches, had a broker or decoding
    /// error, or the supplied positions are outside the corresponding response.
    /// These are response indices, rather than Kafka topic or partition IDs.
    #[must_use]
    pub fn next_offset(&self, topic_index: usize, partition_index: usize) -> Option<i64> {
        self.next_offsets
            .get(topic_index)?
            .get(partition_index)
            .copied()
            .flatten()
    }
}

/// Owned fetch data accompanied by progress from fully validated record batches.
///
/// Use [`Self::into_parts`] to preserve progress for empty, control, and compacted
/// batches without adding fields to the existing owned response types.
#[derive(Debug)]
pub struct FetchResponseWithProgress {
    response: OwnedFetchResponse,
    progress: FetchProgress,
}

impl FetchResponseWithProgress {
    /// Borrows the owned response without discarding its associated progress.
    #[must_use]
    pub fn response(&self) -> &OwnedFetchResponse {
        &self.response
    }

    /// Splits the response and its progress, preserving their positional alignment.
    #[must_use]
    pub fn into_parts(self) -> (OwnedFetchResponse, FetchProgress) {
        (self.response, self.progress)
    }

    /// Returns the existing owned response representation and discards progress.
    #[must_use]
    pub fn into_owned(self) -> OwnedFetchResponse {
        self.response
    }
}

impl OwnedPartition {
    /// Returns partition fetch data or the decoding error for this partition.
    ///
    /// # Errors
    ///
    /// Returns a reference to the stored error when decoding failed.
    pub fn data(&self) -> Result<&OwnedData, &Arc<Error>> {
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

/// Converts a generated Fetch response into owned partition data.
///
/// Every record batch is decoded and validated before accepting a partition,
/// including batches following empty compacted batches. Partition-level broker
/// errors, malformed records, and unavailable compression codecs are retained
/// in each partition's data result. Control records are excluded from application
/// messages. Decoder panics become codec errors. Use
/// [`convert_fetch_response_with_progress`] when advancing a consumer position.
#[must_use]
pub fn convert_fetch_response(kp_resp: FetchResponse, correlation_id: i32) -> OwnedFetchResponse {
    convert_fetch_response_inner::<false>(kp_resp, correlation_id).into_owned()
}

/// Converts a generated response while preserving verified batch progress.
///
/// Progress follows the returned response's topic and partition indices and is
/// published only for partitions whose complete record data passes CRC,
/// decompression, exact record framing, batch bounds, and offset ordering checks.
/// A corrupt tail removes the entire partition's messages and progress. The high
/// watermark never serves as a replacement for a batch's next offset.
#[must_use]
pub fn convert_fetch_response_with_progress(
    kp_resp: FetchResponse,
    correlation_id: i32,
) -> FetchResponseWithProgress {
    convert_fetch_response_inner::<true>(kp_resp, correlation_id)
}

fn convert_fetch_response_inner<const WITH_PROGRESS: bool>(
    kp_resp: FetchResponse,
    correlation_id: i32,
) -> FetchResponseWithProgress {
    let mut topics = Vec::with_capacity(kp_resp.responses.len());
    let mut next_offsets = if WITH_PROGRESS {
        Vec::with_capacity(kp_resp.responses.len())
    } else {
        Vec::new()
    };
    for topic in kp_resp.responses {
        let topic_name = topic.topic.to_string();
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        let mut partition_offsets = if WITH_PROGRESS {
            Vec::with_capacity(topic.partitions.len())
        } else {
            Vec::new()
        };
        for partition in topic.partitions {
            let (data, next_offset) = if partition.error_code != 0 {
                (
                    Err(Arc::new(Error::TopicPartitionError {
                        topic_name: topic_name.clone(),
                        partition_id: partition.partition_index,
                        error_code: KafkaCode::from_protocol(partition.error_code)
                            .unwrap_or(KafkaCode::Unknown),
                    })),
                    None,
                )
            } else {
                match decode_partition_records(partition.records, partition.high_watermark) {
                    Ok((data, next_offset)) => (Ok(data), next_offset),
                    Err(error) => (Err(error), None),
                }
            };
            partitions.push(OwnedPartition {
                partition: partition.partition_index,
                data,
                highwatermark: partition.high_watermark,
            });
            if WITH_PROGRESS {
                partition_offsets.push(next_offset);
            }
        }
        topics.push(OwnedTopic {
            topic: topic_name,
            partitions,
        });
        if WITH_PROGRESS {
            next_offsets.push(partition_offsets);
        }
    }
    FetchResponseWithProgress {
        response: OwnedFetchResponse {
            correlation_id,
            topics,
        },
        progress: FetchProgress { next_offsets },
    }
}

fn decode_partition_records(
    records: Option<Bytes>,
    high_watermark: i64,
) -> Result<(OwnedData, Option<i64>), Arc<Error>> {
    let records_bytes = records.unwrap_or_default();
    if records_bytes.is_empty() {
        return Ok((
            OwnedData {
                highwatermark_offset: high_watermark,
                messages: vec![],
            },
            None,
        ));
    }

    let decoded = decode_records_safe(records_bytes).map_err(Arc::new)?;

    Ok((
        OwnedData {
            highwatermark_offset: high_watermark,
            messages: decoded.messages,
        },
        decoded.next_offset,
    ))
}

struct DecodedRecords {
    messages: Vec<OwnedMessage>,
    next_offset: Option<i64>,
}

fn decode_record_batches(mut records: Bytes) -> Result<DecodedRecords, Error> {
    let mut messages = Vec::new();
    let mut next_offset = None;
    let mut previous_record_offset = None;
    // Each partition can contain several batches, including empty batches left
    // by compaction. Decode all of them before accepting the partition data.
    while !records.is_empty() {
        let (base_offset, batch_next_offset, record_count) = batch_offset_bounds(&records)?;
        if next_offset.is_some_and(|previous_next| base_offset < previous_next) {
            return Err(Error::codec());
        }
        let record_set = RecordBatchDecoder::decode_with_custom_compression(
            &mut records,
            Some(|bytes: &mut Bytes, codec| {
                let checked = |decoded: &mut Bytes| {
                    validate_record_frames(decoded.clone(), record_count)
                        .map_err(std::io::Error::other)?;
                    Ok(decoded.clone())
                };
                match codec {
                    RecordCompression::None => compression::None::decompress(bytes, checked),
                    #[cfg(feature = "gzip")]
                    RecordCompression::Gzip => compression::Gzip::decompress(bytes, checked),
                    #[cfg(feature = "snappy")]
                    RecordCompression::Snappy => compression::Snappy::decompress(bytes, checked),
                    #[cfg(feature = "lz4")]
                    RecordCompression::Lz4 => compression::Lz4::decompress(bytes, checked),
                    #[cfg(feature = "zstd")]
                    RecordCompression::Zstd => compression::Zstd::decompress(bytes, checked),
                    #[cfg(not(feature = "gzip"))]
                    RecordCompression::Gzip => Err(disabled_codec_error(codec).into()),
                    #[cfg(not(feature = "snappy"))]
                    RecordCompression::Snappy => Err(disabled_codec_error(codec).into()),
                    #[cfg(not(feature = "lz4"))]
                    RecordCompression::Lz4 => Err(disabled_codec_error(codec).into()),
                    #[cfg(not(feature = "zstd"))]
                    RecordCompression::Zstd => Err(disabled_codec_error(codec).into()),
                }
            }),
        )
        .map_err(|err| map_record_decode_error(&err.to_string()))?;
        if !record_set
            .records
            .first()
            .is_some_and(|record| record.control)
        {
            messages.reserve(record_set.records.len());
        }
        for record in record_set.records {
            if !(base_offset..batch_next_offset).contains(&record.offset)
                || previous_record_offset.is_some_and(|previous| record.offset <= previous)
            {
                return Err(Error::codec());
            }
            previous_record_offset = Some(record.offset);
            if !record.control {
                messages.push(OwnedMessage {
                    offset: record.offset,
                    key: record.key.unwrap_or_default(),
                    value: record.value.unwrap_or_default(),
                });
            }
        }
        next_offset = Some(batch_next_offset);
    }
    Ok(DecodedRecords {
        messages,
        next_offset,
    })
}

fn batch_offset_bounds(records: &[u8]) -> Result<(i64, i64, i32), Error> {
    // The SDK currently supports only magic=2. Preserve its existing codec
    // rejection of legacy and unknown magic without assuming a v2 header size.
    if records.get(16) != Some(&2) || records.len() < 61 {
        return Err(Error::codec());
    }
    let base_offset = i64::from_be_bytes(records[..8].try_into().map_err(|_| Error::codec())?);
    let last_delta = i32::from_be_bytes(records[23..27].try_into().map_err(|_| Error::codec())?);
    let record_count = i32::from_be_bytes(records[57..61].try_into().map_err(|_| Error::codec())?);
    if base_offset < 0 || last_delta < 0 {
        return Err(Error::codec());
    }
    let next_offset = base_offset
        .checked_add(i64::from(last_delta))
        .and_then(|last| last.checked_add(1))
        .ok_or_else(Error::codec)?;
    Ok((base_offset, next_offset, record_count))
}

fn validate_record_frames(mut bytes: Bytes, record_count: i32) -> Result<(), Error> {
    if record_count < 0 {
        return Err(Error::codec());
    }
    // The SDK checks record contents, but does not reject bytes left within a
    // record or after the declared count. Check both boundaries before its
    // count-based Vec reservation so a header cursor cannot skip hidden records.
    for _ in 0..record_count {
        let size = read_record_varint(&mut bytes)?;
        let size = usize::try_from(size).map_err(|_| Error::codec())?;
        // Even null key/value and zero headers require these six field bytes.
        if size < 6 {
            return Err(Error::codec());
        }
        if size > bytes.len() {
            return Err(Error::codec());
        }
        validate_record_frame(bytes.split_to(size))?;
    }
    if !bytes.is_empty() {
        return Err(Error::codec());
    }
    Ok(())
}

fn validate_record_frame(mut frame: Bytes) -> Result<(), Error> {
    let _attributes = frame.try_get_u8().map_err(|_| Error::codec())?;
    let _timestamp_delta = read_record_unsigned_varint(&mut frame, 10, 1)?;
    let _offset_delta = read_record_varint(&mut frame)?;
    skip_nullable_record_bytes(&mut frame)?;
    skip_nullable_record_bytes(&mut frame)?;
    let header_count = read_record_varint(&mut frame)?;
    if header_count < 0 {
        return Err(Error::codec());
    }
    for _ in 0..header_count {
        let key_length = read_record_varint(&mut frame)?;
        skip_record_bytes(&mut frame, key_length)?;
        skip_nullable_record_bytes(&mut frame)?;
    }
    if !frame.is_empty() {
        return Err(Error::codec());
    }
    Ok(())
}

fn read_record_varint(frame: &mut Bytes) -> Result<i32, Error> {
    let value = read_record_unsigned_varint(frame, 5, 0x0f)?;
    let magnitude = i32::try_from(value >> 1).map_err(|_| Error::codec())?;
    Ok(if value & 1 == 0 {
        magnitude
    } else {
        !magnitude
    })
}

// The SDK's primitive Decoder trait is private. This bounded framing reader
// only advances numeric field boundaries; the SDK still parses their contents.
fn read_record_unsigned_varint(
    frame: &mut Bytes,
    max_bytes: u32,
    final_mask: u8,
) -> Result<u64, Error> {
    let mut value = 0u64;
    for index in 0..max_bytes {
        let byte = frame.try_get_u8().map_err(|_| Error::codec())?;
        if index == max_bytes - 1 && byte > final_mask {
            return Err(Error::codec());
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Error::codec())
}

fn skip_nullable_record_bytes(frame: &mut Bytes) -> Result<(), Error> {
    let length = read_record_varint(frame)?;
    if length == -1 {
        return Ok(());
    }
    skip_record_bytes(frame, length)
}

fn skip_record_bytes(frame: &mut Bytes, length: i32) -> Result<(), Error> {
    let length = usize::try_from(length).map_err(|_| Error::codec())?;
    if length > frame.len() {
        return Err(Error::codec());
    }
    frame.advance(length);
    Ok(())
}

#[cfg(not(all(
    feature = "gzip",
    feature = "snappy",
    feature = "lz4",
    feature = "zstd"
)))]
fn disabled_codec_error(codec: RecordCompression) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("Support for {codec:?} is not enabled as a cargo feature"),
    )
}

/// Decode all fetched record batches, mapping decoder panics to codec errors.
fn decode_records_safe(records: Bytes) -> Result<DecodedRecords, crate::error::Error> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        decode_record_batches(records)
    }));
    match result {
        Ok(r) => r,
        Err(_) => Err(crate::error::Error::codec()),
    }
}

fn map_record_decode_error(message: &str) -> crate::error::Error {
    if is_disabled_compression_feature_error(message) {
        crate::error::Error::unsupported_compression()
    } else {
        crate::error::Error::codec()
    }
}

fn is_disabled_compression_feature_error(message: &str) -> bool {
    message.contains("Support for") && message.contains("not enabled as a cargo feature")
}

#[cfg(test)]
mod tests {
    use kafka_protocol::records::{
        Compression as KpCompression, Record, RecordBatchEncoder, RecordEncodeOptions,
        TimestampType,
    };

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
            highwatermark: 50,
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
            highwatermark: 200,
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
            data: Err(err),
            highwatermark: 0,
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
                    highwatermark: 10,
                },
                OwnedPartition {
                    partition: 1,
                    data: Ok(OwnedData {
                        highwatermark_offset: 20,
                        messages: vec![],
                    }),
                    highwatermark: 20,
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
                    highwatermark: 5,
                }],
            }],
        };
        assert_eq!(resp.correlation_id, 42);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].topic, "orders");
    }

    #[cfg(feature = "compression")]
    #[test]
    fn convert_fetch_response_decodes_compressed_record_batches() {
        for compression in [
            KpCompression::Gzip,
            KpCompression::Snappy,
            KpCompression::Lz4,
            KpCompression::Zstd,
        ] {
            let response = FetchResponse::default().with_responses(vec![
                kafka_protocol::messages::fetch_response::FetchableTopicResponse::default()
                    .with_topic(TopicName::from(StrBytes::from_string("topic-a".to_owned())))
                    .with_partitions(vec![
                        KpPartitionData::default()
                            .with_partition_index(0)
                            .with_error_code(0)
                            .with_high_watermark(1)
                            .with_records(Some(encoded_records(compression, 0))),
                    ]),
            ]);

            let converted = convert_fetch_response(response, 7);
            let data = converted.topics[0].partitions[0]
                .data()
                .unwrap_or_else(|err| panic!("{compression:?} should decode: {err}"));

            assert_eq!(data.messages.len(), 1);
            assert_eq!(&*data.messages[0].key, b"key");
            assert_eq!(&*data.messages[0].value, b"value");
        }
    }

    #[test]
    fn partition_decoder_returns_records_from_every_batch() {
        let mut batches = bytes::BytesMut::new();
        batches.extend_from_slice(&encoded_records(KpCompression::None, 4));
        batches.extend_from_slice(&encoded_records(KpCompression::None, 5));
        let batches = batches.freeze();

        let data = decode_partition_records(Some(batches.clone()), 6)
            .unwrap()
            .0;
        assert_eq!(data.highwatermark_offset, 6);
        assert_eq!(
            data.messages
                .iter()
                .map(|message| message.offset)
                .collect::<Vec<_>>(),
            [4, 5]
        );
        assert_eq!(decode_records_safe(batches).unwrap().messages.len(), 2);
    }

    #[test]
    fn partition_decoder_continues_after_an_empty_compacted_batch() {
        // A complete magic=2 batch with a valid CRC32C and recordsCount=0.
        let empty_batch: [u8; 61] = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 49, 255, 255, 255, 255, 2, 235, 224, 2, 3, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0,
        ];
        let mut batches = bytes::BytesMut::from(&empty_batch[..]);
        batches.extend_from_slice(&encoded_records(KpCompression::None, 5));

        let data = decode_partition_records(Some(batches.freeze()), 6)
            .unwrap()
            .0;
        assert_eq!(data.messages.len(), 1);
        assert_eq!(data.messages[0].offset, 5);
    }

    #[test]
    fn partition_decoder_rejects_a_corrupt_or_truncated_later_batch() {
        let first = encoded_records(KpCompression::None, 4);
        let mut corrupt = bytes::BytesMut::from(&encoded_records(KpCompression::None, 5)[..]);
        let last_index = corrupt.len() - 1;
        corrupt[last_index] ^= 1;
        for tail in [corrupt.freeze(), Bytes::from_static(&[0; 7])] {
            let mut batches = bytes::BytesMut::from(&first[..]);
            batches.extend_from_slice(&tail);
            let batches = batches.freeze();

            assert!(decode_partition_records(Some(batches.clone()), 6).is_err());
            assert!(decode_records_safe(batches).is_err());
        }
    }

    #[test]
    fn partition_decoder_accepts_record_data_larger_than_one_mib() {
        let mut batches = bytes::BytesMut::new();
        for offset in 0..16_384 {
            batches.extend_from_slice(&encoded_records(KpCompression::None, offset));
        }
        assert!(batches.len() > 1_048_576);

        let data = decode_partition_records(Some(batches.freeze()), 16_384)
            .unwrap()
            .0;
        assert_eq!(data.messages.len(), 16_384);
        assert_eq!(data.messages.last().unwrap().offset, 16_383);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn partition_decoder_handles_batches_with_different_compression_codecs() {
        let mut batches = bytes::BytesMut::new();
        for (offset, compression) in [
            KpCompression::None,
            KpCompression::Gzip,
            KpCompression::Snappy,
            KpCompression::Lz4,
            KpCompression::Zstd,
        ]
        .into_iter()
        .enumerate()
        {
            batches.extend_from_slice(&encoded_records(
                compression,
                i64::try_from(offset).unwrap(),
            ));
        }

        let data = decode_partition_records(Some(batches.freeze()), 5)
            .unwrap()
            .0;
        assert_eq!(data.messages.len(), 5);
        assert_eq!(data.messages.last().unwrap().offset, 4);
    }

    pub(super) fn encoded_records(compression: KpCompression, offset: i64) -> Bytes {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: -1,
            timestamp: 0,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers: indexmap::IndexMap::default(),
        };
        let mut buf = bytes::BytesMut::new();
        let options = RecordEncodeOptions {
            version: 2,
            compression,
        };
        RecordBatchEncoder::encode(&mut buf, &[record], &options)
            .unwrap_or_else(|err| panic!("{compression:?} should encode: {err}"));
        buf.freeze()
    }
}

#[cfg(test)]
#[path = "fetch_progress_tests.rs"]
mod progress_tests;
