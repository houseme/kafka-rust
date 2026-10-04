use bytes::BytesMut;
use kafka_protocol::messages::fetch_response::FetchableTopicResponse;
use kafka_protocol::records::{
    Compression as KpCompression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

use super::tests::encoded_records;
use super::*;

fn record(offset: i64) -> Record {
    Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset,
        sequence: i32::try_from(offset).unwrap_or(-1),
        timestamp: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers: indexmap::IndexMap::default(),
    }
}

fn control_record(offset: i64, marker: i16) -> Record {
    let mut record = record(offset);
    record.transactional = true;
    record.control = true;
    record.producer_id = 7;
    record.producer_epoch = 0;
    record.sequence = -1;
    let mut key = vec![0, 0];
    key.extend_from_slice(&marker.to_be_bytes());
    record.key = Some(Bytes::from(key));
    record.value = Some(Bytes::from_static(&[0; 6]));
    record
}

fn encode(records: &[Record], compression: KpCompression) -> Bytes {
    let mut bytes = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut bytes,
        records,
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
    )
    .unwrap();
    bytes.freeze()
}

fn rewrite_crc(batch: &mut [u8]) {
    assert!(batch.len() >= 61 && batch[16] == 2);
    let mut crc = !0u32;
    for byte in &batch[21..] {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0u32.wrapping_sub(crc & 1) & 0x82f6_3b78);
        }
    }
    batch[17..21].copy_from_slice(&(!crc).to_be_bytes());
}

fn empty_batch(base: i64, last_delta: i32) -> Bytes {
    let mut bytes = encoded_records(KpCompression::None, base)[..61].to_vec();
    bytes[8..12].copy_from_slice(&49i32.to_be_bytes());
    bytes[23..27].copy_from_slice(&last_delta.to_be_bytes());
    bytes[27..35].copy_from_slice(&(-1i64).to_be_bytes());
    bytes[57..61].copy_from_slice(&0i32.to_be_bytes());
    rewrite_crc(&mut bytes);
    Bytes::from(bytes)
}

fn concatenate(batches: &[Bytes]) -> Bytes {
    let mut bytes = BytesMut::new();
    for batch in batches {
        bytes.extend_from_slice(batch);
    }
    bytes.freeze()
}

fn response(records: Option<Bytes>) -> FetchResponse {
    FetchResponse::default().with_responses(vec![
        FetchableTopicResponse::default()
            .with_topic(StrBytes::from_static_str("topic").into())
            .with_partitions(vec![
                KpPartitionData::default()
                    .with_partition_index(9)
                    .with_high_watermark(900)
                    .with_records(records),
            ]),
    ])
}

fn assert_codec_without_progress(records: Bytes) {
    let (response, progress) =
        convert_fetch_response_with_progress(response(Some(records.clone())), 7).into_parts();
    assert!(matches!(
        response.topics[0].partitions[0]
            .data()
            .unwrap_err()
            .as_ref(),
        Error::Protocol(crate::error::ProtocolError::Codec)
    ));
    assert_eq!(progress.next_offset(0, 0), None);
    assert!(
        convert_fetch_response(self::response(Some(records)), 7).topics[0].partitions[0]
            .data()
            .is_err()
    );
}

#[test]
fn control_commit_and_abort_are_hidden_but_advance_verified_progress() {
    let records = concatenate(&[
        encode(&[record(3)], KpCompression::None),
        encode(&[control_record(4, 1)], KpCompression::None),
        encode(&[control_record(5, 0)], KpCompression::None),
        encode(&[record(6)], KpCompression::None),
    ]);
    let wrapper = convert_fetch_response_with_progress(response(Some(records.clone())), 7);
    assert_eq!(wrapper.response().correlation_id, 7);
    let (converted, progress) = wrapper.into_parts();
    let messages = &converted.topics[0].partitions[0].data().unwrap().messages;
    assert_eq!(
        messages
            .iter()
            .map(|message| message.offset)
            .collect::<Vec<_>>(),
        [3, 6]
    );
    assert_eq!(progress.next_offset(0, 0), Some(7));
    assert_eq!(converted.topics[0].partitions[0].highwatermark, 900);
    assert_eq!(
        convert_fetch_response(response(Some(records)), 7).topics[0].partitions[0]
            .data()
            .unwrap()
            .messages
            .len(),
        2
    );
}

#[test]
fn control_only_batches_have_no_application_messages_and_keep_the_cursor() {
    let records = concatenate(&[
        encode(&[control_record(4, 1)], KpCompression::None),
        encode(&[control_record(5, 0)], KpCompression::None),
    ]);
    let (converted, progress) =
        convert_fetch_response_with_progress(response(Some(records)), 7).into_parts();
    assert_eq!(
        converted.topics[0].partitions[0]
            .data()
            .unwrap()
            .messages
            .len(),
        0
    );
    assert_eq!(progress.next_offset(0, 0), Some(6));
}

#[test]
fn empty_and_trailing_compacted_batches_advance_to_header_end_instead_of_high_watermark() {
    for records in [
        empty_batch(4, 3),
        concatenate(&[encoded_records(KpCompression::None, 3), empty_batch(4, 3)]),
    ] {
        let (converted, progress) =
            convert_fetch_response_with_progress(response(Some(records)), 7).into_parts();
        assert_eq!(progress.next_offset(0, 0), Some(8));
        assert_eq!(
            converted.topics[0].partitions[0]
                .data()
                .unwrap()
                .highwatermark_offset,
            900
        );
    }
}

#[test]
fn progress_is_aligned_by_response_position_without_reusing_partition_ids() {
    let mut generated = response(Some(empty_batch(4, 3)));
    generated.responses[0].partitions.extend([
        KpPartitionData::default()
            .with_partition_index(42)
            .with_records(None),
        KpPartitionData::default()
            .with_partition_index(0)
            .with_records(Some(Bytes::new())),
        KpPartitionData::default()
            .with_partition_index(99)
            .with_error_code(KafkaCode::NotLeaderForPartition as i16)
            .with_records(Some(encoded_records(KpCompression::None, 8))),
    ]);
    generated.responses.push(
        FetchableTopicResponse::default()
            .with_topic(StrBytes::from_static_str("other").into())
            .with_partitions(vec![
                KpPartitionData::default()
                    .with_partition_index(9)
                    .with_records(Some(encoded_records(KpCompression::None, 12))),
            ]),
    );
    let (converted, progress) = convert_fetch_response_with_progress(generated, 7).into_parts();
    assert_eq!(converted.topics[0].partitions[0].partition, 9);
    assert_eq!(progress.next_offset(0, 0), Some(8));
    assert_eq!(progress.next_offset(0, 1), None);
    assert_eq!(progress.next_offset(0, 2), None);
    assert_eq!(progress.next_offset(0, 3), None);
    assert_eq!(progress.next_offset(1, 0), Some(13));
    assert_eq!(progress.next_offset(9, 0), None);
    assert_eq!(progress.next_offset(0, 9), None);
}

#[test]
fn compacted_gaps_nonzero_first_delta_and_removed_tail_are_accepted() {
    let mut batch = encode(&[record(11), record(13)], KpCompression::None).to_vec();
    // The surviving records have deltas 1 and 3 from the preserved original base.
    batch[..8].copy_from_slice(&10i64.to_be_bytes());
    batch[23..27].copy_from_slice(&5i32.to_be_bytes());
    // Rewrite the one-byte offset deltas: lengths, timestamp and key stay intact.
    batch[64] = 2;
    let first_size = usize::from(batch[61] >> 1);
    batch[61 + 1 + first_size + 3] = 6;
    rewrite_crc(&mut batch);
    let (converted, progress) =
        convert_fetch_response_with_progress(response(Some(Bytes::from(batch))), 7).into_parts();
    assert_eq!(
        converted.topics[0].partitions[0]
            .data()
            .unwrap()
            .messages
            .iter()
            .map(|message| message.offset)
            .collect::<Vec<_>>(),
        [11, 13]
    );
    assert_eq!(progress.next_offset(0, 0), Some(16));
}

#[test]
fn valid_crc_negative_and_out_of_bounds_record_deltas_are_rejected() {
    for control in [false, true] {
        for delta in [1, 2] {
            let mut batch = if control {
                encode(&[control_record(10, 1)], KpCompression::None).to_vec()
            } else {
                encoded_records(KpCompression::None, 10).to_vec()
            };
            // Zigzag 1 is -1 and 2 is +1; both escape [base=10,last=10].
            batch[64] = delta;
            rewrite_crc(&mut batch);
            assert_codec_without_progress(Bytes::from(batch));
        }
    }
}

#[test]
fn duplicate_and_reverse_records_are_rejected_with_valid_crc() {
    for offsets in [[11, 11], [12, 11]] {
        let records = offsets.into_iter().map(record).collect::<Vec<_>>();
        assert_codec_without_progress(encode(&records, KpCompression::None));
    }
}

#[test]
fn repeated_reverse_and_overlapping_batch_ranges_are_rejected() {
    for batches in [
        vec![
            encoded_records(KpCompression::None, 4),
            encoded_records(KpCompression::None, 4),
        ],
        vec![
            encoded_records(KpCompression::None, 5),
            encoded_records(KpCompression::None, 4),
        ],
        vec![empty_batch(4, 3), encoded_records(KpCompression::None, 6)],
    ] {
        assert_codec_without_progress(concatenate(&batches));
    }
}

#[test]
fn max_minus_one_record_and_next_max_are_accepted_but_header_overflow_is_rejected() {
    let (converted, progress) = convert_fetch_response_with_progress(
        response(Some(encoded_records(KpCompression::None, i64::MAX - 1))),
        7,
    )
    .into_parts();
    assert_eq!(
        converted.topics[0].partitions[0].data().unwrap().messages[0].offset,
        i64::MAX - 1
    );
    assert_eq!(progress.next_offset(0, 0), Some(i64::MAX));
    for (base, delta) in [(i64::MAX, 0), (i64::MAX - 1, 2), (-1, 0), (3, -1)] {
        assert_codec_without_progress(empty_batch(base, delta));
    }
}

#[test]
fn corrupt_or_truncated_tail_discards_preceding_messages_and_progress() {
    let valid = encoded_records(KpCompression::None, 4);
    let mut corrupt = encoded_records(KpCompression::None, 5).to_vec();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    let mut corrupt_control = encode(&[control_record(5, 1)], KpCompression::None).to_vec();
    let last = corrupt_control.len() - 1;
    corrupt_control[last] ^= 1;
    for tail in [
        Bytes::from(corrupt),
        Bytes::from(corrupt_control),
        Bytes::from_static(&[0; 7]),
        empty_batch(3, 0),
    ] {
        assert_codec_without_progress(concatenate(&[valid.clone(), tail]));
    }
}

#[test]
fn declared_record_count_must_exactly_cover_record_frames_with_valid_crc() {
    for count in [0i32, 1, 3, -1, i32::MAX] {
        let mut batch = encode(&[record(10), record(11)], KpCompression::None).to_vec();
        batch[57..61].copy_from_slice(&count.to_be_bytes());
        rewrite_crc(&mut batch);
        assert_codec_without_progress(Bytes::from(batch));
    }
    for length in [1u8, 0, 120, 254] {
        let mut batch = encoded_records(KpCompression::None, 10).to_vec();
        batch[61] = length;
        rewrite_crc(&mut batch);
        assert_codec_without_progress(Bytes::from(batch));
    }
}

fn embedded_record_frame() -> Bytes {
    let mut batch = encode(&[record(10), record(11)], KpCompression::None).to_vec();
    assert_eq!(&batch[57..61], &2i32.to_be_bytes());
    assert!(batch[61] < 128);
    let combined_size = u8::try_from((batch.len() - 62) * 2).unwrap();
    assert!(combined_size < 128);
    batch[57..61].copy_from_slice(&1i32.to_be_bytes());
    batch[61] = combined_size;
    rewrite_crc(&mut batch);
    Bytes::from(batch)
}

fn assert_embedded_record_is_rejected(batch: Bytes) {
    // The upstream decoder accepts this envelope and ignores the second record
    // hidden within the declared first record's frame. Our cursor must not skip it.
    let mut sdk_bytes = batch.clone();
    let sdk_records = RecordBatchDecoder::decode(&mut sdk_bytes).unwrap();
    assert!(sdk_bytes.is_empty());
    assert_eq!(sdk_records.records.len(), 1);
    assert_eq!(sdk_records.records[0].offset, 10);
    assert_codec_without_progress(batch);
}

#[test]
fn a_record_hidden_inside_an_expanded_record_frame_cannot_advance_progress() {
    assert_embedded_record_is_rejected(embedded_record_frame());
}

#[cfg(feature = "gzip")]
#[test]
fn gzip_record_subframes_are_fully_checked_after_one_decompression() {
    use kafka_protocol::compression::{Compressor, Gzip};

    let embedded = embedded_record_frame();
    let mut payload = BytesMut::new();
    Gzip::compress(&mut payload, |target| {
        target.extend_from_slice(&embedded[61..]);
        Ok(())
    })
    .unwrap();
    let mut compressed = embedded[..61].to_vec();
    compressed.extend_from_slice(&payload);
    let batch_length = i32::try_from(compressed.len() - 12).unwrap();
    compressed[8..12].copy_from_slice(&batch_length.to_be_bytes());
    compressed[21..23].copy_from_slice(&(KpCompression::Gzip as i16).to_be_bytes());
    rewrite_crc(&mut compressed);
    assert_embedded_record_is_rejected(Bytes::from(compressed));
}

fn batch_with_record_body(body: &[u8]) -> Bytes {
    let mut frame = BytesMut::new();
    let mut size = u32::try_from(body.len()).unwrap().checked_mul(2).unwrap();
    while size >= 128 {
        frame.extend_from_slice(&[u8::try_from(size & 0x7f).unwrap() | 0x80]);
        size >>= 7;
    }
    frame.extend_from_slice(&[u8::try_from(size).unwrap()]);
    frame.extend_from_slice(body);
    let mut batch = empty_batch(10, 0).to_vec();
    batch.extend_from_slice(&frame);
    let batch_length = i32::try_from(batch.len() - 12).unwrap();
    batch[8..12].copy_from_slice(&batch_length.to_be_bytes());
    batch[57..61].copy_from_slice(&1i32.to_be_bytes());
    rewrite_crc(&mut batch);
    Bytes::from(batch)
}

#[test]
fn bounded_varint_decodes_signed_extremes_without_consuming_the_next_field() {
    let cases: &[(&[u8], i32)] = &[
        (&[0], 0),
        (&[1], -1),
        (&[2], 1),
        (&[0xff, 0xff, 0xff, 0xff, 0x0f], i32::MIN),
        (&[0xfe, 0xff, 0xff, 0xff, 0x0f], i32::MAX),
    ];
    for &(encoded, expected) in cases {
        let mut bytes = encoded.to_vec();
        bytes.push(0xa5);
        let mut bytes = Bytes::from(bytes);
        assert_eq!(read_record_varint(&mut bytes).unwrap(), expected);
        assert_eq!(bytes.as_ref(), &[0xa5]);
    }
}

#[test]
fn bounded_varint_rejects_truncated_overflowing_and_unterminated_values() {
    let cases: &[&[u8]] = &[
        &[],
        &[0x80],
        &[0x80, 0x80, 0x80, 0x80],
        &[0x80, 0x80, 0x80, 0x80, 0x10],
        &[0x80, 0x80, 0x80, 0x80, 0x80],
    ];
    for &encoded in cases {
        let mut bytes = Bytes::copy_from_slice(encoded);
        assert!(matches!(
            read_record_varint(&mut bytes),
            Err(Error::Protocol(crate::error::ProtocolError::Codec))
        ));
    }
    let mut bytes = Bytes::from_static(&[0x80, 0x80, 0x80, 0x80, 0x80, 0]);
    assert!(read_record_varint(&mut bytes).is_err());
    assert_eq!(bytes.as_ref(), &[0]);
}

#[test]
fn bounded_varlong_accepts_full_width_values_without_consuming_the_next_field() {
    let cases: &[(&[u8], u64)] = &[
        (&[0], 0),
        (&[1], 1),
        (
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1],
            u64::MAX,
        ),
    ];
    for &(encoded, expected) in cases {
        let mut bytes = encoded.to_vec();
        bytes.push(0xa5);
        let mut bytes = Bytes::from(bytes);
        assert_eq!(
            read_record_unsigned_varint(&mut bytes, 10, 1).unwrap(),
            expected
        );
        assert_eq!(bytes.as_ref(), &[0xa5]);
    }
}

#[test]
fn bounded_varlong_rejects_truncated_overflowing_and_unterminated_values() {
    let cases: &[&[u8]] = &[
        &[],
        &[0x80],
        &[0x80; 9],
        &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 2],
        &[0x80; 10],
    ];
    for &encoded in cases {
        let mut bytes = Bytes::copy_from_slice(encoded);
        assert!(matches!(
            read_record_unsigned_varint(&mut bytes, 10, 1),
            Err(Error::Protocol(crate::error::ProtocolError::Codec))
        ));
    }
    let mut bytes = Bytes::from_static(&[
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0,
    ]);
    assert!(read_record_unsigned_varint(&mut bytes, 10, 1).is_err());
    assert_eq!(bytes.as_ref(), &[0]);
}

fn assert_sdk_and_converter_accept_null_record(batch: Bytes) {
    let mut sdk_bytes = batch.clone();
    let sdk_records = RecordBatchDecoder::decode(&mut sdk_bytes).unwrap();
    assert!(sdk_bytes.is_empty());
    assert_eq!(sdk_records.records.len(), 1);
    assert_eq!(sdk_records.records[0].offset, 10);
    assert_eq!(sdk_records.records[0].key, None);
    assert_eq!(sdk_records.records[0].value, None);
    let (decoded, progress) =
        convert_fetch_response_with_progress(response(Some(batch)), 7).into_parts();
    let messages = &decoded.topics[0].partitions[0].data().unwrap().messages;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].offset, 10);
    assert!(messages[0].key.is_empty() && messages[0].value.is_empty());
    assert_eq!(progress.next_offset(0, 0), Some(11));
}

#[test]
fn noncanonical_terminated_numeric_fields_keep_sdk_compatible_record_progress() {
    // Terminated overlong 0/-1 encodings are accepted by the SDK; framing
    // validation must not impose a canonical encoding requirement.
    for body in [
        vec![0, 0x80, 0, 0x80, 0, 0x81, 0, 0x81, 0, 0x80, 0],
        vec![0, 0, 0, 1, 1, 2, 0x80, 0, 0x81, 0],
        vec![
            0, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0, 0x80, 0x80, 0x80, 0x80, 0,
            1, 1, 0x80, 0x80, 0x80, 0x80, 0,
        ],
    ] {
        assert_sdk_and_converter_accept_null_record(batch_with_record_body(&body));
    }
    let mut overlong_size = batch_with_record_body(&[0, 0, 0, 1, 1, 0]).to_vec();
    assert!(overlong_size[61] < 128);
    overlong_size[61] |= 0x80;
    overlong_size.insert(62, 0);
    let length = i32::try_from(overlong_size.len() - 12).unwrap();
    overlong_size[8..12].copy_from_slice(&length.to_be_bytes());
    rewrite_crc(&mut overlong_size);
    assert_sdk_and_converter_accept_null_record(Bytes::from(overlong_size));
}

#[test]
fn overflowing_numeric_fields_accepted_by_sdk_cannot_publish_record_progress() {
    for field in ["timestamp", "offset", "headers"] {
        let mut body = vec![0];
        if field == "timestamp" {
            body.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 2]);
        } else {
            body.push(0);
        }
        if field == "offset" {
            body.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x10]);
        } else {
            body.push(0);
        }
        body.extend_from_slice(&[1, 1]);
        if field == "headers" {
            body.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x10]);
        } else {
            body.push(0);
        }
        let batch = batch_with_record_body(&body);
        let mut sdk_bytes = batch.clone();
        let records = RecordBatchDecoder::decode(&mut sdk_bytes).unwrap();
        assert!(sdk_bytes.is_empty());
        assert_eq!(records.records.len(), 1);
        assert_eq!(records.records[0].offset, 10);
        assert_codec_without_progress(batch);
    }
}

#[test]
fn nullable_and_header_field_lengths_must_fit_the_record_subframe() {
    for body in [
        vec![0, 0, 0, 3, 1, 0],              // key length -2
        vec![0, 0, 0, 1, 3, 0],              // value length -2
        vec![0, 0, 0, 1, 1, 1],              // headers count -1
        vec![0, 0, 0, 1, 1, 2, 1, 1],        // negative header key length
        vec![0, 0, 0, 1, 1, 2, 2, b'h', 3],  // header value length -2
        vec![0, 0, 0, 20, 1, 0],             // key extends beyond the record
        vec![0, 0, 0, 1, 20, 0],             // value extends beyond the record
        vec![0, 0, 0, 1, 1, 2, 20, b'h', 1], // header key extends beyond the record
        vec![0, 0, 0, 1, 1, 2, 2, b'h', 20], // header value extends beyond the record
        vec![0, 0, 0, 1, 1, 0, 0],           // bytes after zero headers
    ] {
        assert_codec_without_progress(batch_with_record_body(&body));
    }
}

#[test]
fn nullable_payloads_and_headers_remain_legal_and_sdk_utf8_validation_is_preserved() {
    for body in [
        vec![0, 0, 0, 1, 1, 0],             // nullable key/value and no headers
        vec![0, 0, 0, 0, 0, 0],             // empty key/value and no headers
        vec![0, 0, 0, 1, 1, 2, 0, 1],       // empty non-null header key and nullable value
        vec![0, 0, 0, 1, 1, 2, 2, b'h', 0], // non-null header key and empty value
    ] {
        let (decoded, progress) =
            convert_fetch_response_with_progress(response(Some(batch_with_record_body(&body))), 7)
                .into_parts();
        assert_eq!(
            decoded.topics[0].partitions[0]
                .data()
                .unwrap()
                .messages
                .len(),
            1
        );
        assert_eq!(progress.next_offset(0, 0), Some(11));
    }
    assert_codec_without_progress(batch_with_record_body(&[0, 0, 0, 1, 1, 2, 2, 255, 1]));
}

#[test]
fn unknown_magic_and_invalid_or_truncated_batch_length_never_produce_progress() {
    let mut magic = encoded_records(KpCompression::None, 10).to_vec();
    magic[16] = 3;
    assert_codec_without_progress(Bytes::from(magic));
    for length in [-1i32, 0, 48, i32::MAX] {
        let mut batch = encoded_records(KpCompression::None, 10).to_vec();
        batch[8..12].copy_from_slice(&length.to_be_bytes());
        assert_codec_without_progress(Bytes::from(batch));
    }
}

#[test]
fn legacy_magic_zero_and_one_keep_the_existing_codec_rejection_without_progress() {
    for magic in [0, 1] {
        // Complete legacy message set: offset, length, IEEE CRC, magic,
        // attributes, optional timestamp, nullable key and a one-byte value.
        let mut message = vec![magic, 0];
        if magic == 1 {
            message.extend_from_slice(&0i64.to_be_bytes());
        }
        message.extend_from_slice(&(-1i32).to_be_bytes());
        message.extend_from_slice(&1i32.to_be_bytes());
        message.push(b'v');
        let mut crc = !0u32;
        for byte in &message {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0u32.wrapping_sub(crc & 1) & 0xedb8_8320);
            }
        }
        let mut bytes = vec![0; 8];
        bytes.extend_from_slice(&i32::try_from(message.len() + 4).unwrap().to_be_bytes());
        bytes.extend_from_slice(&(!crc).to_be_bytes());
        bytes.extend_from_slice(&message);
        assert_codec_without_progress(Bytes::from(bytes));
    }
}

#[cfg(feature = "compression")]
fn empty_compressed_batch(base: i64, last_delta: i32, codec: KpCompression) -> Bytes {
    use kafka_protocol::compression::{Compressor, Gzip, Lz4, Snappy, Zstd};

    let mut payload = BytesMut::new();
    match codec {
        KpCompression::Gzip => Gzip::compress(&mut payload, |_| Ok(())).unwrap(),
        KpCompression::Snappy => Snappy::compress(&mut payload, |_| Ok(())).unwrap(),
        KpCompression::Lz4 => Lz4::compress(&mut payload, |_| Ok(())).unwrap(),
        KpCompression::Zstd => Zstd::compress(&mut payload, |_| Ok(())).unwrap(),
        KpCompression::None => unreachable!("compressed fixture needs a codec"),
    }
    let mut batch = empty_batch(base, last_delta).to_vec();
    batch.extend_from_slice(&payload);
    let batch_length = i32::try_from(batch.len() - 12).unwrap();
    batch[8..12].copy_from_slice(&batch_length.to_be_bytes());
    batch[21..23].copy_from_slice(&(codec as i16).to_be_bytes());
    rewrite_crc(&mut batch);
    Bytes::from(batch)
}

#[cfg(feature = "compression")]
#[test]
fn compressed_control_and_compacted_progress_use_the_same_checked_path() {
    for compression in [
        KpCompression::Gzip,
        KpCompression::Snappy,
        KpCompression::Lz4,
        KpCompression::Zstd,
    ] {
        let records = concatenate(&[
            encode(&[record(3)], compression),
            encode(&[control_record(4, 1)], compression),
            empty_compressed_batch(5, 2, compression),
        ]);
        let (converted, progress) =
            convert_fetch_response_with_progress(response(Some(records)), 7).into_parts();
        assert_eq!(
            converted.topics[0].partitions[0]
                .data()
                .unwrap()
                .messages
                .len(),
            1
        );
        assert_eq!(progress.next_offset(0, 0), Some(8));
    }
}

#[cfg(any(
    feature = "gzip",
    feature = "snappy",
    feature = "lz4",
    feature = "zstd"
))]
fn assert_compressed_framing(compression: KpCompression) {
    let encoded = encode(&[record(10), record(11)], compression);
    let (converted, progress) =
        convert_fetch_response_with_progress(response(Some(encoded.clone())), 7).into_parts();
    assert_eq!(
        converted.topics[0].partitions[0]
            .data()
            .unwrap()
            .messages
            .len(),
        2
    );
    assert_eq!(progress.next_offset(0, 0), Some(12));
    for count in [0i32, 1, 3, -1, i32::MAX] {
        let mut batch = encoded.to_vec();
        batch[57..61].copy_from_slice(&count.to_be_bytes());
        rewrite_crc(&mut batch);
        assert_codec_without_progress(Bytes::from(batch));
    }
}

#[cfg(not(all(
    feature = "gzip",
    feature = "snappy",
    feature = "lz4",
    feature = "zstd"
)))]
fn assert_disabled_compression(compression: KpCompression) {
    let mut bytes = encoded_records(KpCompression::None, 10).to_vec();
    bytes[21..23].copy_from_slice(&(compression as i16).to_be_bytes());
    rewrite_crc(&mut bytes);
    let (converted, progress) =
        convert_fetch_response_with_progress(response(Some(Bytes::from(bytes))), 7).into_parts();
    assert!(matches!(
        converted.topics[0].partitions[0]
            .data()
            .unwrap_err()
            .as_ref(),
        Error::Protocol(crate::error::ProtocolError::UnsupportedCompression)
    ));
    assert_eq!(progress.next_offset(0, 0), None);
}

macro_rules! compression_feature_test {
    ($name:ident, $feature:literal, $codec:expr) => {
        #[test]
        fn $name() {
            #[cfg(feature = $feature)]
            assert_compressed_framing($codec);
            #[cfg(not(feature = $feature))]
            assert_disabled_compression($codec);
        }
    };
}

compression_feature_test!(
    gzip_record_framing_or_feature_error,
    "gzip",
    KpCompression::Gzip
);
compression_feature_test!(
    snappy_record_framing_or_feature_error,
    "snappy",
    KpCompression::Snappy
);
compression_feature_test!(
    lz4_record_framing_or_feature_error,
    "lz4",
    KpCompression::Lz4
);
compression_feature_test!(
    zstd_record_framing_or_feature_error,
    "zstd",
    KpCompression::Zstd
);
