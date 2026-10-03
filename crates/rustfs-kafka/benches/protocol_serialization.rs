use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kafka_protocol::messages::{
    ApiKey, FetchResponse, ProduceRequest, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::{
    Compression as KpCompression, Record as KpRecord, RecordBatchDecoder, RecordBatchEncoder,
    RecordEncodeOptions, TimestampType,
};
use rustfs_kafka::client::{decode_response_payload, encode_request_frame};
use rustfs_kafka::producer::{Compression, Record};
use std::hint::black_box;

fn bench_record_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_creation");
    for size in [64, 1024, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let value = vec![0u8; size];
            b.iter(|| {
                black_box(Record::from_key_value(
                    "bench-topic",
                    black_box(b"key"),
                    black_box(&value),
                ));
            });
        });
    }
    group.finish();
}

fn bench_compression_enum(c: &mut Criterion) {
    let compressions = [
        Compression::NONE,
        Compression::GZIP,
        Compression::SNAPPY,
        Compression::LZ4,
        Compression::ZSTD,
    ];

    c.bench_function("compression_enum_operations", |b| {
        b.iter(|| {
            for comp in &compressions {
                let _disc = black_box(*comp as i32);
                let _debug = black_box(format!("{:?}", comp));
            }
        });
    });
}

// These measure the generated codecs and complete wire-frame serialization.
// They exclude network I/O and the private adapters' grouping/payload copies.
const CODEC_CASES: [(usize, usize); 4] = [(1, 64), (64, 1024), (256, 1024), (16, 65536)];
const PRODUCE_VERSION: i16 = 9;
const FETCH_VERSION: i16 = 12;

fn bench_generated_produce_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("generated_produce_encode");
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Produce as i16)
        .with_request_api_version(PRODUCE_VERSION)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_static_str("bench-client")));
    for (count, size) in CODEC_CASES {
        let records = codec_records(count, size);
        // Report value payload bytes per second; count is included in the id.
        group.throughput(Throughput::Bytes(u64::try_from(count * size).unwrap()));
        group.bench_function(BenchmarkId::new(count.to_string(), size), |b| {
            b.iter(|| {
                let request = generated_produce_request(black_box(&records));
                black_box(encode_request_frame(&header, &request, PRODUCE_VERSION).unwrap());
            });
        });
    }
    group.finish();
}

fn bench_generated_fetch_decoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("generated_fetch_decode");
    for (count, size) in CODEC_CASES {
        let records = codec_records(count, size);
        let payload = generated_fetch_payload(&records);
        assert_eq!(decode_generated_fetch(payload.clone()), count);
        group.throughput(Throughput::Elements(u64::try_from(count).unwrap()));
        group.bench_function(BenchmarkId::new(count.to_string(), size), |b| {
            b.iter(|| black_box(decode_generated_fetch(black_box(payload.clone()))));
        });
    }
    group.finish();
}

fn codec_records(count: usize, size: usize) -> Vec<KpRecord> {
    let value = Bytes::from(vec![42; size]);
    (0..count)
        .map(|index| KpRecord {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i64::try_from(index).unwrap(),
            sequence: i32::try_from(index).unwrap() - 1,
            timestamp: 0,
            key: Some(Bytes::from_static(b"key")),
            value: Some(value.clone()),
            headers: indexmap::IndexMap::default(),
        })
        .collect()
}

fn encode_records(records: &[KpRecord]) -> Bytes {
    let mut bytes = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut bytes,
        records,
        &RecordEncodeOptions {
            version: 2,
            compression: KpCompression::None,
        },
    )
    .unwrap();
    bytes.freeze()
}

fn generated_produce_request(records: &[KpRecord]) -> ProduceRequest {
    ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(30_000)
        .with_topic_data(vec![
            kafka_protocol::messages::produce_request::TopicProduceData::default()
                .with_name(TopicName::from(StrBytes::from_static_str("bench-topic")))
                .with_partition_data(vec![
                    kafka_protocol::messages::produce_request::PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(encode_records(records))),
                ]),
        ])
}

fn generated_fetch_payload(records: &[KpRecord]) -> Bytes {
    let mut batches = BytesMut::new();
    for batch in records.chunks(8) {
        batches.extend_from_slice(&encode_records(batch));
    }
    let response = FetchResponse::default().with_responses(vec![
        kafka_protocol::messages::fetch_response::FetchableTopicResponse::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("bench-topic")))
            .with_partitions(vec![
                kafka_protocol::messages::fetch_response::PartitionData::default()
                    .with_partition_index(0)
                    .with_high_watermark(i64::try_from(records.len()).unwrap())
                    .with_records(Some(batches.freeze())),
            ]),
    ]);
    let mut payload = BytesMut::new();
    ResponseHeader::default()
        .with_correlation_id(1)
        .encode(&mut payload, FetchResponse::header_version(FETCH_VERSION))
        .unwrap();
    response.encode(&mut payload, FETCH_VERSION).unwrap();
    payload.freeze()
}

fn decode_generated_fetch(payload: Bytes) -> usize {
    let response = decode_response_payload::<FetchResponse>(payload, FETCH_VERSION).unwrap();
    let mut count = 0;
    for topic in response.responses {
        for partition in topic.partitions {
            if let Some(mut records) = partition.records {
                while !records.is_empty() {
                    let batch = RecordBatchDecoder::decode(&mut records).unwrap();
                    count += batch.records.len();
                    black_box(batch);
                }
            }
        }
    }
    count
}

criterion_group!(
    benches,
    bench_record_serialization,
    bench_compression_enum,
    bench_generated_produce_encoding,
    bench_generated_fetch_decoding
);
criterion_main!(benches);
