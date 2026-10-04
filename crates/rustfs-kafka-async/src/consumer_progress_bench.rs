//! Manual CPU measurement of the actual consumer progress pipeline.
//! This test excludes setup, reset, decoding, network I/O, and commit.

use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{FetchResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use rustfs_kafka::client::fetch_kp::{
    FetchProgress, OwnedFetchResponse, convert_fetch_response_with_progress,
};
use rustfs_kafka::error::Result;

use super::{AsyncConsumer, AsyncConsumerMode, FETCH_PARTITION_MAX_BYTES, NativeConsumer};
use crate::AsyncKafkaClient;

const WARMUP: usize = 256;
const DEFAULT_SAMPLES: usize = 9;
const ITERATIONS: u32 = 1_024;
const MESSAGE_OFFSET: i64 = 41;
const NEXT_OFFSET: i64 = MESSAGE_OFFSET + 1;
const VARIANT: &str = "candidate";

struct ProgressFixture {
    native: Box<NativeConsumer>,
    responses: Vec<(OwnedFetchResponse, FetchProgress)>,
    keys: Vec<(String, i32)>,
}

fn progress_fixture(
    runtime: &tokio::runtime::Runtime,
    topic_count: usize,
    partitions_per_topic: usize,
) -> ProgressFixture {
    let topics: Vec<String> = (0..topic_count)
        .map(|index| format!("bench-topic-{index:03}"))
        .collect();
    let mut native = runtime.block_on(async {
        let client = AsyncKafkaClient::new(Vec::new()).await.unwrap();
        let consumer = AsyncConsumer::from_client(client, "bench-group".to_owned(), topics.clone())
            .await
            .unwrap();
        let AsyncConsumerMode::Native(native) = consumer.mode;
        native
    });
    let mut keys = Vec::with_capacity(topic_count * partitions_per_topic);
    let records = benchmark_record_batch();
    let response_topics = topics
        .into_iter()
        .map(|topic| {
            let partitions = (0..partitions_per_topic)
                .map(|partition| {
                    let partition = i32::try_from(partition).unwrap();
                    keys.push((topic.clone(), partition));
                    seed_progress(&mut native, &topic, partition);
                    native.leaders.insert(
                        (topic.clone(), partition),
                        "bench-broker.invalid:9092".to_owned(),
                    );
                    PartitionData::default()
                        .with_partition_index(partition)
                        .with_high_watermark(NEXT_OFFSET)
                        .with_records(Some(records.clone()))
                })
                .collect();
            FetchableTopicResponse::default()
                .with_topic(TopicName::from(StrBytes::from_string(topic)))
                .with_partitions(partitions)
        })
        .collect();
    let (response, progress) = convert_fetch_response_with_progress(
        FetchResponse::default().with_responses(response_topics),
        7,
    )
    .into_parts();
    let mut message_count = 0;
    for (topic_index, topic) in response.topics.iter().enumerate() {
        for (partition_index, partition) in topic.partitions.iter().enumerate() {
            let messages = &partition.data().unwrap().messages;
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].offset, MESSAGE_OFFSET);
            assert_eq!(messages[0].key.as_ref(), b"k");
            assert_eq!(messages[0].value.as_ref(), b"v");
            assert_eq!(
                progress.next_offset(topic_index, partition_index),
                Some(NEXT_OFFSET)
            );
            message_count += messages.len();
        }
    }
    assert_eq!(message_count, keys.len());
    ProgressFixture {
        native,
        responses: vec![(response, progress)],
        keys,
    }
}

fn benchmark_record_batch() -> Bytes {
    let record = Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: MESSAGE_OFFSET,
        sequence: -1,
        timestamp: 0,
        key: Some(Bytes::from_static(b"k")),
        value: Some(Bytes::from_static(b"v")),
        headers: Default::default(),
    };
    let mut bytes = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut bytes,
        &[record],
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    bytes.freeze()
}

fn verify_progress(fixture: &ProgressFixture, expected: i64) -> i64 {
    assert_eq!(
        progress_entries(&fixture.native),
        (fixture.keys.len(), fixture.keys.len())
    );
    assert_eq!(fixture.native.leaders.len(), fixture.keys.len());
    let mut checksum = 0;
    for key in &fixture.keys {
        let (topic, partition) = key;
        assert!(fixture.native.leaders.contains_key(key));
        let (offset, dirty) = progress_values(&fixture.native, key);
        assert_eq!(offset, Some(expected), "offset for {topic}:{partition}");
        assert_eq!(
            dirty,
            Some(expected),
            "dirty offset for {topic}:{partition}"
        );
        checksum += offset.unwrap() + dirty.unwrap();
    }
    checksum
}

fn measure_case(
    runtime: &tokio::runtime::Runtime,
    topics: usize,
    partitions: usize,
    samples: usize,
) {
    let mut fixture = progress_fixture(runtime, topics, partitions);
    let requested: Vec<(&str, i32, i64, i32)> = fixture
        .keys
        .iter()
        .map(|(topic, partition)| (topic.as_str(), *partition, 0, FETCH_PARTITION_MAX_BYTES))
        .collect();
    let mut timings = Vec::with_capacity(samples);
    assert_eq!(verify_progress(&fixture, 0), 0);
    for _ in 0..WARMUP {
        reset_progress(&mut fixture.native);
        run_progress(&mut fixture.native, &mut fixture.responses, &requested).unwrap();
    }
    verify_progress(&fixture, NEXT_OFFSET);
    for sample in 0..samples {
        reset_progress(&mut fixture.native);
        assert_eq!(verify_progress(&fixture, 0), 0);
        let mut elapsed = Duration::ZERO;
        for _ in 0..ITERATIONS {
            // Mutate existing values only. Reset and all fixture allocations
            // stay outside the measured validation/publication interval.
            reset_progress(&mut fixture.native);
            let start = Instant::now();
            let result = run_progress(
                black_box(&mut fixture.native),
                black_box(&mut fixture.responses),
                black_box(&requested),
            );
            elapsed += start.elapsed();
            black_box(result).expect("benchmark must publish a valid nonempty response");
        }
        let checksum = verify_progress(&fixture, NEXT_OFFSET);
        assert_eq!(
            checksum,
            i64::try_from(fixture.keys.len()).unwrap() * NEXT_OFFSET * 2
        );
        timings.push(elapsed);
        eprintln!(
            "progress_cpu_sample variant={VARIANT} topics={topics} partitions={partitions} tp={} sample={sample} iterations={ITERATIONS} measured_ns={} checksum={checksum}",
            fixture.keys.len(),
            elapsed.as_nanos()
        );
    }
    let median = median_ns(&mut timings);
    let iterations = u128::from(ITERATIONS);
    let per_partition = iterations * u128::from(u32::try_from(fixture.keys.len()).unwrap());
    eprintln!(
        "progress_cpu_summary variant={VARIANT} topics={topics} partitions={partitions} tp={} warmup={WARMUP} samples={samples} iterations={ITERATIONS} sample_median_ns={median} ns_per_publish={} ns_per_tp={}",
        fixture.keys.len(),
        decimal_rate(median, iterations),
        decimal_rate(median, per_partition)
    );
}

fn median_ns(samples: &mut [Duration]) -> u128 {
    samples.sort_unstable();
    let middle = samples.len() / 2;
    if samples.len().is_multiple_of(2) {
        (samples[middle - 1].as_nanos() + samples[middle].as_nanos()) / 2
    } else {
        samples[middle].as_nanos()
    }
}

fn decimal_rate(numerator: u128, denominator: u128) -> String {
    let whole = numerator / denominator;
    let fraction = (numerator % denominator) * 1_000 / denominator;
    format!("{whole}.{fraction:03}")
}

#[test]
#[ignore = "manual release CPU benchmark; run serially after building both variants"]
fn native_consumer_progress_cpu_bench() {
    assert!(
        !black_box(cfg!(debug_assertions)),
        "run this ignored test with --release"
    );
    let samples = std::env::var("KAFKA_PROGRESS_BENCH_SAMPLES").map_or(DEFAULT_SAMPLES, |value| {
        value
            .parse::<usize>()
            .expect("sample count must be a positive integer")
    });
    assert!(samples > 0, "sample count must be positive");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for (topics, partitions) in [(10, 100), (1, 1_000)] {
        measure_case(&runtime, topics, partitions, samples);
    }
}

// Only these adapters differ between revisions. They seed/reset/inspect state
// and dispatch to the actual production functions; they do not reproduce the
// consumer's validation, trimming, staging, or publication algorithms.
fn seed_progress(native: &mut NativeConsumer, topic: &str, partition: i32) {
    native
        .offsets
        .entry(topic.to_owned())
        .or_default()
        .insert(partition, 0);
    native
        .dirty_offsets
        .entry(topic.to_owned())
        .or_default()
        .insert(partition, 0);
}

fn reset_progress(native: &mut NativeConsumer) {
    for partitions in native.offsets.values_mut() {
        for value in partitions.values_mut() {
            *value = 0;
        }
    }
    for partitions in native.dirty_offsets.values_mut() {
        for value in partitions.values_mut() {
            *value = 0;
        }
    }
}

fn progress_entries(native: &NativeConsumer) -> (usize, usize) {
    (
        native
            .offsets
            .values()
            .map(std::collections::HashMap::len)
            .sum(),
        native
            .dirty_offsets
            .values()
            .map(std::collections::HashMap::len)
            .sum(),
    )
}

fn progress_values(native: &NativeConsumer, key: &(String, i32)) -> (Option<i64>, Option<i64>) {
    (
        native
            .offsets
            .get(key.0.as_str())
            .and_then(|partitions| partitions.get(&key.1))
            .copied(),
        native
            .dirty_offsets
            .get(key.0.as_str())
            .and_then(|partitions| partitions.get(&key.1))
            .copied(),
    )
}

fn run_progress(
    native: &mut NativeConsumer,
    responses: &mut [(OwnedFetchResponse, FetchProgress)],
    requested: &[(&str, i32, i64, i32)],
) -> Result<()> {
    for (response, progress) in responses.iter_mut() {
        super::validate_and_trim_fetch_response(response, progress, requested)?;
    }
    super::publish_response_offsets(&mut native.offsets, &mut native.dirty_offsets, responses)
}
