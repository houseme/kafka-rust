#![cfg(feature = "integration_tests")]
//! Consumer offset validation against a real Kafka broker.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rustfs_kafka::client::{Compression, FetchOffset, KafkaClient, TopicConfig};
#[cfg(any(feature = "security", feature = "security-ring"))]
use rustfs_kafka::client::{SaslConfig, SecurityConfig};
use rustfs_kafka::consumer::Consumer;
use rustfs_kafka::error::{Error, KafkaCode};
use rustfs_kafka::producer::{Producer, Record, RequiredAcks};

#[test]
fn seek_into_record_batch_returns_only_requested_offsets_once() {
    let fixture = OffsetTopic::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut client = new_client();
    wait_for_topic(&mut client, &fixture.name, deadline);
    let mut producer = new_producer(client);
    let values: [&[u8]; 6] = [
        b"offset-0",
        b"offset-1",
        b"offset-2",
        b"offset-3",
        b"offset-4",
        b"offset-5",
    ];
    let records: Vec<_> = values
        .iter()
        .map(|&value| Record::from_value(&fixture.name, value).with_partition(0))
        .collect();

    // A single send_all to one partition places all six records in one RecordBatch.
    const MAX_PRODUCE_ATTEMPTS: usize = 5;
    for attempt in 1..=MAX_PRODUCE_ATTEMPTS {
        assert!(
            Instant::now() < deadline,
            "fixture Produce deadline expired"
        );
        let confirms = producer
            .send_all(&records)
            .expect("fixture Produce failed; only explicit leader rejection ACKs may be replayed");
        assert_eq!(confirms.len(), 1);
        assert_eq!(confirms[0].topic, fixture.name);
        assert_eq!(confirms[0].partition_confirms.len(), 1);
        assert_eq!(confirms[0].partition_confirms[0].partition, 0);
        match confirms[0].partition_confirms[0].offset {
            Ok(offset) => {
                assert_eq!(offset, 0, "fixture batch must be appended exactly once");
                break;
            }
            Err(code @ (KafkaCode::LeaderNotAvailable | KafkaCode::NotLeaderForPartition)) => {
                // Only explicit leader errors enter this path. The readiness
                // check requires an empty log before repeating the batch.
                assert!(
                    attempt < MAX_PRODUCE_ATTEMPTS && Instant::now() < deadline,
                    "fixture leader rejected Produce after {attempt} attempts: {code:?}"
                );
                fixture_backoff();
                let mut client = producer.into_client();
                wait_for_topic(&mut client, &fixture.name, deadline);
                producer = new_producer(client);
            }
            Err(code) => panic!("fixture Produce rejected the batch without retry: {code:?}"),
        }
    }

    let mut consumer = Consumer::from_client(producer.into_client())
        .with_topic_partitions(fixture.name.clone(), &[0])
        .with_fallback_offset(FetchOffset::Earliest)
        .with_fetch_max_wait_time(Duration::from_millis(100))
        .create()
        .unwrap();
    assert_invalid_consumed_offsets(&mut consumer, &fixture.name, None);
    consumer.seek(&fixture.name, 0, 3).unwrap();

    let messages = consumer.poll().unwrap();
    let mut received = Vec::new();
    for message_set in messages.iter() {
        assert_eq!(message_set.topic(), fixture.name);
        assert_eq!(message_set.partition(), 0);
        received.extend(
            message_set
                .messages()
                .iter()
                .map(|message| (message.offset, message.value.to_vec())),
        );
        consumer.consume_messageset(&message_set).unwrap();
    }
    assert_eq!(
        received,
        vec![
            (3, values[3].to_vec()),
            (4, values[4].to_vec()),
            (5, values[5].to_vec()),
        ],
        "seeking into a batch must discard records before the requested offset"
    );
    assert_eq!(consumer.last_consumed_message(&fixture.name, 0), Some(5));
    assert_invalid_consumed_offsets(&mut consumer, &fixture.name, Some(5));
    assert!(
        consumer.poll().unwrap().is_empty(),
        "the next poll must not redeliver the batch"
    );

    // Keep this consumer group-less: the boundary marker must not reach a real group.
    assert!(consumer.group().is_empty());
    consumer
        .consume_message(&fixture.name, 0, i64::MAX - 1)
        .unwrap();
    assert_eq!(
        consumer.last_consumed_message(&fixture.name, 0),
        Some(i64::MAX - 1)
    );
    assert_invalid_consumed_offsets(&mut consumer, &fixture.name, Some(i64::MAX - 1));
}

#[test]
fn future_time_no_match_is_preserved_by_client_and_rejected_by_consumer() {
    let fixture = OffsetTopic::new();
    let mut client = new_client();
    wait_for_topic(
        &mut client,
        &fixture.name,
        Instant::now() + Duration::from_secs(15),
    );
    let future_timestamp = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
    .checked_add(3_600_000)
    .unwrap();
    let fallback = FetchOffset::ByTime(future_timestamp);
    let offsets = client.list_offsets(&[&fixture.name], fallback).unwrap();
    assert_eq!(offsets.len(), 1);
    let partitions = offsets
        .get(&fixture.name)
        .expect("future time response missing fixture topic");
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].partition, 0);
    assert_eq!(partitions[0].offset, -1);
    assert_eq!(partitions[0].time, -1);

    let Err(error) = Consumer::from_client(client)
        .with_topic_partitions(fixture.name.clone(), &[0])
        .with_fallback_offset(fallback)
        .create()
    else {
        panic!("a time query with no matching record must reject consumer initialization");
    };
    assert!(
        matches!(&error, Error::Kafka(KafkaCode::OffsetOutOfRange)),
        "expected OffsetOutOfRange for a time query with no match, got {error:?}"
    );
    assert!(!error.is_retriable());
}

fn assert_invalid_consumed_offsets(consumer: &mut Consumer, topic: &str, expected: Option<i64>) {
    for offset in [-2, -1, i64::MIN, i64::MAX] {
        let error = consumer.consume_message(topic, 0, offset).unwrap_err();
        assert!(
            matches!(error, Error::Config(_)),
            "consuming invalid offset {offset} must return Config, got {error:?}"
        );
        assert_eq!(
            consumer.last_consumed_message(topic, 0),
            expected,
            "rejecting offset {offset} must preserve the consumed state"
        );
    }
}

fn new_producer(client: KafkaClient) -> Producer {
    Producer::from_client(client)
        .with_required_acks(RequiredAcks::All)
        .with_ack_timeout(Duration::from_secs(5))
        .create()
        .unwrap()
}

fn new_client() -> KafkaClient {
    let builder = KafkaClient::builder()
        .with_hosts(vec!["127.0.0.1:9092".to_owned()])
        .with_conn_rw_timeout(5);
    #[cfg(any(feature = "security", feature = "security-ring"))]
    let builder = if std::env::var("KAFKA_CLIENT_SECURE").is_ok_and(|value| !value.is_empty()) {
        let mut security = SecurityConfig::new().with_hostname_verification(false);
        let mechanism = std::env::var("KAFKA_CLIENT_SASL_MECHANISM").unwrap_or_default();
        if !mechanism.is_empty() {
            security = security.with_sasl(SaslConfig::new(
                mechanism,
                std::env::var("KAFKA_CLIENT_SASL_USERNAME").unwrap_or_else(|_| "test".to_owned()),
                std::env::var("KAFKA_CLIENT_SASL_PASSWORD")
                    .unwrap_or_else(|_| "test-pass".to_owned()),
            ));
        }
        builder.with_security(security)
    } else {
        builder
    };
    let mut client = builder.build();
    client.set_compression(
        match std::env::var("KAFKA_CLIENT_COMPRESSION")
            .unwrap_or_default()
            .to_uppercase()
            .as_str()
        {
            "" | "NONE" => Compression::NONE,
            "GZIP" => Compression::GZIP,
            "SNAPPY" => Compression::SNAPPY,
            "LZ4" => Compression::LZ4,
            "ZSTD" => Compression::ZSTD,
            other => panic!("unknown compression: {other}"),
        },
    );
    client
}

struct OffsetTopic {
    name: String,
    admin: KafkaClient,
}

impl OffsetTopic {
    fn new() -> Self {
        let mut fixture = Self {
            name: format!(
                "kafka-rust-consumer-offsets-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ),
            admin: new_client(),
        };
        fixture.admin.load_metadata_all().unwrap();
        let response = fixture
            .admin
            .create_topics(
                &[TopicConfig::new(&fixture.name)
                    .with_partitions(1)
                    .with_config("cleanup.policy", "delete")],
                Duration::from_secs(10),
            )
            .unwrap();
        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response.results[0].error_code, 0,
            "creating consumer offset fixture failed: {:?}",
            response.results[0]
        );
        fixture
    }
}

impl Drop for OffsetTopic {
    fn drop(&mut self) {
        let _ = self
            .admin
            .delete_topics(&[&self.name], Duration::from_secs(5));
    }
}

fn wait_for_topic(client: &mut KafkaClient, topic: &str, deadline: Instant) {
    loop {
        assert!(
            Instant::now() < deadline,
            "consumer offset fixture did not become ready"
        );
        client.load_metadata(&[topic]).unwrap();
        if client.topics().partitions(topic).is_some_and(|partitions| {
            partitions
                .partition(0)
                .is_some_and(|partition| partition.is_available())
        }) {
            // Metadata can name a leader before that broker has opened the partition.
            match client.list_offsets(&[topic], FetchOffset::Latest) {
                Ok(offsets) => {
                    assert_eq!(offsets.len(), 1);
                    let partitions = offsets
                        .get(topic)
                        .expect("fixture offset response missing topic");
                    assert_eq!(partitions.len(), 1);
                    assert_eq!(partitions[0].partition, 0);
                    assert_eq!(partitions[0].offset, 0, "fixture must still be empty");
                    return;
                }
                Err(Error::TopicPartitionError {
                    topic_name,
                    partition_id: 0,
                    error_code: KafkaCode::LeaderNotAvailable | KafkaCode::NotLeaderForPartition,
                }) if topic_name == topic => {}
                Err(error) => panic!("fixture ListOffsets readiness failed: {error:?}"),
            }
        }
        fixture_backoff();
    }
}

#[allow(clippy::disallowed_methods)]
fn fixture_backoff() {
    std::thread::sleep(Duration::from_millis(100));
}
