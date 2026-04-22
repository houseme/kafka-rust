pub use super::*;

use std::collections::{HashMap, HashSet};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rustfs_kafka::client::{FetchOffset, KafkaClient, PartitionOffset};
use rustfs_kafka::consumer::Consumer;
use rustfs_kafka::producer::Record;
use rustfs_kafka::producer::{Producer, RequiredAcks};

use rand::Rng;
use tracing::debug;

const RANDOM_MESSAGE_SIZE: usize = 32;

pub fn test_producer() -> Producer {
    let client = new_ready_kafka_client();

    Producer::from_client(client)
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::All)
        .create()
        .unwrap()
}

/// This is to avoid copy/pasting the consumer's config, whether
/// the consumer is built using `from_hosts` or `from_client`.
///
/// TODO: This can go away if we don't use the builder pattern.
macro_rules! test_consumer_config {
    ( $x:expr, $topic:expr, $group:expr ) => {
        $x.with_topic_partitions($topic.to_owned(), &TEST_TOPIC_PARTITIONS)
            .with_group($group.to_owned())
            .with_fallback_offset(rustfs_kafka::consumer::FetchOffset::Latest)
            .with_offset_storage(Some(rustfs_kafka::consumer::GroupOffsetStorage::Kafka))
    };
}

static TEST_GROUP_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn unique_test_group() -> String {
    let seq = TEST_GROUP_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("kafka-rust-tester-{}-{seq}", std::process::id())
}

pub fn unique_test_topic(prefix: &str) -> String {
    let seq = TEST_GROUP_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{seq}", std::process::id())
}

pub fn create_test_topic(topic: &str) {
    let tests_dir = format!("{}/tests", env!("CARGO_MANIFEST_DIR"));
    let topic_spec = format!("{}:{}:1", topic, TEST_TOPIC_PARTITIONS.len());
    const MAX_CREATE_TOPIC_ATTEMPTS: usize = 40;
    let mut last_stdout = String::new();
    let mut last_stderr = String::new();
    let mut created = false;

    for attempt in 0..MAX_CREATE_TOPIC_ATTEMPTS {
        let output = Command::new("docker")
            .args([
                "compose",
                "exec",
                "-T",
                "-e",
                "KAFKA_PORT=9092",
                "-e",
                &format!("KAFKA_CREATE_TOPICS={topic_spec}"),
                "kafka",
                "/usr/bin/create-topics.sh",
            ])
            .current_dir(&tests_dir)
            .output()
            .unwrap_or_else(|err| {
                panic!("failed to spawn docker compose for test topic {topic}: {err}")
            });

        last_stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        last_stderr = String::from_utf8_lossy(&output.stderr).into_owned();

        if output.status.success() {
            created = true;
            break;
        }

        if attempt % 5 == 0 {
            debug!(
                "still waiting to create test topic: topic={}, attempt={}, stderr={}",
                topic,
                attempt,
                last_stderr.trim()
            );
        }
        poll_backoff(500);
    }

    assert!(
        created,
        "failed to create test topic {} via docker compose after {} attempts\nstdout:\n{}\nstderr:\n{}",
        topic, MAX_CREATE_TOPIC_ATTEMPTS, last_stdout, last_stderr
    );

    let mut client = new_ready_kafka_client();

    const MAX_METADATA_ATTEMPTS: usize = 100;
    for attempt in 0..MAX_METADATA_ATTEMPTS {
        client.load_metadata_all().unwrap_or_else(|err| {
            panic!("failed to reload metadata for test topic {topic}: {err}")
        });

        if let Some(partitions) = client.topics().partitions(topic) {
            let mut available = partitions.available_ids().to_vec();
            let mut expected = TEST_TOPIC_PARTITIONS.to_vec();
            available.sort_unstable();
            expected.sort_unstable();
            if available == expected {
                return;
            }
        }

        if attempt % 10 == 0 {
            debug!(
                "still waiting for topic metadata: topic={}, attempt={}",
                topic, attempt
            );
        }
        poll_backoff(50);
    }

    panic!("timed out waiting for topic metadata for {topic}");
}

/// Return a Consumer builder with some defaults
pub fn test_consumer_builder() -> rustfs_kafka::consumer::Builder {
    test_consumer_builder_with_group(TEST_GROUP_NAME)
}

pub fn test_consumer_builder_with_group(group: &str) -> rustfs_kafka::consumer::Builder {
    test_consumer_builder_with_topic_and_group(TEST_TOPIC_NAME, group)
}

pub fn test_consumer_builder_with_topic_and_group(
    topic: &str,
    group: &str,
) -> rustfs_kafka::consumer::Builder {
    test_consumer_config!(
        Consumer::from_client(new_ready_kafka_client()),
        topic,
        group
    )
}

pub fn test_consumer_with_topic_and_group(topic: &str, group: &str) -> Consumer {
    test_consumer_with_client_topic_and_group(new_ready_kafka_client(), topic, group)
}

/// Return a ready Kafka consumer with all default settings
pub fn test_consumer_with_client_topic_and_group(
    mut client: KafkaClient,
    topic: &str,
    group: &str,
) -> Consumer {
    let topics = [topic, TEST_TOPIC_NAME_2];

    // Fetch the latest offsets and commit those so that this consumer
    // is always at the latest offset before being used.
    let latest_offsets = client.fetch_offsets(&topics, FetchOffset::Latest).unwrap();

    debug!("latest_offsets: {:?}", latest_offsets);

    for (topic, partition_offsets) in latest_offsets {
        for po in partition_offsets {
            if po.offset == -1 {
                continue;
            }

            client
                .commit_offset(group, &topic, po.partition, po.offset)
                .unwrap();
        }
    }

    client.load_metadata_all().expect("failed to load metadata");
    let partition_offsets: HashSet<PartitionOffset> = client
        .fetch_group_topic_offset(group, topic)
        .unwrap()
        .into_iter()
        .collect();

    debug!("partition_offsets: {:?}", partition_offsets);

    test_consumer_config!(Consumer::from_client(client), topic, group)
        .create()
        .unwrap()
}

/// Send `num_messages` randomly-generated messages to the given topic with
/// the given producer.
fn send_random_messages(producer: &mut Producer, topic: &str, num_messages: u32) {
    let mut random_message_buf = [0u8; RANDOM_MESSAGE_SIZE];
    let mut rng = rand::rng();

    for _ in 0..num_messages {
        rng.fill_bytes(&mut random_message_buf);
        producer
            .send(&Record::from_value(topic, &random_message_buf[..]))
            .expect("failed to send random message");
    }
}

/// Fetch the committed offsets for this group and topic on the given client.
/// If the offset is -1 (i.e., there isn't an offset committed for a partition),
/// use the given optional default value in place of -1. If the default value is
/// None, -1 is kept.
pub(crate) fn get_group_offsets(
    client: &mut KafkaClient,
    group: &str,
    topic: &str,
    default_offset: Option<i64>,
) -> HashMap<i32, i64> {
    client
        .fetch_group_topic_offset(group, topic)
        .unwrap()
        .iter()
        .map(|po| {
            let offset = if let Some(default) = default_offset {
                if po.offset == -1 { default } else { po.offset }
            } else {
                po.offset
            };

            (po.partition, offset)
        })
        .collect()
}

/// Given two maps that represent committed offsets at some point in time, where
/// `older` represents some earlier point in time, and `newer` represents some
/// later point in time, compute the number of messages committed between
/// `earlier` and `later`—i.e., for each partition, compute the differences
/// `newer.partition.offset` - `older.partition.offset`, and sum them up.
pub(crate) fn diff_group_offsets(older: &HashMap<i32, i64>, newer: &HashMap<i32, i64>) -> i64 {
    newer
        .iter()
        .map(|(partition, new_offset)| {
            let old_offset = older.get(partition).unwrap();
            new_offset - old_offset
        })
        .sum()
}

/// Sleep for `millis` milliseconds between consumer poll attempts.
///
/// `std::thread::sleep` is disallowed project-wide in favour of
/// `RetryPolicy`, but integration test poll-loops are not retry loops —
/// they simply need a short pause so the broker can accumulate the next
/// batch of messages before the consumer fetches again.
#[allow(clippy::disallowed_methods)]
pub(crate) fn poll_backoff(millis: u64) {
    std::thread::sleep(std::time::Duration::from_millis(millis));
}

mod consumer;
mod producer;
