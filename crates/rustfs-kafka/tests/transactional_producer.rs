#![cfg(feature = "integration_tests")]
//! Real-broker transaction tests. The fixture uses the same bootstrap and
//! security environment as test_kafka. Generated isolation=1 Fetch requests
//! verify transaction visibility; the high-level read-uncommitted consumer
//! independently verifies control filtering and forward progress.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{
    ApiKey, FetchRequest, FetchResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::RecordBatchDecoder;
use rustfs_kafka::client::{Compression, FetchOffset, KafkaClient, RetryPolicy, TopicConfig};
#[cfg(any(feature = "security", feature = "security-ring"))]
use rustfs_kafka::client::{SaslConfig, SecurityConfig};
use rustfs_kafka::consumer::Consumer;
use rustfs_kafka::producer::{Producer, Record, RequiredAcks, TransactionalProducer};
const API_VERSION_FETCH: i16 = 12;

const TOPICS: [&str; 2] = ["kafka-rust-txn-test", "kafka-rust-txn-test2"];
type Position = (String, i32);
type VisibleValue = (String, i32, String);

fn new_client() -> KafkaClient {
    if std::env::var_os("KAFKA_TRANSACTION_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
    }
    let mut builder = KafkaClient::builder()
        .with_hosts(vec!["127.0.0.1:9092".to_owned()])
        .with_conn_rw_timeout(5)
        .with_retry_policy(RetryPolicy::Fixed {
            interval: Duration::from_millis(100),
            max_attempts: 100,
        });
    #[cfg(any(feature = "security", feature = "security-ring"))]
    if std::env::var("KAFKA_CLIENT_SECURE").is_ok_and(|value| !value.is_empty()) {
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
        builder = builder.with_security(security);
    }
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
    client.load_metadata_all().unwrap();
    ensure_transaction_topics(&mut client);
    client
}

fn ensure_transaction_topics(client: &mut KafkaClient) {
    let missing: Vec<TopicConfig> = TOPICS
        .iter()
        .filter(|topic| !client.topics().contains(topic))
        .map(|topic| {
            TopicConfig::new(*topic)
                .with_partitions(2)
                .with_replication_factor(1)
                .with_config("cleanup.policy", "delete")
                .with_config("retention.ms", "-1")
        })
        .collect();
    if !missing.is_empty() {
        let response = client
            .create_topics(&missing, Duration::from_secs(10))
            .unwrap();
        assert_eq!(response.results.len(), missing.len());
        for topic in response.results {
            assert!(
                matches!(topic.error_code, 0 | 36),
                "creating transaction fixture {} failed: {:?}",
                topic.name,
                topic
            );
        }
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if TOPICS.iter().all(|topic| {
            client.topics().partitions(topic).is_some_and(|partitions| {
                [0, 1].iter().all(|&partition| {
                    partitions
                        .partition(partition)
                        .is_some_and(|partition| partition.is_available())
                })
            })
        }) {
            // A metadata leader can appear before the broker opens the new partition.
            match client.list_offsets(&TOPICS, FetchOffset::Latest) {
                Ok(offsets) => {
                    assert_eq!(offsets.len(), TOPICS.len());
                    for topic in TOPICS {
                        let partitions = offsets
                            .get(topic)
                            .expect("transaction readiness response missing topic");
                        assert_eq!(partitions.len(), 2);
                        assert!(
                            [0, 1].iter().all(|&expected| {
                                partitions
                                    .iter()
                                    .filter(|partition| {
                                        partition.partition == expected && partition.offset >= 0
                                    })
                                    .count()
                                    == 1
                            }),
                            "transaction readiness response missing a valid partition offset: {topic}: {partitions:?}"
                        );
                    }
                    return;
                }
                Err(rustfs_kafka::error::Error::TopicPartitionError {
                    topic_name,
                    partition_id: 0 | 1,
                    error_code:
                        rustfs_kafka::error::KafkaCode::LeaderNotAvailable
                        | rustfs_kafka::error::KafkaCode::NotLeaderForPartition,
                }) if TOPICS.contains(&topic_name.as_str()) => {}
                Err(error) => panic!("transaction fixture ListOffsets readiness failed: {error:?}"),
            }
        }
        assert!(
            Instant::now() < deadline,
            "transaction fixture partitions did not become ready"
        );
        std::thread::sleep(Duration::from_millis(100));
        client.load_metadata_all().unwrap();
    }
}

fn unique_prefix(label: &str) -> String {
    format!(
        "txn-live-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn starting_offsets(client: &mut KafkaClient) -> HashMap<Position, i64> {
    client
        .fetch_offsets(&TOPICS, FetchOffset::Latest)
        .unwrap()
        .into_iter()
        .flat_map(|(topic, partitions)| {
            partitions
                .into_iter()
                .filter(|partition| partition.partition < 2)
                .map(move |partition| ((topic.clone(), partition.partition), partition.offset))
        })
        .collect()
}

fn transactional_producer(transactional_id: &str) -> TransactionalProducer {
    TransactionalProducer::from_client(new_client())
        .with_transactional_id(transactional_id)
        .with_ack_timeout_ms(10_000)
        .create()
        .unwrap_or_else(|err| panic!("transaction producer setup failed: {err}"))
}

struct Snapshot {
    values: HashSet<VisibleValue>,
    /// Watermarks permit proving an open transaction restricts isolation=1.
    stable_offsets: HashMap<Position, i64>,
    high_watermarks: HashMap<Position, i64>,
    first_offsets: HashMap<Position, i64>,
    transactional_records: usize,
}

/// A Fetch response may end at a log-segment boundary even when its byte
/// budget is ample. Each partition therefore needs a cursor and abort state
/// across pages; a response is not a complete snapshot of its watermark.
fn snapshot(
    client: &mut KafkaClient,
    starts: &HashMap<Position, i64>,
    prefix: &str,
    isolation: i8,
) -> Snapshot {
    let mut snapshot = Snapshot {
        values: HashSet::new(),
        stable_offsets: HashMap::new(),
        high_watermarks: HashMap::new(),
        first_offsets: HashMap::new(),
        transactional_records: 0,
    };
    for ((topic, partition), &start) in starts {
        let mut cursor = start;
        let mut stop_at = None;
        let mut aborted = VecDeque::new();
        let mut seen_aborted = HashSet::new();
        let mut active_aborts = HashSet::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        for page in 0..1_024 {
            assert!(
                Instant::now() < deadline,
                "transaction reader exceeded its bound: topic={topic}, partition={partition}, cursor={cursor}, stop_at={stop_at:?}, page={page}"
            );
            let data = fetch_partition_page(client, topic, *partition, cursor, isolation);
            snapshot
                .stable_offsets
                .insert((topic.clone(), *partition), data.last_stable_offset);
            snapshot
                .high_watermarks
                .insert((topic.clone(), *partition), data.high_watermark);
            let end = *stop_at.get_or_insert(if isolation == 1 {
                data.last_stable_offset
            } else {
                data.high_watermark
            });
            assert!(
                end >= 0,
                "broker did not provide the requested isolation watermark"
            );
            let response_end = if isolation == 1 {
                data.last_stable_offset
            } else {
                data.high_watermark
            };
            assert!(response_end >= 0);
            let page_end = end.min(response_end);
            if cursor >= page_end {
                break;
            }
            if isolation == 1 {
                for transaction in data.aborted_transactions.unwrap_or_default() {
                    let range = (transaction.first_offset, i64::from(transaction.producer_id));
                    // Brokers can repeat an earlier range on a later page. It
                    // must not reactivate a PID whose ABORT marker we consumed.
                    if seen_aborted.insert(range) {
                        aborted.push_back(range);
                    }
                }
                aborted.make_contiguous().sort_unstable();
            }
            let mut records = data.records.unwrap_or_default();
            let mut next_cursor = cursor;
            while !records.is_empty() {
                assert!(
                    records.len() >= 61 && records[16] == 2,
                    "invalid magic=2 record batch header"
                );
                let base_offset = i64::from_be_bytes(records[..8].try_into().unwrap());
                let last_delta = i32::from_be_bytes(records[23..27].try_into().unwrap());
                assert!(base_offset >= 0 && last_delta >= 0);
                let last_offset = base_offset.checked_add(i64::from(last_delta)).unwrap();
                let batch = RecordBatchDecoder::decode(&mut records).unwrap();
                // Record count can be zero after compaction. The header still
                // spans offsets, and control batches must also advance cursor.
                next_cursor = next_cursor
                    .max(last_offset.checked_add(1).unwrap())
                    .min(page_end);
                for record in batch.records {
                    assert!(
                        record.offset >= base_offset && record.offset <= last_offset,
                        "decoded record escaped batch offsets: topic={topic}, partition={partition}, record_offset={}, base_offset={base_offset}, last_offset={last_offset}, codec={:?}",
                        record.offset,
                        batch.compression
                    );
                    if record.offset >= page_end {
                        // A response can carry batches beyond this observation's
                        // conservative boundary. Neither data nor markers in that
                        // range are evidence of a stable outcome. Keep the
                        // cursor below this page's boundary and observe again.
                        eprintln!(
                            "transaction reader deferred a record beyond its page boundary: topic={topic}, partition={partition}, offset={}, response_lso={}, response_hw={}, cursor={cursor}, frozen_end={end}, page_end={page_end}, pid={}, epoch={}, transactional={}, control={}, active_aborted={active_aborts:?}, pending_aborted={aborted:?}, batch_base={base_offset}, batch_last={last_offset}, codec={:?}",
                            record.offset,
                            data.last_stable_offset,
                            data.high_watermark,
                            record.producer_id,
                            record.producer_epoch,
                            record.transactional,
                            record.control,
                            batch.compression
                        );
                        continue;
                    }
                    if record.offset < cursor {
                        continue;
                    }
                    while aborted
                        .front()
                        .is_some_and(|&(first, _)| first <= record.offset)
                    {
                        active_aborts.insert(aborted.pop_front().unwrap().1);
                    }
                    if record.control {
                        let key = record.key.as_deref().expect("control record key");
                        assert_eq!(key.len(), 4);
                        let marker = i16::from_be_bytes(key[2..4].try_into().unwrap());
                        assert!(matches!(marker, 0 | 1), "unexpected transaction marker");
                        if marker == 0 {
                            active_aborts.remove(&record.producer_id);
                        }
                        continue;
                    }
                    if isolation == 1
                        && record.transactional
                        && active_aborts.contains(&record.producer_id)
                    {
                        continue;
                    }
                    if let Some(value) = record
                        .value
                        .as_deref()
                        .and_then(|value| std::str::from_utf8(value).ok())
                        && value.starts_with(prefix)
                    {
                        assert!(
                            record.offset < page_end && record.offset < end,
                            "reader must never deliver beyond a sampled or frozen watermark"
                        );
                        assert!(
                            record.transactional,
                            "transactional send wrote an ordinary record"
                        );
                        snapshot
                            .first_offsets
                            .entry((topic.clone(), *partition))
                            .or_insert(record.offset);
                        if snapshot
                            .values
                            .insert((topic.clone(), *partition, value.to_owned()))
                        {
                            snapshot.transactional_records += 1;
                        }
                    }
                }
            }
            if next_cursor <= cursor {
                // A bounded partial observation is retried by the outer strict
                // value assertion; do not spin or pretend the watermark was read.
                eprintln!(
                    "transaction fetch made no progress: topic={topic}, partition={partition}, cursor={cursor}, end={end}, page={page}"
                );
                break;
            }
            cursor = next_cursor.min(end);
            if cursor >= end {
                break;
            }
            assert!(page < 1_023, "transaction reader exceeded its page bound");
        }
    }
    snapshot
}

fn fetch_partition_page(
    client: &mut KafkaClient,
    topic: &str,
    partition: i32,
    offset: i64,
    isolation: i8,
) -> kafka_protocol::messages::fetch_response::PartitionData {
    let host = client
        .topics()
        .partitions(topic)
        .unwrap()
        .partition(partition)
        .unwrap()
        .leader()
        .unwrap()
        .host()
        .to_owned();
    let correlation = client.next_correlation_id();
    let request = FetchRequest::default()
        .with_replica_id((-1).into())
        .with_max_wait_ms(100)
        .with_min_bytes(1)
        .with_max_bytes(4 * 1024 * 1024)
        .with_isolation_level(isolation)
        .with_session_epoch(-1)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(StrBytes::from_string(topic.to_owned()).into())
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(partition)
                        .with_fetch_offset(offset)
                        .with_partition_max_bytes(4 * 1024 * 1024),
                ]),
        ]);
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Fetch as i16)
        .with_request_api_version(API_VERSION_FETCH)
        .with_correlation_id(correlation)
        .with_client_id(Some(StrBytes::from_string(client.client_id().to_owned())));
    let mut frame = BytesMut::new();
    frame.extend_from_slice(&[0; 4]);
    header
        .encode(&mut frame, FetchRequest::header_version(API_VERSION_FETCH))
        .unwrap();
    request.encode(&mut frame, API_VERSION_FETCH).unwrap();
    let frame_size = i32::try_from(frame.len() - 4).unwrap();
    frame[..4].copy_from_slice(&frame_size.to_be_bytes());
    let conn = client.get_conn_mut(&host).unwrap();
    conn.send(&frame).unwrap();
    let mut size = [0; 4];
    conn.read_exact(&mut size).unwrap();
    let mut payload = conn
        .read_exact_alloc(u64::try_from(i32::from_be_bytes(size)).unwrap())
        .unwrap();
    let response_header = ResponseHeader::decode(
        &mut payload,
        FetchResponse::header_version(API_VERSION_FETCH),
    )
    .unwrap();
    assert_eq!(response_header.correlation_id, correlation);
    let mut response = FetchResponse::decode(&mut payload, API_VERSION_FETCH).unwrap();
    assert!(payload.is_empty());
    assert_eq!(response.error_code, 0);
    assert_eq!(response.responses.len(), 1);
    let mut topic_response = response.responses.pop().unwrap();
    assert_eq!(topic_response.topic.as_str(), topic);
    assert_eq!(topic_response.partitions.len(), 1);
    let data = topic_response.partitions.pop().unwrap();
    assert_eq!(data.partition_index, partition);
    assert_eq!(data.error_code, 0);
    data
}

fn write_phase(
    producer: &mut TransactionalProducer,
    starts: &HashMap<Position, i64>,
    prefix: &str,
    phase: &str,
) -> HashSet<VisibleValue> {
    starts
        .keys()
        .map(|(topic, partition)| {
            let value = format!("{prefix}-{phase}-{topic}-{partition}");
            producer
                .send(
                    &Record::from_key_value(topic, prefix, value.as_str())
                        .with_partition(*partition),
                )
                .unwrap();
            (topic.clone(), *partition, value)
        })
        .collect()
}

fn wait_for_values(
    client: &mut KafkaClient,
    starts: &HashMap<Position, i64>,
    prefix: &str,
    expected: &HashSet<VisibleValue>,
) -> Snapshot {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let current = snapshot(client, starts, prefix, 1);
        if &current.values == expected {
            return current;
        }
        assert!(
            Instant::now() < deadline,
            "read-committed values did not converge: actual={:?}, expected={expected:?}",
            current.values
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_raw_appends(
    client: &mut KafkaClient,
    starts: &HashMap<Position, i64>,
    prefix: &str,
    required: &HashSet<VisibleValue>,
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let raw = snapshot(client, starts, prefix, 0);
        if required.is_subset(&raw.values) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "transaction case must have appended records: required={required:?}, actual={:?}, starts={starts:?}, stable={:?}, high_watermarks={:?}",
            raw.values,
            raw.stable_offsets,
            raw.high_watermarks
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn consumer_from_positions(starts: &HashMap<Position, i64>) -> Consumer {
    let mut subscriptions: HashMap<&str, Vec<i32>> = HashMap::new();
    for (topic, partition) in starts.keys() {
        subscriptions.entry(topic).or_default().push(*partition);
    }
    let mut builder = Consumer::from_client(new_client())
        .with_fallback_offset(FetchOffset::Latest)
        .with_fetch_max_wait_time(Duration::from_millis(100));
    for (topic, partitions) in subscriptions {
        builder = builder.with_topic_partitions(topic.to_owned(), &partitions);
    }
    let mut consumer = builder.create().unwrap();
    assert!(consumer.group().is_empty());
    for ((topic, partition), &offset) in starts {
        consumer.seek(topic, *partition, offset).unwrap();
    }
    consumer
}

fn assert_uncommitted_consumer_values(
    starts: &HashMap<Position, i64>,
    prefix: &str,
    expected: &HashSet<VisibleValue>,
) {
    let mut consumer = consumer_from_positions(starts);
    let mut actual = HashSet::new();
    let mut last_business_offsets: HashMap<Position, i64> = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    for page in 0..256 {
        assert!(
            Instant::now() < deadline,
            "high-level transaction reader timed out"
        );
        let sets = consumer.poll().unwrap();
        for set in sets.iter_ref() {
            for message in set.messages() {
                assert_ne!(
                    message.key.as_ref(),
                    &[0, 0, 0, 0],
                    "ABORT marker leaked to application"
                );
                assert_ne!(
                    message.key.as_ref(),
                    &[0, 0, 0, 1],
                    "COMMIT marker leaked to application"
                );
                let value = std::str::from_utf8(&message.value).unwrap();
                if value.starts_with(prefix) {
                    let position = (set.topic().to_owned(), set.partition());
                    let value = (position.0.clone(), position.1, value.to_owned());
                    assert!(
                        expected.contains(&value),
                        "unexpected transaction payload: {value:?}"
                    );
                    assert!(
                        actual.insert(value),
                        "transaction payload was fetched twice"
                    );
                    last_business_offsets.insert(position, message.offset);
                }
            }
        }
        for (topic, partition) in starts.keys() {
            assert_eq!(consumer.last_consumed_message(topic, *partition), None);
        }
        if &actual == expected {
            break;
        }
        assert!(
            page < 255,
            "high-level transaction reader exceeded its page bound"
        );
    }
    assert_eq!(
        &actual, expected,
        "read-uncommitted must retain aborted business payloads"
    );
    assert_eq!(last_business_offsets.len(), starts.len());
    for ((topic, partition), offset) in last_business_offsets {
        consumer.consume_message(&topic, partition, offset).unwrap();
        assert_eq!(
            consumer.last_consumed_message(&topic, partition),
            Some(offset)
        );
    }
}

fn assert_marker_only_consumer_progress(client: &mut KafkaClient, prefix: &str) {
    // The concurrent fencing case writes only TOPICS[0]/0. This partition's
    // last append is the lifecycle case's final COMMIT marker.
    let topic = TOPICS[1];
    let partition = 1;
    let latest = starting_offsets(client)[&(topic.to_owned(), partition)];
    let marker_offset = latest.checked_sub(1).unwrap();
    let data = fetch_partition_page(client, topic, partition, marker_offset, 0);
    let mut records = data.records.clone().unwrap();
    let mut found = false;
    while !records.is_empty() {
        for record in RecordBatchDecoder::decode(&mut records).unwrap().records {
            if record.offset == marker_offset {
                assert!(record.control);
                assert_eq!(record.key.as_deref(), Some(&[0, 0, 0, 1][..]));
                found = true;
            }
        }
    }
    assert!(found, "Latest-1 must identify a real COMMIT control record");
    let generated = FetchResponse::default().with_responses(vec![
        kafka_protocol::messages::fetch_response::FetchableTopicResponse::default()
            .with_topic(StrBytes::from_string(topic.to_owned()).into())
            .with_partitions(vec![data]),
    ]);
    let (decoded, progress) =
        rustfs_kafka::client::fetch_kp::convert_fetch_response_with_progress(generated, 0)
            .into_parts();
    assert!(
        decoded.topics[0].partitions[0]
            .data()
            .unwrap()
            .messages
            .is_empty()
    );
    assert_eq!(progress.next_offset(0, 0), Some(latest));

    let mut consumer = consumer_from_positions(&HashMap::from([(
        (topic.to_owned(), partition),
        marker_offset,
    )]));
    assert!(
        consumer.poll().unwrap().is_empty(),
        "marker-only Fetch must be successful and empty"
    );
    assert_eq!(consumer.last_consumed_message(topic, partition), None);
    let value = format!("{prefix}-ordinary-after-marker");
    let mut producer = Producer::from_client(new_client())
        .with_required_acks(RequiredAcks::All)
        .with_ack_timeout(Duration::from_secs(10))
        .create()
        .unwrap();
    producer
        .send(&Record::from_key_value(topic, prefix, value.as_str()).with_partition(partition))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let message_offset = loop {
        assert!(
            Instant::now() < deadline,
            "ordinary payload after marker did not become readable"
        );
        let sets = consumer.poll().unwrap();
        if sets.is_empty() {
            continue;
        }
        let mut sets = sets.iter_ref();
        let set = sets.next().unwrap();
        assert!(sets.next().is_none());
        assert_eq!(set.topic(), topic);
        assert_eq!(set.partition(), partition);
        let messages = set.messages();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].offset, latest);
        assert_eq!(messages[0].value.as_ref(), value.as_bytes());
        break messages[0].offset;
    };
    assert_eq!(consumer.last_consumed_message(topic, partition), None);
    consumer
        .consume_message(topic, partition, message_offset)
        .unwrap();
    assert_eq!(
        consumer.last_consumed_message(topic, partition),
        Some(message_offset)
    );
    assert!(
        consumer.poll().unwrap().is_empty(),
        "the new business payload must be fetched only once"
    );
}

#[test]
fn transaction_commit_abort_and_reuse_have_read_committed_visibility() {
    let prefix = unique_prefix("lifecycle");
    let mut reader = new_client();
    let starts = starting_offsets(&mut reader);
    assert_eq!(
        starts.len(),
        4,
        "fixture requires two partitions in both topics"
    );
    let mut producer = transactional_producer(&prefix);
    // Empty transactions must complete locally without invalid EndTxn requests.
    producer.begin().unwrap();
    producer.commit().unwrap();
    producer.begin().unwrap();
    producer.abort().unwrap();

    producer.begin().unwrap();
    assert!(producer.begin().is_err());
    let committed = write_phase(&mut producer, &starts, &prefix, "commit");
    let uncommitted = snapshot(&mut reader, &starts, &prefix, 0);
    assert_eq!(uncommitted.values, committed);
    assert_eq!(uncommitted.transactional_records, 4);
    let pending = snapshot(&mut reader, &starts, &prefix, 1);
    assert!(pending.values.is_empty());
    eprintln!(
        "pending transaction diagnostics: starts={starts:?}, stable={:?}, high_watermarks={:?}, first_record_offsets={:?}, raw_values={:?}",
        pending.stable_offsets,
        pending.high_watermarks,
        uncommitted.first_offsets,
        uncommitted.values
    );
    for position in starts.keys() {
        let first_uncommitted = uncommitted.first_offsets[position];
        assert!(first_uncommitted >= starts[position]);
        // Other transactions can finish between the Latest query and this
        // producer's first write, appending control markers after captured start.
        // LSO must still stop at this transaction's own first unstable record.
        assert!(
            pending.stable_offsets[position] <= first_uncommitted,
            "pending LSO crossed its first unstable record: starts={starts:?}, stable={:?}, high_watermarks={:?}, first_record_offsets={:?}, raw_values={:?}",
            pending.stable_offsets,
            pending.high_watermarks,
            uncommitted.first_offsets,
            uncommitted.values
        );
    }
    producer.commit().unwrap();
    wait_for_values(&mut reader, &starts, &prefix, &committed);

    producer.begin().unwrap();
    let aborted = write_phase(&mut producer, &starts, &prefix, "abort");
    wait_for_raw_appends(&mut reader, &starts, &prefix, &aborted);
    producer.abort().unwrap();
    wait_for_raw_appends(&mut reader, &starts, &prefix, &aborted);

    // Same producer ID/epoch after abort: filtering the whole producer ID would
    // incorrectly hide these committed values, and resetting sequence would fail.
    producer.begin().unwrap();
    let resumed = write_phase(&mut producer, &starts, &prefix, "after-abort");
    producer.commit().unwrap();
    let expected = committed.union(&resumed).cloned().collect();
    let visible = wait_for_values(&mut reader, &starts, &prefix, &expected);
    assert!(visible.values.is_disjoint(&aborted));
    assert_eq!(visible.transactional_records, 8);
    let uncommitted_expected = expected.union(&aborted).cloned().collect();
    assert_uncommitted_consumer_values(&starts, &prefix, &uncommitted_expected);
    assert_marker_only_consumer_progress(&mut reader, &prefix);
    eprintln!("transaction proof prefix={prefix}, committed=8, aborted=4, topics={TOPICS:?}");
}

#[test]
fn producer_fencing_poison_prevents_retry_and_replacement_commits() {
    let prefix = unique_prefix("fencing");
    let mut reader = new_client();
    let mut starts = starting_offsets(&mut reader);
    starts.retain(|(topic, partition), _| topic == TOPICS[0] && *partition == 0);
    assert_eq!(starts.len(), 1);
    let mut original = transactional_producer(&prefix);
    original.begin().unwrap();
    let fenced = write_phase(&mut original, &starts, &prefix, "fenced");
    let mut replacement = transactional_producer(&prefix);
    let record = Record::from_value(TOPICS[0], "must-not-retry").with_partition(0);
    assert!(original.send(&record).is_err());
    assert!(original.begin().is_err());
    assert!(original.send(&record).is_err());
    assert!(original.commit().is_err());
    assert!(original.abort().is_err());
    replacement.begin().unwrap();
    let expected = write_phase(&mut replacement, &starts, &prefix, "replacement");
    replacement.commit().unwrap();
    let visible = wait_for_values(&mut reader, &starts, &prefix, &expected);
    assert!(visible.values.is_disjoint(&fenced));
    wait_for_raw_appends(&mut reader, &starts, &prefix, &fenced);
    eprintln!(
        "fencing proof prefix={prefix}, committed=1, fenced=1, topic={}",
        TOPICS[0]
    );
}
