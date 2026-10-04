//! TCP coverage for verified batch cursors, including hidden application data.

use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use super::tests::{checked, read_request, reply};
use super::*;

const TOPIC: &str = "batch-progress";
const GROUP: &str = "batch-progress-group";

fn topic() -> TopicName {
    StrBytes::from_static_str(TOPIC).into()
}

fn native(consumer: &mut AsyncConsumer) -> &mut NativeConsumer {
    let AsyncConsumerMode::Native(native) = &mut consumer.mode;
    native
}

async fn consumer(brokers: &[std::net::SocketAddr], offset: i64) -> AsyncConsumer {
    let mut consumer = checked(
        AsyncConsumer::builder(vec![brokers[0].to_string()])
            .with_group(GROUP.to_owned())
            .with_topic(TOPIC.to_owned())
            .with_native_retry_attempts(3)
            .with_native_retry_backoff(Duration::ZERO)
            .build(),
    )
    .await
    .unwrap();
    let state = native(&mut consumer);
    for (partition, broker) in brokers.iter().enumerate() {
        let partition = i32::try_from(partition).unwrap();
        state
            .leaders
            .insert((TOPIC.to_owned(), partition), broker.to_string());
        insert_topic_offset(&mut state.offsets, TOPIC, partition, offset);
    }
    state.coordinator = Some(brokers[0].to_string());
    consumer
}

fn record(offset: i64, control: bool) -> Record {
    Record {
        transactional: control,
        control,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id: if control { 7 } else { -1 },
        producer_epoch: if control { 0 } else { -1 },
        timestamp_type: TimestampType::Creation,
        offset,
        // Preserve offset - sequence so the SDK encodes ordinary records
        // together. Tests that corrupt one batch must not accidentally create
        // independent valid batches for each offset.
        sequence: i32::try_from(offset).unwrap(),
        timestamp: 0,
        // A real commit control marker, rather than an application value.
        key: Some(if control {
            Bytes::from_static(&[0, 0, 0, 1])
        } else {
            Bytes::from_static(b"key")
        }),
        value: Some(if control {
            Bytes::from_static(&[0; 6])
        } else {
            Bytes::from_static(b"value")
        }),
        headers: Default::default(),
    }
}

fn encode(records: &[Record]) -> Bytes {
    let mut bytes = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut bytes,
        records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
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
    let mut batch = encode(&[record(base, false)])[..61].to_vec();
    batch[8..12].copy_from_slice(&49i32.to_be_bytes());
    batch[23..27].copy_from_slice(&last_delta.to_be_bytes());
    batch[27..35].copy_from_slice(&(-1i64).to_be_bytes());
    batch[57..61].copy_from_slice(&0i32.to_be_bytes());
    rewrite_crc(&mut batch);
    Bytes::from(batch)
}

fn response(partition: i32, records: Option<Bytes>, error: i16) -> FetchResponse {
    FetchResponse::default().with_responses(vec![
        FetchableTopicResponse::default()
            .with_topic(topic())
            .with_partitions(vec![
                PartitionData::default()
                    .with_partition_index(partition)
                    .with_high_watermark(i64::MAX)
                    .with_error_code(error)
                    .with_records(records),
            ]),
    ])
}

async fn fetch(socket: &mut TcpStream, partition: i32, offset: i64) -> RequestHeader {
    let (header, request) =
        read_request::<FetchRequest>(socket, ApiKey::Fetch, API_VERSION_FETCH).await;
    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].topic, topic());
    assert_eq!(request.topics[0].partitions.len(), 1);
    assert_eq!(request.topics[0].partitions[0].partition, partition);
    assert_eq!(request.topics[0].partitions[0].fetch_offset, offset);
    header
}

async fn commit(socket: &mut TcpStream, offset: i64) {
    let (header, request) = read_request::<OffsetCommitRequest>(
        socket,
        ApiKey::OffsetCommit,
        API_VERSION_OFFSET_COMMIT,
    )
    .await;
    assert_eq!(request.group_id.as_str(), GROUP);
    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].name, topic());
    assert_eq!(request.topics[0].partitions.len(), 1);
    assert_eq!(request.topics[0].partitions[0].partition_index, 0);
    assert_eq!(request.topics[0].partitions[0].committed_offset, offset);
    reply(
        socket,
        &header,
        API_VERSION_OFFSET_COMMIT,
        OffsetCommitResponse::default().with_topics(vec![
            OffsetCommitResponseTopic::default()
                .with_name(topic())
                .with_partitions(vec![
                    OffsetCommitResponsePartition::default().with_partition_index(0),
                ]),
        ]),
    )
    .await;
}

async fn assert_progress(records: Option<Bytes>, start: i64, next: i64, delivered: &[i64]) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = checked(listener.accept()).await.unwrap();
        let header = fetch(&mut socket, 0, start).await;
        reply(
            &mut socket,
            &header,
            API_VERSION_FETCH,
            response(0, records, 0),
        )
        .await;
        if next > start {
            commit(&mut socket, next).await;
        }
        let header = fetch(&mut socket, 0, next).await;
        reply(
            &mut socket,
            &header,
            API_VERSION_FETCH,
            response(0, None, 0),
        )
        .await;
    });
    let mut consumer = consumer(&[addr], start).await;
    let messages = checked(consumer.poll()).await.unwrap();
    let actual: Vec<_> = messages
        .iter()
        .flat_map(|set| {
            set.messages()
                .iter()
                .map(|message| message.offset)
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(actual, delivered);
    for set in messages.iter() {
        for message in set.messages() {
            assert_eq!(message.key.as_ref(), b"key");
            assert_eq!(message.value.as_ref(), b"value");
        }
    }
    assert_eq!(native(&mut consumer).offsets[TOPIC][&0], next);
    if next > start {
        assert_eq!(native(&mut consumer).dirty_offsets[TOPIC][&0], next);
    } else {
        assert!(native(&mut consumer).dirty_offsets.is_empty());
    }
    checked(consumer.commit()).await.unwrap();
    assert!(native(&mut consumer).dirty_offsets.is_empty());
    assert!(checked(consumer.poll()).await.unwrap().is_empty());
    assert_eq!(native(&mut consumer).offsets[TOPIC][&0], next);
    assert!(native(&mut consumer).dirty_offsets.is_empty());
    checked(server).await.unwrap();
}

#[tokio::test]
async fn empty_compacted_batch_advances_next_fetch_and_commit_without_messages() {
    assert_progress(Some(empty_batch(7, 4)), 7, 12, &[]).await;
}

#[tokio::test]
async fn control_only_batches_are_hidden_but_advance_fetch_and_commit() {
    assert_progress(Some(encode(&[record(3, true), record(4, true)])), 3, 5, &[]).await;
}

#[tokio::test]
async fn compacted_tail_uses_header_cursor_beyond_the_last_visible_record() {
    let mut records = encode(&[record(3, false)]).to_vec();
    records[23..27].copy_from_slice(&6i32.to_be_bytes());
    rewrite_crc(&mut records);
    assert_progress(Some(Bytes::from(records)), 3, 10, &[3]).await;
}

#[tokio::test]
async fn offset_gaps_and_hidden_tail_controls_advance_the_verified_cursor() {
    assert_progress(
        Some(encode(&[
            record(3, false),
            record(6, false),
            record(7, true),
        ])),
        3,
        8,
        &[3, 6],
    )
    .await;
}

#[tokio::test]
async fn old_hidden_batches_and_absent_batches_never_rewind_or_use_high_watermark() {
    assert_progress(Some(empty_batch(0, 4)), 7, 7, &[]).await;
    assert_progress(None, 7, 7, &[]).await;
    assert_progress(Some(Bytes::new()), 7, 7, &[]).await;
}

#[derive(Clone, Copy)]
enum LaterFailure {
    Codec,
    Broker,
    Cancel,
}

async fn assert_hidden_progress_is_atomic(failure: LaterFailure) {
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let brokers = [first.local_addr().unwrap(), second.local_addr().unwrap()];
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let blocked = Arc::new(Notify::new());
    let mut servers = Vec::new();
    for (index, listener) in [first, second].into_iter().enumerate() {
        let fetch_count = Arc::clone(&fetch_count);
        let blocked = Arc::clone(&blocked);
        servers.push(tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let partition = i32::try_from(index).unwrap();
            let header = fetch(&mut socket, partition, 3).await;
            if fetch_count.fetch_add(1, Ordering::Relaxed) == 0 {
                reply(
                    &mut socket,
                    &header,
                    API_VERSION_FETCH,
                    response(partition, Some(empty_batch(3, 8)), 0),
                )
                .await;
            } else {
                match failure {
                    LaterFailure::Codec => {
                        let malformed = encode(&[record(5, false), record(3, false)]);
                        reply(
                            &mut socket,
                            &header,
                            API_VERSION_FETCH,
                            response(partition, Some(malformed), 0),
                        )
                        .await;
                    }
                    LaterFailure::Broker => {
                        reply(
                            &mut socket,
                            &header,
                            API_VERSION_FETCH,
                            response(partition, None, 29),
                        )
                        .await;
                    }
                    LaterFailure::Cancel => {
                        blocked.notify_one();
                        std::future::pending::<()>().await;
                    }
                }
            }
        }));
    }
    let mut consumer = consumer(&brokers, 3).await;
    // Existing dirty progress must also survive a failed or cancelled poll.
    let original = HashMap::from([(TOPIC.to_owned(), HashMap::from([(0, 3), (1, 3)]))]);
    native(&mut consumer).dirty_offsets = original.clone();
    match failure {
        LaterFailure::Cancel => {
            checked(async {
                tokio::select! {
                    result = consumer.poll() => panic!("poll returned before cancellation: {result:?}"),
                    () = blocked.notified() => {}
                }
            })
            .await;
        }
        LaterFailure::Codec => {
            assert!(matches!(
                checked(consumer.poll()).await,
                Err(Error::Protocol(ProtocolError::Codec))
            ));
            assert_eq!(consumer.native_error_stats().unwrap().total_errors, 1);
        }
        LaterFailure::Broker => {
            assert!(matches!(
                checked(consumer.poll()).await,
                Err(Error::Kafka(KafkaCode::TopicAuthorizationFailed))
            ));
        }
    }
    assert_eq!(fetch_count.load(Ordering::Relaxed), 2);
    assert_eq!(native(&mut consumer).offsets, original);
    assert_eq!(native(&mut consumer).dirty_offsets, original);
    for server in servers {
        if matches!(failure, LaterFailure::Cancel) && !server.is_finished() {
            server.abort();
            assert!(checked(server).await.unwrap_err().is_cancelled());
        } else {
            checked(server).await.unwrap();
        }
    }
}

#[tokio::test]
async fn later_broker_failure_preserves_pending_hidden_progress_and_dirty() {
    assert_hidden_progress_is_atomic(LaterFailure::Broker).await;
}

#[tokio::test]
async fn later_valid_crc_malformed_order_is_terminal_and_preserves_all_progress() {
    assert_hidden_progress_is_atomic(LaterFailure::Codec).await;
}

#[tokio::test]
async fn cancelling_later_broker_does_not_publish_empty_batch_progress() {
    assert_hidden_progress_is_atomic(LaterFailure::Cancel).await;
}

#[tokio::test]
async fn valid_crc_bad_batch_bounds_take_precedence_over_a_retriable_partition_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = checked(listener.accept()).await.unwrap();
        let (header, request) =
            read_request::<FetchRequest>(&mut socket, ApiKey::Fetch, API_VERSION_FETCH).await;
        assert_eq!(request.topics[0].partitions.len(), 2);
        let mut batch = encode(&[record(3, false), record(5, false)]).to_vec();
        assert_eq!(
            usize::try_from(i32::from_be_bytes(batch[8..12].try_into().unwrap())).unwrap() + 12,
            batch.len(),
            "bounds fixture must contain exactly one batch"
        );
        batch[23..27].copy_from_slice(&0i32.to_be_bytes());
        rewrite_crc(&mut batch);
        reply(
            &mut socket,
            &header,
            API_VERSION_FETCH,
            FetchResponse::default().with_responses(vec![
                FetchableTopicResponse::default()
                    .with_topic(topic())
                    .with_partitions(vec![
                        PartitionData::default()
                            .with_partition_index(0)
                            .with_error_code(6),
                        PartitionData::default()
                            .with_partition_index(1)
                            .with_records(Some(Bytes::from(batch))),
                    ]),
            ]),
        )
        .await;
    });
    let mut consumer = consumer(&[addr], 3).await;
    let state = native(&mut consumer);
    state
        .leaders
        .insert((TOPIC.to_owned(), 1), addr.to_string());
    insert_topic_offset(&mut state.offsets, TOPIC, 1, 3);
    let original = state.offsets.clone();
    assert!(matches!(
        checked(consumer.poll()).await,
        Err(Error::Protocol(ProtocolError::Codec))
    ));
    assert_eq!(consumer.native_error_stats().unwrap().total_errors, 1);
    assert_eq!(native(&mut consumer).offsets, original);
    assert!(native(&mut consumer).dirty_offsets.is_empty());
    assert!(!native(&mut consumer).metadata_refresh_needed);
    checked(server).await.unwrap();
}
