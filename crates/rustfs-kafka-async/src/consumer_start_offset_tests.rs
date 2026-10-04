//! TCP regression coverage for atomic, broker-batched start-position lookup.

use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{Buf, Bytes};
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::protocol::{Decodable, HeaderVersion};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};

use super::tests::{checked, reply};
use super::*;

const TOPIC_A: &str = "start-a";
const TOPIC_B: &str = "start-b";
const GROUP: &str = "start-group";

#[derive(Clone, Copy)]
struct Partition {
    topic: &'static str,
    partition: i32,
    broker: usize,
    committed: i64,
    fallback: i64,
    existing: Option<i64>,
}

const PARTITIONS: [Partition; 7] = [
    Partition {
        topic: TOPIC_A,
        partition: 0,
        broker: 0,
        committed: 11,
        fallback: 11,
        existing: None,
    },
    Partition {
        topic: TOPIC_A,
        partition: 1,
        broker: 0,
        committed: -1,
        fallback: 21,
        existing: None,
    },
    Partition {
        topic: TOPIC_A,
        partition: 2,
        broker: 1,
        committed: -1,
        fallback: 31,
        existing: None,
    },
    Partition {
        topic: TOPIC_B,
        partition: 0,
        broker: 0,
        committed: -1,
        fallback: 41,
        existing: None,
    },
    Partition {
        topic: TOPIC_B,
        partition: 1,
        broker: 1,
        committed: 51,
        fallback: 51,
        existing: None,
    },
    Partition {
        topic: TOPIC_B,
        partition: 2,
        broker: 1,
        committed: -1,
        fallback: i64::MAX,
        existing: None,
    },
    Partition {
        topic: TOPIC_A,
        partition: 9,
        broker: 0,
        committed: 77,
        fallback: 77,
        existing: Some(77),
    },
];

#[derive(Clone, Copy)]
enum Fault {
    None,
    MissingTopic,
    MissingPartition,
    DuplicateTopic,
    DuplicatePartition,
    ExtraTopic,
    ExtraPartition,
    NegativeOffset,
    UnknownOffset,
    KafkaError,
    MalformedWithRetriableError,
    Block,
}

struct Context {
    hosts: [std::net::SocketAddr; 2],
    timestamp: i64,
    fault: Fault,
    all_committed: bool,
    offset_fetches: AtomicUsize,
    list_offsets: AtomicUsize,
    metadata: AtomicUsize,
    fetches: AtomicUsize,
    blocked: Notify,
}

impl Context {
    fn missing(&self) -> Vec<Partition> {
        PARTITIONS
            .iter()
            .copied()
            .filter(|partition| partition.existing.is_none())
            .collect()
    }

    fn expected(&self) -> TopicOffsets {
        let mut offsets: TopicOffsets = HashMap::new();
        for partition in PARTITIONS {
            let offset = partition.existing.unwrap_or(if partition.committed >= 0 {
                partition.committed
            } else {
                partition.fallback
            });
            offsets
                .entry(partition.topic.to_owned())
                .or_default()
                .insert(partition.partition, offset);
        }
        offsets
    }

    fn sorted_keys(partitions: impl IntoIterator<Item = Partition>) -> Vec<(String, i32)> {
        let mut keys: Vec<_> = partitions
            .into_iter()
            .map(|partition| (partition.topic.to_owned(), partition.partition))
            .collect();
        keys.sort_unstable();
        keys
    }

    fn partition(topic: &str, partition: i32) -> Partition {
        *PARTITIONS
            .iter()
            .find(|candidate| candidate.topic == topic && candidate.partition == partition)
            .unwrap()
    }
}

enum Request {
    OffsetFetch(OffsetFetchRequest),
    ListOffsets(ListOffsetsRequest),
    Metadata(MetadataRequest),
    Fetch(FetchRequest),
    Coordinator(FindCoordinatorRequest),
}

async fn read_request(socket: &mut TcpStream) -> Option<(RequestHeader, Request)> {
    let size = match socket.read_i32().await {
        Ok(size) => size,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            return None;
        }
        Err(error) => panic!("mock request read failed: {error}"),
    };
    let mut frame = vec![0; usize::try_from(size).unwrap()];
    checked(socket.read_exact(&mut frame)).await.unwrap();
    let mut frame = Bytes::from(frame);
    let key = i16::from_be_bytes([frame[0], frame[1]]);
    let version = i16::from_be_bytes([frame[2], frame[3]]);
    let header_version = match key {
        9 => OffsetFetchRequest::header_version(version),
        2 => ListOffsetsRequest::header_version(version),
        3 => MetadataRequest::header_version(version),
        1 => FetchRequest::header_version(version),
        10 => FindCoordinatorRequest::header_version(version),
        _ => panic!("unexpected mock API key {key}"),
    };
    let header = RequestHeader::decode(&mut frame, header_version).unwrap();
    let request = match key {
        9 => Request::OffsetFetch(OffsetFetchRequest::decode(&mut frame, version).unwrap()),
        2 => Request::ListOffsets(ListOffsetsRequest::decode(&mut frame, version).unwrap()),
        3 => Request::Metadata(MetadataRequest::decode(&mut frame, version).unwrap()),
        1 => Request::Fetch(FetchRequest::decode(&mut frame, version).unwrap()),
        _ => Request::Coordinator(FindCoordinatorRequest::decode(&mut frame, version).unwrap()),
    };
    assert!(!frame.has_remaining());
    Some((header, request))
}

fn list_reply(
    request: &ListOffsetsRequest,
    context: &Context,
    fault: Fault,
) -> ListOffsetsResponse {
    let topics = request
        .topics
        .iter()
        .map(|topic| {
            ListOffsetsTopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            let expected =
                                Context::partition(topic.name.as_str(), partition.partition_index);
                            ListOffsetsPartitionResponse::default()
                                .with_partition_index(partition.partition_index)
                                .with_timestamp(if context.timestamp >= 0 {
                                    context.timestamp
                                } else {
                                    -1
                                })
                                .with_offset(expected.fallback)
                        })
                        .collect(),
                )
        })
        .collect();
    let mut response = ListOffsetsResponse::default().with_topics(topics);
    match fault {
        Fault::None | Fault::Block => {}
        Fault::MissingTopic => {
            response.topics.pop();
        }
        Fault::MissingPartition => {
            response.topics[0].partitions.pop();
        }
        Fault::DuplicateTopic => response.topics.push(response.topics[0].clone()),
        Fault::DuplicatePartition => {
            let partition = response.topics[0].partitions[0].clone();
            response.topics[0].partitions.push(partition);
        }
        Fault::ExtraTopic => {
            let mut topic = response.topics[0].clone();
            topic.name = TopicName::from(StrBytes::from_static_str("extra-start-topic"));
            response.topics.push(topic);
        }
        Fault::ExtraPartition => response.topics[0].partitions.push(
            ListOffsetsPartitionResponse::default()
                .with_partition_index(99)
                .with_offset(0),
        ),
        Fault::NegativeOffset => response.topics[0].partitions[0].offset = -2,
        Fault::UnknownOffset => {
            response.topics[0].partitions[0].offset = -1;
            response.topics[0].partitions[0].timestamp = -1;
        }
        Fault::KafkaError => {
            let partition = &mut response.topics[0].partitions[0];
            partition.error_code = 6;
            partition.offset = -2; // Ignored payload of the explicit Kafka error.
        }
        Fault::MalformedWithRetriableError => {
            response.topics[0].partitions[0].offset = -2;
            response.topics[1].partitions[0].error_code = 6;
            response.topics[1].partitions[0].offset = -2;
        }
    }
    response
}

async fn connection(mut socket: TcpStream, broker: usize, context: Arc<Context>) {
    while let Some((header, request)) = read_request(&mut socket).await {
        match request {
            Request::OffsetFetch(request) => {
                assert_eq!(header.request_api_version, API_VERSION_OFFSET_FETCH);
                assert_eq!(broker, 0);
                assert_eq!(request.group_id.as_str(), GROUP);
                context.offset_fetches.fetch_add(1, Ordering::Relaxed);
                let mut requested = Vec::new();
                for topic in request.topics.as_ref().unwrap() {
                    requested.extend(
                        topic
                            .partition_indexes
                            .iter()
                            .map(|&partition| (topic.name.to_string(), partition)),
                    );
                }
                requested.sort_unstable();
                assert_eq!(requested, Context::sorted_keys(context.missing()));
                let topics = request
                    .topics
                    .unwrap()
                    .into_iter()
                    .map(|topic| {
                        OffsetFetchResponseTopic::default()
                            .with_name(topic.name.clone())
                            .with_partitions(
                                topic
                                    .partition_indexes
                                    .into_iter()
                                    .map(|partition| {
                                        let expected =
                                            Context::partition(topic.name.as_str(), partition);
                                        OffsetFetchResponsePartition::default()
                                            .with_partition_index(partition)
                                            .with_committed_offset(if context.all_committed {
                                                expected.fallback
                                            } else {
                                                expected.committed
                                            })
                                    })
                                    .collect(),
                            )
                    })
                    .collect();
                reply(
                    &mut socket,
                    &header,
                    API_VERSION_OFFSET_FETCH,
                    OffsetFetchResponse::default().with_topics(topics),
                )
                .await;
            }
            Request::ListOffsets(request) => {
                assert_eq!(header.request_api_version, API_VERSION_LIST_OFFSETS);
                let index = context.list_offsets.fetch_add(1, Ordering::Relaxed);
                let mut requested = Vec::new();
                for topic in &request.topics {
                    for partition in &topic.partitions {
                        assert_eq!(partition.timestamp, context.timestamp);
                        assert_eq!(
                            Context::partition(topic.name.as_str(), partition.partition_index)
                                .broker,
                            broker
                        );
                        requested.push((topic.name.to_string(), partition.partition_index));
                    }
                }
                requested.sort_unstable();
                assert_eq!(
                    requested,
                    Context::sorted_keys(context.missing().into_iter().filter(|partition| {
                        partition.broker == broker && partition.committed == -1
                    }))
                );
                let fault = if index == 1 {
                    context.fault
                } else {
                    Fault::None
                };
                if matches!(fault, Fault::Block) {
                    context.blocked.notify_one();
                    std::future::pending::<()>().await;
                }
                reply(
                    &mut socket,
                    &header,
                    API_VERSION_LIST_OFFSETS,
                    list_reply(&request, &context, fault),
                )
                .await;
            }
            Request::Metadata(request) => {
                assert_eq!(header.request_api_version, API_VERSION_METADATA);
                context.metadata.fetch_add(1, Ordering::Relaxed);
                let mut requested: Vec<_> = request
                    .topics
                    .unwrap()
                    .into_iter()
                    .map(|topic| topic.name.unwrap().to_string())
                    .collect();
                requested.sort_unstable();
                assert_eq!(requested, vec![TOPIC_A.to_owned(), TOPIC_B.to_owned()]);
                let brokers = context
                    .hosts
                    .iter()
                    .enumerate()
                    .map(|(node, host)| {
                        MetadataResponseBroker::default()
                            .with_node_id(BrokerId::from(i32::try_from(node).unwrap()))
                            .with_host(StrBytes::from_string(host.ip().to_string()))
                            .with_port(i32::from(host.port()))
                    })
                    .collect();
                let topics = [TOPIC_A, TOPIC_B]
                    .into_iter()
                    .map(|topic| {
                        MetadataResponseTopic::default()
                            .with_name(Some(TopicName::from(StrBytes::from_static_str(topic))))
                            .with_partitions(
                                PARTITIONS
                                    .iter()
                                    .filter(|partition| partition.topic == topic)
                                    .map(|partition| {
                                        MetadataResponsePartition::default()
                                            .with_partition_index(partition.partition)
                                            .with_leader_id(BrokerId::from(
                                                i32::try_from(partition.broker).unwrap(),
                                            ))
                                    })
                                    .collect(),
                            )
                    })
                    .collect();
                reply(
                    &mut socket,
                    &header,
                    API_VERSION_METADATA,
                    MetadataResponse::default()
                        .with_brokers(brokers)
                        .with_topics(topics),
                )
                .await;
            }
            Request::Fetch(request) => {
                assert_eq!(header.request_api_version, API_VERSION_FETCH);
                context.fetches.fetch_add(1, Ordering::Relaxed);
                let expected = context.expected();
                let responses = request
                    .topics
                    .into_iter()
                    .map(|topic| {
                        let partitions = topic
                            .partitions
                            .into_iter()
                            .map(|partition| {
                                assert!(partition.fetch_offset >= 0);
                                assert_eq!(
                                    partition.fetch_offset,
                                    expected[topic.topic.as_str()][&partition.partition]
                                );
                                PartitionData::default()
                                    .with_partition_index(partition.partition)
                                    .with_high_watermark(partition.fetch_offset)
                                    .with_records(None)
                            })
                            .collect();
                        FetchableTopicResponse::default()
                            .with_topic(topic.topic)
                            .with_partitions(partitions)
                    })
                    .collect();
                reply(
                    &mut socket,
                    &header,
                    API_VERSION_FETCH,
                    FetchResponse::default().with_responses(responses),
                )
                .await;
            }
            Request::Coordinator(request) => {
                assert_eq!(request.key.as_str(), GROUP);
                reply(
                    &mut socket,
                    &header,
                    API_VERSION_FIND_COORDINATOR,
                    FindCoordinatorResponse::default()
                        .with_host(StrBytes::from_string(context.hosts[0].ip().to_string()))
                        .with_port(i32::from(context.hosts[0].port())),
                )
                .await;
            }
        }
    }
}

async fn broker(listener: TcpListener, index: usize, context: Arc<Context>) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, _) = accepted.unwrap();
                connections.spawn(connection(socket, index, Arc::clone(&context)));
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => { result.unwrap(); }
        }
    }
}

struct Fixture {
    consumer: AsyncConsumer,
    context: Arc<Context>,
    servers: Vec<JoinHandle<()>>,
}

impl Fixture {
    async fn new(mode: FetchOffset, fault: Fault, all_committed: bool) -> Self {
        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hosts = [first.local_addr().unwrap(), second.local_addr().unwrap()];
        let context = Arc::new(Context {
            hosts,
            timestamp: match mode {
                FetchOffset::Earliest => -2,
                FetchOffset::Latest => -1,
                FetchOffset::ByTime(timestamp) => timestamp,
            },
            fault,
            all_committed,
            offset_fetches: AtomicUsize::new(0),
            list_offsets: AtomicUsize::new(0),
            metadata: AtomicUsize::new(0),
            fetches: AtomicUsize::new(0),
            blocked: Notify::new(),
        });
        let servers = [first, second]
            .into_iter()
            .enumerate()
            .map(|(index, listener)| tokio::spawn(broker(listener, index, Arc::clone(&context))))
            .collect();
        let mut consumer = checked(
            AsyncConsumer::builder(vec![hosts[0].to_string()])
                .with_group(GROUP.to_owned())
                .with_topics(vec![TOPIC_A.to_owned(), TOPIC_B.to_owned()])
                .with_fallback_offset(mode)
                .with_native_retry_attempts(2)
                .with_native_retry_backoff(Duration::ZERO)
                .build(),
        )
        .await
        .unwrap();
        let AsyncConsumerMode::Native(native) = &mut consumer.mode;
        native.coordinator = Some(hosts[0].to_string());
        for partition in PARTITIONS {
            native.leaders.insert(
                (partition.topic.to_owned(), partition.partition),
                hosts[partition.broker].to_string(),
            );
            if let Some(offset) = partition.existing {
                insert_topic_offset(
                    &mut native.offsets,
                    partition.topic,
                    partition.partition,
                    offset,
                );
                insert_topic_offset(
                    &mut native.dirty_offsets,
                    partition.topic,
                    partition.partition,
                    offset,
                );
            }
        }
        Self {
            consumer,
            context,
            servers,
        }
    }

    fn native(&mut self) -> &mut NativeConsumer {
        let AsyncConsumerMode::Native(native) = &mut self.consumer.mode;
        native
    }

    fn assert_initialized(&mut self) {
        let expected = self.context.expected();
        assert_eq!(self.native().offsets, expected);
    }

    async fn finish(self) {
        drop(self.consumer);
        for server in self.servers {
            server.abort();
            let error = checked(server).await.unwrap_err();
            assert!(error.is_cancelled(), "mock broker failed: {error}");
        }
    }
}

#[tokio::test]
async fn mixed_committed_and_unset_positions_use_one_list_offsets_request_per_broker() {
    let mut fixture = Fixture::new(FetchOffset::Latest, Fault::None, false).await;
    let dirty = fixture.native().dirty_offsets.clone();
    checked(fixture.native().ensure_start_offsets())
        .await
        .unwrap();
    fixture.assert_initialized();
    assert_eq!(fixture.native().dirty_offsets, dirty);
    assert_eq!(fixture.context.offset_fetches.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.context.list_offsets.load(Ordering::Relaxed), 2);
    fixture.finish().await;
}

#[tokio::test]
async fn fully_committed_positions_need_no_list_offsets_request() {
    let mut fixture = Fixture::new(FetchOffset::Earliest, Fault::None, true).await;
    checked(fixture.native().ensure_start_offsets())
        .await
        .unwrap();
    fixture.assert_initialized();
    assert_eq!(fixture.context.list_offsets.load(Ordering::Relaxed), 0);
    fixture.finish().await;
}

async fn malformed_case(fault: Fault) {
    let mut fixture = Fixture::new(FetchOffset::Latest, fault, false).await;
    let original = fixture.native().offsets.clone();
    let dirty = fixture.native().dirty_offsets.clone();
    assert!(matches!(
        checked(fixture.consumer.poll()).await,
        Err(Error::Protocol(ProtocolError::Codec))
    ));
    assert_eq!(fixture.native().offsets, original);
    assert_eq!(fixture.native().dirty_offsets, dirty);
    assert_eq!(fixture.context.list_offsets.load(Ordering::Relaxed), 2);
    assert_eq!(fixture.context.metadata.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.context.fetches.load(Ordering::Relaxed), 0);
    checked(fixture.native().ensure_start_offsets())
        .await
        .unwrap();
    fixture.assert_initialized();
    assert_eq!(fixture.native().dirty_offsets, dirty);
    assert_eq!(fixture.context.offset_fetches.load(Ordering::Relaxed), 2);
    assert_eq!(fixture.context.list_offsets.load(Ordering::Relaxed), 4);
    fixture.finish().await;
}

#[tokio::test]
async fn missing_list_offsets_topic_preserves_all_initialization_state() {
    malformed_case(Fault::MissingTopic).await;
}
#[tokio::test]
async fn missing_list_offsets_partition_preserves_all_initialization_state() {
    malformed_case(Fault::MissingPartition).await;
}
#[tokio::test]
async fn duplicate_list_offsets_topic_preserves_all_initialization_state() {
    malformed_case(Fault::DuplicateTopic).await;
}
#[tokio::test]
async fn duplicate_list_offsets_partition_preserves_all_initialization_state() {
    malformed_case(Fault::DuplicatePartition).await;
}
#[tokio::test]
async fn extra_list_offsets_topic_preserves_all_initialization_state() {
    malformed_case(Fault::ExtraTopic).await;
}
#[tokio::test]
async fn extra_list_offsets_partition_preserves_all_initialization_state() {
    malformed_case(Fault::ExtraPartition).await;
}
#[tokio::test]
async fn invalid_negative_success_offset_preserves_all_initialization_state() {
    malformed_case(Fault::NegativeOffset).await;
}
#[tokio::test]
async fn malformed_success_offset_takes_precedence_over_a_retriable_error() {
    malformed_case(Fault::MalformedWithRetriableError).await;
}

#[tokio::test]
async fn unknown_offset_is_not_codec_or_a_negative_fetch_for_any_fallback_mode() {
    for mode in [
        FetchOffset::Earliest,
        FetchOffset::Latest,
        FetchOffset::ByTime(1_234),
    ] {
        let mut fixture = Fixture::new(mode, Fault::UnknownOffset, false).await;
        let original = fixture.native().offsets.clone();
        let dirty = fixture.native().dirty_offsets.clone();
        assert!(matches!(
            checked(fixture.consumer.poll()).await,
            Err(Error::Kafka(KafkaCode::OffsetOutOfRange))
        ));
        assert_eq!(fixture.native().offsets, original);
        assert_eq!(fixture.native().dirty_offsets, dirty);
        assert_eq!(fixture.context.fetches.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.context.metadata.load(Ordering::Relaxed), 0);
        checked(fixture.native().ensure_start_offsets())
            .await
            .unwrap();
        fixture.assert_initialized();
        assert_eq!(fixture.context.offset_fetches.load(Ordering::Relaxed), 2);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn explicit_kafka_error_retries_without_partial_start_positions() {
    let mut fixture = Fixture::new(FetchOffset::Latest, Fault::KafkaError, false).await;
    let dirty = fixture.native().dirty_offsets.clone();
    assert!(checked(fixture.consumer.poll()).await.unwrap().is_empty());
    fixture.assert_initialized();
    assert_eq!(fixture.native().dirty_offsets, dirty);
    assert_eq!(fixture.context.offset_fetches.load(Ordering::Relaxed), 2);
    assert_eq!(fixture.context.list_offsets.load(Ordering::Relaxed), 4);
    assert_eq!(fixture.context.metadata.load(Ordering::Relaxed), 1);
    fixture.finish().await;
}

#[tokio::test]
async fn cancelled_later_broker_lookup_does_not_publish_committed_or_fallback_positions() {
    let mut fixture = Fixture::new(FetchOffset::Latest, Fault::Block, false).await;
    let original = fixture.native().offsets.clone();
    let dirty = fixture.native().dirty_offsets.clone();
    let context = Arc::clone(&fixture.context);
    checked(async {
        tokio::select! {
            result = fixture.native().ensure_start_offsets() => panic!("blocked initialization completed: {result:?}"),
            () = context.blocked.notified() => {}
        }
    }).await;
    assert_eq!(fixture.native().offsets, original);
    assert_eq!(fixture.native().dirty_offsets, dirty);
    checked(fixture.native().ensure_start_offsets())
        .await
        .unwrap();
    fixture.assert_initialized();
    assert_eq!(fixture.native().dirty_offsets, dirty);
    assert_eq!(fixture.context.offset_fetches.load(Ordering::Relaxed), 2);
    assert_eq!(fixture.context.list_offsets.load(Ordering::Relaxed), 4);
    fixture.finish().await;
}
