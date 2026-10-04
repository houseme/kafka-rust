use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FindCoordinatorRequest,
    FindCoordinatorResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest,
    MetadataResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchResponse, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::client::{FetchOffset, GroupOffsetStorage, KafkaClient};
use crate::consumer::Consumer;
use crate::error::{Error, KafkaCode, ProtocolError, Result};

#[test]
fn tcp_mixed_committed_and_unset_offsets_query_time_once() {
    let (consumer, server) = mock_consumer(
        Some(vec![(0, 3), (1, -1)]),
        &[0, 1],
        true,
        FetchOffset::ByTime(777),
        vec![
            OffsetStep::new(-1, &[(0, 10), (1, 12)]),
            OffsetStep::new(-2, &[(0, 0), (1, 0)]),
            OffsetStep::new(777, &[(0, 9), (1, 8)]),
        ],
    );
    let consumer = consumer.unwrap();
    assert_eq!(fetch_positions(&consumer), vec![(0, 3), (1, 8)]);
    assert_eq!(consumer.last_consumed_message("t", 0), Some(2));
    assert_eq!(consumer.last_consumed_message("t", 1), None);
    drop(consumer);
    assert_eq!(
        server.join().unwrap(),
        vec![query(-1, &[0, 1]), query(-2, &[0, 1]), query(777, &[0, 1]),]
    );
}

#[test]
fn tcp_valid_committed_offsets_do_not_query_time() {
    let (consumer, server) = mock_consumer(
        Some(vec![(0, 3), (1, 6)]),
        &[0, 1],
        true,
        FetchOffset::ByTime(777),
        vec![
            OffsetStep::new(-1, &[(0, 10), (1, 12)]),
            OffsetStep::new(-2, &[(0, 0), (1, 0)]),
        ],
    );
    let consumer = consumer.unwrap();
    assert_eq!(fetch_positions(&consumer), vec![(0, 3), (1, 6)]);
    drop(consumer);
    assert_eq!(
        server.join().unwrap(),
        vec![query(-1, &[0, 1]), query(-2, &[0, 1])]
    );
}

#[test]
fn tcp_valid_committed_assignment_skips_time_with_a_leaderless_sibling() {
    let (consumer, server) = mock_consumer(
        Some(vec![(0, 3)]),
        &[0],
        false,
        FetchOffset::ByTime(777),
        vec![
            OffsetStep::new(-1, &[(0, 10)]),
            OffsetStep::new(-2, &[(0, 0)]),
        ],
    );
    let consumer = consumer.unwrap();
    assert_eq!(fetch_positions(&consumer), vec![(0, 3)]);
    assert_eq!(consumer.last_consumed_message("t", 0), Some(2));
    drop(consumer);
    assert_eq!(
        server.join().unwrap(),
        vec![query(-1, &[0]), query(-2, &[0])]
    );
}

#[test]
fn tcp_expired_committed_offset_uses_time_without_replacing_valid_sibling() {
    let (consumer, server) = mock_consumer(
        Some(vec![(0, 3), (1, 6)]),
        &[0, 1],
        true,
        FetchOffset::ByTime(777),
        vec![
            OffsetStep::new(-1, &[(0, 10), (1, 12)]),
            OffsetStep::new(-2, &[(0, 5), (1, 0)]),
            OffsetStep::new(777, &[(0, 8), (1, 9)]),
        ],
    );
    let consumer = consumer.unwrap();
    assert_eq!(fetch_positions(&consumer), vec![(0, 8), (1, 6)]);
    assert_eq!(consumer.last_consumed_message("t", 0), None);
    assert_eq!(consumer.last_consumed_message("t", 1), Some(5));
    drop(consumer);
    assert_eq!(
        server.join().unwrap(),
        vec![query(-1, &[0, 1]), query(-2, &[0, 1]), query(777, &[0, 1]),]
    );
}

#[test]
fn tcp_expired_and_unset_partitions_share_one_time_lookup() {
    let (consumer, server) = mock_consumer(
        Some(vec![(0, 3), (1, -1)]),
        &[0, 1],
        true,
        FetchOffset::ByTime(777),
        vec![
            OffsetStep::new(-1, &[(0, 10), (1, 12)]),
            OffsetStep::new(-2, &[(0, 5), (1, 0)]),
            OffsetStep::new(777, &[(0, 8), (1, 9)]),
        ],
    );
    let consumer = consumer.unwrap();
    assert_eq!(fetch_positions(&consumer), vec![(0, 8), (1, 9)]);
    assert_eq!(consumer.last_consumed_message("t", 0), None);
    assert_eq!(consumer.last_consumed_message("t", 1), None);
    drop(consumer);
    assert_eq!(
        server.join().unwrap(),
        vec![query(-1, &[0, 1]), query(-2, &[0, 1]), query(777, &[0, 1])]
    );
}

#[test]
fn tcp_fallback_discards_old_consumed_progress_before_committing_new_progress() {
    let (consumer, server) = mock_consumer(
        Some(vec![(0, 50)]),
        &[0],
        false,
        FetchOffset::ByTime(777),
        vec![
            OffsetStep::new(-1, &[(0, 10)]),
            OffsetStep::new(-2, &[(0, 0)]),
            OffsetStep::new(777, &[(0, 7)]),
            OffsetStep::Commit(8),
        ],
    );
    let mut consumer = consumer.unwrap();
    assert_eq!(fetch_positions(&consumer), vec![(0, 7)]);
    assert_eq!(consumer.last_consumed_message("t", 0), None);
    consumer.consume_message("t", 0, 7).unwrap();
    assert_eq!(consumer.last_consumed_message("t", 0), Some(7));
    assert_eq!(consumer.state.consumed_offsets.len(), 1);
    assert!(
        consumer
            .state
            .consumed_offsets
            .values()
            .all(|offset| offset.dirty)
    );
    consumer.commit_consumed().unwrap();
    assert_eq!(consumer.last_consumed_message("t", 0), Some(7));
    assert!(
        consumer
            .state
            .consumed_offsets
            .values()
            .all(|offset| !offset.dirty)
    );
    drop(consumer);
    assert_eq!(
        server.join().unwrap(),
        vec![query(-1, &[0]), query(-2, &[0]), query(777, &[0])]
    );
}

#[test]
fn tcp_group_less_time_lookup_uses_matching_offsets() {
    let (consumer, server) = mock_consumer(
        None,
        &[0, 1],
        true,
        FetchOffset::ByTime(777),
        vec![OffsetStep::new(777, &[(0, 4), (1, 8)])],
    );
    let consumer = consumer.unwrap();
    assert_eq!(fetch_positions(&consumer), vec![(0, 4), (1, 8)]);
    assert_eq!(consumer.last_consumed_message("t", 0), None);
    assert_eq!(consumer.last_consumed_message("t", 1), None);
    drop(consumer);
    assert_eq!(server.join().unwrap(), vec![query(777, &[0, 1])]);
}

#[test]
fn tcp_time_no_match_rejects_initialization_without_retry_or_fetch() {
    let (consumer, server) = mock_consumer(
        None,
        &[0],
        false,
        FetchOffset::ByTime(777),
        vec![OffsetStep::new(777, &[(0, -1)])],
    );
    let error = consumer.unwrap_err();
    assert_eq!(kafka_code(&error), Some(KafkaCode::OffsetOutOfRange));
    assert!(!error.is_retriable());
    assert_eq!(server.join().unwrap(), vec![query(777, &[0])]);
}

#[test]
fn tcp_unset_partition_time_no_match_rejects_mixed_initialization_without_retry_or_fetch() {
    let (consumer, server) = mock_consumer(
        Some(vec![(0, 3), (1, -1)]),
        &[0, 1],
        true,
        FetchOffset::ByTime(777),
        vec![
            OffsetStep::new(-1, &[(0, 10), (1, 12)]),
            OffsetStep::new(-2, &[(0, 0), (1, 0)]),
            OffsetStep::new(777, &[(0, 9), (1, -1)]),
        ],
    );
    let error = consumer.unwrap_err();
    assert_eq!(kafka_code(&error), Some(KafkaCode::OffsetOutOfRange));
    assert!(!error.is_retriable());
    assert_eq!(
        server.join().unwrap(),
        vec![query(-1, &[0, 1]), query(-2, &[0, 1]), query(777, &[0, 1])]
    );
}

#[test]
fn tcp_malformed_time_offsets_reject_initialization_as_codec_errors() {
    for offset in [-2, i64::MIN] {
        let (consumer, server) = mock_consumer(
            None,
            &[0],
            false,
            FetchOffset::ByTime(777),
            vec![OffsetStep::new(777, &[(0, offset)])],
        );
        let error = consumer.unwrap_err();
        assert!(is_codec_error(&error), "offset {offset}: {error:?}");
        assert!(!error.is_retriable());
        assert_eq!(server.join().unwrap(), vec![query(777, &[0])]);
    }
}

#[test]
fn tcp_maximum_time_offset_is_a_valid_initial_fetch_position() {
    let (consumer, server) = mock_consumer(
        None,
        &[0],
        false,
        FetchOffset::ByTime(777),
        vec![OffsetStep::new(777, &[(0, i64::MAX)])],
    );
    let consumer = consumer.unwrap();
    assert_eq!(fetch_positions(&consumer), vec![(0, i64::MAX)]);
    drop(consumer);
    assert_eq!(server.join().unwrap(), vec![query(777, &[0])]);
}

#[test]
fn tcp_healthy_assignment_initializes_with_a_leaderless_sibling() {
    let (consumer, server) = mock_consumer(
        None,
        &[0],
        false,
        FetchOffset::Earliest,
        vec![OffsetStep::new(-2, &[(0, 4)])],
    );
    let consumer = consumer.unwrap();
    assert_eq!(fetch_positions(&consumer), vec![(0, 4)]);
    drop(consumer);
    assert_eq!(server.join().unwrap(), vec![query(-2, &[0])]);
}

#[test]
fn tcp_leaderless_assignment_is_rejected_before_offset_queries() {
    for assignments in [&[1][..], &[0, 1][..]] {
        let (consumer, server) =
            mock_consumer(None, assignments, false, FetchOffset::Earliest, Vec::new());
        let error = consumer.unwrap_err();
        assert_eq!(kafka_code(&error), Some(KafkaCode::UnknownTopicOrPartition));
        assert_eq!(server.join().unwrap(), Vec::<ObservedQuery>::new());
    }
}

fn fetch_positions(consumer: &Consumer) -> Vec<(i32, i64)> {
    let mut positions: Vec<_> = consumer
        .state
        .fetch_requests()
        .map(|request| {
            assert_eq!(request.topic, "t");
            (request.partition, request.offset)
        })
        .collect();
    positions.sort_unstable();
    positions
}

#[derive(Debug, PartialEq, Eq)]
struct ObservedQuery {
    timestamp: i64,
    partitions: Vec<i32>,
}

fn query(timestamp: i64, partitions: &[i32]) -> ObservedQuery {
    ObservedQuery {
        timestamp,
        partitions: partitions.to_vec(),
    }
}

enum OffsetStep {
    Lookup {
        timestamp: i64,
        rows: Vec<(i32, i64)>,
    },
    Commit(i64),
}

impl OffsetStep {
    fn new(timestamp: i64, rows: &[(i32, i64)]) -> Self {
        Self::Lookup {
            timestamp,
            rows: rows.to_vec(),
        }
    }
}

fn mock_consumer(
    committed: Option<Vec<(i32, i64)>>,
    assignments: &[i32],
    p1_has_leader: bool,
    fallback: FetchOffset,
    script: Vec<OffsetStep>,
) -> (Result<Consumer>, JoinHandle<Vec<ObservedQuery>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let with_group = committed.is_some();
    let group_assignments = assignments.to_vec();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (header, _) = read_request::<ApiVersionsRequest>(&mut stream, ApiKey::ApiVersions);
        write_response(&mut stream, &header, &ApiVersionsResponse::default());
        let (header, _) = read_request::<MetadataRequest>(&mut stream, ApiKey::Metadata);
        write_response(
            &mut stream,
            &header,
            &metadata_response(address, p1_has_leader),
        );
        if let Some(committed) = committed {
            serve_group_offsets(&mut stream, address, &group_assignments, &committed);
        }
        let mut observed = Vec::new();
        for step in script {
            match step {
                OffsetStep::Lookup { timestamp, rows } => {
                    let (header, request) =
                        read_request::<ListOffsetsRequest>(&mut stream, ApiKey::ListOffsets);
                    assert_eq!(request.topics.len(), 1);
                    assert_eq!(request.topics[0].name.as_str(), "t");
                    let mut partitions = Vec::new();
                    for partition in &request.topics[0].partitions {
                        assert_eq!(partition.timestamp, timestamp);
                        partitions.push(partition.partition_index);
                    }
                    partitions.sort_unstable();
                    let mut expected: Vec<_> =
                        rows.iter().map(|&(partition, _)| partition).collect();
                    expected.sort_unstable();
                    assert_eq!(partitions, expected);
                    observed.push(ObservedQuery {
                        timestamp,
                        partitions,
                    });
                    write_response(
                        &mut stream,
                        &header,
                        &list_offsets_response(&rows, timestamp),
                    );
                }
                OffsetStep::Commit(offset) => {
                    let (header, request) =
                        read_request::<OffsetCommitRequest>(&mut stream, ApiKey::OffsetCommit);
                    assert_eq!(request.group_id.as_str(), "initialization-group");
                    assert_eq!(request.topics.len(), 1);
                    assert_eq!(request.topics[0].name.as_str(), "t");
                    assert_eq!(request.topics[0].partitions.len(), 1);
                    let partition = &request.topics[0].partitions[0];
                    assert_eq!(partition.partition_index, 0);
                    assert_eq!(partition.committed_offset, offset);
                    write_response(&mut stream, &header, &commit_response());
                }
            }
        }
        // Dropping the result must close the connection without any extra lookup or Fetch.
        assert_connection_closed(&mut stream);
        observed
    });
    let mut client = KafkaClient::builder()
        .with_hosts(vec![address.to_string()])
        .with_conn_rw_timeout(2)
        .with_group_offset_storage(Some(GroupOffsetStorage::Kafka))
        .build();
    client.load_metadata_all().unwrap();
    let builder = Consumer::from_client(client)
        .with_topic_partitions("t".to_owned(), assignments)
        .with_fallback_offset(fallback);
    let builder = if with_group {
        builder.with_group("initialization-group".to_owned())
    } else {
        builder
    };
    (builder.create(), server)
}

fn serve_group_offsets(
    stream: &mut TcpStream,
    address: SocketAddr,
    assignments: &[i32],
    committed: &[(i32, i64)],
) {
    let (header, request) = read_request::<FindCoordinatorRequest>(stream, ApiKey::FindCoordinator);
    assert_eq!(request.key.as_str(), "initialization-group");
    let response = FindCoordinatorResponse::default()
        .with_node_id(BrokerId::from(1))
        .with_host(StrBytes::from_string(address.ip().to_string()))
        .with_port(i32::from(address.port()));
    write_response(stream, &header, &response);
    let (header, request) = read_request::<OffsetFetchRequest>(stream, ApiKey::OffsetFetch);
    assert_eq!(request.group_id.as_str(), "initialization-group");
    let topics = request
        .topics
        .as_ref()
        .expect("OffsetFetch targets missing");
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name.as_str(), "t");
    let mut requested = topics[0].partition_indexes.clone();
    requested.sort_unstable();
    assert_eq!(requested.as_slice(), assignments);
    write_response(stream, &header, &offset_fetch_response(committed));
}

fn read_request<T: Decodable + HeaderVersion>(
    stream: &mut TcpStream,
    key: ApiKey,
) -> (RequestHeader, T) {
    let mut size = [0; 4];
    stream.read_exact(&mut size).unwrap();
    let mut frame = vec![0; usize::try_from(i32::from_be_bytes(size)).unwrap()];
    stream.read_exact(&mut frame).unwrap();
    let version = i16::from_be_bytes(frame[2..4].try_into().unwrap());
    let mut frame = Bytes::from(frame);
    let header = RequestHeader::decode(&mut frame, T::header_version(version)).unwrap();
    assert_eq!(header.request_api_key, key as i16);
    let request = T::decode(&mut frame, version).unwrap();
    assert!(!frame.has_remaining(), "request contains trailing bytes");
    (header, request)
}

fn write_response<T: Encodable + HeaderVersion>(
    stream: &mut TcpStream,
    request: &RequestHeader,
    response: &T,
) {
    let version = request.request_api_version;
    let mut payload = BytesMut::new();
    ResponseHeader::default()
        .with_correlation_id(request.correlation_id)
        .encode(&mut payload, T::header_version(version))
        .unwrap();
    response.encode(&mut payload, version).unwrap();
    stream
        .write_all(&i32::try_from(payload.len()).unwrap().to_be_bytes())
        .unwrap();
    stream.write_all(&payload).unwrap();
}

fn topic_name() -> TopicName {
    TopicName::from(StrBytes::from_static_str("t"))
}

fn metadata_response(address: SocketAddr, p1_has_leader: bool) -> MetadataResponse {
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    MetadataResponse::default()
        .with_brokers(vec![
            MetadataResponseBroker::default()
                .with_node_id(BrokerId::from(1))
                .with_host(StrBytes::from_string(address.ip().to_string()))
                .with_port(i32::from(address.port())),
        ])
        .with_topics(vec![
            MetadataResponseTopic::default()
                .with_name(Some(topic_name()))
                .with_partitions(
                    (0..2)
                        .map(|partition| {
                            MetadataResponsePartition::default()
                                .with_partition_index(partition)
                                .with_leader_id(BrokerId::from(
                                    if partition == 0 || p1_has_leader {
                                        1
                                    } else {
                                        -1
                                    },
                                ))
                                .with_replica_nodes(vec![BrokerId::from(1)])
                                .with_isr_nodes(vec![BrokerId::from(1)])
                        })
                        .collect(),
                ),
        ])
}

fn offset_fetch_response(rows: &[(i32, i64)]) -> OffsetFetchResponse {
    use kafka_protocol::messages::offset_fetch_response::{
        OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    };
    OffsetFetchResponse::default().with_topics(vec![
        OffsetFetchResponseTopic::default()
            .with_name(topic_name())
            .with_partitions(
                rows.iter()
                    .map(|&(partition, offset)| {
                        OffsetFetchResponsePartition::default()
                            .with_partition_index(partition)
                            .with_committed_offset(offset)
                    })
                    .collect(),
            ),
    ])
}

fn list_offsets_response(rows: &[(i32, i64)], timestamp: i64) -> ListOffsetsResponse {
    use kafka_protocol::messages::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };
    ListOffsetsResponse::default().with_topics(vec![
        ListOffsetsTopicResponse::default()
            .with_name(topic_name())
            .with_partitions(
                rows.iter()
                    .map(|&(partition, offset)| {
                        ListOffsetsPartitionResponse::default()
                            .with_partition_index(partition)
                            .with_timestamp(if timestamp >= 0 && offset >= 0 {
                                timestamp
                            } else {
                                -1
                            })
                            .with_offset(offset)
                    })
                    .collect(),
            ),
    ])
}

fn commit_response() -> OffsetCommitResponse {
    use kafka_protocol::messages::offset_commit_response::{
        OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    };
    OffsetCommitResponse::default().with_topics(vec![
        OffsetCommitResponseTopic::default()
            .with_name(topic_name())
            .with_partitions(vec![
                OffsetCommitResponsePartition::default()
                    .with_partition_index(0)
                    .with_error_code(0),
            ]),
    ])
}

fn assert_connection_closed(stream: &mut TcpStream) {
    let mut marker = [0];
    match stream.read(&mut marker) {
        Ok(0) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        other => panic!("unexpected initialization request, retry, or Fetch: {other:?}"),
    }
}

fn kafka_code(error: &Error) -> Option<KafkaCode> {
    match error {
        Error::Kafka(code) => Some(*code),
        Error::TopicPartitionError { error_code, .. } => Some(*error_code),
        Error::BrokerRequestError { source, .. } => kafka_code(source),
        _ => None,
    }
}

fn is_codec_error(error: &Error) -> bool {
    match error {
        Error::Protocol(ProtocolError::Codec) => true,
        Error::BrokerRequestError { source, .. } => is_codec_error(source),
        _ => false,
    }
}
