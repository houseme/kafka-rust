//! Origin-specific coordinator connection failures and final retry failures.

use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use tokio::net::{TcpListener, TcpStream};

use super::tests::{checked, read_request, reply};
use super::*;

const TOPIC: &str = "coordinator-recovery";
const GROUP: &str = "coordinator-recovery-group";

fn topic() -> TopicName {
    TopicName::from(StrBytes::from_static_str(TOPIC))
}

fn native(consumer: &mut AsyncConsumer) -> &mut NativeConsumer {
    let AsyncConsumerMode::Native(native) = &mut consumer.mode;
    native
}

async fn consumer(hosts: Vec<String>) -> AsyncConsumer {
    let client = checked(AsyncKafkaClient::new(hosts)).await.unwrap();
    let mut consumer = AsyncConsumer::from_client(client, GROUP.to_owned(), vec![TOPIC.to_owned()])
        .await
        .unwrap();
    let state = native(&mut consumer);
    state.retry_attempts = 1;
    state.retry_backoff = Duration::ZERO;
    state.offsets = HashMap::from([(TOPIC.to_owned(), HashMap::from([(0, 7)]))]);
    state.dirty_offsets = state.offsets.clone();
    consumer
}

async fn find_coordinator(socket: &mut TcpStream, addr: std::net::SocketAddr) {
    let (header, request) = read_request::<FindCoordinatorRequest>(
        socket,
        ApiKey::FindCoordinator,
        API_VERSION_FIND_COORDINATOR,
    )
    .await;
    assert_eq!(request.key.as_str(), GROUP);
    reply(
        socket,
        &header,
        API_VERSION_FIND_COORDINATOR,
        FindCoordinatorResponse::default()
            .with_node_id(BrokerId::from(0))
            .with_host(StrBytes::from_string(addr.ip().to_string()))
            .with_port(i32::from(addr.port())),
    )
    .await;
}

async fn successful_commit(socket: &mut TcpStream) {
    let (header, request) = read_request::<OffsetCommitRequest>(
        socket,
        ApiKey::OffsetCommit,
        API_VERSION_OFFSET_COMMIT,
    )
    .await;
    assert_eq!(request.group_id.as_str(), GROUP);
    assert_eq!(request.topics[0].partitions[0].committed_offset, 7);
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

#[tokio::test]
async fn coordinator_connect_failures_clear_only_the_coordinator_origin() {
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = closed.local_addr().unwrap();
    drop(closed);
    let mut consumer = consumer(Vec::new()).await;
    let old_leaders = HashMap::from([((TOPIC.to_owned(), 0), "unchanged-leader:9092".to_owned())]);
    let state = native(&mut consumer);
    state.leaders = old_leaders.clone();
    state.coordinator = Some(addr.to_string());
    assert!(matches!(
        checked(consumer.commit()).await,
        Err(Error::Connection(_))
    ));
    let state = native(&mut consumer);
    assert!(state.coordinator.is_none());
    assert!(!state.metadata_refresh_needed);
    assert_eq!(state.leaders, old_leaders);
    assert_eq!(state.dirty_offsets[TOPIC][&0], 7);
    state.coordinator = Some(addr.to_string());
    assert!(matches!(
        checked(state.fetch_committed_offsets(&[(TOPIC.to_owned(), 1)])).await,
        Err(Error::Connection(_))
    ));
    assert!(state.coordinator.is_none());
    assert!(!state.metadata_refresh_needed);
    assert_eq!(state.leaders, old_leaders);
    assert_eq!(state.offsets[TOPIC][&0], 7);
}

#[tokio::test]
async fn commit_response_eof_preserves_dirty_and_next_caller_rediscovers_coordinator() {
    let bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replacement = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap_addr = bootstrap.local_addr().unwrap();
    let old_addr = old.local_addr().unwrap();
    let replacement_addr = replacement.local_addr().unwrap();
    let bootstrap_server = tokio::spawn(async move {
        let (mut socket, _) = checked(bootstrap.accept()).await.unwrap();
        find_coordinator(&mut socket, replacement_addr).await;
    });
    let old_server = tokio::spawn(async move {
        let (mut socket, _) = checked(old.accept()).await.unwrap();
        read_request::<OffsetCommitRequest>(
            &mut socket,
            ApiKey::OffsetCommit,
            API_VERSION_OFFSET_COMMIT,
        )
        .await;
    });
    let replacement_server = tokio::spawn(async move {
        let (mut socket, _) = checked(replacement.accept()).await.unwrap();
        successful_commit(&mut socket).await;
    });
    let mut consumer = consumer(vec![bootstrap_addr.to_string()]).await;
    native(&mut consumer).coordinator = Some(old_addr.to_string());
    assert!(matches!(
        checked(consumer.commit()).await,
        Err(Error::Connection(_))
    ));
    let state = native(&mut consumer);
    assert!(state.coordinator.is_none());
    assert!(!state.metadata_refresh_needed);
    assert_eq!(state.dirty_offsets[TOPIC][&0], 7);
    checked(consumer.commit()).await.unwrap();
    assert!(native(&mut consumer).dirty_offsets.is_empty());
    checked(old_server).await.unwrap();
    checked(bootstrap_server).await.unwrap();
    checked(replacement_server).await.unwrap();
}

#[tokio::test]
async fn offset_fetch_response_eof_preserves_progress_and_next_poll_rediscovers_coordinator() {
    let bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replacement = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap_addr = bootstrap.local_addr().unwrap();
    let old_addr = old.local_addr().unwrap();
    let replacement_addr = replacement.local_addr().unwrap();
    let bootstrap_server = tokio::spawn(async move {
        let (mut socket, _) = checked(bootstrap.accept()).await.unwrap();
        find_coordinator(&mut socket, replacement_addr).await;
        let (header, request) =
            read_request::<FetchRequest>(&mut socket, ApiKey::Fetch, API_VERSION_FETCH).await;
        let partitions = request.topics[0]
            .partitions
            .iter()
            .map(|partition| {
                assert_eq!(
                    partition.fetch_offset,
                    if partition.partition == 0 { 7 } else { 21 }
                );
                PartitionData::default()
                    .with_partition_index(partition.partition)
                    .with_high_watermark(partition.fetch_offset)
                    .with_records(None)
            })
            .collect();
        reply(
            &mut socket,
            &header,
            API_VERSION_FETCH,
            FetchResponse::default().with_responses(vec![
                FetchableTopicResponse::default()
                    .with_topic(topic())
                    .with_partitions(partitions),
            ]),
        )
        .await;
    });
    let old_server = tokio::spawn(async move {
        let (mut socket, _) = checked(old.accept()).await.unwrap();
        read_request::<OffsetFetchRequest>(
            &mut socket,
            ApiKey::OffsetFetch,
            API_VERSION_OFFSET_FETCH,
        )
        .await;
    });
    let replacement_server = tokio::spawn(async move {
        let (mut socket, _) = checked(replacement.accept()).await.unwrap();
        let (header, request) = read_request::<OffsetFetchRequest>(
            &mut socket,
            ApiKey::OffsetFetch,
            API_VERSION_OFFSET_FETCH,
        )
        .await;
        assert_eq!(request.topics.unwrap()[0].partition_indexes, [1]);
        reply(
            &mut socket,
            &header,
            API_VERSION_OFFSET_FETCH,
            OffsetFetchResponse::default().with_topics(vec![
                OffsetFetchResponseTopic::default()
                    .with_name(topic())
                    .with_partitions(vec![
                        OffsetFetchResponsePartition::default()
                            .with_partition_index(1)
                            .with_committed_offset(21),
                    ]),
            ]),
        )
        .await;
    });
    let mut consumer = consumer(vec![bootstrap_addr.to_string()]).await;
    let state = native(&mut consumer);
    state.coordinator = Some(old_addr.to_string());
    state.leaders = HashMap::from([
        ((TOPIC.to_owned(), 0), bootstrap_addr.to_string()),
        ((TOPIC.to_owned(), 1), bootstrap_addr.to_string()),
    ]);
    assert!(matches!(
        checked(consumer.poll()).await,
        Err(Error::Connection(_))
    ));
    let state = native(&mut consumer);
    assert!(state.coordinator.is_none());
    assert!(!state.metadata_refresh_needed);
    assert_eq!(state.offsets[TOPIC], HashMap::from([(0, 7)]));
    assert_eq!(state.dirty_offsets[TOPIC], HashMap::from([(0, 7)]));
    assert!(checked(consumer.poll()).await.unwrap().is_empty());
    assert_eq!(
        native(&mut consumer).offsets[TOPIC],
        HashMap::from([(0, 7), (1, 21)])
    );
    assert_eq!(
        native(&mut consumer).dirty_offsets[TOPIC],
        HashMap::from([(0, 7)])
    );
    checked(old_server).await.unwrap();
    checked(bootstrap_server).await.unwrap();
    checked(replacement_server).await.unwrap();
}

#[tokio::test]
async fn final_coordinator_kafka_errors_keep_dirty_and_invalidate_for_next_commit() {
    for code in [14, 15, 16] {
        let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let replacement = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let old_addr = old.local_addr().unwrap();
        let replacement_addr = replacement.local_addr().unwrap();
        let old_server = tokio::spawn(async move {
            let (mut socket, _) = checked(old.accept()).await.unwrap();
            let (header, _) = read_request::<OffsetCommitRequest>(
                &mut socket,
                ApiKey::OffsetCommit,
                API_VERSION_OFFSET_COMMIT,
            )
            .await;
            reply(
                &mut socket,
                &header,
                API_VERSION_OFFSET_COMMIT,
                OffsetCommitResponse::default().with_topics(vec![
                    OffsetCommitResponseTopic::default()
                        .with_name(topic())
                        .with_partitions(vec![
                            OffsetCommitResponsePartition::default()
                                .with_partition_index(0)
                                .with_error_code(code),
                        ]),
                ]),
            )
            .await;
            find_coordinator(&mut socket, replacement_addr).await;
        });
        let replacement_server = tokio::spawn(async move {
            let (mut socket, _) = checked(replacement.accept()).await.unwrap();
            successful_commit(&mut socket).await;
        });
        let mut consumer = consumer(vec![old_addr.to_string()]).await;
        native(&mut consumer).coordinator = Some(old_addr.to_string());
        assert!(
            matches!(checked(consumer.commit()).await, Err(Error::Kafka(found)) if found == map_kafka_code(code).unwrap())
        );
        assert!(native(&mut consumer).coordinator.is_none());
        assert_eq!(native(&mut consumer).dirty_offsets[TOPIC][&0], 7);
        checked(consumer.commit()).await.unwrap();
        assert!(native(&mut consumer).dirty_offsets.is_empty());
        checked(old_server).await.unwrap();
        checked(replacement_server).await.unwrap();
    }
}
