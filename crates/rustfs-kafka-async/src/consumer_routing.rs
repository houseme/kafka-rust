//! Checked control-plane routing snapshots for the native consumer.

use std::collections::{HashMap, HashSet};

use kafka_protocol::messages::{FindCoordinatorResponse, MetadataResponse};
use rustfs_kafka::error::{Error, KafkaCode, ProtocolError, Result};

use super::{
    API_VERSION_FIND_COORDINATOR, API_VERSION_METADATA, NativeConsumer,
    build_find_coordinator_request, build_metadata_request, no_host_reachable_error,
};
use crate::wire::{get_kp_response, kafka_code_from_protocol as map_kafka_code, send_kp_request};

type LeaderRoutes = HashMap<(String, i32), String>;

struct MetadataRoutes {
    routes: LeaderRoutes,
    complete: bool,
}

impl NativeConsumer {
    pub(super) async fn refresh_metadata(&mut self) -> Result<()> {
        let host = self.routing_request_host()?;
        let correlation = self.next_correlation();
        let client_id = self.client.client_id().to_owned();
        let conn = self.client.get_connection(&host).await?;
        let (header, request) = build_metadata_request(correlation, &client_id, &self.topics);
        send_kp_request(conn, &header, &request, API_VERSION_METADATA).await?;
        let response = get_kp_response::<MetadataResponse>(conn, API_VERSION_METADATA).await?;
        let snapshot = checked_metadata_routes(response, &self.topics)?;
        if snapshot.routes.is_empty() {
            return Err(Error::Kafka(KafkaCode::LeaderNotAvailable));
        }
        self.leaders = snapshot.routes;
        self.metadata_refresh_needed = !snapshot.complete;
        self.last_partial_metadata_refresh = (!snapshot.complete).then(tokio::time::Instant::now);
        Ok(())
    }

    pub(super) async fn refresh_coordinator(&mut self) -> Result<()> {
        let host = self.routing_request_host()?;
        let correlation = self.next_correlation();
        let client_id = self.client.client_id().to_owned();
        let conn = self.client.get_connection(&host).await?;
        let (header, request) =
            build_find_coordinator_request(correlation, &client_id, &self.group);
        send_kp_request(conn, &header, &request, API_VERSION_FIND_COORDINATOR).await?;
        let response =
            get_kp_response::<FindCoordinatorResponse>(conn, API_VERSION_FIND_COORDINATOR).await?;
        // This request uses v3: the generated decoder reads the legacy fields,
        // never a v4+ batched coordinator result.
        let endpoint = checked_coordinator_endpoint(&response)?;
        self.coordinator = Some(endpoint);
        Ok(())
    }

    fn routing_request_host(&self) -> Result<String> {
        self.client
            .connected_hosts()
            .first()
            .map(|host| (*host).to_owned())
            .or_else(|| self.client.bootstrap_hosts().first().cloned())
            .ok_or_else(no_host_reachable_error)
    }
}

fn checked_coordinator_endpoint(response: &FindCoordinatorResponse) -> Result<String> {
    if response.error_code != 0 {
        return Err(Error::Kafka(
            map_kafka_code(response.error_code).unwrap_or(KafkaCode::Unknown),
        ));
    }
    checked_endpoint(
        i32::from(response.node_id),
        response.host.as_str(),
        response.port,
    )
}

fn checked_endpoint(node: i32, host: &str, port: i32) -> Result<String> {
    if node < 0 || host.is_empty() || !(1..=65_535).contains(&port) {
        return Err(Error::Protocol(ProtocolError::Codec));
    }
    Ok(format!("{host}:{port}"))
}

fn checked_metadata_routes(
    response: MetadataResponse,
    requested: &[String],
) -> Result<MetadataRoutes> {
    let mut brokers = HashMap::with_capacity(response.brokers.len());
    for broker in response.brokers {
        let id = i32::from(broker.node_id);
        let endpoint = checked_endpoint(id, broker.host.as_str(), broker.port)?;
        if brokers.insert(id, endpoint).is_some() {
            return Err(Error::Protocol(ProtocolError::Codec));
        }
    }
    let mut remaining: HashSet<&str> = requested.iter().map(String::as_str).collect();
    let mut routes = HashMap::new();
    let mut complete = true;
    for topic in response.topics {
        let name = topic.name.ok_or(Error::Protocol(ProtocolError::Codec))?;
        if !remaining.remove(name.as_str()) {
            return Err(Error::Protocol(ProtocolError::Codec));
        }
        validate_partition_ids(&topic.partitions)?;
        if topic.error_code != 0 || topic.partitions.is_empty() {
            complete = false;
        }
        for partition in topic.partitions {
            if topic.error_code != 0 || partition.error_code != 0 {
                complete = false;
                continue;
            }
            if let Some(endpoint) = brokers.get(&i32::from(partition.leader_id)) {
                routes.insert(
                    (name.to_string(), partition.partition_index),
                    endpoint.clone(),
                );
            } else {
                complete = false;
            }
        }
    }
    if !remaining.is_empty() {
        return Err(Error::Protocol(ProtocolError::Codec));
    }
    Ok(MetadataRoutes { routes, complete })
}

fn validate_partition_ids(
    partitions: &[kafka_protocol::messages::metadata_response::MetadataResponsePartition],
) -> Result<()> {
    if partitions.iter().enumerate().all(|(index, partition)| {
        usize::try_from(partition.partition_index).is_ok_and(|id| id == index)
    }) {
        return Ok(());
    }
    let mut seen = vec![false; partitions.len()];
    for partition in partitions {
        let index = usize::try_from(partition.partition_index)
            .map_err(|_| Error::Protocol(ProtocolError::Codec))?;
        let entry = seen
            .get_mut(index)
            .ok_or(Error::Protocol(ProtocolError::Codec))?;
        if *entry {
            return Err(Error::Protocol(ProtocolError::Codec));
        }
        *entry = true;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use bytes::{Buf, Bytes, BytesMut};
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::offset_commit_response::{
        OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    };
    use kafka_protocol::messages::offset_fetch_response::{
        OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    };
    use kafka_protocol::messages::{
        ApiKey, FetchRequest, FetchResponse, MetadataRequest, OffsetCommitRequest,
        OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, RequestHeader,
    };
    use kafka_protocol::protocol::{Decodable, HeaderVersion, StrBytes};
    use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};

    use super::super::tests::{checked, reply};
    use super::super::{
        API_VERSION_FETCH, API_VERSION_OFFSET_COMMIT, API_VERSION_OFFSET_FETCH, AsyncConsumer,
        AsyncConsumerMode,
    };
    use super::*;

    const TOPIC: &str = "routing-topic";
    const GROUP: &str = "routing-group";

    fn response(address: SocketAddr, topics: &[(&str, usize)]) -> MetadataResponse {
        MetadataResponse::default()
            .with_brokers(vec![
                MetadataResponseBroker::default()
                    .with_node_id(1.into())
                    .with_host(StrBytes::from_string(address.ip().to_string()))
                    .with_port(i32::from(address.port())),
            ])
            .with_topics(
                topics
                    .iter()
                    .map(|&(name, count)| {
                        MetadataResponseTopic::default()
                            .with_name(Some(StrBytes::from_string(name.to_owned()).into()))
                            .with_partitions(
                                (0..count)
                                    .map(|index| {
                                        MetadataResponsePartition::default()
                                            .with_partition_index(i32::try_from(index).unwrap())
                                            .with_leader_id(1.into())
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            )
    }

    fn valid_coordinator(address: SocketAddr) -> FindCoordinatorResponse {
        FindCoordinatorResponse::default()
            .with_node_id(1.into())
            .with_host(StrBytes::from_string(address.ip().to_string()))
            .with_port(i32::from(address.port()))
    }

    fn fixture_address() -> SocketAddr {
        "127.0.0.1:9092".parse().unwrap()
    }

    #[test]
    fn checked_metadata_accepts_duplicate_subscriptions_and_reordered_dense_partitions() {
        let mut metadata = response(fixture_address(), &[(TOPIC, 2), ("other-topic", 1)]);
        metadata.topics[0].partitions.reverse();
        let snapshot = checked_metadata_routes(
            metadata,
            &[TOPIC.to_owned(), TOPIC.to_owned(), "other-topic".to_owned()],
        )
        .unwrap();
        assert!(snapshot.complete);
        let routes = snapshot.routes;
        assert_eq!(routes.len(), 3);
        assert_eq!(
            routes[&(TOPIC.to_owned(), 1)],
            fixture_address().to_string()
        );
    }

    #[test]
    fn metadata_errors_and_unknown_leaders_are_not_routes_but_healthy_partitions_remain() {
        let mut metadata = response(fixture_address(), &[(TOPIC, 3), ("other-topic", 1)]);
        metadata.topics[0].partitions[0].error_code = KafkaCode::ReplicaNotAvailable as i16;
        metadata.topics[0].partitions[1].leader_id = 99.into();
        metadata.topics[1].error_code = KafkaCode::TopicAuthorizationFailed as i16;
        let snapshot =
            checked_metadata_routes(metadata, &[TOPIC.to_owned(), "other-topic".to_owned()])
                .unwrap();
        assert!(!snapshot.complete);
        let routes = snapshot.routes;
        assert_eq!(routes.len(), 1);
        assert!(routes.contains_key(&(TOPIC.to_owned(), 2)));
        let mut unavailable = response(fixture_address(), &[(TOPIC, 1)]);
        unavailable.topics[0].partitions[0].leader_id = (-1).into();
        assert_eq!(
            checked_metadata_routes(unavailable, &[TOPIC.to_owned()])
                .unwrap()
                .routes,
            LeaderRoutes::new()
        );
    }

    #[test]
    fn partial_metadata_covers_empty_topics_errors_and_unavailable_leaders() {
        for fault in [
            "topic-error",
            "empty-topic",
            "partition-error",
            "leaderless",
            "unknown",
        ] {
            let mut metadata = response(fixture_address(), &[(TOPIC, 1), ("other-topic", 1)]);
            let partial = &mut metadata.topics[1];
            match fault {
                "topic-error" => partial.error_code = KafkaCode::TopicAuthorizationFailed as i16,
                "empty-topic" => partial.partitions.clear(),
                "partition-error" => {
                    partial.partitions[0].error_code = KafkaCode::ReplicaNotAvailable as i16;
                }
                "leaderless" => partial.partitions[0].leader_id = (-1).into(),
                "unknown" => partial.partitions[0].leader_id = 99.into(),
                _ => unreachable!(),
            }
            let snapshot =
                checked_metadata_routes(metadata, &[TOPIC.to_owned(), "other-topic".to_owned()])
                    .unwrap();
            assert!(!snapshot.complete, "{fault}");
            assert_eq!(snapshot.routes.len(), 1, "{fault}");
            assert!(snapshot.routes.contains_key(&(TOPIC.to_owned(), 0)));
        }
    }

    #[test]
    fn malformed_metadata_identity_or_descriptors_are_codec_errors() {
        for fault in [
            "node",
            "host",
            "port",
            "duplicate-node",
            "negative-partition",
            "duplicate-partition",
            "non-dense",
            "missing-topic",
            "duplicate-topic",
            "extra-topic",
            "missing-name",
        ] {
            let mut metadata = response(fixture_address(), &[(TOPIC, 2)]);
            match fault {
                "node" => metadata.brokers[0].node_id = (-1).into(),
                "host" => metadata.brokers[0].host = StrBytes::new(),
                "port" => metadata.brokers[0].port = 65_536,
                "duplicate-node" => metadata.brokers.push(metadata.brokers[0].clone()),
                "negative-partition" => metadata.topics[0].partitions[0].partition_index = -1,
                "duplicate-partition" => metadata.topics[0].partitions[1].partition_index = 0,
                "non-dense" => metadata.topics[0].partitions[1].partition_index = 9,
                "missing-topic" => metadata.topics.clear(),
                "duplicate-topic" => metadata.topics.push(metadata.topics[0].clone()),
                "extra-topic" => {
                    metadata.topics[0].name = Some(StrBytes::from_static_str("unsubscribed").into())
                }
                "missing-name" => metadata.topics[0].name = None,
                _ => unreachable!(),
            }
            assert!(
                matches!(
                    checked_metadata_routes(metadata, &[TOPIC.to_owned()]),
                    Err(Error::Protocol(ProtocolError::Codec))
                ),
                "{fault}"
            );
        }
    }

    #[test]
    fn legacy_find_validates_only_successful_descriptors_and_preserves_broker_errors() {
        assert_eq!(
            checked_coordinator_endpoint(&valid_coordinator(fixture_address())).unwrap(),
            fixture_address().to_string()
        );
        for fault in ["node", "host", "zero-port", "negative-port", "large-port"] {
            let mut response = valid_coordinator(fixture_address());
            match fault {
                "node" => response.node_id = (-1).into(),
                "host" => response.host = StrBytes::new(),
                "zero-port" => response.port = 0,
                "negative-port" => response.port = -1,
                "large-port" => response.port = 65_536,
                _ => unreachable!(),
            }
            assert!(matches!(
                checked_coordinator_endpoint(&response),
                Err(Error::Protocol(ProtocolError::Codec))
            ));
        }
        let rejection = FindCoordinatorResponse::default()
            .with_error_code(KafkaCode::NotCoordinatorForGroup as i16);
        assert!(matches!(
            checked_coordinator_endpoint(&rejection),
            Err(Error::Kafka(KafkaCode::NotCoordinatorForGroup))
        ));
    }

    fn native(consumer: &mut AsyncConsumer) -> &mut NativeConsumer {
        match &mut consumer.mode {
            AsyncConsumerMode::Native(native) => native,
        }
    }

    async fn consumer(address: SocketAddr, topics: Vec<String>) -> AsyncConsumer {
        checked(
            AsyncConsumer::builder(vec![address.to_string()])
                .with_group(GROUP.to_owned())
                .with_topics(topics)
                .with_native_retry_attempts(1)
                .with_native_retry_backoff(std::time::Duration::ZERO)
                .build(),
        )
        .await
        .unwrap()
    }

    async fn request<T: Decodable + HeaderVersion>(
        socket: &mut TcpStream,
        api: ApiKey,
        version: i16,
    ) -> (RequestHeader, T) {
        let size = checked(socket.read_i32()).await.unwrap();
        let mut frame = vec![0; usize::try_from(size).unwrap()];
        checked(socket.read_exact(&mut frame)).await.unwrap();
        let mut frame = Bytes::from(frame);
        let header = RequestHeader::decode(&mut frame, T::header_version(version)).unwrap();
        assert_eq!(header.request_api_key, api as i16);
        assert_eq!(header.request_api_version, version);
        let request = T::decode(&mut frame, version).unwrap();
        assert!(!frame.has_remaining());
        (header, request)
    }

    async fn serve_find(socket: &mut TcpStream, endpoint: SocketAddr) {
        let (header, request) = request::<kafka_protocol::messages::FindCoordinatorRequest>(
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
            valid_coordinator(endpoint),
        )
        .await;
    }

    async fn serve_optional_find(socket: &mut TcpStream, endpoint: SocketAddr) {
        let size = match checked(socket.read_i32()).await {
            Ok(size) => size,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return,
            Err(error) => panic!("reading an optional discovery request failed: {error}"),
        };
        let mut frame = vec![0; usize::try_from(size).unwrap()];
        checked(socket.read_exact(&mut frame)).await.unwrap();
        let mut frame = Bytes::from(frame);
        let header = RequestHeader::decode(
            &mut frame,
            ApiKey::FindCoordinator.request_header_version(API_VERSION_FIND_COORDINATOR),
        )
        .unwrap();
        assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
        assert_eq!(header.request_api_version, API_VERSION_FIND_COORDINATOR);
        let request = kafka_protocol::messages::FindCoordinatorRequest::decode(
            &mut frame,
            API_VERSION_FIND_COORDINATOR,
        )
        .unwrap();
        assert_eq!(request.key.as_str(), GROUP);
        assert!(!frame.has_remaining());
        reply(
            socket,
            &header,
            API_VERSION_FIND_COORDINATOR,
            valid_coordinator(endpoint),
        )
        .await;
    }

    async fn serve_offsets(socket: &mut TcpStream) {
        let (header, request) =
            request::<OffsetFetchRequest>(socket, ApiKey::OffsetFetch, API_VERSION_OFFSET_FETCH)
                .await;
        assert_eq!(request.group_id.as_str(), GROUP);
        reply(
            socket,
            &header,
            API_VERSION_OFFSET_FETCH,
            OffsetFetchResponse::default().with_topics(vec![
                OffsetFetchResponseTopic::default()
                    .with_name(StrBytes::from_static_str(TOPIC).into())
                    .with_partitions(vec![
                        OffsetFetchResponsePartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(42),
                    ]),
            ]),
        )
        .await;
    }

    async fn serve_commit(socket: &mut TcpStream, error: i16) {
        let (header, request) =
            request::<OffsetCommitRequest>(socket, ApiKey::OffsetCommit, API_VERSION_OFFSET_COMMIT)
                .await;
        assert_eq!(request.group_id.as_str(), GROUP);
        assert_eq!(request.topics[0].partitions[0].committed_offset, 43);
        reply(
            socket,
            &header,
            API_VERSION_OFFSET_COMMIT,
            OffsetCommitResponse::default().with_topics(vec![
                OffsetCommitResponseTopic::default()
                    .with_name(StrBytes::from_static_str(TOPIC).into())
                    .with_partitions(vec![
                        OffsetCommitResponsePartition::default()
                            .with_partition_index(0)
                            .with_error_code(error),
                    ]),
            ]),
        )
        .await;
    }

    async fn serve_fetch(socket: &mut TcpStream) {
        serve_fetch_positions(socket, &[(0, 42)]).await;
    }

    async fn serve_fetch_positions(socket: &mut TcpStream, expected: &[(i32, i64)]) {
        let (header, request) =
            request::<FetchRequest>(socket, ApiKey::Fetch, API_VERSION_FETCH).await;
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].topic.as_str(), TOPIC);
        let actual: HashMap<_, _> = request.topics[0]
            .partitions
            .iter()
            .map(|partition| (partition.partition, partition.fetch_offset))
            .collect();
        assert_eq!(actual, expected.iter().copied().collect());
        let partitions = expected
            .iter()
            .map(|&(partition, offset)| fetch_partition(partition, offset))
            .collect();
        let response = FetchResponse::default().with_responses(vec![
            kafka_protocol::messages::fetch_response::FetchableTopicResponse::default()
                .with_topic(StrBytes::from_static_str(TOPIC).into())
                .with_partitions(partitions),
        ]);
        reply(socket, &header, API_VERSION_FETCH, response).await;
    }

    fn fetch_partition(
        partition: i32,
        offset: i64,
    ) -> kafka_protocol::messages::fetch_response::PartitionData {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: -1,
            timestamp: 0,
            key: None,
            value: Some(Bytes::from_static(b"delivered")),
            headers: kafka_protocol::indexmap::IndexMap::default(),
        };
        let mut records = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut records,
            &[record],
            &RecordEncodeOptions {
                version: 2,
                compression: kafka_protocol::records::Compression::None,
            },
        )
        .unwrap();
        kafka_protocol::messages::fetch_response::PartitionData::default()
            .with_partition_index(partition)
            .with_high_watermark(offset + 1)
            .with_records(Some(records.freeze()))
    }

    async fn serve_metadata_snapshot(socket: &mut TcpStream, snapshot: MetadataResponse) {
        let (header, request) =
            request::<MetadataRequest>(socket, ApiKey::Metadata, API_VERSION_METADATA).await;
        let topics = request.topics.unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name.as_ref().unwrap().as_str(), TOPIC);
        reply(socket, &header, API_VERSION_METADATA, snapshot).await;
    }

    #[tokio::test]
    async fn partial_metadata_keeps_fetching_during_backoff_then_recovers_missing_partitions() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let mut partial = response(address, &[(TOPIC, 2)]);
            partial.topics[0].partitions[1].leader_id = (-1).into();
            serve_metadata_snapshot(&mut socket, partial).await;
            serve_fetch_positions(&mut socket, &[(0, 42)]).await;
            // The next ordinary poll must fetch the healthy partition without
            // probing Metadata again while the partial snapshot's backoff runs.
            serve_fetch_positions(&mut socket, &[(0, 43)]).await;
            serve_metadata_snapshot(&mut socket, response(address, &[(TOPIC, 2)])).await;
            serve_fetch_positions(&mut socket, &[(0, 44), (1, 42)]).await;
            // A complete snapshot no longer schedules a recovery probe.
            serve_fetch_positions(&mut socket, &[(0, 45), (1, 43)]).await;
        });
        let mut consumer = consumer(address, vec![TOPIC.to_owned()]).await;
        let native = native(&mut consumer);
        native.retry_backoff = std::time::Duration::from_secs(86_400);
        native.coordinator = Some(address.to_string());
        native.offsets = HashMap::from([(TOPIC.to_owned(), HashMap::from([(0, 42), (1, 42)]))]);
        checked(native.poll()).await.unwrap();
        assert_eq!(native.leaders.len(), 1);
        assert!(native.metadata_refresh_needed);
        let pending_refresh = native.last_partial_metadata_refresh;
        assert!(pending_refresh.is_some());
        assert_eq!(native.offsets[TOPIC], HashMap::from([(0, 43), (1, 42)]));
        checked(native.poll()).await.unwrap();
        assert_eq!(native.last_partial_metadata_refresh, pending_refresh);
        assert_eq!(native.offsets[TOPIC], HashMap::from([(0, 44), (1, 42)]));

        // Make the pending probe due without a wall-clock sleep.
        native.retry_backoff = std::time::Duration::ZERO;
        let sets = checked(native.poll()).await.unwrap();
        assert_eq!(
            sets.iter().map(|set| set.messages().len()).sum::<usize>(),
            2
        );
        assert_eq!(native.leaders.len(), 2);
        assert!(!native.metadata_refresh_needed);
        assert!(native.last_partial_metadata_refresh.is_none());
        assert_eq!(native.offsets[TOPIC], HashMap::from([(0, 45), (1, 43)]));
        checked(native.poll()).await.unwrap();
        assert_eq!(native.offsets[TOPIC], HashMap::from([(0, 46), (1, 44)]));
        assert_eq!(native.coordinator, Some(address.to_string()));
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn metadata_refresh_is_atomic_for_malformed_and_unavailable_candidates_and_recovers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            for stage in [
                "good",
                "node",
                "duplicate-partition",
                "extra",
                "unavailable",
                "partial",
                "recovered",
            ] {
                let (header, request) =
                    request::<MetadataRequest>(&mut socket, ApiKey::Metadata, API_VERSION_METADATA)
                        .await;
                assert_eq!(
                    request.topics.unwrap().len(),
                    2,
                    "duplicate subscriptions must be encoded once"
                );
                let mut metadata = response(address, &[(TOPIC, 1), ("other-topic", 1)]);
                match stage {
                    "node" => metadata.brokers[0].node_id = (-1).into(),
                    "duplicate-partition" => {
                        let duplicate = metadata.topics[0].partitions[0].clone();
                        metadata.topics[0].partitions.push(duplicate);
                    }
                    "extra" => metadata
                        .topics
                        .push(response(address, &[("unsubscribed", 1)]).topics.remove(0)),
                    "unavailable" => {
                        for topic in &mut metadata.topics {
                            topic.partitions[0].leader_id = (-1).into();
                        }
                    }
                    "partial" => {
                        metadata.topics[1].error_code = KafkaCode::TopicAuthorizationFailed as i16
                    }
                    _ => {}
                }
                reply(&mut socket, &header, API_VERSION_METADATA, metadata).await;
            }
        });
        let mut consumer = consumer(
            address,
            vec![TOPIC.to_owned(), TOPIC.to_owned(), "other-topic".to_owned()],
        )
        .await;
        let native = native(&mut consumer);
        checked(native.refresh_metadata()).await.unwrap();
        let expected = native.leaders.clone();
        native.metadata_refresh_needed = true;
        native.last_partial_metadata_refresh = Some(tokio::time::Instant::now());
        let pending_refresh = native.last_partial_metadata_refresh;
        native.offsets = HashMap::from([(TOPIC.to_owned(), HashMap::from([(0, 43)]))]);
        native.dirty_offsets = native.offsets.clone();
        let positions = native.offsets.clone();
        for _ in 0..3 {
            assert!(matches!(
                checked(native.refresh_metadata()).await,
                Err(Error::Protocol(ProtocolError::Codec))
            ));
            assert_eq!(native.leaders, expected);
            assert!(native.metadata_refresh_needed);
            assert_eq!(native.last_partial_metadata_refresh, pending_refresh);
        }
        assert!(matches!(
            checked(native.refresh_metadata()).await,
            Err(Error::Kafka(KafkaCode::LeaderNotAvailable))
        ));
        assert_eq!(native.leaders, expected);
        assert!(native.metadata_refresh_needed);
        assert_eq!(native.last_partial_metadata_refresh, pending_refresh);
        checked(native.refresh_metadata()).await.unwrap();
        assert_eq!(native.leaders.len(), 1);
        assert!(native.metadata_refresh_needed);
        assert!(native.last_partial_metadata_refresh.is_some());
        assert!(native.leaders.contains_key(&(TOPIC.to_owned(), 0)));
        checked(native.refresh_metadata()).await.unwrap();
        assert_eq!(native.leaders, expected);
        assert!(!native.metadata_refresh_needed);
        assert!(native.last_partial_metadata_refresh.is_none());
        assert_eq!(native.offsets, positions);
        assert_eq!(native.dirty_offsets, positions);
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn invalid_find_does_not_overwrite_a_valid_cache_and_later_valid_find_routes_to_the_endpoint()
     {
        let bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap_address = bootstrap.local_addr().unwrap();
        let coordinator = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator_address = coordinator.local_addr().unwrap();
        let bootstrap_server = tokio::spawn(async move {
            let (mut socket, _) = checked(bootstrap.accept()).await.unwrap();
            let (header, _) = request::<kafka_protocol::messages::FindCoordinatorRequest>(
                &mut socket,
                ApiKey::FindCoordinator,
                API_VERSION_FIND_COORDINATOR,
            )
            .await;
            reply(
                &mut socket,
                &header,
                API_VERSION_FIND_COORDINATOR,
                valid_coordinator(bootstrap_address).with_port(65_536),
            )
            .await;
            serve_find(&mut socket, coordinator_address).await;
        });
        let coordinator_server = tokio::spawn(async move {
            let (mut socket, _) = checked(coordinator.accept()).await.unwrap();
            serve_offsets(&mut socket).await;
        });
        let mut consumer = consumer(bootstrap_address, vec![TOPIC.to_owned()]).await;
        let native = native(&mut consumer);
        native.coordinator = Some(bootstrap_address.to_string());
        assert!(matches!(
            checked(native.refresh_coordinator()).await,
            Err(Error::Protocol(ProtocolError::Codec))
        ));
        assert_eq!(native.coordinator, Some(bootstrap_address.to_string()));
        checked(native.refresh_coordinator()).await.unwrap();
        assert_eq!(native.coordinator, Some(coordinator_address.to_string()));
        let offsets = checked(native.fetch_committed_offsets(&[(TOPIC.to_owned(), 0)]))
            .await
            .unwrap();
        assert_eq!(offsets[TOPIC][&0], 42);
        checked(bootstrap_server).await.unwrap();
        checked(coordinator_server).await.unwrap();
    }

    #[tokio::test]
    async fn metadata_io_failure_keeps_validated_routes_and_the_coordinator_cache() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let _ = request::<MetadataRequest>(&mut socket, ApiKey::Metadata, API_VERSION_METADATA)
                .await;
        });
        let mut consumer = consumer(address, vec![TOPIC.to_owned()]).await;
        let native = native(&mut consumer);
        native.coordinator = Some(address.to_string());
        native.leaders = HashMap::from([((TOPIC.to_owned(), 0), address.to_string())]);
        let expected = native.leaders.clone();
        native.metadata_refresh_needed = true;
        native.last_partial_metadata_refresh = Some(tokio::time::Instant::now());
        let pending_refresh = native.last_partial_metadata_refresh;
        assert!(matches!(
            checked(native.refresh_metadata()).await,
            Err(Error::Connection(_))
        ));
        assert_eq!(native.leaders, expected);
        assert!(native.metadata_refresh_needed);
        assert_eq!(native.last_partial_metadata_refresh, pending_refresh);
        assert_eq!(native.coordinator, Some(address.to_string()));
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn terminal_commit_coordinator_rejection_preserves_dirty_then_next_manual_commit_discovers()
     {
        for code in [
            KafkaCode::GroupLoadInProgress,
            KafkaCode::GroupCoordinatorNotAvailable,
            KafkaCode::NotCoordinatorForGroup,
        ] {
            let bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let new = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let bootstrap_address = bootstrap.local_addr().unwrap();
            let old_address = old.local_addr().unwrap();
            let new_address = new.local_addr().unwrap();
            let bootstrap_server = tokio::spawn(async move {
                let (mut socket, _) = checked(bootstrap.accept()).await.unwrap();
                serve_optional_find(&mut socket, new_address).await;
            });
            let old_server = tokio::spawn(async move {
                let (mut socket, _) = checked(old.accept()).await.unwrap();
                serve_commit(&mut socket, code as i16).await;
                serve_optional_find(&mut socket, new_address).await;
            });
            let new_server = tokio::spawn(async move {
                let (mut socket, _) = checked(new.accept()).await.unwrap();
                serve_commit(&mut socket, 0).await;
            });
            let mut consumer = consumer(bootstrap_address, vec![TOPIC.to_owned()]).await;
            let native = native(&mut consumer);
            native.coordinator = Some(old_address.to_string());
            native.offsets = HashMap::from([(TOPIC.to_owned(), HashMap::from([(0, 43)]))]);
            native.dirty_offsets = native.offsets.clone();
            assert!(
                matches!(checked(native.commit()).await, Err(Error::Kafka(error)) if error == code)
            );
            assert!(native.coordinator.is_none());
            assert_eq!(native.dirty_offsets, native.offsets);
            checked(native.commit()).await.unwrap();
            assert_eq!(native.dirty_offsets, super::super::TopicOffsets::new());
            assert_eq!(native.coordinator, Some(new_address.to_string()));
            drop(consumer);
            checked(bootstrap_server).await.unwrap();
            checked(old_server).await.unwrap();
            checked(new_server).await.unwrap();
        }
    }

    #[tokio::test]
    async fn coordinator_offset_fetch_connection_and_response_io_allow_later_discovery() {
        for connect_failure in [false, true] {
            let bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let new = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let bootstrap_address = bootstrap.local_addr().unwrap();
            let old_address = old.local_addr().unwrap();
            let new_address = new.local_addr().unwrap();
            let bootstrap_server = tokio::spawn(async move {
                let (mut socket, _) = checked(bootstrap.accept()).await.unwrap();
                serve_find(&mut socket, new_address).await;
            });
            let old_server = if connect_failure {
                drop(old);
                None
            } else {
                Some(tokio::spawn(async move {
                    let (mut socket, _) = checked(old.accept()).await.unwrap();
                    let _ = request::<OffsetFetchRequest>(
                        &mut socket,
                        ApiKey::OffsetFetch,
                        API_VERSION_OFFSET_FETCH,
                    )
                    .await;
                    // The coordinator closes before its response is available.
                }))
            };
            let new_server = tokio::spawn(async move {
                let (mut socket, _) = checked(new.accept()).await.unwrap();
                serve_offsets(&mut socket).await;
            });
            let mut consumer = consumer(bootstrap_address, vec![TOPIC.to_owned()]).await;
            let native = native(&mut consumer);
            native.coordinator = Some(old_address.to_string());
            let partitions = [(TOPIC.to_owned(), 0)];
            assert!(matches!(
                checked(native.fetch_committed_offsets(&partitions)).await,
                Err(Error::Connection(_))
            ));
            assert!(native.coordinator.is_none());
            assert_eq!(native.offsets, super::super::TopicOffsets::new());
            assert_eq!(native.dirty_offsets, super::super::TopicOffsets::new());
            checked(native.refresh_coordinator()).await.unwrap();
            let committed = checked(native.fetch_committed_offsets(&partitions))
                .await
                .unwrap();
            assert_eq!(committed[TOPIC][&0], 42);
            assert_eq!(native.coordinator, Some(new_address.to_string()));
            drop(consumer);
            checked(bootstrap_server).await.unwrap();
            if let Some(server) = old_server {
                checked(server).await.unwrap();
            }
            checked(new_server).await.unwrap();
        }
    }

    #[tokio::test]
    async fn terminal_commit_connection_or_response_io_preserves_dirty_until_manual_recovery() {
        for connect_failure in [false, true] {
            let bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let new = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let bootstrap_address = bootstrap.local_addr().unwrap();
            let old_address = old.local_addr().unwrap();
            let new_address = new.local_addr().unwrap();
            let bootstrap_server = tokio::spawn(async move {
                let (mut socket, _) = checked(bootstrap.accept()).await.unwrap();
                serve_find(&mut socket, new_address).await;
            });
            let old_server = if connect_failure {
                drop(old);
                None
            } else {
                Some(tokio::spawn(async move {
                    let (mut socket, _) = checked(old.accept()).await.unwrap();
                    let _ = request::<OffsetCommitRequest>(
                        &mut socket,
                        ApiKey::OffsetCommit,
                        API_VERSION_OFFSET_COMMIT,
                    )
                    .await;
                }))
            };
            let new_server = tokio::spawn(async move {
                let (mut socket, _) = checked(new.accept()).await.unwrap();
                serve_commit(&mut socket, 0).await;
            });
            let mut consumer = consumer(bootstrap_address, vec![TOPIC.to_owned()]).await;
            let native = native(&mut consumer);
            native.coordinator = Some(old_address.to_string());
            native.offsets = HashMap::from([(TOPIC.to_owned(), HashMap::from([(0, 43)]))]);
            native.dirty_offsets = native.offsets.clone();
            assert!(matches!(
                checked(native.commit()).await,
                Err(Error::Connection(_))
            ));
            assert!(native.coordinator.is_none());
            assert_eq!(native.dirty_offsets, native.offsets);
            checked(native.commit()).await.unwrap();
            assert_eq!(native.dirty_offsets, super::super::TopicOffsets::new());
            assert_eq!(native.coordinator, Some(new_address.to_string()));
            drop(consumer);
            checked(bootstrap_server).await.unwrap();
            if let Some(server) = old_server {
                checked(server).await.unwrap();
            }
            checked(new_server).await.unwrap();
        }
    }

    #[tokio::test]
    async fn terminal_leader_connection_io_does_not_invalidate_the_group_coordinator() {
        let bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap_address = bootstrap.local_addr().unwrap();
        let dead_leader = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_address = dead_leader.local_addr().unwrap();
        let recovered = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let recovered_address = recovered.local_addr().unwrap();
        drop(dead_leader);
        let bootstrap_server = tokio::spawn(async move {
            let (mut socket, _) = checked(bootstrap.accept()).await.unwrap();
            let (header, _) =
                request::<MetadataRequest>(&mut socket, ApiKey::Metadata, API_VERSION_METADATA)
                    .await;
            // Metadata recovery must occur without another FindCoordinator.
            reply(
                &mut socket,
                &header,
                API_VERSION_METADATA,
                response(recovered_address, &[(TOPIC, 1)]),
            )
            .await;
        });
        let recovered_server = tokio::spawn(async move {
            let (mut socket, _) = checked(recovered.accept()).await.unwrap();
            serve_fetch(&mut socket).await;
        });
        let mut consumer = consumer(bootstrap_address, vec![TOPIC.to_owned()]).await;
        let native = native(&mut consumer);
        native.coordinator = Some(bootstrap_address.to_string());
        native.leaders = HashMap::from([((TOPIC.to_owned(), 0), dead_address.to_string())]);
        native.offsets = HashMap::from([(TOPIC.to_owned(), HashMap::from([(0, 42)]))]);
        native.retry_backoff = std::time::Duration::from_secs(86_400);
        native.metadata_refresh_needed = true;
        native.last_partial_metadata_refresh = Some(tokio::time::Instant::now());
        assert!(matches!(
            checked(native.poll()).await,
            Err(Error::Connection(_))
        ));
        assert_eq!(native.coordinator, Some(bootstrap_address.to_string()));
        assert_eq!(native.offsets[TOPIC][&0], 42);
        assert_eq!(native.dirty_offsets, super::super::TopicOffsets::new());
        assert!(native.metadata_refresh_needed);
        assert!(native.last_partial_metadata_refresh.is_none());
        let sets = checked(native.poll()).await.unwrap();
        assert_eq!(sets.iter().next().unwrap().messages()[0].offset, 42);
        assert_eq!(native.coordinator, Some(bootstrap_address.to_string()));
        assert_eq!(native.offsets[TOPIC][&0], 43);
        assert!(!native.metadata_refresh_needed);
        drop(consumer);
        checked(bootstrap_server).await.unwrap();
        checked(recovered_server).await.unwrap();
    }
}
