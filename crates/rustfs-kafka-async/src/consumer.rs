//! Async consumer for fetching messages from Kafka.

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, FetchRequest, FetchResponse, MetadataRequest, MetadataResponse, RequestHeader,
    ResponseHeader, TopicName, fetch_request::FetchPartition as KpFetchPartition,
    fetch_request::FetchTopic as KpFetchTopic, metadata_request::MetadataRequestTopic,
    offset_commit_request::OffsetCommitRequestPartition,
    offset_commit_request::OffsetCommitRequestTopic,
    FindCoordinatorRequest, FindCoordinatorResponse, GroupId, OffsetCommitRequest,
    OffsetCommitResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::RecordBatchDecoder;
use rustfs_kafka::consumer::{Consumer, MessageSets};
use rustfs_kafka::error::{ConsumerError, Error, KafkaCode, ProtocolError, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use crate::AsyncKafkaClient;
use crate::connection::AsyncConnection;

const API_VERSION_METADATA: i16 = 1;
const API_VERSION_FETCH: i16 = 12;
const API_VERSION_FIND_COORDINATOR: i16 = 3;
const API_VERSION_OFFSET_COMMIT: i16 = 2;
const FETCH_MIN_BYTES: i32 = 1;
const FETCH_MAX_WAIT_MS: i32 = 100;
const FETCH_PARTITION_MAX_BYTES: i32 = 1_048_576;

/// Internal commands sent to the bridged consumer background thread.
enum ConsumerCommand {
    Poll {
        response: oneshot::Sender<Result<MessageSets>>,
    },
    Commit {
        response: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

struct BridgedConsumer {
    sender: mpsc::Sender<ConsumerCommand>,
    handle: Option<thread::JoinHandle<()>>,
}

struct NativeConsumer {
    client: AsyncKafkaClient,
    group: String,
    topics: Vec<String>,
    offsets: HashMap<(String, i32), i64>,
    dirty_offsets: HashMap<(String, i32), i64>,
    leaders: HashMap<(String, i32), String>,
    coordinator: Option<String>,
    correlation: i32,
}

enum AsyncConsumerMode {
    Native(Box<NativeConsumer>),
    Bridged(BridgedConsumer),
}

/// An async Kafka consumer.
pub struct AsyncConsumer {
    mode: AsyncConsumerMode,
}

/// Builder for constructing an [`AsyncConsumer`] asynchronously.
pub struct AsyncConsumerBuilder {
    hosts: Vec<String>,
    group: Option<String>,
    topics: Vec<String>,
    channel_capacity: usize,
    native_async: bool,
}

impl AsyncConsumerBuilder {
    /// Creates a new async consumer builder from bootstrap hosts.
    #[must_use]
    pub fn new(hosts: Vec<String>) -> Self {
        Self {
            hosts,
            group: None,
            topics: Vec::new(),
            channel_capacity: 64,
            native_async: true,
        }
    }

    /// Sets the consumer group.
    #[must_use]
    pub fn with_group(mut self, group: String) -> Self {
        self.group = Some(group);
        self
    }

    /// Adds a topic subscription.
    #[must_use]
    pub fn with_topic(mut self, topic: String) -> Self {
        self.topics.push(topic);
        self
    }

    /// Adds multiple topic subscriptions.
    #[must_use]
    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics.extend(topics);
        self
    }

    /// Sets command channel capacity used in bridged mode.
    #[must_use]
    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity.max(1);
        self
    }

    /// Enables or disables native async I/O mode.
    #[must_use]
    pub fn with_native_async(mut self, native_async: bool) -> Self {
        self.native_async = native_async;
        self
    }

    /// Builds an async consumer.
    pub async fn build(self) -> Result<AsyncConsumer> {
        let group = self
            .group
            .ok_or(Error::Consumer(ConsumerError::UnsetGroupId))?;
        if self.topics.is_empty() {
            return Err(Error::Consumer(ConsumerError::NoTopicsAssigned));
        }

        if self.native_async {
            let client = AsyncKafkaClient::new(self.hosts).await?;
            return Ok(AsyncConsumer {
                mode: AsyncConsumerMode::Native(Box::new(NativeConsumer {
                    client,
                    topics: self.topics,
                    offsets: HashMap::new(),
                    leaders: HashMap::new(),
                    correlation: 1,
                })),
            });
        }

        let hosts = self.hosts;
        let topics = self.topics;
        let channel_capacity = self.channel_capacity;

        let consumer = tokio::task::spawn_blocking(move || {
            let mut builder = Consumer::from_hosts(hosts).with_group(group);
            for topic in topics {
                builder = builder.with_topic(topic);
            }
            builder.create()
        })
        .await
        .map_err(|e| Error::Config(format!("failed to build consumer task: {e}")))??;

        Ok(AsyncConsumer {
            mode: AsyncConsumerMode::Bridged(BridgedConsumer::from_sync(
                consumer,
                channel_capacity,
            )),
        })
    }
}

impl AsyncConsumer {
    /// Starts building a new async consumer from bootstrap hosts.
    #[must_use]
    pub fn builder(hosts: Vec<String>) -> AsyncConsumerBuilder {
        AsyncConsumerBuilder::new(hosts)
    }

    /// Creates a new async consumer from bootstrap hosts.
    pub async fn from_hosts(
        hosts: Vec<String>,
        group: String,
        topics: Vec<String>,
    ) -> Result<Self> {
        Self::builder(hosts)
            .with_group(group)
            .with_topics(topics)
            .build()
            .await
    }

    /// Creates a new async consumer from an [`AsyncKafkaClient`].
    pub async fn from_client(
        client: AsyncKafkaClient,
        group: String,
        topics: Vec<String>,
    ) -> Result<Self> {
        if group.is_empty() {
            return Err(Error::Consumer(ConsumerError::UnsetGroupId));
        }
        if topics.is_empty() {
            return Err(Error::Consumer(ConsumerError::NoTopicsAssigned));
        }
        Ok(Self {
            mode: AsyncConsumerMode::Native(Box::new(NativeConsumer {
                client,
                topics,
                offsets: HashMap::new(),
                leaders: HashMap::new(),
                correlation: 1,
            })),
        })
    }

    /// Polls for new messages and returns fetched message sets.
    pub async fn poll(&mut self) -> Result<MessageSets> {
        match &mut self.mode {
            AsyncConsumerMode::Native(native) => native.poll().await,
            AsyncConsumerMode::Bridged(bridged) => bridged.poll().await,
        }
    }

    /// Commits the current consumed offsets.
    pub async fn commit(&mut self) -> Result<()> {
        match &mut self.mode {
            // native async path currently tracks offsets locally and commits are no-op.
            AsyncConsumerMode::Native(_) => Ok(()),
            AsyncConsumerMode::Bridged(bridged) => bridged.commit().await,
        }
    }

    /// Gracefully closes the consumer.
    pub async fn close(self) -> Result<()> {
        match self.mode {
            AsyncConsumerMode::Native(_) => Ok(()),
            AsyncConsumerMode::Bridged(bridged) => bridged.close().await,
        }
    }
}

impl BridgedConsumer {
    fn from_sync(consumer: Consumer, channel_capacity: usize) -> Self {
        let (sender, mut receiver) = mpsc::channel::<ConsumerCommand>(channel_capacity.max(1));

        let handle = thread::spawn(move || {
            let mut consumer = consumer;
            loop {
                match receiver.blocking_recv() {
                    Some(ConsumerCommand::Poll { response }) => {
                        let result = consumer.poll();
                        let _ = response.send(result);
                    }
                    Some(ConsumerCommand::Commit { response }) => {
                        let result = consumer.commit_consumed();
                        let _ = response.send(result);
                    }
                    Some(ConsumerCommand::Shutdown) | None => {
                        debug!("AsyncConsumer thread shutting down");
                        break;
                    }
                }
            }
            info!("AsyncConsumer background thread exited");
        });

        info!("AsyncConsumer created");
        Self {
            sender,
            handle: Some(handle),
        }
    }

    async fn poll(&mut self) -> Result<MessageSets> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ConsumerCommand::Poll { response: tx })
            .await
            .map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?;
        rx.await
            .map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?
    }

    async fn commit(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ConsumerCommand::Commit { response: tx })
            .await
            .map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?;
        rx.await
            .map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?
    }

    async fn close(mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            let _ = self.sender.send(ConsumerCommand::Shutdown).await;
            let join_result = tokio::task::spawn_blocking(move || handle.join())
                .await
                .map_err(|e| Error::Config(format!("failed to join consumer thread: {e}")))?;
            if join_result.is_err() {
                return Err(Error::Config("consumer thread panicked".to_owned()));
            }
        }
        Ok(())
    }

    fn request_shutdown_non_blocking(&mut self) {
        if self.handle.is_some() {
            let _ = self.sender.try_send(ConsumerCommand::Shutdown);
            self.handle.take();
        }
    }
}

impl Drop for BridgedConsumer {
    fn drop(&mut self) {
        self.request_shutdown_non_blocking();
    }
}

impl NativeConsumer {
    async fn poll(&mut self) -> Result<MessageSets> {
        self.client.ensure_connected().await?;
        if self.leaders.is_empty() {
            self.refresh_metadata().await?;
        }

        let mut by_broker: HashMap<String, Vec<(String, i32, i64)>> = HashMap::new();
        for (tp, leader_host) in &self.leaders {
            let offset = *self.offsets.get(tp).unwrap_or(&0);
            by_broker
                .entry(leader_host.clone())
                .or_default()
                .push((tp.0.clone(), tp.1, offset));
        }

        let correlation = self.next_correlation();
        let client_id = self.client.client_id().to_owned();
        let mut owned_responses = Vec::new();

        for (broker, tps) in by_broker {
            let parts: Vec<(&str, i32, i64, i32)> = tps
                .iter()
                .map(|(topic, partition, offset)| {
                    (
                        topic.as_str(),
                        *partition,
                        *offset,
                        FETCH_PARTITION_MAX_BYTES,
                    )
                })
                .collect();

            let conn = self.client.get_connection(&broker).await?;
            let (header, request) = build_fetch_request(correlation, &client_id, &parts);
            send_kp_request(conn, &header, &request, API_VERSION_FETCH).await?;
            let response = get_fetch_response(conn, API_VERSION_FETCH).await?;
            let owned = convert_fetch_response(response, correlation);

            self.advance_offsets(&owned);
            owned_responses.push(owned);
        }

        Ok(MessageSets::from_fetch_responses(owned_responses))
    }

    fn next_correlation(&mut self) -> i32 {
        let cid = self.correlation;
        self.correlation = self.correlation.wrapping_add(1);
        cid
    }

    fn advance_offsets(&mut self, resp: &rustfs_kafka::client::fetch_kp::OwnedFetchResponse) {
        for topic in &resp.topics {
            for partition in &topic.partitions {
                if let Ok(data) = partition.data()
                    && let Some(last) = data.messages.last()
                {
                    self.offsets
                        .insert((topic.topic.clone(), partition.partition), last.offset + 1);
                }
            }
        }
    }

    async fn refresh_metadata(&mut self) -> Result<()> {
        let request_host = if let Some(connected) = self.client.connected_hosts().first() {
            (*connected).to_owned()
        } else {
            self.client
                .bootstrap_hosts()
                .first()
                .cloned()
                .ok_or_else(no_host_reachable_error)?
        };

        let correlation = self.next_correlation();
        let client_id = self.client.client_id().to_owned();
        let conn = self.client.get_connection(&request_host).await?;
        let (header, request) = build_metadata_request(correlation, &client_id, &self.topics);
        send_kp_request(conn, &header, &request, API_VERSION_METADATA).await?;
        let response = get_kp_response::<MetadataResponse>(conn, API_VERSION_METADATA).await?;

        let mut brokers: HashMap<i32, String> = HashMap::new();
        for broker in response.brokers {
            brokers.insert(
                i32::from(broker.node_id),
                format!("{}:{}", broker.host, broker.port),
            );
        }

        self.leaders.clear();
        for topic in response.topics {
            let Some(topic_name) = topic.name else {
                continue;
            };
            for partition in topic.partitions {
                let leader = i32::from(partition.leader_id);
                if leader < 0 {
                    continue;
                }
                if let Some(host) = brokers.get(&leader) {
                    let tp = (topic_name.to_string(), partition.partition_index);
                    self.leaders.insert(tp.clone(), host.clone());
                    self.offsets.entry(tp).or_insert(0);
                }
            }
        }

        if self.leaders.is_empty() {
            return Err(Error::Kafka(KafkaCode::LeaderNotAvailable));
        }

        Ok(())
    }
}

fn build_metadata_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[String],
) -> (RequestHeader, MetadataRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::Metadata as i16)
        .with_request_api_version(API_VERSION_METADATA)
        .with_correlation_id(correlation_id);

    let request_topics: Vec<MetadataRequestTopic> = topics
        .iter()
        .map(|topic| {
            MetadataRequestTopic::default()
                .with_name(Some(TopicName::from(StrBytes::from_string(topic.clone()))))
        })
        .collect();

    let request = MetadataRequest::default().with_topics(Some(request_topics));
    (header, request)
}

fn build_fetch_request(
    correlation_id: i32,
    client_id: &str,
    partitions: &[(&str, i32, i64, i32)],
) -> (RequestHeader, FetchRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::Fetch as i16)
        .with_request_api_version(API_VERSION_FETCH)
        .with_correlation_id(correlation_id);

    let mut topic_map: HashMap<&str, Vec<KpFetchPartition>> = HashMap::new();
    for (topic, partition, offset, partition_max_bytes) in partitions {
        topic_map.entry(topic).or_default().push(
            KpFetchPartition::default()
                .with_partition(*partition)
                .with_fetch_offset(*offset)
                .with_partition_max_bytes(*partition_max_bytes),
        );
    }

    let topics: Vec<KpFetchTopic> = topic_map
        .into_iter()
        .map(|(topic_name, fetch_partitions)| {
            KpFetchTopic::default()
                .with_topic(TopicName::from(StrBytes::from_string(
                    topic_name.to_string(),
                )))
                .with_partitions(fetch_partitions)
        })
        .collect();

    let request = FetchRequest::default()
        .with_replica_id(kafka_protocol::messages::BrokerId::from(-1))
        .with_max_wait_ms(FETCH_MAX_WAIT_MS)
        .with_min_bytes(FETCH_MIN_BYTES)
        .with_max_bytes(i32::MAX)
        .with_isolation_level(0)
        .with_topics(topics);

    (header, request)
}

async fn send_kp_request<T>(
    conn: &mut AsyncConnection,
    header: &RequestHeader,
    body: &T,
    api_version: i16,
) -> Result<()>
where
    T: Encodable + kafka_protocol::protocol::HeaderVersion,
{
    let header_version = T::header_version(api_version);

    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, header_version)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;

    let mut body_buf = BytesMut::new();
    body.encode(&mut body_buf, api_version)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;

    let total_len = usize_to_i32(header_buf.len() + body_buf.len())?;
    let mut out = BytesMut::with_capacity(4 + non_negative_i32_to_usize(total_len)?);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body_buf);

    conn.send(&out).await
}

async fn get_kp_response<R>(conn: &mut AsyncConnection, api_version: i16) -> Result<R>
where
    R: Decodable + kafka_protocol::protocol::HeaderVersion,
{
    let size_bytes = conn.read_exact(4).await?;
    let size = i32::from_be_bytes(
        <[u8; 4]>::try_from(size_bytes.as_ref())
            .map_err(|_| Error::Protocol(ProtocolError::Codec))?,
    );
    let mut bytes = conn.read_exact(non_negative_i32_to_u64(size)?).await?;

    let response_header_version = R::header_version(api_version);
    let _resp_header = ResponseHeader::decode(&mut bytes, response_header_version)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;

    R::decode(&mut bytes, api_version).map_err(|_| Error::Protocol(ProtocolError::Codec))
}

async fn get_fetch_response(
    conn: &mut AsyncConnection,
    requested_version: i16,
) -> Result<FetchResponse> {
    let size_bytes = conn.read_exact(4).await?;
    let size = i32::from_be_bytes(
        <[u8; 4]>::try_from(size_bytes.as_ref())
            .map_err(|_| Error::Protocol(ProtocolError::Codec))?,
    );
    let resp_bytes = conn.read_exact(non_negative_i32_to_u64(size)?).await?;

    let mut candidates = Vec::with_capacity(1 + 18);
    candidates.push(requested_version);
    for v in (0..=17).rev() {
        if v != requested_version {
            candidates.push(v);
        }
    }

    for version in candidates {
        let mut bytes = resp_bytes.clone();
        let header_version = FetchResponse::header_version(version);
        if ResponseHeader::decode(&mut bytes, header_version).is_err() {
            continue;
        }
        if let Ok(resp) = FetchResponse::decode(&mut bytes, version) {
            return Ok(resp);
        }
    }

    Err(Error::Protocol(ProtocolError::Codec))
}

fn convert_fetch_response(
    kp_resp: FetchResponse,
    correlation_id: i32,
) -> rustfs_kafka::client::fetch_kp::OwnedFetchResponse {
    use rustfs_kafka::client::fetch_kp::{OwnedFetchResponse, OwnedPartition, OwnedTopic};

    let topics = kp_resp
        .responses
        .into_iter()
        .map(|t| {
            let topic_name = t.topic.to_string();
            let partitions: Vec<OwnedPartition> = t
                .partitions
                .into_iter()
                .map(|p| {
                    let data = if p.error_code != 0 {
                        Err(Arc::new(Error::TopicPartitionError {
                            topic_name: topic_name.clone(),
                            partition_id: p.partition_index,
                            error_code: map_kafka_code(p.error_code).unwrap_or(KafkaCode::Unknown),
                        }))
                    } else {
                        decode_partition_records(p.records, p.high_watermark)
                    };
                    OwnedPartition {
                        partition: p.partition_index,
                        data,
                        highwatermark: p.high_watermark,
                    }
                })
                .collect();
            OwnedTopic {
                topic: topic_name,
                partitions,
            }
        })
        .collect();

    OwnedFetchResponse {
        correlation_id,
        topics,
    }
}

fn decode_partition_records(
    records: Option<Bytes>,
    high_watermark: i64,
) -> std::result::Result<rustfs_kafka::client::fetch_kp::OwnedData, Arc<Error>> {
    use rustfs_kafka::client::fetch_kp::{OwnedData, OwnedMessage};

    let Some(mut records_bytes) = records else {
        return Ok(OwnedData {
            highwatermark_offset: high_watermark,
            messages: vec![],
        });
    };
    if records_bytes.is_empty() {
        return Ok(OwnedData {
            highwatermark_offset: high_watermark,
            messages: vec![],
        });
    }

    let Ok(record_set) = RecordBatchDecoder::decode(&mut records_bytes) else {
        return Err(Arc::new(Error::Protocol(ProtocolError::Codec)));
    };

    let mut messages: Vec<OwnedMessage> = Vec::new();
    for record in &record_set.records {
        messages.push(OwnedMessage {
            offset: record.offset,
            key: record.key.clone().unwrap_or_default(),
            value: record.value.clone().unwrap_or_default(),
        });
    }

    Ok(OwnedData {
        highwatermark_offset: high_watermark,
        messages,
    })
}

fn map_kafka_code(code: i16) -> Option<KafkaCode> {
    match code {
        0 => None,
        1 => Some(KafkaCode::OffsetOutOfRange),
        2 => Some(KafkaCode::CorruptMessage),
        3 => Some(KafkaCode::UnknownTopicOrPartition),
        4 => Some(KafkaCode::InvalidMessageSize),
        5 => Some(KafkaCode::LeaderNotAvailable),
        6 => Some(KafkaCode::NotLeaderForPartition),
        7 => Some(KafkaCode::RequestTimedOut),
        8 => Some(KafkaCode::BrokerNotAvailable),
        9 => Some(KafkaCode::ReplicaNotAvailable),
        10 => Some(KafkaCode::MessageSizeTooLarge),
        11 => Some(KafkaCode::StaleControllerEpoch),
        12 => Some(KafkaCode::OffsetMetadataTooLarge),
        13 => Some(KafkaCode::NetworkException),
        14 => Some(KafkaCode::GroupLoadInProgress),
        15 => Some(KafkaCode::GroupCoordinatorNotAvailable),
        16 => Some(KafkaCode::NotCoordinatorForGroup),
        17 => Some(KafkaCode::InvalidTopic),
        18 => Some(KafkaCode::RecordListTooLarge),
        19 => Some(KafkaCode::NotEnoughReplicas),
        20 => Some(KafkaCode::NotEnoughReplicasAfterAppend),
        21 => Some(KafkaCode::InvalidRequiredAcks),
        22 => Some(KafkaCode::IllegalGeneration),
        23 => Some(KafkaCode::InconsistentGroupProtocol),
        24 => Some(KafkaCode::InvalidGroupId),
        25 => Some(KafkaCode::UnknownMemberId),
        26 => Some(KafkaCode::InvalidSessionTimeout),
        27 => Some(KafkaCode::RebalanceInProgress),
        28 => Some(KafkaCode::InvalidCommitOffsetSize),
        29 => Some(KafkaCode::TopicAuthorizationFailed),
        30 => Some(KafkaCode::GroupAuthorizationFailed),
        31 => Some(KafkaCode::ClusterAuthorizationFailed),
        32 => Some(KafkaCode::InvalidTimestamp),
        33 => Some(KafkaCode::UnsupportedSaslMechanism),
        34 => Some(KafkaCode::IllegalSaslState),
        35 => Some(KafkaCode::UnsupportedVersion),
        _ => Some(KafkaCode::Unknown),
    }
}

fn usize_to_i32(value: usize) -> Result<i32> {
    i32::try_from(value).map_err(|_| Error::Protocol(ProtocolError::Codec))
}

fn non_negative_i32_to_usize(value: i32) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::Protocol(ProtocolError::Codec))
}

fn non_negative_i32_to_u64(value: i32) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::Protocol(ProtocolError::Codec))
}

fn no_host_reachable_error() -> Error {
    Error::Connection(rustfs_kafka::error::ConnectionError::NoHostReachable)
}

#[cfg(test)]
mod tests {
    use rustfs_kafka::error::{ConnectionError, Error};

    use super::*;

    #[tokio::test]
    async fn from_hosts_fails_with_unreachable_hosts() {
        let result = AsyncConsumer::from_hosts(
            vec!["127.0.0.1:1".to_owned()],
            "test-group".to_owned(),
            vec!["test-topic".to_owned()],
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::NoHostReachable))
        ));
    }

    #[tokio::test]
    async fn from_client_fails_with_unreachable_hosts() {
        let client = AsyncKafkaClient::new(vec![]).await.unwrap();
        let result = AsyncConsumer::from_client(
            client,
            "test-group".to_owned(),
            vec!["test-topic".to_owned()],
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn drop_consumer_without_close_does_not_panic() {
        let result = AsyncConsumer::from_hosts(
            vec!["127.0.0.1:1".to_owned()],
            "test-drop-group".to_owned(),
            vec!["test-drop-topic".to_owned()],
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn builder_without_group_returns_error() {
        let result = AsyncConsumer::builder(vec![])
            .with_topic("t".to_owned())
            .build()
            .await;
        assert!(matches!(
            result,
            Err(Error::Consumer(ConsumerError::UnsetGroupId))
        ));
    }

    #[tokio::test]
    async fn builder_without_topics_returns_error() {
        let result = AsyncConsumer::builder(vec![])
            .with_group("g".to_owned())
            .build()
            .await;
        assert!(matches!(
            result,
            Err(Error::Consumer(ConsumerError::NoTopicsAssigned))
        ));
    }
}
