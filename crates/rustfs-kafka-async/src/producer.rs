//! Async producer for sending messages to Kafka.

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader,
    ResponseHeader, TopicName, metadata_request::MetadataRequestTopic,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::{
    Record as KpRecord, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use rustfs_kafka::client::{Compression, RequiredAcks, SecurityConfig};
use rustfs_kafka::error::{ConnectionError, Error, KafkaCode, ProtocolError, Result};
use rustfs_kafka::producer::{AsBytes, Producer, Record};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicI32, Ordering};
use std::thread;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{debug, info};

use crate::AsyncKafkaClient;
use crate::connection::AsyncConnection;

const API_VERSION_PRODUCE: i16 = 9;
const API_VERSION_METADATA: i16 = 1;

/// Internal commands sent to the bridged producer background thread.
enum ProducerCommand {
    Send {
        topic: String,
        key: Bytes,
        value: Bytes,
        partition: i32,
        response: oneshot::Sender<Result<()>>,
    },
    Flush {
        response: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

struct BridgedProducer {
    sender: mpsc::Sender<ProducerCommand>,
    handle: Option<thread::JoinHandle<()>>,
}

struct NativeProducer {
    client: Mutex<AsyncKafkaClient>,
    state: Mutex<NativeProducerState>,
    required_acks: i16,
    ack_timeout_ms: i32,
    compression: Compression,
    correlation: AtomicI32,
}

#[derive(Default)]
struct NativeProducerState {
    brokers: HashMap<i32, String>,
    topics: HashMap<String, TopicRoute>,
    round_robin: HashMap<String, usize>,
}

#[derive(Default)]
struct TopicRoute {
    partitions: HashMap<i32, i32>, // partition -> leader_id
    available_partitions: Vec<i32>,
}

enum AsyncProducerMode {
    Native(Box<NativeProducer>),
    Bridged(BridgedProducer),
}

/// An async Kafka producer.
///
/// This producer supports two execution modes:
/// - Native async I/O mode (default): direct async Kafka Metadata/Produce calls
///   using `tokio` sockets.
/// - Bridged mode: runs synchronous `rustfs_kafka::producer::Producer` on a
///   dedicated background thread (used when security config is supplied or
///   when native mode is explicitly disabled).
pub struct AsyncProducer {
    mode: AsyncProducerMode,
}

/// Configuration for constructing an [`AsyncProducer`].
pub struct AsyncProducerConfig {
    required_acks: RequiredAcks,
    ack_timeout: Duration,
    compression: Compression,
    security: Option<SecurityConfig>,
}

impl AsyncProducerConfig {
    #[must_use]
    pub fn new() -> Self {
        Self {
            required_acks: RequiredAcks::One,
            ack_timeout: Duration::from_secs(30),
            compression: Compression::NONE,
            security: None,
        }
    }

    #[must_use]
    pub fn with_required_acks(mut self, required_acks: RequiredAcks) -> Self {
        self.required_acks = required_acks;
        self
    }

    #[must_use]
    pub fn with_ack_timeout(mut self, ack_timeout: Duration) -> Self {
        self.ack_timeout = ack_timeout;
        self
    }

    #[must_use]
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    #[must_use]
    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.security = Some(security);
        self
    }
}

impl Default for AsyncProducerConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for constructing an [`AsyncProducer`] with non-blocking setup.
pub struct AsyncProducerBuilder {
    hosts: Vec<String>,
    client_id: String,
    config: AsyncProducerConfig,
    channel_capacity: usize,
    native_async: bool,
}

impl AsyncProducerBuilder {
    /// Creates a new async producer builder from bootstrap hosts.
    #[must_use]
    pub fn new(hosts: Vec<String>) -> Self {
        Self {
            hosts,
            client_id: "rustfs-kafka-async".to_owned(),
            config: AsyncProducerConfig::default(),
            channel_capacity: 256,
            native_async: true,
        }
    }

    /// Sets the client ID used by the producer.
    #[must_use]
    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.client_id = client_id;
        self
    }

    /// Sets the required acknowledgement level.
    #[must_use]
    pub fn with_required_acks(mut self, required_acks: RequiredAcks) -> Self {
        self.config = self.config.with_required_acks(required_acks);
        self
    }

    /// Sets the maximum acknowledgement wait timeout.
    #[must_use]
    pub fn with_ack_timeout(mut self, ack_timeout: Duration) -> Self {
        self.config = self.config.with_ack_timeout(ack_timeout);
        self
    }

    /// Sets compression for produced record batches.
    #[must_use]
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.config = self.config.with_compression(compression);
        self
    }

    /// Sets optional TLS/SASL security configuration.
    #[must_use]
    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.config = self.config.with_security(security);
        self
    }

    /// Sets command channel capacity for bridged mode.
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

    /// Builds the async producer.
    pub async fn build(self) -> Result<AsyncProducer> {
        let AsyncProducerBuilder {
            hosts,
            client_id,
            config,
            channel_capacity,
            native_async,
        } = self;

        let has_security = config.security.is_some();

        if native_async && !has_security {
            let client = AsyncKafkaClient::with_client_id(hosts, client_id).await?;
            return AsyncProducer::from_native(client, config);
        }

        let sync_producer = tokio::task::spawn_blocking(move || {
            let mut builder = Producer::from_hosts(hosts)
                .with_client_id(client_id)
                .with_required_acks(config.required_acks)
                .with_ack_timeout(config.ack_timeout)
                .with_compression(config.compression);

            if let Some(security) = config.security {
                builder = builder.with_security(security);
            }

            builder.create()
        })
        .await
        .map_err(|e| Error::Config(format!("failed to build producer task: {e}")))??;

        Ok(AsyncProducer {
            mode: AsyncProducerMode::Bridged(BridgedProducer::from_sync(
                sync_producer,
                channel_capacity,
            )),
        })
    }
}

impl AsyncProducer {
    /// Starts building a new async producer from bootstrap hosts.
    #[must_use]
    pub fn builder(hosts: Vec<String>) -> AsyncProducerBuilder {
        AsyncProducerBuilder::new(hosts)
    }

    /// Creates a new async producer from an [`AsyncKafkaClient`].
    pub async fn new(client: AsyncKafkaClient) -> Result<Self> {
        Self::new_with_config(client, AsyncProducerConfig::default()).await
    }

    /// Creates a new async producer with explicit configuration.
    pub async fn new_with_config(
        client: AsyncKafkaClient,
        config: AsyncProducerConfig,
    ) -> Result<Self> {
        if config.security.is_none() {
            return Self::from_native(client, config);
        }

        // security flow still uses bridged sync producer path
        Self::builder(client.bootstrap_hosts().to_vec())
            .with_client_id(client.client_id().to_owned())
            .with_required_acks(config.required_acks)
            .with_ack_timeout(config.ack_timeout)
            .with_compression(config.compression)
            .build_with_optional_security(config.security)
            .await
    }

    /// Creates a new async producer directly from bootstrap hosts.
    pub async fn from_hosts(hosts: Vec<String>) -> Result<Self> {
        Self::builder(hosts).build().await
    }

    /// Creates a new async producer from hosts with explicit configuration.
    pub async fn from_hosts_with_config(
        hosts: Vec<String>,
        config: AsyncProducerConfig,
    ) -> Result<Self> {
        Self::builder(hosts)
            .with_required_acks(config.required_acks)
            .with_ack_timeout(config.ack_timeout)
            .with_compression(config.compression)
            .build_with_optional_security(config.security)
            .await
    }

    /// Sends a message to Kafka asynchronously.
    pub async fn send<K, V>(&self, record: &Record<'_, K, V>) -> Result<()>
    where
        K: AsBytes,
        V: AsBytes,
    {
        match &self.mode {
            AsyncProducerMode::Native(native) => native.send(record).await,
            AsyncProducerMode::Bridged(bridged) => bridged.send(record).await,
        }
    }

    /// Flushes any pending messages.
    pub async fn flush(&self) -> Result<()> {
        match &self.mode {
            AsyncProducerMode::Native(_) => Ok(()),
            AsyncProducerMode::Bridged(bridged) => bridged.flush().await,
        }
    }

    /// Gracefully shuts down the producer.
    pub async fn close(self) -> Result<()> {
        match self.mode {
            AsyncProducerMode::Native(_) => Ok(()),
            AsyncProducerMode::Bridged(bridged) => bridged.close().await,
        }
    }

    fn from_native(client: AsyncKafkaClient, config: AsyncProducerConfig) -> Result<Self> {
        if client.bootstrap_hosts().is_empty() {
            return Err(no_host_reachable_error());
        }

        let ack_timeout_ms = to_millis_i32(config.ack_timeout)?;
        Ok(Self {
            mode: AsyncProducerMode::Native(
                NativeProducer {
                    client: Mutex::new(client),
                    state: Mutex::new(NativeProducerState::default()),
                    required_acks: config.required_acks as i16,
                    ack_timeout_ms,
                    compression: config.compression,
                    correlation: AtomicI32::new(1),
                }
                .into(),
            ),
        })
    }
}

impl AsyncProducerBuilder {
    async fn build_with_optional_security(
        self,
        security: Option<SecurityConfig>,
    ) -> Result<AsyncProducer> {
        if let Some(security) = security {
            self.with_security(security).build().await
        } else {
            self.build().await
        }
    }
}

impl BridgedProducer {
    fn from_sync(sync_producer: Producer, channel_capacity: usize) -> Self {
        let (sender, mut receiver) = mpsc::channel::<ProducerCommand>(channel_capacity.max(1));

        let handle = thread::spawn(move || {
            let mut producer = sync_producer;
            while let Some(cmd) = receiver.blocking_recv() {
                match cmd {
                    ProducerCommand::Send {
                        topic,
                        key,
                        value,
                        partition,
                        response,
                    } => {
                        let record =
                            Record::from_key_value(topic.as_str(), key.as_ref(), value.as_ref())
                                .with_partition(partition);
                        let result = producer.send(&record);
                        let _ = response.send(result);
                    }
                    ProducerCommand::Flush { response } => {
                        let _ = response.send(Ok(()));
                    }
                    ProducerCommand::Shutdown => {
                        debug!("Async producer shutting down");
                        break;
                    }
                }
            }
            info!("Async producer background thread exited");
        });

        Self {
            sender,
            handle: Some(handle),
        }
    }

    async fn send<K, V>(&self, record: &Record<'_, K, V>) -> Result<()>
    where
        K: AsBytes,
        V: AsBytes,
    {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProducerCommand::Send {
                topic: record.topic.to_owned(),
                key: Bytes::copy_from_slice(record.key.as_bytes()),
                value: Bytes::copy_from_slice(record.value.as_bytes()),
                partition: record.partition,
                response: tx,
            })
            .await
            .map_err(|_| no_host_reachable_error())?;
        rx.await.map_err(|_| no_host_reachable_error())?
    }

    async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProducerCommand::Flush { response: tx })
            .await
            .map_err(|_| no_host_reachable_error())?;
        rx.await.map_err(|_| no_host_reachable_error())?
    }

    async fn close(mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            let _ = self.sender.send(ProducerCommand::Shutdown).await;
            let join_result = tokio::task::spawn_blocking(move || handle.join())
                .await
                .map_err(|e| Error::Config(format!("failed to join producer thread: {e}")))?;
            if join_result.is_err() {
                return Err(Error::Config("producer thread panicked".to_owned()));
            }
        }
        Ok(())
    }

    fn request_shutdown_non_blocking(&mut self) {
        if self.handle.is_some() {
            let _ = self.sender.try_send(ProducerCommand::Shutdown);
            self.handle.take();
        }
    }
}

impl Drop for BridgedProducer {
    fn drop(&mut self) {
        self.request_shutdown_non_blocking();
    }
}

impl NativeProducer {
    async fn send<K, V>(&self, record: &Record<'_, K, V>) -> Result<()>
    where
        K: AsBytes,
        V: AsBytes,
    {
        let topic = record.topic.to_owned();
        let requested_partition = record.partition;
        let key = Bytes::copy_from_slice(record.key.as_bytes());
        let value = Bytes::copy_from_slice(record.value.as_bytes());
        let headers: Vec<(String, Bytes)> = record.headers.iter().cloned().collect();

        let correlation_id = self.correlation.fetch_add(1, Ordering::Relaxed);
        let mut client = self.client.lock().await;
        let mut state = self.state.lock().await;
        client.ensure_connected().await?;

        let (partition, leader_host) = resolve_partition_and_leader(
            &mut client,
            &mut state,
            &topic,
            requested_partition,
            correlation_id,
        )
        .await?;
        let client_id = client.client_id().to_owned();
        let conn = client.get_connection(&leader_host).await?;

        let (header, request) = build_single_produce_request(
            correlation_id,
            &client_id,
            self.required_acks,
            self.ack_timeout_ms,
            self.compression,
            &topic,
            partition,
            key.as_ref(),
            value.as_ref(),
            &headers,
        );

        send_kp_request(conn, &header, &request, API_VERSION_PRODUCE).await?;
        if self.required_acks == 0 {
            return Ok(());
        }

        let response = get_kp_response::<ProduceResponse>(conn, API_VERSION_PRODUCE).await?;
        for topic_resp in response.responses {
            for part in topic_resp.partition_responses {
                if part.error_code != 0 {
                    if let Some(code) = map_kafka_code(part.error_code) {
                        return Err(Error::Kafka(code));
                    }
                    return Err(Error::Kafka(KafkaCode::Unknown));
                }
            }
        }

        Ok(())
    }
}

async fn resolve_partition_and_leader(
    client: &mut AsyncKafkaClient,
    state: &mut NativeProducerState,
    topic: &str,
    requested_partition: i32,
    correlation_id: i32,
) -> Result<(i32, String)> {
    for _ in 0..2 {
        if let Some((partition, leader_host)) =
            try_resolve_from_cache(state, topic, requested_partition)
        {
            return Ok((partition, leader_host));
        }

        refresh_topic_metadata(client, state, topic, correlation_id).await?;
    }

    Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition))
}

fn try_resolve_from_cache(
    state: &mut NativeProducerState,
    topic: &str,
    requested_partition: i32,
) -> Option<(i32, String)> {
    let route = state.topics.get(topic)?;
    let partitions = route.partitions.clone();
    let available_partitions = route.available_partitions.clone();
    let partition = if requested_partition >= 0 {
        requested_partition
    } else {
        pick_round_robin_partition(state, topic, &available_partitions)?
    };

    let leader_id = *partitions.get(&partition)?;
    if leader_id < 0 {
        return None;
    }
    let leader_host = state.brokers.get(&leader_id)?.clone();
    Some((partition, leader_host))
}

fn pick_round_robin_partition(
    state: &mut NativeProducerState,
    topic: &str,
    available_partitions: &[i32],
) -> Option<i32> {
    if available_partitions.is_empty() {
        return None;
    }

    let len = available_partitions.len();
    let idx = match state.round_robin.entry(topic.to_owned()) {
        Entry::Occupied(mut occupied) => {
            let idx = *occupied.get() % len;
            *occupied.get_mut() = occupied.get().wrapping_add(1);
            idx
        }
        Entry::Vacant(vacant) => {
            vacant.insert(1);
            0
        }
    };
    available_partitions.get(idx).copied()
}

async fn refresh_topic_metadata(
    client: &mut AsyncKafkaClient,
    state: &mut NativeProducerState,
    topic: &str,
    correlation_id: i32,
) -> Result<()> {
    let request_host = pick_request_host(client).ok_or_else(no_host_reachable_error)?;
    let client_id = client.client_id().to_owned();
    let conn = client.get_connection(&request_host).await?;
    let (header, request) = build_metadata_request(correlation_id, &client_id, topic);

    send_kp_request(conn, &header, &request, API_VERSION_METADATA).await?;
    let response = get_kp_response::<MetadataResponse>(conn, API_VERSION_METADATA).await?;

    for broker in response.brokers {
        state.brokers.insert(
            i32::from(broker.node_id),
            format!("{}:{}", broker.host, broker.port),
        );
    }

    for topic_meta in response.topics {
        let Some(name) = topic_meta.name else {
            continue;
        };
        if name.as_str() != topic {
            continue;
        }

        let mut route = TopicRoute::default();
        for part in topic_meta.partitions {
            let partition = part.partition_index;
            let leader = i32::from(part.leader_id);
            route.partitions.insert(partition, leader);
            if leader >= 0 {
                route.available_partitions.push(partition);
            }
        }

        route.available_partitions.sort_unstable();
        route.available_partitions.dedup();
        state.topics.insert(topic.to_owned(), route);
        return Ok(());
    }

    Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition))
}

fn pick_request_host(client: &AsyncKafkaClient) -> Option<String> {
    if let Some(connected) = client.connected_hosts().first() {
        return Some((*connected).to_owned());
    }
    client.bootstrap_hosts().first().cloned()
}

fn build_metadata_request(
    correlation_id: i32,
    client_id: &str,
    topic: &str,
) -> (RequestHeader, MetadataRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::Metadata as i16)
        .with_request_api_version(API_VERSION_METADATA)
        .with_correlation_id(correlation_id);

    let request = MetadataRequest::default().with_topics(Some(vec![
        MetadataRequestTopic::default().with_name(Some(TopicName::from(StrBytes::from_string(
            topic.to_owned(),
        )))),
    ]));

    (header, request)
}

#[allow(clippy::too_many_arguments)]
fn build_single_produce_request(
    correlation_id: i32,
    client_id: &str,
    required_acks: i16,
    timeout_ms: i32,
    compression: Compression,
    topic: &str,
    partition: i32,
    key: &[u8],
    value: &[u8],
    headers: &[(String, Bytes)],
) -> (RequestHeader, ProduceRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::Produce as i16)
        .with_request_api_version(API_VERSION_PRODUCE)
        .with_correlation_id(correlation_id);

    let kp_headers = headers
        .iter()
        .map(|(k, v)| (StrBytes::from_string(k.clone()), Some(v.clone())))
        .collect();

    let record = KpRecord {
        transactional: false,
        control: false,
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 0,
        key: if key.is_empty() {
            None
        } else {
            Some(Bytes::copy_from_slice(key))
        },
        value: if value.is_empty() {
            None
        } else {
            Some(Bytes::copy_from_slice(value))
        },
        headers: kp_headers,
    };

    let mut buf = BytesMut::new();
    let options = RecordEncodeOptions {
        version: 2,
        compression: to_kp_compression(compression),
    };
    RecordBatchEncoder::encode(&mut buf, &[record], &options)
        .expect("failed to encode record batch");

    let partition_data = kafka_protocol::messages::produce_request::PartitionProduceData::default()
        .with_index(partition)
        .with_records(Some(buf.freeze()));

    let topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default()
        .with_name(TopicName::from(StrBytes::from_string(topic.to_owned())))
        .with_partition_data(vec![partition_data]);

    let request = ProduceRequest::default()
        .with_transactional_id(None)
        .with_acks(required_acks)
        .with_timeout_ms(timeout_ms)
        .with_topic_data(vec![topic_data]);

    (header, request)
}

fn to_kp_compression(c: Compression) -> kafka_protocol::records::Compression {
    match c {
        Compression::NONE => kafka_protocol::records::Compression::None,
        Compression::GZIP => kafka_protocol::records::Compression::Gzip,
        Compression::SNAPPY => kafka_protocol::records::Compression::Snappy,
        Compression::LZ4 => kafka_protocol::records::Compression::Lz4,
        Compression::ZSTD => kafka_protocol::records::Compression::Zstd,
    }
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

fn to_millis_i32(d: Duration) -> Result<i32> {
    let m = d
        .as_secs()
        .saturating_mul(1_000)
        .saturating_add(u64::from(d.subsec_millis()));
    if m > i32::MAX as u64 {
        Err(Error::Protocol(ProtocolError::InvalidDuration))
    } else {
        i32::try_from(m).map_err(|_| Error::Protocol(ProtocolError::InvalidDuration))
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
    Error::Connection(ConnectionError::NoHostReachable)
}

#[cfg(test)]
mod tests {
    use rustfs_kafka::error::{ConnectionError, Error};

    use super::*;

    #[tokio::test]
    async fn from_hosts_fails_with_unreachable_hosts() {
        let result = AsyncProducer::from_hosts(vec!["127.0.0.1:1".to_owned()]).await;
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::NoHostReachable))
        ));
    }

    #[tokio::test]
    async fn new_fails_with_empty_hosts() {
        let client = AsyncKafkaClient::new(vec![]).await.unwrap();
        let result = AsyncProducer::new(client).await;
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::NoHostReachable))
        ));
    }
}
