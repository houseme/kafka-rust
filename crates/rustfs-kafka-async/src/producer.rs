//! Async producer for sending messages to Kafka.

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader,
    TopicName, metadata_request::MetadataRequestTopic,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Record as KpRecord, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use rustfs_kafka::client::{Compression, RequiredAcks, SecurityConfig};
use rustfs_kafka::error::{ConnectionError, Error, KafkaCode, ProtocolError, Result};
use rustfs_kafka::producer::{AsBytes, Record};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::debug;

use crate::AsyncKafkaClient;
use crate::wire::{get_kp_response, kafka_code_from_protocol as map_kafka_code, send_kp_request};

const API_VERSION_PRODUCE: i16 = 9;
const API_VERSION_METADATA: i16 = 1;

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
}

/// An async Kafka producer.
///
/// This producer always uses native async I/O via tokio sockets.
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

    /// Sets optional TLS security configuration.
    #[must_use]
    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.config = self.config.with_security(security);
        self
    }

    /// Backward-compatible no-op kept for API compatibility.
    #[deprecated(
        since = "1.2.0",
        note = "native async producers no longer use an internal channel; this setting is ignored"
    )]
    #[must_use]
    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity.max(1);
        self
    }

    /// Backward-compatible setting kept for API compatibility.
    #[deprecated(
        since = "1.2.0",
        note = "native async producers are always enabled; this setting is ignored"
    )]
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

        if !native_async {
            debug!(
                "AsyncProducerBuilder::with_native_async(false) is ignored: producer always uses native async I/O"
            );
        }
        let _ = channel_capacity;

        let client = AsyncKafkaClient::with_client_id_and_security(
            hosts,
            client_id,
            config.security.clone(),
        )
        .await?;
        AsyncProducer::from_native(client, config)
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
        if config.security.is_some() && client.security().is_none() {
            return Self::builder(client.bootstrap_hosts().to_vec())
                .with_client_id(client.client_id().to_owned())
                .with_required_acks(config.required_acks)
                .with_ack_timeout(config.ack_timeout)
                .with_compression(config.compression)
                .build_with_optional_security(config.security)
                .await;
        }

        Self::from_native(client, config)
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
        }
    }

    /// Flushes any pending messages.
    pub async fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// Gracefully shuts down the producer.
    pub async fn close(self) -> Result<()> {
        Ok(())
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
        )?;

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
) -> Result<(RequestHeader, ProduceRequest)> {
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
        delete_horizon: false,
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
    RecordBatchEncoder::encode(&mut buf, &[record], &options).map_err(|err| {
        let message = err.to_string();
        map_record_encode_error(&message)
    })?;

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

    Ok((header, request))
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

fn map_record_encode_error(message: &str) -> Error {
    if is_disabled_compression_feature_error(message) {
        Error::Protocol(ProtocolError::UnsupportedCompression)
    } else {
        Error::Protocol(ProtocolError::Codec)
    }
}

fn is_disabled_compression_feature_error(message: &str) -> bool {
    message.contains("Support for") && message.contains("not enabled as a cargo feature")
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

fn no_host_reachable_error() -> Error {
    Error::Connection(ConnectionError::NoHostReachable)
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "gzip"))]
    use rustfs_kafka::error::ProtocolError;
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

    #[cfg(not(feature = "gzip"))]
    #[test]
    fn build_single_produce_request_returns_error_when_codec_feature_is_disabled() {
        let err = build_single_produce_request(
            1,
            "client-a",
            1,
            30_000,
            Compression::GZIP,
            "topic-a",
            0,
            b"key",
            b"value",
            &[],
        )
        .expect_err("disabled gzip support should return an error");

        assert!(matches!(
            err,
            Error::Protocol(ProtocolError::UnsupportedCompression)
        ));
    }

    #[cfg(feature = "compression")]
    #[test]
    fn build_single_produce_request_supports_enabled_compression_codecs() {
        for compression in [
            Compression::GZIP,
            Compression::SNAPPY,
            Compression::LZ4,
            Compression::ZSTD,
        ] {
            build_single_produce_request(
                1,
                "client-a",
                1,
                30_000,
                compression,
                "topic-a",
                0,
                b"key",
                b"value",
                &[],
            )
            .unwrap_or_else(|err| panic!("{compression:?} should encode successfully: {err}"));
        }
    }
}
