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
            conn.complete_request();
            return Ok(());
        }

        let response = get_kp_response::<ProduceResponse>(conn, API_VERSION_PRODUCE).await?;
        for topic_resp in response.responses {
            for part in topic_resp.partition_responses {
                if part.error_code != 0 {
                    let code = map_kafka_code(part.error_code).unwrap_or(KafkaCode::Unknown);
                    if matches!(
                        code,
                        KafkaCode::UnknownTopicOrPartition
                            | KafkaCode::LeaderNotAvailable
                            | KafkaCode::NotLeaderForPartition
                    ) {
                        // Refresh on the next send; retrying here could duplicate a delivery.
                        state.topics.remove(&topic);
                    }
                    return Err(Error::Kafka(code));
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
    let NativeProducerState {
        brokers,
        topics,
        round_robin,
    } = state;
    let route = topics.get(topic)?;
    let partition = if requested_partition >= 0 {
        requested_partition
    } else {
        pick_round_robin_partition(round_robin, topic, &route.available_partitions)?
    };

    let leader_id = *route.partitions.get(&partition)?;
    if leader_id < 0 {
        return None;
    }
    let leader_host = brokers.get(&leader_id)?.clone();
    Some((partition, leader_host))
}

fn pick_round_robin_partition(
    round_robin: &mut HashMap<String, usize>,
    topic: &str,
    available_partitions: &[i32],
) -> Option<i32> {
    if available_partitions.is_empty() {
        return None;
    }

    let len = available_partitions.len();
    let idx = match round_robin.get_mut(topic) {
        Some(next) => {
            let idx = *next % len;
            *next = next.wrapping_add(1);
            idx
        }
        None => {
            round_robin.insert(topic.to_owned(), 1);
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
    use bytes::Buf;
    use kafka_protocol::messages::ResponseHeader;
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};
    use kafka_protocol::records::RecordBatchDecoder;
    #[cfg(not(feature = "gzip"))]
    use rustfs_kafka::error::ProtocolError;
    use rustfs_kafka::error::{ConnectionError, Error};
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

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

    #[test]
    fn explicit_partition_does_not_advance_round_robin_routing() {
        let mut state = NativeProducerState {
            brokers: HashMap::from([(0, "broker:9092".to_owned())]),
            topics: HashMap::from([(
                "topic-a".to_owned(),
                TopicRoute {
                    partitions: HashMap::from([(0, 0), (1, 0)]),
                    available_partitions: vec![0, 1],
                },
            )]),
            ..NativeProducerState::default()
        };
        for (requested, expected) in [(1, 1), (-1, 0), (-1, 1), (-1, 0)] {
            assert_eq!(
                try_resolve_from_cache(&mut state, "topic-a", requested),
                Some((expected, "broker:9092".to_owned()))
            );
        }
    }

    #[test]
    fn round_robin_counter_wraps_without_overflowing() {
        let mut round_robin = HashMap::from([("topic-a".to_owned(), usize::MAX)]);
        assert_eq!(
            pick_round_robin_partition(&mut round_robin, "topic-a", &[0, 1]),
            Some(1)
        );
        assert_eq!(
            pick_round_robin_partition(&mut round_robin, "topic-a", &[0, 1]),
            Some(0)
        );
    }

    #[tokio::test]
    async fn send_refreshes_stale_topic_route_on_next_call_without_retrying() {
        for (error_code, expected_code, required_acks) in [
            (3, KafkaCode::UnknownTopicOrPartition, RequiredAcks::One),
            (5, KafkaCode::LeaderNotAvailable, RequiredAcks::All),
            (6, KafkaCode::NotLeaderForPartition, RequiredAcks::One),
        ] {
            tokio::time::timeout(Duration::from_secs(5), async {
                let old_leader = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let new_leader = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let old_addr = old_leader.local_addr().unwrap();
                let new_addr = new_leader.local_addr().unwrap();
                let acks = required_acks as i16;

                let old_server = tokio::spawn(async move {
                    let (mut socket, _) = old_leader.accept().await.unwrap();
                    let correlation = read_metadata_request(&mut socket).await;
                    write_metadata_response(&mut socket, correlation, &[old_addr, new_addr], 0)
                        .await;
                    let correlation = read_produce_request(&mut socket, 0, acks).await;
                    write_produce_response(&mut socket, correlation, 0, error_code).await;

                    // The next request must refresh metadata, never re-send the failed record.
                    let correlation = read_metadata_request(&mut socket).await;
                    write_metadata_response(&mut socket, correlation, &[old_addr, new_addr], 1)
                        .await;
                });
                let new_server = tokio::spawn(async move {
                    let (mut socket, _) = new_leader.accept().await.unwrap();
                    for partition in [1, 0] {
                        let correlation = read_produce_request(&mut socket, partition, acks).await;
                        write_produce_response(&mut socket, correlation, partition, 0).await;
                    }
                });

                let producer = test_producer(old_addr, required_acks).await;
                let record = test_record();
                let err = producer.send(&record).await.unwrap_err();
                assert!(matches!(err, Error::Kafka(code) if code == expected_code));
                producer.send(&record).await.unwrap();
                producer.send(&record).await.unwrap();
                old_server.await.unwrap();
                new_server.await.unwrap();
            })
            .await
            .expect("leader transition should complete without retrying the failed send");
        }
    }

    #[tokio::test]
    async fn send_preserves_cached_route_after_non_metadata_error() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let correlation = read_metadata_request(&mut socket).await;
                write_metadata_response(&mut socket, correlation, &[addr], 0).await;
                let correlation = read_produce_request(&mut socket, 0, 1).await;
                write_produce_response(&mut socket, correlation, 0, 7).await;
                let correlation = read_produce_request(&mut socket, 1, 1).await;
                write_produce_response(&mut socket, correlation, 1, 0).await;
            });

            let producer = test_producer(addr, RequiredAcks::One).await;
            let record = test_record();
            assert!(matches!(
                producer.send(&record).await,
                Err(Error::Kafka(KafkaCode::RequestTimedOut))
            ));
            producer.send(&record).await.unwrap();
            server.await.unwrap();
        })
        .await
        .expect("non-metadata errors should preserve the route without retrying");
    }

    #[tokio::test]
    async fn send_with_no_acks_sends_records_without_waiting_for_response() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let correlation = read_metadata_request(&mut socket).await;
                write_metadata_response(&mut socket, correlation, &[addr], 0).await;
                // No Produce response is sent for either request.
                read_produce_request(&mut socket, 0, 0).await;
                read_produce_request(&mut socket, 1, 0).await;
            });

            let producer = test_producer(addr, RequiredAcks::None).await;
            let record = test_record();
            producer.send(&record).await.unwrap();
            producer.send(&record).await.unwrap();
            server.await.unwrap();
        })
        .await
        .expect("acks=0 must not wait for a Produce response");
    }

    async fn test_producer(addr: SocketAddr, required_acks: RequiredAcks) -> AsyncProducer {
        AsyncProducer::builder(vec![addr.to_string()])
            .with_client_id("producer-test".to_owned())
            .with_required_acks(required_acks)
            .with_ack_timeout(Duration::from_millis(1_234))
            .build()
            .await
            .unwrap()
    }

    fn test_record() -> Record<'static, &'static str, &'static str> {
        Record::from_key_value("topic-a", "key", "value").with_header("source", "test")
    }

    async fn read_request(
        socket: &mut TcpStream,
        api_key: ApiKey,
        api_version: i16,
        header_version: i16,
    ) -> (i32, Bytes) {
        let size = socket.read_i32().await.unwrap();
        let mut frame = vec![0; usize::try_from(size).unwrap()];
        socket.read_exact(&mut frame).await.unwrap();
        let mut body = Bytes::from(frame);
        let header = RequestHeader::decode(&mut body, header_version).unwrap();
        assert_eq!(header.request_api_key, api_key as i16);
        assert_eq!(header.request_api_version, api_version);
        assert_eq!(header.client_id.unwrap().as_str(), "producer-test");
        (header.correlation_id, body)
    }

    async fn read_metadata_request(socket: &mut TcpStream) -> i32 {
        let (correlation, mut body) = read_request(
            socket,
            ApiKey::Metadata,
            API_VERSION_METADATA,
            MetadataRequest::header_version(API_VERSION_METADATA),
        )
        .await;
        let request = MetadataRequest::decode(&mut body, API_VERSION_METADATA).unwrap();
        let topics = request.topics.unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name.as_ref().unwrap().as_str(), "topic-a");
        assert!(!body.has_remaining());
        correlation
    }

    async fn read_produce_request(socket: &mut TcpStream, partition: i32, acks: i16) -> i32 {
        let (correlation, mut body) = read_request(
            socket,
            ApiKey::Produce,
            API_VERSION_PRODUCE,
            ProduceRequest::header_version(API_VERSION_PRODUCE),
        )
        .await;
        let mut request = ProduceRequest::decode(&mut body, API_VERSION_PRODUCE).unwrap();
        assert_eq!(request.transactional_id, None);
        assert_eq!(request.acks, acks);
        assert_eq!(request.timeout_ms, 1_234);
        assert_eq!(request.topic_data.len(), 1);
        assert_eq!(request.topic_data[0].name.as_str(), "topic-a");
        let partition_data = &mut request.topic_data[0].partition_data;
        assert_eq!(partition_data.len(), 1);
        assert_eq!(partition_data[0].index, partition);
        let mut records = partition_data[0].records.take().unwrap();
        assert!(!body.has_remaining());
        let record_set = RecordBatchDecoder::decode(&mut records).unwrap();
        assert_eq!(record_set.records.len(), 1);
        let record = &record_set.records[0];
        assert_eq!(record.key.as_deref(), Some(b"key".as_slice()));
        assert_eq!(record.value.as_deref(), Some(b"value".as_slice()));
        assert_eq!(
            record.headers.get(&StrBytes::from_static_str("source")),
            Some(&Some(Bytes::from_static(b"test")))
        );
        correlation
    }

    async fn write_metadata_response(
        socket: &mut TcpStream,
        correlation: i32,
        brokers: &[SocketAddr],
        leader: i32,
    ) {
        let brokers = brokers
            .iter()
            .enumerate()
            .map(|(id, addr)| {
                MetadataResponseBroker::default()
                    .with_node_id(i32::try_from(id).unwrap().into())
                    .with_host(StrBytes::from_static_str("127.0.0.1"))
                    .with_port(i32::from(addr.port()))
            })
            .collect();
        let partitions = [0, 1]
            .into_iter()
            .map(|partition| {
                MetadataResponsePartition::default()
                    .with_partition_index(partition)
                    .with_leader_id(leader.into())
                    .with_replica_nodes(vec![leader.into()])
                    .with_isr_nodes(vec![leader.into()])
            })
            .collect();
        let response = MetadataResponse::default()
            .with_brokers(brokers)
            .with_controller_id(0.into())
            .with_topics(vec![
                MetadataResponseTopic::default()
                    .with_name(Some(StrBytes::from_static_str("topic-a").into()))
                    .with_partitions(partitions),
            ]);
        write_response(socket, correlation, &response, API_VERSION_METADATA).await;
    }

    async fn write_produce_response(
        socket: &mut TcpStream,
        correlation: i32,
        partition: i32,
        error_code: i16,
    ) {
        let response = ProduceResponse::default().with_responses(vec![
            TopicProduceResponse::default()
                .with_name(StrBytes::from_static_str("topic-a").into())
                .with_partition_responses(vec![
                    PartitionProduceResponse::default()
                        .with_index(partition)
                        .with_error_code(error_code),
                ]),
        ]);
        write_response(socket, correlation, &response, API_VERSION_PRODUCE).await;
    }

    async fn write_response<R: Encodable + HeaderVersion>(
        socket: &mut TcpStream,
        correlation: i32,
        response: &R,
        api_version: i16,
    ) {
        let mut frame = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(correlation)
            .encode(&mut frame, R::header_version(api_version))
            .unwrap();
        response.encode(&mut frame, api_version).unwrap();
        socket
            .write_i32(i32::try_from(frame.len()).unwrap())
            .await
            .unwrap();
        socket.write_all(&frame).await.unwrap();
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
