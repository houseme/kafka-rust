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

struct BrokerRecords<'a> {
    topics: Vec<TopicRecords<'a>>,
    topic_indices: HashMap<&'a str, usize>,
}

struct TopicRecords<'a> {
    name: &'a str,
    partitions: Vec<PartitionRecords>,
    partition_indices: HashMap<i32, usize>,
}

struct PartitionRecords {
    partition: i32,
    records: Vec<KpRecord>,
}

struct ProduceResponseValidation {
    result: Result<()>,
    malformed: bool,
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
        self.send_all(std::slice::from_ref(record)).await
    }

    /// Sends a batch with one Produce request per destination broker.
    ///
    /// Input order is preserved within each topic partition. Unspecified partitions
    /// use the same round-robin routing as [`Self::send`]. An empty batch succeeds
    /// without contacting a broker.
    ///
    /// This operation is not atomic: earlier broker requests or other partitions
    /// may succeed before an error is returned. Records are never automatically
    /// retried, so retrying the entire batch can duplicate messages. With
    /// [`RequiredAcks::None`], success only confirms that requests were written.
    pub async fn send_all<K, V>(&self, records: &[Record<'_, K, V>]) -> Result<()>
    where
        K: AsBytes,
        V: AsBytes,
    {
        match &self.mode {
            AsyncProducerMode::Native(native) => native.send_all(records).await,
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
    async fn send_all<K, V>(&self, records: &[Record<'_, K, V>]) -> Result<()>
    where
        K: AsBytes,
        V: AsBytes,
    {
        if records.is_empty() {
            return Ok(());
        }
        for record in records {
            record.headers.validate_unique()?;
        }

        let correlation_id = self.correlation.fetch_add(1, Ordering::Relaxed);
        let mut client = self.client.lock().await;
        let mut state = self.state.lock().await;
        client.ensure_connected().await?;

        let mut brokers: Vec<BrokerRecords<'_>> = Vec::new();
        let mut broker_indices = HashMap::new();
        for record in records {
            let (partition, leader_host) = resolve_partition_and_leader(
                &mut client,
                &mut state,
                record.topic,
                record.partition,
                correlation_id,
            )
            .await?;
            let broker_index = match broker_indices.get(leader_host) {
                Some(&index) => index,
                None => {
                    let index = brokers.len();
                    broker_indices.insert(leader_host.to_owned(), index);
                    brokers.push(BrokerRecords {
                        topics: Vec::new(),
                        topic_indices: HashMap::new(),
                    });
                    index
                }
            };
            add_record_to_broker(&mut brokers[broker_index], partition, record)?;
        }

        let client_id = StrBytes::from_string(client.client_id().to_owned());
        // Encode every batch before sending any Produce request, so codec errors
        // cannot cause partial delivery to the brokers processed earlier.
        let mut requests = Vec::with_capacity(brokers.len());
        // The index owns each unique host once. Restore first-seen broker order
        // before moving those host strings into the requests.
        let mut broker_hosts: Vec<_> = broker_indices.into_iter().collect();
        broker_hosts.sort_unstable_by_key(|(_, index)| *index);
        for ((host, index), broker) in broker_hosts.into_iter().zip(brokers) {
            let request_correlation = if index == 0 {
                correlation_id
            } else {
                self.correlation.fetch_add(1, Ordering::Relaxed)
            };
            let (header, request) = build_produce_request(
                request_correlation,
                &client_id,
                self.required_acks,
                self.ack_timeout_ms,
                self.compression,
                broker.topics,
            )?;
            requests.push((host, header, request));
        }

        for (host, header, request) in requests {
            let conn = client.get_connection(&host).await?;
            send_kp_request(conn, &header, &request, API_VERSION_PRODUCE).await?;
            if self.required_acks == 0 {
                conn.complete_request();
                continue;
            }
            let response = get_kp_response::<ProduceResponse>(conn, API_VERSION_PRODUCE).await?;
            let validation = check_produce_response(response, &request, &mut state);
            if validation.malformed {
                conn.invalidate();
            }
            validation.result?;
        }

        Ok(())
    }
}

fn add_record_to_broker<'a, K: AsBytes, V: AsBytes>(
    broker: &mut BrokerRecords<'a>,
    partition: i32,
    record: &Record<'a, K, V>,
) -> Result<()> {
    let topic_index = match broker.topic_indices.get(record.topic) {
        Some(&index) => index,
        None => {
            let index = broker.topics.len();
            broker.topic_indices.insert(record.topic, index);
            broker.topics.push(TopicRecords {
                name: record.topic,
                partitions: Vec::new(),
                partition_indices: HashMap::new(),
            });
            index
        }
    };
    let topic = &mut broker.topics[topic_index];
    let partition_index = match topic.partition_indices.get(&partition) {
        Some(&index) => index,
        None => {
            let index = topic.partitions.len();
            topic.partition_indices.insert(partition, index);
            topic.partitions.push(PartitionRecords {
                partition,
                records: Vec::new(),
            });
            index
        }
    };
    let records = &mut topic.partitions[partition_index].records;
    let count = i32::try_from(records.len().saturating_add(1))
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;
    let offset = count - 1;
    let key = record.key.as_bytes();
    let value = record.value.as_bytes();
    records.push(KpRecord {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: i64::from(offset),
        // Keep base_sequence=-1 while maintaining the encoder's offset/sequence
        // relationship so all records in this partition form a single batch.
        sequence: offset.wrapping_sub(1),
        timestamp: 0,
        key: (!key.is_empty()).then(|| Bytes::copy_from_slice(key)),
        value: (!value.is_empty()).then(|| Bytes::copy_from_slice(value)),
        headers: record
            .headers
            .iter()
            .map(|(key, value)| (StrBytes::from_string(key.clone()), Some(value.clone())))
            .collect(),
    });
    Ok(())
}

fn check_produce_response(
    response: ProduceResponse,
    request: &ProduceRequest,
    state: &mut NativeProducerState,
) -> ProduceResponseValidation {
    let mut expected: HashMap<_, (bool, HashMap<_, bool>)> = request
        .topic_data
        .iter()
        .map(|topic| {
            (
                topic.name.as_str(),
                (
                    false,
                    topic
                        .partition_data
                        .iter()
                        .map(|partition| (partition.index, false))
                        .collect(),
                ),
            )
        })
        .collect();
    let mut first_error = None;
    let mut malformed = false;
    for topic in response.responses {
        let Some((topic_seen, partitions)) = expected.get_mut(topic.name.as_str()) else {
            malformed = true;
            continue;
        };
        if *topic_seen {
            malformed = true;
        }
        *topic_seen = true;
        for partition in topic.partition_responses {
            let Some(seen) = partitions.get_mut(&partition.index) else {
                malformed = true;
                continue;
            };
            if *seen {
                malformed = true;
            }
            *seen = true;
            if partition.error_code == 0 {
                continue;
            }
            let code = map_kafka_code(partition.error_code).unwrap_or(KafkaCode::Unknown);
            if matches!(
                code,
                KafkaCode::UnknownTopicOrPartition
                    | KafkaCode::LeaderNotAvailable
                    | KafkaCode::NotLeaderForPartition
            ) {
                // Refresh on the next call; retrying here could duplicate a delivery.
                state.topics.remove(topic.name.as_str());
            }
            first_error.get_or_insert(code);
        }
    }
    malformed |= expected
        .values()
        .any(|(_, partitions)| partitions.values().any(|seen| !seen));
    let result = match first_error {
        Some(code) => Err(Error::Kafka(code)),
        None if malformed => Err(Error::Protocol(ProtocolError::Codec)),
        None => Ok(()),
    };
    ProduceResponseValidation { result, malformed }
}

async fn resolve_partition_and_leader<'s>(
    client: &mut AsyncKafkaClient,
    state: &'s mut NativeProducerState,
    topic: &str,
    requested_partition: i32,
    correlation_id: i32,
) -> Result<(i32, &'s str)> {
    let (partition, leader_id) = 'resolved: {
        for _ in 0..2 {
            if let Some(route) = try_resolve_from_cache(state, topic, requested_partition) {
                break 'resolved route;
            }

            refresh_topic_metadata(client, state, topic, correlation_id).await?;
        }

        return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
    };
    // Borrow the host only after all asynchronous metadata refreshes complete.
    let leader_host = state
        .brokers
        .get(&leader_id)
        .ok_or(Error::Kafka(KafkaCode::UnknownTopicOrPartition))?;
    Ok((partition, leader_host.as_str()))
}

fn try_resolve_from_cache(
    state: &mut NativeProducerState,
    topic: &str,
    requested_partition: i32,
) -> Option<(i32, i32)> {
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
    brokers
        .contains_key(&leader_id)
        .then_some((partition, leader_id))
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
    let (header, request) = build_metadata_request(correlation_id, client.client_id(), topic);
    let conn = client.get_connection(&request_host).await?;

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

fn build_produce_request(
    correlation_id: i32,
    client_id: &StrBytes,
    required_acks: i16,
    timeout_ms: i32,
    compression: Compression,
    topics: Vec<TopicRecords<'_>>,
) -> Result<(RequestHeader, ProduceRequest)> {
    let header = RequestHeader::default()
        .with_client_id(Some(client_id.clone()))
        .with_request_api_key(ApiKey::Produce as i16)
        .with_request_api_version(API_VERSION_PRODUCE)
        .with_correlation_id(correlation_id);

    let options = RecordEncodeOptions {
        version: 2,
        compression: to_kp_compression(compression),
    };
    let mut topic_data = Vec::with_capacity(topics.len());
    for topic in topics {
        let mut partition_data = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let mut buf = BytesMut::new();
            RecordBatchEncoder::encode(&mut buf, &partition.records, &options)
                .map_err(|err| map_record_encode_error(&err.to_string()))?;
            partition_data.push(
                kafka_protocol::messages::produce_request::PartitionProduceData::default()
                    .with_index(partition.partition)
                    .with_records(Some(buf.freeze())),
            );
        }
        topic_data.push(
            kafka_protocol::messages::produce_request::TopicProduceData::default()
                .with_name(TopicName::from(StrBytes::from_string(
                    topic.name.to_owned(),
                )))
                .with_partition_data(partition_data),
        );
    }

    let request = ProduceRequest::default()
        .with_transactional_id(None)
        .with_acks(required_acks)
        .with_timeout_ms(timeout_ms)
        .with_topic_data(topic_data);

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
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
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
                Some((expected, 0))
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

    #[tokio::test]
    async fn send_all_groups_topics_and_partitions_into_one_request_per_broker() {
        for required_acks in [RequiredAcks::None, RequiredAcks::One, RequiredAcks::All] {
            tokio::time::timeout(Duration::from_secs(5), async {
                let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let first_addr = first.local_addr().unwrap();
                let second_addr = second.local_addr().unwrap();
                let acks = required_acks as i16;
                let first_server = tokio::spawn(async move {
                    let (mut socket, _) = first.accept().await.unwrap();
                    for (topic, leaders) in [("topic-a", [0, 1]), ("topic-b", [0, 0])] {
                        let correlation = read_topic_metadata_request(&mut socket, topic).await;
                        write_topic_metadata_response(
                            &mut socket,
                            correlation,
                            topic,
                            &[first_addr, second_addr],
                            &leaders,
                        )
                        .await;
                    }
                    let (correlation, request) = read_batch_request(&mut socket, acks).await;
                    assert_eq!(request.topic_data.len(), 2);
                    let topic_a = &request.topic_data[0];
                    assert_eq!(topic_a.name.as_str(), "topic-a");
                    assert_eq!(topic_a.partition_data.len(), 1);
                    assert_eq!(topic_a.partition_data[0].index, 0);
                    assert_partition_batch(&topic_a.partition_data[0], &["a0", "a2"]);
                    let topic_b = &request.topic_data[1];
                    assert_eq!(topic_b.name.as_str(), "topic-b");
                    assert_eq!(topic_b.partition_data.len(), 2);
                    assert_eq!(topic_b.partition_data[0].index, 0);
                    assert_partition_batch(&topic_b.partition_data[0], &["b0"]);
                    assert_eq!(topic_b.partition_data[1].index, 1);
                    assert_partition_batch(&topic_b.partition_data[1], &["b1"]);
                    if acks != 0 {
                        write_batch_response(&mut socket, correlation, &request, 0).await;
                    }
                    1
                });
                let second_server = tokio::spawn(async move {
                    let (mut socket, _) = second.accept().await.unwrap();
                    let (correlation, request) = read_batch_request(&mut socket, acks).await;
                    assert_eq!(request.topic_data.len(), 1);
                    assert_eq!(request.topic_data[0].name.as_str(), "topic-a");
                    assert_eq!(request.topic_data[0].partition_data.len(), 1);
                    let partition = &request.topic_data[0].partition_data[0];
                    assert_eq!(partition.index, 1);
                    assert_partition_batch(partition, &["a1", "a3"]);
                    if acks != 0 {
                        write_batch_response(&mut socket, correlation, &request, 0).await;
                    }
                    1
                });

                let producer = test_producer(first_addr, required_acks).await;
                producer
                    .send_all(&[
                        batch_record("topic-a", 0, "a0"),
                        batch_record("topic-a", 1, "a1"),
                        batch_record("topic-b", 0, "b0"),
                        batch_record("topic-a", 0, "a2"),
                        batch_record("topic-a", 1, "a3"),
                        batch_record("topic-b", 1, "b1"),
                    ])
                    .await
                    .unwrap();
                assert_eq!(
                    first_server.await.unwrap() + second_server.await.unwrap(),
                    2
                );
            })
            .await
            .expect("six records across two brokers must complete with two Produce requests");
        }
    }

    #[tokio::test]
    async fn send_all_reuses_cached_host_across_broker_ids_topics_and_round_robin_calls() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                for (topic, leaders) in [("topic-a", [1, 0]), ("topic-b", [0, 1])] {
                    let correlation = read_topic_metadata_request(&mut socket, topic).await;
                    // Distinct broker IDs advertise the same endpoint. Metadata for
                    // topic-b replaces the cached host strings while the batch grows.
                    write_topic_metadata_response(
                        &mut socket,
                        correlation,
                        topic,
                        &[addr, addr],
                        &leaders,
                    )
                    .await;
                }
                let (correlation, request) = read_batch_request(&mut socket, 1).await;
                assert_eq!(request.topic_data.len(), 2);
                let topic_a = &request.topic_data[0];
                assert_eq!(topic_a.name.as_str(), "topic-a");
                assert_eq!(topic_a.partition_data.len(), 2);
                assert_eq!(topic_a.partition_data[0].index, 0);
                assert_partition_batch(&topic_a.partition_data[0], &["a0", "a2"]);
                assert_eq!(topic_a.partition_data[1].index, 1);
                assert_partition_batch(&topic_a.partition_data[1], &["a1"]);
                let topic_b = &request.topic_data[1];
                assert_eq!(topic_b.name.as_str(), "topic-b");
                assert_eq!(topic_b.partition_data.len(), 2);
                assert_eq!(topic_b.partition_data[0].index, 1);
                assert_partition_batch(&topic_b.partition_data[0], &["b1"]);
                assert_eq!(topic_b.partition_data[1].index, 0);
                assert_partition_batch(&topic_b.partition_data[1], &["b0"]);
                write_batch_response(&mut socket, correlation, &request, 0).await;

                // Both topic routes are cached: the next frame must be Produce,
                // sharing this socket and still grouping by host instead of node ID.
                let (correlation, request) = read_batch_request(&mut socket, 1).await;
                assert_eq!(request.topic_data.len(), 2);
                let topic_a = &request.topic_data[0];
                assert_eq!(topic_a.partition_data.len(), 2);
                assert_eq!(topic_a.partition_data[0].index, 0);
                assert_partition_batch(&topic_a.partition_data[0], &["next0", "explicit", "after"]);
                assert_eq!(topic_a.partition_data[1].index, 1);
                assert_partition_batch(&topic_a.partition_data[1], &["next1"]);
                let topic_b = &request.topic_data[1];
                assert_eq!(topic_b.partition_data.len(), 2);
                assert_eq!(topic_b.partition_data[0].index, 0);
                assert_partition_batch(&topic_b.partition_data[0], &["nextb0"]);
                assert_eq!(topic_b.partition_data[1].index, 1);
                assert_partition_batch(&topic_b.partition_data[1], &["nextb1"]);
                write_batch_response(&mut socket, correlation, &request, 0).await;
            });
            let producer = test_producer(addr, RequiredAcks::One).await;
            producer
                .send_all(&[
                    batch_record("topic-a", 0, "a0"),
                    batch_record("topic-b", 1, "b1"),
                    batch_record("topic-a", 1, "a1"),
                    batch_record("topic-b", 0, "b0"),
                    batch_record("topic-a", 0, "a2"),
                ])
                .await
                .unwrap();
            producer
                .send_all(&[
                    batch_record("topic-a", -1, "next0"),
                    batch_record("topic-b", -1, "nextb0"),
                    batch_record("topic-a", -1, "next1"),
                    batch_record("topic-b", -1, "nextb1"),
                    batch_record("topic-a", 0, "explicit"),
                    batch_record("topic-a", -1, "after"),
                ])
                .await
                .unwrap();
            server.await.unwrap();
        })
        .await
        .expect("cached hosts must survive metadata replacement and preserve grouped routing");
    }

    #[tokio::test]
    async fn send_all_with_no_acks_reuses_connection_and_preserves_round_robin() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let correlation = read_metadata_request(&mut socket).await;
                write_metadata_response(&mut socket, correlation, &[addr], 0).await;
                let (_, request) = read_batch_request(&mut socket, 0).await;
                assert_eq!(request.topic_data.len(), 1);
                let partitions = &request.topic_data[0].partition_data;
                assert_eq!(partitions.len(), 2);
                assert_eq!(partitions[0].index, 0);
                assert_partition_batch(&partitions[0], &["first", "third"]);
                assert_eq!(partitions[1].index, 1);
                assert_partition_batch(&partitions[1], &["second"]);
                // Neither Produce request receives a response, and both share the socket.
                read_produce_request(&mut socket, 1, 0).await;
            });
            let producer = test_producer(addr, RequiredAcks::None).await;
            producer
                .send_all(&[
                    batch_record("topic-a", -1, "first"),
                    batch_record("topic-a", -1, "second"),
                    batch_record("topic-a", -1, "third"),
                ])
                .await
                .unwrap();
            producer.send(&test_record()).await.unwrap();
            server.await.unwrap();
        })
        .await
        .expect("acks=0 batches must complete without responses or reconnecting");
    }

    #[tokio::test]
    async fn send_all_empty_batch_does_not_take_locks_or_send_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let producer = test_producer(addr, RequiredAcks::One).await;
        let AsyncProducerMode::Native(native) = &producer.mode;
        let _client_guard = native.client.lock().await;
        let _state_guard = native.state.lock().await;
        let records: [Record<'_, &str, &str>; 0] = [];
        tokio::time::timeout(Duration::from_millis(100), producer.send_all(&records))
            .await
            .expect("an empty batch must not wait for locks")
            .unwrap();
        let (mut socket, _) = listener.accept().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), socket.read_u8())
                .await
                .is_err(),
            "an empty batch must not send any request"
        );
    }

    #[tokio::test]
    async fn later_duplicate_headers_are_rejected_before_locks_metadata_and_round_robin() {
        for required_acks in [RequiredAcks::None, RequiredAcks::One] {
            tokio::time::timeout(Duration::from_secs(5), async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let acks = required_acks as i16;
                let server = tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let correlation = read_metadata_request(&mut socket).await;
                    write_metadata_response(&mut socket, correlation, &[addr], 0).await;
                    // The rejected batch must not advance round-robin or send any
                    // frame. The next explicit valid send is the first Produce.
                    let correlation = read_produce_request(&mut socket, 0, acks).await;
                    if acks != 0 {
                        write_produce_response(&mut socket, correlation, 0, 0).await;
                    }
                });
                let producer = test_producer(addr, required_acks).await;
                let duplicate = test_record()
                    .with_header("trace", "old")
                    .with_header("between", "middle")
                    .with_header("trace", "new");
                {
                    let AsyncProducerMode::Native(native) = &producer.mode;
                    let _client_guard = native.client.lock().await;
                    let state_guard = native.state.lock().await;
                    let records = [test_record(), duplicate];
                    let result = tokio::time::timeout(
                        Duration::from_millis(100),
                        producer.send_all(&records),
                    )
                    .await
                    .expect("complete input validation must precede both locks");
                    assert!(matches!(result, Err(Error::Config(message))
                        if message.contains("codec") && message.contains("duplicate")));
                    assert!(state_guard.topics.is_empty());
                    assert!(state_guard.round_robin.is_empty());
                    assert_eq!(records[1].headers.len(), 4);
                    assert_eq!(
                        records[1].headers.iter().nth(1).unwrap().1,
                        Bytes::from_static(b"old")
                    );
                    assert_eq!(
                        records[1].headers.iter().nth(3).unwrap().1,
                        Bytes::from_static(b"new")
                    );
                }
                producer.send(&test_record()).await.unwrap();
                server.await.unwrap();
            })
            .await
            .expect("a rejected duplicate-header batch must leave valid sending available");
        }
    }

    #[tokio::test]
    async fn send_all_stops_after_partial_broker_failure_without_retrying() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let third = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addrs = [
                first.local_addr().unwrap(),
                second.local_addr().unwrap(),
                third.local_addr().unwrap(),
            ];
            let first_server = tokio::spawn(async move {
                let (mut socket, _) = first.accept().await.unwrap();
                let correlation = read_metadata_request(&mut socket).await;
                write_topic_metadata_response(
                    &mut socket,
                    correlation,
                    "topic-a",
                    &addrs,
                    &[0, 1, 2],
                )
                .await;
                let (correlation, request) = read_batch_request(&mut socket, 1).await;
                assert_partition_batch(&request.topic_data[0].partition_data[0], &["delivered"]);
                write_batch_response(&mut socket, correlation, &request, 0).await;
            });
            let second_server = tokio::spawn(async move {
                let (mut socket, _) = second.accept().await.unwrap();
                let (correlation, request) = read_batch_request(&mut socket, 1).await;
                assert_partition_batch(&request.topic_data[0].partition_data[0], &["failed"]);
                write_batch_response(&mut socket, correlation, &request, 6).await;
            });
            let producer = test_producer(addrs[0], RequiredAcks::One).await;
            assert!(matches!(
                producer
                    .send_all(&[
                        batch_record("topic-a", 0, "delivered"),
                        batch_record("topic-a", 1, "failed"),
                        batch_record("topic-a", 2, "unsent"),
                    ])
                    .await,
                Err(Error::Kafka(KafkaCode::NotLeaderForPartition))
            ));
            first_server.await.unwrap();
            second_server.await.unwrap();
            let AsyncProducerMode::Native(native) = &producer.mode;
            assert!(!native.state.lock().await.topics.contains_key("topic-a"));
            assert!(
                tokio::time::timeout(Duration::from_millis(100), third.accept())
                    .await
                    .is_err(),
                "later brokers must not be contacted after the first failure"
            );
        })
        .await
        .expect("partial broker failure must return the original error without retrying");
    }

    #[tokio::test]
    async fn send_all_rejects_malformed_ack_sets_without_retry_and_reconnects_on_next_call() {
        #[derive(Clone, Copy)]
        enum AckCase {
            Sparse,
            Empty,
            DuplicatePartition,
            DuplicateTopic,
            ExtraPartition,
            ExtraTopic,
            SparseLeaderError,
        }
        for case in [
            AckCase::Sparse,
            AckCase::Empty,
            AckCase::DuplicatePartition,
            AckCase::DuplicateTopic,
            AckCase::ExtraPartition,
            AckCase::ExtraTopic,
            AckCase::SparseLeaderError,
        ] {
            tokio::time::timeout(Duration::from_secs(5), async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let leader_error = matches!(case, AckCase::SparseLeaderError);
                let server = tokio::spawn(async move {
                    let (mut original, _) = listener.accept().await.unwrap();
                    let correlation = read_metadata_request(&mut original).await;
                    write_metadata_response(&mut original, correlation, &[addr], 0).await;
                    let (correlation, request) = read_batch_request(&mut original, 1).await;
                    assert_eq!(request.topic_data.len(), 1);
                    let partitions = &request.topic_data[0].partition_data;
                    assert_eq!(partitions.len(), 2);
                    assert_eq!(partitions[0].index, 0);
                    assert_partition_batch(&partitions[0], &["first"]);
                    assert_eq!(partitions[1].index, 1);
                    assert_partition_batch(&partitions[1], &["second"]);
                    let topics = match case {
                        AckCase::Sparse => vec![ack_topic("topic-a", &[(0, 0)])],
                        AckCase::Empty => vec![],
                        AckCase::DuplicatePartition => {
                            vec![ack_topic("topic-a", &[(0, 0), (0, 0), (1, 0)])]
                        }
                        AckCase::DuplicateTopic => vec![
                            ack_topic("topic-a", &[(0, 0)]),
                            ack_topic("topic-a", &[(1, 0)]),
                        ],
                        AckCase::ExtraPartition => {
                            vec![ack_topic("topic-a", &[(0, 0), (1, 0), (2, 6)])]
                        }
                        AckCase::ExtraTopic => vec![
                            ack_topic("topic-a", &[(0, 0), (1, 0)]),
                            ack_topic("unknown-topic", &[(0, 6)]),
                        ],
                        AckCase::SparseLeaderError => vec![ack_topic("topic-a", &[(0, 6)])],
                    };
                    write_response(
                        &mut original,
                        correlation,
                        &ProduceResponse::default().with_responses(topics),
                        API_VERSION_PRODUCE,
                    )
                    .await;

                    // Keep the original socket open. Only the caller's next explicit
                    // send should create this new connection after invalidation.
                    let (mut replacement, _) = listener.accept().await.unwrap();
                    if leader_error {
                        let correlation = read_metadata_request(&mut replacement).await;
                        write_metadata_response(&mut replacement, correlation, &[addr], 0).await;
                    }
                    let correlation = read_produce_request(&mut replacement, 0, 1).await;
                    write_produce_response(&mut replacement, correlation, 0, 0).await;
                });
                let producer = test_producer(addr, RequiredAcks::One).await;
                let error = producer
                    .send_all(&[
                        batch_record("topic-a", 0, "first"),
                        batch_record("topic-a", 1, "second"),
                    ])
                    .await
                    .unwrap_err();
                if leader_error {
                    assert!(matches!(
                        error,
                        Error::Kafka(KafkaCode::NotLeaderForPartition)
                    ));
                } else {
                    assert!(matches!(error, Error::Protocol(ProtocolError::Codec)));
                }
                let AsyncProducerMode::Native(native) = &producer.mode;
                assert_eq!(
                    native.state.lock().await.topics.contains_key("topic-a"),
                    !leader_error
                );
                producer
                    .send(&test_record().with_partition(0))
                    .await
                    .unwrap();
                server.await.unwrap();
            })
            .await
            .expect("malformed ACKs must fail without retry, then reconnect on explicit send");
        }
    }

    #[cfg(not(feature = "gzip"))]
    #[tokio::test]
    async fn send_all_disabled_codec_sends_no_produce_requests() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let first_addr = first.local_addr().unwrap();
            let second_addr = second.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = first.accept().await.unwrap();
                let correlation = read_metadata_request(&mut socket).await;
                write_topic_metadata_response(
                    &mut socket,
                    correlation,
                    "topic-a",
                    &[first_addr, second_addr],
                    &[0, 1],
                )
                .await;
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), socket.read_u8())
                        .await
                        .is_err(),
                    "a codec error must occur before any Produce request is sent"
                );
            });
            let producer = AsyncProducer::builder(vec![first_addr.to_string()])
                .with_client_id("producer-test".to_owned())
                .with_compression(Compression::GZIP)
                .build()
                .await
                .unwrap();
            producer.send_all::<&str, &str>(&[]).await.unwrap();
            assert!(matches!(
                producer
                    .send_all(&[
                        batch_record("topic-a", 0, "first"),
                        batch_record("topic-a", 1, "second"),
                    ])
                    .await,
                Err(Error::Protocol(ProtocolError::UnsupportedCompression))
            ));
            server.await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(100), second.accept())
                    .await
                    .is_err(),
                "a codec error must not establish a Produce connection to another broker"
            );
        })
        .await
        .expect("disabled codecs must fail before sending to any broker");
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
        read_topic_metadata_request(socket, "topic-a").await
    }

    async fn read_topic_metadata_request(socket: &mut TcpStream, expected_topic: &str) -> i32 {
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
        assert_eq!(topics[0].name.as_ref().unwrap().as_str(), expected_topic);
        assert!(!body.has_remaining());
        correlation
    }

    async fn read_produce_request(socket: &mut TcpStream, partition: i32, acks: i16) -> i32 {
        let (correlation, mut request) = read_batch_request(socket, acks).await;
        assert_eq!(request.topic_data.len(), 1);
        assert_eq!(request.topic_data[0].name.as_str(), "topic-a");
        let partition_data = &mut request.topic_data[0].partition_data;
        assert_eq!(partition_data.len(), 1);
        assert_eq!(partition_data[0].index, partition);
        let mut records = partition_data[0].records.take().unwrap();
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

    async fn read_batch_request(socket: &mut TcpStream, acks: i16) -> (i32, ProduceRequest) {
        let (correlation, mut body) = read_request(
            socket,
            ApiKey::Produce,
            API_VERSION_PRODUCE,
            ProduceRequest::header_version(API_VERSION_PRODUCE),
        )
        .await;
        let request = ProduceRequest::decode(&mut body, API_VERSION_PRODUCE).unwrap();
        assert_eq!(request.transactional_id, None);
        assert_eq!(request.acks, acks);
        assert_eq!(request.timeout_ms, 1_234);
        assert!(!body.has_remaining());
        (correlation, request)
    }

    async fn write_metadata_response(
        socket: &mut TcpStream,
        correlation: i32,
        brokers: &[SocketAddr],
        leader: i32,
    ) {
        write_topic_metadata_response(socket, correlation, "topic-a", brokers, &[leader, leader])
            .await;
    }

    async fn write_topic_metadata_response(
        socket: &mut TcpStream,
        correlation: i32,
        topic: &str,
        brokers: &[SocketAddr],
        leaders: &[i32],
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
        let partitions = leaders
            .iter()
            .enumerate()
            .map(|(partition, &leader)| {
                MetadataResponsePartition::default()
                    .with_partition_index(i32::try_from(partition).unwrap())
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
                    .with_name(Some(StrBytes::from_string(topic.to_owned()).into()))
                    .with_partitions(partitions),
            ]);
        write_response(socket, correlation, &response, API_VERSION_METADATA).await;
    }

    async fn write_batch_response(
        socket: &mut TcpStream,
        correlation: i32,
        request: &ProduceRequest,
        error_code: i16,
    ) {
        let response = ProduceResponse::default().with_responses(
            request
                .topic_data
                .iter()
                .map(|topic| {
                    TopicProduceResponse::default()
                        .with_name(topic.name.clone())
                        .with_partition_responses(
                            topic
                                .partition_data
                                .iter()
                                .map(|partition| {
                                    PartitionProduceResponse::default()
                                        .with_index(partition.index)
                                        .with_error_code(error_code)
                                })
                                .collect(),
                        )
                })
                .collect(),
        );
        write_response(socket, correlation, &response, API_VERSION_PRODUCE).await;
    }

    fn ack_topic(topic: &'static str, partitions: &[(i32, i16)]) -> TopicProduceResponse {
        TopicProduceResponse::default()
            .with_name(StrBytes::from_static_str(topic).into())
            .with_partition_responses(
                partitions
                    .iter()
                    .map(|&(partition, code)| {
                        PartitionProduceResponse::default()
                            .with_index(partition)
                            .with_error_code(code)
                    })
                    .collect(),
            )
    }

    fn assert_partition_batch(partition: &PartitionProduceData, expected: &[&str]) {
        let mut encoded = partition.records.clone().unwrap();
        let decoded = RecordBatchDecoder::decode(&mut encoded).unwrap();
        assert_eq!(decoded.records.len(), expected.len());
        assert!(
            !encoded.has_remaining(),
            "one partition must form one batch"
        );
        for (index, (record, expected)) in decoded.records.iter().zip(expected).enumerate() {
            assert_eq!(record.offset, i64::try_from(index).unwrap());
            assert_eq!(record.sequence, i32::try_from(index).unwrap() - 1);
            assert_eq!(record.key.as_deref(), Some(expected.as_bytes()));
            assert_eq!(record.value.as_deref(), Some(expected.as_bytes()));
            assert_eq!(
                record
                    .headers
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_deref()))
                    .collect::<Vec<_>>(),
                vec![
                    ("first", Some(expected.as_bytes())),
                    ("second", Some(b"tail".as_slice())),
                ]
            );
        }
    }

    fn batch_record(
        topic: &'static str,
        partition: i32,
        value: &'static str,
    ) -> Record<'static, &'static str, &'static str> {
        Record::from_key_value(topic, value, value)
            .with_partition(partition)
            .with_header("first", value)
            .with_header("second", "tail")
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
    fn build_produce_request_returns_error_when_codec_feature_is_disabled() {
        let err = build_test_request(Compression::GZIP)
            .expect_err("disabled gzip support should return an error");

        assert!(matches!(
            err,
            Error::Protocol(ProtocolError::UnsupportedCompression)
        ));
    }

    #[cfg(feature = "compression")]
    #[test]
    fn build_produce_request_supports_enabled_compression_codecs() {
        for compression in [
            Compression::GZIP,
            Compression::SNAPPY,
            Compression::LZ4,
            Compression::ZSTD,
        ] {
            let (_, mut request) = build_test_request(compression)
                .unwrap_or_else(|err| panic!("{compression:?} should encode successfully: {err}"));
            let mut records = request.topic_data[0].partition_data[0]
                .records
                .take()
                .unwrap();
            let decoded = RecordBatchDecoder::decode(&mut records).unwrap();
            assert_eq!(decoded.compression, to_kp_compression(compression));
            assert_eq!(decoded.records.len(), 3);
            assert!(
                !records.has_remaining(),
                "one partition must form one batch"
            );
        }
    }

    #[test]
    fn build_produce_request_uses_one_batch_with_contiguous_offsets_and_sequences() {
        let (_, mut request) = build_test_request(Compression::NONE).unwrap();
        let mut records = request.topic_data[0].partition_data[0]
            .records
            .take()
            .unwrap();
        let decoded = RecordBatchDecoder::decode(&mut records).unwrap();
        assert_eq!(decoded.records.len(), 3);
        for (index, record) in decoded.records.iter().enumerate() {
            assert_eq!(record.offset, i64::try_from(index).unwrap());
            assert_eq!(record.sequence, i32::try_from(index).unwrap() - 1);
            assert_eq!(record.producer_id, -1);
        }
        assert_eq!(decoded.records[2].key, None);
        assert_eq!(decoded.records[2].value, None);
        assert!(
            !records.has_remaining(),
            "one partition must form one batch"
        );
    }

    #[test]
    fn produce_response_preserves_first_error_and_invalidates_all_stale_topics() {
        let mut state = NativeProducerState {
            topics: HashMap::from([
                ("topic-a".to_owned(), TopicRoute::default()),
                ("topic-b".to_owned(), TopicRoute::default()),
                ("topic-c".to_owned(), TopicRoute::default()),
            ]),
            ..NativeProducerState::default()
        };
        let response = ProduceResponse::default().with_responses(
            [("topic-a", 7), ("topic-b", 6), ("topic-c", 5)]
                .into_iter()
                .map(|(topic, code)| {
                    TopicProduceResponse::default()
                        .with_name(StrBytes::from_static_str(topic).into())
                        .with_partition_responses(vec![
                            PartitionProduceResponse::default()
                                .with_index(0)
                                .with_error_code(code),
                        ])
                })
                .collect(),
        );
        let request = ProduceRequest::default().with_topic_data(
            [
                ("topic-a", vec![0]),
                ("topic-b", vec![0]),
                ("topic-c", vec![0, 1]),
            ]
            .into_iter()
            .map(|(topic, partitions)| {
                TopicProduceData::default()
                    .with_name(StrBytes::from_static_str(topic).into())
                    .with_partition_data(
                        partitions
                            .into_iter()
                            .map(|partition| PartitionProduceData::default().with_index(partition))
                            .collect(),
                    )
            })
            .collect(),
        );
        let validation = check_produce_response(response, &request, &mut state);
        assert!(
            validation.malformed,
            "topic-c partition 1 was not acknowledged"
        );
        assert!(matches!(
            validation.result,
            Err(Error::Kafka(KafkaCode::RequestTimedOut))
        ));
        assert!(state.topics.contains_key("topic-a"));
        assert!(!state.topics.contains_key("topic-b"));
        assert!(!state.topics.contains_key("topic-c"));
    }

    fn build_test_request(compression: Compression) -> Result<(RequestHeader, ProduceRequest)> {
        let mut broker = BrokerRecords {
            topics: Vec::new(),
            topic_indices: HashMap::new(),
        };
        for record in [
            test_record(),
            test_record(),
            Record::from_key_value("topic-a", "", ""),
        ] {
            add_record_to_broker(&mut broker, 0, &record)?;
        }
        build_produce_request(
            1,
            &StrBytes::from_static_str("client-a"),
            1,
            30_000,
            compression,
            broker.topics,
        )
    }
}
