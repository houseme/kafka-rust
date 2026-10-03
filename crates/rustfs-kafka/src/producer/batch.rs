use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::client::{self, KafkaClient, KafkaClientInternals, ProduceConfirm};
use crate::error::{Error, ProtocolError, Result};

use super::config::BatchConfig;
use super::config::{Config, DEFAULT_ACK_TIMEOUT_MILLIS, DEFAULT_REQUIRED_ACKS};
use super::partitioner::{DefaultPartitioner, Partitioner, Topics};
use super::{Compression, Record, RequiredAcks, State};

/// Internal representation of a buffered record.
///
/// Owns key/value bytes and shares header values through `Bytes`.
/// The flush iterator borrows these payloads until wire encoding.
struct BatchRecord {
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<(String, Bytes)>,
}

#[derive(PartialEq, Eq)]
enum BatchConfirmation {
    Successful,
    Failed,
    Ambiguous,
}

impl BatchRecord {
    fn byte_size(&self) -> usize {
        self.key.as_ref().map_or(0, Bytes::len)
            + self.value.as_ref().map_or(0, Bytes::len)
            + self
                .headers
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    }
}

/// A producer that batches messages before sending them to Kafka.
///
/// `BatchProducer` accumulates messages internally and flushes them when
/// any of the following conditions is met:
/// - The number of buffered messages reaches `batch_size`
/// - The total buffered bytes reach `max_bytes`
/// - `linger_ms` milliseconds have elapsed since the first message in the batch
/// - `flush()` is called explicitly
pub struct BatchProducer<P = DefaultPartitioner> {
    client: KafkaClient,
    state: State<P>,
    config: Config,
    batch_config: BatchConfig,
    buffer: BTreeMap<(String, i32), Vec<BatchRecord>>,
    buffer_size: usize,
    buffer_bytes: usize,
    batch_start: Option<Instant>,
    failed_flush: bool,
}

impl BatchProducer {
    /// Starts building a new batch producer using the given Kafka client.
    #[must_use]
    pub fn from_client(client: KafkaClient) -> BatchProducerBuilder<DefaultPartitioner> {
        BatchProducerBuilder::new(Some(client), Vec::new())
    }

    /// Starts building a batch producer bootstrapping internally a new kafka
    /// client from the given kafka hosts.
    #[must_use]
    pub fn from_hosts(hosts: Vec<String>) -> BatchProducerBuilder<DefaultPartitioner> {
        BatchProducerBuilder::new(None, hosts)
    }
}

impl<P: Partitioner> BatchProducer<P> {
    /// Adds a message to the batch buffer.
    ///
    /// Returns `Ok(true)` if the batch was automatically flushed,
    /// `Ok(false)` if the message was just buffered.
    ///
    /// # Errors
    ///
    /// Returns an error if producing the batch fails, a partition reports an error,
    /// or a previous flush left unconfirmed records. Explicitly call `flush()` or
    /// `clear()` before sending new records after such a failure.
    pub fn send<K, V>(&mut self, record: &Record<'_, K, V>) -> Result<bool>
    where
        K: super::AsBytes,
        V: super::AsBytes,
    {
        if self.failed_flush {
            return Err(Error::Config(
                "batch contains unconfirmed records; call flush() or clear() before sending more records"
                    .into(),
            ));
        }

        let mut msg = client::ProduceMessage {
            key: to_option(record.key.as_bytes()),
            value: to_option(record.value.as_bytes()),
            topic: record.topic,
            partition: record.partition,
            headers: &record.headers.0,
        };
        self.state
            .partitioner
            .partition(Topics::new(&self.state.partitions), &mut msg);

        let topic = msg.topic.to_owned();
        let partition = msg.partition;
        let record_bytes = msg.key.as_ref().map_or(0, |k| k.len())
            + msg.value.as_ref().map_or(0, |v| v.len())
            + msg
                .headers
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>();

        let batch_record = BatchRecord {
            key: msg.key.map(Bytes::copy_from_slice),
            value: msg.value.map(Bytes::copy_from_slice),
            headers: msg.headers.to_vec(),
        };

        self.buffer
            .entry((topic, partition))
            .or_default()
            .push(batch_record);
        self.buffer_size += 1;
        self.buffer_bytes += record_bytes;

        if self.batch_start.is_none() {
            self.batch_start = Some(Instant::now());
        }

        if self.should_flush() {
            let confirms = self.flush()?;
            for confirm in confirms {
                for partition in confirm.partition_confirms {
                    if let Err(error_code) = partition.offset {
                        return Err(Error::TopicPartitionError {
                            topic_name: confirm.topic,
                            partition_id: partition.partition,
                            error_code,
                        });
                    }
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Flushes all buffered messages to Kafka.
    ///
    /// Successful partition confirmations remove only those partitions' records.
    /// Failed or missing confirmations retain records; partition errors are returned
    /// in the confirmation vector. Transport errors retain the entire buffer, and
    /// an explicit retry can duplicate records already accepted by a broker.
    /// No-ack mode clears the buffer only after all writes succeed.
    ///
    /// # Errors
    ///
    /// Returns an error if producing the batch fails or a required partition
    /// confirmation is missing or duplicated. Successfully confirmed partitions
    /// are removed even when another partition's confirmation is malformed.
    pub fn flush(&mut self) -> Result<Vec<ProduceConfirm>> {
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }

        let messages = self
            .buffer
            .iter()
            .flat_map(|((topic, partition), records)| {
                records.iter().map(move |r| client::ProduceMessage {
                    key: r.key.as_deref(),
                    value: r.value.as_deref(),
                    topic,
                    partition: *partition,
                    headers: &r.headers,
                })
            });

        let confirms = match self.client.internal_produce_messages(
            self.config.required_acks,
            self.config.ack_timeout,
            messages,
        ) {
            Ok(confirms) => confirms,
            Err(error) => {
                self.failed_flush = true;
                return Err(error);
            }
        };

        if self.config.required_acks == 0 {
            self.clear();
            return Ok(confirms);
        }

        self.retire_confirmed_records(&confirms)?;
        Ok(confirms)
    }

    fn retire_confirmed_records(&mut self, confirms: &[ProduceConfirm]) -> Result<()> {
        let mut acknowledgements = HashMap::new();
        for confirm in confirms {
            for partition in &confirm.partition_confirms {
                acknowledgements
                    .entry((confirm.topic.as_str(), partition.partition))
                    .and_modify(|status| *status = BatchConfirmation::Ambiguous)
                    .or_insert_with(|| {
                        if partition.offset.is_ok() {
                            BatchConfirmation::Successful
                        } else {
                            BatchConfirmation::Failed
                        }
                    });
            }
        }

        let malformed = acknowledgements
            .values()
            .any(|status| *status == BatchConfirmation::Ambiguous)
            || self.buffer.keys().any(|(topic, partition)| {
                !acknowledgements.contains_key(&(topic.as_str(), *partition))
            });
        let all_confirmed = self.buffer.keys().all(|(topic, partition)| {
            acknowledgements.get(&(topic.as_str(), *partition))
                == Some(&BatchConfirmation::Successful)
        });
        if all_confirmed {
            self.clear();
        } else {
            let mut removed_count = 0;
            let mut removed_bytes = 0;
            self.buffer.retain(|(topic, partition), records| {
                if acknowledgements.get(&(topic.as_str(), *partition))
                    == Some(&BatchConfirmation::Successful)
                {
                    removed_count += records.len();
                    removed_bytes += records.iter().map(BatchRecord::byte_size).sum::<usize>();
                    false
                } else {
                    true
                }
            });
            self.buffer_size -= removed_count;
            self.buffer_bytes -= removed_bytes;
            self.failed_flush = true;
        }

        if malformed {
            return Err(Error::Protocol(ProtocolError::Codec));
        }
        Ok(())
    }

    /// Returns the number of messages currently buffered.
    #[must_use]
    pub fn buffered_count(&self) -> usize {
        self.buffer_size
    }

    /// Returns the number of bytes currently buffered.
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.buffer_bytes
    }

    /// Discards all buffered messages without sending.
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.buffer_size = 0;
        self.buffer_bytes = 0;
        self.batch_start = None;
        self.failed_flush = false;
    }

    fn should_flush(&self) -> bool {
        if self.buffer_size >= self.batch_config.batch_size {
            return true;
        }
        if self.buffer_bytes >= self.batch_config.max_bytes {
            return true;
        }
        if let Some(start) = self.batch_start
            && start.elapsed() >= Duration::from_millis(self.batch_config.linger_ms)
        {
            return true;
        }
        false
    }
}

fn to_option(data: &[u8]) -> Option<&[u8]> {
    if data.is_empty() { None } else { Some(data) }
}

// --------------------------------------------------------------------
// Builder

use crate::protocol;

#[cfg(any(feature = "security", feature = "security-ring"))]
use crate::client::SecurityConfig;

#[cfg(not(any(feature = "security", feature = "security-ring")))]
type SecurityConfig = ();

/// Builder for constructing a `BatchProducer`.
pub struct BatchProducerBuilder<P = DefaultPartitioner> {
    client: Option<KafkaClient>,
    hosts: Vec<String>,
    compression: Compression,
    ack_timeout: Duration,
    conn_idle_timeout: Duration,
    required_acks: RequiredAcks,
    partitioner: P,
    batch_config: BatchConfig,
    security_config: Option<SecurityConfig>,
    client_id: Option<String>,
}

impl BatchProducerBuilder {
    pub(crate) fn new(
        client: Option<KafkaClient>,
        hosts: Vec<String>,
    ) -> BatchProducerBuilder<DefaultPartitioner> {
        let mut b = BatchProducerBuilder {
            client,
            hosts,
            compression: client::DEFAULT_COMPRESSION,
            ack_timeout: Duration::from_millis(DEFAULT_ACK_TIMEOUT_MILLIS),
            conn_idle_timeout: Duration::from_millis(
                client::DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS,
            ),
            required_acks: DEFAULT_REQUIRED_ACKS,
            partitioner: DefaultPartitioner::default(),
            batch_config: BatchConfig::default(),
            security_config: None,
            client_id: None,
        };
        if let Some(ref c) = b.client {
            b.compression = c.compression();
            b.conn_idle_timeout = c.connection_idle_timeout();
        }
        b
    }
}

impl BatchProducerBuilder {
    /// Specifies the security config to use.
    #[cfg(any(feature = "security", feature = "security-ring"))]
    #[must_use]
    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.security_config = Some(security);
        self
    }

    /// Sets the compression algorithm to use when sending out data.
    #[must_use]
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the maximum time the kafka brokers can await the receipt
    /// of required acknowledgements.
    #[must_use]
    pub fn with_ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = timeout;
        self
    }

    /// Specifies the timeout for idle connections.
    #[must_use]
    pub fn with_connection_idle_timeout(mut self, timeout: Duration) -> Self {
        self.conn_idle_timeout = timeout;
        self
    }

    /// Sets how many acknowledgements the kafka brokers should
    /// receive before responding to sent messages.
    #[must_use]
    pub fn with_required_acks(mut self, acks: RequiredAcks) -> Self {
        self.required_acks = acks;
        self
    }

    /// Specifies a `client_id` to be sent along every request to Kafka
    /// brokers.
    #[must_use]
    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.client_id = Some(client_id);
        self
    }

    /// Sets the maximum number of messages per batch.
    #[must_use]
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_config.batch_size = size;
        self
    }

    /// Sets the maximum time to wait before flushing a batch (milliseconds).
    #[must_use]
    pub fn with_linger(mut self, millis: u64) -> Self {
        self.batch_config.linger_ms = millis;
        self
    }

    /// Sets the maximum total bytes per batch.
    #[must_use]
    pub fn with_max_batch_bytes(mut self, bytes: usize) -> Self {
        self.batch_config.max_bytes = bytes;
        self
    }

    /// Sets the batch configuration.
    #[must_use]
    pub fn with_batch_config(mut self, config: BatchConfig) -> Self {
        self.batch_config = config;
        self
    }
}

impl<P> BatchProducerBuilder<P> {
    /// Sets the partitioner to dispatch when sending messages without
    /// an explicit partition assignment.
    pub fn with_partitioner<Q: Partitioner>(self, partitioner: Q) -> BatchProducerBuilder<Q> {
        BatchProducerBuilder {
            client: self.client,
            hosts: self.hosts,
            compression: self.compression,
            ack_timeout: self.ack_timeout,
            conn_idle_timeout: self.conn_idle_timeout,
            required_acks: self.required_acks,
            partitioner,
            batch_config: self.batch_config,
            security_config: self.security_config,
            client_id: self.client_id,
        }
    }

    #[cfg(not(any(feature = "security", feature = "security-ring")))]
    fn new_kafka_client(hosts: Vec<String>, _: Option<SecurityConfig>) -> KafkaClient {
        KafkaClient::new(hosts)
    }

    #[cfg(any(feature = "security", feature = "security-ring"))]
    fn new_kafka_client(hosts: Vec<String>, security: Option<SecurityConfig>) -> KafkaClient {
        if let Some(security) = security {
            KafkaClient::new_secure(hosts, security)
        } else {
            KafkaClient::new(hosts)
        }
    }

    /// Creates/builds a new batch producer based on the so far supplied settings.
    ///
    /// # Errors
    ///
    /// Returns an error if timeout conversion fails, metadata loading fails, or producer state initialization fails.
    pub fn create(self) -> Result<BatchProducer<P>> {
        let (mut client, need_metadata) = match self.client {
            Some(client) => (client, false),
            None => (
                Self::new_kafka_client(self.hosts, self.security_config),
                true,
            ),
        };
        #[cfg(feature = "producer_timestamp")]
        crate::client::produce_ops::validate_producer_timestamp(client.producer_timestamp())?;
        client.set_compression(self.compression);
        client.set_connection_idle_timeout(self.conn_idle_timeout);
        if let Some(client_id) = self.client_id {
            client.set_client_id(client_id);
        }
        let producer_config = Config {
            ack_timeout: protocol::to_millis_i32(self.ack_timeout)?,
            required_acks: self.required_acks as i16,
            enable_idempotence: false,
            transactional_id: None,
        };
        if need_metadata {
            client.load_metadata_all()?;
        }
        let state = State::new(&mut client, self.partitioner);
        Ok(BatchProducer {
            client,
            state,
            config: producer_config,
            batch_config: self.batch_config,
            buffer: BTreeMap::new(),
            buffer_size: 0,
            buffer_bytes: 0,
            batch_start: None,
            failed_flush: false,
        })
    }
}

// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::config::{DEFAULT_BATCH_SIZE, DEFAULT_LINGER_MS, DEFAULT_MAX_BATCH_BYTES};
    use super::*;

    #[test]
    fn custom_partitioner_preserves_batch_configuration() {
        let builder = BatchProducer::from_hosts(vec!["broker:9092".to_owned()])
            .with_client_id("batch-client".to_owned())
            .with_ack_timeout(Duration::from_secs(7))
            .with_connection_idle_timeout(Duration::from_secs(11))
            .with_required_acks(RequiredAcks::All)
            .with_batch_config(BatchConfig {
                batch_size: 100,
                linger_ms: 20,
                max_bytes: 8192,
            })
            .with_partitioner(super::super::RoundRobinPartitioner::new());

        assert_eq!(builder.hosts, vec!["broker:9092"]);
        assert_eq!(builder.client_id.as_deref(), Some("batch-client"));
        assert_eq!(builder.ack_timeout, Duration::from_secs(7));
        assert_eq!(builder.conn_idle_timeout, Duration::from_secs(11));
        assert!(matches!(builder.required_acks, RequiredAcks::All));
        assert_eq!(builder.batch_config.batch_size, 100);
        assert_eq!(builder.batch_config.linger_ms, 20);
        assert_eq!(builder.batch_config.max_bytes, 8192);
    }

    #[cfg(feature = "producer_timestamp")]
    #[test]
    fn batch_constructor_rejects_an_inherited_log_append_time_mode() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let client = KafkaClient::builder()
            .with_hosts(vec![listener.local_addr().unwrap().to_string()])
            .with_producer_timestamp(Some(crate::client::ProducerTimestamp::LogAppendTime))
            .build();
        let result = BatchProducer::from_client(client).create();
        assert!(matches!(result, Err(Error::Config(message))
            if message.contains("message.timestamp.type=LogAppendTime")));
        assert!(
            listener
                .accept()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
        );
    }

    #[cfg(any(feature = "security", feature = "security-ring"))]
    #[test]
    fn custom_partitioner_preserves_batch_security_configuration() {
        let builder = BatchProducer::from_hosts(Vec::new())
            .with_security(SecurityConfig::new().with_sasl_plain("user".into(), "password".into()))
            .with_partitioner(super::super::RoundRobinPartitioner::new());

        let security = builder.security_config.unwrap();
        assert_eq!(security.sasl_config.unwrap().username(), "user");
    }

    #[test]
    fn test_batch_config_default() {
        let config = BatchConfig::default();
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.linger_ms, DEFAULT_LINGER_MS);
        assert_eq!(config.max_bytes, DEFAULT_MAX_BATCH_BYTES);
    }

    #[test]
    fn test_batch_record_byte_size() {
        let r = BatchRecord {
            key: Some(Bytes::from_static(&[1, 2, 3])),
            value: Some(Bytes::from_static(&[4, 5])),
            headers: vec![("k".to_string(), Bytes::from_static(&[6]))],
        };
        assert_eq!(r.byte_size(), 3 + 2 + 2);
    }

    #[test]
    fn test_batch_record_empty_byte_size() {
        let r = BatchRecord {
            key: None,
            value: None,
            headers: vec![],
        };
        assert_eq!(r.byte_size(), 0);
    }

    fn make_test_producer(batch_config: BatchConfig) -> BatchProducer<DefaultPartitioner> {
        BatchProducer {
            client: KafkaClient::new(vec![]),
            state: State {
                partitions: std::collections::HashMap::new(),
                partitioner: DefaultPartitioner::default(),
            },
            config: Config {
                ack_timeout: 30000,
                required_acks: 1,
                enable_idempotence: false,
                transactional_id: None,
            },
            batch_config,
            buffer: BTreeMap::new(),
            buffer_size: 0,
            buffer_bytes: 0,
            batch_start: None,
            failed_flush: false,
        }
    }

    #[test]
    fn test_should_flush_on_batch_size() {
        let mut bp = make_test_producer(BatchConfig {
            batch_size: 3,
            linger_ms: 5000,
            max_bytes: 1_048_576,
        });
        bp.buffer_size = 3;
        bp.buffer_bytes = 100;
        bp.batch_start = Some(Instant::now());
        assert!(bp.should_flush());
    }

    #[test]
    fn test_should_flush_on_max_bytes() {
        let mut bp = make_test_producer(BatchConfig {
            batch_size: 16_384,
            linger_ms: 5000,
            max_bytes: 100,
        });
        bp.buffer_size = 1;
        bp.buffer_bytes = 100;
        bp.batch_start = Some(Instant::now());
        assert!(bp.should_flush());
    }

    #[test]
    fn test_should_not_flush() {
        let mut bp = make_test_producer(BatchConfig {
            batch_size: 16_384,
            linger_ms: 5000,
            max_bytes: 1_048_576,
        });
        bp.buffer_size = 1;
        bp.buffer_bytes = 50;
        bp.batch_start = Some(Instant::now());
        assert!(!bp.should_flush());
    }

    #[test]
    fn test_clear_resets_state() {
        let mut bp = make_test_producer(BatchConfig::default());
        bp.buffer.insert(
            ("t".to_string(), 0),
            vec![BatchRecord {
                key: Some(Bytes::from_static(&[1])),
                value: Some(Bytes::from_static(&[2])),
                headers: vec![],
            }],
        );
        bp.buffer_size = 1;
        bp.buffer_bytes = 3;
        bp.batch_start = Some(Instant::now());

        bp.clear();
        assert!(bp.buffer.is_empty());
        assert_eq!(bp.buffered_count(), 0);
        assert_eq!(bp.buffered_bytes(), 0);
        assert!(bp.batch_start.is_none());
    }
}

#[cfg(test)]
mod delivery_tests {
    use super::*;
    use crate::error::KafkaCode;
    use crate::producer::Headers;
    use bytes::BytesMut;
    use kafka_protocol::messages::api_versions_response::ApiVersion;
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    use kafka_protocol::messages::{
        ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, MetadataRequest,
        MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader,
        TopicName,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
    use kafka_protocol::records::RecordBatchDecoder;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::thread::JoinHandle;

    type ObservedBatch = Vec<(String, i32, usize)>;

    enum BrokerReply {
        Confirm(Vec<(&'static str, i32, i16)>),
        Disconnect,
        NoAcks,
    }

    fn mock_batch_producer(
        replies: Vec<BrokerReply>,
        acks: RequiredAcks,
        batch_size: usize,
    ) -> (BatchProducer, JoinHandle<Vec<ObservedBatch>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let (header, _) = read_request::<ApiVersionsRequest>(&mut stream, ApiKey::ApiVersions);
            let versions = ApiVersionsResponse::default().with_api_keys(vec![
                ApiVersion::default()
                    .with_api_key(ApiKey::Produce as i16)
                    .with_min_version(0)
                    .with_max_version(9),
                ApiVersion::default()
                    .with_api_key(ApiKey::Metadata as i16)
                    .with_min_version(0)
                    .with_max_version(1),
            ]);
            write_response(&mut stream, &header, &versions);
            let (header, _) = read_request::<MetadataRequest>(&mut stream, ApiKey::Metadata);
            write_response(&mut stream, &header, &metadata_response(address));

            let mut observed = Vec::new();
            for reply in replies {
                let (header, request) =
                    read_request::<ProduceRequest>(&mut stream, ApiKey::Produce);
                let mut partitions = Vec::new();
                for topic in request.topic_data {
                    for partition in topic.partition_data {
                        let mut records = partition.records.unwrap();
                        let count = RecordBatchDecoder::decode_all(&mut records)
                            .unwrap()
                            .iter()
                            .map(|batch| batch.records.len())
                            .sum();
                        partitions.push((topic.name.to_string(), partition.index, count));
                    }
                }
                partitions.sort_unstable();
                observed.push(partitions);
                match reply {
                    BrokerReply::Confirm(confirms) => {
                        write_response(&mut stream, &header, &produce_response(confirms));
                    }
                    BrokerReply::Disconnect => break,
                    BrokerReply::NoAcks => assert_eq!(request.acks, 0),
                }
            }
            observed
        });
        let producer = BatchProducer::from_hosts(vec![address.to_string()])
            .with_required_acks(acks)
            .with_batch_size(batch_size)
            .with_linger(60_000)
            .create()
            .unwrap();
        (producer, server)
    }

    fn read_request<T: Decodable + HeaderVersion>(
        stream: &mut TcpStream,
        api_key: ApiKey,
    ) -> (RequestHeader, T) {
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let mut frame = vec![0; usize::try_from(i32::from_be_bytes(length)).unwrap()];
        stream.read_exact(&mut frame).unwrap();
        let version = i16::from_be_bytes(frame[2..4].try_into().unwrap());
        let mut frame = Bytes::from(frame);
        let header = RequestHeader::decode(&mut frame, T::header_version(version)).unwrap();
        assert_eq!(header.request_api_key, api_key as i16);
        let request = T::decode(&mut frame, version).unwrap();
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

    fn metadata_response(address: SocketAddr) -> MetadataResponse {
        let broker_id = BrokerId::from(1);
        MetadataResponse::default()
            .with_brokers(vec![
                MetadataResponseBroker::default()
                    .with_node_id(broker_id)
                    .with_host(StrBytes::from_string(address.ip().to_string()))
                    .with_port(i32::from(address.port())),
            ])
            .with_topics(
                ["t", "u"]
                    .into_iter()
                    .map(|topic| {
                        MetadataResponseTopic::default()
                            .with_name(Some(TopicName::from(StrBytes::from_static_str(topic))))
                            .with_partitions(
                                [0, 1]
                                    .into_iter()
                                    .map(|partition| {
                                        MetadataResponsePartition::default()
                                            .with_partition_index(partition)
                                            .with_leader_id(broker_id)
                                            .with_replica_nodes(vec![broker_id])
                                            .with_isr_nodes(vec![broker_id])
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            )
    }

    fn produce_response(confirms: Vec<(&'static str, i32, i16)>) -> ProduceResponse {
        let mut topics: BTreeMap<&'static str, Vec<PartitionProduceResponse>> = BTreeMap::new();
        for (topic, partition, error) in confirms {
            topics.entry(topic).or_default().push(
                PartitionProduceResponse::default()
                    .with_index(partition)
                    .with_error_code(error)
                    .with_base_offset(42),
            );
        }
        ProduceResponse::default().with_responses(
            topics
                .into_iter()
                .map(|(topic, partitions)| {
                    TopicProduceResponse::default()
                        .with_name(TopicName::from(StrBytes::from_static_str(topic)))
                        .with_partition_responses(partitions)
                })
                .collect(),
        )
    }

    #[test]
    fn flush_success_clears_the_buffer_without_an_extra_empty_request() {
        let (mut producer, server) = mock_batch_producer(
            vec![BrokerReply::Confirm(vec![("t", 0, 0), ("u", 0, 0)])],
            RequiredAcks::One,
            100,
        );
        producer
            .send(&Record::from_value("t", "first").with_partition(0))
            .unwrap();
        producer
            .send(&Record::from_value("u", "second").with_partition(0))
            .unwrap();
        let confirms = producer.flush().unwrap();
        assert_eq!(confirms.len(), 2);
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (0, 0)
        );
        assert!(producer.batch_start.is_none());
        assert!(!producer.failed_flush);
        assert!(producer.flush().unwrap().is_empty());
        assert_eq!(
            server.join().unwrap(),
            vec![vec![("t".into(), 0, 1), ("u".into(), 0, 1)]]
        );
    }

    #[test]
    fn partial_failure_retires_only_successful_topic_partitions() {
        let (mut producer, server) = mock_batch_producer(
            vec![
                BrokerReply::Confirm(vec![
                    ("t", 0, 0),
                    ("t", 1, 0),
                    ("u", 0, KafkaCode::NotLeaderForPartition as i16),
                ]),
                BrokerReply::Confirm(vec![("u", 0, 0)]),
            ],
            RequiredAcks::One,
            100,
        );
        let mut headers = Headers::new();
        headers.insert("h", "v");
        producer
            .send(
                &Record::from_key_value("t", "k", "sent")
                    .with_partition(0)
                    .with_headers(headers),
            )
            .unwrap();
        producer
            .send(&Record::from_value("t", "ok").with_partition(1))
            .unwrap();
        producer
            .send(&Record::from_value("u", "pending").with_partition(0))
            .unwrap();
        producer
            .send(&Record::from_value("u", "tail").with_partition(0))
            .unwrap();
        let started = producer.batch_start;
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (4, 20)
        );

        let confirms = producer.flush().unwrap();
        assert!(confirms.iter().any(|confirm| {
            confirm
                .partition_confirms
                .iter()
                .any(|partition| partition.offset.is_err())
        }));
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (2, 11)
        );
        assert_eq!(producer.batch_start, started);
        assert!(
            producer
                .send(&Record::from_value("u", "new").with_partition(0))
                .is_err()
        );
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (2, 11)
        );
        producer.flush().unwrap();
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (0, 0)
        );
        assert!(producer.batch_start.is_none());
        assert!(!producer.failed_flush);
        assert_eq!(
            server.join().unwrap(),
            vec![
                vec![("t".into(), 0, 1), ("t".into(), 1, 1), ("u".into(), 0, 2)],
                vec![("u".into(), 0, 2)],
            ]
        );
    }

    #[test]
    fn missing_confirmation_retains_only_the_unconfirmed_partition() {
        let (mut producer, server) = mock_batch_producer(
            vec![
                BrokerReply::Confirm(vec![("t", 0, 0)]),
                BrokerReply::Confirm(vec![("t", 1, 0)]),
            ],
            RequiredAcks::One,
            100,
        );
        producer
            .send(&Record::from_value("t", "ok").with_partition(0))
            .unwrap();
        producer
            .send(&Record::from_value("t", "missing").with_partition(1))
            .unwrap();
        let started = producer.batch_start;
        assert!(matches!(
            producer.flush(),
            Err(Error::Protocol(ProtocolError::Codec))
        ));
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (1, 7)
        );
        assert_eq!(producer.batch_start, started);
        assert!(producer.failed_flush);
        producer.flush().unwrap();
        assert_eq!(producer.buffered_count(), 0);
        assert_eq!(server.join().unwrap()[1], vec![("t".into(), 1, 1)]);
    }

    #[test]
    fn duplicate_or_conflicting_confirmations_keep_records_pending() {
        for duplicate_error in [0, KafkaCode::NotLeaderForPartition as i16] {
            let (mut producer, server) = mock_batch_producer(
                vec![
                    BrokerReply::Confirm(vec![("t", 0, 0), ("t", 0, duplicate_error)]),
                    BrokerReply::Confirm(vec![("t", 0, 0)]),
                ],
                RequiredAcks::One,
                100,
            );
            producer
                .send(&Record::from_value("t", "pending").with_partition(0))
                .unwrap();
            let started = producer.batch_start;
            assert!(matches!(
                producer.flush(),
                Err(Error::Protocol(ProtocolError::Codec))
            ));
            assert_eq!(
                (producer.buffered_count(), producer.buffered_bytes()),
                (1, 7)
            );
            assert_eq!(producer.batch_start, started);
            assert!(producer.failed_flush);
            producer.flush().unwrap();
            assert_eq!(producer.buffered_count(), 0);
            assert_eq!(server.join().unwrap().len(), 2);
        }
    }

    #[test]
    fn transport_failure_retains_the_batch_and_clear_releases_the_send_guard() {
        let (mut producer, server) =
            mock_batch_producer(vec![BrokerReply::Disconnect], RequiredAcks::One, 100);
        producer
            .send(&Record::from_value("t", "first").with_partition(0))
            .unwrap();
        producer
            .send(&Record::from_value("u", "second").with_partition(0))
            .unwrap();
        let started = producer.batch_start;
        assert!(producer.flush().is_err());
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (2, 11)
        );
        assert_eq!(producer.batch_start, started);
        assert!(
            producer
                .send(&Record::from_value("t", "new").with_partition(0))
                .is_err()
        );
        assert_eq!(producer.buffered_count(), 2);
        assert!(producer.flush().is_err());
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (2, 11)
        );
        producer.clear();
        assert!(!producer.failed_flush);
        assert!(producer.batch_start.is_none());
        assert!(
            !producer
                .send(&Record::from_value("t", "new").with_partition(0))
                .unwrap()
        );
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (1, 3)
        );
        assert_eq!(server.join().unwrap().len(), 1);
    }

    #[test]
    fn auto_flush_reports_partition_errors_without_implicitly_retrying() {
        let (mut producer, server) = mock_batch_producer(
            vec![
                BrokerReply::Confirm(vec![("t", 0, KafkaCode::NotLeaderForPartition as i16)]),
                BrokerReply::Confirm(vec![("t", 0, 0)]),
            ],
            RequiredAcks::One,
            1,
        );
        assert!(matches!(
            producer.send(&Record::from_value("t", "pending").with_partition(0)),
            Err(Error::TopicPartitionError {
                error_code: KafkaCode::NotLeaderForPartition,
                ..
            })
        ));
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (1, 7)
        );
        assert!(matches!(
            producer.send(&Record::from_value("t", "new").with_partition(0)),
            Err(Error::Config(_))
        ));
        assert_eq!(producer.buffered_count(), 1);
        producer.flush().unwrap();
        assert_eq!(producer.buffered_count(), 0);
        assert!(!producer.failed_flush);
        assert_eq!(
            server.join().unwrap(),
            vec![vec![("t".into(), 0, 1)], vec![("t".into(), 0, 1)]]
        );
    }

    #[test]
    fn auto_flush_reports_missing_confirmation_and_keeps_the_record() {
        let (mut producer, server) =
            mock_batch_producer(vec![BrokerReply::Confirm(vec![])], RequiredAcks::One, 1);
        assert!(matches!(
            producer.send(&Record::from_value("t", "pending").with_partition(0)),
            Err(Error::Protocol(ProtocolError::Codec))
        ));
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (1, 7)
        );
        assert!(producer.failed_flush);
        assert_eq!(server.join().unwrap().len(), 1);
    }

    #[test]
    fn no_ack_flush_clears_only_after_the_request_is_written() {
        let (mut producer, server) =
            mock_batch_producer(vec![BrokerReply::NoAcks], RequiredAcks::None, 100);
        producer
            .send(&Record::from_value("t", "written").with_partition(0))
            .unwrap();
        assert!(producer.flush().unwrap().is_empty());
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (0, 0)
        );
        assert!(producer.batch_start.is_none());
        assert_eq!(server.join().unwrap(), vec![vec![("t".into(), 0, 1)]]);

        producer.client.reset_metadata();
        producer
            .send(&Record::from_value("t", "retained").with_partition(0))
            .unwrap();
        assert!(producer.flush().is_err());
        assert_eq!(
            (producer.buffered_count(), producer.buffered_bytes()),
            (1, 8)
        );
        assert!(producer.failed_flush);
    }
}
