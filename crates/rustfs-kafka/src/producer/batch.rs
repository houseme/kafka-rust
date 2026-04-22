use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::client::{self, KafkaClient, KafkaClientInternals, ProduceConfirm};
use crate::error::Result;

use super::config::BatchConfig;
use super::config::{Config, DEFAULT_ACK_TIMEOUT_MILLIS, DEFAULT_REQUIRED_ACKS};
use super::partitioner::{DefaultPartitioner, Partitioner, Topics};
use super::{Compression, Record, RequiredAcks, State};

/// Internal representation of a buffered record.
///
/// Uses `Bytes` for key/value/headers to enable zero-copy sharing
/// when the batch is flushed and encoded for the wire protocol.
struct BatchRecord {
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<(String, Bytes)>,
}

impl BatchRecord {
    #[cfg(test)]
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
    /// Returns an error if producing the batch to Kafka fails.
    pub fn send<K, V>(&mut self, record: &Record<'_, K, V>) -> Result<bool>
    where
        K: super::AsBytes,
        V: super::AsBytes,
    {
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
            self.flush()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Flushes all buffered messages to Kafka.
    ///
    /// # Errors
    ///
    /// Returns an error if producing the batch to Kafka fails.
    pub fn flush(&mut self) -> Result<Vec<ProduceConfirm>> {
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }

        let messages: Vec<client::ProduceMessage<'_, '_>> = self
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
            })
            .collect();

        let result = self.client.internal_produce_messages(
            self.config.required_acks,
            self.config.ack_timeout,
            messages.iter(),
        );

        self.buffer.clear();
        self.buffer_size = 0;
        self.buffer_bytes = 0;
        self.batch_start = None;

        result
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

#[cfg(feature = "security")]
use crate::client::SecurityConfig;

#[cfg(not(feature = "security"))]
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
    #[cfg(feature = "security")]
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
            security_config: None,
            client_id: None,
        }
    }

    #[cfg(not(feature = "security"))]
    fn new_kafka_client(hosts: Vec<String>, _: Option<SecurityConfig>) -> KafkaClient {
        KafkaClient::new(hosts)
    }

    #[cfg(feature = "security")]
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
        })
    }
}

// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::config::{DEFAULT_BATCH_SIZE, DEFAULT_LINGER_MS, DEFAULT_MAX_BATCH_BYTES};
    use super::*;

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
