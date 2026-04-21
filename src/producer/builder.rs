use std::time::Duration;

use crate::client::{self, KafkaClient};
use crate::error::Result;
use crate::protocol;

#[cfg(feature = "producer_timestamp")]
use crate::protocol::produce::ProducerTimestamp;

#[cfg(feature = "security")]
use crate::client::SecurityConfig;

#[cfg(not(feature = "security"))]
type SecurityConfig = ();

use super::config::{Config, DEFAULT_ACK_TIMEOUT_MILLIS, DEFAULT_REQUIRED_ACKS};
use super::partitioner::DefaultPartitioner;
use super::{Compression, Partitioner, Producer, RequiredAcks, State};

/// A Kafka Producer builder easing the process of setting up various
/// configuration settings.
pub struct Builder<P = DefaultPartitioner> {
    client: Option<KafkaClient>,
    hosts: Vec<String>,
    compression: Compression,
    ack_timeout: Duration,
    conn_idle_timeout: Duration,
    required_acks: RequiredAcks,
    partitioner: P,
    security_config: Option<SecurityConfig>,
    client_id: Option<String>,
    enable_idempotence: bool,
    transactional_id: Option<String>,
    #[cfg(feature = "producer_timestamp")]
    producer_timestamp: Option<ProducerTimestamp>,
}

impl Builder {
    pub(crate) fn new(
        client: Option<KafkaClient>,
        hosts: Vec<String>,
    ) -> Builder<DefaultPartitioner> {
        let mut b = Builder {
            client,
            hosts,
            compression: client::DEFAULT_COMPRESSION,
            ack_timeout: Duration::from_millis(DEFAULT_ACK_TIMEOUT_MILLIS),
            conn_idle_timeout: Duration::from_millis(
                client::DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS,
            ),
            required_acks: DEFAULT_REQUIRED_ACKS,
            partitioner: DefaultPartitioner::default(),
            security_config: None,
            client_id: None,
            enable_idempotence: false,
            transactional_id: None,
            #[cfg(feature = "producer_timestamp")]
            producer_timestamp: None,
        };
        if let Some(ref c) = b.client {
            b.compression = c.compression();
            b.conn_idle_timeout = c.connection_idle_timeout();
        }
        b
    }

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

    #[cfg(feature = "producer_timestamp")]
    /// Sets the producer timestamp mode.
    #[must_use]
    pub fn with_timestamp(mut self, timestamp: ProducerTimestamp) -> Self {
        self.producer_timestamp = Some(timestamp);
        self
    }

    /// Enables idempotent producer mode (exactly-once per partition).
    ///
    /// When enabled, the producer will request a Producer ID from the broker
    /// and include sequence numbers in produce requests. This requires
    /// `RequiredAcks::All`.
    #[must_use]
    pub fn with_idempotence(mut self, enabled: bool) -> Self {
        self.enable_idempotence = enabled;
        if enabled {
            self.required_acks = RequiredAcks::All;
        }
        self
    }

    /// Sets the transactional ID for transactional producer mode.
    ///
    /// This implicitly enables idempotence and sets `RequiredAcks::All`.
    #[must_use]
    pub fn with_transactional_id(mut self, id: impl Into<String>) -> Self {
        self.transactional_id = Some(id.into());
        self.enable_idempotence = true;
        self.required_acks = RequiredAcks::All;
        self
    }
}

impl<P> Builder<P> {
    /// Sets the partitioner to dispatch when sending messages without
    /// an explicit partition assignment.
    pub fn with_partitioner<Q: Partitioner>(self, partitioner: Q) -> Builder<Q> {
        Builder {
            client: self.client,
            hosts: self.hosts,
            compression: self.compression,
            ack_timeout: self.ack_timeout,
            conn_idle_timeout: self.conn_idle_timeout,
            required_acks: self.required_acks,
            partitioner,
            security_config: None,
            client_id: None,
            enable_idempotence: self.enable_idempotence,
            transactional_id: self.transactional_id,
            #[cfg(feature = "producer_timestamp")]
            producer_timestamp: None,
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

    /// Creates/builds a new producer based on the so far supplied settings.
    pub fn create(self) -> Result<Producer<P>> {
        let (mut client, need_metadata) = match self.client {
            Some(client) => (client, false),
            None => (
                Self::new_kafka_client(self.hosts, self.security_config),
                true,
            ),
        };
        client.set_compression(self.compression);
        client.set_connection_idle_timeout(self.conn_idle_timeout);
        #[cfg(feature = "producer_timestamp")]
        client.set_producer_timestamp(self.producer_timestamp);
        if let Some(client_id) = self.client_id {
            client.set_client_id(client_id);
        }
        let producer_config = Config {
            ack_timeout: protocol::to_millis_i32(self.ack_timeout)?,
            required_acks: self.required_acks as i16,
            enable_idempotence: self.enable_idempotence,
            transactional_id: self.transactional_id,
        };
        if need_metadata {
            client.load_metadata_all()?;
        }
        let state = State::new(&mut client, self.partitioner);
        Ok(Producer {
            client,
            state,
            config: producer_config,
        })
    }
}
