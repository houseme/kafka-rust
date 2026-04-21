//! Kafka Producer - A higher-level API for sending messages to Kafka
//! topics.
//!
//! This module hosts a multi-topic capable producer for a Kafka
//! cluster providing a convenient API for sending messages
//! synchronously.
//!
//! In Kafka, each message is a key/value pair where one or the other
//! is optional.  A `Record` represents all the data necessary to
//! produce such a message to Kafka using the `Producer`.  It
//! specifies the target topic and the target partition the message is
//! supposed to be delivered to as well as the key and the value.
//!
//! # Example
//! ```no_run
//! use std::fmt::Write;
//! use std::time::Duration;
//! use rustfs_kafka::producer::{Producer, Record, RequiredAcks};
//!
//! let mut producer =
//!     Producer::from_hosts(vec!("localhost:9092".to_owned()))
//!         .with_ack_timeout(Duration::from_secs(1))
//!         .with_required_acks(RequiredAcks::One)
//!         .create()
//!         .unwrap();
//!
//! let mut buf = String::with_capacity(2);
//! for i in 0..10 {
//!   let _ = write!(&mut buf, "{}", i); // some computation of the message data to be sent
//!   producer.send(&Record::from_value("my-topic", buf.as_bytes())).unwrap();
//!   buf.clear();
//! }
//! ```

// XXX 1) rethink return values for the send_all() method
// XXX 2) Handle recoverable errors behind the scenes through retry attempts

mod batch;
mod builder;
mod config;
mod partitioner;
mod record;
mod transaction;

pub use self::batch::{BatchProducer, BatchProducerBuilder};
pub use self::partitioner::{
    DefaultPartitioner, Partitioner, RoundRobinPartitioner, StickyPartitioner, UniformPartitioner,
};
pub use self::record::{AsBytes, Headers, Record};
pub use config::BatchConfig;
pub use transaction::{TransactionalBuilder, TransactionalProducer};

pub use crate::client::{Compression, ProduceConfirm, ProducePartitionConfirm, RequiredAcks};
pub use config::DEFAULT_ACK_TIMEOUT_MILLIS;
pub use config::DEFAULT_REQUIRED_ACKS;

use crate::client::KafkaClientInternals;
use crate::client::{self, KafkaClient};
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::slice::from_ref;

use config::Config;
use partitioner::Topics;

pub(crate) struct State<P> {
    partitions: HashMap<String, partitioner::Partitions>,
    partitioner: P,
}

// --------------------------------------------------------------------

/// The Kafka Producer
///
/// See module level documentation.
pub struct Producer<P = DefaultPartitioner> {
    client: KafkaClient,
    state: State<P>,
    config: Config,
}

impl Producer {
    /// Starts building a new producer using the given Kafka client.
    #[must_use]
    pub fn from_client(client: KafkaClient) -> builder::Builder<DefaultPartitioner> {
        Builder::new(Some(client), Vec::new())
    }

    /// Starts building a producer bootstrapping internally a new kafka
    /// client from the given kafka hosts.
    #[must_use]
    pub fn from_hosts(hosts: Vec<String>) -> builder::Builder<DefaultPartitioner> {
        Builder::new(None, hosts)
    }

    /// Borrows the underlying kafka client.
    #[must_use]
    pub fn client(&self) -> &KafkaClient {
        &self.client
    }

    /// Borrows the underlying kafka client as mut.
    pub fn client_mut(&mut self) -> &mut KafkaClient {
        &mut self.client
    }

    /// Destroys this producer returning the underlying kafka client.
    #[must_use]
    pub fn into_client(self) -> KafkaClient {
        self.client
    }
}

impl<P: Partitioner> Producer<P> {
    /// Synchronously send the specified message to Kafka.
    ///
    /// # Panics
    ///
    /// Panics if the response contains an unexpected number of confirms
    /// (this indicates a protocol-level bug).
    ///
    /// # Errors
    ///
    /// Returns an error if producing the message to Kafka fails or if
    /// the broker reports an error for the target partition.
    pub fn send<K, V>(&mut self, rec: &Record<K, V>) -> Result<()>
    where
        K: AsBytes,
        V: AsBytes,
    {
        let mut rs = self.send_all(from_ref(rec))?;

        if self.config.required_acks == 0 {
            Ok(())
        } else {
            assert_eq!(1, rs.len());
            let mut produce_confirm = rs.pop().unwrap();

            assert_eq!(1, produce_confirm.partition_confirms.len());
            produce_confirm
                .partition_confirms
                .pop()
                .unwrap()
                .offset
                .map_err(Error::Kafka)?;
            Ok(())
        }
    }

    /// Synchronously send all of the specified messages to Kafka. To validate
    /// that all of the specified records have been successfully delivered,
    /// inspection of the offsets on the returned confirms is necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if producing messages to Kafka fails or if
    /// any target topic/partition is unknown.
    pub fn send_all<K, V>(&mut self, recs: &[Record<'_, K, V>]) -> Result<Vec<ProduceConfirm>>
    where
        K: AsBytes,
        V: AsBytes,
    {
        let partitioner = &mut self.state.partitioner;
        let partitions = &self.state.partitions;
        let client = &mut self.client;
        let config = &self.config;

        client.internal_produce_messages(
            config.required_acks,
            config.ack_timeout,
            recs.iter().map(|r| {
                let mut m = client::ProduceMessage {
                    key: to_option(r.key.as_bytes()),
                    value: to_option(r.value.as_bytes()),
                    topic: r.topic,
                    partition: r.partition,
                    headers: &r.headers.0,
                };
                partitioner.partition(Topics::new(partitions), &mut m);
                m
            }),
        )
    }
}

fn to_option(data: &[u8]) -> Option<&[u8]> {
    if data.is_empty() { None } else { Some(data) }
}

// --------------------------------------------------------------------

impl<P> State<P> {
    pub(crate) fn new(client: &mut KafkaClient, partitioner: P) -> State<P> {
        let ts = client.topics();
        let mut ids = HashMap::with_capacity(ts.len());
        for t in ts {
            let ps = t.partitions();
            #[allow(clippy::cast_possible_truncation)]
            let num_all_partitions = ps.len() as u32;
            ids.insert(
                t.name().to_owned(),
                partitioner::Partitions {
                    available_ids: ps.available_ids(),
                    num_all_partitions,
                },
            );
        }
        State {
            partitions: ids,
            partitioner,
        }
    }
}

// Re-export Builder for public API
pub use builder::Builder;
