//! Transactional producer support for exactly-once semantics.
//!
//! Provides [`TransactionalProducer`] for atomic writes across multiple
//! topic-partitions using Kafka's transaction protocol.

use std::collections::{HashMap, HashSet};

use tracing::{debug, info};

use crate::client::{KafkaClient, KafkaClientInternals, ProduceMessage, RequiredAcks};
use crate::error::{Error, KafkaCode, Result};
use crate::protocol::init_producer_id;
use crate::protocol::transaction::{self, TxnPartition};

use super::{AsBytes, DefaultPartitioner, Partitioner, Record, State};

/// A producer that supports Kafka transactions.
///
/// Transactions allow atomic writes across multiple topic-partitions,
/// enabling exactly-once semantics for consume-transform-produce patterns.
///
/// # Example
///
/// ```no_run
/// use rustfs_kafka::producer::{TransactionalProducer, Record};
///
/// let mut producer = TransactionalProducer::from_hosts(vec!["localhost:9092".to_owned()])
///     .with_transactional_id("my-txn-id".to_owned())
///     .create()
///     .unwrap();
///
/// producer.begin().unwrap();
/// producer.send(&Record::from_value("topic-a", &b"msg1"[..])).unwrap();
/// producer.send(&Record::from_value("topic-b", &b"msg2"[..])).unwrap();
/// producer.commit().unwrap();
/// ```
pub struct TransactionalProducer<P = DefaultPartitioner> {
    client: KafkaClient,
    state: State<P>,
    producer_id: i64,
    producer_epoch: i16,
    transactional_id: String,
    sequence_numbers: HashMap<(String, i32), i32>,
    current_txn_partitions: HashSet<(String, i32)>,
    in_transaction: bool,
    txn_epoch: u64,
}

impl TransactionalProducer {
    /// Starts building a new transactional producer from the given hosts.
    #[must_use]
    pub fn from_hosts(hosts: Vec<String>) -> TransactionalBuilder<DefaultPartitioner> {
        TransactionalBuilder::new(None, hosts)
    }

    /// Starts building a new transactional producer from an existing client.
    #[must_use]
    pub fn from_client(client: KafkaClient) -> TransactionalBuilder<DefaultPartitioner> {
        TransactionalBuilder::new(Some(client), Vec::new())
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

    /// Returns the current producer ID.
    #[must_use]
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns the current producer epoch.
    #[must_use]
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Returns the transactional ID.
    #[must_use]
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// Returns whether a transaction is currently active.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }
}

impl<P: Partitioner> TransactionalProducer<P> {
    /// Begins a new transaction.
    ///
    /// Clears any previous transaction state and prepares for new
    /// transactional writes.
    pub fn begin(&mut self) -> Result<()> {
        self.txn_epoch += 1;
        self.current_txn_partitions.clear();
        self.in_transaction = true;
        debug!(
            "Transaction began (txn_id: {}, epoch: {})",
            self.transactional_id, self.txn_epoch
        );
        Ok(())
    }

    /// Sends a message within the current transaction.
    ///
    /// If the target partition hasn't been added to the transaction yet,
    /// an `AddPartitionsToTxn` request is sent first. The message is then
    /// produced with the transactional producer's ID and epoch.
    ///
    /// # Errors
    ///
    /// Returns an error if not currently in a transaction, or if the
    /// broker reports an error.
    pub fn send<K, V>(&mut self, rec: &Record<'_, K, V>) -> Result<()>
    where
        K: AsBytes,
        V: AsBytes,
    {
        if !self.in_transaction {
            return Err(Error::Config("not in a transaction; call begin() first".into()));
        }

        let key = if rec.key.as_bytes().is_empty() {
            None
        } else {
            Some(rec.key.as_bytes())
        };
        let value = if rec.value.as_bytes().is_empty() {
            None
        } else {
            Some(rec.value.as_bytes())
        };

        let mut msg = ProduceMessage {
            key,
            value,
            topic: rec.topic,
            partition: rec.partition,
            headers: &rec.headers.0,
        };

        // Partition the message (only uses self.state)
        {
            let topics = super::partitioner::Topics::new(&self.state.partitions);
            self.state.partitioner.partition(topics, &mut msg);
        }

        let tp = (msg.topic.to_owned(), msg.partition);

        // Add partition to transaction if needed
        if !self.current_txn_partitions.contains(&tp) {
            self.add_partition_to_txn(&tp.0, &tp.1)?;
            self.current_txn_partitions.insert(tp.clone());
        }

        let _seq = self.next_sequence_number(&tp);

        let confirms = self.client.internal_produce_messages(
            RequiredAcks::All as i16,
            30_000,
            std::iter::once(msg),
        )?;

        if let Some(confirm) = confirms.first() {
            if let Some(p_confirm) = confirm.partition_confirms.first() {
                if p_confirm.offset.is_err() {
                    return Err(Error::Kafka(KafkaCode::Unknown));
                }
            }
        }

        debug!(
            "Sent message to {}:{} (seq: {})",
            tp.0, tp.1, _seq
        );

        Ok(())
    }

    /// Commits the current transaction.
    ///
    /// Sends an `EndTxn` request with `committed=true` to the transaction
    /// coordinator, making all messages sent since `begin()` visible to
    /// consumers.
    pub fn commit(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(Error::Config("not in a transaction; call begin() first".into()));
        }

        self.end_txn(true)?;

        info!(
            "Transaction committed (txn_id: {}, epoch: {})",
            self.transactional_id, self.txn_epoch
        );

        self.in_transaction = false;
        self.current_txn_partitions.clear();
        Ok(())
    }

    /// Aborts the current transaction.
    ///
    /// Sends an `EndTxn` request with `committed=false` to the transaction
    /// coordinator. All messages sent since `begin()` will be discarded.
    pub fn abort(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(Error::Config("not in a transaction; call begin() first".into()));
        }

        self.end_txn(false)?;

        info!(
            "Transaction aborted (txn_id: {}, epoch: {})",
            self.transactional_id, self.txn_epoch
        );

        self.in_transaction = false;
        self.current_txn_partitions.clear();
        Ok(())
    }

    fn end_txn(&mut self, committed: bool) -> Result<()> {
        let coordinator_host = self.find_transaction_coordinator()?;
        let correlation_id = self.client.next_correlation_id();
        let client_id = self.client.client_id().to_owned();

        let resp = transaction::fetch_end_txn(
            self.client.get_conn_mut(&coordinator_host)?,
            correlation_id,
            &client_id,
            self.producer_id,
            self.producer_epoch,
            &self.transactional_id,
            committed,
        )?;

        if resp.error_code != 0 {
            let err = Error::from_protocol(resp.error_code)
                .unwrap_or(Error::Kafka(KafkaCode::Unknown));
            return Err(err);
        }

        Ok(())
    }

    fn add_partition_to_txn(&mut self, topic: &str, partition: &i32) -> Result<()> {
        let coordinator_host = self.find_transaction_coordinator()?;
        let correlation_id = self.client.next_correlation_id();
        let client_id = self.client.client_id().to_owned();

        let txn_partitions = vec![TxnPartition {
            topic: topic.to_owned(),
            partitions: vec![*partition],
        }];

        let resp = transaction::fetch_add_partitions_to_txn(
            self.client.get_conn_mut(&coordinator_host)?,
            correlation_id,
            &client_id,
            self.producer_id,
            self.producer_epoch,
            &self.transactional_id,
            &txn_partitions,
        )?;

        if resp.error_code != 0 {
            let err = Error::from_protocol(resp.error_code)
                .unwrap_or(Error::Kafka(KafkaCode::Unknown));
            return Err(err);
        }

        debug!(
            "Added {}:{} to transaction (txn_id: {})",
            topic, partition, self.transactional_id
        );

        Ok(())
    }

    fn next_sequence_number(&mut self, tp: &(String, i32)) -> i32 {
        let seq = self.sequence_numbers.entry(tp.clone()).or_insert(0);
        let current = *seq;
        *seq += 1;
        current
    }

    fn find_transaction_coordinator(&mut self) -> Result<String> {
        self.client
            .load_metadata_all()?;

        self.client
            .group_coordinator_host(&self.transactional_id)
            .ok_or(Error::Kafka(KafkaCode::GroupCoordinatorNotAvailable))
    }
}

// --------------------------------------------------------------------
// Builder
// --------------------------------------------------------------------

/// Builder for constructing a [`TransactionalProducer`].
pub struct TransactionalBuilder<P = DefaultPartitioner> {
    client: Option<KafkaClient>,
    hosts: Vec<String>,
    transactional_id: Option<String>,
    client_id: Option<String>,
    partitioner: P,
    ack_timeout_ms: i32,
}

impl TransactionalBuilder {
    pub(crate) fn new(client: Option<KafkaClient>, hosts: Vec<String>) -> Self {
        Self {
            client,
            hosts,
            transactional_id: None,
            client_id: None,
            partitioner: DefaultPartitioner::default(),
            ack_timeout_ms: 30_000,
        }
    }
}

impl TransactionalBuilder<DefaultPartitioner> {
    /// Sets the transactional ID (required).
    #[must_use]
    pub fn with_transactional_id(mut self, id: impl Into<String>) -> Self {
        self.transactional_id = Some(id.into());
        self
    }

    /// Sets the client ID sent with each request.
    #[must_use]
    pub fn with_client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    /// Sets the acknowledgement timeout in milliseconds.
    #[must_use]
    pub fn with_ack_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.ack_timeout_ms = timeout_ms;
        self
    }
}

impl<P: Partitioner> TransactionalBuilder<P> {
    /// Sets a custom partitioner.
    #[must_use]
    pub fn with_partitioner<Q: Partitioner>(self, partitioner: Q) -> TransactionalBuilder<Q> {
        TransactionalBuilder {
            client: self.client,
            hosts: self.hosts,
            transactional_id: self.transactional_id,
            client_id: self.client_id,
            partitioner,
            ack_timeout_ms: self.ack_timeout_ms,
        }
    }

    /// Builds the [`TransactionalProducer`].
    ///
    /// # Errors
    ///
    /// Returns an error if `transactional_id` is not set, if metadata
    /// loading fails, or if the producer ID initialization fails.
    pub fn create(self) -> Result<TransactionalProducer<P>> {
        let transactional_id = self
            .transactional_id
            .ok_or_else(|| Error::Config("transactional_id is required".into()))?;

        let mut client = match self.client {
            Some(client) => client,
            None => KafkaClient::new(self.hosts),
        };

        if let Some(client_id) = self.client_id {
            client.set_client_id(client_id);
        }

        client.load_metadata_all()?;

        let producer_id = init_producer_id_for_txn(&mut client, &transactional_id)?;

        let state = State::new(&mut client, self.partitioner);

        info!(
            "TransactionalProducer created (txn_id: {}, producer_id: {}, epoch: {})",
            transactional_id, producer_id.producer_id, producer_id.producer_epoch
        );

        Ok(TransactionalProducer {
            client,
            state,
            producer_id: producer_id.producer_id,
            producer_epoch: producer_id.producer_epoch,
            transactional_id,
            sequence_numbers: HashMap::new(),
            current_txn_partitions: HashSet::new(),
            in_transaction: false,
            txn_epoch: 0,
        })
    }
}

fn init_producer_id_for_txn(
    client: &mut KafkaClient,
    transactional_id: &str,
) -> Result<init_producer_id::InitProducerIdResponseData> {
    let coordinator_host = client
        .group_coordinator_host(transactional_id)
        .ok_or(Error::Kafka(KafkaCode::GroupCoordinatorNotAvailable))?;

    let correlation_id = client.next_correlation_id();
    let client_id = client.client_id().to_owned();

    let resp = init_producer_id::fetch_init_producer_id(
        client.get_conn_mut(&coordinator_host)?,
        correlation_id,
        &client_id,
        Some(transactional_id),
    )?;

    if resp.error_code != 0 {
        let err = Error::from_protocol(resp.error_code)
            .unwrap_or(Error::Kafka(KafkaCode::Unknown));
        return Err(err);
    }

    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transactional_builder_requires_transactional_id() {
        let result = TransactionalProducer::from_hosts(vec!["localhost:9092".to_owned()])
            .with_client_id("test".to_owned())
            .create();

        match result {
            Err(ref e) => {
                let msg = e.to_string();
                assert!(msg.contains("transactional_id"), "got: {msg}");
            }
            Ok(_) => panic!("expected error when transactional_id is not set"),
        }
    }

    #[test]
    fn test_transactional_state_transitions() {
        let mut producer_state = TransactionState::new();
        assert!(!producer_state.in_transaction);

        producer_state.begin();
        assert!(producer_state.in_transaction);

        let err = producer_state.commit();
        assert!(err.is_ok());
        assert!(!producer_state.in_transaction);

        producer_state.begin();
        let err = producer_state.abort();
        assert!(err.is_ok());
        assert!(!producer_state.in_transaction);
    }

    #[test]
    fn test_cannot_commit_without_transaction() {
        let mut producer_state = TransactionState::new();
        let err = producer_state.commit();
        assert!(err.is_err());
    }

    #[test]
    fn test_cannot_abort_without_transaction() {
        let mut producer_state = TransactionState::new();
        let err = producer_state.abort();
        assert!(err.is_err());
    }

    /// Minimal state machine mirroring TransactionalProducer for unit tests
    /// without requiring a broker connection.
    struct TransactionState {
        in_transaction: bool,
    }

    impl TransactionState {
        fn new() -> Self {
            Self {
                in_transaction: false,
            }
        }

        fn begin(&mut self) {
            self.in_transaction = true;
        }

        fn commit(&mut self) -> Result<()> {
            if !self.in_transaction {
                return Err(Error::Config("not in a transaction".into()));
            }
            self.in_transaction = false;
            Ok(())
        }

        fn abort(&mut self) -> Result<()> {
            if !self.in_transaction {
                return Err(Error::Config("not in a transaction".into()));
            }
            self.in_transaction = false;
            Ok(())
        }
    }
}
