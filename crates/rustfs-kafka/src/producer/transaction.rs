//! Transactional producer support for exactly-once semantics.
//!
//! Provides [`TransactionalProducer`] for atomic writes across multiple
//! topic-partitions using Kafka's transaction protocol.

use std::collections::{HashMap, HashSet};

use tracing::{debug, info};

use crate::client::{KafkaClient, KafkaClientInternals, ProduceMessage};
use crate::error::{Error, KafkaCode, Result};
use crate::protocol::init_producer_id;
use crate::protocol::produce::TransactionContext;
use crate::protocol::transaction::{self, TxnPartition};

use super::{AsBytes, DefaultPartitioner, Partitioner, Record, State};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionStatus {
    Idle,
    Active,
    Poisoned,
}

/// A producer that supports Kafka transactions.
///
/// Transactions allow atomic writes across multiple topic-partitions,
/// with read-committed visibility after commit. Committing consumer offsets as
/// part of the same transaction is not provided by this producer.
///
/// A failed transaction RPC makes this instance unusable. Recreate the producer
/// to establish a new epoch; do not blindly retry a record or commit whose
/// delivery or transaction outcome may be unknown.
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
    status: TransactionStatus,
    coordinator_host: String,
    ack_timeout_ms: i32,
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

    /// Returns whether a begun transaction has not completed successfully.
    ///
    /// A poisoned producer also returns true: a failed RPC does not establish
    /// whether the broker committed, aborted, or is still holding the transaction.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.status != TransactionStatus::Idle
    }
}

impl<P: Partitioner> TransactionalProducer<P> {
    /// Begins a new transaction.
    ///
    /// Prepares for new transactional writes without making a broker request.
    /// An active or poisoned transaction cannot be replaced by another begin.
    ///
    /// # Errors
    ///
    /// Returns an error if transactional state preparation fails.
    pub fn begin(&mut self) -> Result<()> {
        match self.status {
            TransactionStatus::Idle => {}
            TransactionStatus::Active => {
                return Err(Error::Config("a transaction is already active".into()));
            }
            TransactionStatus::Poisoned => return Err(Self::poisoned_error()),
        }
        self.txn_epoch = self.txn_epoch.wrapping_add(1);
        self.current_txn_partitions.clear();
        self.status = TransactionStatus::Active;
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
        self.require_active()?;
        #[cfg(feature = "producer_timestamp")]
        crate::client::produce_ops::validate_producer_timestamp(self.client.producer_timestamp())?;

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

        if !self
            .state
            .partitions
            .get(msg.topic)
            .is_some_and(|partitions| partitions.available_ids().contains(&msg.partition))
        {
            return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
        }
        let tp = (msg.topic.to_owned(), msg.partition);

        if !self.current_txn_partitions.contains(&tp) {
            if let Err(err) = self.add_partition_to_txn(&tp.0, tp.1) {
                self.status = TransactionStatus::Poisoned;
                return Err(err);
            }
            self.current_txn_partitions.insert(tp.clone());
        }

        let seq = self.sequence_numbers.get(&tp).copied().unwrap_or(0);
        let result = self.client.internal_produce_transactional_message(
            self.ack_timeout_ms,
            TransactionContext {
                transactional_id: &self.transactional_id,
                producer_id: self.producer_id,
                producer_epoch: self.producer_epoch,
                sequence: seq,
            },
            &msg,
        );
        if let Err(err) = result {
            // The first failure is reported unchanged. Every subsequent operation
            // refuses reuse, including retries suggested by Error::is_retriable.
            self.status = TransactionStatus::Poisoned;
            return Err(err);
        }
        self.sequence_numbers
            .insert(tp.clone(), advance_sequence(seq));
        debug!("Sent message to {}:{} (seq: {})", tp.0, tp.1, seq);
        Ok(())
    }

    /// Commits the current transaction.
    ///
    /// Sends an `EndTxn` request with `committed=true` to the transaction
    /// coordinator, making all messages sent since `begin()` visible to
    /// consumers.
    ///
    /// # Errors
    ///
    /// Returns an error if no transaction is active or broker commit calls fail.
    pub fn commit(&mut self) -> Result<()> {
        self.finish_transaction(true)
    }

    /// Aborts the current transaction.
    ///
    /// Aborted records remain in the log and are hidden from read-committed
    /// consumers. The broker's accepted sequence numbers remain valid for the
    /// next transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if no transaction is active, this producer was poisoned
    /// by a failed RPC, or the broker abort call fails.
    pub fn abort(&mut self) -> Result<()> {
        self.finish_transaction(false)
    }

    fn finish_transaction(&mut self, committed: bool) -> Result<()> {
        self.require_active()?;
        // begin() is local; without AddPartitionsToTxn the coordinator has no
        // active transaction. Sending EndTxn then would be INVALID_TXN_STATE.
        if !self.current_txn_partitions.is_empty()
            && let Err(err) = self.end_txn(committed)
        {
            self.status = TransactionStatus::Poisoned;
            return Err(err);
        }
        self.status = TransactionStatus::Idle;
        self.current_txn_partitions.clear();
        info!(committed, txn_id = %self.transactional_id, epoch = self.txn_epoch, "Transaction ended");
        Ok(())
    }

    fn require_active(&self) -> Result<()> {
        match self.status {
            TransactionStatus::Active => Ok(()),
            TransactionStatus::Idle => Err(Error::Config(
                "not in a transaction; call begin() first".into(),
            )),
            TransactionStatus::Poisoned => Err(Self::poisoned_error()),
        }
    }

    fn poisoned_error() -> Error {
        Error::Config("transactional producer is unusable after a failed RPC; recreate it before further operations; delivery or transaction outcome may be unknown and must not be blindly retried".into())
    }

    fn end_txn(&mut self, committed: bool) -> Result<()> {
        let coordinator_host = &self.coordinator_host;
        let correlation_id = self.client.next_correlation_id();
        let client_id = self.client.client_id().to_owned();

        let resp = transaction::fetch_end_txn(
            self.client.get_conn_mut(coordinator_host)?,
            correlation_id,
            &client_id,
            self.producer_id,
            self.producer_epoch,
            &self.transactional_id,
            committed,
        )?;

        if resp.throttle_time_ms > 0 {
            debug!(
                "EndTxn throttled by coordinator: {} ms (txn_id: {})",
                resp.throttle_time_ms, self.transactional_id
            );
        }

        if resp.error_code != 0 {
            let err =
                Error::from_protocol(resp.error_code).unwrap_or(Error::Kafka(KafkaCode::Unknown));
            return Err(err);
        }

        Ok(())
    }

    fn add_partition_to_txn(&mut self, topic: &str, partition: i32) -> Result<()> {
        let txn_partitions = [TxnPartition {
            topic: topic.to_owned(),
            partitions: vec![partition],
        }];
        let mut attempt = 1;
        loop {
            let correlation_id = self.client.next_correlation_id();
            let client_id = self.client.client_id().to_owned();
            let resp = transaction::fetch_add_partitions_to_txn(
                self.client.get_conn_mut(&self.coordinator_host)?,
                correlation_id,
                &client_id,
                self.producer_id,
                self.producer_epoch,
                &self.transactional_id,
                &txn_partitions,
            )?;
            if resp.throttle_time_ms > 0 {
                debug!(
                    "AddPartitionsToTxn throttled by coordinator: {} ms (txn_id: {})",
                    resp.throttle_time_ms, self.transactional_id
                );
            }
            if resp.error_code != 0 {
                return Err(Error::from_protocol(resp.error_code)
                    .unwrap_or(Error::Kafka(KafkaCode::Unknown)));
            }
            // The v2 converter verified exactly our one requested partition.
            // CONCURRENT_TRANSACTIONS rejects enrollment while the previous
            // EndTxn markers finish. No Produce has been sent for this record,
            // so waiting on that explicit rejection cannot replay a delivery.
            let [result] = resp.results.as_slice() else {
                let _ = self.client.get_conn_mut(&self.coordinator_host)?.shutdown();
                return Err(Error::codec());
            };
            if result.error_code != 0 {
                debug!(
                    error_code = result.error_code,
                    topic,
                    partition,
                    producer_id = self.producer_id,
                    producer_epoch = self.producer_epoch,
                    attempt,
                    "AddPartitionsToTxn rejected the partition before Produce"
                );
                if result.error_code == 51
                    && let Some(delay) = self.client.transaction_retry_delay(attempt)
                {
                    std::thread::sleep(delay);
                    attempt += 1;
                    continue;
                }
                return Err(Error::TopicPartitionError {
                    topic_name: result.topic.clone(),
                    partition_id: partition,
                    error_code: KafkaCode::from_protocol(result.error_code)
                        .unwrap_or(KafkaCode::Unknown),
                });
            }
            debug!(
                "Added {}:{} to transaction (txn_id: {})",
                topic, partition, self.transactional_id
            );
            return Ok(());
        }
    }
}

fn advance_sequence(sequence: i32) -> i32 {
    if sequence == i32::MAX {
        0
    } else {
        sequence + 1
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

        if transactional_id.is_empty() {
            return Err(Error::Config("transactional_id must not be empty".into()));
        }
        if self.ack_timeout_ms <= 0 {
            return Err(Error::Config("ack_timeout_ms must be positive".into()));
        }

        let mut client = match self.client {
            Some(client) => client,
            None => KafkaClient::new(self.hosts),
        };
        #[cfg(feature = "producer_timestamp")]
        crate::client::produce_ops::validate_producer_timestamp(client.producer_timestamp())?;

        if let Some(client_id) = self.client_id {
            client.set_client_id(client_id);
        }

        client.load_metadata_all()?;

        let mut coordinator_host = client.find_transaction_coordinator(&transactional_id)?;
        let producer_id =
            init_producer_id_for_txn(&mut client, &transactional_id, &mut coordinator_host)?;

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
            status: TransactionStatus::Idle,
            coordinator_host,
            ack_timeout_ms: self.ack_timeout_ms,
            txn_epoch: 0,
        })
    }
}

fn init_producer_id_for_txn(
    client: &mut KafkaClient,
    transactional_id: &str,
    coordinator_host: &mut String,
) -> Result<init_producer_id::InitProducerIdResponseData> {
    let mut attempt = 1;
    loop {
        let correlation_id = client.next_correlation_id();
        let client_id = client.client_id().to_owned();
        let resp = init_producer_id::fetch_init_producer_id(
            client.get_conn_mut(coordinator_host.as_str())?,
            correlation_id,
            &client_id,
            Some(transactional_id),
        )?;
        // These numeric codes explicitly reject initialization before this
        // instance has an identity or any records: COORDINATOR_LOAD_IN_PROGRESS,
        // COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR, and CONCURRENT_TRANSACTIONS.
        // A replacement initializer may have requested abort of its predecessor and receive
        // CONCURRENT_TRANSACTIONS until those markers have completed.
        if matches!(resp.error_code, 14 | 15 | 16 | 51)
            && let Some(delay) = client.transaction_retry_delay(attempt)
        {
            debug!(
                error_code = resp.error_code,
                attempt, "Retrying rejected producer initialization"
            );
            std::thread::sleep(delay);
            attempt += 1;
            if resp.error_code == 16 {
                // The rejected InitProducerId carried no assigned identity.
                // Ownership may have changed since the original discovery.
                *coordinator_host = client.find_transaction_coordinator(transactional_id)?;
            }
            continue;
        }
        if resp.error_code != 0 {
            return Err(
                Error::from_protocol(resp.error_code).unwrap_or(Error::Kafka(KafkaCode::Unknown))
            );
        }
        if resp.producer_id < 0 || resp.producer_epoch < 0 {
            return Err(Error::codec());
        }
        return Ok(resp);
    }
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

    fn disconnected_producer() -> TransactionalProducer {
        let mut client = KafkaClient::new(Vec::new());
        let state = State::new(&mut client, DefaultPartitioner::default());
        TransactionalProducer {
            client,
            state,
            producer_id: 1,
            producer_epoch: 0,
            transactional_id: "test".into(),
            sequence_numbers: HashMap::new(),
            current_txn_partitions: HashSet::new(),
            status: TransactionStatus::Idle,
            coordinator_host: "unused:9092".into(),
            ack_timeout_ms: 30_000,
            txn_epoch: 0,
        }
    }

    #[test]
    fn real_state_machine_rejects_repeated_begin_and_finishes_empty_transactions() {
        let mut producer = disconnected_producer();
        assert!(producer.commit().is_err());
        assert!(producer.abort().is_err());
        producer.begin().unwrap();
        assert!(producer.begin().is_err());
        assert!(producer.in_transaction());
        producer.commit().unwrap();
        assert!(!producer.in_transaction());
        producer.begin().unwrap();
        producer.abort().unwrap();
        assert!(!producer.in_transaction());
    }

    #[test]
    fn poisoned_producer_rejects_all_transaction_operations() {
        let mut producer = disconnected_producer();
        producer.status = TransactionStatus::Poisoned;
        assert!(
            producer
                .begin()
                .unwrap_err()
                .to_string()
                .contains("recreate")
        );
        assert!(
            producer
                .send(&Record::from_value("topic", "value"))
                .is_err()
        );
        assert!(producer.commit().is_err());
        assert!(producer.abort().is_err());
    }

    #[test]
    fn sequence_wraps_and_successful_empty_transactions_preserve_partition_sequences() {
        assert_eq!(advance_sequence(i32::MAX), 0);
        assert_eq!(advance_sequence(0), 1);
        let mut producer = disconnected_producer();
        producer.sequence_numbers.insert(("topic".into(), 0), 7);
        for committed in [false, true] {
            producer.begin().unwrap();
            producer.finish_transaction(committed).unwrap();
            assert_eq!(producer.sequence_numbers[&("topic".into(), 0)], 7);
        }
    }

    #[test]
    fn transaction_configuration_is_validated_before_network_access() {
        for (transactional_id, timeout) in [("", 1), ("valid", 0), ("valid", -1)] {
            assert!(matches!(
                TransactionalProducer::from_hosts(Vec::new())
                    .with_transactional_id(transactional_id)
                    .with_ack_timeout_ms(timeout)
                    .create(),
                Err(Error::Config(_))
            ));
        }
    }

    #[cfg(feature = "producer_timestamp")]
    #[test]
    fn invalid_timestamp_mode_is_rejected_before_transaction_initialization() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let client = KafkaClient::builder()
            .with_hosts(vec![listener.local_addr().unwrap().to_string()])
            .with_producer_timestamp(Some(crate::client::ProducerTimestamp::LogAppendTime))
            .build();
        let result = TransactionalProducer::from_client(client)
            .with_transactional_id("txn-test")
            .create();
        assert!(matches!(result, Err(Error::Config(message))
            if message.contains("message.timestamp.type=LogAppendTime")));
        assert!(
            listener
                .accept()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
        );
    }

    #[cfg(feature = "producer_timestamp")]
    #[test]
    fn active_transaction_can_recover_after_rejecting_a_changed_timestamp_mode() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (check_tx, check_rx) = std::sync::mpsc::channel();
        let (checked_tx, checked_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            initialize_mock(&mut stream, address);
            check_rx.recv().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_millis(100)))
                .unwrap();
            let mut byte = [0];
            let error = stream.peek(&mut byte).unwrap_err();
            assert!(matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ));
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            checked_tx.send(()).unwrap();
            acknowledge_add(&mut stream, 0);
            let header = read_produce(&mut stream, 0);
            write_response(&mut stream, &header, &produce_response(0));
            acknowledge_end(&mut stream, true, 0);
        });
        let mut producer = mock_producer(address);
        producer.begin().unwrap();
        let sequences = producer.sequence_numbers.clone();
        let partitions = producer.current_txn_partitions.clone();
        let identity = (producer.producer_id(), producer.producer_epoch());
        producer
            .client_mut()
            .set_producer_timestamp(Some(crate::client::ProducerTimestamp::LogAppendTime));
        assert!(matches!(
            producer.send(&Record::from_value("topic-a", "value")),
            Err(Error::Config(_))
        ));
        assert_eq!(producer.status, TransactionStatus::Active);
        assert_eq!(producer.sequence_numbers, sequences);
        assert_eq!(producer.current_txn_partitions, partitions);
        assert_eq!(
            (producer.producer_id(), producer.producer_epoch()),
            identity
        );
        check_tx.send(()).unwrap();
        checked_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        producer.client_mut().set_producer_timestamp(None);
        producer
            .send(&Record::from_value("topic-a", "value"))
            .unwrap();
        assert_eq!(producer.sequence_numbers[&("topic-a".into(), 0)], 1);
        producer.commit().unwrap();
        assert_eq!(producer.status, TransactionStatus::Idle);
        server.join().unwrap();
    }

    use bytes::{Buf, Bytes, BytesMut};
    use kafka_protocol::messages::add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    };
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    use kafka_protocol::messages::{
        AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, ApiVersionsRequest,
        ApiVersionsResponse, EndTxnRequest, EndTxnResponse, FindCoordinatorRequest,
        FindCoordinatorResponse, InitProducerIdRequest, InitProducerIdResponse, MetadataRequest,
        MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
    use kafka_protocol::records::RecordBatchDecoder;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::time::Duration;

    fn read_request(stream: &mut TcpStream, expected: ApiKey) -> (RequestHeader, Bytes) {
        let mut size = [0; 4];
        stream.read_exact(&mut size).unwrap();
        let mut payload = vec![0; usize::try_from(i32::from_be_bytes(size)).unwrap()];
        stream.read_exact(&mut payload).unwrap();
        let version = i16::from_be_bytes(payload[2..4].try_into().unwrap());
        let mut payload = Bytes::from(payload);
        let header =
            RequestHeader::decode(&mut payload, expected.request_header_version(version)).unwrap();
        assert_eq!(header.request_api_key, expected as i16);
        (header, payload)
    }

    fn write_response<R: Encodable + HeaderVersion>(
        stream: &mut TcpStream,
        header: &RequestHeader,
        response: &R,
    ) {
        let version = header.request_api_version;
        let mut payload = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(header.correlation_id)
            .encode(&mut payload, R::header_version(version))
            .unwrap();
        response.encode(&mut payload, version).unwrap();
        stream
            .write_all(&i32::try_from(payload.len()).unwrap().to_be_bytes())
            .unwrap();
        stream.write_all(&payload).unwrap();
    }

    fn initialize_metadata_mock(stream: &mut TcpStream, address: SocketAddr) {
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let (header, mut body) = read_request(stream, ApiKey::ApiVersions);
        ApiVersionsRequest::decode(&mut body, header.request_api_version).unwrap();
        write_response(stream, &header, &ApiVersionsResponse::default());
        let (header, mut body) = read_request(stream, ApiKey::Metadata);
        MetadataRequest::decode(&mut body, header.request_api_version).unwrap();
        let response = MetadataResponse::default()
            .with_brokers(vec![
                MetadataResponseBroker::default()
                    .with_node_id(1.into())
                    .with_host(StrBytes::from_static_str("127.0.0.1"))
                    .with_port(i32::from(address.port())),
            ])
            .with_topics(vec![
                MetadataResponseTopic::default()
                    .with_name(Some(StrBytes::from_static_str("topic-a").into()))
                    .with_partitions(vec![
                        MetadataResponsePartition::default()
                            .with_partition_index(0)
                            .with_leader_id(1.into()),
                    ]),
            ]);
        write_response(stream, &header, &response);
    }

    fn initialize_mock(stream: &mut TcpStream, address: SocketAddr) {
        initialize_metadata_mock(stream, address);
        let (header, mut body) = read_request(stream, ApiKey::FindCoordinator);
        let request =
            FindCoordinatorRequest::decode(&mut body, header.request_api_version).unwrap();
        assert_eq!(request.key.as_str(), "txn-test");
        assert_eq!(request.key_type, 1);
        assert!(!body.has_remaining());
        write_response(
            stream,
            &header,
            &FindCoordinatorResponse::default()
                .with_node_id(1.into())
                .with_host(StrBytes::from_static_str("127.0.0.1"))
                .with_port(i32::from(address.port())),
        );
        let (header, mut body) = read_request(stream, ApiKey::InitProducerId);
        let request = InitProducerIdRequest::decode(&mut body, header.request_api_version).unwrap();
        assert_eq!(request.transactional_id.unwrap().as_str(), "txn-test");
        assert!(request.transaction_timeout_ms > 0);
        assert!(!body.has_remaining());
        write_response(
            stream,
            &header,
            &InitProducerIdResponse::default()
                .with_producer_id(12345.into())
                .with_producer_epoch(7),
        );
    }

    fn acknowledge_add(stream: &mut TcpStream, error: i16) {
        let (header, mut body) = read_request(stream, ApiKey::AddPartitionsToTxn);
        let request =
            AddPartitionsToTxnRequest::decode(&mut body, header.request_api_version).unwrap();
        assert_eq!(request.v3_and_below_transactional_id.as_str(), "txn-test");
        assert_eq!(i64::from(request.v3_and_below_producer_id), 12345);
        assert_eq!(request.v3_and_below_producer_epoch, 7);
        assert_eq!(request.v3_and_below_topics[0].partitions, [0]);
        assert!(!body.has_remaining());
        let response =
            AddPartitionsToTxnResponse::default().with_results_by_topic_v3_and_below(vec![
                AddPartitionsToTxnTopicResult::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_results_by_partition(vec![
                        AddPartitionsToTxnPartitionResult::default()
                            .with_partition_index(0)
                            .with_partition_error_code(error),
                    ]),
            ]);
        write_response(stream, &header, &response);
    }

    fn read_produce(stream: &mut TcpStream, sequence: i32) -> RequestHeader {
        let (header, mut body) = read_request(stream, ApiKey::Produce);
        let request = ProduceRequest::decode(&mut body, header.request_api_version).unwrap();
        assert_eq!(request.transactional_id.unwrap().as_str(), "txn-test");
        assert_eq!(request.acks, -1);
        assert_eq!(request.timeout_ms, 1_234);
        assert_eq!(request.topic_data.len(), 1);
        assert_eq!(request.topic_data[0].name.as_str(), "topic-a");
        assert_eq!(request.topic_data[0].partition_data.len(), 1);
        assert_eq!(request.topic_data[0].partition_data[0].index, 0);
        let mut records = request.topic_data[0].partition_data[0]
            .records
            .clone()
            .unwrap();
        let batch = RecordBatchDecoder::decode(&mut records).unwrap();
        assert_eq!(batch.records.len(), 1);
        let record = &batch.records[0];
        assert!(record.transactional);
        assert_eq!(record.producer_id, 12345);
        assert_eq!(record.producer_epoch, 7);
        assert_eq!(record.sequence, sequence);
        assert_eq!(record.offset, 0);
        assert_eq!(record.value.as_deref(), Some(b"value".as_slice()));
        assert!(records.is_empty());
        assert!(body.is_empty());
        header
    }

    fn produce_response(error: i16) -> ProduceResponse {
        ProduceResponse::default().with_responses(vec![
            TopicProduceResponse::default()
                .with_name(StrBytes::from_static_str("topic-a").into())
                .with_partition_responses(vec![
                    PartitionProduceResponse::default()
                        .with_index(0)
                        .with_base_offset(0)
                        .with_error_code(error),
                ]),
        ])
    }

    fn acknowledge_end(stream: &mut TcpStream, committed: bool, error: i16) {
        let (header, mut body) = read_request(stream, ApiKey::EndTxn);
        let request = EndTxnRequest::decode(&mut body, header.request_api_version).unwrap();
        assert_eq!(request.transactional_id.as_str(), "txn-test");
        assert_eq!(i64::from(request.producer_id), 12345);
        assert_eq!(request.producer_epoch, 7);
        assert_eq!(request.committed, committed);
        assert!(body.is_empty());
        write_response(
            stream,
            &header,
            &EndTxnResponse::default().with_error_code(error),
        );
    }

    fn mock_producer(address: SocketAddr) -> TransactionalProducer {
        let client = KafkaClient::builder()
            .with_hosts(vec![address.to_string()])
            .with_conn_rw_timeout(2)
            .build();
        let producer = TransactionalProducer::from_client(client)
            .with_transactional_id("txn-test")
            .with_ack_timeout_ms(1_234)
            .create()
            .unwrap();
        assert!(
            producer
                .client()
                .group_coordinator_host("txn-test")
                .is_none()
        );
        producer
    }

    #[test]
    fn wire_transactions_carry_context_and_keep_sequences_across_commit_and_abort() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            initialize_mock(&mut stream, address);
            let mut sequence = 0;
            for (committed, records) in [(true, 2), (false, 1), (true, 1)] {
                acknowledge_add(&mut stream, 0);
                for _ in 0..records {
                    let header = read_produce(&mut stream, sequence);
                    write_response(&mut stream, &header, &produce_response(0));
                    sequence += 1;
                }
                acknowledge_end(&mut stream, committed, 0);
            }
        });
        let mut producer = mock_producer(address);
        let record = Record::from_value("topic-a", "value").with_partition(0);
        for (committed, records) in [(true, 2), (false, 1), (true, 1)] {
            producer.begin().unwrap();
            for _ in 0..records {
                producer.send(&record).unwrap();
            }
            producer.finish_transaction(committed).unwrap();
        }
        assert_eq!(producer.sequence_numbers[&("topic-a".into(), 0)], 4);
        server.join().unwrap();
    }

    #[test]
    fn failed_transaction_rpcs_poison_and_never_retry_or_advance_unconfirmed_sequences() {
        for failure in [
            "add-error",
            "produce-error",
            "missing-ack",
            "wrong-correlation",
            "disconnect",
            "end-error",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                initialize_mock(&mut stream, address);
                acknowledge_add(&mut stream, if failure == "add-error" { 29 } else { 0 });
                if failure != "add-error" {
                    let mut header = read_produce(&mut stream, 0);
                    if failure == "disconnect" {
                        return;
                    }
                    if failure == "wrong-correlation" {
                        header.correlation_id += 1;
                    }
                    let response = if failure == "missing-ack" {
                        ProduceResponse::default()
                    } else {
                        produce_response(if failure == "produce-error" { 7 } else { 0 })
                    };
                    write_response(&mut stream, &header, &response);
                    if failure == "end-error" {
                        acknowledge_end(&mut stream, true, 16);
                    }
                }
                stream
                    .set_read_timeout(Some(Duration::from_millis(250)))
                    .unwrap();
                let mut byte = [0];
                match stream.read(&mut byte) {
                    Err(err)
                        if matches!(
                            err.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) => {}
                    Ok(0) => {}
                    other => panic!("poisoned producer sent another request: {other:?}"),
                }
            });
            let mut producer = mock_producer(address);
            producer.begin().unwrap();
            let record = Record::from_value("topic-a", "value").with_partition(0);
            if failure == "end-error" {
                producer.send(&record).unwrap();
                assert!(producer.commit().is_err());
                assert_eq!(producer.sequence_numbers[&("topic-a".into(), 0)], 1);
            } else {
                assert!(producer.send(&record).is_err());
                assert!(producer.sequence_numbers.is_empty());
            }
            assert!(producer.in_transaction());
            assert!(producer.begin().is_err());
            assert!(producer.send(&record).is_err());
            assert!(producer.commit().is_err());
            assert!(producer.abort().is_err());
            server.join().unwrap();
        }
    }

    fn respond_to_coordinator(stream: &mut TcpStream, address: SocketAddr, error: i16) {
        let (header, mut body) = read_request(stream, ApiKey::FindCoordinator);
        let request =
            FindCoordinatorRequest::decode(&mut body, header.request_api_version).unwrap();
        assert_eq!(request.key.as_str(), "txn-test");
        assert_eq!(request.key_type, 1);
        assert!(body.is_empty());
        write_response(
            stream,
            &header,
            &FindCoordinatorResponse::default()
                .with_node_id(1.into())
                .with_host(StrBytes::from_static_str("127.0.0.1"))
                .with_port(i32::from(address.port()))
                .with_error_code(error),
        );
    }

    fn respond_to_init(stream: &mut TcpStream) {
        let (header, mut body) = read_request(stream, ApiKey::InitProducerId);
        let request = InitProducerIdRequest::decode(&mut body, header.request_api_version).unwrap();
        assert_eq!(request.transactional_id.unwrap().as_str(), "txn-test");
        assert!(request.transaction_timeout_ms > 0);
        write_response(
            stream,
            &header,
            &InitProducerIdResponse::default()
                .with_producer_id(12345.into())
                .with_producer_epoch(7),
        );
    }

    #[test]
    fn transaction_coordinator_discovery_retries_lazy_startup_and_read_only_io() {
        for reconnect in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                initialize_metadata_mock(&mut stream, address);
                if reconnect {
                    let (_, mut body) = read_request(&mut stream, ApiKey::FindCoordinator);
                    let request = FindCoordinatorRequest::decode(&mut body, 3).unwrap();
                    assert_eq!(request.key_type, 1);
                    // The read-only coordinator lookup loses its response. The
                    // next attempt must use a newly accepted connection.
                    stream.shutdown(std::net::Shutdown::Both).unwrap();
                    drop(stream);
                    stream = listener.accept().unwrap().0;
                    stream
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                } else {
                    for error in [15, 14, 16] {
                        respond_to_coordinator(&mut stream, address, error);
                    }
                }
                respond_to_coordinator(&mut stream, address, 0);
                respond_to_init(&mut stream);
            });
            let client = KafkaClient::builder()
                .with_hosts(vec![address.to_string()])
                .with_conn_rw_timeout(2)
                .with_retry_policy(crate::client::RetryPolicy::Fixed {
                    interval: Duration::ZERO,
                    max_attempts: 4,
                })
                .build();
            let producer = TransactionalProducer::from_client(client)
                .with_transactional_id("txn-test")
                .create()
                .unwrap();
            assert_eq!(producer.producer_id(), 12345);
            assert!(
                producer
                    .client()
                    .group_coordinator_host("txn-test")
                    .is_none()
            );
            server.join().unwrap();
        }
    }

    #[test]
    fn malformed_transaction_acks_retire_the_connection_even_for_client_mut() {
        for malformed_add in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                initialize_mock(&mut stream, address);
                if malformed_add {
                    let (header, _) = read_request(&mut stream, ApiKey::AddPartitionsToTxn);
                    write_response(&mut stream, &header, &AddPartitionsToTxnResponse::default());
                } else {
                    acknowledge_add(&mut stream, 0);
                    let header = read_produce(&mut stream, 0);
                    write_response(&mut stream, &header, &ProduceResponse::default());
                }
                let mut byte = [0];
                assert_eq!(
                    stream.read(&mut byte).unwrap(),
                    0,
                    "malformed ACK connection must be closed"
                );
                let (mut clean, _) = listener.accept().unwrap();
                clean
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                clean.read_exact(&mut byte).unwrap();
                assert_eq!(byte, [42]);
            });
            let mut producer = mock_producer(address);
            producer.begin().unwrap();
            assert!(
                producer
                    .send(&Record::from_value("topic-a", "value").with_partition(0))
                    .is_err()
            );
            assert_eq!(producer.status, TransactionStatus::Poisoned);
            // A caller can retain and use this public client escape hatch. It
            // must get a fresh connection, not the producer's bad stream.
            producer
                .client_mut()
                .get_conn_mut(&address.to_string())
                .unwrap()
                .send(&[42])
                .unwrap();
            server.join().unwrap();
        }
    }

    #[test]
    fn initialization_retries_only_explicit_rejections_before_producer_construction() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            initialize_metadata_mock(&mut stream, address);
            respond_to_coordinator(&mut stream, address, 0);
            for error in [14, 15, 51] {
                let (header, mut body) = read_request(&mut stream, ApiKey::InitProducerId);
                let request =
                    InitProducerIdRequest::decode(&mut body, header.request_api_version).unwrap();
                assert_eq!(request.transactional_id.unwrap().as_str(), "txn-test");
                assert!(body.is_empty());
                write_response(
                    &mut stream,
                    &header,
                    &InitProducerIdResponse::default()
                        .with_producer_id((-1).into())
                        .with_producer_epoch(-1)
                        .with_error_code(error),
                );
            }
            respond_to_init(&mut stream);
        });
        let client = KafkaClient::builder()
            .with_hosts(vec![address.to_string()])
            .with_conn_rw_timeout(2)
            .with_retry_policy(crate::client::RetryPolicy::Fixed {
                interval: Duration::ZERO,
                max_attempts: 4,
            })
            .build();
        let producer = TransactionalProducer::from_client(client)
            .with_transactional_id("txn-test")
            .create()
            .unwrap();
        assert_eq!(producer.producer_id(), 12345);
        assert!(producer.sequence_numbers.is_empty());
        assert!(!producer.in_transaction());
        server.join().unwrap();
    }

    #[test]
    fn initialization_never_retries_io_codec_or_other_rejections() {
        for failure in ["io", "codec", "other-error"] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                initialize_metadata_mock(&mut stream, address);
                respond_to_coordinator(&mut stream, address, 0);
                let (mut header, _) = read_request(&mut stream, ApiKey::InitProducerId);
                if failure == "io" {
                    stream.shutdown(std::net::Shutdown::Both).unwrap();
                } else {
                    if failure == "codec" {
                        header.correlation_id += 1;
                    }
                    write_response(
                        &mut stream,
                        &header,
                        &InitProducerIdResponse::default()
                            .with_producer_id(12345.into())
                            .with_producer_epoch(7)
                            .with_error_code(if failure == "other-error" { 46 } else { 0 }),
                    );
                    let mut byte = [0];
                    assert_eq!(
                        stream.read(&mut byte).unwrap(),
                        0,
                        "initializer retried a non-retriable outcome"
                    );
                }
                listener
            });
            let client = KafkaClient::builder()
                .with_hosts(vec![address.to_string()])
                .with_conn_rw_timeout(2)
                .with_retry_policy(crate::client::RetryPolicy::Fixed {
                    interval: Duration::ZERO,
                    max_attempts: 4,
                })
                .build();
            let result = TransactionalProducer::from_client(client)
                .with_transactional_id("txn-test")
                .create();
            assert!(result.is_err());
            let listener = server.join().unwrap();
            listener.set_nonblocking(true).unwrap();
            assert!(
                matches!(listener.accept(), Err(err) if err.kind() == std::io::ErrorKind::WouldBlock),
                "initializer reopened a connection to retry an uncertain outcome"
            );
        }
    }

    #[test]
    fn initialization_rediscovers_rejected_coordinator_and_caches_its_new_host() {
        let old_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let old_address = old_listener.local_addr().unwrap();
        let new_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let new_address = new_listener.local_addr().unwrap();
        let old_server = std::thread::spawn(move || {
            let (mut stream, _) = old_listener.accept().unwrap();
            initialize_metadata_mock(&mut stream, old_address);
            respond_to_coordinator(&mut stream, old_address, 0);
            let (header, mut body) = read_request(&mut stream, ApiKey::InitProducerId);
            InitProducerIdRequest::decode(&mut body, header.request_api_version).unwrap();
            write_response(
                &mut stream,
                &header,
                &InitProducerIdResponse::default()
                    .with_producer_id((-1).into())
                    .with_producer_epoch(-1)
                    .with_error_code(16),
            );
            // Discovery remains read-only, but must identify the transaction
            // role and return the migrated coordinator before another Init.
            respond_to_coordinator(&mut stream, new_address, 0);
        });
        let new_server = std::thread::spawn(move || {
            let (mut stream, _) = new_listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            respond_to_init(&mut stream);
        });
        let client = KafkaClient::builder()
            .with_hosts(vec![old_address.to_string()])
            .with_conn_rw_timeout(2)
            .with_retry_policy(crate::client::RetryPolicy::Fixed {
                interval: Duration::ZERO,
                max_attempts: 2,
            })
            .build();
        let producer = TransactionalProducer::from_client(client)
            .with_transactional_id("txn-test")
            .create()
            .unwrap();
        assert_eq!(producer.producer_id(), 12345);
        assert_eq!(producer.coordinator_host, new_address.to_string());
        assert!(
            producer
                .client()
                .group_coordinator_host("txn-test")
                .is_none()
        );
        old_server.join().unwrap();
        new_server.join().unwrap();
    }

    #[test]
    fn exhausted_initialization_budget_does_not_rediscover_or_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            initialize_metadata_mock(&mut stream, address);
            respond_to_coordinator(&mut stream, address, 0);
            let (header, _) = read_request(&mut stream, ApiKey::InitProducerId);
            write_response(
                &mut stream,
                &header,
                &InitProducerIdResponse::default().with_error_code(16),
            );
            let mut byte = [0];
            assert_eq!(
                stream.read(&mut byte).unwrap(),
                0,
                "exhausted initializer issued another request"
            );
        });
        let client = KafkaClient::builder()
            .with_hosts(vec![address.to_string()])
            .with_conn_rw_timeout(2)
            .with_retry_policy(crate::client::RetryPolicy::Fixed {
                interval: Duration::ZERO,
                max_attempts: 1,
            })
            .build();
        let result = TransactionalProducer::from_client(client)
            .with_transactional_id("txn-test")
            .create();
        assert!(matches!(
            result,
            Err(Error::Kafka(KafkaCode::NotCoordinatorForGroup))
        ));
        server.join().unwrap();
    }

    #[test]
    fn rejected_partition_enrollment_waits_without_replaying_a_record() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            initialize_mock(&mut stream, address);
            acknowledge_add(&mut stream, 0);
            let header = read_produce(&mut stream, 0);
            write_response(&mut stream, &header, &produce_response(0));
            acknowledge_end(&mut stream, false, 0);
            acknowledge_add(&mut stream, 51);
            // No record is sent between the rejection and the next Add request.
            acknowledge_add(&mut stream, 0);
            let header = read_produce(&mut stream, 1);
            write_response(&mut stream, &header, &produce_response(0));
            acknowledge_end(&mut stream, true, 0);
        });
        let client = KafkaClient::builder()
            .with_hosts(vec![address.to_string()])
            .with_conn_rw_timeout(2)
            .with_retry_policy(crate::client::RetryPolicy::Fixed {
                interval: Duration::ZERO,
                max_attempts: 2,
            })
            .build();
        let mut producer = TransactionalProducer::from_client(client)
            .with_transactional_id("txn-test")
            .with_ack_timeout_ms(1_234)
            .create()
            .unwrap();
        let record = Record::from_value("topic-a", "value").with_partition(0);
        producer.begin().unwrap();
        producer.send(&record).unwrap();
        producer.abort().unwrap();
        producer.begin().unwrap();
        producer.send(&record).unwrap();
        producer.commit().unwrap();
        assert_eq!(producer.sequence_numbers[&("topic-a".into(), 0)], 2);
        server.join().unwrap();
    }

    #[test]
    fn enrollment_rejection_budget_exhaustion_or_epoch_errors_poison_without_produce() {
        for error in [51, 45, 47] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                initialize_mock(&mut stream, address);
                acknowledge_add(&mut stream, error);
                let mut byte = [0];
                assert_eq!(
                    stream.read(&mut byte).unwrap(),
                    0,
                    "failed enrollment sent another RPC"
                );
            });
            let client = KafkaClient::builder()
                .with_hosts(vec![address.to_string()])
                .with_conn_rw_timeout(2)
                .with_retry_policy(crate::client::RetryPolicy::Fixed {
                    interval: Duration::ZERO,
                    max_attempts: if error == 51 { 1 } else { 3 },
                })
                .build();
            let mut producer = TransactionalProducer::from_client(client)
                .with_transactional_id("txn-test")
                .with_ack_timeout_ms(1_234)
                .create()
                .unwrap();
            producer.begin().unwrap();
            assert!(
                producer
                    .send(&Record::from_value("topic-a", "value").with_partition(0))
                    .is_err()
            );
            assert_eq!(producer.status, TransactionStatus::Poisoned);
            assert!(producer.sequence_numbers.is_empty());
            assert!(producer.begin().is_err());
            assert!(producer.commit().is_err());
            assert!(producer.abort().is_err());
            drop(producer);
            server.join().unwrap();
        }
    }

    #[test]
    fn enrollment_io_and_malformed_ack_are_never_retried() {
        for failure in ["io", "missing-ack", "duplicate-ack"] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                initialize_mock(&mut stream, address);
                let (header, _) = read_request(&mut stream, ApiKey::AddPartitionsToTxn);
                if failure == "io" {
                    stream.shutdown(std::net::Shutdown::Both).unwrap();
                } else {
                    let response = if failure == "missing-ack" {
                        AddPartitionsToTxnResponse::default()
                    } else {
                        AddPartitionsToTxnResponse::default().with_results_by_topic_v3_and_below(
                            vec![
                                AddPartitionsToTxnTopicResult::default()
                                    .with_name(StrBytes::from_static_str("topic-a").into())
                                    .with_results_by_partition(vec![
                                            AddPartitionsToTxnPartitionResult::default()
                                                .with_partition_index(0);
                                            2
                                        ]),
                            ],
                        )
                    };
                    write_response(&mut stream, &header, &response);
                    let mut byte = [0];
                    assert_eq!(
                        stream.read(&mut byte).unwrap(),
                        0,
                        "malformed enrollment ACK was not retired before reuse"
                    );
                }
                listener
            });
            let client = KafkaClient::builder()
                .with_hosts(vec![address.to_string()])
                .with_conn_rw_timeout(2)
                .with_retry_policy(crate::client::RetryPolicy::Fixed {
                    interval: Duration::ZERO,
                    max_attempts: 3,
                })
                .build();
            let mut producer = TransactionalProducer::from_client(client)
                .with_transactional_id("txn-test")
                .with_ack_timeout_ms(1_234)
                .create()
                .unwrap();
            producer.begin().unwrap();
            let record = Record::from_value("topic-a", "value").with_partition(0);
            assert!(producer.send(&record).is_err());
            assert_eq!(producer.status, TransactionStatus::Poisoned);
            assert!(producer.sequence_numbers.is_empty());
            assert!(producer.current_txn_partitions.is_empty());
            assert!(producer.send(&record).is_err());
            assert!(producer.begin().is_err());
            assert!(producer.commit().is_err());
            assert!(producer.abort().is_err());
            // Keep the poisoned producer alive while requiring EOF above, then
            // prove no replacement connection was opened to retry enrollment.
            let listener = server.join().unwrap();
            listener.set_nonblocking(true).unwrap();
            assert!(
                matches!(listener.accept(), Err(err) if err.kind() == std::io::ErrorKind::WouldBlock)
            );
        }
    }
}
