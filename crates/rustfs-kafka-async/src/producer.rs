//! Async producer for sending messages to Kafka.

use rustfs_kafka::client::RequiredAcks;
use rustfs_kafka::error::Result;
use rustfs_kafka::producer::{AsBytes, Producer, Record};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use crate::AsyncKafkaClient;

/// Command sent to the async producer's background task.
enum ProducerCommand {
    Send {
        topic: String,
        key: Vec<u8>,
        value: Vec<u8>,
        partition: i32,
        response: oneshot::Sender<Result<()>>,
    },
    Flush {
        response: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

/// An async Kafka producer.
///
/// Sends messages to Kafka brokers asynchronously using a background
/// task. Messages are queued via an MPSC channel and processed by a
/// dedicated tokio task that holds the synchronous producer.
///
/// # Example
///
/// ```no_run
/// use rustfs_kafka_async::{AsyncKafkaClient, AsyncProducer};
/// use rustfs_kafka::producer::Record;
///
/// #[tokio::main]
/// async fn main() -> rustfs_kafka::error::Result<()> {
///     let client = AsyncKafkaClient::new(vec!["localhost:9092".to_owned()]).await?;
///     let mut producer = AsyncProducer::new(client).await?;
///
///     producer.send(&Record::from_value("test-topic", &b"hello"[..])).await?;
///     producer.flush().await?;
///     producer.close().await?;
///     Ok(())
/// }
/// ```
pub struct AsyncProducer {
    sender: mpsc::Sender<ProducerCommand>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl AsyncProducer {
    /// Creates a new async producer from an [`AsyncKafkaClient`].
    pub async fn new(client: AsyncKafkaClient) -> Result<Self> {
        let hosts = client.bootstrap_hosts().to_vec();
        let client_id = client.client_id().to_owned();

        // Create the synchronous producer for the background task
        let sync_producer = Producer::from_hosts(hosts)
            .with_client_id(client_id)
            .with_required_acks(RequiredAcks::One)
            .create()?;

        let (sender, mut receiver) = mpsc::channel::<ProducerCommand>(256);

        let handle = tokio::spawn(async move {
            let mut producer = sync_producer;
            while let Some(cmd) = receiver.recv().await {
                match cmd {
                    ProducerCommand::Send {
                        topic,
                        key,
                        value,
                        partition,
                        response,
                    } => {
                        let result = Self::do_send(
                            &mut producer,
                            &topic,
                            &key,
                            &value,
                            partition,
                        );
                        let _ = response.send(result);
                    }
                    ProducerCommand::Flush { response } => {
                        // Synchronous producer flushes on each send,
                        // so this is a no-op confirmation
                        let _ = response.send(Ok(()));
                    }
                    ProducerCommand::Shutdown => {
                        debug!("Async producer shutting down");
                        break;
                    }
                }
            }
            info!("Async producer background task exited");
        });

        Ok(Self {
            sender,
            handle: Some(handle),
        })
    }

    /// Creates a new async producer directly from bootstrap hosts.
    pub async fn from_hosts(hosts: Vec<String>) -> Result<Self> {
        let client = AsyncKafkaClient::new(hosts).await?;
        Self::new(client).await
    }

    /// Sends a message to Kafka asynchronously.
    ///
    /// The message is queued and sent by the background task. This method
    /// returns once the message has been successfully sent to Kafka.
    pub async fn send<K, V>(&self, record: &Record<'_, K, V>) -> Result<()>
    where
        K: AsBytes,
        V: AsBytes,
    {
        let (tx, rx) = oneshot::channel();

        self.sender
            .send(ProducerCommand::Send {
                topic: record.topic.to_owned(),
                key: record.key.as_bytes().to_vec(),
                value: record.value.as_bytes().to_vec(),
                partition: record.partition,
                response: tx,
            })
            .await
            .map_err(|_| {
                rustfs_kafka::error::Error::Connection(
                    rustfs_kafka::error::ConnectionError::NoHostReachable,
                )
            })?;

        rx.await.map_err(|_| {
            rustfs_kafka::error::Error::Connection(
                rustfs_kafka::error::ConnectionError::NoHostReachable,
            )
        })?
    }

    /// Flushes any pending messages.
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProducerCommand::Flush { response: tx })
            .await
            .map_err(|_| {
                rustfs_kafka::error::Error::Connection(
                    rustfs_kafka::error::ConnectionError::NoHostReachable,
                )
            })?;
        rx.await.map_err(|_| {
            rustfs_kafka::error::Error::Connection(
                rustfs_kafka::error::ConnectionError::NoHostReachable,
            )
        })?
    }

    /// Gracefully shuts down the producer.
    ///
    /// Sends a shutdown signal to the background task and waits for it
    /// to complete.
    pub async fn close(mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            let _ = self.sender.send(ProducerCommand::Shutdown).await;
            let _ = handle.await;
        }
        Ok(())
    }

    fn do_send(
        producer: &mut Producer,
        topic: &str,
        key: &[u8],
        value: &[u8],
        partition: i32,
    ) -> Result<()> {
        let record = Record::from_key_value(topic, key, value).with_partition(partition);
        producer.send(&record)
    }
}

impl Drop for AsyncProducer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
