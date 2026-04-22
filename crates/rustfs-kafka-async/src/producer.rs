//! Async producer for sending messages to Kafka.

use bytes::Bytes;
use rustfs_kafka::client::RequiredAcks;
use rustfs_kafka::error::Result;
use rustfs_kafka::producer::{AsBytes, Producer, Record};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use crate::AsyncKafkaClient;

/// Internal commands sent to the async producer's background task.
///
/// Variants that expect a result carry a `oneshot::Sender` so the async
/// caller can await the outcome of the synchronous operation executed in the
/// background task.
enum ProducerCommand {
    Send {
        topic: String,
        key: Bytes,
        value: Bytes,
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
/// This wrapper runs a synchronous `Producer` inside a tokio background task
/// and exposes an async-friendly API. Calls to `send` queue the message on an
/// MPSC channel and then await a `oneshot` response indicating success or
/// failure of the underlying synchronous `Producer::send`.
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
    ///
    /// The function constructs a synchronous `Producer` configured with the
    /// given client's bootstrap hosts and moves it into a background tokio
    /// task. The returned `AsyncProducer` sends commands to that task.
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
                        let result = Self::do_send(&mut producer, &topic, &key, &value, partition);
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
    /// The message is queued to the background task which performs the
    /// blocking `Producer::send` call. This method awaits a confirmation via
    /// a `oneshot` channel and returns once the synchronous send completes or
    /// fails.
    pub async fn send<K, V>(&self, record: &Record<'_, K, V>) -> Result<()>
    where
        K: AsBytes,
        V: AsBytes,
    {
        let (tx, rx) = oneshot::channel();

        self.sender
            .send(ProducerCommand::Send {
                topic: record.topic.to_owned(),
                key: Bytes::copy_from_slice(record.key.as_bytes()),
                value: Bytes::copy_from_slice(record.value.as_bytes()),
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
    ///
    /// The synchronous producer flushes on each send in this implementation,
    /// so `flush` simply sends a no-op confirmation to the background task and
    /// awaits the acknowledgement.
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
    /// Sends a shutdown signal to the background task and awaits the task
    /// completion. After `close` returns, the background task has exited.
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
        key: &Bytes,
        value: &Bytes,
        partition: i32,
    ) -> Result<()> {
        let record =
            Record::from_key_value(topic, key.as_ref(), value.as_ref()).with_partition(partition);
        producer.send(&record)
    }
}

impl Drop for AsyncProducer {
    fn drop(&mut self) {
        // On drop we abort the background task to avoid leaking it. Consumers
        // who want a graceful shutdown should call `close().await`.
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
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
    async fn new_fails_with_unreachable_hosts() {
        // AsyncKafkaClient with empty hosts succeeds (no connections attempted),
        // but Producer::from_hosts will fail because it needs actual connections
        let client = AsyncKafkaClient::new(vec![]).await.unwrap();
        let result = AsyncProducer::new(client).await;
        assert!(result.is_err());
    }
}
