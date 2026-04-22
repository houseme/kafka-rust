//! Async producer for sending messages to Kafka.

use bytes::Bytes;
use rustfs_kafka::client::{RequiredAcks, SecurityConfig};
use rustfs_kafka::error::{ConnectionError, Error, Result};
use rustfs_kafka::producer::{AsBytes, Producer, Record};
use std::thread;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use crate::AsyncKafkaClient;

/// Internal commands sent to the async producer's background thread.
///
/// Variants that expect a result carry a `oneshot::Sender` so the async
/// caller can await the outcome of the synchronous operation executed in the
/// background thread.
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
/// This wrapper runs a synchronous `Producer` inside a dedicated background
/// thread and exposes an async-friendly API. Calls to `send` queue the message
/// on an MPSC channel and then await a `oneshot` response indicating success
/// or failure of the underlying synchronous `Producer::send`.
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
    handle: Option<thread::JoinHandle<()>>,
}

/// Configuration for constructing an [`AsyncProducer`].
pub struct AsyncProducerConfig {
    required_acks: RequiredAcks,
    ack_timeout: Duration,
    security: Option<SecurityConfig>,
}

impl AsyncProducerConfig {
    #[must_use]
    pub fn new() -> Self {
        Self {
            required_acks: RequiredAcks::One,
            ack_timeout: Duration::from_secs(30),
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
        }
    }

    /// Sets the client ID used by the underlying synchronous producer.
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

    /// Sets optional TLS/SASL security configuration.
    #[must_use]
    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.config = self.config.with_security(security);
        self
    }

    /// Sets command channel capacity used between async callers and the
    /// background producer thread.
    #[must_use]
    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity.max(1);
        self
    }

    /// Builds the async producer.
    ///
    /// The synchronous producer creation is moved to a blocking thread to
    /// avoid stalling the tokio scheduler during DNS/TCP/metadata operations.
    pub async fn build(self) -> Result<AsyncProducer> {
        let client_id = self.client_id;
        let hosts = self.hosts;
        let config = self.config;

        let channel_capacity = self.channel_capacity;
        let sync_producer = tokio::task::spawn_blocking(move || {
            let mut builder = Producer::from_hosts(hosts)
                .with_client_id(client_id)
                .with_required_acks(config.required_acks)
                .with_ack_timeout(config.ack_timeout);

            if let Some(security) = config.security {
                builder = builder.with_security(security);
            }

            builder.create()
        })
        .await
        .map_err(|e| Error::Config(format!("failed to build producer task: {e}")))??;

        Ok(AsyncProducer::from_sync(sync_producer, channel_capacity))
    }
}

impl AsyncProducer {
    /// Starts building a new async producer from bootstrap hosts.
    #[must_use]
    pub fn builder(hosts: Vec<String>) -> AsyncProducerBuilder {
        AsyncProducerBuilder::new(hosts)
    }

    /// Creates a new async producer from an [`AsyncKafkaClient`].
    ///
    /// This is equivalent to `AsyncProducer::builder(client.bootstrap_hosts().to_vec())`
    /// with the client's current `client_id`, then awaiting `build()`.
    pub async fn new(client: AsyncKafkaClient) -> Result<Self> {
        Self::new_with_config(client, AsyncProducerConfig::default()).await
    }

    /// Creates a new async producer with explicit configuration.
    pub async fn new_with_config(
        client: AsyncKafkaClient,
        config: AsyncProducerConfig,
    ) -> Result<Self> {
        Self::builder(client.bootstrap_hosts().to_vec())
            .with_client_id(client.client_id().to_owned())
            .with_required_acks(config.required_acks)
            .with_ack_timeout(config.ack_timeout)
            .build_with_optional_security(config.security)
            .await
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
            .build_with_optional_security(config.security)
            .await
    }

    /// Sends a message to Kafka asynchronously.
    ///
    /// The message is queued to the background thread which performs the
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
            .map_err(|_| no_host_reachable_error())?;

        rx.await.map_err(|_| no_host_reachable_error())?
    }

    /// Flushes any pending messages.
    ///
    /// The synchronous producer flushes on each send in this implementation,
    /// so `flush` simply sends a no-op confirmation to the background thread and
    /// awaits the acknowledgement.
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProducerCommand::Flush { response: tx })
            .await
            .map_err(|_| no_host_reachable_error())?;
        rx.await.map_err(|_| no_host_reachable_error())?
    }

    /// Gracefully shuts down the producer.
    ///
    /// Sends a shutdown signal to the background thread and awaits thread
    /// completion. After `close` returns, the background thread has exited.
    pub async fn close(mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            let _ = self.sender.send(ProducerCommand::Shutdown).await;
            let join_result = tokio::task::spawn_blocking(move || handle.join())
                .await
                .map_err(|e| Error::Config(format!("failed to join producer thread: {e}")))?;
            if join_result.is_err() {
                return Err(Error::Config("producer thread panicked".to_owned()));
            }
        }
        Ok(())
    }

    fn from_sync(sync_producer: Producer, channel_capacity: usize) -> Self {
        let (sender, mut receiver) = mpsc::channel::<ProducerCommand>(channel_capacity.max(1));

        let handle = thread::spawn(move || {
            let mut producer = sync_producer;
            while let Some(cmd) = receiver.blocking_recv() {
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
            info!("Async producer background thread exited");
        });

        Self {
            sender,
            handle: Some(handle),
        }
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
        if self.handle.is_some() {
            let _ = self.sender.try_send(ProducerCommand::Shutdown);
            // Dropping `JoinHandle` detaches the thread and avoids blocking in
            // `Drop` on a runtime worker thread.
            self.handle.take();
        }
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

fn no_host_reachable_error() -> Error {
    Error::Connection(ConnectionError::NoHostReachable)
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
