//! Async consumer for fetching messages from Kafka.

use std::thread;

use rustfs_kafka::consumer::{Consumer, MessageSets};
use rustfs_kafka::error::{ConsumerError, Error, Result};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use crate::AsyncKafkaClient;

/// Command sent to the consumer's background thread.
enum ConsumerCommand {
    Poll {
        response: oneshot::Sender<Result<MessageSets>>,
    },
    Commit {
        response: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

/// An async Kafka consumer.
///
/// Wraps the synchronous [`Consumer`] in a dedicated background thread,
/// communicating via MPSC channels. This avoids `'static` lifetime
/// requirements of `spawn_blocking`.
///
/// # Example
///
/// ```no_run
/// use rustfs_kafka_async::AsyncConsumer;
///
/// #[tokio::main]
/// async fn main() -> rustfs_kafka::error::Result<()> {
///     let mut consumer = AsyncConsumer::from_hosts(
///         vec!["localhost:9092".to_owned()],
///         "my-group".to_owned(),
///         vec!["my-topic".to_owned()],
///     ).await?;
///
///     let messages = consumer.poll().await?;
///     for ms in messages.iter() {
///         for m in ms.messages() {
///             println!("key={:?} value={:?}", m.key, m.value);
///         }
///     }
///
///     consumer.close().await?;
///     Ok(())
/// }
/// ```
pub struct AsyncConsumer {
    sender: mpsc::Sender<ConsumerCommand>,
    handle: Option<thread::JoinHandle<()>>,
}

impl AsyncConsumer {
    /// Creates a new async consumer from bootstrap hosts.
    pub async fn from_hosts(
        hosts: Vec<String>,
        group: String,
        topics: Vec<String>,
    ) -> Result<Self> {
        let consumer = {
            let mut builder = Consumer::from_hosts(hosts).with_group(group);
            for topic in topics {
                builder = builder.with_topic(topic);
            }
            builder.create()?
        };

        let (sender, mut receiver) = mpsc::channel::<ConsumerCommand>(64);

        let handle = thread::spawn(move || {
            let mut consumer = consumer;
            loop {
                match receiver.blocking_recv() {
                    Some(ConsumerCommand::Poll { response }) => {
                        let result = consumer.poll();
                        let _ = response.send(result);
                    }
                    Some(ConsumerCommand::Commit { response }) => {
                        let result = consumer.commit_consumed();
                        let _ = response.send(result);
                    }
                    Some(ConsumerCommand::Shutdown) | None => {
                        debug!("AsyncConsumer thread shutting down");
                        break;
                    }
                }
            }
            info!("AsyncConsumer background thread exited");
        });

        info!("AsyncConsumer created");
        Ok(Self {
            sender,
            handle: Some(handle),
        })
    }

    /// Creates a new async consumer from an [`AsyncKafkaClient`].
    pub async fn from_client(
        client: AsyncKafkaClient,
        group: String,
        topics: Vec<String>,
    ) -> Result<Self> {
        Self::from_hosts(client.bootstrap_hosts().to_vec(), group, topics).await
    }

    /// Polls for new messages.
    ///
    /// Returns the fetched message sets.
    pub async fn poll(&mut self) -> Result<MessageSets> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ConsumerCommand::Poll { response: tx })
            .await
            .map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?;
        rx.await.map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?
    }

    /// Commits the current consumed offsets.
    pub async fn commit(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ConsumerCommand::Commit { response: tx })
            .await
            .map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?;
        rx.await.map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?
    }

    /// Gracefully closes the consumer.
    pub async fn close(mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            let _ = self.sender.send(ConsumerCommand::Shutdown).await;
            let _ = handle.join();
        }
        Ok(())
    }
}

impl Drop for AsyncConsumer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().ok();
        }
    }
}
