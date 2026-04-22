//! Async consumer for fetching messages from Kafka.

use std::thread;

use rustfs_kafka::consumer::{Consumer, MessageSets};
use rustfs_kafka::error::{ConsumerError, Error, Result};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use crate::AsyncKafkaClient;

/// Internal commands sent to the consumer's background thread.
///
/// Each command that expects a response carries a `oneshot::Sender` so the
/// async caller can await the operation's result. This enum is part of the
/// internal implementation and not exported outside the crate.
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
/// This wrapper runs the synchronous `rustfs_kafka::consumer::Consumer` inside
/// a dedicated background thread and communicates with it using an MPSC
/// channel. Async callers can `poll` and `commit` via async functions that use
/// `oneshot` channels to receive results from the background thread. Using a
/// dedicated thread avoids having to make the synchronous `Consumer` `'static`
/// for `spawn_blocking`.
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

/// Builder for constructing an [`AsyncConsumer`] asynchronously.
pub struct AsyncConsumerBuilder {
    hosts: Vec<String>,
    group: Option<String>,
    topics: Vec<String>,
    channel_capacity: usize,
}

impl AsyncConsumerBuilder {
    /// Creates a new async consumer builder from bootstrap hosts.
    #[must_use]
    pub fn new(hosts: Vec<String>) -> Self {
        Self {
            hosts,
            group: None,
            topics: Vec::new(),
            channel_capacity: 64,
        }
    }

    /// Sets the consumer group.
    #[must_use]
    pub fn with_group(mut self, group: String) -> Self {
        self.group = Some(group);
        self
    }

    /// Adds a topic subscription.
    #[must_use]
    pub fn with_topic(mut self, topic: String) -> Self {
        self.topics.push(topic);
        self
    }

    /// Adds multiple topic subscriptions.
    #[must_use]
    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics.extend(topics);
        self
    }

    /// Sets command channel capacity used between async callers and the
    /// background consumer thread.
    #[must_use]
    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity.max(1);
        self
    }

    /// Builds an async consumer.
    ///
    /// The synchronous consumer construction runs on a blocking thread to
    /// avoid stalling the tokio scheduler.
    pub async fn build(self) -> Result<AsyncConsumer> {
        let group = self
            .group
            .ok_or(Error::Consumer(ConsumerError::UnsetGroupId))?;
        if self.topics.is_empty() {
            return Err(Error::Consumer(ConsumerError::NoTopicsAssigned));
        }

        let hosts = self.hosts;
        let topics = self.topics;
        let channel_capacity = self.channel_capacity;

        let consumer = tokio::task::spawn_blocking(move || {
            let mut builder = Consumer::from_hosts(hosts).with_group(group);
            for topic in topics {
                builder = builder.with_topic(topic);
            }
            builder.create()
        })
        .await
        .map_err(|e| Error::Config(format!("failed to build consumer task: {e}")))??;

        Ok(AsyncConsumer::from_sync(consumer, channel_capacity))
    }
}

impl AsyncConsumer {
    /// Starts building a new async consumer from bootstrap hosts.
    #[must_use]
    pub fn builder(hosts: Vec<String>) -> AsyncConsumerBuilder {
        AsyncConsumerBuilder::new(hosts)
    }

    /// Creates a new async consumer from bootstrap hosts.
    ///
    /// This will construct a synchronous `Consumer` from the provided hosts,
    /// group and topics and spawn a background thread that runs the consumer
    /// event loop. If the bootstrap hosts are unreachable an error is
    /// returned.
    pub async fn from_hosts(
        hosts: Vec<String>,
        group: String,
        topics: Vec<String>,
    ) -> Result<Self> {
        Self::builder(hosts)
            .with_group(group)
            .with_topics(topics)
            .build()
            .await
    }

    /// Creates a new async consumer from an [`AsyncKafkaClient`].
    pub async fn from_client(
        client: AsyncKafkaClient,
        group: String,
        topics: Vec<String>,
    ) -> Result<Self> {
        Self::from_hosts(client.bootstrap_hosts().to_vec(), group, topics).await
    }

    /// Polls for new messages and returns fetched message sets.
    ///
    /// This sends a `Poll` command to the background thread and awaits the
    /// response via a `oneshot` channel. If the background thread has been
    /// shut down the returned error maps to `Error::Consumer(ConsumerError::NoTopicsAssigned)`.
    pub async fn poll(&mut self) -> Result<MessageSets> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ConsumerCommand::Poll { response: tx })
            .await
            .map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?;
        rx.await
            .map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?
    }

    /// Commits the current consumed offsets.
    ///
    /// Sends a `Commit` command to the background thread which calls
    /// `commit_consumed` on the synchronous `Consumer`.
    pub async fn commit(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ConsumerCommand::Commit { response: tx })
            .await
            .map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?;
        rx.await
            .map_err(|_| Error::Consumer(ConsumerError::NoTopicsAssigned))?
    }

    /// Gracefully closes the consumer by signaling shutdown and joining the
    /// background thread.
    pub async fn close(mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            let _ = self.sender.send(ConsumerCommand::Shutdown).await;
            let join_result = tokio::task::spawn_blocking(move || handle.join())
                .await
                .map_err(|e| Error::Config(format!("failed to join consumer thread: {e}")))?;
            if join_result.is_err() {
                return Err(Error::Config("consumer thread panicked".to_owned()));
            }
        }
        Ok(())
    }

    fn from_sync(consumer: Consumer, channel_capacity: usize) -> Self {
        let (sender, mut receiver) = mpsc::channel::<ConsumerCommand>(channel_capacity.max(1));

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
        Self {
            sender,
            handle: Some(handle),
        }
    }
}

impl Drop for AsyncConsumer {
    fn drop(&mut self) {
        if self.handle.is_some() {
            let _ = self.sender.try_send(ConsumerCommand::Shutdown);
            // Dropping `JoinHandle` detaches the thread and avoids blocking in
            // `Drop` on a runtime worker thread.
            self.handle.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use rustfs_kafka::error::{ConnectionError, Error};

    use super::*;

    #[tokio::test]
    async fn from_hosts_fails_with_unreachable_hosts() {
        let result = AsyncConsumer::from_hosts(
            vec!["127.0.0.1:1".to_owned()],
            "test-group".to_owned(),
            vec!["test-topic".to_owned()],
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::NoHostReachable))
        ));
    }

    #[tokio::test]
    async fn from_client_fails_with_unreachable_hosts() {
        let client = AsyncKafkaClient::new(vec![]).await.unwrap();
        let result = AsyncConsumer::from_client(
            client,
            "test-group".to_owned(),
            vec!["test-topic".to_owned()],
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn drop_consumer_without_close_does_not_panic() {
        let result = AsyncConsumer::from_hosts(
            vec!["127.0.0.1:1".to_owned()],
            "test-drop-group".to_owned(),
            vec!["test-drop-topic".to_owned()],
        )
        .await;
        assert!(result.is_err());
        // No panic on drop - if we reach here, Drop is clean.
    }

    #[tokio::test]
    async fn builder_without_group_returns_error() {
        let result = AsyncConsumer::builder(vec![])
            .with_topic("t".to_owned())
            .build()
            .await;
        assert!(matches!(
            result,
            Err(Error::Consumer(ConsumerError::UnsetGroupId))
        ));
    }

    #[tokio::test]
    async fn builder_without_topics_returns_error() {
        let result = AsyncConsumer::builder(vec![])
            .with_group("g".to_owned())
            .build()
            .await;
        assert!(matches!(
            result,
            Err(Error::Consumer(ConsumerError::NoTopicsAssigned))
        ));
    }
}
