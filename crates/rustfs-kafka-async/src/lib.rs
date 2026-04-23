//! Async Kafka client built on top of the tokio runtime.
//!
//! This crate provides native asynchronous Kafka clients built on tokio.
//! It exposes three primary types:
//!
//! - [`AsyncKafkaClient`]: bootstrap and connection management for async code.
//! - [`AsyncProducer`]: an async producer using non-blocking Kafka protocol I/O.
//! - [`AsyncProducerBuilder`]: async builder for configuring and creating an
//!   `AsyncProducer` without blocking the tokio scheduler.
//! - [`AsyncConsumer`]: an async consumer using non-blocking Kafka protocol I/O.
//! - [`AsyncConsumerBuilder`]: async builder for configuring and creating an
//!   `AsyncConsumer` without blocking the tokio scheduler.
//!
//! # Example
//!
//! ```no_run
//! use rustfs_kafka_async::{AsyncKafkaClient, AsyncProducer};
//! use rustfs_kafka::producer::Record;
//!
//! #[tokio::main]
//! async fn main() -> rustfs_kafka::error::Result<()> {
//!     // Create an async client from bootstrap hosts
//!     let client = AsyncKafkaClient::new(vec!["localhost:9092".to_owned()]).await?;
//!     // Create an async producer which manages a background task
//!     let mut producer = AsyncProducer::new(client).await?;
//!
//!     // Send a single message and close the producer
//!     producer.send(&Record::from_value("test-topic", &b"hello"[..])).await?;
//!     producer.close().await?;
//!     Ok(())
//! }
//! ```

mod client;
mod connection;
mod consumer;
mod metrics;
mod producer;

pub use client::AsyncKafkaClient;
pub use consumer::{AsyncConsumer, AsyncConsumerBuilder};
pub use producer::{AsyncProducer, AsyncProducerBuilder, AsyncProducerConfig};

// Re-export core types from the sync crate for convenience
pub use rustfs_kafka::client::{RequiredAcks, SaslConfig, SecurityConfig};
pub use rustfs_kafka::error;
pub use rustfs_kafka::producer::{AsBytes, Headers, Record};
