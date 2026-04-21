//! Async Kafka client built on tokio.
//!
//! Provides [`AsyncKafkaClient`], [`AsyncProducer`], and [`AsyncConsumer`]
//! for asynchronous interaction with Kafka brokers using the tokio runtime.
//!
//! # Example
//!
//! ```no_run
//! use rustfs_kafka_async::{AsyncKafkaClient, AsyncProducer};
//! use rustfs_kafka::producer::Record;
//!
//! #[tokio::main]
//! async fn main() -> rustfs_kafka::error::Result<()> {
//!     let client = AsyncKafkaClient::new(vec!["localhost:9092".to_owned()]).await?;
//!     let mut producer = AsyncProducer::new(client).await?;
//!
//!     producer.send(&Record::from_value("test-topic", &b"hello"[..])).await?;
//!     producer.close().await?;
//!     Ok(())
//! }
//! ```

mod client;
mod connection;
mod consumer;
mod producer;

pub use client::AsyncKafkaClient;
pub use consumer::AsyncConsumer;
pub use producer::AsyncProducer;

// Re-export core types from the sync crate for convenience
pub use rustfs_kafka::error;
pub use rustfs_kafka::producer::{AsBytes, Headers, Record};
