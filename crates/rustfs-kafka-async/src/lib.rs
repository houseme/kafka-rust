//! Async Kafka client built on top of the tokio runtime.
//!
//! This crate provides lightweight asynchronous wrappers around the
//! synchronous APIs in `rustfs-kafka`. It exposes three primary types:
//!
//! - [`AsyncKafkaClient`]: bootstrap and connection management for async code.
//! - [`AsyncProducer`]: an async-friendly producer which runs a synchronous
//!   `Producer` inside a background tokio task.
//! - [`AsyncConsumer`]: an async-friendly consumer which runs a synchronous
//!   `Consumer` inside a dedicated background thread.
//!
//! These wrappers use MPSC/oneshot channels and join/abort semantics to bridge
//! between the synchronous core implementation and asynchronous callers.
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
mod producer;

pub use client::AsyncKafkaClient;
pub use consumer::AsyncConsumer;
pub use producer::AsyncProducer;

// Re-export core types from the sync crate for convenience
pub use rustfs_kafka::error;
pub use rustfs_kafka::producer::{AsBytes, Headers, Record};
