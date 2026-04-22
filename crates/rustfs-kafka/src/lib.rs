//! Clients for communicating with a [Kafka](http://kafka.apache.org/)
//! cluster.  These are:
//!
//! - `rustfs_kafka::producer::Producer` - for sending message to Kafka
//! - `rustfs_kafka::consumer::Consumer` - for retrieving/consuming messages from Kafka
//! - `rustfs_kafka::client::KafkaClient` - a lower-level, general purpose client leaving
//!   you with more power but also more responsibility
//!
//! See module level documentation corresponding to each client individually.
#![recursion_limit = "128"]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

pub mod client;
mod compression;
pub mod consumer;
pub mod error;
#[cfg(feature = "metrics")]
mod metrics;
mod network;
pub mod producer;
mod protocol;
mod utils;

#[cfg(feature = "security")]
mod tls;

pub use self::error::{Error, Result};
