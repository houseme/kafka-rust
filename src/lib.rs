//! Clients for comunicating with a [Kafka](http://kafka.apache.org/)
//! cluster.  These are:
//!
//! - `kafka::producer::Producer` - for sending message to Kafka
//! - `kafka::consumer::Consumer` - for retrieving/consuming messages from Kafka
//! - `kafka::client::KafkaClient` - a lower-level, general purpose client leaving
//!   you with more power but also more responsibility
//!
//! See module level documentation corresponding to each client individually.
#![recursion_limit = "128"]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(deprecated)] // Allow deprecated items (e.g., OpenSSL backend)

pub mod client;
mod client_internals;
mod codecs;
mod compression;
pub mod consumer;
pub mod error;
pub mod producer;
mod protocol;
mod utils;

#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
mod tls;

pub use self::error::{Error, Result};
