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
#![allow(deprecated)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::struct_field_names)]
#![allow(clippy::missing_transmute_annotations)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::manual_let_else)]

pub mod client;
mod compression;
pub mod consumer;
pub mod error;
mod network;
pub mod producer;
mod protocol;
mod utils;

#[cfg(feature = "security")]
mod tls;

pub use self::error::{Error, Result};
