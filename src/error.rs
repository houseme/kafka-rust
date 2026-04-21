//! Error types and error handling utilities.
//!
//! This module provides the main [`Error`] enum, sub-error types for each
//! layer ([`ConnectionError`], [`ProtocolError`], [`ConsumerError`]), and
//! the [`KafkaCode`] enum for Kafka server error codes.
//!
//! # Retriable Errors
//!
//! Use [`Error::is_retriable()`] to determine if an error can be resolved
//! by retrying. Retriable conditions include:
//! - `LeaderNotAvailable`, `NotLeaderForPartition`
//! - `GroupLoadInProgress`, `GroupCoordinatorNotAvailable`
//! - `NetworkException`, `RequestTimedOut`
//! - Connection I/O errors and timeouts
//!
//! # Broker Context
//!
//! Errors from broker operations can be enriched with context using
//! [`Error::with_broker_context()`], which captures the broker host and
//! API key for improved debuggability.

use std::time::Duration;
use std::{io, result, sync::Arc};

use thiserror::Error;

pub type Result<T> = result::Result<T, Error>;

// --------------------------------------------------------------------
// Sub-error types
// --------------------------------------------------------------------

/// Errors from the network/connection layer.
#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[cfg(feature = "security")]
    #[error("TLS error: {0}")]
    Tls(String),

    #[error("Connection timeout after {0:?}")]
    Timeout(Duration),

    #[error("No host reachable")]
    NoHostReachable,
}

/// Errors from the protocol layer (encoding, decoding, version negotiation).
#[derive(Debug, Clone, Error)]
pub enum ProtocolError {
    #[error("Unsupported protocol version")]
    UnsupportedVersion,

    #[error("Unsupported compression format")]
    UnsupportedCompression,

    #[error("Unexpected end of data")]
    UnexpectedEOF,

    #[error("Encoding/decoding error")]
    Codec,

    #[error("String decoding error")]
    StringDecode,

    #[error("Invalid duration")]
    InvalidDuration,
}

/// Errors from the consumer layer.
#[derive(Debug, Clone, Error)]
pub enum ConsumerError {
    #[error("No topic assigned")]
    NoTopicsAssigned,

    #[error("Offset storage not configured")]
    UnsetOffsetStorage,

    #[error("Group ID not configured")]
    UnsetGroupId,
}

// --------------------------------------------------------------------
// Main Error type
// --------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum Error {
    // === Network layer ===
    #[error("Connection error: {0}")]
    Connection(#[source] ConnectionError),

    // === Protocol layer ===
    #[error("Protocol error: {0}")]
    Protocol(#[source] ProtocolError),

    // === Kafka server ===
    /// An error as reported by a remote Kafka server
    #[error("Kafka Error ({0:?})")]
    Kafka(KafkaCode),

    // === Client configuration ===
    #[error("Configuration error: {0}")]
    Config(String),

    // === Consumer ===
    #[error("Consumer error: {0}")]
    Consumer(#[source] ConsumerError),

    // === Context wrappers ===
    /// An error when transmitting a request for a particular topic and partition.
    #[error("Topic Partition Error ({topic_name:?}, {partition_id:?}, {error_code:?})")]
    TopicPartitionError {
        topic_name: String,
        partition_id: i32,
        error_code: KafkaCode,
    },

    /// An error from a broker request that preserves the request context.
    #[error("Broker request to {broker} failed ({api_key}): {source}")]
    BrokerRequestError {
        broker: String,
        api_key: &'static str,
        #[source]
        source: Box<Self>,
    },
}

// Allow io::Error to convert to Error via Connection(Io(..))
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Connection(ConnectionError::Io(e))
    }
}

impl From<Arc<Self>> for Error {
    fn from(e: Arc<Self>) -> Self {
        match Arc::try_unwrap(e) {
            Ok(err) => err,
            Err(arc) => match &*arc {
                Self::Connection(ConnectionError::Io(e)) => {
                    Self::Connection(ConnectionError::Io(io::Error::new(e.kind(), e.to_string())))
                }
                Self::Connection(ConnectionError::Tls(s)) => {
                    Self::Connection(ConnectionError::Tls(s.clone()))
                }
                Self::Connection(ConnectionError::Timeout(d)) => {
                    Self::Connection(ConnectionError::Timeout(*d))
                }
                Self::Connection(ConnectionError::NoHostReachable) => {
                    Self::Connection(ConnectionError::NoHostReachable)
                }
                Self::Protocol(p) => Self::Protocol(p.clone()),
                Self::Kafka(c) => Self::Kafka(*c),
                Self::Config(s) => Self::Config(s.clone()),
                Self::Consumer(c) => Self::Consumer(c.clone()),
                Self::TopicPartitionError {
                    topic_name,
                    partition_id,
                    error_code,
                } => Self::TopicPartitionError {
                    topic_name: topic_name.clone(),
                    partition_id: *partition_id,
                    error_code: *error_code,
                },
                Self::BrokerRequestError {
                    broker,
                    api_key,
                    source: _,
                } => Self::BrokerRequestError {
                    broker: broker.clone(),
                    api_key,
                    source: Box::new(Self::from(Arc::clone(&arc))),
                },
            },
        }
    }
}

// --------------------------------------------------------------------
// Convenience constructors
// --------------------------------------------------------------------

impl Error {
    #[inline]
    pub(crate) fn codec() -> Self {
        Self::Protocol(ProtocolError::Codec)
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn unexpected_eof() -> Self {
        Self::Protocol(ProtocolError::UnexpectedEOF)
    }

    #[inline]
    pub(crate) fn no_host_reachable() -> Self {
        Self::Connection(ConnectionError::NoHostReachable)
    }

    #[inline]
    pub(crate) fn invalid_duration() -> Self {
        Self::Protocol(ProtocolError::InvalidDuration)
    }

    #[inline]
    pub(crate) fn no_topics_assigned() -> Self {
        Self::Consumer(ConsumerError::NoTopicsAssigned)
    }

    #[inline]
    pub(crate) fn unset_group_id() -> Self {
        Self::Consumer(ConsumerError::UnsetGroupId)
    }

    #[cfg(feature = "security")]
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn tls(msg: impl Into<String>) -> Self {
        Self::Connection(ConnectionError::Tls(msg.into()))
    }

    /// Wraps this error with broker request context (broker host and API key name).
    #[must_use]
    pub fn with_broker_context(self, broker: impl Into<String>, api_key: &'static str) -> Self {
        Error::BrokerRequestError {
            broker: broker.into(),
            api_key,
            source: Box::new(self),
        }
    }

    /// Returns `true` if this error is likely transient and can be resolved by retrying.
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Kafka(code) => code.is_retriable(),
            Self::Connection(conn_err) => matches!(
                conn_err,
                ConnectionError::Io(_)
                    | ConnectionError::Timeout(_)
                    | ConnectionError::NoHostReachable
            ),
            Self::TopicPartitionError { error_code, .. } => error_code.is_retriable(),
            Self::BrokerRequestError { source, .. } => source.is_retriable(),
            _ => false,
        }
    }

    /// Returns `true` if this error originates from the connection/network layer.
    pub fn is_connection_error(&self) -> bool {
        matches!(self, Self::Connection(_))
    }

    /// Returns `true` if this error originates from the protocol layer.
    pub fn is_protocol_error(&self) -> bool {
        matches!(self, Self::Protocol(_))
    }

    /// Returns `true` if this error originates from the consumer layer.
    pub fn is_consumer_error(&self) -> bool {
        matches!(self, Self::Consumer(_))
    }

    pub(crate) fn from_protocol(n: i16) -> Option<Error> {
        KafkaCode::from_protocol(n).map(Error::Kafka)
    }
}

// --------------------------------------------------------------------
// KafkaCode
// --------------------------------------------------------------------

/// Various errors reported by a remote Kafka server.
/// See also [Kafka Errors](http://kafka.apache.org/protocol.html)
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum KafkaCode {
    /// An unexpected server error
    Unknown = -1,
    /// The requested offset is outside the range of offsets
    /// maintained by the server for the given topic/partition
    OffsetOutOfRange = 1,
    /// This indicates that a message contents does not match its CRC
    CorruptMessage = 2,
    /// This request is for a topic or partition that does not exist
    /// on this broker.
    UnknownTopicOrPartition = 3,
    /// The message has a negative size
    InvalidMessageSize = 4,
    /// This error is thrown if we are in the middle of a leadership
    /// election and there is currently no leader for this partition
    /// and hence it is unavailable for writes.
    LeaderNotAvailable = 5,
    /// This error is thrown if the client attempts to send messages
    /// to a replica that is not the leader for some partition. It
    /// indicates that the clients metadata is out of date.
    NotLeaderForPartition = 6,
    /// This error is thrown if the request exceeds the user-specified
    /// time limit in the request.
    RequestTimedOut = 7,
    /// This is not a client facing error and is used mostly by tools
    /// when a broker is not alive.
    BrokerNotAvailable = 8,
    /// If replica is expected on a broker, but is not (this can be
    /// safely ignored).
    ReplicaNotAvailable = 9,
    /// The server has a configurable maximum message size to avoid
    /// unbounded memory allocation. This error is thrown if the
    /// client attempt to produce a message larger than this maximum.
    MessageSizeTooLarge = 10,
    /// Internal error code for broker-to-broker communication.
    StaleControllerEpoch = 11,
    /// If you specify a string larger than configured maximum for
    /// offset metadata
    OffsetMetadataTooLarge = 12,
    /// The server disconnected before a response was received.
    NetworkException = 13,
    /// The broker returns this error code for an offset fetch request
    /// if it is still loading offsets (after a leader change for that
    /// offsets topic partition), or in response to group membership
    /// requests (such as heartbeats) when group metadata is being
    /// loaded by the coordinator.
    GroupLoadInProgress = 14,
    /// The broker returns this error code for group coordinator
    /// requests, offset commits, and most group management requests
    /// if the offsets topic has not yet been created, or if the group
    /// coordinator is not active.
    GroupCoordinatorNotAvailable = 15,
    /// The broker returns this error code if it receives an offset
    /// fetch or commit request for a group that it is not a
    /// coordinator for.
    NotCoordinatorForGroup = 16,
    /// For a request which attempts to access an invalid topic
    /// (e.g. one which has an illegal name), or if an attempt is made
    /// to write to an internal topic (such as the consumer offsets
    /// topic).
    InvalidTopic = 17,
    /// If a message batch in a produce request exceeds the maximum
    /// configured segment size.
    RecordListTooLarge = 18,
    /// Returned from a produce request when the number of in-sync
    /// replicas is lower than the configured minimum and requiredAcks is
    /// -1.
    NotEnoughReplicas = 19,
    /// Returned from a produce request when the message was written
    /// to the log, but with fewer in-sync replicas than required.
    NotEnoughReplicasAfterAppend = 20,
    /// Returned from a produce request if the requested requiredAcks is
    /// invalid (anything other than -1, 1, or 0).
    InvalidRequiredAcks = 21,
    /// Returned from group membership requests (such as heartbeats) when
    /// the generation id provided in the request is not the current
    /// generation.
    IllegalGeneration = 22,
    /// Returned in join group when the member provides a protocol type or
    /// set of protocols which is not compatible with the current group.
    InconsistentGroupProtocol = 23,
    /// Returned in join group when the groupId is empty or null.
    InvalidGroupId = 24,
    /// Returned from group requests (offset commits/fetches, heartbeats,
    /// etc) when the memberId is not in the current generation.
    UnknownMemberId = 25,
    /// Return in join group when the requested session timeout is outside
    /// of the allowed range on the broker
    InvalidSessionTimeout = 26,
    /// Returned in heartbeat requests when the coordinator has begun
    /// rebalancing the group. This indicates to the client that it
    /// should rejoin the group.
    RebalanceInProgress = 27,
    /// This error indicates that an offset commit was rejected because of
    /// oversize metadata.
    InvalidCommitOffsetSize = 28,
    /// Returned by the broker when the client is not authorized to access
    /// the requested topic.
    TopicAuthorizationFailed = 29,
    /// Returned by the broker when the client is not authorized to access
    /// a particular groupId.
    GroupAuthorizationFailed = 30,
    /// Returned by the broker when the client is not authorized to use an
    /// inter-broker or administrative API.
    ClusterAuthorizationFailed = 31,
    /// The timestamp of the message is out of acceptable range.
    InvalidTimestamp = 32,
    /// The broker does not support the requested SASL mechanism.
    UnsupportedSaslMechanism = 33,
    /// Request is not valid given the current SASL state.
    IllegalSaslState = 34,
    /// The version of API is not supported.
    UnsupportedVersion = 35,
    // CAUTION! When adding to this list, KafkaCode::from_protocol must be updated. If there's a better way, please open an issue for it!
}

impl KafkaCode {
    /// Returns `true` if this error code represents a transient, retriable condition.
    pub fn is_retriable(self) -> bool {
        matches!(
            self,
            KafkaCode::LeaderNotAvailable
                | KafkaCode::NotLeaderForPartition
                | KafkaCode::GroupLoadInProgress
                | KafkaCode::GroupCoordinatorNotAvailable
                | KafkaCode::NotCoordinatorForGroup
                | KafkaCode::NetworkException
                | KafkaCode::RequestTimedOut
                | KafkaCode::RebalanceInProgress
        )
    }

    pub(crate) fn from_protocol(n: i16) -> Option<KafkaCode> {
        if n == 0 {
            return None;
        }
        if n >= KafkaCode::OffsetOutOfRange as i16 && n <= KafkaCode::UnsupportedVersion as i16 {
            return Some(unsafe { std::mem::transmute(n as i8) });
        }
        Some(KafkaCode::Unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn test_kafka_code_from_i32() {
        assert!(KafkaCode::from_protocol(0).is_none());
        assert_eq!(
            KafkaCode::from_protocol(1),
            Some(KafkaCode::OffsetOutOfRange)
        );
        assert_eq!(
            KafkaCode::from_protocol(6),
            Some(KafkaCode::NotLeaderForPartition)
        );
        assert_eq!(KafkaCode::from_protocol(999), Some(KafkaCode::Unknown));
    }

    #[test]
    fn test_error_display() {
        let msg = Error::Kafka(KafkaCode::LeaderNotAvailable).to_string();
        assert!(msg.contains("LeaderNotAvailable"), "got: {msg}");

        let msg = Error::no_host_reachable().to_string();
        assert!(msg.contains("host"), "got: {msg}");

        let msg = Error::unexpected_eof().to_string();
        assert!(msg.contains("end of data"), "got: {msg}");
    }

    #[test]
    fn test_error_with_broker_context() {
        let err = Error::Kafka(KafkaCode::NotLeaderForPartition)
            .with_broker_context("broker1:9092", "Produce");
        let msg = err.to_string();
        assert!(msg.contains("broker1:9092"), "got: {msg}");
        assert!(msg.contains("Produce"), "got: {msg}");
    }

    #[test]
    fn test_error_io_conversion() {
        let io_err = io::Error::new(ErrorKind::ConnectionRefused, "refused");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Connection(ConnectionError::Io(_))));
    }

    #[test]
    fn test_topic_partition_error() {
        let err = Error::TopicPartitionError {
            topic_name: "test-topic".into(),
            partition_id: 0,
            error_code: KafkaCode::LeaderNotAvailable,
        };
        let msg = err.to_string();
        assert!(msg.contains("test-topic"), "got: {msg}");
    }

    #[test]
    fn test_is_retriable_kafka_errors() {
        assert!(Error::Kafka(KafkaCode::LeaderNotAvailable).is_retriable());
        assert!(Error::Kafka(KafkaCode::NotLeaderForPartition).is_retriable());
        assert!(Error::Kafka(KafkaCode::GroupLoadInProgress).is_retriable());
        assert!(Error::Kafka(KafkaCode::GroupCoordinatorNotAvailable).is_retriable());
        assert!(Error::Kafka(KafkaCode::NotCoordinatorForGroup).is_retriable());
        assert!(Error::Kafka(KafkaCode::NetworkException).is_retriable());
        assert!(Error::Kafka(KafkaCode::RequestTimedOut).is_retriable());
        assert!(Error::Kafka(KafkaCode::RebalanceInProgress).is_retriable());
    }

    #[test]
    fn test_is_not_retriable_kafka_errors() {
        assert!(!Error::Kafka(KafkaCode::UnknownTopicOrPartition).is_retriable());
        assert!(!Error::Kafka(KafkaCode::MessageSizeTooLarge).is_retriable());
        assert!(!Error::Kafka(KafkaCode::Unknown).is_retriable());
    }

    #[test]
    fn test_is_retriable_connection_errors() {
        assert!(
            Error::Connection(ConnectionError::Io(io::Error::new(
                ErrorKind::ConnectionReset,
                "reset"
            )))
            .is_retriable()
        );
        assert!(Error::Connection(ConnectionError::Timeout(Duration::from_secs(5))).is_retriable());
        assert!(Error::Connection(ConnectionError::NoHostReachable).is_retriable());
        #[cfg(feature = "security")]
        assert!(!Error::Connection(ConnectionError::Tls("bad cert".into())).is_retriable());
    }

    #[test]
    fn test_is_retriable_topic_partition_error() {
        let err = Error::TopicPartitionError {
            topic_name: "t".into(),
            partition_id: 0,
            error_code: KafkaCode::LeaderNotAvailable,
        };
        assert!(err.is_retriable());

        let err = Error::TopicPartitionError {
            topic_name: "t".into(),
            partition_id: 0,
            error_code: KafkaCode::UnknownTopicOrPartition,
        };
        assert!(!err.is_retriable());
    }

    #[test]
    fn test_is_retriable_broker_request_error() {
        let inner = Error::Kafka(KafkaCode::NotLeaderForPartition);
        let err = inner.with_broker_context("broker:9092", "Produce");
        assert!(err.is_retriable());
    }

    #[test]
    fn test_is_retriable_non_retriable_errors() {
        assert!(!Error::codec().is_retriable());
        assert!(!Error::no_topics_assigned().is_retriable());
        assert!(!Error::Config("bad".into()).is_retriable());
    }

    #[test]
    fn test_kafka_code_is_retriable() {
        assert!(KafkaCode::LeaderNotAvailable.is_retriable());
        assert!(KafkaCode::NetworkException.is_retriable());
        assert!(!KafkaCode::UnknownTopicOrPartition.is_retriable());
        assert!(!KafkaCode::OffsetOutOfRange.is_retriable());
    }

    #[test]
    fn test_error_category_queries() {
        assert!(
            Error::Connection(ConnectionError::Io(io::Error::new(ErrorKind::Other, "err")))
                .is_connection_error()
        );
        assert!(!Error::codec().is_connection_error());
        assert!(Error::codec().is_protocol_error());
        assert!(Error::no_topics_assigned().is_consumer_error());
    }

    #[test]
    fn test_protocol_error_variants() {
        assert!(
            Error::Protocol(ProtocolError::UnsupportedVersion)
                .to_string()
                .contains("version")
        );
        assert!(
            Error::Protocol(ProtocolError::UnsupportedCompression)
                .to_string()
                .contains("compression")
        );
        assert!(
            Error::Protocol(ProtocolError::Codec)
                .to_string()
                .contains("Encoding")
        );
        assert!(
            Error::Protocol(ProtocolError::StringDecode)
                .to_string()
                .contains("String")
        );
        assert!(
            Error::Protocol(ProtocolError::InvalidDuration)
                .to_string()
                .contains("duration")
        );
    }

    #[test]
    fn test_consumer_error_variants() {
        assert!(
            Error::Consumer(ConsumerError::NoTopicsAssigned)
                .to_string()
                .contains("topic")
        );
        assert!(
            Error::Consumer(ConsumerError::UnsetOffsetStorage)
                .to_string()
                .contains("Offset")
        );
        assert!(
            Error::Consumer(ConsumerError::UnsetGroupId)
                .to_string()
                .contains("Group")
        );
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// KafkaCode::from_protocol never panics for any i16 input
        #[test]
        fn kafka_code_from_any_i16(code in proptest::num::i16::ANY) {
            let _ = KafkaCode::from_protocol(code);
        }

        /// Out-of-range error codes map to Unknown
        #[test]
        fn kafka_code_unknown_for_out_of_range(code in 36i16..=1000i16) {
            assert_eq!(KafkaCode::from_protocol(code), Some(KafkaCode::Unknown));
        }

        /// Error::with_broker_context always produces a string containing broker
        #[test]
        fn broker_context_chainable(broker in "[a-z]{1,20}") {
            let err = Error::no_host_reachable();
            let wrapped = err.with_broker_context(broker.clone(), "Produce");
            let msg = wrapped.to_string();
            assert!(msg.contains(&broker));
            assert!(msg.contains("Produce"));
        }

        /// is_retriable is safe to call on any error variant
        #[test]
        fn is_retriable_safe(code in proptest::num::i16::ANY) {
            if let Some(kafka_code) = KafkaCode::from_protocol(code) {
                let err = Error::Kafka(kafka_code);
                let _ = err.is_retriable();
            }
        }

        /// Error display never panics
        #[test]
        fn error_display_safe(code in proptest::num::i16::ANY) {
            if let Some(kafka_code) = KafkaCode::from_protocol(code) {
                let err = Error::Kafka(kafka_code);
                let msg = err.to_string();
                assert!(!msg.is_empty());
            }
        }
    }
}
