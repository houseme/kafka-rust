//! Native async consumer observability helpers.

use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use rustfs_kafka::error::{Error, KafkaCode};

use crate::wire::{kafka_code_from_protocol, kafka_code_to_protocol};

pub(crate) const DEFAULT_NATIVE_RECENT_ERROR_LIMIT: usize = 32;

/// Native consumer error snapshot for diagnostics.
#[derive(Debug, Clone)]
pub struct NativeConsumerErrorSnapshot {
    pub phase: String,
    pub class: String,
    pub kafka_code: Option<KafkaCode>,
    pub message: String,
    pub timestamp_unix_ms: u128,
}

/// Native consumer error statistics.
#[derive(Debug, Clone)]
pub struct NativeConsumerErrorStats {
    pub total_errors: u64,
    pub kafka_code_counts: HashMap<String, u64>,
    pub class_counts: HashMap<String, u64>,
    pub last_error: Option<NativeConsumerErrorSnapshot>,
    pub recent_errors: Vec<NativeConsumerErrorSnapshot>,
}

#[derive(Debug, Clone)]
pub(crate) struct NativeConsumerObservability {
    total_errors: u64,
    kafka_code_counts: HashMap<String, u64>,
    class_counts: HashMap<String, u64>,
    last_error: Option<NativeConsumerErrorSnapshot>,
    recent_errors: VecDeque<NativeConsumerErrorSnapshot>,
    recent_error_limit: usize,
}

impl Default for NativeConsumerObservability {
    fn default() -> Self {
        Self::new(DEFAULT_NATIVE_RECENT_ERROR_LIMIT)
    }
}

impl NativeConsumerObservability {
    pub(crate) fn new(recent_error_limit: usize) -> Self {
        Self {
            total_errors: 0,
            kafka_code_counts: HashMap::new(),
            class_counts: HashMap::new(),
            last_error: None,
            recent_errors: VecDeque::new(),
            recent_error_limit: recent_error_limit.max(1),
        }
    }

    pub(crate) fn stats(&self) -> NativeConsumerErrorStats {
        NativeConsumerErrorStats {
            total_errors: self.total_errors,
            kafka_code_counts: self.kafka_code_counts.clone(),
            class_counts: self.class_counts.clone(),
            last_error: self.last_error.clone(),
            recent_errors: self.recent_errors.iter().cloned().collect(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.total_errors = 0;
        self.kafka_code_counts.clear();
        self.class_counts.clear();
        self.last_error = None;
        self.recent_errors.clear();
    }

    pub(crate) fn record_error(&mut self, phase: &str, err: &Error) {
        self.total_errors = self.total_errors.saturating_add(1);
        let class = error_class(err);
        let kafka_code = kafka_code_from_error(err).map(kafka_code_to_protocol);
        let kafka_code_label = kafka_code.map(|code| code.to_string());

        *self.class_counts.entry(class.clone()).or_insert(0) += 1;
        if let Some(code) = &kafka_code_label {
            *self.kafka_code_counts.entry(code.clone()).or_insert(0) += 1;
        }

        let snapshot = NativeConsumerErrorSnapshot {
            phase: phase.to_owned(),
            class,
            kafka_code: kafka_code.and_then(kafka_code_from_protocol),
            message: err.to_string(),
            timestamp_unix_ms: now_unix_ms(),
        };
        self.last_error = Some(snapshot.clone());
        self.recent_errors.push_back(snapshot.clone());
        while self.recent_errors.len() > self.recent_error_limit {
            let _ = self.recent_errors.pop_front();
        }

        crate::metrics::record_native_consumer_error(
            phase,
            &snapshot.class,
            kafka_code_label.as_deref(),
            self.recent_errors.len(),
            snapshot.timestamp_unix_ms,
        );
    }
}

fn kafka_code_from_error(err: &Error) -> Option<KafkaCode> {
    match err {
        Error::Kafka(code) => Some(*code),
        Error::TopicPartitionError { error_code, .. } => Some(*error_code),
        Error::BrokerRequestError { source, .. } => kafka_code_from_error(source),
        _ => None,
    }
}

fn error_class(err: &Error) -> String {
    match err {
        Error::Kafka(_) => "kafka".to_owned(),
        Error::Connection(_) => "connection".to_owned(),
        Error::Protocol(_) => "protocol".to_owned(),
        Error::Config(_) => "config".to_owned(),
        Error::Consumer(_) => "consumer".to_owned(),
        Error::TopicPartitionError { .. } => "topic_partition".to_owned(),
        Error::BrokerRequestError { .. } => "broker_request".to_owned(),
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
}

#[cfg(test)]
mod tests {
    use rustfs_kafka::error::ConnectionError;

    use super::*;

    #[test]
    fn observability_records_recent_snapshots_and_numeric_codes() {
        let mut observability = NativeConsumerObservability::new(2);

        observability.record_error("poll", &Error::Kafka(KafkaCode::LeaderNotAvailable));
        observability.record_error("poll", &Error::Kafka(KafkaCode::NotLeaderForPartition));
        observability.record_error(
            "commit",
            &Error::Kafka(KafkaCode::GroupCoordinatorNotAvailable),
        );

        let stats = observability.stats();
        assert_eq!(stats.total_errors, 3);
        assert_eq!(stats.kafka_code_counts.get("5").copied(), Some(1));
        assert_eq!(stats.kafka_code_counts.get("6").copied(), Some(1));
        assert_eq!(stats.kafka_code_counts.get("15").copied(), Some(1));
        assert_eq!(stats.recent_errors.len(), 2);
        assert_eq!(stats.recent_errors[0].phase, "poll");
        assert_eq!(stats.recent_errors[1].phase, "commit");

        observability.clear();
        let reset = observability.stats();
        assert_eq!(reset.total_errors, 0);
        assert!(reset.kafka_code_counts.is_empty());
        assert!(reset.recent_errors.is_empty());
    }

    #[test]
    fn observability_classifies_non_kafka_errors() {
        let mut observability = NativeConsumerObservability::new(4);
        observability.record_error(
            "connect",
            &Error::Connection(ConnectionError::NoHostReachable),
        );

        let stats = observability.stats();
        assert_eq!(stats.total_errors, 1);
        assert_eq!(stats.class_counts.get("connection").copied(), Some(1));
        assert!(stats.kafka_code_counts.is_empty());
        assert_eq!(stats.last_error.unwrap().class, "connection");
    }
}
