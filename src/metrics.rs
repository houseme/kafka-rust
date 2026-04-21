//! Internal metrics definitions for rustfs-kafka.
//!
//! This module is only available when the `metrics` feature is enabled.
//! Use a compatible metrics exporter (e.g., `metrics-exporter-prometheus`)
//! to collect and expose these metrics.

use metrics::{counter, gauge, histogram};

/// Records a produce operation.
pub fn record_produce(topic: &str, bytes: usize, message_count: usize, duration_ms: f64) {
    counter!("kafka.produce.messages_total", "topic" => topic.to_owned())
        .increment(message_count as u64);
    counter!("kafka.produce.bytes_total", "topic" => topic.to_owned()).increment(bytes as u64);
    histogram!("kafka.produce.duration_seconds", "topic" => topic.to_owned())
        .record(duration_ms / 1000.0);
}

/// Records a produce error.
pub fn record_produce_error(topic: &str, error_type: &str) {
    counter!("kafka.produce.errors_total", "topic" => topic.to_owned(), "error" => error_type.to_owned())
        .increment(1);
}

/// Records a fetch operation.
pub fn record_fetch(topic: &str, bytes: usize, message_count: usize, duration_ms: f64) {
    counter!("kafka.fetch.messages_total", "topic" => topic.to_owned())
        .increment(message_count as u64);
    counter!("kafka.fetch.bytes_total", "topic" => topic.to_owned()).increment(bytes as u64);
    histogram!("kafka.fetch.duration_seconds", "topic" => topic.to_owned())
        .record(duration_ms / 1000.0);
}

/// Records a fetch error.
pub fn record_fetch_error(topic: &str, error_type: &str) {
    counter!("kafka.fetch.errors_total", "topic" => topic.to_owned(), "error" => error_type.to_owned())
        .increment(1);
}

/// Updates the active connection count gauge.
pub fn update_connection_count(count: usize) {
    gauge!("kafka.connection.active").set(count as f64);
}

/// Records a connection error.
pub fn record_connection_error(broker: &str, error_type: &str) {
    counter!("kafka.connection.errors_total", "broker" => broker.to_owned(), "error" => error_type.to_owned())
        .increment(1);
}

/// Records a metadata refresh.
pub fn record_metadata_refresh(duration_ms: f64) {
    counter!("kafka.metadata.refresh_total").increment(1);
    histogram!("kafka.metadata.refresh_duration_seconds").record(duration_ms / 1000.0);
}
