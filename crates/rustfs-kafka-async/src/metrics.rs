//! Internal metrics definitions for rustfs-kafka-async.
//!
//! This module is only available when the `metrics` feature is enabled.

#[cfg(feature = "metrics")]
use metrics::counter;

/// Records native consumer errors on the key observability dimensions.
pub fn record_native_consumer_error(phase: &str, class: &str, kafka_code: Option<&str>) {
    #[cfg(feature = "metrics")]
    {
        counter!("kafka.async.consumer.native.errors_total").increment(1);
        counter!(
            "kafka.async.consumer.native.errors_by_phase_total",
            "phase" => phase.to_owned()
        )
        .increment(1);
        counter!(
            "kafka.async.consumer.native.errors_by_class_total",
            "class" => class.to_owned()
        )
        .increment(1);
        if let Some(code) = kafka_code {
            counter!(
                "kafka.async.consumer.native.errors_by_kafka_code_total",
                "kafka_code" => code.to_owned()
            )
            .increment(1);
        }
    }

    #[cfg(not(feature = "metrics"))]
    {
        let _ = phase;
        let _ = class;
        let _ = kafka_code;
    }
}
