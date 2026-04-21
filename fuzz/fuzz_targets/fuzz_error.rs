#![no_main]

use libfuzzer_sys::fuzz_target;
use rustfs_kafka::error::Error;
use rustfs_kafka::error::KafkaCode;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    // Use the first 2 bytes to construct an i16 error code
    let code = i16::from_be_bytes([data[0], data[data.len() - 1]]);

    // KafkaCode::from_protocol should never panic
    if let Some(kafka_code) = KafkaCode::from_protocol(code) {
        let err = Error::Kafka(kafka_code);
        // is_retriable should never panic
        let _ = err.is_retriable();
        // Display should never panic
        let _msg = err.to_string();
    }

    // Broker context wrapping should never panic
    let err = Error::no_host_reachable()
        .with_broker_context("fuzz-broker", "Produce");
    let _msg = err.to_string();
});
