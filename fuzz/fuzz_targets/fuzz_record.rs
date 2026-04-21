#![no_main]

use libfuzzer_sys::fuzz_target;
use rustfs_kafka::producer::{Record, Headers, AsBytes};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    // Fuzz record creation and header manipulation
    let mut headers = Headers::new();
    let split_at = data.len() / 2;
    if split_at > 0 {
        headers.insert("fuzz-key", &data[..split_at]);
    }
    if data.len() > split_at {
        headers.insert("fuzz-val", &data[split_at..]);
    }

    let topic = "fuzz-topic";
    let _record = Record::from_key_value(topic, &data, &data).with_headers(headers);

    // Fuzz from_value with varying data
    let _record2 = Record::from_value(topic, data);
});
