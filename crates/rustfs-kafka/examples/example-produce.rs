use std::time::Duration;

use rustfs_kafka::producer::{Headers, Producer, Record, RequiredAcks};

/// Minimal producer example.
fn main() {
    tracing_subscriber::fmt::init();

    if let Err(e) = run() {
        eprintln!("produce example failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> rustfs_kafka::Result<()> {
    let brokers = vec!["localhost:9092".to_owned()];
    let topic = "my-topic";

    let mut producer = Producer::from_hosts(brokers)
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .with_client_id("rustfs-kafka-example-produce".to_owned())
        .create()?;

    // Value-only message.
    producer.send(&Record::from_value(topic, b"hello, kafka".as_slice()))?;

    // Key/value message with headers.
    let mut headers = Headers::new();
    headers.insert("trace-id", b"example-trace-id");
    producer.send(&Record {
        topic,
        partition: -1,
        key: b"demo-key".as_slice(),
        value: b"hello with headers".as_slice(),
        headers,
    })?;

    Ok(())
}
