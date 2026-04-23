# rustfs-kafka Usage Guide

This guide covers common usage for both:

- `rustfs-kafka` (sync APIs)
- `rustfs-kafka-async` (tokio-based async wrapper)

## 1. Add Dependencies

```toml
[dependencies]
rustfs-kafka = "1.2.0"
rustfs-kafka-async = "1.2.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

If you only need synchronous APIs, `rustfs-kafka-async` and `tokio` are not required.

## 2. Synchronous Client (`rustfs-kafka`)

### Create a Client

```rust,no_run
use rustfs_kafka::client::KafkaClient;

fn main() -> rustfs_kafka::error::Result<()> {
    let mut client = KafkaClient::new(vec!["127.0.0.1:9092".to_owned()]);
    client.load_metadata_all()?;
    Ok(())
}
```

### Produce Messages

```rust,no_run
use rustfs_kafka::producer::{Producer, Record, RequiredAcks};
use std::time::Duration;

fn main() -> rustfs_kafka::error::Result<()> {
    let mut producer = Producer::from_hosts(vec!["127.0.0.1:9092".to_owned()])
        .with_required_acks(RequiredAcks::All)
        .with_ack_timeout(Duration::from_secs(1))
        .create()?;

    producer.send(&Record::from_value("demo-topic", b"hello"))?;
    Ok(())
}
```

### Consume Messages

```rust,no_run
use rustfs_kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

fn main() -> rustfs_kafka::error::Result<()> {
    let mut consumer = Consumer::from_hosts(vec!["127.0.0.1:9092".to_owned()])
        .with_topic("demo-topic".to_owned())
        .with_group("demo-group".to_owned())
        .with_fallback_offset(FetchOffset::Latest)
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .create()?;

    for ms in consumer.poll()?.iter() {
        for m in ms.messages() {
            println!("message: {:?}", m.value);
        }
        consumer.consume_messageset(&ms)?;
    }
    consumer.commit_consumed()?;
    Ok(())
}
```

## 3. Asynchronous Client (`rustfs-kafka-async`)

### Async Producer

```rust,no_run
use rustfs_kafka::producer::Record;
use rustfs_kafka_async::AsyncProducer;

#[tokio::main]
async fn main() -> rustfs_kafka::error::Result<()> {
    let producer = AsyncProducer::builder(vec!["127.0.0.1:9092".to_owned()])
        .with_client_id("demo-async-producer".to_owned())
        .build()
        .await?;
    producer.send(&Record::from_value("demo-topic", b"hello async")).await?;
    producer.flush().await?;
    producer.close().await?;
    Ok(())
}
```

### Async Consumer

```rust,no_run
use rustfs_kafka_async::AsyncConsumer;

#[tokio::main]
async fn main() -> rustfs_kafka::error::Result<()> {
    let mut consumer = AsyncConsumer::builder(vec!["127.0.0.1:9092".to_owned()])
        .with_group("demo-group".to_owned())
        .with_topic("demo-topic".to_owned())
        .build()
        .await?;

    let messages = consumer.poll().await?;
    for ms in messages.iter() {
        for m in ms.messages() {
            println!("async message: {:?}", m.value);
        }
    }
    consumer.commit().await?;
    consumer.close().await?;
    Ok(())
}
```

## 4. TLS and Feature Flags

- Default TLS feature: `security` (rustls + aws-lc-rs).
- Alternative TLS provider: `security-ring`.
- Disable all default features:

```toml
rustfs-kafka = { version = "1.2.0", default-features = false }
```

## 5. Integration Testing

The repository includes Docker-based integration tests:

```bash
cd crates/rustfs-kafka/tests
./run-all-tests
```

Examples:

```bash
./run-all-tests 4.2.0
SECURES=secure ./run-all-tests 3.9.2
COMPRESSIONS=NONE:SNAPPY:GZIP ./run-all-tests 3.9.2:4.1.2:4.2.0
```

Async secure SASL acceptance checks:

```bash
./run-async-secure-tests
```
