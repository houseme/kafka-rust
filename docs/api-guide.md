# rustfs-kafka API Guide

This guide focuses on the current public APIs in `rustfs-kafka` (sync) and the most common usage patterns.

## 1. Quick Start

### 1.1 Producer

```rust,no_run
use std::time::Duration;
use rustfs_kafka::producer::{Producer, Record, RequiredAcks};

let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_client_id("app-producer".to_owned())
    .with_ack_timeout(Duration::from_secs(1))
    .with_required_acks(RequiredAcks::One)
    .create()
    .unwrap();

producer.send(&Record::from_value("my-topic", b"hello")).unwrap();
```

### 1.2 Consumer

```rust,no_run
use rustfs_kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_topic("my-topic".to_owned())
    .with_group("my-group".to_owned())
    .with_fallback_offset(FetchOffset::Earliest)
    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
    .create()
    .unwrap();

for ms in consumer.poll().unwrap() {
    for m in ms.messages() {
        println!("offset={} value={:?}", m.offset, m.value);
    }
    consumer.consume_messageset(&ms).unwrap();
}
consumer.commit_consumed().unwrap();
```

### 1.3 KafkaClient (mid-level)

```rust,no_run
use rustfs_kafka::client::KafkaClient;

let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
client.load_metadata_all().unwrap();
```

## 2. Producer APIs

### 2.1 Single and batch send

- `Producer::send(&Record)` sends one record.
- `Producer::send_all(&[Record])` sends a batch synchronously.

### 2.2 Partitioner selection

- `DefaultPartitioner` (default, key-hash based)
- `RoundRobinPartitioner`
- `StickyPartitioner`
- `UniformPartitioner`

### 2.3 Batch producer

```rust,no_run
use rustfs_kafka::producer::{BatchProducer, Record};
use std::time::Duration;

let mut batch = BatchProducer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_batch_size(100)
    .with_linger(Duration::from_millis(5))
    .create()
    .unwrap();

batch.send(Record::from_value("my-topic", b"msg-1")).unwrap();
batch.send(Record::from_value("my-topic", b"msg-2")).unwrap();
let _confirms = batch.flush().unwrap();
```

### 2.4 Transactional producer

Use `TransactionalProducer` for exactly-once style workflows.

```rust,no_run
use rustfs_kafka::producer::{TransactionalProducer, Record};

let mut tx = TransactionalProducer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_transactional_id("txn-demo".to_owned())
    .create()
    .unwrap();

tx.begin().unwrap();
tx.send(&Record::from_value("my-topic", b"in-txn")).unwrap();
tx.commit().unwrap();
```

## 3. Consumer APIs

### 3.1 Offset strategy

- `FetchOffset::Earliest`
- `FetchOffset::Latest`
- `FetchOffset::ByTime(i64)`

### 3.2 Pause/resume partitions

```rust,no_run
// consumer.pause("my-topic", &[0, 1]);
// consumer.resume("my-topic", &[1]);
```

### 3.3 Group offset APIs through `KafkaClient`

- `commit_offset`, `commit_offsets`
- `fetch_group_offsets`, `fetch_group_topic_offset`

## 4. KafkaClient Administrative APIs

### 4.1 Topic metadata and offsets

- `load_metadata_all`, `load_metadata`
- `fetch_offsets`, `list_offsets`, `fetch_topic_offsets`

### 4.2 Topic create/delete

```rust,no_run
use std::time::Duration;
use rustfs_kafka::client::{KafkaClient, TopicConfig};

let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
client.load_metadata_all().unwrap();

let topics = vec![TopicConfig::new("demo-topic").with_partitions(3)];
let _ = client.create_topics(&topics, Duration::from_secs(10)).unwrap();
let _ = client.delete_topics(&["demo-topic"], Duration::from_secs(10)).unwrap();
```

## 5. TLS

Enable TLS with default feature `security` (rustls + aws-lc-rs):

```toml
[dependencies]
rustfs-kafka = "0.22"
```

`security-ring` switches rustls crypto provider to `ring`:

```toml
[dependencies]
rustfs-kafka = { version = "0.22", default-features = false, features = ["security-ring"] }
```

## 6. Metrics

Enable metrics feature:

```toml
[dependencies]
rustfs-kafka = { version = "0.22", features = ["metrics"] }
```

Metrics include produce/fetch/metadata refresh and connection-level counters/gauges.

## 7. Feature Flags

- `security` (default)
- `security-ring`
- `producer_timestamp`
- `metrics`
- `nightly`
- `integration_tests`

## 8. Async crate

For async wrappers built on tokio, see `crates/rustfs-kafka-async` and its README.
