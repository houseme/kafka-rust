# rustfs-kafka API Guide

## Quick Start

### Producer

```rust,no_run
use std::time::Duration;
use rustfs_kafka::producer::{Producer, Record, RequiredAcks};

let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_ack_timeout(Duration::from_secs(1))
    .with_required_acks(RequiredAcks::One)
    .create()
    .unwrap();

producer.send(&Record::from_value("my-topic", b"hello")).unwrap();
```

### Consumer

```rust,no_run
use rustfs_kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_topic_partitions("my-topic".to_owned(), &[0, 1])
    .with_fallback_offset(FetchOffset::Earliest)
    .with_group("my-group".to_owned())
    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
    .create()
    .unwrap();

loop {
    for ms in consumer.poll().unwrap().iter() {
        for m in ms.messages() {
            println!("{:?}", m);
        }
        consumer.consume_messageset(&ms);
    }
    consumer.commit_consumed().unwrap();
}
```

### KafkaClient

```rust,no_run
use rustfs_kafka::client::KafkaClient;

let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
client.load_metadata_all().unwrap();
```

## Producer API

### Basic Send

```rust,no_run
use rustfs_kafka::producer::{Producer, Record};

let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
    .create()
    .unwrap();

// Send with value only
producer.send(&Record::from_value("topic", b"hello")).unwrap();

// Send with key and value
producer.send(&Record::from_key_value("topic", b"key", b"value")).unwrap();

// Send with headers
let record = Record::from_key_value("topic", b"key", b"value")
    .with_header("trace-id", b"abc-123");
producer.send(&record).unwrap();
```

### Batch Send

```rust,no_run
use rustfs_kafka::producer::{Producer, Record};

let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
    .create()
    .unwrap();

let records = vec![
    Record::from_value("topic", b"msg1"),
    Record::from_value("topic", b"msg2"),
];
let confirms = producer.send_all(&records).unwrap();
```

### BatchProducer

```rust,no_run
use rustfs_kafka::producer::{BatchProducer, Record};

let mut batch = BatchProducer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_batch_size(100)
    .with_linger(Duration::from_millis(5))
    .create()
    .unwrap();

batch.send(Record::from_value("topic", b"msg1")).unwrap();
batch.send(Record::from_value("topic", b"msg2")).unwrap();
let confirms = batch.flush().unwrap();
```

### Custom Partitioner

```rust,no_run
use rustfs_kafka::producer::{Producer, RoundRobinPartitioner, Partitioner};

let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_partitioner(RoundRobinPartitioner::new())
    .create()
    .unwrap();
```

Available partitioners:
- `DefaultPartitioner` — hash-based key partitioning (default)
- `RoundRobinPartitioner` — round-robin for keyless messages
- `StickyPartitioner` — sticks to one partition per batch
- `UniformPartitioner` — random uniform distribution

### Compression

```rust,no_run
use rustfs_kafka::producer::{Producer, Compression};

let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_compression(Compression::GZIP)
    .create()
    .unwrap();
```

Available compression types: `NONE`, `GZIP`, `SNAPPY`, `LZ4`, `ZSTD`.

### Acknowledgment Strategies

```rust,no_run
use rustfs_kafka::producer::RequiredAcks;

RequiredAcks::None  // No acknowledgment (fire and forget)
RequiredAcks::One   // Wait for leader write
RequiredAcks::All   // Wait for all in-sync replicas
```

## Consumer API

### Basic Consumption

```rust,no_run
use rustfs_kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_topic("my-topic".to_owned())
    .with_fallback_offset(FetchOffset::Latest)
    .with_group("my-group".to_owned())
    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
    .create()
    .unwrap();
```

### Offset Management

```rust,no_run
// Fallback offset when no committed offset exists
consumer.with_fallback_offset(FetchOffset::Earliest);
consumer.with_fallback_offset(FetchOffset::Latest);

// Seek to a specific offset
consumer.seek("my-topic", 0, 42).unwrap();
```

### Pause/Resume Partitions

```rust,no_run
consumer.pause("my-topic", &[0, 1]);
consumer.resume("my-topic", &[0]);

if consumer.is_paused("my-topic", 1) {
    println!("Partition 1 is paused");
}
```

### Manual Offset Commit

```rust,no_run
// Mark messages as consumed
consumer.consume_message("topic", 0, 42).unwrap();

// Commit all consumed offsets
consumer.commit_consumed().unwrap();
```

## KafkaClient API

### Connection Management

```rust,no_run
use rustfs_kafka::client::{KafkaClient, SecurityConfig};

// Plaintext
let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);

// TLS
let mut client = KafkaClient::new_secure(
    vec!["localhost:9093".to_owned()],
    SecurityConfig::new().with_ca_cert("ca.pem".to_owned()),
);

// Builder pattern
let mut client = KafkaClient::builder()
    .with_hosts(vec!["localhost:9092".to_owned()])
    .with_client_id("my-app".to_owned())
    .build();
```

### Metadata

```rust,no_run
client.load_metadata_all().unwrap();
client.load_metadata(&["topic1", "topic2"]).unwrap();

for topic in client.topics() {
    println!("{}: {} partitions", topic.name(), topic.partitions().len());
}
```

### Offset Operations

```rust,no_run
use rustfs_kafka::client::FetchOffset;

let offsets = client.fetch_offsets(&["topic"], FetchOffset::Latest).unwrap();

// Commit offsets
client.commit_offset("group", "topic", 0, 42).unwrap();

// Fetch group offsets
let group_offsets = client.fetch_group_offsets(
    "group",
    &[rustfs_kafka::client::FetchGroupOffset::new("topic", 0)],
).unwrap();
```

### Topic Management

```rust,no_run
// Create topics (requires admin protocol support)
// Delete topics (requires admin protocol support)
```

## TLS Configuration

### Basic TLS

```rust,no_run
use rustfs_kafka::client::{KafkaClient, SecurityConfig};

let mut client = KafkaClient::new_secure(
    vec!["localhost:9093".to_owned()],
    SecurityConfig::new().with_ca_cert("ca.pem".to_owned()),
);
```

### Mutual TLS (mTLS)

```rust,no_run
use rustfs_kafka::client::{KafkaClient, SecurityConfig};

let mut client = KafkaClient::new_secure(
    vec!["localhost:9093".to_owned()],
    SecurityConfig::new()
        .with_ca_cert("ca.pem".to_owned())
        .with_client_cert("client.crt".to_owned(), "client.key".to_owned()),
);
```

## Error Handling

### Error Types

```rust,no_run
use rustfs_kafka::error::Error;

match some_operation() {
    Ok(result) => { /* ... */ }
    Err(Error::Kafka(code)) => { /* Server error */ }
    Err(Error::Connection(conn_err)) => { /* Network error */ }
    Err(Error::Protocol(proto_err)) => { /* Protocol error */ }
    Err(Error::Consumer(cons_err)) => { /* Consumer error */ }
    Err(e) => { /* Other error */ }
}
```

### Retriable Errors

```rust,no_run
use rustfs_kafka::error::Error;

match some_operation() {
    Err(e) if e.is_retriable() => {
        // Retry the operation
    }
    Err(e) => {
        // Non-retriable error
    }
    _ => {}
}
```

### Broker Context

```rust,no_run
use rustfs_kafka::error::Error;

// Errors from broker operations include context
match producer.send(&record) {
    Err(e) => {
        // e.to_string() includes broker and API key context
        println!("{}", e);
    }
    Ok(_) => {}
}
```

## Configuration Reference

### Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `security` | yes | TLS support via rustls (aws-lc-rs) |
| `security-ring` | no | TLS with ring crypto backend |
| `producer_timestamp` | no | Timestamp support for records |
| `metrics` | no | Structured metrics via `metrics` crate |
| `integration_tests` | no | Integration test utilities |

### Metrics (Optional)

Enable the `metrics` feature to collect structured metrics:

```toml
[dependencies]
rustfs-kafka = { version = "0.22", features = ["metrics"] }
metrics-exporter-prometheus = "0.16"
```

```rust,no_run
use metrics_exporter_prometheus::PrometheusBuilder;

PrometheusBuilder::new().install().unwrap();
// Metrics are now automatically recorded
```
