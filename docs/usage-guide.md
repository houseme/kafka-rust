# rustfs-kafka Usage Guide

This guide covers common usage for both:

- `rustfs-kafka` (sync APIs)
- `rustfs-kafka-async` (tokio-based async wrapper)

## 1. Add Dependencies

```toml
[dependencies]
rustfs-kafka = "1.3.1"
rustfs-kafka-async = "1.3.1"
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

For a batch, call `producer.send_all(&records).await?`. Records are grouped by broker, topic, and partition;
each broker receives one Produce request and each partition retains input order. Empty batches succeed immediately.
All batches are encoded before sending Produce, so a codec error sends no Produce requests. A failure after sending
starts can leave earlier brokers with accepted records. The batch is not atomic across brokers; retrying the whole
batch can duplicate delivery. Acknowledged sends require a complete, unique set of partition confirmations.

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

## 4. Delivery, Routing, and Recovery

Sync consumers exclude paused partitions from both normal and oversized-message retry fetches. Paused retries
remain pending until `resume`; pausing every partition makes `poll` return an empty result. Resuming retains the
partition's next fetch offset. A committed offset equal to the earliest retained offset is a valid starting point.
Sync consumers publish offsets, retry buffer changes, and retry queue updates only after all partition responses
succeed. Transport errors, partition errors, and oversized-message errors preserve progress and pending retries;
a response that omits the retried partition also leaves that retry pending.

Async consumers query committed group offsets before applying `Earliest`, `Latest`, or `ByTime` to partitions
without a committed offset. Metadata refreshes preserve established positions. Progress advances after all broker
responses for a poll succeed, immediately before the returned message sets are delivered. Failed or cancelled polls
leave progress for undelivered messages unchanged. Call `commit` only after successfully processing the returned
messages; polling still advances the in-memory position before application processing.

Sync transport writes complete request frames and flushes TCP/TLS before reading the response. Fetch decoding
includes all record batches returned for a partition. Produce requests maintain contiguous relative offsets within
each partition batch, including compressed batches.

`StickyPartitioner` keeps its batch state per topic and chooses another available partition if the previous one
loses its leader. Explicit record partitions remain unchanged. Calling `with_partitioner` on either sync producer
builder preserves previously selected TLS, client ID, and acknowledgement settings. The regular producer also
retains its timestamp setting, and the batch producer retains its batching settings.

Async producer cache hits borrow partition routes instead of copying all partition metadata. A
`NotLeaderForPartition`, `LeaderNotAvailable`, or `UnknownTopicOrPartition` response clears that topic's route.
The failed call returns the broker error; the next send reloads metadata. Application retries still need to account
for uncertain delivery after network errors.

Batch producer `flush` retains its buffer after transport failure. With acknowledgements enabled, it removes only
partitions with unique successful confirmations; failed, missing, or duplicate confirmations keep those partitions
pending. The returned confirmation list exposes broker partition errors. Automatic flush propagates them as errors.
While unconfirmed records remain after a failure, `send` rejects new records before enqueuing them. Explicit `flush`
retries pending records, and `clear` discards them. Earlier brokers may have accepted records before a transport
failure, so explicit retries still require an application decision about duplicates. With `acks=0`, a successful
complete write clears the batch without claiming broker confirmation.

Async typed requests retain pending correlation IDs across send/receive, reject mismatched IDs or extra response
bytes, and retire failed or cancelled connections before their next use. Raw `send` and `read_exact` preserve their
individual IO semantics; callers own protocol boundaries across separate raw operations. Raw `request_response`
protects the complete frame exchange and returns the payload for caller decoding. Low-level Produce failures after
a sending attempt do not fail over to another broker automatically.

`list_offsets` returns the broker's timestamp, including `-1` when no timestamp is available. `fetch_offsets` keeps
its existing offset-only return type. Sync pool checkout selects the oldest connection, tries another on failure,
and updates only the chosen connection's checkout time.

### Transactional Producer

Use `TransactionalProducer::from_client(client).with_transactional_id(id).create()` with a configured plain or
secure client. The producer discovers a transaction coordinator independently from group coordinators and initializes
a producer ID/epoch with a 60-second transaction timeout. `with_ack_timeout_ms` controls Produce acknowledgement
timeout. `begin`, `send`, and `commit`/`abort` carry the actual transaction context; sequences advance after valid
confirmation and remain continuous across successful transactions. Empty transactions complete locally, and a
second `begin` while active returns an error.

Initialization retries only explicit coordinator-loading, coordinator-unavailable, coordinator-moved, or
concurrent-transaction rejections according to the client's retry policy. A moved coordinator is rediscovered.
Transport and codec failures during initialization are returned.

Before sending a record, a complete single-partition AddPartitions response that explicitly rejects the operation
as a concurrent transaction can be retried within the configured policy. No Produce is sent before that partition
is accepted. Other AddPartitions errors and unknown IO outcomes are returned without replaying records.

After creation, any transaction RPC failure reports its error and blocks subsequent `begin`, `send`, `commit`, and `abort`
on that instance. Recreate the producer with the same transaction ID to acquire a new epoch. The API does not
silently replay an uncertain Produce or EndTxn operation. The producer exposes no transactional offset-commit
workflow; high-level consumers currently read uncommitted data. Validate abort visibility using a Kafka
`read_committed` consumer.

The automatic group heartbeat thread still does not send broker heartbeats. Full group lifecycle, transactional
consumer offsets, high-level read-committed consumers, and broader protocol-version negotiation remain tracked in
[rustfs/backlog#2713](https://github.com/rustfs/backlog/issues/2713).

## 5. TLS and Feature Flags

### Low-Level Protocol Building Blocks

`KafkaClient::send_raw_protocol_request` and
`AsyncKafkaClient::send_raw_protocol_request` expose advanced typed access to
generated `kafka-protocol` request/response pairs. `TelemetrySession` tracks
broker telemetry subscriptions and builds compatible push options, while
`ShareConsumerSession` composes share-group heartbeat, fetch, and acknowledgement
options from local member/assignment state.

- Default TLS feature: `security` (rustls + aws-lc-rs).
- Alternative TLS provider: `security-ring`.
- Either TLS feature enables the same secure client, producer, consumer, and SASL APIs. To select ring without
  enabling the default aws-lc-rs backend, use `default-features = false, features = ["security-ring", "compression"]`.
- Default trust roots come from `webpki-roots`; configure private or enterprise CAs with
  `SecurityConfig::with_ca_cert`.
- Disable all default features:

```toml
rustfs-kafka = { version = "1.3.1", default-features = false }
```

- Default builds enable gzip, snappy, lz4, and zstd record batch codecs through
  `compression`.
- Smaller builds can opt into individual codecs:

```toml
rustfs-kafka = { version = "1.3.1", default-features = false, features = ["security", "gzip"] }
```

## 6. Integration Testing and Protocol Benchmarks

The repository includes Docker-based integration tests:

The default compression matrix covers NONE, SNAPPY, GZIP, LZ4, and ZSTD through the crate's `compression` feature.

```bash
cd crates/rustfs-kafka/tests
./run-all-tests
```

Examples:

```bash
./run-all-tests 4.2.0
SECURES=secure ./run-all-tests 3.9.2
COMPRESSIONS=NONE:SNAPPY:GZIP:LZ4:ZSTD ./run-all-tests 3.9.2:4.1.2:4.2.0
```

Async secure SASL acceptance checks:

```bash
./run-sync-secure-tests
./run-async-secure-tests
```

Run the generated protocol codec benchmarks in release mode:

```bash
cargo bench -p rustfs-kafka --bench protocol_serialization
```

These fixed-input benchmarks measure generated Produce frame encoding and multi-batch Fetch decoding. They
provide codec timings and throughput, not private adapter allocation costs or end-to-end broker throughput.

Compare sequential acknowledged sends with native batching against an existing plaintext topic:

```bash
cargo run -p rustfs-kafka-async --release --example batch-throughput -- localhost:9092 kafka-rust-test 128 16
```

The example sends 1 KiB values to partition 0 with `acks=all`, warms metadata and IO, and measures A1-B1-B2-A2.
It reports no speedup conclusion when sequential baseline drift exceeds 15 percent.
