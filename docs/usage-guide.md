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

Native async Fetch uses the same complete record decoder as sync Fetch. Empty compacted batches do not hide later
batches; corrupt, truncated, or unsupported trailing batches fail the entire poll before progress is published.
Poll request routing borrows cached broker/topic names instead of copying them for every partition. OffsetFetch
coordinator errors (`NotCoordinatorForGroup`, `GroupCoordinatorNotAvailable`, `GroupLoadInProgress`) invalidate
the coordinator cache, retry within the configured limit, and retain offsets and uncommitted progress. An exhausted
attempt also invalidates the cache so a later poll can discover the coordinator again.

Kafka can return a complete record batch containing records before the requested position. High-level sync and
async consumers filter that prefix before delivery, so seeking or restoring a committed position inside a batch
does not redeliver older records. Every record is validated before filtering: negative or maximum `i64` message
offsets are codec errors, while `i64::MAX - 1` can advance to the valid next position `i64::MAX`. Failed polls retain
existing fetch progress and pending commits. Async progress maps store each topic key once across its partitions.

Use `message_sets.iter_ref()` to process borrowed sets without allocating topic strings or cloning message vectors.
Each view exposes `topic()`, `partition()`, and `messages()`. Sync callers can mark a view with
`consumer.consume_messageset_ref(&view)`. `view.to_owned()` produces the existing owned `MessageSet` when it must
outlive the response; `iter()` and `IntoIterator` retain their owned behavior.

`StickyPartitioner` keeps its batch state per topic and chooses another available partition if the previous one
loses its leader. Explicit record partitions remain unchanged. Calling `with_partitioner` on either sync producer
builder preserves previously selected TLS, client ID, and acknowledgement settings. The regular producer also
retains its timestamp setting, and the batch producer retains its batching settings.

Enable `producer_timestamp` and select `ProducerTimestamp::CreateTime` to include the current Unix milliseconds
in sync Produce batches. One clock sample applies to every broker and partition in that call, including
transactional batches. The default `None` retains zero timestamps. Producers constructed from a client inherit
its setting; the regular producer can override it with `with_timestamp`. `LogAppendTime` belongs to the broker's `message.timestamp.type` policy;
selecting it on a sync producer returns a configuration error before sending metadata or transaction requests.

Async producer cache hits borrow partition routes instead of copying all partition metadata. A
`NotLeaderForPartition`, `LeaderNotAvailable`, or `UnknownTopicOrPartition` response clears that topic's route.
The failed call returns the broker error; the next send reloads metadata. Application retries still need to account
for uncertain delivery after network errors.

Batch producer `flush` retains its buffer after transport failure. With acknowledgements enabled, it removes only
partitions with unique successful confirmations; failed, missing, or duplicate confirmations keep those partitions
pending. The returned confirmation list exposes broker partition errors. Automatic flush propagates them as errors.
Unexpected topic or partition confirmations return a codec error; uniquely confirmed requested partitions are
still retired. Buffered topic names are shared across their partitions and records.
An ACK with error code zero and a negative base offset becomes a failed `Unknown` partition confirmation; buffered
records for that partition remain pending while other uniquely confirmed successes retire.
While unconfirmed records remain after a failure, `send` rejects new records before enqueuing them. Explicit `flush`
retries pending records, and `clear` discards them. Earlier brokers may have accepted records before a transport
failure, so explicit retries still require an application decision about duplicates. With `acks=0`, a successful
complete write clears the batch without claiming broker confirmation.

Async typed requests retain pending correlation IDs across send/receive, reject mismatched IDs or extra response
bytes, and retire failed or cancelled connections before their next use. Raw `send` and `read_exact` preserve their
individual IO semantics; callers own protocol boundaries across separate raw operations. Raw `request_response`
protects the complete frame exchange and returns the payload for caller decoding. Low-level Produce failures after
a sending attempt do not fail over to another broker automatically.

Sync typed requests also validate pending correlation IDs, use the actual request version for decoding, and
reject trailing payload bytes. Invalid responses close the connection. Group v1 responses use generated layouts
and Kafka's API keys. A Fetch input containing an unknown topic or partition fails before any broker request;
refresh metadata before retrying it. Async exact reads fill reserved storage directly and stop at the requested
frame boundary, retaining the same cancellation and EOF recovery rules.

`list_offsets` returns the broker's timestamp, including `-1` when no timestamp is available. `fetch_offsets` keeps
its existing offset-only return type. Sync pool checkout selects the oldest connection, tries another on failure,
and updates only the chosen connection's checkout time.

`RetryPolicy::next_delay` returns no delay for a disabled retry or an invalid multiplier (non-finite or
nonpositive). Valid exponent overflow saturates at `max`; shrinking multipliers and zero delays remain
supported. The delay is deterministic and does not include jitter.

Sync Produce borrows broker grouping keys, groups adjacent records with the same target, and maps generated ACKs
directly to confirmations. Every ordinary broker request is encoded before opening a connection, so a local codec
or frame error cannot leave an earlier broker's records sent. This retains all encoded broker frames until sending
begins and can increase peak memory for a multi-broker batch. Produce metrics count input records and value bytes
once per topic for a completed transport call, including `acks=0`.

Admin requests try another bootstrap broker if connection setup fails. After a sending attempt, mutations and
unclassified API keys return uncertain delivery errors to the caller; application retries require a delivery
decision. Explicitly read-only queries retain failover. Frame encoding happens before connection setup.
Sync response buffers use checked size conversion and fallible reservation; failed allocation or exact reads
close the connection and clear pending response context. Consumer responses containing unknown cached topics or
partitions return codec errors while retaining fetch progress and pending retries.

Successful OffsetFetch and OffsetCommit replies must acknowledge every requested topic/partition exactly once.
Missing, duplicated, or extra acknowledgements return codec errors without fallback or clearing pending commits;
malformed commit acknowledgements are not automatically replayed. OffsetFetch `-1` explicitly means unset; values
below `-1` are invalid. Calling `consume_message` with a negative offset or `i64::MAX` returns a configuration error
before changing consumed state. Local invalid pending commit values are also rejected before IO.

Full metadata refresh fetches and validates the replacement before publishing it. A network or malformed snapshot
leaves existing metadata and coordinator caches available; successful replacement clears caches tied to reset broker
slots. Incremental refresh preserves those caches. FindCoordinator can update a broker's endpoint without changing
its slot, so existing references follow the reported host/port. Invalid broker descriptors or partition IDs fail
before publishing any metadata.

Ordinary `Producer` builders reject idempotence or transaction IDs at `create`; use `TransactionalProducer` for the
implemented transaction flow. Kafka supports ordered duplicate header keys, but the current record codec uses a map
and cannot preserve them. Producers now return a configuration error before routing or sending such records.
`Headers::validate_unique` and `producer::validate_unique_headers` expose the same check. Unique keys, empty values,
and key case remain unchanged; local rejection retains active transaction state and buffered delivery obligations.

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

`GroupCoordinator::join_group` discovers the coordinator for a fresh client and sends a versioned subscription.
The leader loads the union of all members' topics, preserves empty subscriptions, and associates custom assignor
results with their member IDs. Duplicate, unknown, missing, or malformed assignments fail before Sync. Followers
send an empty assignment list. Subscription and assignment decoding validates complete known schemas while
retaining compatibility with future appended fields.

Call `heartbeat` periodically and use `leave_group` to exit manually. Explicit coordinator errors invalidate the
cache and return the current error; the next manual call rediscovers the coordinator. No idle logging worker is
spawned. Constructor heartbeat/max-poll parameters remain accepted for API compatibility. Applications schedule
heartbeats and enforce their max-poll policy. Full group lifecycle, transactional
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

Run the ignored consumer progress CPU measurement separately, with no concurrent builds or broker tests:

```bash
cargo test -p rustfs-kafka-async --release --lib consumer::consumer_progress_bench::native_consumer_progress_cpu_bench -- --ignored --exact --nocapture --test-threads=1
```

This measures the production Fetch identity/offset validation, prefix filtering, staging, and progress publication
for two layouts of 1000 topic/partition pairs with populated progress maps. Setup, decoding, network IO, commit,
map reset, and result verification are outside timing. The default is 256 warmup calls and nine samples of 1024
calls; every sample verifies all published offsets. Compare revisions in separate Cargo target directories, verify
the source and executable for each revision, and retain all A1-B1-B2-A2 samples. A baseline drift above 15 percent
precludes a performance conclusion. This measurement does not establish allocation counts or broker throughput.

Compare sequential acknowledged sends with native batching against an existing plaintext topic:

```bash
cargo run -p rustfs-kafka-async --release --example batch-throughput -- localhost:9092 kafka-rust-test 128 16
```

The example sends 1 KiB values to partition 0 with `acks=all`, warms metadata and IO, and measures A1-B1-B2-A2.
Warmup is bounded to 12 reference windows and requires three consecutive adjacent windows within 10 percent.
It reports no speedup conclusion if warmup fails to stabilize or sequential baseline drift exceeds 15 percent.
