# rustfs-kafka

[![Rust](https://github.com/houseme/kafka-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/houseme/kafka-rust/actions/workflows/rust.yml)
[![crates.io](https://img.shields.io/crates/v/rustfs-kafka.svg)](https://crates.io/crates/rustfs-kafka)
[![docs.rs](https://docs.rs/rustfs-kafka/badge.svg)](https://docs.rs/rustfs-kafka/)
[![License](https://img.shields.io/crates/l/rustfs-kafka)](LICENSE)
[![Crates.io](https://img.shields.io/crates/d/rustfs-kafka)](https://crates.io/crates/rustfs-kafka)

Fork project: forked from [kafka-rust](https://github.com/kafka-rust/kafka-rust).

`rustfs-kafka` is a Rust Kafka client workspace containing:

- `rustfs-kafka`: synchronous client/producer/consumer/admin APIs.
- `rustfs-kafka-async`: async wrapper crate based on tokio.

Current release target: `1.3.1`.

## Crates

```toml
[dependencies]
rustfs-kafka = "1.3.1"
rustfs-kafka-async = "1.3.1"
```

## Core Features

- Kafka client metadata, fetch, produce, offset commit, committed-offset deletion, API version, cluster/config
  inspection/mutation, config-resource discovery, topic partition discovery, ACL inspection/mutation, delegation token
  lifecycle, client quota inspection/mutation, SCRAM credential mutation, broker log directory reassignment, KRaft
  quorum/feature/broker lifecycle/voter admin, replica directory assignment, partition reassignment query/mutation,
  partition expansion, record deletion,
  leader election, leader-epoch offsets, active producer, transaction offset commit, consumer group deletion/inspection,
  and share group inspection/mutation APIs.
- Typed raw `kafka-protocol` request support for advanced generated protocol messages that do not
  have a stable high-level client workflow.
- Runtime building blocks for telemetry subscription tracking and share-consumer request composition.
- High-level `Consumer` and `Producer` abstractions.
- TLS support via rustls:
    - `security` (default, aws-lc-rs provider, `webpki-roots` trust store)
    - `security-ring` (ring provider, `webpki-roots` trust store)
- Custom/private CAs are supported through `SecurityConfig::with_ca_cert`; system native root stores are not loaded by
  default.
- Async security authentication support includes SASL `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512` over TLS.
- Optional `metrics` support.
- Optional `producer_timestamp`.
- Integration test harness with Kafka `3.9.2`, `4.1.2`, and `4.2.0`.

## Feature Flags (`rustfs-kafka`)

| Feature              | Default | Description                          |
|----------------------|---------|--------------------------------------|
| `security`           | Yes     | rustls + aws-lc-rs TLS backend       |
| `security-ring`      | No      | rustls + ring TLS backend            |
| `compression`        | Yes     | gzip, snappy, lz4, and zstd codecs   |
| `gzip`               | Yes     | gzip record batch codec              |
| `snappy`             | Yes     | snappy record batch codec            |
| `lz4`                | Yes     | lz4 record batch codec               |
| `zstd`               | Yes     | zstd record batch codec              |
| `metrics`            | No      | metrics integration                  |
| `producer_timestamp` | No      | producer timestamp support           |
| `nightly`            | No      | nightly-only optimizations           |
| `integration_tests`  | No      | integration test compilation helpers |

Default builds include Kafka record batch compression support. For smaller builds, disable default features and enable
only the codecs you need, for example `features = ["security", "gzip"]`.

## Runtime Behavior

- Sync request frames are written completely and flushed before waiting for a response.
- Fetch responses decode every record batch in each partition; Produce batches use contiguous relative offsets.
- Sync consumers honor `pause`/`resume` in regular and retry fetches. Committed offsets at the earliest retained
  position remain valid. Failed polls preserve fetch progress and outstanding retries.
- Async consumers initialize from committed offsets, then use the configured fallback for partitions without a
  committed position. A failed or cancelled multi-broker poll does not advance progress for unreturned messages.
- Sticky partitioning maintains separate state per topic and reselects when a partition becomes unavailable.
- Producer builders preserve TLS, client ID, and other configuration when a custom partitioner is selected.
- Async producers borrow cached partition routes. A broker leader error invalidates the affected topic route;
  the next send refreshes metadata and the failed send returns its original error.
- `AsyncProducer::send_all` batches records by broker, topic, and partition, sending one Produce request per broker.
  It preserves partition order and supports the same compression and acknowledgement settings as `send`.
- Batch producers retain unconfirmed records after failed flushes and retire only uniquely confirmed partitions.
  Further `send` calls require an explicit `flush` or `clear` while a failed batch remains pending.
- Async typed requests validate correlation and consume the complete response. Interrupted or failed connections
  are discarded and reconnected on the next checkout; Produce errors are returned without automatic replay.
- `list_offsets` preserves broker timestamps, and sync connection selection reconnects only the selected broker.
- Sync typed responses validate the pending correlation, requested version, and complete payload before reuse.
  Fetching an unknown topic or partition returns an error before sending any broker request.
- With `producer_timestamp` enabled, sync `CreateTime` timestamps use the current Unix milliseconds once per
  Produce call. The default retains zero timestamps. Configure `LogAppendTime` on the broker; selecting it on
  a sync producer returns a configuration error before network activity.
- Retry backoff caps overflow at the configured maximum and rejects non-finite or nonpositive multipliers.
- Async Fetch shares the sync decoder, consuming every batch and rejecting corrupt tails before publishing offsets.
  Coordinator errors during offset initialization invalidate the cached coordinator and use bounded rediscovery.
- Sync Produce prepares all broker frames before IO and maps confirmations directly. Negative offsets in successful
  ACKs become failed confirmations, preserving those records in buffered producers.
- Admin mutations return errors after a sending attempt without automatic replay; explicitly read-only APIs retain
  broker failover. Response buffer reservations fail with errors and retire incomplete connections.

`TransactionalProducer` uses a separate transaction coordinator and sends transaction IDs, producer identities,
epochs, and sequences in transactional batches. Commit and abort preserve sequence state. A transaction RPC failure
blocks further reuse; construct a new producer instead of retrying that instance. Transactional consumer-offset
workflows and read-committed high-level consumers remain follow-up work. `GroupCoordinator` discovers coordinators
and uses each member's subscription for assignments; callers schedule its manual `heartbeat` method. Automatic
heartbeat and rebalance scheduling remain follow-up work. Further lifecycle work is tracked in
[rustfs/backlog#2713](https://github.com/rustfs/backlog/issues/2713).

## Documentation

- API docs: [docs.rs/rustfs-kafka](https://docs.rs/rustfs-kafka/)
- Workspace docs index: [docs/README.md](docs/README.md)
- Usage guide (sync + async): [docs/usage-guide.md](docs/usage-guide.md)
- Async crate readme: [crates/rustfs-kafka-async/README.md](crates/rustfs-kafka-async/README.md)

## Local Development

The child crate manifests omit explicit `rust-version` metadata. The workspace retains its compiler reference
alongside the Rust 2024 edition and dependency declarations in `Cargo.toml`.

```bash
cargo build
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

Integration tests (Docker required):

```bash
cd crates/rustfs-kafka/tests
./run-all-tests
./run-sync-secure-tests
./run-async-secure-tests
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
