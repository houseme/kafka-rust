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

## Documentation

- API docs: [docs.rs/rustfs-kafka](https://docs.rs/rustfs-kafka/)
- Workspace docs index: [docs/README.md](docs/README.md)
- Usage guide (sync + async): [docs/usage-guide.md](docs/usage-guide.md)
- Async crate readme: [crates/rustfs-kafka-async/README.md](crates/rustfs-kafka-async/README.md)

## Local Development

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
