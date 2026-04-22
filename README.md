# rustfs-kafka

Fork project: forked from [kafka-rust](https://github.com/kafka-rust/kafka-rust).

`rustfs-kafka` is a Rust Kafka client workspace containing:

- `rustfs-kafka`: synchronous client/producer/consumer/admin APIs.
- `rustfs-kafka-async`: async wrapper crate based on tokio.

Current release target: `1.0.0`.

## Crates

```toml
[dependencies]
rustfs-kafka = "1.0.0"
rustfs-kafka-async = "1.0.0"
```

## Core Features

- Kafka client metadata, fetch, produce, and offset commit APIs.
- High-level `Consumer` and `Producer` abstractions.
- TLS support via rustls:
  - `security` (default, aws-lc-rs provider)
  - `security-ring` (ring provider)
- Optional `metrics` support.
- Optional `producer_timestamp`.
- Integration test harness with Kafka `3.9.2`, `4.1.2`, and `4.2.0`.

## Feature Flags (`rustfs-kafka`)

| Feature | Default | Description |
|---|---|---|
| `security` | Yes | rustls + aws-lc-rs TLS backend |
| `security-ring` | No | rustls + ring TLS backend |
| `metrics` | No | metrics integration |
| `producer_timestamp` | No | producer timestamp support |
| `nightly` | No | nightly-only optimizations |
| `integration_tests` | No | integration test compilation helpers |

Note: compression codec support is provided by `kafka-protocol`; enable the needed codec features on that dependency when required.

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
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
