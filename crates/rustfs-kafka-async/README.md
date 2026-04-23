# rustfs-kafka-async

[![Rust](https://github.com/houseme/kafka-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/houseme/kafka-rust/actions/workflows/rust.yml)
[![crates.io](https://img.shields.io/crates/v/rustfs-kafka-async.svg)](https://crates.io/crates/rustfs-kafka-async)
[![docs.rs](https://docs.rs/rustfs-kafka-async/badge.svg)](https://docs.rs/rustfs-kafka-async/)
[![License](https://img.shields.io/crates/l/rustfs-kafka-async)](../../LICENSE)
[![Crates.io](https://img.shields.io/crates/d/rustfs-kafka-async)](https://crates.io/crates/rustfs-kafka-async)

Async wrappers for `rustfs-kafka`, built on top of `tokio`.

This crate provides:

- `AsyncKafkaClient`
- `AsyncProducer`
- `AsyncProducerBuilder`
- `AsyncConsumer`
- `AsyncConsumerBuilder`

The current implementation uses native tokio async I/O for metadata,
produce/fetch, and commit request paths.

Security flow supports TLS and SASL authentication in native async mode,
including `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`.

## Installation

```toml
[dependencies]
rustfs-kafka-async = "1.2.0"
```

## Quick Example

```rust,no_run
use rustfs_kafka::producer::Record;
use rustfs_kafka_async::AsyncProducer;

#[tokio::main]
async fn main() -> rustfs_kafka::Result<()> {
    let producer = AsyncProducer::builder(vec!["localhost:9092".to_owned()])
        .with_client_id("example-async-producer".to_owned())
        .build()
        .await?;
    producer.send(&Record::from_value("my-topic", b"hello async")).await?;
    producer.flush().await?;
    producer.close().await?;
    Ok(())
}
```

## Notes

- This crate is intentionally lightweight and reuses protocol data structures from `rustfs-kafka`.
- Native async producer path supports metadata-backed auto partition resolution.
- Secure integration coverage includes Docker end-to-end checks for SASL `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`.
- For full feature details, consult the root crate docs and `docs/usage-guide.md` in the repository.
