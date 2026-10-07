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
`AsyncKafkaClient::send_raw_protocol_request` is available for advanced
typed access to generated `kafka-protocol` request/response pairs.
Telemetry and share-consumer session helper types are re-exported from the
sync crate for native async callers that compose those low-level requests.

Security flow supports TLS and SASL authentication in native async mode,
including `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`.

## Installation

```toml
[dependencies]
rustfs-kafka-async = "1.4.0"
```

Default builds include gzip, snappy, lz4, and zstd record batch codecs. With
`default-features = false`, enable individual codec features such as `gzip` or
`zstd` when those formats are required.

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
- `AsyncProducer::send_all` batches a slice of records into one Produce request per broker, preserving per-partition
  order and the configured acknowledgement and compression settings. Cross-broker partial failure may leave earlier
  records delivered; retrying the whole batch can duplicate them.
- Cached producer routes are borrowed; a leader error clears the affected topic route so the next send refreshes
  metadata. The failed send returns its broker error without an automatic resend.
- Native batches own each distinct broker host once and share the encoded client ID across request headers.
- Consumers start from committed group offsets, using the configured fallback only when a partition has no
  committed offset. Metadata refreshes retain existing positions.
- Failed or cancelled multi-broker polls do not advance offsets for messages that were not returned. A successful
  poll advances in-memory progress; commit after application processing succeeds.
- Fetch decoding shares the sync implementation, consumes all record batches, and rejects corrupt or unsupported
  tails before offset publication. Poll grouping borrows cached host/topic names and avoids an intermediate vector.
- Offset initialization retries coordinator migration/loading errors within the configured limit and invalidates
  the coordinator cache after the final failed attempt, preserving offsets and pending commits.
- High-level Fetch validates all requested partitions and message offsets, filters older batch prefixes, and stores
  each topic key once per progress map. Use `MessageSets::iter_ref` for borrowed message views.
- OffsetFetch/Commit success responses require complete unique partition acknowledgements. Malformed responses
  preserve progress and pending commits; only an explicit `-1` committed offset selects the fallback.
- Duplicate header keys are rejected before locking, routing, or IO because the current codec cannot preserve their
  ordered values. Unique headers keep their values and order.
- Native startup queries unresolved fallback offsets once per leader broker across topics. All lookups succeed before
  starting positions are published; failed or cancelled initialization preserves progress and pending commits.
- An unknown fallback offset is not a concrete starting position: poll returns `OffsetOutOfRange`, and a later explicit
  poll can query again. Routing snapshots are checked before publication; coordinator transport failures and exhausted
  coordinator errors allow rediscovery, while leader failures trigger metadata refresh without clearing group state.
- Typed responses validate correlation IDs and full payload consumption. Connections interrupted by cancellation
  or transport failure are discarded on their next checkout. After sending begins, bootstrap failover is limited to
  explicitly read-only APIs; mutations and unknown raw typed requests are returned without automatic replay.
- Raw `send`/`read_exact` protect individual IO operations; callers own protocol boundaries between separate calls.
- Native Fetch hides COMMIT/ABORT control markers, retains transactional business records in read-uncommitted mode,
  and advances through verified empty or compacted batches. The batch cursor is independent of the high watermark.
- Async Produce prepares every complete broker frame before sending any Produce request. ACK bookkeeping retains
  target identities, and completed writes release their frame before waiting for confirmations.
- Exact reads fill reserved storage directly without zero-initializing the whole response, and stop at the requested
  length so adjacent frames remain available to the following read.
- Secure integration coverage includes Docker end-to-end checks for SASL `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`.
- For full feature details, consult the root crate docs and `docs/usage-guide.md` in the repository.
