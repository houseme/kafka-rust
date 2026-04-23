# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0] - 2026-04-23

### Added

- Added `AsyncProducerBuilder` in `rustfs-kafka-async` with async `build()` and producer configuration methods.
- Added `AsyncConsumerBuilder` in `rustfs-kafka-async` with async `build()` and consumer configuration methods.

### Changed

- `AsyncProducer` now defaults to a native async I/O path (Metadata + Produce over tokio sockets) and falls back to bridged sync mode when needed.
- Native async producer now includes metadata/leader caching and auto partition selection when `Record.partition < 0`.
- Native async consumer now supports coordinator discovery and Kafka `OffsetCommit` requests (no longer native no-op commit).
- Native async consumer now initializes start offsets via group `OffsetFetch`, with fallback to configured `FetchOffset` strategy.
- Native async consumer now retries recoverable `Fetch`/`Commit` failures by refreshing leader metadata or group coordinator.
- Native async consumer retry policy is now configurable (attempts + backoff) via async builder settings.
- Native async consumer now exposes error observability: cumulative error-class/code counters and latest error snapshot.
- Native async consumer observability now also emits metrics counters on key failure paths (`total`, `by_phase`, `by_class`, `by_kafka_code`) under feature-gated `metrics`.
- Reworked `AsyncProducer` internals to run synchronous producer operations on a dedicated background thread.
- Moved async producer construction to blocking-safe setup (`spawn_blocking`) to avoid blocking the tokio scheduler.
- Moved async consumer construction to blocking-safe setup (`spawn_blocking`) to avoid blocking the tokio scheduler.
- Updated async producer/consumer shutdown paths to avoid blocking `Drop` on tokio runtime threads.
- Bumped workspace/crate versions to `1.1.0` and synced release docs (`README.md`, `crates/rustfs-kafka-async/README.md`, `docs/usage-guide.md`, `AGENT.md`).

## [1.0.0] - 2026-04-22

### Added

- Added `docs/usage-guide.md` with end-to-end usage for both `rustfs-kafka` and `rustfs-kafka-async`.
- Added agent guidance documents:
  - `AGENT.md`
  - `crates/rustfs-kafka/Agent.md`
  - `crates/rustfs-kafka-async/Agent.md`

### Changed

- Updated workspace and crate versions to `1.0.0`.
- Updated root `README.md` for the 1.0.0 release and current workspace layout.
- Updated `crates/rustfs-kafka-async/README.md` dependency examples to `1.0.0`.
- Updated `docs/README.md` to focus on tracked documentation content.
- Hardened integration tests for secure/compression matrix stability:
  - `test_consumer_commit_messageset`
  - `test_consumer_commit_messageset_no_consumes`
- Updated `CONTRIBUTING.md` with current CI and release guidance.

### Removed

- Removed `dprint.json` (no active formatter integration in CI/tooling).
- Switched from dual-license files to single Apache-2.0 project licensing.

## [0.22.0] - 2026-04-21

### Added

- **kafka-protocol integration**: Migrated from hand-written protocol implementation
  to [kafka-protocol](https://crates.io/crates/kafka-protocol) v0.17 code-generated Kafka wire protocol library.
- **Record Batch v2**: Produce requests now use Record Batch v2 format for improved compatibility with modern Kafka
  brokers.
- **ZSTD compression**: Added `zstd` feature for ZSTD compression support.
- **LZ4 native**: Added `lz4_native` feature using the `lz4` C-binding crate as an alternative to `lz4_flex`.
- **TCP Keepalive & NoDelay**: All TCP connections are configured with keepalive (10s idle, 20s interval) and
  `TCP_NODELAY` for improved broker compatibility.
- **Connection state machine**: Connections track health via `Connected`/`Terminated` states. Terminated connections (
  due to IO errors or broker restarts) are automatically reconnected on next use.
- **BrokerRequestError**: Broker request failures now include contextual information (broker host and API key name) for
  improved debugging and retry logic.
- **API version negotiation**: Client sends `ApiVersionsRequest` to each broker on first metadata request, discovering
  supported protocol version ranges.

### Changed

- **BREAKING**: `fetch::Message.key` and `fetch::Message.value` changed from `&'a [u8]` to `bytes::Bytes` (owned).
- **BREAKING**: `fetch::Topic.topic()` changed from returning `&'a str` to `&str` (owned `String`).
- **BREAKING**: `fetch::Response` removed `raw_data` field; responses are fully decoded.
- **BREAKING**: Protocol versions upgraded from v0 to v1-v4 (transparent to users but affects minimum broker
  compatibility).
- **BREAKING**: Removed `byteorder` and `crc-fast` dependencies.
- **BREAKING**: Removed `fnv` dependency (replaced by `indexmap` for producer partitioner).
- **Unified protocol module**: Merged `protocol/` and `protocol2/` into a single `protocol/` module. Data types and
  kafka-protocol adapter functions coexist in the same files.
- Removed `lz4_flex` dependency; `lz4` feature now uses the `lz4_flex` crate directly.
- Removed unused `protocol/list_offset.rs` (dead code from legacy implementation).

### Removed

- `src/codecs.rs` — Legacy serialization traits (416 lines).
- `src/protocol/fetch.rs` — Legacy fetch response parser (802 lines).
- `src/protocol/zreader.rs` — Legacy response reader (216 lines).
- `protocol/list_offset.rs` — Unused legacy types.

### Migration Guide

Update your `Cargo.toml`:

```toml
# Before
rustfs-kafka = "0.21"

# After
rustfs-kafka = "0.22"
```

Update fetch message access (`Bytes` implements `AsRef<[u8]>` and `Deref<Target=[u8]>`):

```rust
// Before
let key: & [u8] = msg.key;

// After
let key: & [u8] = & msg.key;
// or simply: &msg.key[..]
```

## [0.21.0] - 2026-04-20

### Added

- **rustls 0.23 TLS backend**:
    - `rustls` 0.23 as the TLS implementation with `aws-lc-rs` as the default crypto provider.
    - Enabled `prefer-post-quantum` feature by default for future-proof security.
    - Enabled `tls12` feature for broad compatibility (TLS 1.2 and 1.3 supported).
- **Flexible Crypto Providers**:
    - `security`: Uses `aws-lc-rs` (default).
    - `security-ring`: Uses `ring` as an alternative crypto provider.
- **Certificate Handling**:
    - Integrated `rustls-pki-types` with PEM support for robust certificate parsing.
    - Integrated `rustls-native-certs` (v0.8) for loading system certificates.
    - Integrated `webpki-roots` (v1.0) as the fallback root certificate store.

### Changed

- **BREAKING**: Crate renamed from `kafka` to `rustfs-kafka`.
    - Update all `use kafka::` to `use rustfs_kafka::` in downstream code.
    - Update `Cargo.toml` dependency from `kafka` to `rustfs-kafka`.
- **BREAKING**: Removed OpenSSL TLS backend entirely.
    - Only rustls is supported going forward.
    - Removed `security-openssl`, `security-rustls`, `security-rustls-default`, `security-rustls-ring` feature flags.
    - Simplified to `security` (aws-lc-rs) and `security-ring` (ring).
    - Removed `openssl` dependency.
- **Updated Dependencies**:
    - `rustls`: 0.21 → 0.23
    - `webpki-roots`: 0.25 → 1.0
    - `rustls-native-certs`: 0.6 → 0.8
    - Replaced `rustls-pemfile` with `rustls-pki-types` for type definitions and parsing.
    - Replaced `lazy_static` with `std::sync::LazyLock`.
    - Replaced `crc` with `crc-fast` for improved performance.

### Migration Guide

Update your `Cargo.toml`:

```toml
# Before
kafka = { version = "0.10", features = ["security"] }

# After
rustfs-kafka = "0.21"
```

To use `ring` instead of `aws-lc-rs`:

```toml
rustfs-kafka = { version = "0.21", default-features = false, features = ["security-ring"] }
```

To build without TLS:

```toml
rustfs-kafka = { version = "0.21", default-features = false }
```

Update your code imports:

```rust
// Before
use kafka::client::KafkaClient;
use kafka::consumer::Consumer;
use kafka::producer::Producer;

// After
use rustfs_kafka::client::KafkaClient;
use rustfs_kafka::consumer::Consumer;
use rustfs_kafka::producer::Producer;
```

## [0.9.0] 2022-04-29

- Updated to support Rust 2021
- Brought all of the dependencies up to date, so this could cause **breaking changes**
- Removed the try! methods
- Updated the error mechanism to use thiserror and anyhow.
- Removed error-chain, as it is deprecated
- This is a non breaking change, but I wanted to bump the version as it has been over two years since the last release.
  Contributors:
- Thank you to midnightexigent and tshepang for your contributions
- Thank you to dead10ck for your support.

## [0.8.0] 2019-09-10

- Upgrade openssl to v0.10. This may be a **breaking change** for your
  application code, since openssl v0.10 is a breaking change. Thanks to @l4l!
- Run integration tests on various configurations with compression and
  encryption.

## [0.7.0] 2017-10-17

### Fixed

- [**BREAKING**] Fixed #101. The `Consumer` was erroneously committing the offset
  of the _last consumed message_ instead of the next offset that it should read,
  which is what [the Kafka protocol specifies it should
  be](https://kafka.apache.org/documentation.html#theconsumer). This means that:

    - When you upgrade, your consumers will read the last message it consumed again.
    - The consumers will now be committing one offset past where they were before.
      If you've come to rely on this behavior in any way, you should correct it.
