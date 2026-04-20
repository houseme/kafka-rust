# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
rustfs-kafka = "0.20"
```

To use `ring` instead of `aws-lc-rs`:

```toml
rustfs-kafka = { version = "0.20", default-features = false, features = ["snappy", "gzip", "security-ring"] }
```

To build without TLS:

```toml
rustfs-kafka = { version = "0.20", default-features = false, features = ["snappy", "gzip"] }
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
