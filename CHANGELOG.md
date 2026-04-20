# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **rustls 0.23 TLS backend (Default)**:
    - Replaced OpenSSL with `rustls` 0.23 as the default TLS implementation.
    - Enabled `prefer-post-quantum` feature by default for future-proof security.
    - Enabled `tls12` feature for broad compatibility (TLS 1.2 and 1.3 supported).
- **Flexible Crypto Providers**:
    - `security-rustls-default`: Uses `aws-lc-rs` (via `rustls/aws-lc-rs`).
    - `security-rustls-ring`: Uses `ring` (via `rustls/ring`).
- **Certificate Handling**:
    - Integrated `rustls-pki-types` with PEM support for robust certificate parsing.
    - Integrated `rustls-native-certs` (v0.8) for loading system certificates.
    - Integrated `webpki-roots` (v1.0) as the fallback root certificate store.

### Changed

- **BREAKING (Minor)**: The `security` feature now maps to `security-rustls-default` instead of directly depending on
  OpenSSL.
    - Default TLS backend is now rustls (pure Rust) with `aws-lc-rs`
    - Existing code using `SecurityConfig::new()` will automatically use rustls
    - No API changes required for most users
- **Updated Dependencies**:
    - `rustls`: 0.21 → 0.23 (latest stable)
    - `webpki-roots`: 0.25 → 1.0
    - `rand`: 0.8 → 0.9
    - `rustls-native-certs`: 0.6 → 0.8
    - Replaced `rustls-pemfile` with `rustls-pki-types` (feature `pem`) for type definitions and parsing.
    - Replaced `lazy_static` with `std::sync::LazyLock` (requires Rust 1.80+).
    - Replaced `crc` with `crc-fast` for improved performance.

### Deprecated

- **OpenSSL backend**: The OpenSSL-based TLS implementation is now deprecated and will be removed in the next major
  version.
    - Use `security-openssl` feature flag to continue using OpenSSL temporarily
    - Migrate to rustls by using the default `security` feature
    - See `examples/example-rustls.rs` for rustls usage examples

### Migration Guide

For most users, no changes are required. The default build will now use rustls instead of OpenSSL.

If you explicitly need OpenSSL (not recommended):

```toml
kafka = { version = "0.11", default-features = false, features = ["snappy", "gzip", "security-openssl"] }
```

To use rustls explicitly (recommended):

```toml
kafka = { version = "0.11", features = ["security"] }
# or
kafka = { version = "0.11", features = ["security-rustls-default"] }
```

To use rustls with `ring` crypto provider:

```toml
kafka = { version = "0.11", default-features = false, features = ["snappy", "gzip", "security-rustls-ring"] }
```

**Migrating from `rustls-pemfile` to `rustls-pki-types`**:
Internal implementation now uses `rustls-pki-types` for PEM parsing. If you were using `rustls-pemfile` directly in
conjunction with this crate's internals, please update to `rustls-pki-types`.

Benefits of migrating to rustls:

- No native OpenSSL dependencies required
- Better cross-compilation support
- Consistent TLS behavior across platforms
- Modern TLS 1.2+ only
- Pure Rust implementation

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
