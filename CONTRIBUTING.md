# Contributing to rustfs-kafka

Thanks for your interest in contributing to rustfs-kafka!

## Development Environment

### Prerequisites

- Rust 1.93.0+
- Kafka (for integration tests; Docker recommended)

### Build

```bash
cargo build
```

### Test

```bash
# Unit tests
cargo test

# Integration tests (requires running Kafka)
cargo test --features integration_tests

# All feature combinations
cargo test --all-features

# Property-based tests (more iterations)
PROPTEST_CASES=10000 cargo test
```

### Code Quality

```bash
cargo clippy --all-features -- -D warnings
cargo fmt --check
```

### Benchmarks

```bash
cargo bench
```

## Commit Message Format

- `feat: new feature description`
- `fix: bug fix description`
- `refactor: refactoring description`
- `docs: documentation update`
- `test: test-related changes`
- `chore: build/tooling changes`

## Pull Request Requirements

- All CI checks pass
- New code has test coverage
- Public APIs have documentation comments
- No new clippy warnings
- `cargo fmt` applied

## Code Style

- Follow `rustfmt.toml` configuration
- Follow `clippy` recommendations
- Use `#![deny(clippy::all)]` as the minimum standard
- Prefer `thiserror` for error types
- Use `tracing` for logging (not `println!` or `eprintln!`)

## Project Structure

```
src/
├── client/        # KafkaClient mid-level API
├── consumer/      # Consumer high-level API
├── producer/      # Producer high-level API
├── protocol/      # Kafka protocol layer
├── network/       # Connection management
├── tls/           # TLS abstraction
├── compression/   # Compression types
├── error.rs       # Error types
└── lib.rs         # Crate root
```

## Feature Flags

| Feature | Description |
|---------|-------------|
| `security` (default) | TLS support via rustls |
| `security-ring` | TLS with ring backend |
| `producer_timestamp` | Timestamp support |
| `metrics` | Structured metrics via `metrics` crate |
| `integration_tests` | Integration test support |
