# Contributing to rustfs-kafka

Thanks for your interest in contributing.

## Prerequisites

- Rust `1.93.0` or newer
- Docker (for integration tests)

## Quick Start

```bash
cargo build
cargo test
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
```

## Integration Tests

```bash
cd crates/rustfs-kafka/tests
./run-all-tests
```

Useful scoped runs:

```bash
# Specific Kafka version
./run-all-tests 4.2.0

# Secure mode only
SECURES=secure ./run-all-tests 3.9.2
```

## Pull Request Checklist

- CI passes for lint, tests, and build matrix.
- New behavior includes tests.
- Public API updates include docs/examples.
- `CHANGELOG.md` is updated for user-facing changes.

## Commit Style

Conventional prefixes are preferred:

- `feat:`
- `fix:`
- `refactor:`
- `docs:`
- `test:`
- `chore:`

## Release Notes

For release updates:

1. Update workspace version in `Cargo.toml`.
2. Update `CHANGELOG.md`.
3. Ensure `README.md` and `docs/usage-guide.md` reflect current APIs.
4. Verify license metadata and files are consistent.
