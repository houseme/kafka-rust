# AGENT Guide

This document describes how agents and contributors should work in this
repository.

## Workspace Overview

- `crates/rustfs-kafka`: synchronous Kafka client library.
- `crates/rustfs-kafka-async`: async wrappers based on tokio.
- `docs/`: project usage and API guides.
- `crates/rustfs-kafka/tests`: Docker-based integration test harness.

## Primary Capabilities

- Kafka metadata loading and broker communication.
- Producer APIs (single send, batch, transactional).
- Consumer APIs (poll, consume, commit, rebalance helpers).
- Group offset operations (commit/fetch offsets).
- TLS transport using rustls (`security`, `security-ring`).
- Async wrappers for producer/consumer/client (`rustfs-kafka-async`).

## Agent Rules

1. Keep API behavior backward-compatible unless explicitly introducing a breaking change.
2. Prefer minimal and reviewable patches; avoid unrelated formatting churn.
3. For user-visible behavior changes, update:
   - `README.md`
   - `docs/usage-guide.md`
   - `CHANGELOG.md`
4. Keep license metadata and files consistent with Apache-2.0.

## Validation Checklist

```bash
cargo build
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

Integration verification when touching protocol/consumer/producer paths:

```bash
cd crates/rustfs-kafka/tests
./run-all-tests
```

## Release Checklist (1.x)

1. Bump workspace version in `Cargo.toml`.
2. Confirm crate docs/readme examples use the new version.
3. Update `CHANGELOG.md`.
4. Run quality gates (`fmt`, `clippy`, tests).
5. Ensure docs in `docs/` match current behavior.
