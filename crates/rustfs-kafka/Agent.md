# rustfs-kafka Agent Notes

Scope: synchronous core crate.

## What This Crate Owns

- `client/`: KafkaClient and low-level request/response workflows.
- `consumer/`: high-level consumer state machine and offset commit flow.
- `producer/`: high-level producer APIs (including batch/transactional).
- `protocol/`: Kafka protocol building/parsing via `kafka-protocol`.
- `network/`: connection and pooling logic.
- `tls/`: rustls connector paths.

## High-Risk Change Areas

- Consumer offset semantics (`consume_messageset`, `commit_consumed`).
- Protocol version negotiation and request encoding.
- Reconnection/retry behavior in network and client layers.
- Security mode configuration (`security` / `security-ring` feature paths).

## Required Test Coverage for Risky Changes

```bash
cargo test -p rustfs-kafka
cargo clippy -p rustfs-kafka --all-targets --all-features -- -D warnings
cd tests && ./run-all-tests 3.9.2:4.1.2:4.2.0
```

If a change is specific to secure paths, include secure matrix validation.

## Documentation Sync

When behavior changes:

- update root `README.md` if end-user visible,
- update `docs/usage-guide.md` and/or `docs/api-guide.md`,
- record in `CHANGELOG.md`.
