# rustfs-kafka-async Agent Notes

Scope: async wrapper crate for `rustfs-kafka`.

## What This Crate Owns

- `AsyncKafkaClient`
- `AsyncProducer`
- `AsyncConsumer`
- async bridging and lifecycle management (channels/tasks/threads)

## Design Constraints

- Keep wrappers lightweight and focused on async ergonomics.
- Delegate protocol correctness to `rustfs-kafka`.
- Ensure graceful shutdown semantics (`close`) remain predictable.
- Preserve error mapping consistency with the sync crate.

## Validation

```bash
cargo test -p rustfs-kafka-async
cargo clippy -p rustfs-kafka-async --all-targets --all-features -- -D warnings
```

If async changes impact sync behavior assumptions, run integration checks in
`crates/rustfs-kafka/tests`.

## Docs Sync

When async APIs change:

- update `crates/rustfs-kafka-async/README.md`,
- update `docs/usage-guide.md`,
- add a changelog entry in `CHANGELOG.md`.
