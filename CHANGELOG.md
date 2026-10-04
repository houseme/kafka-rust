# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added MessageSets::iter_ref and Consumer::consume_messageset_ref for borrowed topic/message views while preserving owned iteration. Sync consumers filter batch prefixes before the requested position, validate offset bounds before progress publication or commit IO, and build fetch requests lazily; added seek/consume coverage to the Kafka and secure transport matrices.
- Added a release batching-throughput example with identical payload and acknowledgement settings, explicit warmup, ABBA ordering, and a baseline-drift gate.
- Added AsyncProducer::send_all with one Produce request per broker, partition-order preservation, complete ACK validation, and no redundant key/value pre-copying.
- Added fixed-input generated Produce encoding and multi-batch Fetch decoding benchmarks with byte and record throughput reporting.

### Changed

- Batch producers cache each partition payload total and use it when retiring unique successful confirmations, avoiding repeated record scans. Checked record/byte counters and post-partitioner header validation reject local errors before buffer mutation; empty acknowledgement wrappers remain malformed while confirmed records retire normally.
- Documented batched atomic startup, raw and high-level unknown-offset handling, ByTime fallback, consumed-marker reset, checked routing and bounded partial metadata probing, coordinator recovery, and the successful header guard CPU benchmark scope.
- Header preflight uses at most six borrowed comparisons for two to four keys and retains hashing for larger inputs, with the same errors, key equality, values, and order. Added an ignored release CPU benchmark of the actual successful guard with fixed inputs and ABBA measurement instructions.
- Documented consumer prefix filtering, offset acknowledgement and sentinel contracts, borrowed message views, atomic metadata replacement and coordinator endpoint updates, producer input limits, and the scope of consumer progress CPU measurements.
- Documented complete async Fetch decoding, coordinator recovery, admin delivery decisions, safe response reservation, Produce preflight memory and metrics, and manual group scheduling with automatic lifecycle work still tracked in backlog.
- Sync Produce borrows broker keys, groups adjacent record targets, maps ACKs directly, and pre-encodes all broker frames before IO. Negative successful offsets become failed confirmations, and metrics count input value bytes and records once per topic; preflight can retain more multi-broker wire data at once.
- Documented sync response validation, Fetch input errors, timestamp inheritance and broker policy, retry edge cases, buffer and route allocation changes, and bounded performance measurement gates.
- The batching example requires three consecutive reference warmup comparisons within 10 percent, caps warmup at twelve windows, and retains the 15 percent ABBA baseline-drift gate.
- Batch producers store topic names once across partitions and records. Unexpected confirmations return a codec error while uniquely confirmed requested partitions retire; ordering, retained failures, counters, and explicit retry guards are preserved.
- Async Produce routing borrows cached broker hosts and owns each distinct batch host once; request headers share the encoded client ID while retaining broker, partition, and record order.
- Async exact reads fill safely reserved storage directly, bound reads to the requested length, and preserve cancellation and EOF recovery. Cancelled-socket tests accept Linux reset semantics while verifying fresh connections.
- Documented native batching, explicit batch retry decisions, connection recovery, transactional producer boundaries, and child manifest metadata; placed the sync poll correction under Unreleased.
- Sync connection checkout no longer clones all hosts or reconnects the whole pool, and first checkout avoids a second connection when idle timeout is zero.
- Removed explicit rust-version inheritance from both child crate manifests.
- Kafka integration cleanup now includes the ZooKeeper profile when switching versions or exiting the test matrix.
- Documented producer routing, consumer progress and retry behavior, TLS feature selection, benchmark scope, and current transaction and group lifecycle limitations.
- The default Kafka integration matrix now covers NONE, SNAPPY, GZIP, LZ4, and ZSTD through the crate's compression feature.

### Fixed

- Ordinary producers require complete unique resolved target acknowledgements and return codec errors instead of panicking on malformed confirmations. Invalid replies retire existing target connections without reconnecting or replay; valid partition errors, empty input, and no-ack sends retain their contracts.
- Native async consumers publish verified batch positions and pending commits only after all broker responses succeed, including safe control-only and compacted progress. Missing cached positions and malformed data return codec errors; added TCP cancellation/error coverage, secure transaction-marker recovery, and updated the actual progress CPU fixture.
- Sync consumers use verified batch cursors to progress through control-only and compacted batches without growing retry buffers or marking business messages consumed. Complete response validation and decoder-error priority precede atomic cursor/retry publication; added TCP and real transaction coverage.
- Fetch adapters filter transaction control records and retain verified batch cursors through empty and compacted batches. CRC/decompression, exact record framing, batch bounds and strict offset ordering are checked before accepting a partition; existing owned response fields remain unchanged.
- Transaction integration fixtures now require complete read-only ListOffsets readiness after metadata leaders appear. Only expected fixture-partition leader errors are retried within a bounded budget; transport, codec, authentication, and transaction write failures remain immediate failures.
- Native async routing validates metadata and FindCoordinator v3 descriptors before publishing caches. Partial snapshots keep healthy routes and probe missing routes with the configured backoff; full snapshots end probing. Leader failures force metadata rediscovery, while coordinator transport failures and final coordinator rejections invalidate coordinator state without dropping progress or changing retry budgets.
- Native async startup groups unresolved fallback ListOffsets requests by leader broker across topics, validates complete responses and offset semantics, and publishes committed/fallback positions only after all lookups succeed. Failures or cancellation retain offsets and pending commits; unknown positions return OffsetOutOfRange without an implicit latest fallback or negative Fetch.
- Sync consumers resolve ByTime fallback positions once when needed, reject unknown starting positions without sending negative Fetch offsets, and require routes only for assigned partitions. Initialization resets consumed markers that disagree with the selected cursor after all lookups succeed, allowing lower fallback progress to be consumed and committed; added TCP and live no-match coverage.
- Sync ListOffsets validates complete unique targets per broker and successful offset bounds before projecting results, retires malformed connections, deduplicates input topics, and returns unknown nonzero broker errors explicitly. Raw unknown-offset sentinels, timestamps, maximum positions, and distinct partitions across brokers are preserved.
- Fresh-topic consumer offset tests verify empty ListOffsets readiness and retry only complete leader-error Produce confirmations within a bounded budget, checking an empty log before repeating the batch. Transport and other errors fail immediately, retaining single-append and no-redelivery assertions.
- Async consumers validate complete unique Fetch/OffsetFetch/OffsetCommit responses and numeric offsets before filtering batch prefixes or publishing progress. Malformed responses retain offsets and pending commits; topic-grouped progress maps and bounded dense request matching reduce repeated lookup work. Added secure within-batch committed-offset recovery and an ignored CPU microbenchmark of the production validation/publication path.
- Sync OffsetFetch and OffsetCommit require unique, complete requested topic/partition acknowledgements, reject duplicate input targets before IO, and retire malformed connections without replaying commits. OffsetFetch processes valid top-level errors first and rejects successful committed offsets below the explicit unset sentinel -1.
- Sync, async, batch, and transactional producers reject duplicate header keys before routing or IO because the current map-based record codec cannot preserve them. Added reusable header validation; unique headers retain their order and values, and local rejection preserves buffered obligations and active transaction state.
- Ordinary producer creation rejects unsupported idempotence and transaction IDs before metadata or connection IO; callers can use the dedicated transactional producer for the implemented transaction flow.
- Metadata refresh validates broker descriptors and partition identities before publishing; failed full refreshes retain the previous snapshot, successful replacement clears dependent coordinator caches, and rediscovered endpoints update existing broker slots. Removed unused private response headers and replica/ISR copies while preserving public metadata views.
- GroupCoordinator now discovers fresh coordinators, encodes real subscriptions, assigns the union of member topics by member ID, validates metadata bounds and complete known schemas, and invalidates coordinator errors for the next manual call. Leave failures preserve membership; removed the idle logging worker while retaining constructor compatibility and manual heartbeat semantics.
- Sync consumers return codec errors for unknown cached response topics or partitions, including OffsetOutOfRange responses, without panicking or publishing staged fetch and retry changes.
- Async consumers share the complete, panic-safe Fetch converter, reject malformed trailing batches before publishing progress, borrow poll routing keys, and rediscover coordinators after bounded offset-initialization retries. Added TLS/SASL multi-batch delivery and committed-offset recovery coverage.
- Admin mutations and unclassified APIs return uncertain errors after a sending attempt without automatic replay. Read-only queries retain IO failover, unreachable bootstrap brokers remain selectable, and frames are encoded before connecting without cloning all hosts.
- Sync response buffers now check size conversion and reserve fallibly before exact reads; allocation or read failures close the connection and clear pending response context. SASL uses the same safe helper.
- Spelling checks now recognize Kafka's LSO terminology used by the transaction isolation tests.
- Exponential retry safely caps large attempts and duration overflow, rejects non-finite or nonpositive multipliers, and supports zero or shrinking delays; its documentation now describes deterministic backoff.
- Sync CreateTime uses one current Unix timestamp per Produce call, including compressed and transactional batches, while None retains zero. Producer constructors and active transactions reject client-side LogAppendTime before metadata or transaction IO; the integration matrix now checks CreateTime with ByTime queries.
- Sync typed responses now validate pending correlation IDs, request versions, and complete payloads, closing invalid connections. Removed speculative Fetch/FindCoordinator decoding, corrected Group API keys and generated v1 layouts, validated SASL replies, rejected unknown Fetch inputs before IO, and corrected per-topic Fetch metrics.
- Sync consumers publish fetched offsets, retry buffer changes, and retry queue updates only after every partition response succeeds; failed or incomplete retry fetches keep their pending retry.
- Transactional producers now use generated transaction codecs, discover transaction coordinators, send producer identity and sequence context, preserve sequences across commit and abort, and block unsafe reuse after RPC failures. Added real-broker isolation and fencing coverage.
- Async typed requests now track pending correlations, reject mismatched or trailing response data, retire failed or cancelled connections, and avoid replaying low-level Produce after a sending attempt.
- Batch producers stream buffered records into encoding, retain failed or ambiguous deliveries, retire only uniquely confirmed partitions, and require explicit flush or clear before accepting more records after failure.
- ListOffsets now preserves broker timestamps and maps directly to the existing public offset types without intermediate per-partition vectors.
- Async polls now propagate codec and unsupported-compression partition errors before delivering messages or advancing offsets, while preserving Kafka error retry classification.
- The standalone `security-ring` feature now enables TLS and SASL modules, public configuration APIs, and secure builder paths without requiring `security`.
- Async producers borrow cached partition routes and invalidate topic metadata after leader errors without automatically resending failed records.
- Async consumers restore committed or fallback starting offsets, propagate OffsetFetch top-level errors, and advance progress only after every broker response succeeds.
- Producer and batch producer builders retain TLS and client ID configuration when selecting a custom partitioner; the regular producer also retains its timestamp setting.
- Sticky partitioning now maintains per-topic state, reselects unavailable partitions, and avoids per-message mutex locking.
- Sync consumers honor pause/resume in normal and retry fetches, accept committed offsets at the earliest retained boundary, and cap retry buffer growth without overflow.
- OffsetFetch top-level errors now participate in retry and coordinator rediscovery; unavailable coordinator connections return an error instead of panicking.
- Fetch decoding now consumes every batch, including empty compacted batches, rejects corrupt tails, and moves decoded payloads without redundant Bytes clones.
- Produce record batches now use contiguous relative offsets while preserving a single non-idempotent batch per partition.
- Sync transport now completes partial writes, flushes request frames, and terminates connections on write or flush errors.
- Updated test assertions to satisfy Rust 1.99 Clippy and improve failure diagnostics.

## [1.3.1] - 2026-09-04

### Added

- Added `KafkaClient::resolved_api_version` and `AsyncKafkaClient::resolved_api_version`
  to expose the effective cached broker API version selected for a host and API key.
- Added GitHub Actions release automation for tagged/manual releases, crates.io publishing,
  GitHub Release creation, and dry-run validation.
- Added a repository-local release skill documenting the kafka-rust release workflow.

### Changed

- High-level sync request paths now clamp metadata, fetch, produce, offset, and admin
  protocol versions to cached broker `ApiVersions` ranges when available.
- Async typed protocol helpers now cache `ApiVersions` responses and clamp high-level
  generated request/response codecs through the same broker version resolver while
  preserving caller-selected versions for raw protocol requests.
- Added `CreateTopics` and `DeleteTopics` to API version constant, fallback, and resolver
  coverage.

### Fixed

- Fixed crate package readme paths so published packages use the intended crate readmes.

## [1.3.0] - 2026-09-04

### Added

- Added read-only Kafka management APIs on `KafkaClient` using `kafka-protocol` generated messages:
  - `fetch_api_versions`
  - `describe_cluster` / `describe_cluster_with_options`
  - `describe_acls` / `describe_acls_with_filter`
  - `create_acls`
  - `delete_acls`
  - `describe_configs` / `describe_configs_with_options`
  - `incremental_alter_configs`
  - `list_config_resources` / `list_config_resources_for`
  - `describe_delegation_tokens` / `describe_delegation_tokens_for`
  - `describe_log_dirs` / `describe_log_dirs_for`
  - `describe_quorum`
  - `create_partitions` / `create_partitions_with_options`
  - `delete_records`
  - `elect_leaders` / `elect_preferred_leaders` / `elect_unclean_leaders`
  - `alter_partition_reassignments`
  - `list_partition_reassignments` / `list_partition_reassignments_for`
  - `offsets_for_leader_epochs`
  - `describe_client_quotas` / `describe_client_quotas_with_options`
  - `alter_client_quotas`
  - `describe_user_scram_credentials` / `describe_user_scram_credentials_for`
  - `alter_user_scram_credentials`
  - `describe_producers`
  - `list_transactions` / `list_transactions_with_options`
  - `describe_transactions`
  - `add_offsets_to_txn`
  - `txn_offset_commit`
  - `describe_topic_partitions` / `describe_topic_partitions_with_options`
  - `describe_consumer_groups` / `describe_consumer_groups_with_options`
  - `describe_share_groups` / `describe_share_groups_with_options`
  - `describe_share_group_offsets` / `describe_share_group_offsets_with_options`
  - `alter_share_group_offsets`
  - `delete_share_group_offsets`
  - `alter_replica_log_dirs`
  - `update_features`
  - `unregister_broker`
  - `assign_replicas_to_dirs`
  - `add_raft_voter` / `remove_raft_voter` / `update_raft_voter`
  - `list_groups` / `list_groups_with_filters`
  - `delete_groups`
  - `describe_groups` / `describe_groups_with_options`
  - `delete_group_offsets`
  - `get_telemetry_subscriptions`
  - `push_telemetry`
  - `consumer_group_heartbeat`
  - `share_group_heartbeat`
  - `share_fetch`
  - `share_acknowledge`
- Added `KafkaClient::send_raw_protocol_request` for advanced typed access to generated
  `kafka-protocol` requests that are not represented by stable high-level client APIs.
- Added `AsyncKafkaClient::send_raw_protocol_request` to provide the same typed raw protocol escape
  hatch for native tokio clients.
- Added API version defaults and `api_key` constants for remaining generated broker, controller,
  coordinator, raft, and share-state protocol messages.
- Added `TelemetrySession` for tracking broker telemetry subscriptions and building compatible
  `PushTelemetryOptions`.
- Added `ShareConsumerSession` and `ShareFetchSessionConfig` for composing share-consumer
  heartbeat, fetch, and acknowledgement calls from coordinator assignment state.
- Added public response data types for broker API versions, cluster brokers, config resources/entries, config resource
  discovery, listed groups, deleted groups, described groups, described group members, ACL resources, ACL mutation
  results, config mutation results, delegation tokens, log directory diagnostics, KRaft quorum state, topic partition
  discovery, partition reassignments, partition reassignment mutations, partition expansion, record deletion,
  leader election, leader-epoch offset lookup, client quotas, client quota mutation results, SCRAM credential metadata,
  SCRAM credential mutation results, active producers, transactions, transactional offset commit results,
  committed-offset deletion results, modern consumer group descriptions, share group descriptions, share group offsets,
  share group offset mutation results, low-level modern consumer/share-consumer protocol results, cluster feature
  updates, KRaft broker lifecycle/voter operations, replica directory assignment results, and low-level telemetry
  subscription/push results.
- `rustfs-kafka-async` now re-exports the sync crate's public admin and diagnostic data types for convenience.
- Added `docs/protocol-coverage.md` to track `kafka-protocol` `0.18.0` API coverage and prioritize remaining
  client-facing protocol work.

### Changed

- Upgraded `kafka-protocol` from `0.17.0` to `0.18.0` and updated Produce record construction for the new
  `delete_horizon` record field.
- Default TLS root loading now uses the bundled `webpki-roots` set plus explicit `ca_cert_path` configuration only.
  `rustls-native-certs` and its platform-specific transitive crates are no longer dependencies.
- `rustfs-kafka-async` now re-exports `TlsConfig` alongside `SecurityConfig` for easier async TLS configuration.
- Sync TLS stream implementations now require `Sync` in addition to `Send`, allowing TLS-backed sync
  connections to satisfy thread-sharing bounds in multi-threaded callers.
- Bumped workspace dependencies:
  - `rustls`: `0.23.39` -> `0.23.40`
  - `metrics`: `0.24.3` -> `0.24.6`
  - `tokio`: `1.52.1` -> `1.52.3`
  - `ctor`: `0.10.1` -> `1.0.6`
- Updated CI benchmark workflow artifact upload action:
  - `actions/upload-artifact`: `v4` -> `v6`
- Centralized generated Kafka request framing on a single `compute_size`-based buffer path shared by
  sync transport, SASL, admin helpers, and async wire helpers.
- Optimized async SCRAM salted-password derivation with `pbkdf2_hmac_array`.
- Declared read-only workflow token permissions and updated checkout actions:
  - `actions/checkout`: `v6` -> `v7`

### Fixed

- Updated integration test ctor annotation to match `ctor` `1.x` requirements:
  - `#[ctor::ctor]` -> `#[ctor::ctor(unsafe)]` in `crates/rustfs-kafka/tests/test_kafka.rs`

### Deprecated

- Deprecated `KafkaClient::alter_configs`; use `KafkaClient::incremental_alter_configs` for new config mutations.
- Deprecated async builder compatibility toggles that are ignored by the native async implementation:
  - `AsyncProducerBuilder::with_channel_capacity`
  - `AsyncProducerBuilder::with_native_async`
  - `AsyncConsumerBuilder::with_channel_capacity`
  - `AsyncConsumerBuilder::with_native_async`

## [1.2.0] - 2026-04-23

### Added

- Added native sync SASL support in `rustfs-kafka` for:
  - `PLAIN`
  - `SCRAM-SHA-256`
  - `SCRAM-SHA-512`
- Added native async SASL support in `rustfs-kafka-async` for:
  - `PLAIN`
  - `SCRAM-SHA-256`
  - `SCRAM-SHA-512`
- Added SCRAM challenge/response loop handling in native async auth flow, including server challenge parsing and final server-signature verification.
- Added Docker secure integration acceptance script:
  - `crates/rustfs-kafka/tests/run-async-secure-tests`
  - `crates/rustfs-kafka/tests/run-sync-secure-tests`
- Added async secure integration test:
  - `crates/rustfs-kafka-async/tests/sasl_secure_integration.rs`

### Changed

- `AsyncProducer` and `AsyncConsumer` now run on native async I/O paths only; legacy bridged sync fallback behavior is removed from runtime behavior (compat flags remain no-op for API compatibility).
- `KafkaConnection` sync secure path now performs SASL handshake/authenticate before entering pooled request flow.
- Extended secure Docker test harness to support SASL mechanisms in secure profile (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`) for end-to-end validation.
- Updated workspace/crate versions and release docs to `1.2.0` (`README.md`, `docs/usage-guide.md`, `CHANGELOG.md`, `AGENT.md`, crate agent/readme docs).

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
