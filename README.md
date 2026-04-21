# rustfs-kafka

A Rust client for Apache Kafka, forked from [kafka-rust](https://github.com/kafka-rust/kafka-rust).

## Documentation

- This library is primarily documented through examples in its [API documentation](https://docs.rs/rustfs-kafka/).
- Documentation about Kafka itself can be found at [its project home page](http://kafka.apache.org/).
- Architecture guides and improvement plans are available in the [docs directory](docs/).

## Project Structure

```
rustfs-kafka/
├── src/                        # Core library
│   ├── client/                 # KafkaClient — mid-level API
│   ├── consumer/               # Consumer — high-level consumer API
│   ├── producer/               # Producer — high-level producer API
│   ├── protocol/               # Kafka wire protocol implementations
│   ├── network/                # TCP/TLS connections and pooling
│   ├── compression/            # Compression codec (NONE, SNAPPY, GZIP)
│   ├── error.rs                # Error types
│   └── utils.rs                # Shared utilities
├── crates/
│   └── rustfs-kafka-async/     # Async client built on tokio
├── benches/                    # Criterion benchmarks
├── examples/                   # Standalone example programs
├── tests/                      # Integration tests (Docker-based)
└── docs/                       # Architecture guides and task plans
```

## Installation

```toml
[dependencies]
rustfs-kafka = "0.22"
```

To build the usual `cargo build` should suffice. The crate supports various features which can be turned off at compile
time.

### Feature Flags

| Feature              | Default | Description                                              |
|----------------------|---------|----------------------------------------------------------|
| `security`           | Yes     | TLS via rustls with aws-lc-rs crypto provider            |
| `security-ring`      | No      | TLS via rustls with ring crypto provider                 |
| `producer_timestamp` | No      | Include timestamps in producer records (requires chrono) |
| `metrics`            | No      | Prometheus-compatible metrics support                    |
| `nightly`            | No      | Enable nightly-only optimizations                        |
| `integration_tests`  | No      | Compile integration tests                                |

Compression (NONE, SNAPPY, GZIP, LZ4, ZSTD) is handled by the `kafka-protocol` crate
and is always available without feature flags.

### TLS Support

**rustfs-kafka** uses **rustls** as the TLS backend, providing a pure-Rust, secure, and portable TLS implementation with
`aws-lc-rs` as the default crypto provider.

#### Available Security Features

- `security` (default): Uses `rustls` with `aws-lc-rs` as the crypto provider.
- `security-ring`: Uses `rustls` with `ring` as the crypto provider.

#### Using rustls (Default)

rustls is enabled by default and requires no additional system dependencies:

```toml
[dependencies]
rustfs-kafka = "0.22"
```

To use `ring` instead of `aws-lc-rs`:

```toml
[dependencies]
rustfs-kafka = { version = "0.22", default-features = false, features = ["security-ring"] }
```

Benefits of rustls:

- Pure Rust implementation - no native dependencies
- Better cross-compilation support (musl, alpine, etc.)
- Modern TLS 1.2+ only
- Simpler builds - no OpenSSL development libraries required
- Consistent behavior across platforms

**Note:** rustls requires TLS 1.2+. Legacy TLS 1.0/1.1 not supported.

#### Disabling TLS

To build without any TLS support:

```toml
[dependencies]
rustfs-kafka = { version = "0.22", default-features = false }
```

## Supported Kafka version

`rustfs-kafka` is tested against Kafka 3.9.2, 4.1.2, and 4.2.0. Kafka 4.x uses KRaft mode (no ZooKeeper), while 3.x uses
the traditional ZooKeeper-based mode.

## Examples

As mentioned, the [cargo generated documentation](https://docs.rs/rustfs-kafka/) contains some examples. Further,
standalone, compilable example programs are provided in the [examples directory](examples/):

- `example-produce.rs` — Basic message production
- `example-consume.rs` — Basic message consumption
- `example-fetch.rs` — Low-level fetch API usage
- `console-producer.rs` — Read from stdin and produce to Kafka
- `console-consumer.rs` — Consume from Kafka and print to stdout
- `offset-monitor.rs` — Monitor consumer group offsets
- `example-rustls.rs` — TLS connection example

### Async API

The `rustfs-kafka-async` crate provides an async wrapper built on tokio:

```toml
[dependencies]
rustfs-kafka-async = "0.22"
```

## Consumer

This is a higher-level consumer API for Kafka and is provided by the module `rustfs_kafka::consumer`. Features include
automatic offset management with consumer group coordination, configurable fallback offset, partition assignment
strategies, and manual or automatic offset committing.

## Producer

This is a higher-level producer API for Kafka and is provided by the module `rustfs_kafka::producer`. Features include
automatic partition assignment through pluggable partitioners, batch producer for efficient message grouping,
transactional producer for exactly-once semantics, and configurable compression.

## KafkaClient

`KafkaClient` in the `rustfs_kafka::client` module is the central point of this API. However, this is a mid-level
abstraction for Kafka rather suitable for building higher-level APIs. Applications typically want to use the already
mentioned `Consumer` and `Producer`. Nevertheless, the main features of `KafkaClient` are:

- Loading metadata
- Fetching topic offsets
- Sending messages
- Fetching messages
- Committing a consumer group's offsets
- Fetching a consumer group's offsets

## Bugs / Features / Contributing

There's still a lot of room for improvement. Not everything works right at the moment, and testing coverage could be
better. **Use it in production at your own risk.** Have a look at
the [issue tracker](https://github.com/houseme/kafka-rust/issues) and feel free to contribute by reporting new problems
or contributing to existing ones.

### Integration tests

When working locally, the integration tests require Docker and docker-compose. Run the tests via the included
`run-all-tests` script in the `tests` directory:

```bash
# Run against all default Kafka versions (3.9.2, 4.1.2, 4.2.0)
./tests/run-all-tests

# Run against a specific version
./tests/run-all-tests 4.2.0

# Run only non-secure tests
SECURES= ./tests/run-all-tests 3.9.2
```

## Creating a topic

Given a local Kafka server installation you can create topics with the following command:

```
kafka-topics.sh --topic my-topic --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

See also [Kafka's quickstart guide](https://kafka.apache.org/documentation.html#quickstart) for more information.

## Alternative/Related projects

- [rust-rdkafka](https://github.com/fede1024/rust-rdkafka) is an emerging alternative Kafka client library for Rust
  based on `librdkafka`.
