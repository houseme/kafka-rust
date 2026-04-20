# rustfs-kafka

A Rust client for Apache Kafka, forked from [kafka-rust](https://github.com/kafka-rust/kafka-rust).

## Documentation

- This library is primarily documented through examples in its [API documentation](https://docs.rs/rustfs-kafka/).
- Documentation about Kafka itself can be found at [its project home page](http://kafka.apache.org/).

## Installation

```toml
[dependencies]
rustfs-kafka = "0.20"
```

To build the usual `cargo build` should suffice. The crate supports various features which can be turned off at compile time.

### TLS Support

**rustfs-kafka** uses **rustls** as the TLS backend, providing a pure-Rust, secure, and portable TLS implementation with `aws-lc-rs` as the default crypto provider.

#### Available Security Features

- `security` (default): Uses `rustls` with `aws-lc-rs` as the crypto provider.
- `security-ring`: Uses `rustls` with `ring` as the crypto provider.

#### Using rustls (Default)

rustls is enabled by default and requires no additional system dependencies:

```toml
[dependencies]
rustfs-kafka = "0.20"
```

To use `ring` instead of `aws-lc-rs`:

```toml
[dependencies]
rustfs-kafka = { version = "0.20", default-features = false, features = ["snappy", "gzip", "security-ring"] }
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
rustfs-kafka = { version = "0.20", default-features = false, features = ["snappy", "gzip"] }
```

## Supported Kafka version

`rustfs-kafka` is tested for compatibility with a select few Kafka versions from 0.8.2 to 3.8.0. However, not all features from Kafka 0.9 and newer are supported yet.

## Examples

As mentioned, the [cargo generated documentation](https://docs.rs/rustfs-kafka/) contains some examples. Further, standalone, compilable example programs are provided in the [examples directory](examples/).

## Consumer

This is a higher-level consumer API for Kafka and is provided by the module `rustfs_kafka::consumer`. It provides convenient offset management support on behalf of a specified group.

## Producer

This is a higher-level producer API for Kafka and is provided by the module `rustfs_kafka::producer`. It provides convenient automatic partition assignment capabilities through partitioners.

## KafkaClient

`KafkaClient` in the `rustfs_kafka::client` module is the central point of this API. However, this is a mid-level abstraction for Kafka rather suitable for building higher-level APIs. Applications typically want to use the already mentioned `Consumer` and `Producer`. Nevertheless, the main features of `KafkaClient` are:

- Loading metadata
- Fetching topic offsets
- Sending messages
- Fetching messages
- Committing a consumer group's offsets
- Fetching a consumer group's offsets

## Bugs / Features / Contributing

There's still a lot of room for improvement. Not everything works right at the moment, and testing coverage could be better. **Use it in production at your own risk.** Have a look at the [issue tracker](https://github.com/houseme/kafka-rust/issues) and feel free to contribute by reporting new problems or contributing to existing ones.

### Integration tests

When working locally, the integration tests require Docker (1.10.0+) and docker-compose (1.6.0+). Run the tests via the included `run-all-tests` script in the `tests` directory.

## Creating a topic

Given a local Kafka server installation you can create topics with the following command:

```
kafka-topics.sh --topic my-topic --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

See also [Kafka's quickstart guide](https://kafka.apache.org/documentation.html#quickstart) for more information.

## Alternative/Related projects

- [rust-rdkafka](https://github.com/fede1024/rust-rdkafka) is an emerging alternative Kafka client library for Rust based on `librdkafka`.
