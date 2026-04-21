# rustfs-kafka-async

Async wrappers for `rustfs-kafka`, built on top of `tokio`.

This crate provides:

- `AsyncKafkaClient`
- `AsyncProducer`
- `AsyncConsumer`

The current implementation uses background tasks/threads to bridge the sync core APIs.

## Installation

```toml
[dependencies]
rustfs-kafka-async = "0.1"
```

## Quick Example

```rust,no_run
use rustfs_kafka::producer::Record;
use rustfs_kafka_async::{AsyncKafkaClient, AsyncProducer};

#[tokio::main]
async fn main() -> rustfs_kafka::Result<()> {
    let client = AsyncKafkaClient::new(vec!["localhost:9092".to_owned()]).await?;
    let producer = AsyncProducer::new(client).await?;
    producer.send(&Record::from_value("my-topic", b"hello async")).await?;
    producer.flush().await?;
    producer.close().await?;
    Ok(())
}
```

## Notes

- This crate is intentionally lightweight and reuses the mature sync protocol/client logic from `rustfs-kafka`.
- For full feature details, consult the root crate docs and `docs/api-guide.md` in the repository.
