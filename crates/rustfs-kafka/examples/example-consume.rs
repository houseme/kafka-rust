use rustfs_kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

/// Minimal consumer example.
fn main() {
    tracing_subscriber::fmt::init();

    if let Err(e) = run() {
        eprintln!("consume example failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> rustfs_kafka::Result<()> {
    let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("my-topic".to_owned())
        .with_group("my-group".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .with_client_id("rustfs-kafka-example-consume".to_owned())
        .create()?;

    let message_sets = consumer.poll()?;
    for message_set in message_sets.iter() {
        for message in message_set.messages() {
            println!(
                "{}:{}@{}: {:?}",
                message_set.topic(),
                message_set.partition(),
                message.offset,
                message.value
            );
        }
        consumer.consume_messageset(&message_set)?;
    }

    consumer.commit_consumed()?;
    Ok(())
}
