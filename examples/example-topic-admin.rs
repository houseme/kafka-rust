use std::time::Duration;

use rustfs_kafka::client::{KafkaClient, TopicConfig};

/// Create and delete topics with `KafkaClient` admin APIs.
fn main() {
    tracing_subscriber::fmt::init();

    if let Err(e) = run() {
        eprintln!("topic admin example failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> rustfs_kafka::Result<()> {
    let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    client.load_metadata_all()?;

    let topic_name = "example-admin-topic";
    let topics = vec![TopicConfig::new(topic_name).with_partitions(3)];

    let create = client.create_topics(&topics, Duration::from_secs(10))?;
    for result in create.results {
        println!(
            "create topic={} error_code={} error_message={:?}",
            result.name, result.error_code, result.error_message
        );
    }

    let delete = client.delete_topics(&[topic_name], Duration::from_secs(10))?;
    for result in delete.results {
        println!(
            "delete topic={} error_code={}",
            result.name, result.error_code
        );
    }

    Ok(())
}
