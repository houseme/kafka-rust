use rustfs_kafka::client::{FetchPartition, KafkaClient};

/// Low-level fetch example using `KafkaClient`.
fn main() {
    tracing_subscriber::fmt::init();

    if let Err(e) = run() {
        eprintln!("fetch example failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> rustfs_kafka::Result<()> {
    let broker = "localhost:9092";
    let topic = "my-topic";
    let partition = 0;
    let offset = 0;

    let mut client = KafkaClient::new(vec![broker.to_owned()]);
    client.load_metadata_all()?;

    if !client.topics().contains(topic) {
        println!("topic not found on cluster: {topic}");
        return Ok(());
    }

    let responses = client.fetch_messages_kp([FetchPartition::new(topic, partition, offset)])?;
    for response in responses {
        for topic_response in response.topics {
            for partition_response in topic_response.partitions {
                match partition_response.data {
                    Ok(data) => {
                        println!(
                            "topic={} partition={} highwatermark={} messages={}",
                            topic_response.topic,
                            partition_response.partition,
                            data.highwatermark_offset,
                            data.messages.len()
                        );
                    }
                    Err(e) => {
                        println!(
                            "partition error topic={} partition={}: {e}",
                            topic_response.topic, partition_response.partition
                        );
                    }
                }
            }
        }
    }

    Ok(())
}
