#![cfg(all(feature = "integration_tests", feature = "producer_timestamp"))]
//! Producer timestamp validation against a real Kafka broker.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rustfs_kafka::client::{Compression, FetchOffset, KafkaClient, ProducerTimestamp, TopicConfig};
#[cfg(any(feature = "security", feature = "security-ring"))]
use rustfs_kafka::client::{SaslConfig, SecurityConfig};
use rustfs_kafka::producer::{Producer, Record, RequiredAcks};

#[test]
fn create_time_timestamps_are_queryable_by_time() {
    let mut fixture = TimestampTopic::new();
    let mut client = new_client();
    wait_for_topic(&mut client, &fixture.name);
    let mut producer = Producer::from_client(client)
        .with_timestamp(ProducerTimestamp::CreateTime)
        .with_required_acks(RequiredAcks::All)
        .with_ack_timeout(Duration::from_secs(5))
        .create()
        .unwrap();
    let requested_time = unix_millis();
    producer
        .send(&Record::from_value(&fixture.name, b"timestamp-value".as_slice()).with_partition(0))
        .unwrap();
    let after_send = unix_millis();
    let offsets = producer
        .client_mut()
        .list_offsets(&[&fixture.name], FetchOffset::ByTime(requested_time))
        .unwrap();
    let partition = &offsets[&fixture.name][0];
    assert_eq!(partition.partition, 0);
    assert_eq!(
        partition.offset, 0,
        "ByTime must locate the newly produced record"
    );
    assert!(
        partition.time > 0 && partition.time >= requested_time && partition.time <= after_send,
        "expected actual CreateTime timestamp between {requested_time} and {after_send}, got {}",
        partition.time
    );
    fixture.cleanup();
}

fn new_client() -> KafkaClient {
    let builder = KafkaClient::builder()
        .with_hosts(vec!["127.0.0.1:9092".to_owned()])
        .with_conn_rw_timeout(5);
    #[cfg(any(feature = "security", feature = "security-ring"))]
    let builder = if std::env::var("KAFKA_CLIENT_SECURE").is_ok_and(|value| !value.is_empty()) {
        let mut security = SecurityConfig::new().with_hostname_verification(false);
        let mechanism = std::env::var("KAFKA_CLIENT_SASL_MECHANISM").unwrap_or_default();
        if !mechanism.is_empty() {
            security = security.with_sasl(SaslConfig::new(
                mechanism,
                std::env::var("KAFKA_CLIENT_SASL_USERNAME").unwrap_or_else(|_| "test".to_owned()),
                std::env::var("KAFKA_CLIENT_SASL_PASSWORD")
                    .unwrap_or_else(|_| "test-pass".to_owned()),
            ));
        }
        builder.with_security(security)
    } else {
        builder
    };
    let mut client = builder.build();
    client.set_compression(
        match std::env::var("KAFKA_CLIENT_COMPRESSION")
            .unwrap_or_default()
            .to_uppercase()
            .as_str()
        {
            "" | "NONE" => Compression::NONE,
            "GZIP" => Compression::GZIP,
            "SNAPPY" => Compression::SNAPPY,
            "LZ4" => Compression::LZ4,
            "ZSTD" => Compression::ZSTD,
            other => panic!("unknown compression: {other}"),
        },
    );
    client
}

struct TimestampTopic {
    name: String,
    admin: KafkaClient,
    deleted: bool,
}

impl TimestampTopic {
    fn new() -> Self {
        let mut fixture = Self {
            name: format!(
                "kafka-rust-create-time-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ),
            admin: new_client(),
            deleted: false,
        };
        fixture.admin.load_metadata_all().unwrap();
        let response = fixture
            .admin
            .create_topics(
                &[TopicConfig::new(&fixture.name)
                    .with_config("cleanup.policy", "delete")
                    .with_config("message.timestamp.type", "CreateTime")],
                Duration::from_secs(10),
            )
            .unwrap();
        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response.results[0].error_code, 0,
            "creating timestamp fixture failed: {:?}",
            response.results[0]
        );
        fixture
    }

    fn cleanup(&mut self) {
        let response = self
            .admin
            .delete_topics(&[&self.name], Duration::from_secs(5))
            .unwrap();
        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response.results[0].error_code, 0,
            "deleting timestamp fixture failed: {:?}",
            response.results[0]
        );
        self.deleted = true;
    }
}

impl Drop for TimestampTopic {
    fn drop(&mut self) {
        if !self.deleted {
            let _ = self
                .admin
                .delete_topics(&[&self.name], Duration::from_secs(5));
        }
    }
}

#[allow(clippy::disallowed_methods)]
fn wait_for_topic(client: &mut KafkaClient, topic: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        client.load_metadata(&[topic]).unwrap();
        if client.topics().partitions(topic).is_some_and(|partitions| {
            partitions
                .partition(0)
                .is_some_and(|partition| partition.is_available())
        }) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timestamp fixture did not become ready"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn unix_millis() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}
