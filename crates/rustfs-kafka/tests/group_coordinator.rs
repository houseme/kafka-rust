#![cfg(feature = "integration_tests")]
//! Real-broker manual Find/Join/Sync/Heartbeat/Leave validation.
//! Automatic heartbeat scheduling is not exercised by this fixture.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rustfs_kafka::client::{KafkaClient, RetryPolicy, TopicConfig};
#[cfg(any(feature = "security", feature = "security-ring"))]
use rustfs_kafka::client::{SaslConfig, SecurityConfig};
use rustfs_kafka::consumer::{GroupCoordinator, RangeAssignor};
use rustfs_kafka::error::{Error, KafkaCode};

#[test]
fn fresh_coordinator_can_manually_join_sync_heartbeat_and_leave() {
    const MAX_JOIN_ATTEMPTS: usize = 32;
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();
    let mut fixture = GroupTopic::new();
    let client = new_client();
    assert!(client.group_coordinator_host(&fixture.group).is_none());
    let mut coordinator =
        GroupCoordinator::new(client, fixture.group.clone(), 10_000, 10_000, 0, 30_000);
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut attempt = 0;
    let assignment = loop {
        assert!(
            attempt < MAX_JOIN_ATTEMPTS && Instant::now() < deadline,
            "bounded group join retries exhausted after {attempt} attempts"
        );
        attempt += 1;
        match coordinator.join_group(&RangeAssignor, &[fixture.topic.clone()]) {
            Ok(assignment) => break assignment,
            Err(error) => {
                eprintln!(
                    "group join attempt {attempt} failed: {error:?} (member_id={:?}, generation={:?})",
                    coordinator.member_id(),
                    coordinator.generation_id()
                );
                let retryable = matches!(
                    &error,
                    Error::Kafka(
                        KafkaCode::GroupLoadInProgress
                            | KafkaCode::GroupCoordinatorNotAvailable
                            | KafkaCode::NotCoordinatorForGroup
                    )
                );
                assert!(
                    retryable && attempt < MAX_JOIN_ATTEMPTS && Instant::now() < deadline,
                    "group fixture join failed after {attempt} attempts: {error:?}"
                );
                // These broker responses explicitly rejected this request. The
                // next manual invocation exercises coordinator rediscovery.
                std::thread::park_timeout(
                    Duration::from_millis(100)
                        .min(deadline.saturating_duration_since(Instant::now())),
                );
            }
        }
    };
    assert!(
        coordinator
            .member_id()
            .is_some_and(|member| !member.is_empty())
    );
    assert!(
        coordinator
            .generation_id()
            .is_some_and(|generation| generation > 0)
    );
    assert_eq!(assignment.topic_partitions.len(), 1);
    assert_eq!(assignment.topic_partitions[0].topic, fixture.topic);
    assert_eq!(assignment.topic_partitions[0].partitions, [0]);
    coordinator.heartbeat().unwrap();
    coordinator.leave_group().unwrap();
    assert!(coordinator.member_id().is_none());
    assert!(coordinator.generation_id().is_none());
    fixture.cleanup();
}

fn new_client() -> KafkaClient {
    let builder = KafkaClient::builder()
        .with_hosts(vec!["127.0.0.1:9092".to_owned()])
        .with_conn_rw_timeout(5)
        // Bound discovery's own rejection retries before the fixture retries Join.
        .with_retry_policy(RetryPolicy::Fixed {
            interval: Duration::from_millis(100),
            max_attempts: 4,
        });
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
    builder.build()
}

struct GroupTopic {
    topic: String,
    group: String,
    admin: KafkaClient,
    deleted: bool,
}

impl GroupTopic {
    fn new() -> Self {
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut fixture = Self {
            topic: format!("kafka-rust-group-topic-{suffix}"),
            group: format!("kafka-rust-group-{suffix}"),
            admin: new_client(),
            deleted: false,
        };
        fixture.admin.load_metadata_all().unwrap();
        let response = fixture
            .admin
            .create_topics(
                &[TopicConfig::new(&fixture.topic).with_config("cleanup.policy", "delete")],
                Duration::from_secs(10),
            )
            .unwrap();
        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response.results[0].error_code, 0,
            "creating group fixture failed: {:?}",
            response.results[0]
        );
        wait_for_topic(&mut fixture.admin, &fixture.topic);
        fixture
    }

    fn cleanup(&mut self) {
        let response = self
            .admin
            .delete_topics(&[&self.topic], Duration::from_secs(5))
            .unwrap();
        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response.results[0].error_code, 0,
            "deleting group fixture failed: {:?}",
            response.results[0]
        );
        self.deleted = true;
    }
}

impl Drop for GroupTopic {
    fn drop(&mut self) {
        if !self.deleted {
            let _ = self
                .admin
                .delete_topics(&[&self.topic], Duration::from_secs(5));
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
            "group fixture did not become ready"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
