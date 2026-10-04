#[cfg(feature = "integration_tests")]
mod integration {
    use rustfs_kafka::client::{RequiredAcks, SaslConfig, SecurityConfig};
    use rustfs_kafka::consumer::FetchOffset;
    use rustfs_kafka::kafka_protocol::messages::{
        ApiKey, FindCoordinatorRequest, FindCoordinatorResponse, GroupId, OffsetCommitRequest,
        OffsetCommitResponse, TopicName,
        offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    };
    use rustfs_kafka::kafka_protocol::protocol::StrBytes;
    use rustfs_kafka::producer::Record;
    use rustfs_kafka_async::{AsyncConsumer, AsyncKafkaClient, AsyncProducer, AsyncProducerConfig};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const LOCAL_KAFKA_BOOTSTRAP_HOST: &str = "127.0.0.1:9092";
    const TEST_TOPIC_NAME: &str = "kafka-rust-test";

    const KAFKA_CLIENT_SECURE: &str = "KAFKA_CLIENT_SECURE";
    const KAFKA_CLIENT_SASL_MECHANISM: &str = "KAFKA_CLIENT_SASL_MECHANISM";
    const KAFKA_CLIENT_SASL_USERNAME: &str = "KAFKA_CLIENT_SASL_USERNAME";
    const KAFKA_CLIENT_SASL_PASSWORD: &str = "KAFKA_CLIENT_SASL_PASSWORD";

    fn security_from_env() -> Option<SecurityConfig> {
        let secure_val = std::env::var(KAFKA_CLIENT_SECURE).ok()?;
        if secure_val.is_empty() {
            return None;
        }

        let mut config = SecurityConfig::new().with_hostname_verification(false);

        let mechanism = std::env::var(KAFKA_CLIENT_SASL_MECHANISM).unwrap_or_default();
        if !mechanism.is_empty() {
            let username =
                std::env::var(KAFKA_CLIENT_SASL_USERNAME).unwrap_or_else(|_| "test".to_owned());
            let password = std::env::var(KAFKA_CLIENT_SASL_PASSWORD)
                .unwrap_or_else(|_| "test-pass".to_owned());
            config = config.with_sasl(SaslConfig::new(mechanism, username, password));
        }

        Some(config)
    }

    #[tokio::test]
    async fn test_async_producer_send_with_secure_profile() {
        let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()];
        let mut producer_config = AsyncProducerConfig::new().with_required_acks(RequiredAcks::One);
        if let Some(security) = security_from_env() {
            producer_config = producer_config.with_security(security);
        }

        let producer = AsyncProducer::from_hosts_with_config(hosts, producer_config)
            .await
            .expect("failed to create async producer with secure profile");

        let payload = format!(
            "secure-sasl-e2e:{}",
            std::env::var(KAFKA_CLIENT_SASL_MECHANISM).unwrap_or_else(|_| "TLS".to_owned())
        );
        let record = Record::from_value(TEST_TOPIC_NAME, payload.as_bytes());

        producer
            .send(&record)
            .await
            .expect("failed to produce message with secure profile");
    }

    #[tokio::test]
    async fn test_async_consumer_reads_and_commits_multiple_batches_with_secure_profile() {
        tokio::time::timeout(Duration::from_secs(60), async {
            let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()];
            let security = security_from_env();
            let mut producer_config =
                AsyncProducerConfig::new().with_required_acks(RequiredAcks::One);
            if let Some(security) = &security {
                producer_config = producer_config.with_security(security.clone());
            }
            let producer = AsyncProducer::from_hosts_with_config(hosts.clone(), producer_config)
                .await
                .expect("failed to create async producer with secure profile");
            let run_id = format!(
                "{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
            );
            let group = format!("async-batches-{run_id}");
            let first = format!("async-batch-first-{run_id}");
            let second = format!("async-batch-second-{run_id}");
            // Separate Produce requests create separate record batches on the
            // same partition before the consumer's first Fetch snapshot.
            for value in [&first, &second] {
                producer
                    .send(&Record::from_value(TEST_TOPIC_NAME, value.as_bytes()).with_partition(0))
                    .await
                    .expect("failed to produce a record batch");
            }
            let mut builder = AsyncConsumer::builder(hosts.clone())
                .with_group(group.clone())
                .with_topic(TEST_TOPIC_NAME.to_owned())
                .with_native_retry_attempts(64)
                .with_native_retry_backoff(Duration::from_millis(100))
                .with_fallback_offset(FetchOffset::Earliest);
            if let Some(security) = &security {
                builder = builder.with_security(security.clone());
            }
            let mut consumer = builder
                .build()
                .await
                .expect("failed to create async consumer with secure profile");
            let messages = consumer
                .poll()
                .await
                .expect("failed to fetch record batches");
            let mut first_offset = None;
            let mut second_offset = None;
            for set in messages.iter() {
                for message in set.messages() {
                    if message.value == first.as_bytes() {
                        assert_eq!(set.partition(), 0);
                        first_offset = Some(message.offset);
                    }
                    if message.value == second.as_bytes() {
                        assert_eq!(set.partition(), 0);
                        second_offset = Some(message.offset);
                    }
                }
            }
            assert!(
                second_offset.expect("second batch must be present in the first poll")
                    > first_offset.expect("first batch must be present in the first poll")
            );
            consumer
                .commit()
                .await
                .expect("failed to commit delivered offsets");
            consumer.close().await.unwrap();

            let mut builder = AsyncConsumer::builder(hosts)
                .with_group(group)
                .with_topic(TEST_TOPIC_NAME.to_owned())
                .with_native_retry_attempts(64)
                .with_native_retry_backoff(Duration::from_millis(100))
                .with_fallback_offset(FetchOffset::Earliest);
            if let Some(security) = security {
                builder = builder.with_security(security);
            }
            let mut resumed = builder
                .build()
                .await
                .expect("failed to recreate async consumer");
            let messages = resumed
                .poll()
                .await
                .expect("failed to resume committed offsets");
            for set in messages.iter() {
                assert!(
                    set.messages()
                        .iter()
                        .all(|message| message.value != first.as_bytes()
                            && message.value != second.as_bytes())
                );
            }
            resumed.close().await.unwrap();
            producer.close().await.unwrap();
        })
        .await
        .expect("secure multi-batch consumer test timed out");
    }

    async fn seed_partition_zero_offset(
        hosts: &[String],
        security: &Option<SecurityConfig>,
        group: &str,
        offset: i64,
    ) {
        let mut bootstrap = AsyncKafkaClient::with_client_id_and_security(
            hosts.to_vec(),
            "async-offset-seed".to_owned(),
            security.clone(),
        )
        .await
        .expect("failed to connect for committed-offset setup");
        for attempt in 0..64 {
            let coordinator: FindCoordinatorResponse = bootstrap
                .send_raw_protocol_request(
                    ApiKey::FindCoordinator as i16,
                    3,
                    &FindCoordinatorRequest::default()
                        .with_key(StrBytes::from_string(group.to_owned()))
                        .with_key_type(0),
                )
                .await
                .expect("coordinator lookup IO failed");
            if coordinator.error_code != 0 {
                assert!(
                    matches!(coordinator.error_code, 14..=16),
                    "terminal coordinator error: {}",
                    coordinator.error_code
                );
                assert!(attempt < 63, "coordinator setup retry budget exhausted");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let host = format!("{}:{}", coordinator.host, coordinator.port);
            let mut client = AsyncKafkaClient::with_client_id_and_security(
                vec![host],
                "async-offset-seed".to_owned(),
                security.clone(),
            )
            .await
            .expect("coordinator setup IO failed");
            let request = OffsetCommitRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_string(group.to_owned())))
                .with_generation_id_or_member_epoch(-1)
                .with_member_id(StrBytes::from_static_str(""))
                .with_retention_time_ms(-1)
                .with_topics(vec![
                    OffsetCommitRequestTopic::default()
                        .with_name(TopicName::from(StrBytes::from_static_str(TEST_TOPIC_NAME)))
                        .with_partitions(vec![
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(0)
                                .with_committed_offset(offset),
                        ]),
                ]);
            let response: OffsetCommitResponse = client
                .send_raw_protocol_request(ApiKey::OffsetCommit as i16, 2, &request)
                .await
                .expect("committed-offset setup IO failed");
            assert_eq!(response.topics.len(), 1);
            assert_eq!(response.topics[0].name.as_str(), TEST_TOPIC_NAME);
            assert_eq!(response.topics[0].partitions.len(), 1);
            assert_eq!(response.topics[0].partitions[0].partition_index, 0);
            let code = response.topics[0].partitions[0].error_code;
            if code == 0 {
                return;
            }
            assert!(
                matches!(code, 14..=16),
                "terminal committed-offset setup error: {code}"
            );
            assert!(
                attempt < 63,
                "committed-offset setup retry budget exhausted"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        unreachable!("bounded retry loop must return or report exhaustion")
    }

    #[tokio::test]
    async fn test_async_consumer_resumes_inside_one_batch_without_redelivering_its_prefix() {
        tokio::time::timeout(Duration::from_secs(60), async {
            let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()];
            let security = security_from_env();
            let mut config = AsyncProducerConfig::new().with_required_acks(RequiredAcks::One);
            if let Some(security) = &security {
                config = config.with_security(security.clone());
            }
            let producer = AsyncProducer::from_hosts_with_config(hosts.clone(), config)
                .await
                .unwrap();
            let run_id = format!(
                "{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let values: Vec<_> = (0..4)
                .map(|index| format!("async-interior-{run_id}-{index}"))
                .collect();
            let records: Vec<_> = values
                .iter()
                .map(|value| {
                    Record::from_value(TEST_TOPIC_NAME, value.as_bytes()).with_partition(0)
                })
                .collect();
            producer
                .send_all(&records)
                .await
                .expect("single-batch Produce failed");
            let mut probe_builder = AsyncConsumer::builder(hosts.clone())
                .with_group(format!("async-probe-{run_id}"))
                .with_topic(TEST_TOPIC_NAME.to_owned())
                .with_native_retry_attempts(64)
                .with_native_retry_backoff(Duration::from_millis(100))
                .with_fallback_offset(FetchOffset::Earliest);
            if let Some(security) = &security {
                probe_builder = probe_builder.with_security(security.clone());
            }
            let mut probe = probe_builder.build().await.unwrap();
            let messages = probe.poll().await.expect("failed to observe batch offsets");
            let mut offsets = [None; 4];
            for set in messages.iter() {
                for message in set.messages() {
                    for (index, value) in values.iter().enumerate() {
                        if message.value == value.as_bytes() {
                            assert_eq!(set.partition(), 0);
                            offsets[index] = Some(message.offset);
                        }
                    }
                }
            }
            let offsets =
                offsets.map(|offset| offset.expect("all records must be visible in one poll"));
            assert!(offsets.windows(2).all(|pair| pair[1] == pair[0] + 1));
            probe.close().await.unwrap();
            let group = format!("async-interior-{run_id}");
            seed_partition_zero_offset(&hosts, &security, &group, offsets[2]).await;
            let mut builder = AsyncConsumer::builder(hosts)
                .with_group(group)
                .with_topic(TEST_TOPIC_NAME.to_owned())
                .with_native_retry_attempts(64)
                .with_native_retry_backoff(Duration::from_millis(100))
                .with_fallback_offset(FetchOffset::Earliest);
            if let Some(security) = security {
                builder = builder.with_security(security);
            }
            let mut resumed = builder.build().await.unwrap();
            let messages = resumed
                .poll()
                .await
                .expect("failed to resume within a batch");
            let mut found = [false; 4];
            for set in messages.iter() {
                for message in set.messages() {
                    for (index, value) in values.iter().enumerate() {
                        if message.value == value.as_bytes() {
                            found[index] = true;
                        }
                    }
                }
            }
            assert_eq!(found, [false, false, true, true]);
            resumed
                .commit()
                .await
                .expect("failed to commit resumed batch suffix");
            resumed.close().await.unwrap();
            producer.close().await.unwrap();
        })
        .await
        .expect("secure within-batch resume test timed out");
    }
}
