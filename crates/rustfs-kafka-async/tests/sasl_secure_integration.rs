#[cfg(feature = "integration_tests")]
mod integration {
    use rustfs_kafka::client::{
        FetchGroupOffset, GroupOffsetStorage, KafkaClient, RequiredAcks, RetryPolicy, SaslConfig,
        SecurityConfig, encode_request_frame,
    };
    use rustfs_kafka::consumer::FetchOffset;
    use rustfs_kafka::kafka_protocol::messages::{
        ApiKey, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
        GroupId, OffsetCommitRequest, OffsetCommitResponse, RequestHeader, ResponseHeader,
        TopicName,
        fetch_request::{FetchPartition, FetchTopic},
        offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    };
    use rustfs_kafka::kafka_protocol::protocol::{Decodable, HeaderVersion, StrBytes};
    use rustfs_kafka::kafka_protocol::records::RecordBatchDecoder;
    use rustfs_kafka::producer::{Record, TransactionalProducer};
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

    fn transaction_fixture_client(
        hosts: Vec<String>,
        security: Option<SecurityConfig>,
    ) -> KafkaClient {
        let mut builder = KafkaClient::builder()
            .with_hosts(hosts)
            .with_conn_rw_timeout(5)
            .with_group_offset_storage(Some(GroupOffsetStorage::Kafka))
            .with_retry_policy(RetryPolicy::Fixed {
                interval: Duration::from_millis(100),
                max_attempts: 64,
            });
        if let Some(security) = security {
            builder = builder.with_security(security);
        }
        let mut client = builder.build();
        client.load_metadata(&[TEST_TOPIC_NAME]).unwrap();
        client
    }

    fn transaction_fixture_fetch(client: &mut KafkaClient, offset: i64) -> FetchResponse {
        let host = client
            .topics()
            .partitions(TEST_TOPIC_NAME)
            .unwrap()
            .partition(0)
            .unwrap()
            .leader()
            .unwrap()
            .host()
            .to_owned();
        let correlation = client.next_correlation_id();
        let request = FetchRequest::default()
            .with_replica_id((-1).into())
            .with_max_wait_ms(100)
            .with_min_bytes(1)
            .with_max_bytes(1_048_576)
            .with_isolation_level(0)
            .with_session_epoch(-1)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(StrBytes::from_static_str(TEST_TOPIC_NAME).into())
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(offset)
                            .with_partition_max_bytes(1_048_576),
                    ]),
            ]);
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Fetch as i16)
            .with_request_api_version(12)
            .with_correlation_id(correlation)
            .with_client_id(Some(StrBytes::from_static_str(
                "async-transaction-observer",
            )));
        let frame = encode_request_frame(&header, &request, 12).unwrap();
        let conn = client.get_conn_mut(&host).unwrap();
        conn.send(&frame)
            .expect("read-only transaction observer send failed");
        let mut size = [0; 4];
        conn.read_exact(&mut size)
            .expect("transaction observer length read failed");
        let mut bytes = conn
            .read_exact_alloc(u64::try_from(i32::from_be_bytes(size)).unwrap())
            .expect("transaction observer body read failed");
        let header = ResponseHeader::decode(&mut bytes, FetchResponse::header_version(12)).unwrap();
        assert_eq!(header.correlation_id, correlation);
        let response = FetchResponse::decode(&mut bytes, 12).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(response.error_code, 0);
        assert_eq!(response.responses.len(), 1);
        assert_eq!(response.responses[0].topic.as_str(), TEST_TOPIC_NAME);
        assert_eq!(response.responses[0].partitions.len(), 1);
        assert_eq!(response.responses[0].partitions[0].partition_index, 0);
        assert_eq!(response.responses[0].partitions[0].error_code, 0);
        response
    }

    fn wait_for_transaction_markers(
        producer: &mut TransactionalProducer,
        start: i64,
        values: &[String; 3],
    ) -> (i64, i64, [i64; 3]) {
        let producer_id = producer.producer_id();
        let mut cursor = start;
        let mut offsets = [None; 3];
        let mut markers = [0; 2];
        let mut last_marker = None;
        for page in 0..64 {
            // A Fetch page may stop before the end of the log. Accumulate
            // observations while advancing only from fully verified batch
            // headers; the high watermark cannot skip unseen data or markers.
            // EndTxn acknowledgement can also precede marker visibility.
            let response = transaction_fixture_fetch(producer.client_mut(), cursor);
            let partition = &response.responses[0].partitions[0];
            let high_watermark = partition.high_watermark;
            let mut records = partition.records.clone().unwrap_or_default();
            let (owned, progress) =
                rustfs_kafka::client::fetch_kp::convert_fetch_response_with_progress(
                    response.clone(),
                    0,
                )
                .into_parts();
            owned.topics[0].partitions[0]
                .data()
                .unwrap_or_else(|error| {
                    panic!("transaction observer malformed page={page} cursor={cursor}: {error}")
                });
            let verified_next = progress.next_offset(0, 0);
            let mut bounds = Vec::new();
            while !records.is_empty() {
                assert!(records.len() >= 61 && records[16] == 2);
                let base = i64::from_be_bytes(records[..8].try_into().unwrap());
                let delta = i32::from_be_bytes(records[23..27].try_into().unwrap());
                let last = base.checked_add(i64::from(delta)).unwrap();
                let next = last.checked_add(1).unwrap();
                let batch_producer = i64::from_be_bytes(records[43..51].try_into().unwrap());
                let attributes = i16::from_be_bytes(records[21..23].try_into().unwrap());
                let control = attributes & (1 << 5) != 0;
                let count = i32::from_be_bytes(records[57..61].try_into().unwrap());
                bounds.push((base, last, next, batch_producer, control, count));
                let batch = RecordBatchDecoder::decode(&mut records).unwrap();
                for record in batch.records {
                    // A broker can include an older whole batch before the
                    // requested cursor. Do not count its records twice.
                    if record.offset < cursor || record.producer_id != producer_id {
                        continue;
                    }
                    assert!((base..next).contains(&record.offset));
                    if record.control {
                        let key = record.key.as_ref().expect("transaction marker key");
                        assert_eq!(key.len(), 4);
                        let marker = i16::from_be_bytes(key[2..4].try_into().unwrap());
                        assert!(matches!(marker, 0 | 1));
                        markers[usize::try_from(marker).unwrap()] += 1;
                        last_marker = Some(record.offset);
                    } else {
                        for (index, value) in values.iter().enumerate() {
                            if record.value.as_deref() == Some(value.as_bytes()) {
                                assert!(offsets[index].replace(record.offset).is_none());
                            }
                        }
                    }
                }
            }
            let next_cursor = cursor.max(verified_next.unwrap_or(cursor));
            if !bounds.is_empty() || page < 2 || page == 63 {
                eprintln!(
                    "async_txn_observer page={page} producer_id={producer_id} request_cursor={cursor} verified_next={verified_next:?} next_cursor={next_cursor} high_watermark={high_watermark} offsets={offsets:?} markers_abort_commit={markers:?} tail_marker={last_marker:?} batch_bounds_base_last_next_pid_control_count={bounds:?}"
                );
            }
            if offsets.iter().all(Option::is_some) && markers == [1, 2] {
                let offsets = offsets.map(Option::unwrap);
                assert!(offsets.windows(2).all(|pair| pair[1] > pair[0]));
                let tail_marker = last_marker.unwrap();
                assert!(tail_marker > offsets[2]);
                assert!(next_cursor > tail_marker);
                return (next_cursor, tail_marker, offsets);
            }
            assert!(
                page < 63,
                "transaction marker visibility budget exhausted: producer_id={producer_id}, start={start}, cursor={cursor}, verified_next={verified_next:?}, high_watermark={high_watermark}, offsets={offsets:?}, markers_abort_commit={markers:?}, tail_marker={last_marker:?}, batch_bounds_base_last_next_pid_control_count={bounds:?}"
            );
            if next_cursor == cursor {
                // Only visibility waiting sleeps. Fully decoded nonempty pages
                // continue immediately; no transaction mutation is retried.
                std::thread::sleep(Duration::from_millis(100));
            }
            cursor = next_cursor;
        }
        unreachable!("bounded observation must return or report exhaustion")
    }

    fn committed_partition_zero(client: &mut KafkaClient, group: &str) -> i64 {
        let offsets = client
            .fetch_group_offsets(group, [FetchGroupOffset::new(TEST_TOPIC_NAME, 0)])
            .expect("failed to observe committed transaction cursor");
        assert_eq!(offsets.len(), 1);
        let partitions = &offsets[TEST_TOPIC_NAME];
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].partition, 0);
        partitions[0].offset
    }

    #[tokio::test]
    async fn test_native_read_uncommitted_hides_transaction_controls_and_restores_batch_progress() {
        tokio::time::timeout(Duration::from_secs(60), async {
            let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()];
            let security = security_from_env();
            let run_id = format!(
                "{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
            );
            let values = [
                format!("async-txn-commit-first-{run_id}"),
                format!("async-txn-abort-{run_id}"),
                format!("async-txn-commit-last-{run_id}"),
            ];
            let txn_id = format!("async-controls-{run_id}");
            let producer_hosts = hosts.clone();
            let producer_security = security.clone();
            let (producer, start) = tokio::task::spawn_blocking(move || {
                let client = transaction_fixture_client(producer_hosts, producer_security);
                let mut producer = TransactionalProducer::from_client(client)
                    .with_transactional_id(txn_id)
                    .with_ack_timeout_ms(10_000)
                    .create()
                    .expect("transaction fixture setup failed");
                let offsets = producer
                    .client_mut()
                    .fetch_offsets(&[TEST_TOPIC_NAME], FetchOffset::Latest)
                    .unwrap();
                let start = offsets[TEST_TOPIC_NAME]
                    .iter()
                    .find(|offset| offset.partition == 0)
                    .unwrap()
                    .offset;
                assert!(start >= 0);
                (producer, start)
            })
            .await
            .unwrap();
            let group = format!("async-txn-cursor-{run_id}");
            seed_partition_zero_offset(&hosts, &security, &group, start).await;
            let write_values = values.clone();
            let (mut producer, expected_cursor, tail_marker, written_offsets) =
                tokio::task::spawn_blocking(move || {
                    let mut producer = producer;
                    for (index, value) in write_values.iter().enumerate() {
                        producer.begin().unwrap();
                        producer
                            .send(
                                &Record::from_key_value(
                                    TEST_TOPIC_NAME,
                                    b"async-txn-data".as_slice(),
                                    value.as_bytes(),
                                )
                                .with_partition(0),
                            )
                            .expect("transactional fixture Produce failed; no retry");
                        if index == 1 {
                            producer
                                .abort()
                                .expect("fixture abort outcome failed; no retry");
                        } else {
                            producer
                                .commit()
                                .expect("fixture commit outcome failed; no retry");
                        }
                    }
                    let (cursor, tail, offsets) =
                        wait_for_transaction_markers(&mut producer, start, &write_values);
                    (producer, cursor, tail, offsets)
                })
                .await
                .unwrap();
            let mut builder = AsyncConsumer::builder(hosts.clone())
                .with_group(group.clone())
                .with_topic(TEST_TOPIC_NAME.to_owned())
                .with_fallback_offset(FetchOffset::Latest)
                .with_native_retry_attempts(64)
                .with_native_retry_backoff(Duration::from_millis(100));
            if let Some(security) = &security {
                builder = builder.with_security(security.clone());
            }
            let mut consumer = builder.build().await.unwrap();
            let mut delivered = [Vec::new(), Vec::new(), Vec::new()];
            let expected = written_offsets.map(|offset| vec![offset]);
            let mut committed = start;
            for page in 0..64 {
                // Native poll returns one Fetch response per broker, rather
                // than promising a complete snapshot across segment boundaries.
                let messages = consumer
                    .poll()
                    .await
                    .expect("native transactional page fetch failed");
                for set in messages.iter() {
                    for message in set.messages() {
                        assert!(
                            message.key.as_ref() != &[0, 0, 0, 0][..]
                                && message.key.as_ref() != &[0, 0, 0, 1][..],
                            "control marker escaped into native application messages"
                        );
                        for (index, value) in values.iter().enumerate() {
                            if message.value == value.as_bytes() {
                                assert_eq!(set.partition(), 0);
                                assert_eq!(message.key.as_ref(), b"async-txn-data");
                                delivered[index].push(message.offset);
                                assert_eq!(
                                    delivered[index].len(),
                                    1,
                                    "native page {page} delivered fixture value {index} twice"
                                );
                            }
                        }
                    }
                }
                consumer
                    .commit()
                    .await
                    .expect("native transaction page cursor commit failed");
                let observe_group = group.clone();
                let (returned_producer, observed_commit) = tokio::task::spawn_blocking(move || {
                    let mut producer = producer;
                    let committed =
                        committed_partition_zero(producer.client_mut(), &observe_group);
                    (producer, committed)
                })
                .await
                .unwrap();
                producer = returned_producer;
                assert!(observed_commit >= committed, "native committed cursor regressed");
                committed = observed_commit;
                eprintln!(
                    "async_txn_native page={page} delivered={delivered:?} committed={committed} expected_cursor={expected_cursor} tail_marker={tail_marker}"
                );
                for (index, offset) in written_offsets.iter().enumerate() {
                    if committed > *offset {
                        assert_eq!(
                            delivered[index], expected[index],
                            "native page {page} committed past missing fixture value {index}"
                        );
                    }
                }
                if committed >= expected_cursor && committed > tail_marker {
                    // Advancing beyond a missing business value is an immediate
                    // failure, even when the broker accepted OffsetCommit.
                    // isolation=0 must include the aborted value exactly once.
                    assert_eq!(delivered, expected, "native cursor skipped fixture data");
                    break;
                }
                assert!(
                    page < 63,
                    "native transaction page budget exhausted: delivered={delivered:?}, committed={committed}, expected_cursor={expected_cursor}, tail_marker={tail_marker}"
                );
            }
            assert_eq!(delivered, expected);
            assert!(committed >= expected_cursor && committed > tail_marker);
            consumer.close().await.unwrap();
            let mut builder = AsyncConsumer::builder(hosts)
                .with_group(group.clone())
                .with_topic(TEST_TOPIC_NAME.to_owned())
                .with_fallback_offset(FetchOffset::Latest)
                .with_native_retry_attempts(64)
                .with_native_retry_backoff(Duration::from_millis(100));
            if let Some(security) = security {
                builder = builder.with_security(security);
            }
            let mut resumed = builder.build().await.unwrap();
            let messages = resumed
                .poll()
                .await
                .expect("native transaction cursor restore failed");
            for set in messages.iter() {
                for message in set.messages() {
                    assert!(values.iter().all(|value| message.value != value.as_bytes()));
                    assert!(
                        message.key.as_ref() != &[0, 0, 0, 0][..]
                            && message.key.as_ref() != &[0, 0, 0, 1][..]
                    );
                }
            }
            resumed.commit().await.unwrap();
            resumed.close().await.unwrap();
            let resumed_commit = tokio::task::spawn_blocking(move || {
                let mut producer = producer;
                committed_partition_zero(producer.client_mut(), &group)
            })
            .await
            .unwrap();
            assert!(resumed_commit >= committed && resumed_commit > tail_marker);
        })
        .await
        .expect("secure native transaction-control consumer test timed out");
    }
}
