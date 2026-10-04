//! Produce message operations for [`KafkaClient`].
//!
//! Handles sending messages to Kafka brokers, grouping messages by their
//! target broker, and supporting both fire-and-forget (acks=0) and
//! acknowledged produce modes with optional metrics recording.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::compression::Compression;
use crate::error::{Error, KafkaCode, Result};
use crate::protocol;
use crate::protocol::api_versions::ApiVersionCache;

use super::config::ClientConfig;
use super::state::ClientState;
use super::transport;
use super::{ProduceConfirm, ProduceMessage, RequiredAcks};
use crate::network::Connections;

type BrokerMessage<'a, 'b> = (
    &'a str,
    i32,
    Option<&'b [u8]>,
    Option<&'b [u8]>,
    &'b [(String, bytes::Bytes)],
);
type BrokerMessages<'a, 'b, 'h> = HashMap<&'h str, Vec<BrokerMessage<'a, 'b>>>;

struct PreparedProduceRequest<'a> {
    host: &'a str,
    frame: bytes::Bytes,
    api_version: i16,
}

struct ProduceRequestContext<'a> {
    conn_pool: &'a mut Connections,
    correlation_id: i32,
    client_id: &'a str,
    required_acks: i16,
    ack_timeout_ms: i32,
    compression: Compression,
    timestamp: i64,
    api_versions: &'a ApiVersionCache,
    no_acks: bool,
}

#[tracing::instrument(skip(conn_pool, state, config, messages), fields(acks = ?acks))]
pub(crate) fn internal_produce_messages_kp<'a, 'b, I, J>(
    conn_pool: &mut Connections,
    state: &mut ClientState,
    config: &ClientConfig,
    api_versions: &ApiVersionCache,
    acks: RequiredAcks,
    ack_timeout: Duration,
    messages: I,
) -> Result<Vec<ProduceConfirm>>
where
    J: AsRef<ProduceMessage<'a, 'b>>,
    I: IntoIterator<Item = J>,
{
    #[cfg(feature = "metrics")]
    let start = Instant::now();
    let timestamp = sample_producer_timestamp(config.producer_timestamp)?;
    let correlation = state.next_correlation_id();

    // Collect messages into (broker, Vec<(topic, partition, key, value, headers)>)
    // We extract broker info first, then bundle with header references.
    let mut broker_msgs: BrokerMessages<'a, 'b, '_> = HashMap::new();
    #[cfg(feature = "metrics")]
    let mut topic_stats: HashMap<&str, (usize, usize)> = HashMap::new();
    for msg in messages {
        let msg = msg.as_ref();
        #[cfg(feature = "metrics")]
        {
            let (bytes, count) = topic_stats.entry(msg.topic).or_default();
            *bytes += msg.value.map_or(0, <[u8]>::len);
            *count += 1;
        }
        let Some(broker) = state.find_broker(msg.topic, msg.partition) else {
            #[cfg(feature = "metrics")]
            crate::metrics::record_produce_error(msg.topic, "UnknownTopicOrPartition");
            return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
        };
        broker_msgs.entry(broker).or_default().push((
            msg.topic,
            msg.partition,
            msg.key,
            msg.value,
            msg.headers,
        ));
    }

    let mut ctx = ProduceRequestContext {
        conn_pool,
        correlation_id: correlation,
        client_id: &config.client_id,
        required_acks: acks as i16,
        ack_timeout_ms: protocol::to_millis_i32(ack_timeout)?,
        compression: config.compression,
        timestamp,
        api_versions,
        no_acks: acks as i16 == 0,
    };
    let result = produce_messages_inner(&mut ctx, broker_msgs);

    #[cfg(feature = "metrics")]
    {
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        match &result {
            Ok(_) => {
                for (topic, (bytes, count)) in topic_stats {
                    crate::metrics::record_produce(topic, bytes, count, elapsed);
                }
            }
            Err(e) => {
                let error_type = format!("{e:?}");
                crate::metrics::record_produce_error("_unknown", &error_type);
            }
        }
    }

    result
}

fn produce_messages_inner(
    ctx: &mut ProduceRequestContext<'_>,
    broker_msgs: BrokerMessages<'_, '_, '_>,
) -> Result<Vec<ProduceConfirm>> {
    let requests = prepare_produce_requests(ctx, broker_msgs)?;
    let now = Instant::now();
    let mut res: Vec<ProduceConfirm> = Vec::new();

    for PreparedProduceRequest {
        host,
        frame,
        api_version,
    } in requests
    {
        let conn = ctx
            .conn_pool
            .get_conn(host, now)
            .map_err(|e| e.with_broker_context(host, "Produce"))?;
        tracing::trace!("kp_send_request: sending {} bytes", frame.len());
        conn.send_request(&frame, ctx.correlation_id, api_version)
            .map_err(|e| e.with_broker_context(host, "Produce"))?;

        if ctx.no_acks {
            continue;
        }

        let kp_resp = transport::kp_get_response::<kafka_protocol::messages::ProduceResponse>(
            conn,
            api_version,
        )
        .map_err(|e| e.with_broker_context(host, "Produce"))?;
        res.extend(protocol::produce::into_produce_confirmations(kp_resp));
    }

    Ok(res)
}

fn prepare_produce_requests<'h>(
    ctx: &ProduceRequestContext<'_>,
    broker_msgs: BrokerMessages<'_, '_, 'h>,
) -> Result<Vec<PreparedProduceRequest<'h>>> {
    // Validate every record batch and request frame before opening any connection.
    // A later broker's local encoding error must not leave earlier messages sent.
    let mut requests = Vec::with_capacity(broker_msgs.len());
    for (host, msgs) in broker_msgs {
        let (mut header, request) = if ctx.timestamp == 0 {
            protocol::produce::build_produce_request(
                ctx.correlation_id,
                ctx.client_id,
                ctx.required_acks,
                ctx.ack_timeout_ms,
                ctx.compression,
                &msgs,
            )?
        } else {
            protocol::produce::build_produce_request_with_options(
                ctx.correlation_id,
                ctx.client_id,
                ctx.required_acks,
                ctx.ack_timeout_ms,
                ctx.compression,
                &msgs,
                protocol::produce::ProduceRequestOptions {
                    timestamp: ctx.timestamp,
                    ..Default::default()
                },
            )?
        };
        let api_version = transport::apply_request_api_version(
            ctx.api_versions,
            host,
            &mut header,
            crate::protocol::API_VERSION_PRODUCE,
        );
        let frame = protocol::encode_request_frame(&header, &request, api_version)
            .map_err(|e| e.with_broker_context(host, "Produce"))?;
        requests.push(PreparedProduceRequest {
            host,
            frame,
            api_version,
        });
    }
    Ok(requests)
}

/// Transactional sends are synchronous and never retried here. A transport,
/// decoding, or broker error must be handled by the transaction state machine.
pub(crate) fn produce_transactional_message(
    client: &mut super::KafkaClient,
    ack_timeout_ms: i32,
    context: protocol::produce::TransactionContext<'_>,
    message: &ProduceMessage<'_, '_>,
) -> Result<()> {
    let timestamp = sample_producer_timestamp(client.config.producer_timestamp)?;
    let host = client
        .state
        .find_broker(message.topic, message.partition)
        .ok_or(Error::Kafka(KafkaCode::UnknownTopicOrPartition))?
        .to_owned();
    let correlation = client.state.next_correlation_id();
    let (mut header, request) = protocol::produce::build_transactional_produce_request(
        correlation,
        &client.config.client_id,
        ack_timeout_ms,
        client.config.compression,
        (
            message.topic,
            message.partition,
            message.key,
            message.value,
            message.headers,
        ),
        context,
        timestamp,
    )?;
    let version = transport::apply_request_api_version(
        &client.api_versions,
        &host,
        &mut header,
        protocol::API_VERSION_PRODUCE,
    );
    let frame = protocol::encode_request_frame(&header, &request, version)
        .map_err(|err| err.with_broker_context(&host, "Produce"))?;
    let conn = client.conn_pool.get_conn(&host, Instant::now())?;
    tracing::trace!("kp_send_request: sending {} bytes", frame.len());
    conn.send_request(&frame, correlation, version)
        .map_err(|err| err.with_broker_context(&host, "Produce"))?;
    let response =
        protocol::transaction::read_response::<kafka_protocol::messages::ProduceResponse>(
            conn,
            correlation,
            version,
        )
        .map_err(|err| err.with_broker_context(&host, "Produce"))?;
    let result = validate_transactional_produce_response(&response, message);
    if matches!(&result, Err(Error::Protocol(_))) {
        let _ = conn.shutdown();
    }
    result
}

pub(crate) fn validate_producer_timestamp(
    timestamp: Option<protocol::produce::ProducerTimestamp>,
) -> Result<()> {
    if matches!(
        timestamp,
        Some(protocol::produce::ProducerTimestamp::LogAppendTime)
    ) {
        return Err(Error::Config(
            "LogAppendTime is a broker/topic policy, not a producer timestamp mode; configure message.timestamp.type=LogAppendTime on the topic and use CreateTime or None on the producer".into(),
        ));
    }
    Ok(())
}

fn sample_producer_timestamp(
    timestamp: Option<protocol::produce::ProducerTimestamp>,
) -> Result<i64> {
    validate_producer_timestamp(timestamp)?;
    if matches!(
        timestamp,
        Some(protocol::produce::ProducerTimestamp::CreateTime)
    ) {
        unix_timestamp_millis(SystemTime::now())
    } else {
        Ok(0)
    }
}

fn unix_timestamp_millis(time: SystemTime) -> Result<i64> {
    let elapsed = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Config("system clock is before the Unix epoch".into()))?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| Error::Config("Unix timestamp exceeds Kafka's i64 milliseconds range".into()))
}

fn validate_transactional_produce_response(
    response: &kafka_protocol::messages::ProduceResponse,
    message: &ProduceMessage<'_, '_>,
) -> Result<()> {
    let [topic] = response.responses.as_slice() else {
        return Err(Error::codec());
    };
    let [partition] = topic.partition_responses.as_slice() else {
        return Err(Error::codec());
    };
    if topic.name.as_str() != message.topic || partition.index != message.partition {
        return Err(Error::codec());
    }
    if partition.error_code != 0 {
        return Err(Error::Kafka(
            KafkaCode::from_protocol(partition.error_code).unwrap_or(KafkaCode::Unknown),
        ));
    }
    if partition.base_offset < 0 {
        return Err(Error::codec());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::thread::JoinHandle;

    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::{
        ApiKey, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};
    use kafka_protocol::records::{Record, RecordBatchDecoder, TimestampType};

    use super::*;
    use crate::client::KafkaClient;
    use crate::protocol::metadata::{
        BrokerMetadata, MetadataResponseData, PartitionMetadata, TopicMetadata,
    };
    use crate::protocol::produce::ProducerTimestamp;

    #[test]
    fn default_timestamp_mode_keeps_zero_in_produce_wire_records() {
        let records = capture_produced_records(None);
        assert_eq!(records.len(), 3);
        for record in records {
            assert_eq!(record.timestamp, 0);
            assert_eq!(record.timestamp_type, TimestampType::Creation);
        }
    }

    #[cfg(feature = "producer_timestamp")]
    #[test]
    fn create_time_is_sampled_once_for_all_messages_and_brokers() {
        let before = unix_timestamp_millis(SystemTime::now()).unwrap();
        let records = capture_produced_records(Some(ProducerTimestamp::CreateTime));
        let after = unix_timestamp_millis(SystemTime::now()).unwrap();
        assert_eq!(records.len(), 3);
        let timestamp = records[0].timestamp;
        assert!(timestamp > 0 && timestamp >= before && timestamp <= after);
        for record in records {
            assert_eq!(record.timestamp, timestamp);
            assert_eq!(record.timestamp_type, TimestampType::Creation);
        }
    }

    #[cfg(feature = "producer_timestamp")]
    #[test]
    fn log_append_time_is_rejected_before_opening_a_broker_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = KafkaClient::builder()
            .with_hosts(vec![address.to_string()])
            .with_producer_timestamp(Some(ProducerTimestamp::LogAppendTime))
            .build();
        configure_metadata(&mut client, &[address]);
        let result = client.produce_messages(
            RequiredAcks::One,
            Duration::from_secs(1),
            &[ProduceMessage {
                topic: "topic-a",
                partition: 0,
                key: None,
                value: Some(b"value"),
                headers: &[],
            }],
        );
        assert!(matches!(result, Err(Error::Config(message))
            if message.contains("message.timestamp.type=LogAppendTime")));
        assert!(
            listener
                .accept()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
        );
    }

    #[test]
    fn timestamps_before_the_unix_epoch_return_a_configuration_error() {
        assert!(matches!(
            unix_timestamp_millis(UNIX_EPOCH - Duration::from_millis(1)),
            Err(Error::Config(_))
        ));
        assert_eq!(sample_producer_timestamp(None).unwrap(), 0);
    }

    #[test]
    fn all_broker_frames_are_preflighted_before_opening_connections() {
        let first = TcpListener::bind("127.0.0.1:0").unwrap();
        let second = TcpListener::bind("127.0.0.1:0").unwrap();
        first.set_nonblocking(true).unwrap();
        second.set_nonblocking(true).unwrap();
        let addresses = [first.local_addr().unwrap(), second.local_addr().unwrap()];
        let long_topic = "x".repeat(usize::try_from(i16::MAX).unwrap() + 1);
        let mut client = KafkaClient::builder().with_conn_rw_timeout(1).build();
        configure_metadata_for_topics(&mut client, &addresses, &["topic-a", &long_topic]);
        client.api_versions.insert_api_versions(
            addresses[1].to_string(),
            &[protocol::api_versions::BrokerApiVersion {
                api_key: ApiKey::Produce as i16,
                min_version: 3,
                max_version: 8,
            }],
        );
        let messages = [
            ProduceMessage {
                topic: "topic-a",
                partition: 0,
                key: None,
                value: Some(b"valid"),
                headers: &[],
            },
            ProduceMessage {
                topic: &long_topic,
                partition: 1,
                key: None,
                value: Some(b"invalid-frame"),
                headers: &[],
            },
        ];
        assert!(
            client
                .produce_messages(RequiredAcks::One, Duration::from_secs(1), &messages)
                .is_err()
        );
        for listener in [&first, &second] {
            assert!(
                listener
                    .accept()
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
            );
        }
    }

    #[cfg(not(feature = "gzip"))]
    #[test]
    fn disabled_record_codec_is_rejected_before_connecting_to_any_broker() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut client = KafkaClient::builder().with_conn_rw_timeout(1).build();
        configure_metadata(&mut client, &[listener.local_addr().unwrap()]);
        client.set_compression(Compression::GZIP);
        let messages = [ProduceMessage {
            topic: "topic-a",
            partition: 0,
            key: None,
            value: Some(b"value"),
            headers: &[],
        }];
        assert!(matches!(
            client.produce_messages(RequiredAcks::One, Duration::from_secs(1), &messages),
            Err(Error::Protocol(
                crate::error::ProtocolError::UnsupportedCompression
            ))
        ));
        assert!(
            listener
                .accept()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
        );
    }

    fn capture_produced_records(mode: Option<ProducerTimestamp>) -> Vec<Record> {
        let (first, first_server) = mock_produce_broker();
        let (second, second_server) = mock_produce_broker();
        let builder = KafkaClient::builder()
            .with_hosts(vec![first.to_string(), second.to_string()])
            .with_conn_rw_timeout(5);
        #[cfg(feature = "producer_timestamp")]
        let builder = builder.with_producer_timestamp(mode);
        #[cfg(not(feature = "producer_timestamp"))]
        assert!(mode.is_none());
        let mut client = builder.build();
        configure_metadata(&mut client, &[first, second]);
        let messages = [
            ProduceMessage {
                topic: "topic-a",
                partition: 0,
                key: None,
                value: Some(b"first"),
                headers: &[],
            },
            ProduceMessage {
                topic: "topic-a",
                partition: 1,
                key: None,
                value: Some(b"other-partition"),
                headers: &[],
            },
            ProduceMessage {
                topic: "topic-a",
                partition: 0,
                key: None,
                value: Some(b"second"),
                headers: &[],
            },
        ];
        let confirms = client
            .produce_messages(RequiredAcks::One, Duration::from_secs(1), &messages)
            .unwrap();
        assert_eq!(confirms.len(), 2);
        let mut records = first_server.join().unwrap();
        records.extend(second_server.join().unwrap());
        records
    }

    fn configure_metadata(client: &mut KafkaClient, addresses: &[SocketAddr]) {
        configure_metadata_for_topics(client, addresses, &["topic-a", "topic-b"]);
    }

    fn configure_metadata_for_topics(
        client: &mut KafkaClient,
        addresses: &[SocketAddr],
        topics: &[&str],
    ) {
        client.state.update_metadata(MetadataResponseData {
            brokers: addresses
                .iter()
                .enumerate()
                .map(|(index, address)| BrokerMetadata {
                    node_id: i32::try_from(index).unwrap() + 1,
                    host: address.ip().to_string(),
                    port: i32::from(address.port()),
                })
                .collect(),
            topics: topics
                .iter()
                .map(|topic| TopicMetadata {
                    topic: (*topic).to_owned(),
                    partitions: (0..addresses.len())
                        .map(|index| PartitionMetadata {
                            id: i32::try_from(index).unwrap(),
                            leader: i32::try_from(index).unwrap() + 1,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
        });
    }

    fn mock_produce_broker() -> (SocketAddr, JoinHandle<Vec<Record>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut size = [0; 4];
            stream.read_exact(&mut size).unwrap();
            let mut payload = vec![0; usize::try_from(i32::from_be_bytes(size)).unwrap()];
            stream.read_exact(&mut payload).unwrap();
            let version = i16::from_be_bytes(payload[2..4].try_into().unwrap());
            let mut payload = Bytes::from(payload);
            let header =
                RequestHeader::decode(&mut payload, ProduceRequest::header_version(version))
                    .unwrap();
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let request = ProduceRequest::decode(&mut payload, version).unwrap();
            assert!(payload.is_empty());
            let mut records = Vec::new();
            let responses = request.topic_data.into_iter().map(|topic| {
                kafka_protocol::messages::produce_response::TopicProduceResponse::default()
                    .with_name(topic.name)
                    .with_partition_responses(topic.partition_data.into_iter().map(|partition| {
                        let mut bytes = partition.records.unwrap();
                        for batch in RecordBatchDecoder::decode_all(&mut bytes).unwrap() {
                            records.extend(batch.records);
                        }
                        kafka_protocol::messages::produce_response::PartitionProduceResponse::default()
                            .with_index(partition.index).with_base_offset(0)
                    }).collect())
            }).collect();
            if request.acks == 0 {
                return records;
            }
            let response = ProduceResponse::default().with_responses(responses);
            let mut payload = BytesMut::new();
            ResponseHeader::default()
                .with_correlation_id(header.correlation_id)
                .encode(&mut payload, ProduceResponse::header_version(version))
                .unwrap();
            response.encode(&mut payload, version).unwrap();
            stream
                .write_all(&i32::try_from(payload.len()).unwrap().to_be_bytes())
                .unwrap();
            stream.write_all(&payload).unwrap();
            records
        });
        (address, server)
    }

    #[cfg(feature = "metrics")]
    mod produce_metrics_tests {
        use super::*;
        use metrics::{
            Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
        };
        use std::sync::atomic::Ordering;
        use std::sync::{Arc, Mutex};

        #[derive(Default)]
        struct CounterRecorder {
            counters: Mutex<HashMap<Key, Arc<metrics::atomics::AtomicU64>>>,
        }

        impl Recorder for CounterRecorder {
            fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
                let counter = self
                    .counters
                    .lock()
                    .unwrap()
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(metrics::atomics::AtomicU64::new(0)))
                    .clone();
                Counter::from_arc(counter)
            }
            fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
                Gauge::noop()
            }
            fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
                Histogram::noop()
            }
        }

        impl CounterRecorder {
            fn value(&self, name: &str, topic: &str) -> u64 {
                self.counters
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|(key, _)| {
                        key.name() == name
                            && key
                                .labels()
                                .any(|label| label.key() == "topic" && label.value() == topic)
                    })
                    .map_or(0, |(_, counter)| counter.load(Ordering::Relaxed))
            }
        }

        #[test]
        fn counters_are_per_input_topic_across_brokers_with_and_without_acks() {
            for acks in [RequiredAcks::One, RequiredAcks::None] {
                let recorder = CounterRecorder::default();
                metrics::with_local_recorder(&recorder, || {
                    let (first, first_server) = mock_produce_broker();
                    let (second, second_server) = mock_produce_broker();
                    let mut client = KafkaClient::builder().with_conn_rw_timeout(5).build();
                    configure_metadata(&mut client, &[first, second]);
                    let messages = [
                        ProduceMessage {
                            topic: "topic-a",
                            partition: 0,
                            key: Some(b"key"),
                            value: Some(b"aaa"),
                            headers: &[],
                        },
                        ProduceMessage {
                            topic: "topic-b",
                            partition: 0,
                            key: None,
                            value: Some(b"ccccccc"),
                            headers: &[],
                        },
                        ProduceMessage {
                            topic: "topic-a",
                            partition: 1,
                            key: None,
                            value: Some(b"bb"),
                            headers: &[],
                        },
                    ];
                    let confirms = client
                        .produce_messages(acks, Duration::from_secs(1), &messages)
                        .unwrap();
                    assert_eq!(confirms.is_empty(), matches!(acks, RequiredAcks::None));
                    assert_eq!(first_server.join().unwrap().len(), 2);
                    assert_eq!(second_server.join().unwrap().len(), 1);
                });
                assert_eq!(recorder.value("kafka.produce.messages_total", "topic-a"), 2);
                assert_eq!(recorder.value("kafka.produce.messages_total", "topic-b"), 1);
                assert_eq!(recorder.value("kafka.produce.bytes_total", "topic-a"), 5);
                assert_eq!(recorder.value("kafka.produce.bytes_total", "topic-b"), 7);
                assert_eq!(
                    recorder.value("kafka.produce.messages_total", "_unknown"),
                    0
                );
            }
        }

        #[test]
        fn local_encoding_failure_keeps_the_existing_error_metric() {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let mut client = KafkaClient::builder()
                .with_conn_rw_timeout(1)
                .with_client_id("x".repeat(usize::try_from(i16::MAX).unwrap() + 1))
                .build();
            configure_metadata(&mut client, &[listener.local_addr().unwrap()]);
            let recorder = CounterRecorder::default();
            metrics::with_local_recorder(&recorder, || {
                let messages = [ProduceMessage {
                    topic: "topic-a",
                    partition: 0,
                    key: None,
                    value: Some(b"value"),
                    headers: &[],
                }];
                assert!(
                    client
                        .produce_messages(RequiredAcks::One, Duration::from_secs(1), &messages)
                        .is_err()
                );
            });
            assert_eq!(recorder.value("kafka.produce.errors_total", "_unknown"), 1);
            assert_eq!(recorder.value("kafka.produce.messages_total", "topic-a"), 0);
            assert!(
                listener
                    .accept()
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
            );
        }
    }
}
