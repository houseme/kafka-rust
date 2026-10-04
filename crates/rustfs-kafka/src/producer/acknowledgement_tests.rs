use super::*;
use crate::error::KafkaCode;
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, MetadataRequest, MetadataResponse,
    ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::{Record as WireRecord, RecordBatchDecoder};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::thread::JoinHandle;
use std::time::Duration;

struct ResolvedPartitioner;

impl Partitioner for ResolvedPartitioner {
    fn partition(&mut self, _: Topics<'_>, message: &mut client::ProduceMessage<'_, '_>) {
        if message.partition < 0 {
            message.partition = 1;
        }
    }
}

#[test]
fn single_send_rejects_malformed_confirmations_without_panicking_or_replaying() {
    let malformed = vec![
        ProduceResponse::default(),
        empty_topic_response("t"),
        response(&[("wrong-topic", 1, 0, 42)]),
        response(&[("t", 0, 0, 42)]),
        response(&[("t", 1, 0, 42), ("t", 1, 0, 42)]),
        response(&[("t", 1, 0, 42), ("t", 0, 0, 42)]),
    ];
    for malformed in malformed {
        let (mut producer, server) = mock_producer(Some(malformed), RequiredAcks::One, true);
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            producer.send(&Record::from_value("t", "value"))
        }));
        assert!(matches!(
            outcome,
            Ok(Err(Error::Protocol(crate::error::ProtocolError::Codec)))
        ));
        // Keep the producer alive while proving semantic failures close only its
        // existing connection, without an automatic replay or new connection.
        assert_eq!(server.join().unwrap().len(), 1);
    }
}

#[test]
fn send_all_detects_an_omitted_resolved_target() {
    let (mut producer, server) =
        mock_producer(Some(response(&[("t", 0, 0, 42)])), RequiredAcks::One, true);
    let records = [
        Record::from_value("t", "first").with_partition(0),
        Record::from_value("t", "second"),
    ];
    assert!(matches!(
        producer.send_all(&records),
        Err(Error::Protocol(crate::error::ProtocolError::Codec))
    ));
    assert_eq!(server.join().unwrap().len(), 2);
}

#[test]
fn send_all_preserves_partition_errors_and_negative_success_offsets() {
    for error_code in [0, KafkaCode::NotLeaderForPartition as i16] {
        let (mut producer, server) = mock_producer(
            Some(response(&[("t", 0, error_code, -1), ("t", 1, 0, 42)])),
            RequiredAcks::One,
            false,
        );
        let records = [
            Record::from_value("t", "first").with_partition(0),
            Record::from_value("t", "second"),
        ];
        let confirms = producer.send_all(&records).unwrap();
        assert_eq!(confirms.len(), 1);
        assert_eq!(
            confirms[0].partition_confirms[0].offset,
            Err(if error_code == 0 {
                KafkaCode::Unknown
            } else {
                KafkaCode::NotLeaderForPartition
            })
        );
        assert_eq!(confirms[0].partition_confirms[1].offset, Ok(42));
        assert_eq!(server.join().unwrap().len(), 2);
    }
}

#[test]
fn ack_zero_empty_payload_completes_a_write_without_waiting_for_a_response() {
    let (mut producer, server) = mock_producer(None, RequiredAcks::None, false);
    producer.send(&Record::from_value("t", "")).unwrap();
    let records = server.join().unwrap();
    assert_eq!(records.len(), 1);
    // Preserve the existing AsBytes contract: empty high-level values are null.
    assert!(records[0].key.is_none());
    assert!(records[0].value.is_none());
}

#[test]
fn empty_send_all_succeeds_without_contacts_in_both_ack_modes() {
    for acks in [RequiredAcks::None, RequiredAcks::One] {
        let mut producer = Producer::from_client(KafkaClient::new(Vec::new()))
            .with_required_acks(acks)
            .create()
            .unwrap();
        let records: [Record<'_, (), ()>; 0] = [];
        assert!(producer.send_all(&records).unwrap().is_empty());
    }
}

#[test]
fn aggregate_ack_guard_allows_disjoint_same_topic_wrappers_from_different_brokers() {
    let expected = HashSet::from([("t", 0), ("t", 1)]);
    let confirms = vec![
        ProduceConfirm {
            topic: "t".into(),
            partition_confirms: vec![ProducePartitionConfirm {
                partition: 0,
                offset: Ok(42),
            }],
        },
        ProduceConfirm {
            topic: "t".into(),
            partition_confirms: vec![ProducePartitionConfirm {
                partition: 1,
                offset: Ok(51),
            }],
        },
    ];
    assert!(validate_produce_acknowledgements(&confirms, &expected).is_ok());
}

fn mock_producer(
    response: Option<ProduceResponse>,
    acks: RequiredAcks,
    expect_retired: bool,
) -> (Producer<ResolvedPartitioner>, JoinHandle<Vec<WireRecord>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let (header, _) = read_request::<ApiVersionsRequest>(&mut stream, ApiKey::ApiVersions);
        write_response(&mut stream, &header, &ApiVersionsResponse::default());
        let (header, _) = read_request::<MetadataRequest>(&mut stream, ApiKey::Metadata);
        write_response(&mut stream, &header, &metadata_response(address));
        let (header, request) = read_request::<ProduceRequest>(&mut stream, ApiKey::Produce);
        let mut records = Vec::new();
        for topic in request.topic_data {
            assert_eq!(topic.name.as_str(), "t");
            for partition in topic.partition_data {
                assert!(matches!(partition.index, 0 | 1));
                let mut bytes = partition.records.unwrap();
                for batch in RecordBatchDecoder::decode_all(&mut bytes).unwrap() {
                    records.extend(batch.records);
                }
            }
        }
        if let Some(response) = response {
            assert_ne!(request.acks, 0);
            write_response(&mut stream, &header, &response);
        } else {
            assert_eq!(request.acks, 0);
        }
        if expect_retired {
            let mut marker = [0];
            match stream.read(&mut marker) {
                Ok(0) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                other => panic!("semantic failure reused a connection: {other:?}"),
            }
            listener.set_nonblocking(true).unwrap();
            assert!(
                listener
                    .accept()
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
            );
        }
        records
    });
    let producer = Producer::from_hosts(vec![address.to_string()])
        .with_required_acks(acks)
        .with_partitioner(ResolvedPartitioner)
        .create()
        .unwrap();
    (producer, server)
}

fn read_request<T: Decodable + HeaderVersion>(
    stream: &mut TcpStream,
    key: ApiKey,
) -> (RequestHeader, T) {
    let mut size = [0; 4];
    stream.read_exact(&mut size).unwrap();
    let mut frame = vec![0; usize::try_from(i32::from_be_bytes(size)).unwrap()];
    stream.read_exact(&mut frame).unwrap();
    let version = i16::from_be_bytes(frame[2..4].try_into().unwrap());
    let mut frame = Bytes::from(frame);
    let header = RequestHeader::decode(&mut frame, T::header_version(version)).unwrap();
    assert_eq!(header.request_api_key, key as i16);
    (header, T::decode(&mut frame, version).unwrap())
}

fn write_response<T: Encodable + HeaderVersion>(
    stream: &mut TcpStream,
    request: &RequestHeader,
    response: &T,
) {
    let mut payload = BytesMut::new();
    let version = request.request_api_version;
    ResponseHeader::default()
        .with_correlation_id(request.correlation_id)
        .encode(&mut payload, T::header_version(version))
        .unwrap();
    response.encode(&mut payload, version).unwrap();
    stream
        .write_all(&i32::try_from(payload.len()).unwrap().to_be_bytes())
        .unwrap();
    stream.write_all(&payload).unwrap();
}

fn topic_name(name: &'static str) -> TopicName {
    TopicName::from(StrBytes::from_static_str(name))
}

fn metadata_response(address: SocketAddr) -> MetadataResponse {
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    MetadataResponse::default()
        .with_brokers(vec![
            MetadataResponseBroker::default()
                .with_node_id(BrokerId::from(1))
                .with_host(StrBytes::from_string(address.ip().to_string()))
                .with_port(i32::from(address.port())),
        ])
        .with_topics(vec![
            MetadataResponseTopic::default()
                .with_name(Some(topic_name("t")))
                .with_partitions(
                    [0, 1]
                        .into_iter()
                        .map(|partition| {
                            MetadataResponsePartition::default()
                                .with_partition_index(partition)
                                .with_leader_id(BrokerId::from(1))
                                .with_replica_nodes(vec![BrokerId::from(1)])
                                .with_isr_nodes(vec![BrokerId::from(1)])
                        })
                        .collect(),
                ),
        ])
}

fn response(rows: &[(&'static str, i32, i16, i64)]) -> ProduceResponse {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    let mut topics = BTreeMap::new();
    for &(topic, partition, error, offset) in rows {
        topics.entry(topic).or_insert_with(Vec::new).push(
            PartitionProduceResponse::default()
                .with_index(partition)
                .with_error_code(error)
                .with_base_offset(offset),
        );
    }
    ProduceResponse::default().with_responses(
        topics
            .into_iter()
            .map(|(name, partitions)| {
                TopicProduceResponse::default()
                    .with_name(topic_name(name))
                    .with_partition_responses(partitions)
            })
            .collect(),
    )
}

fn empty_topic_response(name: &'static str) -> ProduceResponse {
    ProduceResponse::default().with_responses(vec![
        kafka_protocol::messages::produce_response::TopicProduceResponse::default()
            .with_name(topic_name(name)),
    ])
}
