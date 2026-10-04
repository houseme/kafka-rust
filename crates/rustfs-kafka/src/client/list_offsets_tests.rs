use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{
    ApiKey, ListOffsetsRequest, ListOffsetsResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

use super::validate_list_offsets_response;
use crate::client::{FetchOffset, KafkaClient};
use crate::error::{Error, KafkaCode, ProtocolError};
use crate::protocol::metadata::{
    BrokerMetadata, MetadataResponseData, PartitionMetadata, TopicMetadata,
};

type Row = (&'static str, i32, i16, i64, i64);

fn response(rows: &[Row]) -> ListOffsetsResponse {
    let mut topics: Vec<ListOffsetsTopicResponse> = Vec::new();
    for &(topic, partition, error, offset, timestamp) in rows {
        let index = topics
            .iter()
            .position(|existing| existing.name.as_str() == topic)
            .unwrap_or_else(|| {
                topics.push(
                    ListOffsetsTopicResponse::default()
                        .with_name(StrBytes::from_static_str(topic).into()),
                );
                topics.len() - 1
            });
        topics[index].partitions.push(
            ListOffsetsPartitionResponse::default()
                .with_partition_index(partition)
                .with_error_code(error)
                .with_offset(offset)
                .with_timestamp(timestamp),
        );
    }
    ListOffsetsResponse::default().with_topics(topics)
}

fn malformed_responses() -> Vec<ListOffsetsResponse> {
    let good = [("t", 0, 0, 5, 7), ("t", 1, 0, 10, 8)];
    let mut repeated = response(&good[..1]);
    repeated.topics.extend(response(&good[1..]).topics);
    let mut repeated_empty = response(&good);
    repeated_empty
        .topics
        .push(ListOffsetsTopicResponse::default().with_name(StrBytes::from_static_str("t").into()));
    let mut extra_empty = response(&good);
    extra_empty.topics.push(
        ListOffsetsTopicResponse::default().with_name(StrBytes::from_static_str("extra").into()),
    );
    vec![
        response(&[]),
        response(&good[..1]),
        response(&[good[0], good[0], good[1]]),
        response(&[good[0], good[1], ("t", 2, 0, 99, 9)]),
        response(&[good[0], ("extra", 1, 0, 99, 9)]),
        response(&[("t", -1, 0, 5, 7), good[1]]),
        response(&[("t", 0, 0, -2, 7), good[1]]),
        response(&[("t", 0, 0, i64::MIN, 7), good[1]]),
        // An explicit retriable error must not hide another malformed success.
        response(&[("t", 0, 0, -2, 7), ("t", 1, 6, -1, -1)]),
        repeated,
        repeated_empty,
        extra_empty,
    ]
}

#[test]
fn list_offsets_requires_complete_unique_targets_and_valid_successful_offsets() {
    let requested = [("t", 0, -1), ("t", 1, -1)];
    for response in malformed_responses() {
        assert!(matches!(
            validate_list_offsets_response(&response, &requested),
            Err(Error::Protocol(ProtocolError::Codec))
        ));
    }
    for offset in [-1, 0, i64::MAX] {
        let valid = response(&[("t", 1, 0, offset, -1), ("t", 0, 6, -2, -1)]);
        assert!(validate_list_offsets_response(&valid, &requested).is_ok());
    }
}

fn broker(
    response: ListOffsetsResponse,
    expected_partitions: Vec<i32>,
    expect_retirement: bool,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut size = [0; 4];
        stream.read_exact(&mut size).unwrap();
        let mut bytes = vec![0; usize::try_from(i32::from_be_bytes(size)).unwrap()];
        stream.read_exact(&mut bytes).unwrap();
        let version = i16::from_be_bytes(bytes[2..4].try_into().unwrap());
        let mut bytes = Bytes::from(bytes);
        let header =
            RequestHeader::decode(&mut bytes, ListOffsetsRequest::header_version(version)).unwrap();
        assert_eq!(header.request_api_key, ApiKey::ListOffsets as i16);
        let request = ListOffsetsRequest::decode(&mut bytes, version).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].name.as_str(), "t");
        let partitions: Vec<_> = request.topics[0]
            .partitions
            .iter()
            .map(|partition| partition.partition_index)
            .collect();
        assert_eq!(partitions, expected_partitions);
        let mut bytes = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(header.correlation_id)
            .encode(&mut bytes, ListOffsetsResponse::header_version(version))
            .unwrap();
        response.encode(&mut bytes, version).unwrap();
        stream
            .write_all(&i32::try_from(bytes.len()).unwrap().to_be_bytes())
            .unwrap();
        stream.write_all(&bytes).unwrap();
        if expect_retirement {
            // The caller keeps its client alive through join: EOF proves shutdown.
            match stream.read(&mut size) {
                Ok(0) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                other => panic!("malformed ListOffsets was retained or replayed: {other:?}"),
            }
            listener.set_nonblocking(true).unwrap();
            assert!(
                listener
                    .accept()
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
            );
        }
    });
    (address, server)
}

fn client(addresses: &[SocketAddr]) -> KafkaClient {
    let mut client = KafkaClient::builder().with_conn_rw_timeout(2).build();
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
        topics: vec![TopicMetadata {
            error: 0,
            topic: "t".into(),
            partitions: (0..2)
                .map(|partition| PartitionMetadata {
                    error: 0,
                    id: partition,
                    leader: if addresses.len() == 1 {
                        1
                    } else {
                        partition + 1
                    },
                })
                .collect(),
        }],
    });
    client
}

#[test]
fn malformed_list_offsets_closes_the_connection_without_partial_results_or_replay() {
    for response in malformed_responses() {
        let (address, server) = broker(response, vec![0, 1], true);
        let mut client = client(&[address]);
        let result = client.fetch_offsets(&["t"], FetchOffset::Latest);
        assert!(
            matches!(result, Err(Error::BrokerRequestError { api_key: "ListOffsets", source, .. })
            if matches!(source.as_ref(), Error::Protocol(ProtocolError::Codec)))
        );
        server.join().unwrap();
    }
}

#[test]
fn duplicate_input_topics_query_once_and_raw_sentinel_timestamp_and_max_are_preserved() {
    for offset in [-1, 0, i64::MAX] {
        let (address, server) = broker(
            response(&[("t", 1, 0, offset, -1), ("t", 0, 0, 5, 123)]),
            vec![0, 1],
            false,
        );
        let mut client = client(&[address]);
        let offsets = client
            .list_offsets(&["t", "t"], FetchOffset::ByTime(999))
            .unwrap();
        let mut values: Vec<_> = offsets["t"]
            .iter()
            .map(|value| (value.partition, value.offset, value.time))
            .collect();
        values.sort_unstable();
        assert_eq!(values, [(0, 5, 123), (1, offset, -1)]);
        server.join().unwrap();
    }
}

#[test]
fn complete_list_offsets_preserves_known_and_unknown_nonzero_errors() {
    for (code, expected) in [
        (6, KafkaCode::NotLeaderForPartition),
        (12345, KafkaCode::Unknown),
    ] {
        let (address, server) = broker(
            response(&[("t", 0, 0, 5, 7), ("t", 1, code, -1, -1)]),
            vec![0, 1],
            false,
        );
        let mut client = client(&[address]);
        assert!(matches!(
            client.fetch_offsets(&["t"], FetchOffset::Latest),
            Err(Error::TopicPartitionError { topic_name, partition_id: 1, error_code })
            if topic_name == "t" && error_code == expected
        ));
        server.join().unwrap();
    }
}

#[test]
fn one_topic_spread_across_brokers_merges_distinct_partition_results() {
    let (first, first_server) = broker(response(&[("t", 0, 0, 5, 7)]), vec![0], false);
    let (second, second_server) = broker(response(&[("t", 1, 0, 10, 8)]), vec![1], false);
    let mut client = client(&[first, second]);
    let offsets = client.fetch_offsets(&["t"], FetchOffset::Latest).unwrap();
    let mut values: Vec<_> = offsets["t"]
        .iter()
        .map(|value| (value.partition, value.offset))
        .collect();
    values.sort_unstable();
    assert_eq!(values, [(0, 5), (1, 10)]);
    first_server.join().unwrap();
    second_server.join().unwrap();
}
