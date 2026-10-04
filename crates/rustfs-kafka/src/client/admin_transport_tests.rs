use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, CreateTopicsRequest, CreateTopicsResponse, DescribeClusterRequest,
    DescribeClusterResponse, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

use super::{KafkaClient, admin_request_is_read_only};
use crate::error::Result;

const CREATE_TOPICS_VERSION: i16 = 2;

#[derive(Clone, Copy)]
enum ResponseFault {
    Disconnect,
    Correlation,
    Trailing,
    NegativeLength,
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap()
}

fn accept(stream: &TcpListener) -> TcpStream {
    stream.set_nonblocking(true).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        match stream.accept() {
            Ok((socket, _)) => {
                socket.set_nonblocking(false).unwrap();
                return socket;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "mock accept timed out"
                );
                std::thread::park_timeout(Duration::from_millis(1));
            }
            Err(error) => panic!("mock accept failed: {error}"),
        }
    }
}

fn client(hosts: Vec<String>) -> KafkaClient {
    KafkaClient::builder()
        .with_hosts(hosts)
        .with_conn_rw_timeout(2)
        .build()
}

fn header(key: ApiKey, correlation: i32, client_id: &str) -> RequestHeader {
    RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(0)
        .with_correlation_id(correlation)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
}

fn read_request<R: Decodable + HeaderVersion>(stream: &mut TcpStream) -> RequestHeader {
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut length = [0; 4];
    stream.read_exact(&mut length).unwrap();
    let mut payload = vec![0; usize::try_from(i32::from_be_bytes(length)).unwrap()];
    stream.read_exact(&mut payload).unwrap();
    let version = i16::from_be_bytes(payload[2..4].try_into().unwrap());
    let mut payload = Bytes::from(payload);
    let header = RequestHeader::decode(&mut payload, R::header_version(version)).unwrap();
    R::decode(&mut payload, version).unwrap();
    assert!(payload.is_empty());
    header
}

fn write_response<R: Encodable + HeaderVersion>(
    stream: &mut TcpStream,
    header: &RequestHeader,
    response: &R,
    fault: Option<ResponseFault>,
) {
    if matches!(fault, Some(ResponseFault::Disconnect)) {
        return;
    }
    if matches!(fault, Some(ResponseFault::NegativeLength)) {
        stream.write_all(&(-1i32).to_be_bytes()).unwrap();
        return;
    }
    let mut bytes = BytesMut::new();
    let correlation =
        header.correlation_id + i32::from(matches!(fault, Some(ResponseFault::Correlation)));
    ResponseHeader::default()
        .with_correlation_id(correlation)
        .encode(&mut bytes, R::header_version(header.request_api_version))
        .unwrap();
    response
        .encode(&mut bytes, header.request_api_version)
        .unwrap();
    if matches!(fault, Some(ResponseFault::Trailing)) {
        bytes.extend_from_slice(&[0]);
    }
    stream
        .write_all(&i32::try_from(bytes.len()).unwrap().to_be_bytes())
        .unwrap();
    stream.write_all(&bytes).unwrap();
}

fn create_request(client: &mut KafkaClient) -> Result<CreateTopicsResponse> {
    client.try_admin_request(
        "CreateTopics",
        CREATE_TOPICS_VERSION,
        |correlation, client_id| {
            let topic = kafka_protocol::messages::create_topics_request::CreatableTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("topic-a")))
                .with_num_partitions(1)
                .with_replication_factor(1);
            (
                header(ApiKey::CreateTopics, correlation, client_id),
                CreateTopicsRequest::default().with_topics(vec![topic]),
            )
        },
        |response| response,
    )
}

fn successful_create_broker(listener: TcpListener) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut stream = accept(&listener);
        let header = read_request::<CreateTopicsRequest>(&mut stream);
        assert_eq!(header.request_api_key, ApiKey::CreateTopics as i16);
        write_response(&mut stream, &header, &CreateTopicsResponse::default(), None);
    })
}

#[test]
fn uncertain_mutations_are_not_replayed_after_transport_or_frame_failures() {
    for fault in [
        ResponseFault::Disconnect,
        ResponseFault::Correlation,
        ResponseFault::Trailing,
        ResponseFault::NegativeLength,
    ] {
        let first = listener();
        let backup = listener();
        backup.set_nonblocking(true).unwrap();
        let mut client = client(vec![
            first.local_addr().unwrap().to_string(),
            backup.local_addr().unwrap().to_string(),
        ]);
        let server = std::thread::spawn(move || {
            let mut stream = accept(&first);
            let header = read_request::<CreateTopicsRequest>(&mut stream);
            assert_eq!(header.request_api_key, ApiKey::CreateTopics as i16);
            write_response(
                &mut stream,
                &header,
                &CreateTopicsResponse::default(),
                Some(fault),
            );
        });
        assert!(create_request(&mut client).is_err());
        server.join().unwrap();
        assert_eq!(
            backup.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}

#[test]
fn mutation_can_select_another_bootstrap_before_a_sending_attempt() {
    let unavailable = listener();
    let unavailable_host = unavailable.local_addr().unwrap().to_string();
    drop(unavailable);
    let backup = listener();
    let mut client = client(vec![
        unavailable_host,
        backup.local_addr().unwrap().to_string(),
    ]);
    let server = successful_create_broker(backup);
    assert!(create_request(&mut client).is_ok());
    server.join().unwrap();
}

#[test]
fn read_only_admin_request_can_fail_over_after_a_response_disconnect() {
    let first = listener();
    let backup = listener();
    let mut client = client(vec![
        first.local_addr().unwrap().to_string(),
        backup.local_addr().unwrap().to_string(),
    ]);
    let first_server = std::thread::spawn(move || {
        let mut stream = accept(&first);
        let header = read_request::<DescribeClusterRequest>(&mut stream);
        assert_eq!(header.request_api_key, ApiKey::DescribeCluster as i16);
    });
    let backup_server = std::thread::spawn(move || {
        let mut stream = accept(&backup);
        let header = read_request::<DescribeClusterRequest>(&mut stream);
        let response = DescribeClusterResponse::default()
            .with_cluster_id(StrBytes::from_static_str("backup-cluster"));
        write_response(&mut stream, &header, &response, None);
    });
    let response: DescribeClusterResponse = client
        .try_admin_request(
            "DescribeCluster",
            0,
            |correlation, client_id| {
                (
                    header(ApiKey::DescribeCluster, correlation, client_id),
                    DescribeClusterRequest::default(),
                )
            },
            |response| response,
        )
        .unwrap();
    assert_eq!(response.cluster_id.as_str(), "backup-cluster");
    first_server.join().unwrap();
    backup_server.join().unwrap();
}

#[test]
fn admin_encoding_failure_does_not_open_a_broker_connection() {
    let broker = listener();
    broker.set_nonblocking(true).unwrap();
    let mut client = client(vec![broker.local_addr().unwrap().to_string()]);
    let result: Result<CreateTopicsResponse> = client.try_admin_request(
        "CreateTopics",
        CREATE_TOPICS_VERSION,
        |correlation, client_id| {
            let topic = kafka_protocol::messages::create_topics_request::CreatableTopic::default()
                .with_name(TopicName::from(StrBytes::from_string("x".repeat(32_768))));
            (
                header(ApiKey::CreateTopics, correlation, client_id),
                CreateTopicsRequest::default().with_topics(vec![topic]),
            )
        },
        |response| response,
    );
    assert!(result.is_err());
    assert_eq!(
        broker.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn unknown_and_mutating_admin_keys_are_conservative() {
    for key in [
        ApiKey::CreateTopics,
        ApiKey::DeleteAcls,
        ApiKey::AlterConfigs,
        ApiKey::RenewDelegationToken,
        ApiKey::PushTelemetry,
    ] {
        assert!(!admin_request_is_read_only(key as i16));
    }
    assert!(!admin_request_is_read_only(i16::MAX));
    assert!(admin_request_is_read_only(ApiKey::DescribeCluster as i16));
}
