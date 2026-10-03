//! Low-level Kafka protocol transport utilities.
//!
//! Provides functions for sending requests and receiving responses over
//! a [`KafkaConnection`], handling the Kafka wire protocol frame format
//! (4-byte length prefix + header + body).

use tracing::trace;

use crate::error::Result;

use crate::network::KafkaConnection;

pub(crate) fn apply_request_api_version(
    api_versions: &crate::protocol::api_versions::ApiVersionCache,
    host: &str,
    header: &mut kafka_protocol::messages::RequestHeader,
    fallback: i16,
) -> i16 {
    let api_version = api_versions.negotiate(host, header.request_api_key, fallback);
    header.request_api_version = api_version;
    api_version
}

pub(crate) fn kp_send_request<T>(
    conn: &mut KafkaConnection,
    header: &kafka_protocol::messages::RequestHeader,
    body: &T,
    api_version: i16,
) -> Result<()>
where
    T: kafka_protocol::protocol::Encodable + kafka_protocol::protocol::HeaderVersion,
{
    if header.request_api_version != api_version {
        return Err(crate::error::Error::codec());
    }
    let out = crate::protocol::encode_request_frame(header, body, api_version)?;
    trace!("kp_send_request: sending {} bytes", out.len());
    conn.send_request(&out, header.correlation_id, api_version)
}

pub(crate) fn kp_get_response<
    R: kafka_protocol::protocol::Decodable + kafka_protocol::protocol::HeaderVersion,
>(
    conn: &mut KafkaConnection,
    api_version: i16,
) -> Result<R> {
    conn.read_response(api_version)
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::{
        ApiKey, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
        ProduceRequest, RequestHeader, ResponseHeader,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;

    use super::*;
    use crate::client::KafkaClient;

    fn client() -> KafkaClient {
        KafkaClient::builder().with_conn_rw_timeout(2).build()
    }

    fn read_request(stream: &mut TcpStream, key: ApiKey) -> (RequestHeader, Bytes) {
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
            RequestHeader::decode(&mut bytes, key.request_header_version(version)).unwrap();
        assert_eq!(header.request_api_key, key as i16);
        (header, bytes)
    }

    fn response_payload<R: Encodable + HeaderVersion>(
        correlation: i32,
        version: i16,
        response: &R,
    ) -> BytesMut {
        let mut bytes = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(correlation)
            .encode(&mut bytes, R::header_version(version))
            .unwrap();
        response.encode(&mut bytes, version).unwrap();
        bytes
    }

    fn write_payload(stream: &mut TcpStream, bytes: &[u8]) {
        stream
            .write_all(&i32::try_from(bytes.len()).unwrap().to_be_bytes())
            .unwrap();
        stream.write_all(bytes).unwrap();
    }

    fn coordinator_header(correlation: i32, version: i16) -> RequestHeader {
        RequestHeader::default()
            .with_request_api_key(ApiKey::FindCoordinator as i16)
            .with_request_api_version(version)
            .with_correlation_id(correlation)
    }

    fn coordinator_request() -> FindCoordinatorRequest {
        FindCoordinatorRequest::default().with_key(StrBytes::from_static_str("group"))
    }

    fn coordinator_response() -> FindCoordinatorResponse {
        FindCoordinatorResponse::default()
            .with_node_id(1.into())
            .with_host(StrBytes::from_static_str("broker"))
            .with_port(9092)
    }

    #[test]
    fn typed_transport_reuses_connection_for_both_response_header_versions() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            for version in [0, 3] {
                let (header, mut bytes) = read_request(&mut stream, ApiKey::FindCoordinator);
                assert_eq!(header.request_api_version, version);
                FindCoordinatorRequest::decode(&mut bytes, version).unwrap();
                assert!(bytes.is_empty());
                write_payload(
                    &mut stream,
                    &response_payload(header.correlation_id, version, &coordinator_response()),
                );
            }
        });
        let mut client = client();
        let conn = client.get_conn_mut(&host).unwrap();
        let mut versions = crate::protocol::api_versions::ApiVersionCache::new();
        for version in [0, 3] {
            versions.insert_api_versions(
                host.clone(),
                &[crate::protocol::api_versions::BrokerApiVersion {
                    api_key: ApiKey::FindCoordinator as i16,
                    min_version: 0,
                    max_version: version,
                }],
            );
            let mut header = coordinator_header(100 + i32::from(version), 3);
            let negotiated = apply_request_api_version(&versions, &host, &mut header, 3);
            assert_eq!(negotiated, version);
            kp_send_request(conn, &header, &coordinator_request(), negotiated).unwrap();
            let response = kp_get_response::<FindCoordinatorResponse>(conn, version).unwrap();
            assert_eq!(response.host.as_str(), "broker");
            assert!(!conn.is_terminated());
        }
        server.join().unwrap();
    }

    #[test]
    fn invalid_typed_responses_retire_the_socket_before_pool_reuse() {
        for defect in [
            "negative-size",
            "short-header",
            "short-body",
            "wrong-correlation",
            "trailing-data",
            "wrong-version",
            "truncated-frame",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let host = listener.local_addr().unwrap().to_string();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let (header, _) = read_request(&mut stream, ApiKey::FindCoordinator);
                let mut bytes = match defect {
                    "short-header" => BytesMut::from(&[0, 0][..]),
                    "short-body" => {
                        let mut bytes = BytesMut::new();
                        ResponseHeader::default()
                            .with_correlation_id(header.correlation_id)
                            .encode(&mut bytes, 1)
                            .unwrap();
                        bytes
                    }
                    "wrong-correlation" => {
                        response_payload(header.correlation_id + 1, 3, &coordinator_response())
                    }
                    "wrong-version" => {
                        response_payload(header.correlation_id, 0, &coordinator_response())
                    }
                    _ => response_payload(header.correlation_id, 3, &coordinator_response()),
                };
                if defect == "trailing-data" {
                    bytes.extend_from_slice(&[42]);
                }
                let size = if defect == "negative-size" {
                    -1
                } else {
                    i32::try_from(bytes.len()).unwrap() + i32::from(defect == "truncated-frame")
                };
                stream.write_all(&size.to_be_bytes()).unwrap();
                if size >= 0 {
                    stream.write_all(&bytes).unwrap();
                }
                if defect == "truncated-frame" {
                    stream.shutdown(std::net::Shutdown::Write).unwrap();
                }
                let mut marker = [0];
                assert_eq!(
                    stream.read(&mut marker).unwrap(),
                    0,
                    "invalid frame socket remained reusable"
                );
                let (mut clean, _) = listener.accept().unwrap();
                clean
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                clean.read_exact(&mut marker).unwrap();
                assert_eq!(marker, [42]);
            });
            let mut client = client();
            let conn = client.get_conn_mut(&host).unwrap();
            kp_send_request(conn, &coordinator_header(7, 3), &coordinator_request(), 3).unwrap();
            assert!(
                kp_get_response::<FindCoordinatorResponse>(conn, 3).is_err(),
                "accepted {defect}"
            );
            assert!(conn.is_terminated());
            client.get_conn_mut(&host).unwrap().send(&[42]).unwrap();
            server.join().unwrap();
        }
    }

    #[test]
    fn acks_zero_is_replaced_by_the_next_typed_request_context() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let (header, mut bytes) = read_request(&mut stream, ApiKey::Produce);
            let produce = ProduceRequest::decode(&mut bytes, header.request_api_version).unwrap();
            assert_eq!(produce.acks, 0);
            assert!(bytes.is_empty());
            let (header, _) = read_request(&mut stream, ApiKey::FindCoordinator);
            assert_eq!(header.correlation_id, 19);
            write_payload(
                &mut stream,
                &response_payload(19, 0, &coordinator_response()),
            );
        });
        let mut client = client();
        let conn = client.get_conn_mut(&host).unwrap();
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Produce as i16)
            .with_request_api_version(9)
            .with_correlation_id(18);
        kp_send_request(conn, &header, &ProduceRequest::default().with_acks(0), 9).unwrap();
        kp_send_request(conn, &coordinator_header(19, 0), &coordinator_request(), 0).unwrap();
        kp_get_response::<FindCoordinatorResponse>(conn, 0).unwrap();
        assert!(!conn.is_terminated());
        server.join().unwrap();
    }

    #[test]
    fn negotiated_old_fetch_version_is_used_without_guessing_response_versions() {
        for response_version in [4, 12] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let host = listener.local_addr().unwrap().to_string();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let (header, mut bytes) = read_request(&mut stream, ApiKey::Fetch);
                assert_eq!(header.request_api_version, 4);
                FetchRequest::decode(&mut bytes, 4).unwrap();
                assert!(bytes.is_empty());
                write_payload(
                    &mut stream,
                    &response_payload(
                        header.correlation_id,
                        response_version,
                        &FetchResponse::default(),
                    ),
                );
            });
            let mut versions = crate::protocol::api_versions::ApiVersionCache::new();
            versions.insert_api_versions(
                host.clone(),
                &[crate::protocol::api_versions::BrokerApiVersion {
                    api_key: ApiKey::Fetch as i16,
                    min_version: 0,
                    max_version: 4,
                }],
            );
            let (mut header, request) = crate::protocol::fetch::build_fetch_request(
                42,
                "client",
                -1,
                100,
                1,
                i32::MAX,
                &[],
            );
            let version = apply_request_api_version(&versions, &host, &mut header, 12);
            assert_eq!(version, 4);
            let mut client = client();
            let conn = client.get_conn_mut(&host).unwrap();
            kp_send_request(conn, &header, &request, version).unwrap();
            let response = kp_get_response::<FetchResponse>(conn, version);
            assert_eq!(response.is_ok(), response_version == version);
            assert_eq!(conn.is_terminated(), response_version != version);
            server.join().unwrap();
        }
    }
}
