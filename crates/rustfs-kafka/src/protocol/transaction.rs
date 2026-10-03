//! Generated codecs for Kafka transaction requests and responses.

use std::collections::HashSet;

use bytes::{Buf, Bytes};
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, EndTxnRequest, EndTxnResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, RequestHeader, ResponseHeader,
    TransactionalId,
};
use kafka_protocol::protocol::{Decodable, HeaderVersion, StrBytes};

use crate::error::{Error, KafkaCode, Result};
use crate::network::KafkaConnection;

pub const API_KEY_END_TXN: i16 = ApiKey::EndTxn as i16;
pub const API_KEY_ADD_PARTITIONS_TO_TXN: i16 = ApiKey::AddPartitionsToTxn as i16;
pub const API_VERSION_END_TXN: i16 = 2;
pub const API_VERSION_ADD_PARTITIONS_TO_TXN: i16 = 2;
const API_VERSION_FIND_TRANSACTION_COORDINATOR: i16 = 3;

fn request_header(
    api_key: i16,
    version: i16,
    correlation_id: i32,
    client_id: &str,
) -> RequestHeader {
    RequestHeader::default()
        .with_request_api_key(api_key)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
}

/// Transaction RPCs must never accept an unrelated or incomplete response as
/// an acknowledgement. The ordinary client transport keeps its existing API.
pub(crate) fn read_response<R: Decodable + HeaderVersion>(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    version: i16,
) -> Result<R> {
    let result = (|| {
        let mut size = [0; 4];
        conn.read_exact(&mut size)?;
        let bytes = conn.read_exact_alloc(crate::protocol::non_negative_i32_to_u64(
            i32::from_be_bytes(size),
        )?)?;
        decode_response::<R>(bytes, correlation_id, version)
    })();
    if result.is_err() {
        // Even a fully read malformed frame cannot be trusted for later RPCs.
        // Callers can access the client after poisoning the producer, so the
        // pooled connection must be retired independently of producer state.
        let _ = conn.shutdown();
    }
    result
}

fn decode_response<R: Decodable + HeaderVersion>(
    mut bytes: Bytes,
    correlation_id: i32,
    version: i16,
) -> Result<R> {
    let header = ResponseHeader::decode(&mut bytes, R::header_version(version))
        .map_err(|_| Error::codec())?;
    if header.correlation_id != correlation_id {
        return Err(Error::codec());
    }
    let response = R::decode(&mut bytes, version).map_err(|_| Error::codec())?;
    if bytes.has_remaining() {
        return Err(Error::codec());
    }
    Ok(response)
}

pub(crate) fn fetch_transaction_coordinator(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    transactional_id: &str,
) -> Result<String> {
    let version = API_VERSION_FIND_TRANSACTION_COORDINATOR;
    let request = FindCoordinatorRequest::default()
        .with_key(StrBytes::from_string(transactional_id.to_owned()))
        .with_key_type(1);
    let header = request_header(
        ApiKey::FindCoordinator as i16,
        version,
        correlation_id,
        client_id,
    );
    let frame = crate::protocol::encode_request_frame(&header, &request, version)?;
    conn.send(&frame)?;
    let response: FindCoordinatorResponse = read_response(conn, correlation_id, version)?;
    if response.error_code != 0 {
        return Err(Error::Kafka(
            KafkaCode::from_protocol(response.error_code).unwrap_or(KafkaCode::Unknown),
        ));
    }
    if i32::from(response.node_id) < 0
        || response.host.is_empty()
        || !(1..=65_535).contains(&response.port)
    {
        let _ = conn.shutdown();
        return Err(Error::codec());
    }
    // IPv6 addresses must remain valid host:port connection targets.
    let host = response.host.as_str();
    Ok(if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{}", response.port)
    } else {
        format!("{host}:{}", response.port)
    })
}

/// Parsed response from an `EndTxn` request.
#[derive(Debug, Clone)]
pub struct EndTxnResponseData {
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

/// Build an `EndTxn` request.
pub fn build_end_txn_request(
    correlation_id: i32,
    client_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    transactional_id: &str,
    committed: bool,
) -> Result<Vec<u8>> {
    let version = API_VERSION_END_TXN;
    let request = EndTxnRequest::default()
        .with_transactional_id(TransactionalId(StrBytes::from_string(
            transactional_id.to_owned(),
        )))
        .with_producer_id(producer_id.into())
        .with_producer_epoch(producer_epoch)
        .with_committed(committed);
    let header = request_header(API_KEY_END_TXN, version, correlation_id, client_id);
    crate::protocol::encode_request_frame(&header, &request, version).map(|frame| frame.to_vec())
}

/// Send an `EndTxn` request and parse the correlated response.
pub fn fetch_end_txn(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    transactional_id: &str,
    committed: bool,
) -> Result<EndTxnResponseData> {
    let request = build_end_txn_request(
        correlation_id,
        client_id,
        producer_id,
        producer_epoch,
        transactional_id,
        committed,
    )?;
    conn.send(&request)?;
    let response: EndTxnResponse = read_response(conn, correlation_id, API_VERSION_END_TXN)?;
    Ok(EndTxnResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
    })
}

/// A topic partition to add to a transaction.
#[derive(Debug, Clone)]
pub struct TxnPartition {
    pub topic: String,
    pub partitions: Vec<i32>,
}

/// Parsed response from an `AddPartitionsToTxn` request.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnResponseData {
    pub throttle_time_ms: i32,
    /// Zero for the v2 response; errors are returned for each topic below.
    pub error_code: i16,
    pub results: Vec<TxnPartitionResult>,
}

/// Result for adding a single topic's partitions to a transaction.
#[derive(Debug, Clone)]
pub struct TxnPartitionResult {
    pub topic: String,
    /// The first nonzero partition error for this topic, or zero.
    pub error_code: i16,
}

/// Build an `AddPartitionsToTxn` request.
pub fn build_add_partitions_to_txn_request(
    correlation_id: i32,
    client_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    transactional_id: &str,
    partitions: &[TxnPartition],
) -> Result<Vec<u8>> {
    let version = API_VERSION_ADD_PARTITIONS_TO_TXN;
    let request = AddPartitionsToTxnRequest::default()
        .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_string(
            transactional_id.to_owned(),
        )))
        .with_v3_and_below_producer_id(producer_id.into())
        .with_v3_and_below_producer_epoch(producer_epoch)
        .with_v3_and_below_topics(
            partitions
                .iter()
                .map(|topic| {
                    AddPartitionsToTxnTopic::default()
                        .with_name(StrBytes::from_string(topic.topic.clone()).into())
                        .with_partitions(topic.partitions.clone())
                })
                .collect(),
        );
    let header = request_header(
        API_KEY_ADD_PARTITIONS_TO_TXN,
        version,
        correlation_id,
        client_id,
    );
    crate::protocol::encode_request_frame(&header, &request, version).map(|frame| frame.to_vec())
}

/// Send an `AddPartitionsToTxn` request, requiring a result for every requested partition.
pub fn fetch_add_partitions_to_txn(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    transactional_id: &str,
    partitions: &[TxnPartition],
) -> Result<AddPartitionsToTxnResponseData> {
    let request = build_add_partitions_to_txn_request(
        correlation_id,
        client_id,
        producer_id,
        producer_epoch,
        transactional_id,
        partitions,
    )?;
    conn.send(&request)?;
    let response: AddPartitionsToTxnResponse =
        read_response(conn, correlation_id, API_VERSION_ADD_PARTITIONS_TO_TXN)?;
    let result = convert_add_partitions_response(response, partitions);
    if result.is_err() {
        let _ = conn.shutdown();
    }
    result
}

fn convert_add_partitions_response(
    response: AddPartitionsToTxnResponse,
    requested: &[TxnPartition],
) -> Result<AddPartitionsToTxnResponseData> {
    let mut expected: HashSet<(String, i32)> = requested
        .iter()
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .map(|&partition| (topic.topic.clone(), partition))
        })
        .collect();
    let mut results = Vec::with_capacity(response.results_by_topic_v3_and_below.len());
    for topic in response.results_by_topic_v3_and_below {
        if topic.results_by_partition.is_empty() {
            return Err(Error::codec());
        }
        let mut error_code = 0;
        for partition in topic.results_by_partition {
            if !expected.remove(&(topic.name.to_string(), partition.partition_index)) {
                return Err(Error::codec());
            }
            if error_code == 0 {
                error_code = partition.partition_error_code;
            }
        }
        results.push(TxnPartitionResult {
            topic: topic.name.to_string(),
            error_code,
        });
    }
    if !expected.is_empty() {
        return Err(Error::codec());
    }
    Ok(AddPartitionsToTxnResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: 0,
        results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use kafka_protocol::messages::add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    };
    use kafka_protocol::protocol::Encodable;

    #[test]
    fn end_txn_frame_uses_generated_header_and_api_key() {
        for committed in [false, true] {
            let frame = build_end_txn_request(42, "client", 12345, 7, "txn-1", committed).unwrap();
            let mut payload = Bytes::copy_from_slice(&frame[4..]);
            assert_eq!(
                usize::try_from(i32::from_be_bytes(frame[..4].try_into().unwrap())).unwrap(),
                payload.len()
            );
            let header = RequestHeader::decode(
                &mut payload,
                EndTxnRequest::header_version(API_VERSION_END_TXN),
            )
            .unwrap();
            assert_eq!(header.request_api_key, 26);
            assert_eq!(header.correlation_id, 42);
            let request = EndTxnRequest::decode(&mut payload, API_VERSION_END_TXN).unwrap();
            assert_eq!(request.committed, committed);
            assert_eq!(request.transactional_id.as_str(), "txn-1");
            assert_eq!(i64::from(request.producer_id), 12345);
            assert_eq!(request.producer_epoch, 7);
            assert!(!payload.has_remaining());
        }
    }

    #[test]
    fn add_partitions_frame_uses_generated_v2_layout() {
        let partitions = [TxnPartition {
            topic: "topic-a".into(),
            partitions: vec![0, 2],
        }];
        let frame =
            build_add_partitions_to_txn_request(5, "client", 123, 2, "txn", &partitions).unwrap();
        let mut payload = Bytes::copy_from_slice(&frame[4..]);
        let header = RequestHeader::decode(
            &mut payload,
            AddPartitionsToTxnRequest::header_version(API_VERSION_ADD_PARTITIONS_TO_TXN),
        )
        .unwrap();
        assert_eq!(header.request_api_key, 24);
        let request =
            AddPartitionsToTxnRequest::decode(&mut payload, API_VERSION_ADD_PARTITIONS_TO_TXN)
                .unwrap();
        assert_eq!(request.v3_and_below_transactional_id.as_str(), "txn");
        assert_eq!(request.v3_and_below_topics[0].partitions, [0, 2]);
        assert!(!payload.has_remaining());
    }

    #[test]
    fn transaction_response_rejects_wrong_correlation_or_trailing_data() {
        let mut bytes = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(4)
            .encode(
                &mut bytes,
                EndTxnResponse::header_version(API_VERSION_END_TXN),
            )
            .unwrap();
        EndTxnResponse::default()
            .encode(&mut bytes, API_VERSION_END_TXN)
            .unwrap();
        assert!(
            decode_response::<EndTxnResponse>(bytes.clone().freeze(), 5, API_VERSION_END_TXN)
                .is_err()
        );
        assert!(
            decode_response::<EndTxnResponse>(bytes.clone().freeze(), 4, API_VERSION_END_TXN)
                .is_ok()
        );
        bytes.extend_from_slice(&[0]);
        assert!(decode_response::<EndTxnResponse>(bytes.freeze(), 4, API_VERSION_END_TXN).is_err());
    }

    fn add_response(partitions: &[(i32, i16)]) -> AddPartitionsToTxnResponse {
        AddPartitionsToTxnResponse::default().with_results_by_topic_v3_and_below(vec![
            AddPartitionsToTxnTopicResult::default()
                .with_name(StrBytes::from_static_str("topic-a").into())
                .with_results_by_partition(
                    partitions
                        .iter()
                        .map(|&(partition, error)| {
                            AddPartitionsToTxnPartitionResult::default()
                                .with_partition_index(partition)
                                .with_partition_error_code(error)
                        })
                        .collect(),
                ),
        ])
    }

    #[test]
    fn add_partitions_requires_all_partition_acks_and_preserves_errors() {
        let requested = [TxnPartition {
            topic: "topic-a".into(),
            partitions: vec![0, 2],
        }];
        let result =
            convert_add_partitions_response(add_response(&[(0, 0), (2, 31)]), &requested).unwrap();
        assert_eq!(result.results[0].error_code, 31);
        for response in [
            add_response(&[(0, 0)]),
            add_response(&[(0, 0), (0, 0)]),
            add_response(&[(0, 0), (3, 0)]),
            AddPartitionsToTxnResponse::default(),
        ] {
            assert!(convert_add_partitions_response(response, &requested).is_err());
        }
    }

    #[test]
    fn invalid_transaction_response_frames_close_the_stream_before_client_reuse() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::time::Duration;

        fn response_payload(correlation: i32) -> Vec<u8> {
            let mut bytes = BytesMut::new();
            ResponseHeader::default()
                .with_correlation_id(correlation)
                .encode(
                    &mut bytes,
                    EndTxnResponse::header_version(API_VERSION_END_TXN),
                )
                .unwrap();
            EndTxnResponse::default()
                .encode(&mut bytes, API_VERSION_END_TXN)
                .unwrap();
            bytes.to_vec()
        }

        for defect in [
            "negative-size",
            "short-header",
            "short-body",
            "wrong-correlation",
            "trailing-data",
            "truncated-frame",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut payload = match defect {
                    "short-header" => vec![0; 2],
                    "short-body" => 4_i32.to_be_bytes().to_vec(),
                    "wrong-correlation" => response_payload(5),
                    _ => response_payload(4),
                };
                if defect == "trailing-data" {
                    payload.push(0);
                }
                let size = if defect == "negative-size" {
                    -1
                } else {
                    i32::try_from(payload.len()).unwrap() + i32::from(defect == "truncated-frame")
                };
                stream.write_all(&size.to_be_bytes()).unwrap();
                if size >= 0 {
                    stream.write_all(&payload).unwrap();
                }
                if defect == "truncated-frame" {
                    stream.shutdown(std::net::Shutdown::Write).unwrap();
                }
                let mut marker = [0];
                assert_eq!(
                    stream.read(&mut marker).unwrap(),
                    0,
                    "invalid frame must retire its stream"
                );
                let (mut clean, _) = listener.accept().unwrap();
                clean
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                clean.read_exact(&mut marker).unwrap();
                assert_eq!(marker, [42]);
            });
            let mut client = crate::client::KafkaClient::new(Vec::new());
            let conn = client.get_conn_mut(&address.to_string()).unwrap();
            assert!(read_response::<EndTxnResponse>(conn, 4, API_VERSION_END_TXN).is_err());
            assert!(conn.is_terminated());
            let clean = client.get_conn_mut(&address.to_string()).unwrap();
            assert!(!clean.is_terminated());
            clean.send(&[42]).unwrap();
            server.join().unwrap();
        }
    }
}
