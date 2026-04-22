//! Transaction protocol implementations.
//!
//! Provides `EndTxn` and `AddPartitionsToTxn` request builders and response
//! parsers for Kafka transactional producer support.

use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::protocol::{Decodable, Encodable};

use crate::error::{Error, Result};
use crate::network::KafkaConnection;

pub const API_KEY_END_TXN: i16 = 21;
pub const API_KEY_ADD_PARTITIONS_TO_TXN: i16 = 24;

pub const API_VERSION_END_TXN: i16 = 2;
pub const API_VERSION_ADD_PARTITIONS_TO_TXN: i16 = 2;

// --------------------------------------------------------------------
// Encoding helpers
// --------------------------------------------------------------------

fn encode_string(buf: &mut BytesMut, s: &str) {
    let len = crate::protocol::usize_to_i16(s.len())
        .expect("Kafka string length must fit in i16 for protocol encoding");
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn decode_string(bytes: &mut bytes::Bytes) -> Result<String> {
    if bytes.len() < 2 {
        return Err(Error::codec());
    }
    let len = crate::protocol::non_negative_i16_to_usize(i16::from_be_bytes([bytes[0], bytes[1]]))?;
    bytes.advance(2);
    if bytes.len() < len {
        return Err(Error::codec());
    }
    let s = String::from_utf8(bytes[..len].to_vec()).map_err(|_| Error::codec())?;
    bytes.advance(len);
    Ok(s)
}

fn build_frame(header: &RequestHeader, body: &[u8], api_version: i16) -> Result<Vec<u8>> {
    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, api_version)
        .map_err(|_| Error::codec())?;

    let total_len = crate::protocol::usize_to_i32(header_buf.len() + body.len())?;
    let out_len = crate::protocol::non_negative_i32_to_usize(total_len)?;
    let mut out = BytesMut::with_capacity(4 + out_len);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(body);

    Ok(out.to_vec())
}

fn read_response(conn: &mut KafkaConnection, api_version: i16) -> Result<bytes::Bytes> {
    let mut buf = [0u8; 4];
    conn.read_exact(&mut buf)?;
    let size = i32::from_be_bytes(buf);
    let resp_bytes = conn.read_exact_alloc(crate::protocol::non_negative_i32_to_u64(size)?)?;
    let mut bytes = resp_bytes;
    let _header = ResponseHeader::decode(&mut bytes, api_version).map_err(|_| Error::codec())?;
    Ok(bytes)
}

// --------------------------------------------------------------------
// EndTxn
// --------------------------------------------------------------------

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
    let mut body = BytesMut::new();

    encode_string(&mut body, transactional_id);
    body.extend_from_slice(&producer_id.to_be_bytes());
    body.extend_from_slice(&producer_epoch.to_be_bytes());
    // committed: boolean as int8
    body.put_i8(i8::from(committed));
    body.put_u8(0); // tagged fields

    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_END_TXN)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    build_frame(&header, &body, version)
}

/// Send an `EndTxn` request and parse the response.
pub fn fetch_end_txn(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    transactional_id: &str,
    committed: bool,
) -> Result<EndTxnResponseData> {
    let version = API_VERSION_END_TXN;
    let request_bytes = build_end_txn_request(
        correlation_id,
        client_id,
        producer_id,
        producer_epoch,
        transactional_id,
        committed,
    )?;
    conn.send(&request_bytes)?;

    let mut bytes = read_response(conn, version)?;

    let throttle_time_ms = if bytes.len() < 4 {
        return Err(Error::codec());
    } else {
        let v = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        bytes.advance(4);
        v
    };

    let error_code = if bytes.len() < 2 {
        return Err(Error::codec());
    } else {
        let v = i16::from_be_bytes([bytes[0], bytes[1]]);
        bytes.advance(2);
        v
    };

    Ok(EndTxnResponseData {
        throttle_time_ms,
        error_code,
    })
}

// --------------------------------------------------------------------
// AddPartitionsToTxn
// --------------------------------------------------------------------

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
    pub error_code: i16,
    pub results: Vec<TxnPartitionResult>,
}

/// Result for adding a single topic's partitions to a transaction.
#[derive(Debug, Clone)]
pub struct TxnPartitionResult {
    pub topic: String,
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
    let mut body = BytesMut::new();

    encode_string(&mut body, transactional_id);
    body.extend_from_slice(&producer_id.to_be_bytes());
    body.extend_from_slice(&producer_epoch.to_be_bytes());

    // partitions array
    body.extend_from_slice(&crate::protocol::usize_to_i32(partitions.len())?.to_be_bytes());
    for tp in partitions {
        encode_string(&mut body, &tp.topic);
        body.extend_from_slice(&crate::protocol::usize_to_i32(tp.partitions.len())?.to_be_bytes());
        for &p in &tp.partitions {
            body.extend_from_slice(&p.to_be_bytes());
        }
    }
    body.put_u8(0); // tagged fields

    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_ADD_PARTITIONS_TO_TXN)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    build_frame(&header, &body, version)
}

/// Send an `AddPartitionsToTxn` request and parse the response.
pub fn fetch_add_partitions_to_txn(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    transactional_id: &str,
    partitions: &[TxnPartition],
) -> Result<AddPartitionsToTxnResponseData> {
    let version = API_VERSION_ADD_PARTITIONS_TO_TXN;
    let request_bytes = build_add_partitions_to_txn_request(
        correlation_id,
        client_id,
        producer_id,
        producer_epoch,
        transactional_id,
        partitions,
    )?;
    conn.send(&request_bytes)?;

    let mut bytes = read_response(conn, version)?;

    let throttle_time_ms = if bytes.len() < 4 {
        return Err(Error::codec());
    } else {
        let v = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        bytes.advance(4);
        v
    };

    let error_code = if bytes.len() < 2 {
        return Err(Error::codec());
    } else {
        let v = i16::from_be_bytes([bytes[0], bytes[1]]);
        bytes.advance(2);
        v
    };

    // results array
    let num_results = if bytes.len() < 4 {
        return Err(Error::codec());
    } else {
        let v = crate::protocol::non_negative_i32_to_usize(i32::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
        ]))?;
        bytes.advance(4);
        v
    };

    let mut results = Vec::with_capacity(num_results);
    for _ in 0..num_results {
        let topic = decode_string(&mut bytes)?;
        let ec = if bytes.len() < 2 {
            return Err(Error::codec());
        } else {
            let v = i16::from_be_bytes([bytes[0], bytes[1]]);
            bytes.advance(2);
            v
        };
        results.push(TxnPartitionResult {
            topic,
            error_code: ec,
        });
    }

    Ok(AddPartitionsToTxnResponseData {
        throttle_time_ms,
        error_code,
        results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_end_txn_request() {
        let req = build_end_txn_request(1, "client", 12345, 1, "txn-1", true);
        assert!(req.is_ok());
        assert!(req.unwrap().len() > 4);
    }

    #[test]
    fn test_build_add_partitions_to_txn_request() {
        let partitions = vec![TxnPartition {
            topic: "t1".to_owned(),
            partitions: vec![0, 1],
        }];
        let req = build_add_partitions_to_txn_request(1, "client", 12345, 1, "txn-1", &partitions);
        assert!(req.is_ok());
    }

    #[test]
    fn test_end_txn_commit_vs_abort() {
        let commit = build_end_txn_request(1, "c", 0, 0, "t", true).unwrap();
        let abort = build_end_txn_request(1, "c", 0, 0, "t", false).unwrap();
        // They should differ (the committed byte)
        assert_ne!(commit, abort);
    }
}
