//! `DeleteTopics` protocol (API key 20) for topic administration.

use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::protocol::{Decodable, Encodable};

use crate::error::{Error, Result};
use crate::network::KafkaConnection;

pub const API_KEY_DELETE_TOPICS: i16 = 20;
pub const API_VERSION_DELETE_TOPICS: i16 = 2;

/// Result of deleting a single topic.
#[derive(Debug, Clone)]
pub struct DeleteTopicResult {
    pub name: String,
    pub error_code: i16,
}

/// Parsed response from a `DeleteTopics` request.
#[derive(Debug, Clone)]
pub struct DeleteTopicsResponseData {
    pub results: Vec<DeleteTopicResult>,
}

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

/// Build a `DeleteTopics` request.
pub fn build_delete_topics_request(
    correlation_id: i32,
    client_id: &str,
    topic_names: &[&str],
    timeout_ms: i32,
) -> Result<Vec<u8>> {
    let version = API_VERSION_DELETE_TOPICS;

    let mut body = BytesMut::new();
    // topics array length
    body.extend_from_slice(&crate::protocol::usize_to_i32(topic_names.len())?.to_be_bytes());
    for name in topic_names {
        encode_string(&mut body, name);
        // tagged fields (empty)
        body.put_u8(0);
    }
    // timeout_ms
    body.extend_from_slice(&timeout_ms.to_be_bytes());
    // tagged fields (empty)
    body.put_u8(0);

    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_DELETE_TOPICS)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, version)
        .map_err(|_| Error::codec())?;

    let total_len = crate::protocol::usize_to_i32(header_buf.len() + body.len())?;
    let out_len = crate::protocol::non_negative_i32_to_usize(total_len)?;
    let mut out = BytesMut::with_capacity(4 + out_len);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body);

    Ok(out.to_vec())
}

/// Send a `DeleteTopics` request and parse the response.
pub fn fetch_delete_topics(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    topic_names: &[&str],
    timeout_ms: i32,
) -> Result<DeleteTopicsResponseData> {
    let version = API_VERSION_DELETE_TOPICS;

    let request_bytes =
        build_delete_topics_request(correlation_id, client_id, topic_names, timeout_ms)?;
    conn.send(&request_bytes)?;

    let size = {
        let mut buf = [0u8; 4];
        conn.read_exact(&mut buf)?;
        i32::from_be_bytes(buf)
    };
    let resp_bytes = conn.read_exact_alloc(crate::protocol::non_negative_i32_to_u64(size)?)?;
    let mut bytes = bytes::Bytes::from(resp_bytes);

    let _resp_header = ResponseHeader::decode(&mut bytes, version).map_err(|_| Error::codec())?;

    // throttle_time_ms
    if bytes.len() < 4 {
        return Err(Error::codec());
    }
    bytes.advance(4);

    // results array length
    if bytes.len() < 4 {
        return Err(Error::codec());
    }
    let num_results = crate::protocol::non_negative_i32_to_usize(i32::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3],
    ]))?;
    bytes.advance(4);

    let mut results = Vec::with_capacity(num_results);
    for _ in 0..num_results {
        let name = decode_string(&mut bytes)?;
        if bytes.len() < 2 {
            return Err(Error::codec());
        }
        let error_code = i16::from_be_bytes([bytes[0], bytes[1]]);
        bytes.advance(2);

        // tagged fields
        if !bytes.is_empty() {
            let _tag_count = bytes[0];
            bytes.advance(1);
        }

        results.push(DeleteTopicResult { name, error_code });
    }

    Ok(DeleteTopicsResponseData { results })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_topics_request_builds() {
        let req = build_delete_topics_request(1, "test-client", &["topic-a", "topic-b"], 10000);
        assert!(req.is_ok());
        assert!(req.unwrap().len() > 4);
    }

    #[test]
    fn test_delete_topics_empty_list() {
        let req = build_delete_topics_request(1, "test-client", &[], 10000);
        assert!(req.is_ok());
    }
}
