//! CreateTopics protocol (API key 19) for topic administration.

use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::protocol::{Decodable, Encodable};

use crate::error::{Error, Result};
use crate::network::KafkaConnection;

pub const API_KEY_CREATE_TOPICS: i16 = 19;
pub const API_VERSION_CREATE_TOPICS: i16 = 2;

/// Configuration for creating a new topic.
#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub configs: Vec<(String, String)>,
}

impl TopicConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            num_partitions: 1,
            replication_factor: 1,
            configs: Vec::new(),
        }
    }

    pub fn with_partitions(mut self, n: i32) -> Self {
        self.num_partitions = n;
        self
    }

    pub fn with_replication_factor(mut self, f: i16) -> Self {
        self.replication_factor = f;
        self
    }

    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((key.into(), value.into()));
        self
    }
}

/// Result of creating a single topic.
#[derive(Debug, Clone)]
pub struct TopicResult {
    pub name: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// Parsed response from a CreateTopics request.
#[derive(Debug, Clone)]
pub struct CreateTopicsResponseData {
    pub results: Vec<TopicResult>,
}

fn encode_string(buf: &mut BytesMut, s: &str) {
    let len = s.len() as i16;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn encode_nullable_string(buf: &mut BytesMut, s: &str) {
    let len = s.len() as i16;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn decode_string(bytes: &mut bytes::Bytes) -> Result<String> {
    if bytes.len() < 2 {
        return Err(Error::codec());
    }
    let len = i16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    bytes.advance(2);
    if bytes.len() < len {
        return Err(Error::codec());
    }
    let s = String::from_utf8(bytes[..len].to_vec()).map_err(|_| Error::codec())?;
    bytes.advance(len);
    Ok(s)
}

fn decode_nullable_string(bytes: &mut bytes::Bytes) -> Result<Option<String>> {
    if bytes.len() < 2 {
        return Err(Error::codec());
    }
    let len = i16::from_be_bytes([bytes[0], bytes[1]]);
    bytes.advance(2);
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if bytes.len() < len {
        return Err(Error::codec());
    }
    let s = String::from_utf8(bytes[..len].to_vec()).map_err(|_| Error::codec())?;
    bytes.advance(len);
    Ok(Some(s))
}

/// Build a CreateTopics request.
pub fn build_create_topics_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[TopicConfig],
    timeout_ms: i32,
) -> Result<Vec<u8>> {
    let version = API_VERSION_CREATE_TOPICS;

    let mut body = BytesMut::new();
    // topics array length
    body.extend_from_slice(&(topics.len() as i32).to_be_bytes());
    for topic in topics {
        encode_string(&mut body, &topic.name);
        body.extend_from_slice(&topic.num_partitions.to_be_bytes());
        body.extend_from_slice(&topic.replication_factor.to_be_bytes());
        // configs array
        body.extend_from_slice(&(topic.configs.len() as i32).to_be_bytes());
        for (key, value) in &topic.configs {
            encode_nullable_string(&mut body, key);
            encode_nullable_string(&mut body, value);
        }
        // tagged fields (empty, 0 tags)
        body.put_u8(0);
    }
    // timeout_ms
    body.extend_from_slice(&timeout_ms.to_be_bytes());
    // tagged fields (empty)
    body.put_u8(0);

    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_CREATE_TOPICS)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, version)
        .map_err(|_| Error::codec())?;

    let total_len = (header_buf.len() + body.len()) as i32;
    let mut out = BytesMut::with_capacity(4 + total_len as usize);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body);

    Ok(out.to_vec())
}

/// Send a CreateTopics request and parse the response.
pub fn fetch_create_topics(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    topics: &[TopicConfig],
    timeout_ms: i32,
) -> Result<CreateTopicsResponseData> {
    let version = API_VERSION_CREATE_TOPICS;

    let request_bytes = build_create_topics_request(correlation_id, client_id, topics, timeout_ms)?;
    conn.send(&request_bytes)?;

    let size = {
        let mut buf = [0u8; 4];
        conn.read_exact(&mut buf)?;
        i32::from_be_bytes(buf)
    };
    let resp_bytes = conn.read_exact_alloc(size as u64)?;
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
    let num_results = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    bytes.advance(4);

    let mut results = Vec::with_capacity(num_results);
    for _ in 0..num_results {
        let name = decode_string(&mut bytes)?;
        if bytes.len() < 2 {
            return Err(Error::codec());
        }
        let error_code = i16::from_be_bytes([bytes[0], bytes[1]]);
        bytes.advance(2);

        let error_message = if version >= 1 && !bytes.is_empty() {
            decode_nullable_string(&mut bytes)?
        } else {
            None
        };

        // Skip remaining fields we don't need (configs, partitions, etc.)
        // For simplicity, skip the rest of the topic result
        // In a production impl we'd parse all fields properly

        // tagged fields
        if !bytes.is_empty() {
            let _tag_count = bytes[0];
            bytes.advance(1);
        }

        results.push(TopicResult {
            name,
            error_code,
            error_message,
        });
    }

    Ok(CreateTopicsResponseData { results })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_config_builder() {
        let config = TopicConfig::new("test-topic")
            .with_partitions(3)
            .with_replication_factor(2)
            .with_config("retention.ms", "86400000");

        assert_eq!(config.name, "test-topic");
        assert_eq!(config.num_partitions, 3);
        assert_eq!(config.replication_factor, 2);
        assert_eq!(config.configs.len(), 1);
    }

    #[test]
    fn test_topic_config_default() {
        let config = TopicConfig::new("simple");
        assert_eq!(config.num_partitions, 1);
        assert_eq!(config.replication_factor, 1);
        assert!(config.configs.is_empty());
    }

    #[test]
    fn test_build_create_topics_request() {
        let topics = vec![TopicConfig::new("test").with_partitions(3)];
        let req = build_create_topics_request(1, "client", &topics, 10000);
        match &req {
            Err(e) => panic!("build failed: {e:?}"),
            Ok(bytes) => assert!(bytes.len() > 4),
        }
    }
}
