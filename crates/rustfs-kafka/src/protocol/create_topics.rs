//! `CreateTopics` protocol (API key 19) for topic administration.

use bytes::BytesMut;
use kafka_protocol::messages::{
    CreateTopicsRequest, CreateTopicsResponse, RequestHeader, ResponseHeader,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

use crate::error::{Error, Result};
use crate::network::KafkaConnection;

pub const API_KEY_CREATE_TOPICS: i16 = 19;
pub const API_VERSION_CREATE_TOPICS: i16 = 2;

/// Configuration for creating a new topic.
#[derive(Debug, Clone)]
pub struct TopicConfig {
    /// Topic name to be created.
    pub name: String,
    /// Number of partitions for the topic.
    pub num_partitions: i32,
    /// Replication factor for the topic.
    pub replication_factor: i16,
    /// Optional topic-level configurations as key/value pairs.
    pub configs: Vec<(String, String)>,
}

impl TopicConfig {
    /// Create a new `TopicConfig` with sane defaults.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            num_partitions: 1,
            replication_factor: 1,
            configs: Vec::new(),
        }
    }

    /// Set the number of partitions for the topic.
    #[must_use]
    pub fn with_partitions(mut self, n: i32) -> Self {
        self.num_partitions = n;
        self
    }

    /// Set the replication factor for the topic.
    #[must_use]
    pub fn with_replication_factor(mut self, f: i16) -> Self {
        self.replication_factor = f;
        self
    }

    /// Add a configuration key/value pair to the topic creation request.
    #[must_use]
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((key.into(), value.into()));
        self
    }
}

/// Result of creating a single topic returned by the broker.
#[derive(Debug, Clone)]
pub struct TopicResult {
    /// Topic name associated with this result.
    pub name: String,
    /// Broker error code for this topic creation (0 == success).
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Parsed response from a `CreateTopics` request.
#[derive(Debug, Clone)]
pub struct CreateTopicsResponseData {
    /// Per-topic results returned by the broker.
    pub results: Vec<TopicResult>,
}

/// Build a generated `CreateTopics` request and header.
#[must_use]
pub fn build_create_topics_protocol_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[TopicConfig],
    timeout_ms: i32,
) -> (RequestHeader, CreateTopicsRequest) {
    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_CREATE_TOPICS)
        .with_request_api_version(API_VERSION_CREATE_TOPICS)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));
    let topics = topics.iter().map(to_creatable_topic).collect();
    let request = CreateTopicsRequest::default()
        .with_topics(topics)
        .with_timeout_ms(timeout_ms);

    (header, request)
}

fn to_creatable_topic(topic: &TopicConfig) -> CreatableTopic {
    CreatableTopic::default()
        .with_name(StrBytes::from_string(topic.name.clone()).into())
        .with_num_partitions(topic.num_partitions)
        .with_replication_factor(topic.replication_factor)
        .with_configs(
            topic
                .configs
                .iter()
                .map(|(key, value)| {
                    CreatableTopicConfig::default()
                        .with_name(StrBytes::from_string(key.clone()))
                        .with_value(Some(StrBytes::from_string(value.clone())))
                })
                .collect(),
        )
}

/// Build a `CreateTopics` request.
pub fn build_create_topics_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[TopicConfig],
    timeout_ms: i32,
) -> Result<Vec<u8>> {
    let (header, request) =
        build_create_topics_protocol_request(correlation_id, client_id, topics, timeout_ms);
    encode_framed_create_topics_request(&header, &request)
}

fn encode_framed_create_topics_request(
    header: &RequestHeader,
    request: &CreateTopicsRequest,
) -> Result<Vec<u8>> {
    let version = API_VERSION_CREATE_TOPICS;
    let mut header_buf = BytesMut::new();
    header
        .encode(
            &mut header_buf,
            CreateTopicsRequest::header_version(version),
        )
        .map_err(|_| Error::codec())?;

    let mut body_buf = BytesMut::new();
    request
        .encode(&mut body_buf, version)
        .map_err(|_| Error::codec())?;

    let total_len = crate::protocol::usize_to_i32(header_buf.len() + body_buf.len())?;
    let out_len = crate::protocol::non_negative_i32_to_usize(total_len)?;
    let mut out = BytesMut::with_capacity(4 + out_len);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body_buf);

    Ok(out.to_vec())
}

/// Convert a generated `CreateTopicsResponse` into the crate's public shape.
#[must_use]
pub fn convert_create_topics_response(response: CreateTopicsResponse) -> CreateTopicsResponseData {
    CreateTopicsResponseData {
        results: response
            .topics
            .into_iter()
            .map(|topic| TopicResult {
                name: topic.name.to_string(),
                error_code: topic.error_code,
                error_message: topic.error_message.map(|message| message.to_string()),
            })
            .collect(),
    }
}

/// Send a `CreateTopics` request and parse the response.
#[allow(dead_code)]
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
    let resp_bytes = conn.read_exact_alloc(crate::protocol::non_negative_i32_to_u64(size)?)?;
    let mut bytes = resp_bytes;

    let _resp_header =
        ResponseHeader::decode(&mut bytes, CreateTopicsResponse::header_version(version))
            .map_err(|_| Error::codec())?;
    let response = CreateTopicsResponse::decode(&mut bytes, version).map_err(|_| Error::codec())?;

    Ok(convert_create_topics_response(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, Bytes};
    use kafka_protocol::messages::create_topics_response::CreatableTopicResult;

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

    #[test]
    fn test_build_create_topics_protocol_request_preserves_fields() {
        let topics = vec![
            TopicConfig::new("test")
                .with_partitions(3)
                .with_replication_factor(2)
                .with_config("cleanup.policy", "compact"),
        ];
        let (header, request) = build_create_topics_protocol_request(42, "client", &topics, 30000);

        assert_eq!(header.request_api_key, API_KEY_CREATE_TOPICS);
        assert_eq!(header.request_api_version, API_VERSION_CREATE_TOPICS);
        assert_eq!(header.correlation_id, 42);
        assert_eq!(request.timeout_ms, 30000);
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].name.to_string(), "test");
        assert_eq!(request.topics[0].num_partitions, 3);
        assert_eq!(request.topics[0].replication_factor, 2);
        assert_eq!(request.topics[0].configs.len(), 1);
        assert_eq!(
            request.topics[0].configs[0].name.to_string(),
            "cleanup.policy"
        );
        assert_eq!(
            request.topics[0].configs[0]
                .value
                .as_ref()
                .map(ToString::to_string),
            Some("compact".to_owned())
        );
    }

    #[test]
    fn test_build_create_topics_request_writes_v2_body_without_flexible_tags() {
        let topics = vec![
            TopicConfig::new("topic-a")
                .with_partitions(3)
                .with_replication_factor(2)
                .with_config("retention.ms", "60000"),
        ];
        let frame = build_create_topics_request(7, "client-a", &topics, 10_000).unwrap();
        let mut bytes = Bytes::from(frame);
        let frame_len = bytes.get_i32();
        assert_eq!(usize::try_from(frame_len).unwrap(), bytes.remaining());

        let header = RequestHeader::decode(
            &mut bytes,
            CreateTopicsRequest::header_version(API_VERSION_CREATE_TOPICS),
        )
        .unwrap();
        assert_eq!(header.request_api_key, API_KEY_CREATE_TOPICS);
        assert_eq!(header.request_api_version, API_VERSION_CREATE_TOPICS);
        assert_eq!(header.correlation_id, 7);
        assert_eq!(
            header.client_id.as_ref().map(ToString::to_string),
            Some("client-a".to_owned())
        );

        assert_eq!(bytes.get_i32(), 1);
        assert_eq!(bytes.get_i16(), 7);
        assert_eq!(&bytes.copy_to_bytes(7)[..], b"topic-a");
        assert_eq!(bytes.get_i32(), 3);
        assert_eq!(bytes.get_i16(), 2);
        assert_eq!(bytes.get_i32(), 0);
        assert_eq!(bytes.get_i32(), 1);
        assert_eq!(bytes.get_i16(), 12);
        assert_eq!(&bytes.copy_to_bytes(12)[..], b"retention.ms");
        assert_eq!(bytes.get_i16(), 5);
        assert_eq!(&bytes.copy_to_bytes(5)[..], b"60000");
        assert_eq!(bytes.get_i32(), 10_000);
        assert_eq!(bytes.get_u8(), 0);
        assert!(!bytes.has_remaining());
    }

    #[test]
    fn test_convert_create_topics_response_preserves_error_message() {
        let response = CreateTopicsResponse::default().with_topics(vec![
            CreatableTopicResult::default()
                .with_name(StrBytes::from_static_str("topic-a").into())
                .with_error_code(36)
                .with_error_message(Some(StrBytes::from_static_str("already exists"))),
        ]);

        let converted = convert_create_topics_response(response);

        assert_eq!(converted.results.len(), 1);
        assert_eq!(converted.results[0].name, "topic-a");
        assert_eq!(converted.results[0].error_code, 36);
        assert_eq!(
            converted.results[0].error_message.as_deref(),
            Some("already exists")
        );
    }
}
