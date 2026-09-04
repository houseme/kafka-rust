//! `DeleteTopics` protocol (API key 20) for topic administration.

use kafka_protocol::messages::{DeleteTopicsRequest, DeleteTopicsResponse, RequestHeader};
use kafka_protocol::protocol::StrBytes;

use crate::error::Result;

pub const API_KEY_DELETE_TOPICS: i16 = 20;
pub const API_VERSION_DELETE_TOPICS: i16 = 2;

/// Result of deleting a single topic.
#[derive(Debug, Clone)]
pub struct DeleteTopicResult {
    /// Topic name targeted for deletion.
    pub name: String,
    /// Broker error code for the delete operation (0 == success).
    pub error_code: i16,
}

/// Parsed response from a `DeleteTopics` request.
#[derive(Debug, Clone)]
pub struct DeleteTopicsResponseData {
    /// Per-topic results returned by the broker for the delete request.
    pub results: Vec<DeleteTopicResult>,
}

/// Build a generated `DeleteTopics` request and header.
#[must_use]
pub fn build_delete_topics_protocol_request(
    correlation_id: i32,
    client_id: &str,
    topic_names: &[&str],
    timeout_ms: i32,
) -> (RequestHeader, DeleteTopicsRequest) {
    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_DELETE_TOPICS)
        .with_request_api_version(API_VERSION_DELETE_TOPICS)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));
    let topic_names = topic_names
        .iter()
        .map(|name| StrBytes::from_string((*name).to_owned()).into())
        .collect();
    let request = DeleteTopicsRequest::default()
        .with_topic_names(topic_names)
        .with_timeout_ms(timeout_ms);

    (header, request)
}

/// Build a framed `DeleteTopics` request.
///
/// # Errors
///
/// Returns an error if the generated request cannot be encoded or if the
/// encoded frame length does not fit the Kafka wire format.
pub fn build_delete_topics_request(
    correlation_id: i32,
    client_id: &str,
    topic_names: &[&str],
    timeout_ms: i32,
) -> Result<Vec<u8>> {
    let (header, request) =
        build_delete_topics_protocol_request(correlation_id, client_id, topic_names, timeout_ms);
    encode_framed_delete_topics_request(&header, &request)
}

fn encode_framed_delete_topics_request(
    header: &RequestHeader,
    request: &DeleteTopicsRequest,
) -> Result<Vec<u8>> {
    let version = API_VERSION_DELETE_TOPICS;
    crate::protocol::encode_request_frame(header, request, version).map(|frame| frame.to_vec())
}

/// Convert a generated `DeleteTopicsResponse` into the crate's public shape.
#[must_use]
pub fn convert_delete_topics_response(response: DeleteTopicsResponse) -> DeleteTopicsResponseData {
    DeleteTopicsResponseData {
        results: response
            .responses
            .into_iter()
            .map(|topic| DeleteTopicResult {
                name: topic.name.map(|name| name.to_string()).unwrap_or_default(),
                error_code: topic.error_code,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, Bytes};
    use kafka_protocol::messages::delete_topics_response::DeletableTopicResult as KpDeletableTopicResult;
    use kafka_protocol::protocol::{Decodable, HeaderVersion};

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

    #[test]
    fn test_build_delete_topics_protocol_request_preserves_fields() {
        let (header, request) =
            build_delete_topics_protocol_request(42, "client", &["topic-a", "topic-b"], 30000);

        assert_eq!(header.request_api_key, API_KEY_DELETE_TOPICS);
        assert_eq!(header.request_api_version, API_VERSION_DELETE_TOPICS);
        assert_eq!(header.correlation_id, 42);
        assert_eq!(request.timeout_ms, 30000);
        assert_eq!(request.topic_names.len(), 2);
        assert_eq!(request.topic_names[0].to_string(), "topic-a");
        assert_eq!(request.topic_names[1].to_string(), "topic-b");
    }

    #[test]
    fn test_build_delete_topics_request_writes_v2_body_without_flexible_tags() {
        let frame =
            build_delete_topics_request(7, "client-a", &["topic-a", "topic-b"], 10_000).unwrap();
        let mut bytes = Bytes::from(frame);
        let frame_len = bytes.get_i32();
        assert_eq!(usize::try_from(frame_len).unwrap(), bytes.remaining());

        let header = RequestHeader::decode(
            &mut bytes,
            DeleteTopicsRequest::header_version(API_VERSION_DELETE_TOPICS),
        )
        .unwrap();
        assert_eq!(header.request_api_key, API_KEY_DELETE_TOPICS);
        assert_eq!(header.request_api_version, API_VERSION_DELETE_TOPICS);
        assert_eq!(header.correlation_id, 7);
        assert_eq!(
            header.client_id.as_ref().map(ToString::to_string),
            Some("client-a".to_owned())
        );

        assert_eq!(bytes.get_i32(), 2);
        assert_eq!(bytes.get_i16(), 7);
        assert_eq!(&bytes.copy_to_bytes(7)[..], b"topic-a");
        assert_eq!(bytes.get_i16(), 7);
        assert_eq!(&bytes.copy_to_bytes(7)[..], b"topic-b");
        assert_eq!(bytes.get_i32(), 10_000);
        assert!(!bytes.has_remaining());
    }

    #[test]
    fn test_convert_delete_topics_response_preserves_topic_errors() {
        let response = DeleteTopicsResponse::default().with_responses(vec![
            KpDeletableTopicResult::default()
                .with_name(Some(StrBytes::from_static_str("topic-a").into()))
                .with_error_code(3),
        ]);

        let converted = convert_delete_topics_response(response);

        assert_eq!(converted.results.len(), 1);
        assert_eq!(converted.results[0].name, "topic-a");
        assert_eq!(converted.results[0].error_code, 3);
    }
}
