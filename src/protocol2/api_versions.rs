//! API version negotiation via Kafka's ApiVersionsRequest (API key 18).

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::protocol2::{
    API_VERSION_FETCH, API_VERSION_FIND_COORDINATOR, API_VERSION_LIST_OFFSETS,
    API_VERSION_METADATA, API_VERSION_OFFSET_COMMIT, API_VERSION_OFFSET_FETCH,
    API_VERSION_PRODUCE,
};
use tracing::{debug, info};

use crate::client::network::KafkaConnection;

/// API key numbers as defined by the Kafka protocol.
pub mod api_key {
    pub const PRODUCE: i16 = 0;
    pub const FETCH: i16 = 1;
    pub const LIST_OFFSETS: i16 = 2;
    pub const METADATA: i16 = 3;
    pub const FIND_COORDINATOR: i16 = 10;
    pub const OFFSET_COMMIT: i16 = 8;
    pub const OFFSET_FETCH: i16 = 9;
    pub const API_VERSIONS: i16 = 18;
}

/// The version of the ApiVersions request we send.
const API_VERSIONS_REQUEST_VERSION: i16 = 3;

/// Negotiated API version ranges for a single broker.
#[derive(Debug, Clone)]
pub struct BrokerApiVersions {
    versions: HashMap<i16, (i16, i16)>, // api_key -> (min_version, max_version)
}

impl BrokerApiVersions {
    /// Create from the parsed ApiVersions response.
    fn from_response(
        resp: kafka_protocol::messages::ApiVersionsResponse,
    ) -> BrokerApiVersions {
        let mut versions = HashMap::new();
        for av in resp.api_keys {
            versions.insert(av.api_key, (av.min_version, av.max_version));
        }
        BrokerApiVersions { versions }
    }

    /// Get the best version for the given API key, clamped to the requested range.
    /// Returns `fallback` if the broker doesn't support the API.
    pub fn negotiate(&self, api_key: i16, fallback: i16) -> i16 {
        match self.versions.get(&api_key) {
            Some(&(min, max)) => {
                if fallback < min {
                    debug!(
                        "API key {}: our version {} below broker min {}, using min",
                        api_key, fallback, min
                    );
                    min
                } else if fallback > max {
                    debug!(
                        "API key {}: our version {} above broker max {}, using max",
                        api_key, fallback, max
                    );
                    max
                } else {
                    fallback
                }
            }
            None => {
                debug!("API key {}: not supported by broker, using fallback {}", api_key, fallback);
                fallback
            }
        }
    }
}

/// Send an ApiVersionsRequest and parse the response.
pub fn fetch_api_versions(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
) -> Result<BrokerApiVersions> {
    use bytes::BytesMut;
    use kafka_protocol::messages::{ApiVersionsRequest, RequestHeader};
    use kafka_protocol::protocol::{Decodable, Encodable};
    use kafka_protocol::protocol::StrBytes;

    let request = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("rustfs-kafka"));

    let header = RequestHeader::default()
        .with_request_api_key(api_key::API_VERSIONS)
        .with_request_api_version(API_VERSIONS_REQUEST_VERSION)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    let mut header_buf = BytesMut::new();
    header.encode(&mut header_buf, API_VERSIONS_REQUEST_VERSION)
        .map_err(|_| Error::CodecError)?;

    let mut body_buf = BytesMut::new();
    request.encode(&mut body_buf, API_VERSIONS_REQUEST_VERSION)
        .map_err(|_| Error::CodecError)?;

    let total_len = (header_buf.len() + body_buf.len()) as i32;
    let mut out = BytesMut::with_capacity(4 + total_len as usize);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body_buf);

    conn.send(&out)?;

    let size = {
        let mut buf = [0u8; 4];
        conn.read_exact(&mut buf)?;
        i32::from_be_bytes(buf)
    };
    let resp_bytes = conn.read_exact_alloc(size as u64)?;
    let mut bytes = bytes::Bytes::from(resp_bytes);

    use kafka_protocol::messages::ResponseHeader;
    let _resp_header = ResponseHeader::decode(&mut bytes, API_VERSIONS_REQUEST_VERSION)
        .map_err(|_| Error::CodecError)?;

    let kp_resp = kafka_protocol::messages::ApiVersionsResponse::decode(
        &mut bytes,
        API_VERSIONS_REQUEST_VERSION,
    )
    .map_err(|_| Error::CodecError)?;

    let result = BrokerApiVersions::from_response(kp_resp);
    info!("Negotiated API versions: {:?}", result);
    Ok(result)
}

/// Stores negotiated API versions per broker.
#[derive(Debug, Default)]
pub struct ApiVersionCache {
    broker_versions: HashMap<String, BrokerApiVersions>,
}

impl ApiVersionCache {
    pub fn new() -> Self {
        ApiVersionCache {
            broker_versions: HashMap::new(),
        }
    }

    /// Check if we have negotiated versions for a broker.
    pub fn contains(&self, host: &str) -> bool {
        self.broker_versions.contains_key(host)
    }

    /// Insert negotiated versions for a broker.
    pub fn insert(&mut self, host: String, versions: BrokerApiVersions) {
        self.broker_versions.insert(host, versions);
    }

    /// Get or fetch API versions for a broker.
    pub fn get_or_fetch(
        &mut self,
        host: &str,
        conn: &mut KafkaConnection,
        correlation_id: i32,
        client_id: &str,
    ) -> Result<&BrokerApiVersions> {
        if !self.broker_versions.contains_key(host) {
            let versions = fetch_api_versions(conn, correlation_id, client_id)?;
            self.broker_versions.insert(host.to_owned(), versions);
        }
        Ok(self.broker_versions.get(host).unwrap())
    }

    /// Invalidate cached versions for a broker (e.g., after reconnect).
    pub fn invalidate(&mut self, host: &str) {
        self.broker_versions.remove(host);
    }

    /// Negotiate the best API version for a specific broker and API key.
    pub fn negotiate(&self, host: &str, api_key: i16, fallback: i16) -> i16 {
        self.broker_versions
            .get(host)
            .map(|v| v.negotiate(api_key, fallback))
            .unwrap_or(fallback)
    }
}

/// Resolve the effective API version for a given API key using cached negotiations.
/// Falls back to hardcoded defaults if no negotiation has occurred.
pub fn resolve_api_version(
    cache: &ApiVersionCache,
    host: &str,
    api_key: i16,
    default: i16,
) -> i16 {
    cache.negotiate(host, api_key, default)
}

/// Resolve all our API versions for a given broker.
pub fn resolve_all_api_versions(cache: &ApiVersionCache, host: &str) -> ApiVersions {
    ApiVersions {
        produce: resolve_api_version(cache, host, api_key::PRODUCE, API_VERSION_PRODUCE),
        fetch: resolve_api_version(cache, host, api_key::FETCH, API_VERSION_FETCH),
        metadata: resolve_api_version(cache, host, api_key::METADATA, API_VERSION_METADATA),
        list_offsets: resolve_api_version(cache, host, api_key::LIST_OFFSETS, API_VERSION_LIST_OFFSETS),
        find_coordinator: resolve_api_version(cache, host, api_key::FIND_COORDINATOR, API_VERSION_FIND_COORDINATOR),
        offset_commit: resolve_api_version(cache, host, api_key::OFFSET_COMMIT, API_VERSION_OFFSET_COMMIT),
        offset_fetch: resolve_api_version(cache, host, api_key::OFFSET_FETCH, API_VERSION_OFFSET_FETCH),
    }
}

/// Resolved API versions for all supported Kafka APIs.
#[derive(Debug, Copy, Clone)]
pub struct ApiVersions {
    pub produce: i16,
    pub fetch: i16,
    pub metadata: i16,
    pub list_offsets: i16,
    pub find_coordinator: i16,
    pub offset_commit: i16,
    pub offset_fetch: i16,
}

impl Default for ApiVersions {
    fn default() -> Self {
        ApiVersions {
            produce: API_VERSION_PRODUCE,
            fetch: API_VERSION_FETCH,
            metadata: API_VERSION_METADATA,
            list_offsets: API_VERSION_LIST_OFFSETS,
            find_coordinator: API_VERSION_FIND_COORDINATOR,
            offset_commit: API_VERSION_OFFSET_COMMIT,
            offset_fetch: API_VERSION_OFFSET_FETCH,
        }
    }
}
