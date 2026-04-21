//! API version negotiation via Kafka's `ApiVersionsRequest` (API key 18).
//!
//! Infrastructure for negotiating API versions with Kafka brokers. Currently
//! used during metadata requests; full per-request version negotiation will
//! be wired up in a future release.

#![allow(dead_code)]

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::protocol::{
    API_VERSION_FETCH, API_VERSION_FIND_COORDINATOR, API_VERSION_LIST_OFFSETS,
    API_VERSION_METADATA, API_VERSION_OFFSET_COMMIT, API_VERSION_OFFSET_FETCH,
    API_VERSION_PRODUCE,
};
use tracing::{debug, info};

use crate::network::KafkaConnection;

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

/// The version of the `ApiVersions` request we send.
const API_VERSIONS_REQUEST_VERSION: i16 = 3;

/// Negotiated API version ranges for a single broker.
#[derive(Debug, Clone)]
pub struct BrokerApiVersions {
    versions: HashMap<i16, (i16, i16)>, // api_key -> (min_version, max_version)
}

impl BrokerApiVersions {
    /// Create from the parsed `ApiVersions` response.
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
        if let Some(&(min, max)) = self.versions.get(&api_key) {
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
        } else {
            debug!("API key {}: not supported by broker, using fallback {}", api_key, fallback);
            fallback
        }
    }
}

/// Send an `ApiVersionsRequest` and parse the response.
pub fn fetch_api_versions(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
) -> Result<BrokerApiVersions> {
    use bytes::BytesMut;
    use kafka_protocol::messages::{ApiVersionsRequest, RequestHeader, ResponseHeader};
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
        .map_err(|_| Error::codec())?;

    let mut body_buf = BytesMut::new();
    request.encode(&mut body_buf, API_VERSIONS_REQUEST_VERSION)
        .map_err(|_| Error::codec())?;

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
    let _resp_header = ResponseHeader::decode(&mut bytes, API_VERSIONS_REQUEST_VERSION)
        .map_err(|_| Error::codec())?;

    let kp_resp = kafka_protocol::messages::ApiVersionsResponse::decode(
        &mut bytes,
        API_VERSIONS_REQUEST_VERSION,
    )
    .map_err(|_| Error::codec())?;

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
            .map_or(fallback, |v| v.negotiate(api_key, fallback))
    }

    /// Returns the negotiated version for the given API key,
    /// falling back to a safe default if no version information is available.
    pub fn get_or_fallback(&self, host: &str, api_key: i16) -> i16 {
        let fallback = Self::fallback_version(api_key);
        self.negotiate(host, api_key, fallback)
    }

    /// Returns the fallback (minimum supported) version for an API key.
    #[must_use]
    pub fn fallback_version(api_key: i16) -> i16 {
        match api_key {
            api_key::PRODUCE => API_VERSION_PRODUCE,
            api_key::FETCH => API_VERSION_FETCH,
            api_key::METADATA => API_VERSION_METADATA,
            api_key::LIST_OFFSETS => API_VERSION_LIST_OFFSETS,
            api_key::FIND_COORDINATOR => API_VERSION_FIND_COORDINATOR,
            api_key::OFFSET_COMMIT => API_VERSION_OFFSET_COMMIT,
            api_key::OFFSET_FETCH => API_VERSION_OFFSET_FETCH,
            api_key::API_VERSIONS => 0,
            _ => 0,
        }
    }

    /// Returns true if no broker versions have been cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.broker_versions.is_empty()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_api_versions_from_response_empty() {
        // Simulate an empty ApiVersionsResponse (no api_keys).
        let resp = kafka_protocol::messages::ApiVersionsResponse::default();
        let bv = BrokerApiVersions::from_response(resp);
        // Negotiating anything on an empty set should return the fallback.
        assert_eq!(bv.negotiate(api_key::PRODUCE, 3), 3);
        assert_eq!(bv.negotiate(api_key::FETCH, 4), 4);
    }

    #[test]
    fn broker_api_versions_negotiate_clamps_to_range() {
        use kafka_protocol::messages::api_versions_response::ApiVersion;
        let resp = kafka_protocol::messages::ApiVersionsResponse::default().with_api_keys(vec![
            ApiVersion::default()
                .with_api_key(api_key::PRODUCE)
                .with_min_version(3)
                .with_max_version(8),
        ]);
        let bv = BrokerApiVersions::from_response(resp);

        // Within range -> returned as-is.
        assert_eq!(bv.negotiate(api_key::PRODUCE, 5), 5);
        // Below min -> clamped up.
        assert_eq!(bv.negotiate(api_key::PRODUCE, 1), 3);
        // Above max -> clamped down.
        assert_eq!(bv.negotiate(api_key::PRODUCE, 12), 8);
        // Unknown key -> fallback.
        assert_eq!(bv.negotiate(99, 7), 7);
    }

    #[test]
    fn api_version_cache_new_is_empty() {
        let cache = ApiVersionCache::new();
        assert!(!cache.contains("broker1:9092"));
        assert!(!cache.contains("any-host"));
    }

    #[test]
    fn api_version_cache_insert_and_contains() {
        let mut cache = ApiVersionCache::new();
        let bv = BrokerApiVersions::from_response(kafka_protocol::messages::ApiVersionsResponse::default());
        cache.insert("broker1:9092".to_string(), bv);
        assert!(cache.contains("broker1:9092"));
        assert!(!cache.contains("broker2:9092"));
    }

    #[test]
    fn api_version_cache_invalidate() {
        let mut cache = ApiVersionCache::new();
        let bv = BrokerApiVersions::from_response(kafka_protocol::messages::ApiVersionsResponse::default());
        cache.insert("broker1:9092".to_string(), bv);
        assert!(cache.contains("broker1:9092"));
        cache.invalidate("broker1:9092");
        assert!(!cache.contains("broker1:9092"));
    }

    #[test]
    fn api_version_cache_negotiate_falls_back_when_missing() {
        let cache = ApiVersionCache::new();
        // No broker in cache -> returns fallback.
        assert_eq!(cache.negotiate("unknown:9092", api_key::FETCH, 4), 4);
    }

    #[test]
    fn api_version_cache_negotiate_with_known_broker() {
        use kafka_protocol::messages::api_versions_response::ApiVersion;
        let mut cache = ApiVersionCache::new();
        let resp = kafka_protocol::messages::ApiVersionsResponse::default().with_api_keys(vec![
            ApiVersion::default()
                .with_api_key(api_key::METADATA)
                .with_min_version(1)
                .with_max_version(12),
        ]);
        let bv = BrokerApiVersions::from_response(resp);
        cache.insert("broker1:9092".to_string(), bv);

        // Within range.
        assert_eq!(cache.negotiate("broker1:9092", api_key::METADATA, 7), 7);
        // Above max.
        assert_eq!(cache.negotiate("broker1:9092", api_key::METADATA, 20), 12);
        // Unknown API key for this broker -> fallback.
        assert_eq!(cache.negotiate("broker1:9092", api_key::FETCH, 4), 4);
    }

    #[test]
    fn api_versions_default_has_expected_fields() {
        let v = ApiVersions::default();
        assert_eq!(v.produce, API_VERSION_PRODUCE);
        assert_eq!(v.fetch, API_VERSION_FETCH);
        assert_eq!(v.metadata, API_VERSION_METADATA);
        assert_eq!(v.list_offsets, API_VERSION_LIST_OFFSETS);
        assert_eq!(v.find_coordinator, API_VERSION_FIND_COORDINATOR);
        assert_eq!(v.offset_commit, API_VERSION_OFFSET_COMMIT);
        assert_eq!(v.offset_fetch, API_VERSION_OFFSET_FETCH);
    }

    #[test]
    fn resolve_all_api_versions_uses_defaults_for_unknown_broker() {
        let cache = ApiVersionCache::new();
        let v = resolve_all_api_versions(&cache, "unknown");
        let d = ApiVersions::default();
        assert_eq!(v.produce, d.produce);
        assert_eq!(v.fetch, d.fetch);
        assert_eq!(v.metadata, d.metadata);
        assert_eq!(v.list_offsets, d.list_offsets);
        assert_eq!(v.find_coordinator, d.find_coordinator);
        assert_eq!(v.offset_commit, d.offset_commit);
        assert_eq!(v.offset_fetch, d.offset_fetch);
    }

    #[test]
    fn fallback_version_known_apis() {
        assert_eq!(ApiVersionCache::fallback_version(api_key::PRODUCE), API_VERSION_PRODUCE);
        assert_eq!(ApiVersionCache::fallback_version(api_key::FETCH), API_VERSION_FETCH);
        assert_eq!(ApiVersionCache::fallback_version(api_key::METADATA), API_VERSION_METADATA);
        assert_eq!(ApiVersionCache::fallback_version(api_key::LIST_OFFSETS), API_VERSION_LIST_OFFSETS);
        assert_eq!(ApiVersionCache::fallback_version(api_key::FIND_COORDINATOR), API_VERSION_FIND_COORDINATOR);
        assert_eq!(ApiVersionCache::fallback_version(api_key::OFFSET_COMMIT), API_VERSION_OFFSET_COMMIT);
        assert_eq!(ApiVersionCache::fallback_version(api_key::OFFSET_FETCH), API_VERSION_OFFSET_FETCH);
    }

    #[test]
    fn fallback_version_unknown_api() {
        assert_eq!(ApiVersionCache::fallback_version(99), 0);
        assert_eq!(ApiVersionCache::fallback_version(-1), 0);
    }

    #[test]
    fn get_or_fallback_empty_cache_returns_fallback() {
        let cache = ApiVersionCache::new();
        assert_eq!(cache.get_or_fallback("unknown:9092", api_key::PRODUCE), API_VERSION_PRODUCE);
    }

    #[test]
    fn get_or_fallback_populated_cache_negotiates() {
        use kafka_protocol::messages::api_versions_response::ApiVersion;
        let mut cache = ApiVersionCache::new();
        let resp = kafka_protocol::messages::ApiVersionsResponse::default().with_api_keys(vec![
            ApiVersion::default()
                .with_api_key(api_key::PRODUCE)
                .with_min_version(3)
                .with_max_version(8),
        ]);
        let bv = BrokerApiVersions::from_response(resp);
        cache.insert("broker1:9092".to_string(), bv);

        // Should negotiate within range, using fallback if API key unknown
        assert_eq!(cache.get_or_fallback("broker1:9092", api_key::PRODUCE), API_VERSION_PRODUCE);
        assert_eq!(cache.get_or_fallback("broker1:9092", api_key::FETCH), API_VERSION_FETCH);
    }
}
