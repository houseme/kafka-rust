//! Kafka client telemetry protocol helpers.
//!
//! These helpers cover Kafka's telemetry wire messages and subscription state
//! without introducing a background scheduler or OpenTelemetry encoder.

use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse,
    PushTelemetryRequest, PushTelemetryResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;
use uuid::Uuid;

use super::{API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS, API_VERSION_PUSH_TELEMETRY};

/// No compression for telemetry payloads.
pub const TELEMETRY_COMPRESSION_NONE: i8 = 0;
/// GZIP compression for telemetry payloads.
pub const TELEMETRY_COMPRESSION_GZIP: i8 = 1;
/// Snappy compression for telemetry payloads.
pub const TELEMETRY_COMPRESSION_SNAPPY: i8 = 2;
/// LZ4 compression for telemetry payloads.
pub const TELEMETRY_COMPRESSION_LZ4: i8 = 3;
/// ZSTD compression for telemetry payloads.
pub const TELEMETRY_COMPRESSION_ZSTD: i8 = 4;

/// Options for a `GetTelemetrySubscriptions` request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetTelemetrySubscriptionsOptions {
    /// Unique ID for this client instance. Use nil for the first request.
    pub client_instance_id: Uuid,
}

impl GetTelemetrySubscriptionsOptions {
    /// Create options for the first telemetry subscription request.
    #[must_use]
    pub fn initial() -> Self {
        Self {
            client_instance_id: Uuid::nil(),
        }
    }

    /// Create options for a known client instance ID.
    #[must_use]
    pub fn for_client_instance(client_instance_id: Uuid) -> Self {
        Self { client_instance_id }
    }
}

/// Parsed response from `GetTelemetrySubscriptions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTelemetrySubscriptionsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Broker error code.
    pub error_code: i16,
    /// Assigned client instance ID.
    pub client_instance_id: Uuid,
    /// Current subscription set ID.
    pub subscription_id: i32,
    /// Accepted Kafka compression type codes for pushed telemetry.
    pub accepted_compression_types: Vec<i8>,
    /// Broker-selected push interval in milliseconds.
    pub push_interval_ms: i32,
    /// Maximum telemetry payload bytes accepted by the broker.
    pub telemetry_max_bytes: i32,
    /// Whether monotonic/counter metrics should be emitted as deltas.
    pub delta_temporality: bool,
    /// Requested metric name prefixes.
    pub requested_metrics: Vec<String>,
}

/// Backwards-friendly shorter alias for telemetry subscription responses.
pub type TelemetrySubscriptionsResponseData = GetTelemetrySubscriptionsResponseData;

/// Client-side telemetry subscription state.
///
/// This helper tracks the broker-assigned telemetry subscription metadata and
/// builds matching `PushTelemetry` options. It intentionally does not encode
/// OpenTelemetry protobuf payloads or run a background scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetrySession {
    /// Unique ID for this client instance.
    pub client_instance_id: Uuid,
    /// Current subscription set ID.
    pub subscription_id: i32,
    /// Accepted Kafka compression type codes for pushed telemetry.
    pub accepted_compression_types: Vec<i8>,
    /// Broker-selected push interval in milliseconds.
    pub push_interval_ms: i32,
    /// Maximum telemetry payload bytes accepted by the broker.
    pub telemetry_max_bytes: i32,
    /// Whether monotonic/counter metrics should be emitted as deltas.
    pub delta_temporality: bool,
    /// Requested metric name prefixes.
    pub requested_metrics: Vec<String>,
}

impl TelemetrySession {
    /// Create an initial session before the broker assigns a client instance ID.
    #[must_use]
    pub fn initial() -> Self {
        Self {
            client_instance_id: Uuid::nil(),
            subscription_id: 0,
            accepted_compression_types: vec![TELEMETRY_COMPRESSION_NONE],
            push_interval_ms: 0,
            telemetry_max_bytes: 0,
            delta_temporality: false,
            requested_metrics: Vec::new(),
        }
    }

    /// Create a session from a successful subscription response.
    #[must_use]
    pub fn from_subscription(response: &GetTelemetrySubscriptionsResponseData) -> Self {
        let mut session = Self::initial();
        let _ = session.apply_subscription(response);
        session
    }

    /// Build options for the next `GetTelemetrySubscriptions` request.
    #[must_use]
    pub fn subscription_options(&self) -> GetTelemetrySubscriptionsOptions {
        GetTelemetrySubscriptionsOptions::for_client_instance(self.client_instance_id)
    }

    /// Merge a subscription response into this session.
    ///
    /// Returns `true` when the response was successful and the session was
    /// updated. Non-zero Kafka error responses are left for callers to handle
    /// and do not modify the current session.
    #[must_use]
    pub fn apply_subscription(&mut self, response: &GetTelemetrySubscriptionsResponseData) -> bool {
        if response.error_code != 0 {
            return false;
        }

        self.client_instance_id = response.client_instance_id;
        self.subscription_id = response.subscription_id;
        self.accepted_compression_types
            .clone_from(&response.accepted_compression_types);
        self.push_interval_ms = response.push_interval_ms;
        self.telemetry_max_bytes = response.telemetry_max_bytes;
        self.delta_temporality = response.delta_temporality;
        self.requested_metrics
            .clone_from(&response.requested_metrics);
        true
    }

    /// Choose a compression type supported by the broker.
    ///
    /// The first preferred type accepted by the broker wins. If none match,
    /// the first broker-accepted type is returned, falling back to no
    /// compression when the broker returned an empty list.
    #[must_use]
    pub fn select_compression_type(&self, preferred: &[i8]) -> i8 {
        preferred
            .iter()
            .copied()
            .find(|candidate| self.accepted_compression_types.contains(candidate))
            .or_else(|| self.accepted_compression_types.first().copied())
            .unwrap_or(TELEMETRY_COMPRESSION_NONE)
    }

    /// Build a non-terminating telemetry push using broker-compatible compression.
    #[must_use]
    pub fn push_options(
        &self,
        metrics: Bytes,
        preferred_compression: &[i8],
    ) -> PushTelemetryOptions {
        PushTelemetryOptions::new(self.client_instance_id, self.subscription_id, metrics)
            .with_compression_type(self.select_compression_type(preferred_compression))
    }

    /// Build a terminating telemetry push using broker-compatible compression.
    #[must_use]
    pub fn terminating_push_options(
        &self,
        metrics: Bytes,
        preferred_compression: &[i8],
    ) -> PushTelemetryOptions {
        self.push_options(metrics, preferred_compression)
            .with_terminating(true)
    }
}

/// Options for a `PushTelemetry` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTelemetryOptions {
    /// Unique ID for this client instance.
    pub client_instance_id: Uuid,
    /// Current subscription set ID.
    pub subscription_id: i32,
    /// Whether the client is terminating telemetry for this connection.
    pub terminating: bool,
    /// Kafka compression type code used for `metrics`.
    pub compression_type: i8,
    /// OpenTelemetry `MetricsData` v1 protobuf payload, optionally compressed.
    pub metrics: Bytes,
}

impl PushTelemetryOptions {
    /// Create a telemetry push request with an uncompressed payload.
    #[must_use]
    pub fn new(client_instance_id: Uuid, subscription_id: i32, metrics: Bytes) -> Self {
        Self {
            client_instance_id,
            subscription_id,
            terminating: false,
            compression_type: TELEMETRY_COMPRESSION_NONE,
            metrics,
        }
    }

    /// Set the Kafka compression type code used for the payload.
    #[must_use]
    pub fn with_compression_type(mut self, compression_type: i8) -> Self {
        self.compression_type = compression_type;
        self
    }

    /// Mark whether this push terminates telemetry for the connection.
    #[must_use]
    pub fn with_terminating(mut self, terminating: bool) -> Self {
        self.terminating = terminating;
        self
    }
}

/// Parsed response from `PushTelemetry`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTelemetryResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Broker error code.
    pub error_code: i16,
}

/// Build a `GetTelemetrySubscriptions` request.
pub fn build_get_telemetry_subscriptions_request(
    correlation_id: i32,
    client_id: &str,
    options: GetTelemetrySubscriptionsOptions,
) -> (RequestHeader, GetTelemetrySubscriptionsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::GetTelemetrySubscriptions,
        API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS,
    );
    let request = GetTelemetrySubscriptionsRequest::default()
        .with_client_instance_id(options.client_instance_id);

    (header, request)
}

/// Build a `PushTelemetry` request.
pub fn build_push_telemetry_request(
    correlation_id: i32,
    client_id: &str,
    options: &PushTelemetryOptions,
) -> (RequestHeader, PushTelemetryRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::PushTelemetry,
        API_VERSION_PUSH_TELEMETRY,
    );
    let request = PushTelemetryRequest::default()
        .with_client_instance_id(options.client_instance_id)
        .with_subscription_id(options.subscription_id)
        .with_terminating(options.terminating)
        .with_compression_type(options.compression_type)
        .with_metrics(options.metrics.clone());

    (header, request)
}

/// Convert a generated `GetTelemetrySubscriptionsResponse` into the crate's public shape.
#[must_use]
pub fn convert_get_telemetry_subscriptions_response(
    response: GetTelemetrySubscriptionsResponse,
) -> GetTelemetrySubscriptionsResponseData {
    GetTelemetrySubscriptionsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        client_instance_id: response.client_instance_id,
        subscription_id: response.subscription_id,
        accepted_compression_types: response.accepted_compression_types,
        push_interval_ms: response.push_interval_ms,
        telemetry_max_bytes: response.telemetry_max_bytes,
        delta_temporality: response.delta_temporality,
        requested_metrics: response
            .requested_metrics
            .into_iter()
            .map(|metric| metric.to_string())
            .collect(),
    }
}

/// Convert a generated `PushTelemetryResponse` into the crate's public shape.
#[must_use]
pub fn convert_push_telemetry_response(
    response: PushTelemetryResponse,
) -> PushTelemetryResponseData {
    let throttle_time_ms = response.throttle_time_ms;
    let error_code = response.error_code;
    let _unknown_tagged_fields = response.unknown_tagged_fields;

    PushTelemetryResponseData {
        throttle_time_ms,
        error_code,
    }
}

fn request_header(
    correlation_id: i32,
    client_id: &str,
    api_key: ApiKey,
    api_version: i16,
) -> RequestHeader {
    RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(api_key as i16)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_telemetry_subscriptions_initial_request_uses_nil_client_instance() {
        let (header, request) = build_get_telemetry_subscriptions_request(
            7,
            "client-a",
            GetTelemetrySubscriptionsOptions::initial(),
        );

        assert_eq!(
            header.request_api_key,
            ApiKey::GetTelemetrySubscriptions as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS
        );
        assert_eq!(header.correlation_id, 7);
        assert_eq!(request.client_instance_id, Uuid::nil());
    }

    #[test]
    fn get_telemetry_subscriptions_request_preserves_client_instance() {
        let client_instance_id = Uuid::from_u128(1);
        let (_header, request) = build_get_telemetry_subscriptions_request(
            8,
            "client-b",
            GetTelemetrySubscriptionsOptions::for_client_instance(client_instance_id),
        );

        assert_eq!(request.client_instance_id, client_instance_id);
    }

    #[test]
    fn push_telemetry_request_preserves_payload_metadata() {
        let client_instance_id = Uuid::from_u128(2);
        let options =
            PushTelemetryOptions::new(client_instance_id, 42, Bytes::from_static(b"otel-metrics"))
                .with_compression_type(1)
                .with_terminating(true);

        let (header, request) = build_push_telemetry_request(9, "client-c", &options);

        assert_eq!(header.request_api_key, ApiKey::PushTelemetry as i16);
        assert_eq!(header.request_api_version, API_VERSION_PUSH_TELEMETRY);
        assert_eq!(request.client_instance_id, client_instance_id);
        assert_eq!(request.subscription_id, 42);
        assert!(request.terminating);
        assert_eq!(request.compression_type, 1);
        assert_eq!(request.metrics, Bytes::from_static(b"otel-metrics"));
    }

    #[test]
    fn get_telemetry_subscriptions_response_maps_all_fields() {
        let client_instance_id = Uuid::from_u128(3);
        let response = GetTelemetrySubscriptionsResponse::default()
            .with_throttle_time_ms(10)
            .with_error_code(0)
            .with_client_instance_id(client_instance_id)
            .with_subscription_id(99)
            .with_accepted_compression_types(vec![0, 1, 4])
            .with_push_interval_ms(30_000)
            .with_telemetry_max_bytes(1_048_576)
            .with_delta_temporality(true)
            .with_requested_metrics(vec![
                StrBytes::from_static_str("org.apache.kafka.producer"),
                StrBytes::from_static_str("org.apache.kafka.consumer"),
            ]);

        let converted = convert_get_telemetry_subscriptions_response(response);

        assert_eq!(converted.throttle_time_ms, 10);
        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.client_instance_id, client_instance_id);
        assert_eq!(converted.subscription_id, 99);
        assert_eq!(converted.accepted_compression_types, vec![0, 1, 4]);
        assert_eq!(converted.push_interval_ms, 30_000);
        assert_eq!(converted.telemetry_max_bytes, 1_048_576);
        assert!(converted.delta_temporality);
        assert_eq!(
            converted.requested_metrics,
            vec![
                "org.apache.kafka.producer".to_owned(),
                "org.apache.kafka.consumer".to_owned(),
            ]
        );
    }

    #[test]
    fn push_telemetry_response_maps_all_fields() {
        let response = PushTelemetryResponse::default()
            .with_throttle_time_ms(11)
            .with_error_code(42);

        let converted = convert_push_telemetry_response(response);

        assert_eq!(
            converted,
            PushTelemetryResponseData {
                throttle_time_ms: 11,
                error_code: 42,
            }
        );
    }

    #[test]
    fn telemetry_session_tracks_successful_subscription_response() {
        let client_instance_id = Uuid::from_u128(4);
        let response = GetTelemetrySubscriptionsResponseData {
            throttle_time_ms: 0,
            error_code: 0,
            client_instance_id,
            subscription_id: 9,
            accepted_compression_types: vec![
                TELEMETRY_COMPRESSION_ZSTD,
                TELEMETRY_COMPRESSION_GZIP,
            ],
            push_interval_ms: 15_000,
            telemetry_max_bytes: 65_536,
            delta_temporality: true,
            requested_metrics: vec!["org.apache.kafka.producer".to_owned()],
        };

        let session = TelemetrySession::from_subscription(&response);

        assert_eq!(session.client_instance_id, client_instance_id);
        assert_eq!(session.subscription_id, 9);
        assert_eq!(session.push_interval_ms, 15_000);
        assert_eq!(session.telemetry_max_bytes, 65_536);
        assert!(session.delta_temporality);
        assert_eq!(
            session.subscription_options().client_instance_id,
            client_instance_id
        );
    }

    #[test]
    fn telemetry_session_ignores_error_subscription_response() {
        let mut session = TelemetrySession::initial();
        let response = GetTelemetrySubscriptionsResponseData {
            throttle_time_ms: 0,
            error_code: 42,
            client_instance_id: Uuid::from_u128(5),
            subscription_id: 10,
            accepted_compression_types: vec![TELEMETRY_COMPRESSION_ZSTD],
            push_interval_ms: 30_000,
            telemetry_max_bytes: 100,
            delta_temporality: true,
            requested_metrics: vec!["ignored".to_owned()],
        };

        assert!(!session.apply_subscription(&response));
        assert_eq!(session, TelemetrySession::initial());
    }

    #[test]
    fn telemetry_session_builds_push_options_with_preferred_compression() {
        let client_instance_id = Uuid::from_u128(6);
        let session = TelemetrySession {
            client_instance_id,
            subscription_id: 11,
            accepted_compression_types: vec![
                TELEMETRY_COMPRESSION_GZIP,
                TELEMETRY_COMPRESSION_ZSTD,
            ],
            push_interval_ms: 1_000,
            telemetry_max_bytes: 1_024,
            delta_temporality: false,
            requested_metrics: Vec::new(),
        };

        let push = session.push_options(
            Bytes::from_static(b"metrics"),
            &[TELEMETRY_COMPRESSION_LZ4, TELEMETRY_COMPRESSION_ZSTD],
        );
        let terminating =
            session.terminating_push_options(Bytes::new(), &[TELEMETRY_COMPRESSION_LZ4]);

        assert_eq!(push.client_instance_id, client_instance_id);
        assert_eq!(push.subscription_id, 11);
        assert_eq!(push.compression_type, TELEMETRY_COMPRESSION_ZSTD);
        assert_eq!(push.metrics, Bytes::from_static(b"metrics"));
        assert!(!push.terminating);
        assert_eq!(terminating.compression_type, TELEMETRY_COMPRESSION_GZIP);
        assert!(terminating.terminating);
    }
}
