//! Async Kafka client for metadata and connection management.

use std::time::Duration;

use rustfs_kafka::client::{
    ApiVersionCache, ApiVersionsResponseData, ConsumerGroupHeartbeatOptions,
    ConsumerGroupHeartbeatResponseData, CreateTopicsResponseData, DeleteTopicsResponseData,
    GetTelemetrySubscriptionsOptions, PushTelemetryOptions, PushTelemetryResponseData,
    SecurityConfig, ShareAcknowledgeOptions, ShareAcknowledgeResponseData, ShareFetchOptions,
    ShareFetchResponseData, ShareGroupHeartbeatOptions, ShareGroupHeartbeatResponseData,
    TelemetrySubscriptionsResponseData, TopicConfig, build_consumer_group_heartbeat_request,
    build_create_topics_protocol_request, build_delete_topics_protocol_request,
    build_get_telemetry_subscriptions_request, build_push_telemetry_request,
    build_share_acknowledge_request, build_share_fetch_request,
    build_share_group_heartbeat_request, convert_api_versions_response,
    convert_consumer_group_heartbeat_response, convert_create_topics_response,
    convert_delete_topics_response, convert_get_telemetry_subscriptions_response,
    convert_push_telemetry_response, convert_share_acknowledge_response,
    convert_share_fetch_response, convert_share_group_heartbeat_response,
};
use rustfs_kafka::error::{ConnectionError, Error, ProtocolError, Result};
use tokio::task::JoinSet;
use tracing::{debug, info};

use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, RequestHeader};
use kafka_protocol::protocol::StrBytes;

use crate::connection::{AsyncConnection, AsyncConnectionPool};
use crate::wire::{get_kp_response, send_kp_request};

/// An async Kafka client for bootstrap and connection management.
///
/// This lightweight client manages a pool of [`AsyncConnection`]s and is
/// intended to be used by other async wrappers (producer/consumer) to obtain
/// connections to brokers without imposing `Sync`/`'static` constraints on the
/// higher-level code. It will attempt to connect to the provided bootstrap
/// hosts on creation (unless the host list is empty), but will not continuously
/// maintain metadata — callers can use [`ensure_connected`] to trigger a
/// reconnection to bootstrap hosts when necessary.
pub struct AsyncKafkaClient {
    pool: AsyncConnectionPool,
    bootstrap_hosts: Vec<String>,
    client_id: String,
    security: Option<SecurityConfig>,
    correlation: i32,
    api_versions: ApiVersionCache,
}

#[derive(Clone, Copy)]
enum RequestVersionMode {
    Exact,
    Negotiated,
}

impl AsyncKafkaClient {
    /// Creates a new async client and connects to the bootstrap brokers.
    pub async fn new(hosts: Vec<String>) -> Result<Self> {
        Self::with_client_id_and_security(hosts, "rustfs-kafka-async".to_owned(), None).await
    }

    /// Creates a new async client with a specific client ID.
    ///
    /// Attempts to connect to the provided `hosts` in order until a
    /// connection succeeds. If no hosts are reachable and the `hosts` list is
    /// non-empty, an error `Error::Connection(ConnectionError::NoHostReachable)`
    /// is returned.
    pub async fn with_client_id(hosts: Vec<String>, client_id: String) -> Result<Self> {
        Self::with_client_id_and_security(hosts, client_id, None).await
    }

    /// Creates a new async client with optional TLS security.
    pub async fn with_client_id_and_security(
        hosts: Vec<String>,
        client_id: String,
        security: Option<SecurityConfig>,
    ) -> Result<Self> {
        let mut pool = AsyncConnectionPool::new_with_security(security.clone());
        let connected = connect_any_bootstrap(&mut pool, &hosts, security.as_ref()).await;

        if !connected && !hosts.is_empty() {
            return Err(Error::Connection(ConnectionError::NoHostReachable));
        }

        info!(
            "AsyncKafkaClient created with {} bootstrap hosts",
            hosts.len()
        );

        Ok(Self {
            pool,
            bootstrap_hosts: hosts,
            client_id,
            security,
            correlation: 0,
            api_versions: ApiVersionCache::new(),
        })
    }

    /// Returns the client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the bootstrap hosts.
    #[must_use]
    pub fn bootstrap_hosts(&self) -> &[String] {
        &self.bootstrap_hosts
    }

    /// Returns the configured optional security settings.
    #[must_use]
    pub fn security(&self) -> Option<&SecurityConfig> {
        self.security.as_ref()
    }

    /// Gets (or creates) a mutable reference to a connection for `host`.
    ///
    /// If a connection for `host` does not yet exist, the underlying
    /// [`AsyncConnection::connect`] is attempted and the connection is stored in
    /// the internal pool. The returned reference is tied to the mutable
    /// borrow of `self` and therefore short-lived.
    pub async fn get_connection(&mut self, host: &str) -> Result<&mut AsyncConnection> {
        self.pool.get(host).await
    }

    /// Gets the list of currently connected hosts.
    ///
    /// This returns the host addresses for which there is an established
    /// connection in the internal pool. The returned `Vec<&str>` is a snapshot
    /// of the current keys and does not hold any borrow on `self` afterwards.
    #[must_use]
    pub fn connected_hosts(&self) -> Vec<&str> {
        self.pool.hosts()
    }

    /// Returns the effective API version for `api_key` on `host`.
    ///
    /// If this client has cached an `ApiVersions` response for the host, the
    /// sync crate's default version is clamped to the broker-advertised range.
    /// Otherwise this returns the sync crate's default fallback version.
    #[must_use]
    pub fn resolved_api_version(&self, host: &str, api_key: i16) -> i16 {
        self.api_versions.get_or_fallback(host, api_key)
    }

    /// Ensures the client has at least one active connection.
    ///
    /// If the client was created with bootstrap hosts and the internal pool is
    /// currently empty, this will attempt to connect to the bootstrap hosts in
    /// order until one succeeds. It is a no-op when `bootstrap_hosts` is empty
    /// or when the pool already contains connections.
    pub async fn ensure_connected(&mut self) -> Result<()> {
        if !self.bootstrap_hosts.is_empty() && self.pool.hosts().is_empty() {
            let security = self.security.clone();
            let connected =
                connect_any_bootstrap(&mut self.pool, &self.bootstrap_hosts, security.as_ref())
                    .await;
            if !connected {
                return Err(Error::Connection(ConnectionError::NoHostReachable));
            }
        }
        Ok(())
    }

    fn next_correlation_id(&mut self) -> i32 {
        self.correlation = (self.correlation + 1) % (1i32 << 30);
        self.correlation
    }

    async fn send_built_protocol_request<Req, Resp, Out, Build, Convert>(
        &mut self,
        operation: &'static str,
        api_version: i16,
        build_request: Build,
        convert_response: Convert,
    ) -> Result<Out>
    where
        Req: kafka_protocol::protocol::Encodable + kafka_protocol::protocol::HeaderVersion,
        Resp: kafka_protocol::protocol::Decodable + kafka_protocol::protocol::HeaderVersion,
        Build: FnOnce(i32, &str) -> (RequestHeader, Req),
        Convert: FnOnce(Resp) -> Out,
    {
        let correlation_id = self.next_correlation_id();
        let client_id = self.client_id.clone();
        let (header, request) = build_request(correlation_id, &client_id);
        let response = self
            .send_prebuilt_protocol_request(
                operation,
                api_version,
                RequestVersionMode::Negotiated,
                &header,
                &request,
            )
            .await?;
        Ok(convert_response(response))
    }

    async fn send_prebuilt_protocol_request<Req, Resp>(
        &mut self,
        operation: &'static str,
        api_version: i16,
        version_mode: RequestVersionMode,
        header: &RequestHeader,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: kafka_protocol::protocol::Encodable + kafka_protocol::protocol::HeaderVersion,
        Resp: kafka_protocol::protocol::Decodable + kafka_protocol::protocol::HeaderVersion,
    {
        self.send_prebuilt_protocol_request_with_host(
            operation,
            api_version,
            version_mode,
            header,
            request,
        )
        .await
        .map(|(_, response)| response)
    }

    async fn send_prebuilt_protocol_request_with_host<Req, Resp>(
        &mut self,
        operation: &'static str,
        api_version: i16,
        version_mode: RequestVersionMode,
        header: &RequestHeader,
        request: &Req,
    ) -> Result<(String, Resp)>
    where
        Req: kafka_protocol::protocol::Encodable + kafka_protocol::protocol::HeaderVersion,
        Resp: kafka_protocol::protocol::Decodable + kafka_protocol::protocol::HeaderVersion,
    {
        let hosts = self.request_hosts();
        let mut last_err: Option<Error> = None;

        for host in hosts {
            let effective_api_version = match version_mode {
                RequestVersionMode::Exact => api_version,
                RequestVersionMode::Negotiated => {
                    self.api_versions
                        .negotiate(&host, header.request_api_key, api_version)
                }
            };
            let conn = match self.get_connection(&host).await {
                Ok(conn) => conn,
                Err(e) => {
                    last_err = Some(e.with_broker_context(&host, operation));
                    continue;
                }
            };

            let mut header = header.clone();
            header.request_api_version = effective_api_version;

            match send_kp_request(conn, &header, request, effective_api_version).await {
                Ok(()) => match get_kp_response::<Resp>(conn, effective_api_version).await {
                    Ok(resp) => return Ok((host, resp)),
                    Err(e) => last_err = Some(e.with_broker_context(&host, operation)),
                },
                Err(e) => last_err = Some(e.with_broker_context(&host, operation)),
            }
        }

        Err(last_err.unwrap_or(Error::Connection(ConnectionError::NoHostReachable)))
    }

    /// Sends a typed low-level `kafka-protocol` request and returns its raw generated response.
    ///
    /// This is intended for advanced users who need async protocol coverage beyond
    /// the stable high-level async producer and consumer methods. Callers are
    /// responsible for choosing a valid `api_key`, `api_version`, and generated
    /// request/response type pair.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable, the request cannot be
    /// encoded, the response cannot be decoded, or the selected API/version is
    /// not accepted by the broker.
    pub async fn send_raw_protocol_request<Req, Resp>(
        &mut self,
        api_key: i16,
        api_version: i16,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: kafka_protocol::protocol::Encodable + kafka_protocol::protocol::HeaderVersion,
        Resp: kafka_protocol::protocol::Decodable + kafka_protocol::protocol::HeaderVersion,
    {
        let correlation_id = self.next_correlation_id();
        let header = RequestHeader::default()
            .with_client_id(Some(StrBytes::from_string(self.client_id.clone())))
            .with_request_api_key(api_key)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id);
        self.send_prebuilt_protocol_request(
            "RawProtocolRequest",
            api_version,
            RequestVersionMode::Exact,
            &header,
            request,
        )
        .await
    }

    /// Fetches Kafka API version ranges advertised by a broker.
    ///
    /// This async convenience API uses the same raw generated protocol path as
    /// [`send_raw_protocol_request`] and converts the response into the public
    /// `rustfs-kafka` response shape.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub async fn fetch_api_versions(&mut self) -> Result<ApiVersionsResponseData> {
        let correlation_id = self.next_correlation_id();
        let header = RequestHeader::default()
            .with_client_id(Some(StrBytes::from_string(self.client_id.clone())))
            .with_request_api_key(ApiKey::ApiVersions as i16)
            .with_request_api_version(0)
            .with_correlation_id(correlation_id);
        let request = ApiVersionsRequest::default();
        let (host, response) = self
            .send_prebuilt_protocol_request_with_host(
                "ApiVersions",
                0,
                RequestVersionMode::Exact,
                &header,
                &request,
            )
            .await?;
        let response = convert_api_versions_response(response);
        self.api_versions
            .insert_api_versions(host, &response.api_keys);
        Ok(response)
    }

    /// Creates topics using the generated `kafka-protocol` `CreateTopics` codec.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable, `timeout` does not fit the
    /// Kafka protocol field, or the broker response cannot be decoded.
    pub async fn create_topics(
        &mut self,
        topics: &[TopicConfig],
        timeout: Duration,
    ) -> Result<CreateTopicsResponseData> {
        let timeout_ms = duration_to_millis_i32(timeout)?;
        self.send_built_protocol_request(
            "CreateTopics",
            2,
            |correlation_id, client_id| {
                build_create_topics_protocol_request(correlation_id, client_id, topics, timeout_ms)
            },
            convert_create_topics_response,
        )
        .await
    }

    /// Deletes topics by name using the generated `kafka-protocol` `DeleteTopics` codec.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable, `timeout` does not fit the
    /// Kafka protocol field, or the broker response cannot be decoded.
    pub async fn delete_topics(
        &mut self,
        topic_names: &[&str],
        timeout: Duration,
    ) -> Result<DeleteTopicsResponseData> {
        let timeout_ms = duration_to_millis_i32(timeout)?;
        self.send_built_protocol_request(
            "DeleteTopics",
            2,
            |correlation_id, client_id| {
                build_delete_topics_protocol_request(
                    correlation_id,
                    client_id,
                    topic_names,
                    timeout_ms,
                )
            },
            convert_delete_topics_response,
        )
        .await
    }

    /// Fetches broker-side client telemetry subscription settings.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub async fn get_telemetry_subscriptions(
        &mut self,
        client_instance_id: uuid::Uuid,
    ) -> Result<TelemetrySubscriptionsResponseData> {
        let options = GetTelemetrySubscriptionsOptions::for_client_instance(client_instance_id);
        self.send_built_protocol_request(
            "GetTelemetrySubscriptions",
            0,
            |correlation_id, client_id| {
                build_get_telemetry_subscriptions_request(correlation_id, client_id, options)
            },
            convert_get_telemetry_subscriptions_response,
        )
        .await
    }

    /// Pushes an encoded client telemetry payload to a broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub async fn push_telemetry(
        &mut self,
        options: &PushTelemetryOptions,
    ) -> Result<PushTelemetryResponseData> {
        self.send_built_protocol_request(
            "PushTelemetry",
            0,
            |correlation_id, client_id| {
                build_push_telemetry_request(correlation_id, client_id, options)
            },
            convert_push_telemetry_response,
        )
        .await
    }

    /// Sends a low-level modern consumer-group heartbeat.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub async fn consumer_group_heartbeat(
        &mut self,
        options: &ConsumerGroupHeartbeatOptions,
    ) -> Result<ConsumerGroupHeartbeatResponseData> {
        self.send_built_protocol_request(
            "ConsumerGroupHeartbeat",
            1,
            |correlation_id, client_id| {
                build_consumer_group_heartbeat_request(correlation_id, client_id, options)
            },
            convert_consumer_group_heartbeat_response,
        )
        .await
    }

    /// Sends a low-level share-group heartbeat.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub async fn share_group_heartbeat(
        &mut self,
        options: &ShareGroupHeartbeatOptions,
    ) -> Result<ShareGroupHeartbeatResponseData> {
        self.send_built_protocol_request(
            "ShareGroupHeartbeat",
            1,
            |correlation_id, client_id| {
                build_share_group_heartbeat_request(correlation_id, client_id, options)
            },
            convert_share_group_heartbeat_response,
        )
        .await
    }

    /// Sends a low-level share fetch request.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub async fn share_fetch(
        &mut self,
        options: &ShareFetchOptions,
    ) -> Result<ShareFetchResponseData> {
        self.send_built_protocol_request(
            "ShareFetch",
            1,
            |correlation_id, client_id| {
                build_share_fetch_request(correlation_id, client_id, options)
            },
            convert_share_fetch_response,
        )
        .await
    }

    /// Sends a low-level share acknowledgement request.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub async fn share_acknowledge(
        &mut self,
        options: &ShareAcknowledgeOptions,
    ) -> Result<ShareAcknowledgeResponseData> {
        self.send_built_protocol_request(
            "ShareAcknowledge",
            1,
            |correlation_id, client_id| {
                build_share_acknowledge_request(correlation_id, client_id, options)
            },
            convert_share_acknowledge_response,
        )
        .await
    }

    fn request_hosts(&self) -> Vec<String> {
        if self.bootstrap_hosts.is_empty() {
            self.connected_hosts()
                .into_iter()
                .map(str::to_owned)
                .collect()
        } else {
            self.bootstrap_hosts.clone()
        }
    }
}

async fn connect_any_bootstrap(
    pool: &mut AsyncConnectionPool,
    hosts: &[String],
    security: Option<&SecurityConfig>,
) -> bool {
    let mut set = JoinSet::new();
    for host in hosts {
        let host = host.clone();
        let security = security.cloned();
        set.spawn(async move {
            let connection =
                crate::connection::AsyncConnection::connect(&host, security.as_ref()).await;
            (host, connection)
        });
    }

    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((host, Ok(connection))) => {
                pool.insert(host, connection);
                return true;
            }
            Ok((host, Err(e))) => {
                debug!("Failed to connect to {}: {}", host, e);
            }
            Err(e) => {
                debug!("Bootstrap connect task failed to join: {}", e);
            }
        }
    }

    false
}

fn duration_to_millis_i32(timeout: Duration) -> Result<i32> {
    i32::try_from(timeout.as_millis()).map_err(|_| Error::Protocol(ProtocolError::Codec))
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use kafka_protocol::messages::{
        ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest,
    };
    use kafka_protocol::protocol::{Decodable, HeaderVersion};
    use rustfs_kafka::error::{ConnectionError, Error};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn new_with_empty_hosts_succeeds() {
        let result = AsyncKafkaClient::new(vec![]).await;
        assert!(result.is_ok());
        let client = result.unwrap();
        assert!(client.bootstrap_hosts().is_empty());
        assert!(client.connected_hosts().is_empty());
    }

    #[tokio::test]
    async fn new_with_unreachable_hosts_returns_error() {
        let result = AsyncKafkaClient::new(vec!["127.0.0.1:1".to_owned()]).await;
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::NoHostReachable))
        ));
    }

    #[tokio::test]
    async fn with_client_id_unreachable_returns_error() {
        let result = AsyncKafkaClient::with_client_id(
            vec!["127.0.0.1:1".to_owned()],
            "my-custom-client".to_owned(),
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::NoHostReachable))
        ));
    }

    #[tokio::test]
    async fn ensure_connected_with_empty_hosts_is_ok() {
        let client = AsyncKafkaClient {
            pool: AsyncConnectionPool::new(),
            bootstrap_hosts: vec![],
            client_id: "test".to_owned(),
            security: None,
            correlation: 0,
            api_versions: ApiVersionCache::new(),
        };
        // ensure_connected with empty bootstrap_hosts is a no-op
        assert!(client.bootstrap_hosts.is_empty());
        assert!(client.connected_hosts().is_empty());
    }

    #[tokio::test]
    async fn raw_protocol_request_surfaces_no_host() {
        let mut client = AsyncKafkaClient::new(vec![]).await.unwrap();
        let request = kafka_protocol::messages::FetchSnapshotRequest::default();

        let result: Result<kafka_protocol::messages::FetchSnapshotResponse> = client
            .send_raw_protocol_request(
                kafka_protocol::messages::ApiKey::FetchSnapshot as i16,
                1,
                &request,
            )
            .await;

        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::NoHostReachable))
        ));
    }

    #[tokio::test]
    async fn async_typed_protocol_helpers_surface_no_host() {
        let mut client = AsyncKafkaClient::new(vec![]).await.unwrap();

        assert_no_host(client.fetch_api_versions().await);
        assert_no_host(
            client
                .create_topics(&[TopicConfig::new("topic-a")], Duration::from_secs(10))
                .await,
        );
        assert_no_host(
            client
                .delete_topics(&["topic-a"], Duration::from_secs(10))
                .await,
        );
        assert_no_host(client.get_telemetry_subscriptions(uuid::Uuid::nil()).await);
        assert_no_host(
            client
                .push_telemetry(&PushTelemetryOptions::new(
                    uuid::Uuid::nil(),
                    0,
                    bytes::Bytes::new(),
                ))
                .await,
        );
        assert_no_host(
            client
                .consumer_group_heartbeat(&ConsumerGroupHeartbeatOptions::new(
                    "group-a", "member-a",
                ))
                .await,
        );
        assert_no_host(
            client
                .share_group_heartbeat(&ShareGroupHeartbeatOptions::new("share-a", "member-a"))
                .await,
        );
        assert_no_host(
            client
                .share_fetch(&ShareFetchOptions::new("share-a", "member-a"))
                .await,
        );
        assert_no_host(
            client
                .share_acknowledge(&ShareAcknowledgeOptions::new("share-a", "member-a"))
                .await,
        );
    }

    #[test]
    fn duration_to_millis_rejects_timeout_overflow() {
        assert!(duration_to_millis_i32(Duration::from_millis(i32::MAX as u64)).is_ok());
        assert!(matches!(
            duration_to_millis_i32(Duration::from_millis(i32::MAX as u64 + 1)),
            Err(Error::Protocol(ProtocolError::Codec))
        ));
    }

    #[tokio::test]
    async fn raw_protocol_request_round_trips_generated_types() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut size_buf = [0u8; 4];
            socket.read_exact(&mut size_buf).await.unwrap();
            let size = i32::from_be_bytes(size_buf);
            let mut frame = vec![0; usize::try_from(size).unwrap()];
            socket.read_exact(&mut frame).await.unwrap();

            let mut bytes = bytes::Bytes::from(frame);
            let header =
                RequestHeader::decode(&mut bytes, ApiVersionsRequest::header_version(0)).unwrap();
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            assert_eq!(header.request_api_version, 0);
            assert_eq!(header.correlation_id, 1);
            assert_eq!(
                header.client_id.as_ref().map(ToString::to_string),
                Some("async-raw-test".to_owned())
            );
            assert!(!bytes.has_remaining());

            let mut response_frame = Vec::new();
            response_frame.extend_from_slice(&1i32.to_be_bytes());
            response_frame.extend_from_slice(&0i16.to_be_bytes());
            response_frame.extend_from_slice(&1i32.to_be_bytes());
            response_frame.extend_from_slice(&(ApiKey::ApiVersions as i16).to_be_bytes());
            response_frame.extend_from_slice(&0i16.to_be_bytes());
            response_frame.extend_from_slice(&4i16.to_be_bytes());

            let total_len = i32::try_from(response_frame.len()).unwrap();
            socket.write_all(&total_len.to_be_bytes()).await.unwrap();
            socket.write_all(&response_frame).await.unwrap();
        });

        let mut client =
            AsyncKafkaClient::with_client_id(vec![addr.to_string()], "async-raw-test".to_owned())
                .await
                .unwrap();

        let response: ApiVersionsResponse = client
            .send_raw_protocol_request(
                ApiKey::ApiVersions as i16,
                0,
                &ApiVersionsRequest::default(),
            )
            .await
            .unwrap();

        server.await.unwrap();
        assert_eq!(response.error_code, 0);
        assert_eq!(response.api_keys.len(), 1);
        assert_eq!(response.api_keys[0].api_key, ApiKey::ApiVersions as i16);
    }

    #[tokio::test]
    async fn raw_protocol_request_preserves_explicit_version_with_cache() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut size_buf = [0u8; 4];
            socket.read_exact(&mut size_buf).await.unwrap();
            let size = i32::from_be_bytes(size_buf);
            let mut frame = vec![0; usize::try_from(size).unwrap()];
            socket.read_exact(&mut frame).await.unwrap();

            let mut bytes = bytes::Bytes::from(frame);
            let header =
                RequestHeader::decode(&mut bytes, ApiVersionsRequest::header_version(0)).unwrap();
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            assert_eq!(header.request_api_version, 0);

            let mut response_frame = Vec::new();
            response_frame.extend_from_slice(&1i32.to_be_bytes());
            response_frame.extend_from_slice(&0i16.to_be_bytes());
            response_frame.extend_from_slice(&0i32.to_be_bytes());

            let total_len = i32::try_from(response_frame.len()).unwrap();
            socket.write_all(&total_len.to_be_bytes()).await.unwrap();
            socket.write_all(&response_frame).await.unwrap();
        });

        let mut client = AsyncKafkaClient::with_client_id(
            vec![addr.to_string()],
            "async-raw-version-test".to_owned(),
        )
        .await
        .unwrap();
        client.api_versions.insert_api_versions(
            addr.to_string(),
            &[rustfs_kafka::client::BrokerApiVersion {
                api_key: ApiKey::ApiVersions as i16,
                min_version: 1,
                max_version: 4,
            }],
        );

        let response: ApiVersionsResponse = client
            .send_raw_protocol_request(
                ApiKey::ApiVersions as i16,
                0,
                &ApiVersionsRequest::default(),
            )
            .await
            .unwrap();

        server.await.unwrap();
        assert_eq!(response.error_code, 0);
        assert!(response.api_keys.is_empty());
    }

    #[tokio::test]
    async fn fetch_api_versions_populates_version_cache() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut size_buf = [0u8; 4];
            socket.read_exact(&mut size_buf).await.unwrap();
            let size = i32::from_be_bytes(size_buf);
            let mut frame = vec![0; usize::try_from(size).unwrap()];
            socket.read_exact(&mut frame).await.unwrap();

            let mut bytes = bytes::Bytes::from(frame);
            let header =
                RequestHeader::decode(&mut bytes, ApiVersionsRequest::header_version(0)).unwrap();
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            assert_eq!(header.request_api_version, 0);
            assert_eq!(header.correlation_id, 1);

            let mut response_frame = Vec::new();
            response_frame.extend_from_slice(&1i32.to_be_bytes());
            response_frame.extend_from_slice(&0i16.to_be_bytes());
            response_frame.extend_from_slice(&1i32.to_be_bytes());
            response_frame.extend_from_slice(&(ApiKey::CreateTopics as i16).to_be_bytes());
            response_frame.extend_from_slice(&0i16.to_be_bytes());
            response_frame.extend_from_slice(&1i16.to_be_bytes());

            let total_len = i32::try_from(response_frame.len()).unwrap();
            socket.write_all(&total_len.to_be_bytes()).await.unwrap();
            socket.write_all(&response_frame).await.unwrap();
        });

        let mut client = AsyncKafkaClient::with_client_id(
            vec![addr.to_string()],
            "async-version-cache-test".to_owned(),
        )
        .await
        .unwrap();

        let response = client.fetch_api_versions().await.unwrap();

        server.await.unwrap();
        assert_eq!(response.api_keys.len(), 1);
        assert_eq!(response.api_keys[0].api_key, ApiKey::CreateTopics as i16);
        assert_eq!(
            client.resolved_api_version(&addr.to_string(), ApiKey::CreateTopics as i16),
            1
        );
    }

    #[tokio::test]
    async fn typed_create_topics_uses_real_header_context() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut size_buf = [0u8; 4];
            socket.read_exact(&mut size_buf).await.unwrap();
            let size = i32::from_be_bytes(size_buf);
            let mut frame = vec![0; usize::try_from(size).unwrap()];
            socket.read_exact(&mut frame).await.unwrap();

            let mut bytes = bytes::Bytes::from(frame);
            let header =
                RequestHeader::decode(&mut bytes, CreateTopicsRequest::header_version(2)).unwrap();
            assert_eq!(header.request_api_key, ApiKey::CreateTopics as i16);
            assert_eq!(header.request_api_version, 2);
            assert_eq!(header.correlation_id, 1);
            assert_eq!(
                header.client_id.as_ref().map(ToString::to_string),
                Some("async-typed-test".to_owned())
            );

            assert_eq!(bytes.get_i32(), 1);
            let topic_name_len = usize::try_from(bytes.get_i16()).unwrap();
            assert_eq!(&bytes.copy_to_bytes(topic_name_len)[..], b"topic-a");
            assert_eq!(bytes.get_i32(), 1);
            assert_eq!(bytes.get_i16(), 1);
            assert_eq!(bytes.get_i32(), 0);
            assert_eq!(bytes.get_i32(), 0);
            assert_eq!(bytes.get_i32(), 10_000);
            assert_eq!(bytes.get_u8(), 0);
            assert!(!bytes.has_remaining());

            let mut response_frame = Vec::new();
            response_frame.extend_from_slice(&1i32.to_be_bytes());
            response_frame.extend_from_slice(&0i32.to_be_bytes());
            response_frame.extend_from_slice(&1i32.to_be_bytes());
            response_frame.extend_from_slice(&7i16.to_be_bytes());
            response_frame.extend_from_slice(b"topic-a");
            response_frame.extend_from_slice(&0i16.to_be_bytes());
            response_frame.extend_from_slice(&(-1i16).to_be_bytes());

            let total_len = i32::try_from(response_frame.len()).unwrap();
            socket.write_all(&total_len.to_be_bytes()).await.unwrap();
            socket.write_all(&response_frame).await.unwrap();
        });

        let mut client =
            AsyncKafkaClient::with_client_id(vec![addr.to_string()], "async-typed-test".to_owned())
                .await
                .unwrap();

        let response = client
            .create_topics(&[TopicConfig::new("topic-a")], Duration::from_secs(10))
            .await
            .unwrap();

        server.await.unwrap();
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].name, "topic-a");
        assert_eq!(response.results[0].error_code, 0);
    }

    fn assert_no_host<T>(result: Result<T>) {
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::NoHostReachable))
        ));
    }
}
