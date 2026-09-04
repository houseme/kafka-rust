//! API version negotiation via Kafka's `ApiVersionsRequest` (API key 18).
//!
//! Infrastructure for negotiating API versions with Kafka brokers. Currently
//! used during metadata requests; full per-request version negotiation will
//! be wired up in a future release.

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::protocol::{
    API_VERSION_ADD_OFFSETS_TO_TXN, API_VERSION_ADD_RAFT_VOTER, API_VERSION_ALLOCATE_PRODUCER_IDS,
    API_VERSION_ALTER_CLIENT_QUOTAS, API_VERSION_ALTER_CONFIGS, API_VERSION_ALTER_PARTITION,
    API_VERSION_ALTER_PARTITION_REASSIGNMENTS, API_VERSION_ALTER_REPLICA_LOG_DIRS,
    API_VERSION_ALTER_SHARE_GROUP_OFFSETS, API_VERSION_ALTER_USER_SCRAM_CREDENTIALS,
    API_VERSION_ASSIGN_REPLICAS_TO_DIRS, API_VERSION_BEGIN_QUORUM_EPOCH,
    API_VERSION_BROKER_HEARTBEAT, API_VERSION_BROKER_REGISTRATION,
    API_VERSION_CONSUMER_GROUP_DESCRIBE, API_VERSION_CONSUMER_GROUP_HEARTBEAT,
    API_VERSION_CONTROLLER_REGISTRATION, API_VERSION_CREATE_ACLS,
    API_VERSION_CREATE_DELEGATION_TOKEN, API_VERSION_CREATE_PARTITIONS, API_VERSION_DELETE_ACLS,
    API_VERSION_DELETE_GROUPS, API_VERSION_DELETE_RECORDS, API_VERSION_DELETE_SHARE_GROUP_OFFSETS,
    API_VERSION_DELETE_SHARE_GROUP_STATE, API_VERSION_DESCRIBE_ACLS,
    API_VERSION_DESCRIBE_CLIENT_QUOTAS, API_VERSION_DESCRIBE_CLUSTER, API_VERSION_DESCRIBE_CONFIGS,
    API_VERSION_DESCRIBE_DELEGATION_TOKEN, API_VERSION_DESCRIBE_GROUPS,
    API_VERSION_DESCRIBE_LOG_DIRS, API_VERSION_DESCRIBE_PRODUCERS, API_VERSION_DESCRIBE_QUORUM,
    API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS, API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
    API_VERSION_DESCRIBE_TRANSACTIONS, API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
    API_VERSION_ELECT_LEADERS, API_VERSION_END_QUORUM_EPOCH, API_VERSION_ENVELOPE,
    API_VERSION_EXPIRE_DELEGATION_TOKEN, API_VERSION_FETCH, API_VERSION_FETCH_SNAPSHOT,
    API_VERSION_FIND_COORDINATOR, API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS,
    API_VERSION_INCREMENTAL_ALTER_CONFIGS, API_VERSION_INITIALIZE_SHARE_GROUP_STATE,
    API_VERSION_LIST_CONFIG_RESOURCES, API_VERSION_LIST_GROUPS, API_VERSION_LIST_OFFSETS,
    API_VERSION_LIST_PARTITION_REASSIGNMENTS, API_VERSION_LIST_TRANSACTIONS, API_VERSION_METADATA,
    API_VERSION_OFFSET_COMMIT, API_VERSION_OFFSET_DELETE, API_VERSION_OFFSET_FETCH,
    API_VERSION_OFFSET_FOR_LEADER_EPOCH, API_VERSION_PRODUCE, API_VERSION_PUSH_TELEMETRY,
    API_VERSION_READ_SHARE_GROUP_STATE, API_VERSION_READ_SHARE_GROUP_STATE_SUMMARY,
    API_VERSION_REMOVE_RAFT_VOTER, API_VERSION_RENEW_DELEGATION_TOKEN,
    API_VERSION_SHARE_ACKNOWLEDGE, API_VERSION_SHARE_FETCH, API_VERSION_SHARE_GROUP_DESCRIBE,
    API_VERSION_SHARE_GROUP_HEARTBEAT, API_VERSION_TXN_OFFSET_COMMIT,
    API_VERSION_UNREGISTER_BROKER, API_VERSION_UPDATE_FEATURES, API_VERSION_UPDATE_RAFT_VOTER,
    API_VERSION_VOTE, API_VERSION_WRITE_SHARE_GROUP_STATE, API_VERSION_WRITE_TXN_MARKERS,
};
use tracing::{debug, info};

use crate::network::KafkaConnection;

pub use crate::protocol::api_keys as api_key;

/// One Kafka API version range advertised by a broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerApiVersion {
    /// Kafka API key.
    pub api_key: i16,
    /// Minimum version supported by the broker.
    pub min_version: i16,
    /// Maximum version supported by the broker.
    pub max_version: i16,
}

/// Parsed response from an `ApiVersions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponseData {
    /// Top-level broker error code.
    pub error_code: i16,
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// API version ranges advertised by the broker.
    pub api_keys: Vec<BrokerApiVersion>,
}

/// The version of the `ApiVersions` request we send.
///
/// Use the most compatible non-flexible request to avoid broker/header schema
/// mismatches during bootstrap negotiation.
const API_VERSIONS_REQUEST_VERSION: i16 = 0;

/// Negotiated API version ranges for a single broker.
#[derive(Debug, Clone)]
pub struct BrokerApiVersions {
    #[allow(dead_code)]
    versions: HashMap<i16, (i16, i16)>, // api_key -> (min_version, max_version)
}

impl BrokerApiVersions {
    /// Create from the parsed `ApiVersions` response.
    fn from_response(resp: kafka_protocol::messages::ApiVersionsResponse) -> BrokerApiVersions {
        let response = convert_api_versions_response(resp);
        BrokerApiVersions::from_api_versions(&response.api_keys)
    }

    pub(crate) fn from_api_versions(api_versions: &[BrokerApiVersion]) -> BrokerApiVersions {
        let versions = api_versions
            .iter()
            .map(|api_version| {
                (
                    api_version.api_key,
                    (api_version.min_version, api_version.max_version),
                )
            })
            .collect();

        BrokerApiVersions { versions }
    }

    /// Get the best version for the given API key, clamped to the requested range.
    /// Returns `fallback` if the broker doesn't support the API.
    #[allow(dead_code)]
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
            debug!(
                "API key {}: not supported by broker, using fallback {}",
                api_key, fallback
            );
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
    let kp_resp = fetch_api_versions_response_raw(conn, correlation_id, client_id)?;
    let result = BrokerApiVersions::from_response(kp_resp);
    info!("Negotiated API versions: {:?}", result);
    Ok(result)
}

/// Send an `ApiVersionsRequest` and return public response data.
pub fn fetch_api_versions_data(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
) -> Result<ApiVersionsResponseData> {
    fetch_api_versions_response_raw(conn, correlation_id, client_id)
        .map(convert_api_versions_response)
}

/// Convert a generated `ApiVersionsResponse` into the crate's public shape.
#[must_use]
pub fn convert_api_versions_response(
    response: kafka_protocol::messages::ApiVersionsResponse,
) -> ApiVersionsResponseData {
    ApiVersionsResponseData {
        error_code: response.error_code,
        throttle_time_ms: response.throttle_time_ms,
        api_keys: response
            .api_keys
            .into_iter()
            .map(|api_version| BrokerApiVersion {
                api_key: api_version.api_key,
                min_version: api_version.min_version,
                max_version: api_version.max_version,
            })
            .collect(),
    }
}

fn fetch_api_versions_response_raw(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
) -> Result<kafka_protocol::messages::ApiVersionsResponse> {
    use bytes::BytesMut;
    use kafka_protocol::messages::{
        ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};

    let request = ApiVersionsRequest::default();

    let header = RequestHeader::default()
        .with_request_api_key(api_key::API_VERSIONS)
        .with_request_api_version(API_VERSIONS_REQUEST_VERSION)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(kafka_protocol::protocol::StrBytes::from_string(
            client_id.to_owned(),
        )));
    let request_header_version = ApiVersionsRequest::header_version(API_VERSIONS_REQUEST_VERSION);
    let response_header_version = ApiVersionsResponse::header_version(API_VERSIONS_REQUEST_VERSION);

    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, request_header_version)
        .map_err(|_| Error::codec())?;

    let mut body_buf = BytesMut::new();
    request
        .encode(&mut body_buf, API_VERSIONS_REQUEST_VERSION)
        .map_err(|_| Error::codec())?;

    let total_len = crate::protocol::usize_to_i32(header_buf.len() + body_buf.len())?;
    let out_len = crate::protocol::non_negative_i32_to_usize(total_len)?;
    let mut out = BytesMut::with_capacity(4 + out_len);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body_buf);

    conn.send(&out)?;

    let size = {
        let mut buf = [0u8; 4];
        conn.read_exact(&mut buf)?;
        i32::from_be_bytes(buf)
    };
    let resp_bytes = conn.read_exact_alloc(crate::protocol::non_negative_i32_to_u64(size)?)?;
    let mut bytes = resp_bytes;
    let _resp_header =
        ResponseHeader::decode(&mut bytes, response_header_version).map_err(|_| Error::codec())?;

    let kp_resp = kafka_protocol::messages::ApiVersionsResponse::decode(
        &mut bytes,
        API_VERSIONS_REQUEST_VERSION,
    )
    .map_err(|_| Error::codec())?;

    Ok(kp_resp)
}

/// Stores negotiated API versions per broker.
#[derive(Debug, Default)]
pub struct ApiVersionCache {
    broker_versions: HashMap<String, BrokerApiVersions>,
}

impl ApiVersionCache {
    #[must_use]
    pub fn new() -> Self {
        ApiVersionCache {
            broker_versions: HashMap::new(),
        }
    }

    /// Check if we have negotiated versions for a broker.
    #[must_use]
    pub fn contains(&self, host: &str) -> bool {
        self.broker_versions.contains_key(host)
    }

    /// Insert negotiated versions for a broker.
    pub fn insert(&mut self, host: String, versions: BrokerApiVersions) {
        self.broker_versions.insert(host, versions);
    }

    /// Get or fetch API versions for a broker.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker cannot be reached or if the
    /// `ApiVersions` response cannot be decoded.
    #[allow(dead_code)]
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
        self.broker_versions.get(host).ok_or_else(Error::codec)
    }

    /// Invalidate cached versions for a broker (e.g., after reconnect).
    #[allow(dead_code)]
    pub fn invalidate(&mut self, host: &str) {
        self.broker_versions.remove(host);
    }

    /// Negotiate the best API version for a specific broker and API key.
    #[must_use]
    #[allow(dead_code)]
    pub fn negotiate(&self, host: &str, api_key: i16, fallback: i16) -> i16 {
        self.broker_versions
            .get(host)
            .map_or(fallback, |v| v.negotiate(api_key, fallback))
    }

    /// Returns the negotiated version for the given API key,
    /// falling back to a safe default if no version information is available.
    #[must_use]
    #[allow(dead_code)]
    pub fn get_or_fallback(&self, host: &str, api_key: i16) -> i16 {
        let fallback = Self::fallback_version(api_key);
        self.negotiate(host, api_key, fallback)
    }

    /// Returns the crate's default fallback version for an API key.
    #[must_use]
    #[allow(dead_code)]
    pub fn fallback_version(api_key: i16) -> i16 {
        match api_key {
            api_key::PRODUCE => API_VERSION_PRODUCE,
            api_key::FETCH => API_VERSION_FETCH,
            api_key::METADATA => API_VERSION_METADATA,
            api_key::LIST_OFFSETS => API_VERSION_LIST_OFFSETS,
            api_key::FIND_COORDINATOR => API_VERSION_FIND_COORDINATOR,
            api_key::OFFSET_COMMIT => API_VERSION_OFFSET_COMMIT,
            api_key::OFFSET_FETCH => API_VERSION_OFFSET_FETCH,
            api_key::DELETE_RECORDS => API_VERSION_DELETE_RECORDS,
            api_key::OFFSET_FOR_LEADER_EPOCH => API_VERSION_OFFSET_FOR_LEADER_EPOCH,
            api_key::DESCRIBE_GROUPS => API_VERSION_DESCRIBE_GROUPS,
            api_key::LIST_GROUPS => API_VERSION_LIST_GROUPS,
            api_key::DESCRIBE_ACLS => API_VERSION_DESCRIBE_ACLS,
            api_key::CREATE_ACLS => API_VERSION_CREATE_ACLS,
            api_key::DELETE_ACLS => API_VERSION_DELETE_ACLS,
            api_key::DESCRIBE_CONFIGS => API_VERSION_DESCRIBE_CONFIGS,
            api_key::ALTER_CONFIGS => API_VERSION_ALTER_CONFIGS,
            api_key::ALTER_REPLICA_LOG_DIRS => API_VERSION_ALTER_REPLICA_LOG_DIRS,
            api_key::CREATE_DELEGATION_TOKEN => API_VERSION_CREATE_DELEGATION_TOKEN,
            api_key::RENEW_DELEGATION_TOKEN => API_VERSION_RENEW_DELEGATION_TOKEN,
            api_key::EXPIRE_DELEGATION_TOKEN => API_VERSION_EXPIRE_DELEGATION_TOKEN,
            api_key::INCREMENTAL_ALTER_CONFIGS => API_VERSION_INCREMENTAL_ALTER_CONFIGS,
            api_key::DESCRIBE_LOG_DIRS => API_VERSION_DESCRIBE_LOG_DIRS,
            api_key::CREATE_PARTITIONS => API_VERSION_CREATE_PARTITIONS,
            api_key::DESCRIBE_DELEGATION_TOKEN => API_VERSION_DESCRIBE_DELEGATION_TOKEN,
            api_key::DELETE_GROUPS => API_VERSION_DELETE_GROUPS,
            api_key::ELECT_LEADERS => API_VERSION_ELECT_LEADERS,
            api_key::ALTER_PARTITION_REASSIGNMENTS => API_VERSION_ALTER_PARTITION_REASSIGNMENTS,
            api_key::LIST_PARTITION_REASSIGNMENTS => API_VERSION_LIST_PARTITION_REASSIGNMENTS,
            api_key::OFFSET_DELETE => API_VERSION_OFFSET_DELETE,
            api_key::DESCRIBE_CLIENT_QUOTAS => API_VERSION_DESCRIBE_CLIENT_QUOTAS,
            api_key::ALTER_CLIENT_QUOTAS => API_VERSION_ALTER_CLIENT_QUOTAS,
            api_key::DESCRIBE_USER_SCRAM_CREDENTIALS => API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
            api_key::ALTER_USER_SCRAM_CREDENTIALS => API_VERSION_ALTER_USER_SCRAM_CREDENTIALS,
            api_key::DESCRIBE_QUORUM => API_VERSION_DESCRIBE_QUORUM,
            api_key::UPDATE_FEATURES => API_VERSION_UPDATE_FEATURES,
            api_key::DESCRIBE_CLUSTER => API_VERSION_DESCRIBE_CLUSTER,
            api_key::DESCRIBE_PRODUCERS => API_VERSION_DESCRIBE_PRODUCERS,
            api_key::UNREGISTER_BROKER => API_VERSION_UNREGISTER_BROKER,
            api_key::ASSIGN_REPLICAS_TO_DIRS => API_VERSION_ASSIGN_REPLICAS_TO_DIRS,
            api_key::ADD_RAFT_VOTER => API_VERSION_ADD_RAFT_VOTER,
            api_key::REMOVE_RAFT_VOTER => API_VERSION_REMOVE_RAFT_VOTER,
            api_key::UPDATE_RAFT_VOTER => API_VERSION_UPDATE_RAFT_VOTER,
            api_key::DESCRIBE_TRANSACTIONS => API_VERSION_DESCRIBE_TRANSACTIONS,
            api_key::LIST_TRANSACTIONS => API_VERSION_LIST_TRANSACTIONS,
            api_key::CONSUMER_GROUP_HEARTBEAT => API_VERSION_CONSUMER_GROUP_HEARTBEAT,
            api_key::ADD_OFFSETS_TO_TXN => API_VERSION_ADD_OFFSETS_TO_TXN,
            api_key::TXN_OFFSET_COMMIT => API_VERSION_TXN_OFFSET_COMMIT,
            api_key::WRITE_TXN_MARKERS => API_VERSION_WRITE_TXN_MARKERS,
            api_key::VOTE => API_VERSION_VOTE,
            api_key::BEGIN_QUORUM_EPOCH => API_VERSION_BEGIN_QUORUM_EPOCH,
            api_key::END_QUORUM_EPOCH => API_VERSION_END_QUORUM_EPOCH,
            api_key::ALTER_PARTITION => API_VERSION_ALTER_PARTITION,
            api_key::ENVELOPE => API_VERSION_ENVELOPE,
            api_key::FETCH_SNAPSHOT => API_VERSION_FETCH_SNAPSHOT,
            api_key::BROKER_REGISTRATION => API_VERSION_BROKER_REGISTRATION,
            api_key::BROKER_HEARTBEAT => API_VERSION_BROKER_HEARTBEAT,
            api_key::ALLOCATE_PRODUCER_IDS => API_VERSION_ALLOCATE_PRODUCER_IDS,
            api_key::CONTROLLER_REGISTRATION => API_VERSION_CONTROLLER_REGISTRATION,
            api_key::CONSUMER_GROUP_DESCRIBE => API_VERSION_CONSUMER_GROUP_DESCRIBE,
            api_key::GET_TELEMETRY_SUBSCRIPTIONS => API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS,
            api_key::PUSH_TELEMETRY => API_VERSION_PUSH_TELEMETRY,
            api_key::LIST_CONFIG_RESOURCES => API_VERSION_LIST_CONFIG_RESOURCES,
            api_key::DESCRIBE_TOPIC_PARTITIONS => API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
            api_key::SHARE_GROUP_DESCRIBE => API_VERSION_SHARE_GROUP_DESCRIBE,
            api_key::SHARE_GROUP_HEARTBEAT => API_VERSION_SHARE_GROUP_HEARTBEAT,
            api_key::SHARE_FETCH => API_VERSION_SHARE_FETCH,
            api_key::SHARE_ACKNOWLEDGE => API_VERSION_SHARE_ACKNOWLEDGE,
            api_key::INITIALIZE_SHARE_GROUP_STATE => API_VERSION_INITIALIZE_SHARE_GROUP_STATE,
            api_key::READ_SHARE_GROUP_STATE => API_VERSION_READ_SHARE_GROUP_STATE,
            api_key::WRITE_SHARE_GROUP_STATE => API_VERSION_WRITE_SHARE_GROUP_STATE,
            api_key::DELETE_SHARE_GROUP_STATE => API_VERSION_DELETE_SHARE_GROUP_STATE,
            api_key::READ_SHARE_GROUP_STATE_SUMMARY => API_VERSION_READ_SHARE_GROUP_STATE_SUMMARY,
            api_key::DESCRIBE_SHARE_GROUP_OFFSETS => API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
            api_key::ALTER_SHARE_GROUP_OFFSETS => API_VERSION_ALTER_SHARE_GROUP_OFFSETS,
            api_key::DELETE_SHARE_GROUP_OFFSETS => API_VERSION_DELETE_SHARE_GROUP_OFFSETS,
            _ => 0,
        }
    }

    /// Returns true if no broker versions have been cached.
    #[must_use]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.broker_versions.is_empty()
    }
}

/// Resolve the effective API version for a given API key using cached negotiations.
/// Falls back to hardcoded defaults if no negotiation has occurred.
#[allow(dead_code)]
pub fn resolve_api_version(cache: &ApiVersionCache, host: &str, api_key: i16, default: i16) -> i16 {
    cache.negotiate(host, api_key, default)
}

/// Resolve all our API versions for a given broker.
#[allow(dead_code)]
pub fn resolve_all_api_versions(cache: &ApiVersionCache, host: &str) -> ApiVersions {
    let mut versions = ApiVersions::default();
    resolve_core_api_versions(cache, host, &mut versions);
    resolve_admin_api_versions(cache, host, &mut versions);
    resolve_security_api_versions(cache, host, &mut versions);
    resolve_transaction_api_versions(cache, host, &mut versions);
    resolve_group_api_versions(cache, host, &mut versions);
    resolve_internal_api_versions(cache, host, &mut versions);
    versions
}

fn resolve_core_api_versions(cache: &ApiVersionCache, host: &str, versions: &mut ApiVersions) {
    macro_rules! version {
        ($api_key:ident, $default:ident) => {
            resolve_api_version(cache, host, api_key::$api_key, $default)
        };
    }

    versions.produce = version!(PRODUCE, API_VERSION_PRODUCE);
    versions.fetch = version!(FETCH, API_VERSION_FETCH);
    versions.metadata = version!(METADATA, API_VERSION_METADATA);
    versions.list_offsets = version!(LIST_OFFSETS, API_VERSION_LIST_OFFSETS);
    versions.find_coordinator = version!(FIND_COORDINATOR, API_VERSION_FIND_COORDINATOR);
    versions.offset_commit = version!(OFFSET_COMMIT, API_VERSION_OFFSET_COMMIT);
    versions.offset_fetch = version!(OFFSET_FETCH, API_VERSION_OFFSET_FETCH);
    versions.get_telemetry_subscriptions = version!(
        GET_TELEMETRY_SUBSCRIPTIONS,
        API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS
    );
    versions.push_telemetry = version!(PUSH_TELEMETRY, API_VERSION_PUSH_TELEMETRY);
}

fn resolve_admin_api_versions(cache: &ApiVersionCache, host: &str, versions: &mut ApiVersions) {
    macro_rules! version {
        ($api_key:ident, $default:ident) => {
            resolve_api_version(cache, host, api_key::$api_key, $default)
        };
    }

    versions.delete_records = version!(DELETE_RECORDS, API_VERSION_DELETE_RECORDS);
    versions.offset_for_leader_epoch =
        version!(OFFSET_FOR_LEADER_EPOCH, API_VERSION_OFFSET_FOR_LEADER_EPOCH);
    versions.describe_acls = version!(DESCRIBE_ACLS, API_VERSION_DESCRIBE_ACLS);
    versions.create_acls = version!(CREATE_ACLS, API_VERSION_CREATE_ACLS);
    versions.delete_acls = version!(DELETE_ACLS, API_VERSION_DELETE_ACLS);
    versions.describe_configs = version!(DESCRIBE_CONFIGS, API_VERSION_DESCRIBE_CONFIGS);
    versions.alter_configs = version!(ALTER_CONFIGS, API_VERSION_ALTER_CONFIGS);
    versions.incremental_alter_configs = version!(
        INCREMENTAL_ALTER_CONFIGS,
        API_VERSION_INCREMENTAL_ALTER_CONFIGS
    );
    versions.alter_replica_log_dirs =
        version!(ALTER_REPLICA_LOG_DIRS, API_VERSION_ALTER_REPLICA_LOG_DIRS);
    versions.describe_log_dirs = version!(DESCRIBE_LOG_DIRS, API_VERSION_DESCRIBE_LOG_DIRS);
    versions.create_partitions = version!(CREATE_PARTITIONS, API_VERSION_CREATE_PARTITIONS);
    versions.elect_leaders = version!(ELECT_LEADERS, API_VERSION_ELECT_LEADERS);
    versions.alter_partition_reassignments = version!(
        ALTER_PARTITION_REASSIGNMENTS,
        API_VERSION_ALTER_PARTITION_REASSIGNMENTS
    );
    versions.list_partition_reassignments = version!(
        LIST_PARTITION_REASSIGNMENTS,
        API_VERSION_LIST_PARTITION_REASSIGNMENTS
    );
    versions.describe_quorum = version!(DESCRIBE_QUORUM, API_VERSION_DESCRIBE_QUORUM);
    versions.update_features = version!(UPDATE_FEATURES, API_VERSION_UPDATE_FEATURES);
    versions.describe_cluster = version!(DESCRIBE_CLUSTER, API_VERSION_DESCRIBE_CLUSTER);
    versions.describe_producers = version!(DESCRIBE_PRODUCERS, API_VERSION_DESCRIBE_PRODUCERS);
    versions.unregister_broker = version!(UNREGISTER_BROKER, API_VERSION_UNREGISTER_BROKER);
    versions.assign_replicas_to_dirs =
        version!(ASSIGN_REPLICAS_TO_DIRS, API_VERSION_ASSIGN_REPLICAS_TO_DIRS);
    versions.add_raft_voter = version!(ADD_RAFT_VOTER, API_VERSION_ADD_RAFT_VOTER);
    versions.remove_raft_voter = version!(REMOVE_RAFT_VOTER, API_VERSION_REMOVE_RAFT_VOTER);
    versions.update_raft_voter = version!(UPDATE_RAFT_VOTER, API_VERSION_UPDATE_RAFT_VOTER);
    versions.list_config_resources =
        version!(LIST_CONFIG_RESOURCES, API_VERSION_LIST_CONFIG_RESOURCES);
    versions.describe_topic_partitions = version!(
        DESCRIBE_TOPIC_PARTITIONS,
        API_VERSION_DESCRIBE_TOPIC_PARTITIONS
    );
}

fn resolve_security_api_versions(cache: &ApiVersionCache, host: &str, versions: &mut ApiVersions) {
    macro_rules! version {
        ($api_key:ident, $default:ident) => {
            resolve_api_version(cache, host, api_key::$api_key, $default)
        };
    }

    versions.describe_delegation_token = version!(
        DESCRIBE_DELEGATION_TOKEN,
        API_VERSION_DESCRIBE_DELEGATION_TOKEN
    );
    versions.create_delegation_token =
        version!(CREATE_DELEGATION_TOKEN, API_VERSION_CREATE_DELEGATION_TOKEN);
    versions.renew_delegation_token =
        version!(RENEW_DELEGATION_TOKEN, API_VERSION_RENEW_DELEGATION_TOKEN);
    versions.expire_delegation_token =
        version!(EXPIRE_DELEGATION_TOKEN, API_VERSION_EXPIRE_DELEGATION_TOKEN);
    versions.describe_user_scram_credentials = version!(
        DESCRIBE_USER_SCRAM_CREDENTIALS,
        API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS
    );
    versions.alter_user_scram_credentials = version!(
        ALTER_USER_SCRAM_CREDENTIALS,
        API_VERSION_ALTER_USER_SCRAM_CREDENTIALS
    );
}

fn resolve_transaction_api_versions(
    cache: &ApiVersionCache,
    host: &str,
    versions: &mut ApiVersions,
) {
    macro_rules! version {
        ($api_key:ident, $default:ident) => {
            resolve_api_version(cache, host, api_key::$api_key, $default)
        };
    }

    versions.describe_transactions =
        version!(DESCRIBE_TRANSACTIONS, API_VERSION_DESCRIBE_TRANSACTIONS);
    versions.list_transactions = version!(LIST_TRANSACTIONS, API_VERSION_LIST_TRANSACTIONS);
    versions.add_offsets_to_txn = version!(ADD_OFFSETS_TO_TXN, API_VERSION_ADD_OFFSETS_TO_TXN);
    versions.txn_offset_commit = version!(TXN_OFFSET_COMMIT, API_VERSION_TXN_OFFSET_COMMIT);
}

fn resolve_group_api_versions(cache: &ApiVersionCache, host: &str, versions: &mut ApiVersions) {
    macro_rules! version {
        ($api_key:ident, $default:ident) => {
            resolve_api_version(cache, host, api_key::$api_key, $default)
        };
    }

    versions.describe_groups = version!(DESCRIBE_GROUPS, API_VERSION_DESCRIBE_GROUPS);
    versions.list_groups = version!(LIST_GROUPS, API_VERSION_LIST_GROUPS);
    versions.delete_groups = version!(DELETE_GROUPS, API_VERSION_DELETE_GROUPS);
    versions.consumer_group_heartbeat = version!(
        CONSUMER_GROUP_HEARTBEAT,
        API_VERSION_CONSUMER_GROUP_HEARTBEAT
    );
    versions.offset_delete = version!(OFFSET_DELETE, API_VERSION_OFFSET_DELETE);
    versions.describe_client_quotas =
        version!(DESCRIBE_CLIENT_QUOTAS, API_VERSION_DESCRIBE_CLIENT_QUOTAS);
    versions.alter_client_quotas = version!(ALTER_CLIENT_QUOTAS, API_VERSION_ALTER_CLIENT_QUOTAS);
    versions.consumer_group_describe =
        version!(CONSUMER_GROUP_DESCRIBE, API_VERSION_CONSUMER_GROUP_DESCRIBE);
    versions.share_group_describe =
        version!(SHARE_GROUP_DESCRIBE, API_VERSION_SHARE_GROUP_DESCRIBE);
    versions.share_group_heartbeat =
        version!(SHARE_GROUP_HEARTBEAT, API_VERSION_SHARE_GROUP_HEARTBEAT);
    versions.share_fetch = version!(SHARE_FETCH, API_VERSION_SHARE_FETCH);
    versions.share_acknowledge = version!(SHARE_ACKNOWLEDGE, API_VERSION_SHARE_ACKNOWLEDGE);
    versions.describe_share_group_offsets = version!(
        DESCRIBE_SHARE_GROUP_OFFSETS,
        API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS
    );
    versions.alter_share_group_offsets = version!(
        ALTER_SHARE_GROUP_OFFSETS,
        API_VERSION_ALTER_SHARE_GROUP_OFFSETS
    );
    versions.delete_share_group_offsets = version!(
        DELETE_SHARE_GROUP_OFFSETS,
        API_VERSION_DELETE_SHARE_GROUP_OFFSETS
    );
}

fn resolve_internal_api_versions(cache: &ApiVersionCache, host: &str, versions: &mut ApiVersions) {
    macro_rules! version {
        ($api_key:ident, $default:ident) => {
            resolve_api_version(cache, host, api_key::$api_key, $default)
        };
    }

    versions.write_txn_markers = version!(WRITE_TXN_MARKERS, API_VERSION_WRITE_TXN_MARKERS);
    versions.vote = version!(VOTE, API_VERSION_VOTE);
    versions.begin_quorum_epoch = version!(BEGIN_QUORUM_EPOCH, API_VERSION_BEGIN_QUORUM_EPOCH);
    versions.end_quorum_epoch = version!(END_QUORUM_EPOCH, API_VERSION_END_QUORUM_EPOCH);
    versions.alter_partition = version!(ALTER_PARTITION, API_VERSION_ALTER_PARTITION);
    versions.envelope = version!(ENVELOPE, API_VERSION_ENVELOPE);
    versions.fetch_snapshot = version!(FETCH_SNAPSHOT, API_VERSION_FETCH_SNAPSHOT);
    versions.broker_registration = version!(BROKER_REGISTRATION, API_VERSION_BROKER_REGISTRATION);
    versions.broker_heartbeat = version!(BROKER_HEARTBEAT, API_VERSION_BROKER_HEARTBEAT);
    versions.allocate_producer_ids =
        version!(ALLOCATE_PRODUCER_IDS, API_VERSION_ALLOCATE_PRODUCER_IDS);
    versions.controller_registration =
        version!(CONTROLLER_REGISTRATION, API_VERSION_CONTROLLER_REGISTRATION);
    versions.initialize_share_group_state = version!(
        INITIALIZE_SHARE_GROUP_STATE,
        API_VERSION_INITIALIZE_SHARE_GROUP_STATE
    );
    versions.read_share_group_state =
        version!(READ_SHARE_GROUP_STATE, API_VERSION_READ_SHARE_GROUP_STATE);
    versions.write_share_group_state =
        version!(WRITE_SHARE_GROUP_STATE, API_VERSION_WRITE_SHARE_GROUP_STATE);
    versions.delete_share_group_state = version!(
        DELETE_SHARE_GROUP_STATE,
        API_VERSION_DELETE_SHARE_GROUP_STATE
    );
    versions.read_share_group_state_summary = version!(
        READ_SHARE_GROUP_STATE_SUMMARY,
        API_VERSION_READ_SHARE_GROUP_STATE_SUMMARY
    );
}

/// Resolved API versions for all supported Kafka APIs.
#[derive(Debug, Copy, Clone)]
#[allow(dead_code)]
pub struct ApiVersions {
    pub produce: i16,
    pub fetch: i16,
    pub metadata: i16,
    pub list_offsets: i16,
    pub find_coordinator: i16,
    pub offset_commit: i16,
    pub offset_fetch: i16,
    pub get_telemetry_subscriptions: i16,
    pub push_telemetry: i16,
    pub delete_records: i16,
    pub offset_for_leader_epoch: i16,
    pub describe_groups: i16,
    pub list_groups: i16,
    pub describe_acls: i16,
    pub create_acls: i16,
    pub delete_acls: i16,
    pub describe_configs: i16,
    pub alter_configs: i16,
    pub incremental_alter_configs: i16,
    pub alter_replica_log_dirs: i16,
    pub describe_log_dirs: i16,
    pub create_partitions: i16,
    pub describe_delegation_token: i16,
    pub create_delegation_token: i16,
    pub renew_delegation_token: i16,
    pub expire_delegation_token: i16,
    pub delete_groups: i16,
    pub consumer_group_heartbeat: i16,
    pub elect_leaders: i16,
    pub alter_partition_reassignments: i16,
    pub list_partition_reassignments: i16,
    pub offset_delete: i16,
    pub describe_client_quotas: i16,
    pub alter_client_quotas: i16,
    pub describe_user_scram_credentials: i16,
    pub alter_user_scram_credentials: i16,
    pub describe_quorum: i16,
    pub update_features: i16,
    pub describe_cluster: i16,
    pub describe_producers: i16,
    pub unregister_broker: i16,
    pub assign_replicas_to_dirs: i16,
    pub add_raft_voter: i16,
    pub remove_raft_voter: i16,
    pub update_raft_voter: i16,
    pub describe_transactions: i16,
    pub list_transactions: i16,
    pub add_offsets_to_txn: i16,
    pub txn_offset_commit: i16,
    pub write_txn_markers: i16,
    pub vote: i16,
    pub begin_quorum_epoch: i16,
    pub end_quorum_epoch: i16,
    pub alter_partition: i16,
    pub envelope: i16,
    pub fetch_snapshot: i16,
    pub broker_registration: i16,
    pub broker_heartbeat: i16,
    pub allocate_producer_ids: i16,
    pub controller_registration: i16,
    pub consumer_group_describe: i16,
    pub list_config_resources: i16,
    pub describe_topic_partitions: i16,
    pub share_group_describe: i16,
    pub share_group_heartbeat: i16,
    pub share_fetch: i16,
    pub share_acknowledge: i16,
    pub initialize_share_group_state: i16,
    pub read_share_group_state: i16,
    pub write_share_group_state: i16,
    pub delete_share_group_state: i16,
    pub read_share_group_state_summary: i16,
    pub describe_share_group_offsets: i16,
    pub alter_share_group_offsets: i16,
    pub delete_share_group_offsets: i16,
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
            get_telemetry_subscriptions: API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS,
            push_telemetry: API_VERSION_PUSH_TELEMETRY,
            delete_records: API_VERSION_DELETE_RECORDS,
            offset_for_leader_epoch: API_VERSION_OFFSET_FOR_LEADER_EPOCH,
            describe_groups: API_VERSION_DESCRIBE_GROUPS,
            list_groups: API_VERSION_LIST_GROUPS,
            describe_acls: API_VERSION_DESCRIBE_ACLS,
            create_acls: API_VERSION_CREATE_ACLS,
            delete_acls: API_VERSION_DELETE_ACLS,
            describe_configs: API_VERSION_DESCRIBE_CONFIGS,
            alter_configs: API_VERSION_ALTER_CONFIGS,
            incremental_alter_configs: API_VERSION_INCREMENTAL_ALTER_CONFIGS,
            alter_replica_log_dirs: API_VERSION_ALTER_REPLICA_LOG_DIRS,
            describe_log_dirs: API_VERSION_DESCRIBE_LOG_DIRS,
            create_partitions: API_VERSION_CREATE_PARTITIONS,
            describe_delegation_token: API_VERSION_DESCRIBE_DELEGATION_TOKEN,
            create_delegation_token: API_VERSION_CREATE_DELEGATION_TOKEN,
            renew_delegation_token: API_VERSION_RENEW_DELEGATION_TOKEN,
            expire_delegation_token: API_VERSION_EXPIRE_DELEGATION_TOKEN,
            delete_groups: API_VERSION_DELETE_GROUPS,
            consumer_group_heartbeat: API_VERSION_CONSUMER_GROUP_HEARTBEAT,
            elect_leaders: API_VERSION_ELECT_LEADERS,
            alter_partition_reassignments: API_VERSION_ALTER_PARTITION_REASSIGNMENTS,
            list_partition_reassignments: API_VERSION_LIST_PARTITION_REASSIGNMENTS,
            offset_delete: API_VERSION_OFFSET_DELETE,
            describe_client_quotas: API_VERSION_DESCRIBE_CLIENT_QUOTAS,
            alter_client_quotas: API_VERSION_ALTER_CLIENT_QUOTAS,
            describe_user_scram_credentials: API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
            alter_user_scram_credentials: API_VERSION_ALTER_USER_SCRAM_CREDENTIALS,
            describe_quorum: API_VERSION_DESCRIBE_QUORUM,
            update_features: API_VERSION_UPDATE_FEATURES,
            describe_cluster: API_VERSION_DESCRIBE_CLUSTER,
            describe_producers: API_VERSION_DESCRIBE_PRODUCERS,
            unregister_broker: API_VERSION_UNREGISTER_BROKER,
            assign_replicas_to_dirs: API_VERSION_ASSIGN_REPLICAS_TO_DIRS,
            add_raft_voter: API_VERSION_ADD_RAFT_VOTER,
            remove_raft_voter: API_VERSION_REMOVE_RAFT_VOTER,
            update_raft_voter: API_VERSION_UPDATE_RAFT_VOTER,
            describe_transactions: API_VERSION_DESCRIBE_TRANSACTIONS,
            list_transactions: API_VERSION_LIST_TRANSACTIONS,
            add_offsets_to_txn: API_VERSION_ADD_OFFSETS_TO_TXN,
            txn_offset_commit: API_VERSION_TXN_OFFSET_COMMIT,
            write_txn_markers: API_VERSION_WRITE_TXN_MARKERS,
            vote: API_VERSION_VOTE,
            begin_quorum_epoch: API_VERSION_BEGIN_QUORUM_EPOCH,
            end_quorum_epoch: API_VERSION_END_QUORUM_EPOCH,
            alter_partition: API_VERSION_ALTER_PARTITION,
            envelope: API_VERSION_ENVELOPE,
            fetch_snapshot: API_VERSION_FETCH_SNAPSHOT,
            broker_registration: API_VERSION_BROKER_REGISTRATION,
            broker_heartbeat: API_VERSION_BROKER_HEARTBEAT,
            allocate_producer_ids: API_VERSION_ALLOCATE_PRODUCER_IDS,
            controller_registration: API_VERSION_CONTROLLER_REGISTRATION,
            consumer_group_describe: API_VERSION_CONSUMER_GROUP_DESCRIBE,
            list_config_resources: API_VERSION_LIST_CONFIG_RESOURCES,
            describe_topic_partitions: API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
            share_group_describe: API_VERSION_SHARE_GROUP_DESCRIBE,
            share_group_heartbeat: API_VERSION_SHARE_GROUP_HEARTBEAT,
            share_fetch: API_VERSION_SHARE_FETCH,
            share_acknowledge: API_VERSION_SHARE_ACKNOWLEDGE,
            initialize_share_group_state: API_VERSION_INITIALIZE_SHARE_GROUP_STATE,
            read_share_group_state: API_VERSION_READ_SHARE_GROUP_STATE,
            write_share_group_state: API_VERSION_WRITE_SHARE_GROUP_STATE,
            delete_share_group_state: API_VERSION_DELETE_SHARE_GROUP_STATE,
            read_share_group_state_summary: API_VERSION_READ_SHARE_GROUP_STATE_SUMMARY,
            describe_share_group_offsets: API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
            alter_share_group_offsets: API_VERSION_ALTER_SHARE_GROUP_OFFSETS,
            delete_share_group_offsets: API_VERSION_DELETE_SHARE_GROUP_OFFSETS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey as KpApiKey;

    const INTERNAL_GENERATED_API_METADATA: &[(i16, KpApiKey, i16, i16, i16)] = &[
        (
            api_key::WRITE_TXN_MARKERS,
            KpApiKey::WriteTxnMarkers,
            API_VERSION_WRITE_TXN_MARKERS,
            1,
            1,
        ),
        (api_key::VOTE, KpApiKey::Vote, API_VERSION_VOTE, 0, 2),
        (
            api_key::BEGIN_QUORUM_EPOCH,
            KpApiKey::BeginQuorumEpoch,
            API_VERSION_BEGIN_QUORUM_EPOCH,
            0,
            1,
        ),
        (
            api_key::END_QUORUM_EPOCH,
            KpApiKey::EndQuorumEpoch,
            API_VERSION_END_QUORUM_EPOCH,
            0,
            1,
        ),
        (
            api_key::ALTER_PARTITION,
            KpApiKey::AlterPartition,
            API_VERSION_ALTER_PARTITION,
            2,
            3,
        ),
        (
            api_key::ENVELOPE,
            KpApiKey::Envelope,
            API_VERSION_ENVELOPE,
            0,
            0,
        ),
        (
            api_key::FETCH_SNAPSHOT,
            KpApiKey::FetchSnapshot,
            API_VERSION_FETCH_SNAPSHOT,
            0,
            1,
        ),
        (
            api_key::BROKER_REGISTRATION,
            KpApiKey::BrokerRegistration,
            API_VERSION_BROKER_REGISTRATION,
            0,
            4,
        ),
        (
            api_key::BROKER_HEARTBEAT,
            KpApiKey::BrokerHeartbeat,
            API_VERSION_BROKER_HEARTBEAT,
            0,
            1,
        ),
        (
            api_key::ALLOCATE_PRODUCER_IDS,
            KpApiKey::AllocateProducerIds,
            API_VERSION_ALLOCATE_PRODUCER_IDS,
            0,
            0,
        ),
        (
            api_key::CONTROLLER_REGISTRATION,
            KpApiKey::ControllerRegistration,
            API_VERSION_CONTROLLER_REGISTRATION,
            0,
            0,
        ),
        (
            api_key::INITIALIZE_SHARE_GROUP_STATE,
            KpApiKey::InitializeShareGroupState,
            API_VERSION_INITIALIZE_SHARE_GROUP_STATE,
            0,
            0,
        ),
        (
            api_key::READ_SHARE_GROUP_STATE,
            KpApiKey::ReadShareGroupState,
            API_VERSION_READ_SHARE_GROUP_STATE,
            0,
            0,
        ),
        (
            api_key::WRITE_SHARE_GROUP_STATE,
            KpApiKey::WriteShareGroupState,
            API_VERSION_WRITE_SHARE_GROUP_STATE,
            0,
            0,
        ),
        (
            api_key::DELETE_SHARE_GROUP_STATE,
            KpApiKey::DeleteShareGroupState,
            API_VERSION_DELETE_SHARE_GROUP_STATE,
            0,
            0,
        ),
        (
            api_key::READ_SHARE_GROUP_STATE_SUMMARY,
            KpApiKey::ReadShareGroupStateSummary,
            API_VERSION_READ_SHARE_GROUP_STATE_SUMMARY,
            0,
            0,
        ),
    ];

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
    fn convert_api_versions_response_preserves_api_ranges() {
        use kafka_protocol::messages::api_versions_response::ApiVersion;
        let response = kafka_protocol::messages::ApiVersionsResponse::default()
            .with_error_code(0)
            .with_throttle_time_ms(14)
            .with_api_keys(vec![
                ApiVersion::default()
                    .with_api_key(api_key::DESCRIBE_CONFIGS)
                    .with_min_version(1)
                    .with_max_version(4),
            ]);

        let converted = convert_api_versions_response(response);

        assert_eq!(
            converted,
            ApiVersionsResponseData {
                error_code: 0,
                throttle_time_ms: 14,
                api_keys: vec![BrokerApiVersion {
                    api_key: api_key::DESCRIBE_CONFIGS,
                    min_version: 1,
                    max_version: 4,
                }],
            }
        );
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
        let bv = BrokerApiVersions::from_response(
            kafka_protocol::messages::ApiVersionsResponse::default(),
        );
        cache.insert("broker1:9092".to_string(), bv);
        assert!(cache.contains("broker1:9092"));
        assert!(!cache.contains("broker2:9092"));
    }

    #[test]
    fn api_version_cache_invalidate() {
        let mut cache = ApiVersionCache::new();
        let bv = BrokerApiVersions::from_response(
            kafka_protocol::messages::ApiVersionsResponse::default(),
        );
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
    fn api_versions_default_has_expected_core_fields() {
        let v = ApiVersions::default();
        assert_eq!(v.produce, API_VERSION_PRODUCE);
        assert_eq!(v.fetch, API_VERSION_FETCH);
        assert_eq!(v.metadata, API_VERSION_METADATA);
        assert_eq!(v.list_offsets, API_VERSION_LIST_OFFSETS);
        assert_eq!(v.find_coordinator, API_VERSION_FIND_COORDINATOR);
        assert_eq!(v.offset_commit, API_VERSION_OFFSET_COMMIT);
        assert_eq!(v.offset_fetch, API_VERSION_OFFSET_FETCH);
        assert_eq!(
            v.get_telemetry_subscriptions,
            API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS
        );
        assert_eq!(v.push_telemetry, API_VERSION_PUSH_TELEMETRY);
        assert_eq!(v.delete_records, API_VERSION_DELETE_RECORDS);
        assert_eq!(
            v.offset_for_leader_epoch,
            API_VERSION_OFFSET_FOR_LEADER_EPOCH
        );
        assert_eq!(
            v.consumer_group_heartbeat,
            API_VERSION_CONSUMER_GROUP_HEARTBEAT
        );
    }

    #[test]
    fn api_versions_default_has_expected_admin_fields() {
        let v = ApiVersions::default();
        assert_eq!(v.describe_groups, API_VERSION_DESCRIBE_GROUPS);
        assert_eq!(v.list_groups, API_VERSION_LIST_GROUPS);
        assert_eq!(v.describe_acls, API_VERSION_DESCRIBE_ACLS);
        assert_eq!(v.create_acls, API_VERSION_CREATE_ACLS);
        assert_eq!(v.delete_acls, API_VERSION_DELETE_ACLS);
        assert_eq!(v.describe_configs, API_VERSION_DESCRIBE_CONFIGS);
        assert_eq!(v.alter_configs, API_VERSION_ALTER_CONFIGS);
        assert_eq!(
            v.incremental_alter_configs,
            API_VERSION_INCREMENTAL_ALTER_CONFIGS
        );
        assert_eq!(v.alter_replica_log_dirs, API_VERSION_ALTER_REPLICA_LOG_DIRS);
        assert_eq!(v.describe_log_dirs, API_VERSION_DESCRIBE_LOG_DIRS);
        assert_eq!(v.create_partitions, API_VERSION_CREATE_PARTITIONS);
        assert_eq!(
            v.describe_delegation_token,
            API_VERSION_DESCRIBE_DELEGATION_TOKEN
        );
        assert_eq!(
            v.create_delegation_token,
            API_VERSION_CREATE_DELEGATION_TOKEN
        );
        assert_eq!(v.renew_delegation_token, API_VERSION_RENEW_DELEGATION_TOKEN);
        assert_eq!(
            v.expire_delegation_token,
            API_VERSION_EXPIRE_DELEGATION_TOKEN
        );
        assert_eq!(v.delete_groups, API_VERSION_DELETE_GROUPS);
        assert_eq!(v.elect_leaders, API_VERSION_ELECT_LEADERS);
        assert_eq!(
            v.alter_partition_reassignments,
            API_VERSION_ALTER_PARTITION_REASSIGNMENTS
        );
        assert_eq!(
            v.list_partition_reassignments,
            API_VERSION_LIST_PARTITION_REASSIGNMENTS
        );
        assert_eq!(v.offset_delete, API_VERSION_OFFSET_DELETE);
        assert_eq!(v.describe_client_quotas, API_VERSION_DESCRIBE_CLIENT_QUOTAS);
        assert_eq!(v.alter_client_quotas, API_VERSION_ALTER_CLIENT_QUOTAS);
        assert_eq!(
            v.describe_user_scram_credentials,
            API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS
        );
        assert_eq!(
            v.alter_user_scram_credentials,
            API_VERSION_ALTER_USER_SCRAM_CREDENTIALS
        );
        assert_eq!(v.describe_quorum, API_VERSION_DESCRIBE_QUORUM);
        assert_eq!(v.update_features, API_VERSION_UPDATE_FEATURES);
        assert_eq!(v.describe_cluster, API_VERSION_DESCRIBE_CLUSTER);
        assert_eq!(v.describe_producers, API_VERSION_DESCRIBE_PRODUCERS);
        assert_eq!(v.unregister_broker, API_VERSION_UNREGISTER_BROKER);
        assert_eq!(
            v.assign_replicas_to_dirs,
            API_VERSION_ASSIGN_REPLICAS_TO_DIRS
        );
        assert_eq!(v.add_raft_voter, API_VERSION_ADD_RAFT_VOTER);
        assert_eq!(v.remove_raft_voter, API_VERSION_REMOVE_RAFT_VOTER);
        assert_eq!(v.update_raft_voter, API_VERSION_UPDATE_RAFT_VOTER);
        assert_eq!(v.describe_transactions, API_VERSION_DESCRIBE_TRANSACTIONS);
        assert_eq!(v.list_transactions, API_VERSION_LIST_TRANSACTIONS);
        assert_eq!(v.add_offsets_to_txn, API_VERSION_ADD_OFFSETS_TO_TXN);
        assert_eq!(v.txn_offset_commit, API_VERSION_TXN_OFFSET_COMMIT);
        assert_eq!(
            v.consumer_group_describe,
            API_VERSION_CONSUMER_GROUP_DESCRIBE
        );
        assert_eq!(v.list_config_resources, API_VERSION_LIST_CONFIG_RESOURCES);
        assert_eq!(
            v.describe_topic_partitions,
            API_VERSION_DESCRIBE_TOPIC_PARTITIONS
        );
        assert_eq!(v.share_group_describe, API_VERSION_SHARE_GROUP_DESCRIBE);
        assert_eq!(v.share_group_heartbeat, API_VERSION_SHARE_GROUP_HEARTBEAT);
        assert_eq!(v.share_fetch, API_VERSION_SHARE_FETCH);
        assert_eq!(v.share_acknowledge, API_VERSION_SHARE_ACKNOWLEDGE);
        assert_eq!(
            v.describe_share_group_offsets,
            API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS
        );
        assert_eq!(
            v.alter_share_group_offsets,
            API_VERSION_ALTER_SHARE_GROUP_OFFSETS
        );
        assert_eq!(
            v.delete_share_group_offsets,
            API_VERSION_DELETE_SHARE_GROUP_OFFSETS
        );
    }

    #[test]
    fn api_versions_default_has_expected_internal_fields() {
        let v = ApiVersions::default();
        assert_eq!(v.write_txn_markers, API_VERSION_WRITE_TXN_MARKERS);
        assert_eq!(v.vote, API_VERSION_VOTE);
        assert_eq!(v.begin_quorum_epoch, API_VERSION_BEGIN_QUORUM_EPOCH);
        assert_eq!(v.end_quorum_epoch, API_VERSION_END_QUORUM_EPOCH);
        assert_eq!(v.alter_partition, API_VERSION_ALTER_PARTITION);
        assert_eq!(v.envelope, API_VERSION_ENVELOPE);
        assert_eq!(v.fetch_snapshot, API_VERSION_FETCH_SNAPSHOT);
        assert_eq!(v.broker_registration, API_VERSION_BROKER_REGISTRATION);
        assert_eq!(v.broker_heartbeat, API_VERSION_BROKER_HEARTBEAT);
        assert_eq!(v.allocate_producer_ids, API_VERSION_ALLOCATE_PRODUCER_IDS);
        assert_eq!(
            v.controller_registration,
            API_VERSION_CONTROLLER_REGISTRATION
        );
        assert_eq!(
            v.initialize_share_group_state,
            API_VERSION_INITIALIZE_SHARE_GROUP_STATE
        );
        assert_eq!(v.read_share_group_state, API_VERSION_READ_SHARE_GROUP_STATE);
        assert_eq!(
            v.write_share_group_state,
            API_VERSION_WRITE_SHARE_GROUP_STATE
        );
        assert_eq!(
            v.delete_share_group_state,
            API_VERSION_DELETE_SHARE_GROUP_STATE
        );
        assert_eq!(
            v.read_share_group_state_summary,
            API_VERSION_READ_SHARE_GROUP_STATE_SUMMARY
        );
    }

    #[test]
    fn internal_generated_api_metadata_matches_kafka_protocol_018() {
        for &(api_key, expected_api_key, fallback, min, max) in INTERNAL_GENERATED_API_METADATA {
            let parsed = KpApiKey::try_from(api_key).expect("known kafka protocol API key");
            let range = parsed.valid_versions();

            assert_eq!(parsed, expected_api_key);
            assert_eq!((range.min, range.max), (min, max));
            assert_eq!(parsed.request_header_version(fallback), 2);
            assert_eq!(parsed.response_header_version(fallback), 1);
        }
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
        assert_eq!(v.delete_records, d.delete_records);
        assert_eq!(v.offset_for_leader_epoch, d.offset_for_leader_epoch);
        assert_eq!(v.describe_groups, d.describe_groups);
        assert_eq!(v.list_groups, d.list_groups);
        assert_eq!(v.describe_acls, d.describe_acls);
        assert_eq!(v.create_acls, d.create_acls);
        assert_eq!(v.delete_acls, d.delete_acls);
        assert_eq!(v.describe_configs, d.describe_configs);
        assert_eq!(v.alter_configs, d.alter_configs);
        assert_eq!(v.incremental_alter_configs, d.incremental_alter_configs);
        assert_eq!(v.alter_replica_log_dirs, d.alter_replica_log_dirs);
        assert_eq!(v.describe_log_dirs, d.describe_log_dirs);
        assert_eq!(v.create_partitions, d.create_partitions);
        assert_eq!(v.describe_delegation_token, d.describe_delegation_token);
        assert_eq!(v.create_delegation_token, d.create_delegation_token);
        assert_eq!(v.renew_delegation_token, d.renew_delegation_token);
        assert_eq!(v.expire_delegation_token, d.expire_delegation_token);
        assert_eq!(v.delete_groups, d.delete_groups);
        assert_eq!(v.elect_leaders, d.elect_leaders);
        assert_eq!(
            v.alter_partition_reassignments,
            d.alter_partition_reassignments
        );
        assert_eq!(
            v.list_partition_reassignments,
            d.list_partition_reassignments
        );
        assert_eq!(v.offset_delete, d.offset_delete);
        assert_eq!(v.describe_client_quotas, d.describe_client_quotas);
        assert_eq!(v.alter_client_quotas, d.alter_client_quotas);
        assert_eq!(
            v.describe_user_scram_credentials,
            d.describe_user_scram_credentials
        );
        assert_eq!(
            v.alter_user_scram_credentials,
            d.alter_user_scram_credentials
        );
        assert_eq!(v.describe_quorum, d.describe_quorum);
        assert_eq!(v.update_features, d.update_features);
        assert_eq!(v.describe_cluster, d.describe_cluster);
        assert_eq!(v.describe_producers, d.describe_producers);
        assert_eq!(v.unregister_broker, d.unregister_broker);
        assert_eq!(v.describe_transactions, d.describe_transactions);
        assert_eq!(v.list_transactions, d.list_transactions);
        assert_eq!(v.add_offsets_to_txn, d.add_offsets_to_txn);
        assert_eq!(v.txn_offset_commit, d.txn_offset_commit);
        assert_eq!(v.write_txn_markers, d.write_txn_markers);
        assert_eq!(v.vote, d.vote);
        assert_eq!(v.begin_quorum_epoch, d.begin_quorum_epoch);
        assert_eq!(v.end_quorum_epoch, d.end_quorum_epoch);
        assert_eq!(v.alter_partition, d.alter_partition);
        assert_eq!(v.envelope, d.envelope);
        assert_eq!(v.fetch_snapshot, d.fetch_snapshot);
        assert_eq!(v.broker_registration, d.broker_registration);
        assert_eq!(v.broker_heartbeat, d.broker_heartbeat);
        assert_eq!(v.allocate_producer_ids, d.allocate_producer_ids);
        assert_eq!(v.controller_registration, d.controller_registration);
        assert_eq!(v.consumer_group_describe, d.consumer_group_describe);
        assert_eq!(v.list_config_resources, d.list_config_resources);
        assert_eq!(v.describe_topic_partitions, d.describe_topic_partitions);
        assert_eq!(v.share_group_describe, d.share_group_describe);
        assert_eq!(
            v.initialize_share_group_state,
            d.initialize_share_group_state
        );
        assert_eq!(v.read_share_group_state, d.read_share_group_state);
        assert_eq!(v.write_share_group_state, d.write_share_group_state);
        assert_eq!(v.delete_share_group_state, d.delete_share_group_state);
        assert_eq!(
            v.read_share_group_state_summary,
            d.read_share_group_state_summary
        );
        assert_eq!(
            v.describe_share_group_offsets,
            d.describe_share_group_offsets
        );
        assert_eq!(v.alter_share_group_offsets, d.alter_share_group_offsets);
        assert_eq!(v.delete_share_group_offsets, d.delete_share_group_offsets);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn fallback_version_known_apis() {
        for (api_key, expected_version) in [
            (api_key::PRODUCE, API_VERSION_PRODUCE),
            (api_key::FETCH, API_VERSION_FETCH),
            (api_key::METADATA, API_VERSION_METADATA),
            (api_key::LIST_OFFSETS, API_VERSION_LIST_OFFSETS),
            (api_key::FIND_COORDINATOR, API_VERSION_FIND_COORDINATOR),
            (api_key::OFFSET_COMMIT, API_VERSION_OFFSET_COMMIT),
            (api_key::OFFSET_FETCH, API_VERSION_OFFSET_FETCH),
            (api_key::DELETE_RECORDS, API_VERSION_DELETE_RECORDS),
            (
                api_key::OFFSET_FOR_LEADER_EPOCH,
                API_VERSION_OFFSET_FOR_LEADER_EPOCH,
            ),
            (api_key::DESCRIBE_GROUPS, API_VERSION_DESCRIBE_GROUPS),
            (api_key::LIST_GROUPS, API_VERSION_LIST_GROUPS),
            (api_key::DESCRIBE_ACLS, API_VERSION_DESCRIBE_ACLS),
            (api_key::CREATE_ACLS, API_VERSION_CREATE_ACLS),
            (api_key::DELETE_ACLS, API_VERSION_DELETE_ACLS),
            (api_key::DESCRIBE_CONFIGS, API_VERSION_DESCRIBE_CONFIGS),
            (api_key::ALTER_CONFIGS, API_VERSION_ALTER_CONFIGS),
            (
                api_key::INCREMENTAL_ALTER_CONFIGS,
                API_VERSION_INCREMENTAL_ALTER_CONFIGS,
            ),
            (
                api_key::ALTER_REPLICA_LOG_DIRS,
                API_VERSION_ALTER_REPLICA_LOG_DIRS,
            ),
            (api_key::DESCRIBE_LOG_DIRS, API_VERSION_DESCRIBE_LOG_DIRS),
            (api_key::CREATE_PARTITIONS, API_VERSION_CREATE_PARTITIONS),
            (
                api_key::DESCRIBE_DELEGATION_TOKEN,
                API_VERSION_DESCRIBE_DELEGATION_TOKEN,
            ),
            (
                api_key::CREATE_DELEGATION_TOKEN,
                API_VERSION_CREATE_DELEGATION_TOKEN,
            ),
            (
                api_key::RENEW_DELEGATION_TOKEN,
                API_VERSION_RENEW_DELEGATION_TOKEN,
            ),
            (
                api_key::EXPIRE_DELEGATION_TOKEN,
                API_VERSION_EXPIRE_DELEGATION_TOKEN,
            ),
            (api_key::DELETE_GROUPS, API_VERSION_DELETE_GROUPS),
            (api_key::ELECT_LEADERS, API_VERSION_ELECT_LEADERS),
            (
                api_key::ALTER_PARTITION_REASSIGNMENTS,
                API_VERSION_ALTER_PARTITION_REASSIGNMENTS,
            ),
            (
                api_key::LIST_PARTITION_REASSIGNMENTS,
                API_VERSION_LIST_PARTITION_REASSIGNMENTS,
            ),
            (api_key::OFFSET_DELETE, API_VERSION_OFFSET_DELETE),
            (
                api_key::DESCRIBE_CLIENT_QUOTAS,
                API_VERSION_DESCRIBE_CLIENT_QUOTAS,
            ),
            (
                api_key::ALTER_CLIENT_QUOTAS,
                API_VERSION_ALTER_CLIENT_QUOTAS,
            ),
            (
                api_key::DESCRIBE_USER_SCRAM_CREDENTIALS,
                API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
            ),
            (
                api_key::ALTER_USER_SCRAM_CREDENTIALS,
                API_VERSION_ALTER_USER_SCRAM_CREDENTIALS,
            ),
            (api_key::DESCRIBE_QUORUM, API_VERSION_DESCRIBE_QUORUM),
            (api_key::UPDATE_FEATURES, API_VERSION_UPDATE_FEATURES),
            (api_key::DESCRIBE_CLUSTER, API_VERSION_DESCRIBE_CLUSTER),
            (api_key::DESCRIBE_PRODUCERS, API_VERSION_DESCRIBE_PRODUCERS),
            (api_key::UNREGISTER_BROKER, API_VERSION_UNREGISTER_BROKER),
            (
                api_key::DESCRIBE_TRANSACTIONS,
                API_VERSION_DESCRIBE_TRANSACTIONS,
            ),
            (api_key::LIST_TRANSACTIONS, API_VERSION_LIST_TRANSACTIONS),
            (api_key::ADD_OFFSETS_TO_TXN, API_VERSION_ADD_OFFSETS_TO_TXN),
            (api_key::TXN_OFFSET_COMMIT, API_VERSION_TXN_OFFSET_COMMIT),
            (api_key::WRITE_TXN_MARKERS, API_VERSION_WRITE_TXN_MARKERS),
            (api_key::VOTE, API_VERSION_VOTE),
            (api_key::BEGIN_QUORUM_EPOCH, API_VERSION_BEGIN_QUORUM_EPOCH),
            (api_key::END_QUORUM_EPOCH, API_VERSION_END_QUORUM_EPOCH),
            (api_key::ALTER_PARTITION, API_VERSION_ALTER_PARTITION),
            (api_key::ENVELOPE, API_VERSION_ENVELOPE),
            (api_key::FETCH_SNAPSHOT, API_VERSION_FETCH_SNAPSHOT),
            (
                api_key::BROKER_REGISTRATION,
                API_VERSION_BROKER_REGISTRATION,
            ),
            (api_key::BROKER_HEARTBEAT, API_VERSION_BROKER_HEARTBEAT),
            (
                api_key::ALLOCATE_PRODUCER_IDS,
                API_VERSION_ALLOCATE_PRODUCER_IDS,
            ),
            (
                api_key::CONTROLLER_REGISTRATION,
                API_VERSION_CONTROLLER_REGISTRATION,
            ),
            (
                api_key::CONSUMER_GROUP_DESCRIBE,
                API_VERSION_CONSUMER_GROUP_DESCRIBE,
            ),
            (
                api_key::LIST_CONFIG_RESOURCES,
                API_VERSION_LIST_CONFIG_RESOURCES,
            ),
            (
                api_key::DESCRIBE_TOPIC_PARTITIONS,
                API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
            ),
            (
                api_key::SHARE_GROUP_DESCRIBE,
                API_VERSION_SHARE_GROUP_DESCRIBE,
            ),
            (
                api_key::INITIALIZE_SHARE_GROUP_STATE,
                API_VERSION_INITIALIZE_SHARE_GROUP_STATE,
            ),
            (
                api_key::READ_SHARE_GROUP_STATE,
                API_VERSION_READ_SHARE_GROUP_STATE,
            ),
            (
                api_key::WRITE_SHARE_GROUP_STATE,
                API_VERSION_WRITE_SHARE_GROUP_STATE,
            ),
            (
                api_key::DELETE_SHARE_GROUP_STATE,
                API_VERSION_DELETE_SHARE_GROUP_STATE,
            ),
            (
                api_key::READ_SHARE_GROUP_STATE_SUMMARY,
                API_VERSION_READ_SHARE_GROUP_STATE_SUMMARY,
            ),
            (
                api_key::DESCRIBE_SHARE_GROUP_OFFSETS,
                API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
            ),
            (
                api_key::ALTER_SHARE_GROUP_OFFSETS,
                API_VERSION_ALTER_SHARE_GROUP_OFFSETS,
            ),
            (
                api_key::DELETE_SHARE_GROUP_OFFSETS,
                API_VERSION_DELETE_SHARE_GROUP_OFFSETS,
            ),
        ] {
            assert_eq!(ApiVersionCache::fallback_version(api_key), expected_version);
        }
    }

    #[test]
    fn fallback_version_unknown_api() {
        assert_eq!(ApiVersionCache::fallback_version(99), 0);
        assert_eq!(ApiVersionCache::fallback_version(-1), 0);
    }

    #[test]
    fn get_or_fallback_empty_cache_returns_fallback() {
        let cache = ApiVersionCache::new();
        assert_eq!(
            cache.get_or_fallback("unknown:9092", api_key::PRODUCE),
            API_VERSION_PRODUCE
        );
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

        // Known API keys are negotiated against broker ranges; unknown keys use fallback.
        assert_eq!(cache.get_or_fallback("broker1:9092", api_key::PRODUCE), 8);
        assert_eq!(
            cache.get_or_fallback("broker1:9092", api_key::FETCH),
            API_VERSION_FETCH
        );
    }
}
