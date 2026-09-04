//! Resolved API versions for all supported Kafka APIs.
//!
//! Contains the [`ApiVersions`] struct, [`resolve_all_api_versions`], and
//! the per-category `resolve_*` helpers that negotiate versions against a
//! broker's cached capabilities.

use super::ApiVersionCache;
use super::api_keys as api_key;
use crate::protocol::create_topics::API_VERSION_CREATE_TOPICS;
use crate::protocol::delete_topics::API_VERSION_DELETE_TOPICS;
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
    versions.create_topics = version!(CREATE_TOPICS, API_VERSION_CREATE_TOPICS);
    versions.delete_topics = version!(DELETE_TOPICS, API_VERSION_DELETE_TOPICS);
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
    pub create_topics: i16,
    pub delete_topics: i16,
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
            create_topics: API_VERSION_CREATE_TOPICS,
            delete_topics: API_VERSION_DELETE_TOPICS,
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
