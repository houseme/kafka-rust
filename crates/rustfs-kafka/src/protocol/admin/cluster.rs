#![allow(clippy::wildcard_imports)]
//! Cluster administration helpers.

use kafka_protocol::messages::{
    AddRaftVoterRequest, AddRaftVoterResponse, ApiKey, AssignReplicasToDirsRequest,
    AssignReplicasToDirsResponse, DescribeClusterRequest, DescribeClusterResponse,
    DescribeQuorumRequest, DescribeQuorumResponse, ElectLeadersRequest, ElectLeadersResponse,
    RemoveRaftVoterRequest, RemoveRaftVoterResponse, RequestHeader, UnregisterBrokerRequest,
    UnregisterBrokerResponse, UpdateFeaturesRequest, UpdateFeaturesResponse,
    UpdateRaftVoterRequest, UpdateRaftVoterResponse,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{
    API_VERSION_ADD_RAFT_VOTER, API_VERSION_ASSIGN_REPLICAS_TO_DIRS, API_VERSION_DESCRIBE_CLUSTER,
    API_VERSION_DESCRIBE_QUORUM, API_VERSION_ELECT_LEADERS, API_VERSION_REMOVE_RAFT_VOTER,
    API_VERSION_UNREGISTER_BROKER, API_VERSION_UPDATE_FEATURES, API_VERSION_UPDATE_RAFT_VOTER,
};
use super::*;
use super::{optional_str_bytes, request_header, to_add_raft_listener};
use uuid::Uuid;

/// Endpoint type for broker endpoints in `DescribeCluster`.
pub const DESCRIBE_CLUSTER_ENDPOINT_BROKERS: i8 = 1;

/// Endpoint for one `KRaft` quorum node returned by `DescribeQuorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumListener {
    /// Listener name.
    pub name: String,
    /// Listener host.
    pub host: String,
    /// Listener port.
    pub port: u16,
}

/// One `KRaft` quorum node returned by `DescribeQuorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumNode {
    /// Broker or controller node ID.
    pub node_id: i32,
    /// Listeners returned for this node.
    pub listeners: Vec<QuorumListener>,
}

/// Replica state returned by `DescribeQuorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumReplicaState {
    /// Broker or controller replica ID.
    pub replica_id: i32,
    /// Replica directory UUID as a string, or Kafka's nil UUID sentinel.
    pub replica_directory_id: String,
    /// Last known log end offset.
    pub log_end_offset: i64,
    /// Last fetch timestamp in milliseconds, or Kafka's `-1` sentinel.
    pub last_fetch_timestamp: i64,
    /// Last caught-up timestamp in milliseconds, or Kafka's `-1` sentinel.
    pub last_caught_up_timestamp: i64,
}

/// Per-partition `KRaft` quorum state returned by `DescribeQuorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional per-partition broker error message.
    pub error_message: Option<String>,
    /// Current leader ID, or Kafka's `-1` sentinel if unknown.
    pub leader_id: i32,
    /// Latest known leader epoch.
    pub leader_epoch: i32,
    /// High watermark for the quorum partition.
    pub high_watermark: i64,
    /// Current voters in the quorum.
    pub current_voters: Vec<QuorumReplicaState>,
    /// Observers in the quorum.
    pub observers: Vec<QuorumReplicaState>,
}

/// Per-topic `KRaft` quorum state returned by `DescribeQuorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumTopic {
    /// Topic name.
    pub name: String,
    /// Partition quorum states.
    pub partitions: Vec<QuorumPartition>,
}

/// Parsed response from a `DescribeQuorum` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumResponseData {
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional top-level broker error message.
    pub error_message: Option<String>,
    /// Quorum state grouped by topic.
    pub topics: Vec<QuorumTopic>,
    /// Quorum nodes returned by Kafka v2+.
    pub nodes: Vec<QuorumNode>,
}

/// A broker returned by `DescribeCluster`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterBroker {
    /// Broker ID.
    pub broker_id: i32,
    /// Broker host name.
    pub host: String,
    /// Broker port.
    pub port: i32,
    /// Optional broker rack.
    pub rack: Option<String>,
    /// Whether the broker is fenced.
    pub is_fenced: bool,
}

/// Parsed response from a `DescribeCluster` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClusterResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
    /// Endpoint type described by the broker.
    pub endpoint_type: i8,
    /// Kafka cluster ID.
    pub cluster_id: String,
    /// Current controller broker ID.
    pub controller_id: i32,
    /// Brokers returned by the cluster.
    pub brokers: Vec<ClusterBroker>,
    /// Authorized operations bitfield, or Kafka's sentinel when not requested.
    pub cluster_authorized_operations: i32,
}

/// Upgrade type for `UpdateFeatures`: upgrade only (default).
pub const FEATURE_UPGRADE_TYPE_UPGRADE: i8 = 1;
/// Upgrade type for `UpdateFeatures`: safe downgrade only (lossless).
pub const FEATURE_UPGRADE_TYPE_SAFE_DOWNGRADE: i8 = 2;
/// Upgrade type for `UpdateFeatures`: unsafe downgrade (lossy).
pub const FEATURE_UPGRADE_TYPE_UNSAFE_DOWNGRADE: i8 = 3;

/// A feature to update via `UpdateFeatures`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdate {
    /// The feature name to update.
    pub feature: String,
    /// The new maximum version level.
    pub max_version_level: i16,
    /// The upgrade type (1=upgrade, 2=safe downgrade, 3=unsafe downgrade).
    pub upgrade_type: i8,
}

impl FeatureUpdate {
    /// Create a feature upgrade.
    #[must_use]
    pub fn upgrade(feature: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            feature: feature.into(),
            max_version_level,
            upgrade_type: FEATURE_UPGRADE_TYPE_UPGRADE,
        }
    }

    /// Create a safe (lossless) downgrade.
    #[must_use]
    pub fn safe_downgrade(feature: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            feature: feature.into(),
            max_version_level,
            upgrade_type: FEATURE_UPGRADE_TYPE_SAFE_DOWNGRADE,
        }
    }

    /// Create an unsafe (lossy) downgrade.
    #[must_use]
    pub fn unsafe_downgrade(feature: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            feature: feature.into(),
            max_version_level,
            upgrade_type: FEATURE_UPGRADE_TYPE_UNSAFE_DOWNGRADE,
        }
    }
}

/// Per-feature result from `UpdateFeatures`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFeaturesResult {
    /// The feature name.
    pub feature: String,
    /// Per-feature broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Parsed response from an `UpdateFeatures` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFeaturesResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: i16,
    /// Optional top-level error message.
    pub error_message: Option<String>,
    /// Per-feature update results.
    pub results: Vec<UpdateFeaturesResult>,
}

/// Parsed response from an `UnregisterBroker` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnregisterBrokerResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// A network listener used when adding or updating a `KRaft` voter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftVoterListener {
    /// Listener name, such as `CONTROLLER`.
    pub name: String,
    /// Listener host name.
    pub host: String,
    /// Listener port.
    pub port: u16,
}

impl RaftVoterListener {
    /// Create a `KRaft` voter listener.
    #[must_use]
    pub fn new(name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            host: host.into(),
            port,
        }
    }
}

/// Options for an `AddRaftVoter` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddRaftVoterOptions {
    /// Optional cluster ID expected by the controller quorum.
    pub cluster_id: Option<String>,
    /// Broker-side timeout in milliseconds.
    pub timeout_ms: i32,
    /// Replica ID of the voter to add.
    pub voter_id: i32,
    /// Directory ID of the voter to add.
    pub voter_directory_id: Uuid,
    /// Controller listeners for the voter.
    pub listeners: Vec<RaftVoterListener>,
}

impl AddRaftVoterOptions {
    /// Create options for adding a `KRaft` voter.
    #[must_use]
    pub fn new<I>(voter_id: i32, voter_directory_id: Uuid, listeners: I) -> Self
    where
        I: IntoIterator<Item = RaftVoterListener>,
    {
        Self {
            cluster_id: None,
            timeout_ms: 60_000,
            voter_id,
            voter_directory_id,
            listeners: listeners.into_iter().collect(),
        }
    }

    /// Set the expected cluster ID.
    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = Some(cluster_id.into());
        self
    }

    /// Clear the expected cluster ID.
    #[must_use]
    pub fn without_cluster_id(mut self) -> Self {
        self.cluster_id = None;
        self
    }

    /// Set the broker-side timeout in milliseconds.
    #[must_use]
    pub fn with_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

/// Options for a `RemoveRaftVoter` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveRaftVoterOptions {
    /// Optional cluster ID expected by the controller quorum.
    pub cluster_id: Option<String>,
    /// Replica ID of the voter to remove.
    pub voter_id: i32,
    /// Directory ID of the voter to remove.
    pub voter_directory_id: Uuid,
}

impl RemoveRaftVoterOptions {
    /// Create options for removing a `KRaft` voter.
    #[must_use]
    pub fn new(voter_id: i32, voter_directory_id: Uuid) -> Self {
        Self {
            cluster_id: None,
            voter_id,
            voter_directory_id,
        }
    }

    /// Set the expected cluster ID.
    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = Some(cluster_id.into());
        self
    }

    /// Clear the expected cluster ID.
    #[must_use]
    pub fn without_cluster_id(mut self) -> Self {
        self.cluster_id = None;
        self
    }
}

/// Supported `KRaft` protocol version range for `UpdateRaftVoter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftVersionFeature {
    /// Minimum supported `KRaft` protocol version.
    pub min_supported_version: i16,
    /// Maximum supported `KRaft` protocol version.
    pub max_supported_version: i16,
}

impl RaftVersionFeature {
    /// Create a supported `KRaft` protocol version range.
    #[must_use]
    pub fn new(min_supported_version: i16, max_supported_version: i16) -> Self {
        Self {
            min_supported_version,
            max_supported_version,
        }
    }
}

/// Options for an `UpdateRaftVoter` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRaftVoterOptions {
    /// Optional cluster ID expected by the controller quorum.
    pub cluster_id: Option<String>,
    /// Current leader epoch, or `-1` when unknown.
    pub current_leader_epoch: i32,
    /// Replica ID of the voter to update.
    pub voter_id: i32,
    /// Directory ID of the voter to update.
    pub voter_directory_id: Uuid,
    /// Controller listeners for the voter.
    pub listeners: Vec<RaftVoterListener>,
    /// Supported `KRaft` protocol version range.
    pub raft_version_feature: RaftVersionFeature,
}

impl UpdateRaftVoterOptions {
    /// Create options for updating a `KRaft` voter.
    #[must_use]
    pub fn new<I>(
        voter_id: i32,
        voter_directory_id: Uuid,
        listeners: I,
        raft_version_feature: RaftVersionFeature,
    ) -> Self
    where
        I: IntoIterator<Item = RaftVoterListener>,
    {
        Self {
            cluster_id: None,
            current_leader_epoch: -1,
            voter_id,
            voter_directory_id,
            listeners: listeners.into_iter().collect(),
            raft_version_feature,
        }
    }

    /// Set the expected cluster ID.
    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = Some(cluster_id.into());
        self
    }

    /// Clear the expected cluster ID.
    #[must_use]
    pub fn without_cluster_id(mut self) -> Self {
        self.cluster_id = None;
        self
    }

    /// Set the current leader epoch.
    #[must_use]
    pub fn with_current_leader_epoch(mut self, current_leader_epoch: i32) -> Self {
        self.current_leader_epoch = current_leader_epoch;
        self
    }
}

/// Parsed response from `AddRaftVoter` or `RemoveRaftVoter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftVoterResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Current leader details returned by `UpdateRaftVoter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftVoterCurrentLeader {
    /// Replica ID of the current leader, or `-1` when unknown.
    pub leader_id: i32,
    /// Latest known leader epoch.
    pub leader_epoch: i32,
    /// Leader host name.
    pub host: String,
    /// Leader port.
    pub port: i32,
}

/// Parsed response from an `UpdateRaftVoter` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRaftVoterResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Broker error code.
    pub error_code: i16,
    /// Current leader details when Kafka returned the optional tagged field.
    pub current_leader: Option<RaftVoterCurrentLeader>,
}

/// One topic assignment for `AssignReplicasToDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDirectoryTopicAssignment {
    /// Topic ID to assign.
    pub topic_id: Uuid,
    /// Partition indexes to assign to the directory.
    pub partitions: Vec<i32>,
}

impl ReplicaDirectoryTopicAssignment {
    /// Create a topic assignment.
    #[must_use]
    pub fn new<I>(topic_id: Uuid, partitions: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        Self {
            topic_id,
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// One directory assignment for `AssignReplicasToDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDirectoryAssignment {
    /// Directory ID.
    pub directory_id: Uuid,
    /// Topic assignments for this directory.
    pub topics: Vec<ReplicaDirectoryTopicAssignment>,
}

impl ReplicaDirectoryAssignment {
    /// Create a directory assignment.
    #[must_use]
    pub fn new<I>(directory_id: Uuid, topics: I) -> Self
    where
        I: IntoIterator<Item = ReplicaDirectoryTopicAssignment>,
    {
        Self {
            directory_id,
            topics: topics.into_iter().collect(),
        }
    }
}

/// Options for an `AssignReplicasToDirs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsOptions {
    /// ID of the requesting broker.
    pub broker_id: i32,
    /// Epoch of the requesting broker.
    pub broker_epoch: i64,
    /// Directory assignments to apply.
    pub directories: Vec<ReplicaDirectoryAssignment>,
}

impl AssignReplicasToDirsOptions {
    /// Create options for assigning replicas to log directories.
    #[must_use]
    pub fn new<I>(broker_id: i32, broker_epoch: i64, directories: I) -> Self
    where
        I: IntoIterator<Item = ReplicaDirectoryAssignment>,
    {
        Self {
            broker_id,
            broker_epoch,
            directories: directories.into_iter().collect(),
        }
    }
}

/// Per-partition result from `AssignReplicasToDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDirectoryPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Partition-level broker error code.
    pub error_code: i16,
}

/// Per-topic result from `AssignReplicasToDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDirectoryTopicResult {
    /// Topic ID.
    pub topic_id: Uuid,
    /// Per-partition results.
    pub partitions: Vec<ReplicaDirectoryPartitionResult>,
}

/// Per-directory result from `AssignReplicasToDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDirectoryAssignmentResult {
    /// Directory ID.
    pub directory_id: Uuid,
    /// Per-topic results.
    pub topics: Vec<ReplicaDirectoryTopicResult>,
}

/// Parsed response from an `AssignReplicasToDirs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Per-directory assignment results.
    pub directories: Vec<ReplicaDirectoryAssignmentResult>,
}

pub fn build_describe_cluster_request(
    correlation_id: i32,
    client_id: &str,
    include_authorized_operations: bool,
    include_fenced_brokers: bool,
) -> (RequestHeader, DescribeClusterRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeCluster,
        API_VERSION_DESCRIBE_CLUSTER,
    );
    let request = DescribeClusterRequest::default()
        .with_include_cluster_authorized_operations(include_authorized_operations)
        .with_endpoint_type(DESCRIBE_CLUSTER_ENDPOINT_BROKERS)
        .with_include_fenced_brokers(include_fenced_brokers);

    (header, request)
}

/// Build a `ListGroups` request.
pub fn build_describe_quorum_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[TopicPartitionFilter],
) -> (RequestHeader, DescribeQuorumRequest) {
    use kafka_protocol::messages::describe_quorum_request::{
        PartitionData as QuorumPartitionRequest, TopicData as QuorumTopicRequest,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeQuorum,
        API_VERSION_DESCRIBE_QUORUM,
    );
    let topics = topics
        .iter()
        .map(|topic| {
            QuorumTopicRequest::default()
                .with_topic_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .copied()
                        .map(|partition| {
                            QuorumPartitionRequest::default().with_partition_index(partition)
                        })
                        .collect(),
                )
        })
        .collect();
    let request = DescribeQuorumRequest::default().with_topics(topics);

    (header, request)
}

/// Build an `ElectLeaders` request.
pub fn build_elect_leaders_request(
    correlation_id: i32,
    client_id: &str,
    options: &ElectLeadersOptions,
) -> (RequestHeader, ElectLeadersRequest) {
    use kafka_protocol::messages::elect_leaders_request::TopicPartitions;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ElectLeaders,
        API_VERSION_ELECT_LEADERS,
    );
    let topic_partitions = options.topic_partitions.as_ref().map(|topics| {
        topics
            .iter()
            .map(|topic| {
                TopicPartitions::default()
                    .with_topic(StrBytes::from_string(topic.topic.clone()).into())
                    .with_partitions(topic.partitions.clone())
            })
            .collect()
    });
    let request = ElectLeadersRequest::default()
        .with_election_type(options.election_type)
        .with_topic_partitions(topic_partitions)
        .with_timeout_ms(options.timeout_ms);

    (header, request)
}

/// Build a `ConsumerGroupDescribe` request.
pub fn build_update_features_request(
    correlation_id: i32,
    client_id: &str,
    feature_updates: &[FeatureUpdate],
    validate_only: bool,
) -> (RequestHeader, UpdateFeaturesRequest) {
    use kafka_protocol::messages::update_features_request::FeatureUpdateKey;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::UpdateFeatures,
        API_VERSION_UPDATE_FEATURES,
    );
    let updates: Vec<FeatureUpdateKey> = feature_updates
        .iter()
        .map(|update| {
            FeatureUpdateKey::default()
                .with_feature(StrBytes::from_string(update.feature.clone()))
                .with_max_version_level(update.max_version_level)
                .with_upgrade_type(update.upgrade_type)
        })
        .collect();
    let request = UpdateFeaturesRequest::default()
        .with_feature_updates(updates)
        .with_validate_only(validate_only);

    (header, request)
}

/// Build an `UnregisterBroker` request.
pub fn build_unregister_broker_request(
    correlation_id: i32,
    client_id: &str,
    broker_id: i32,
) -> (RequestHeader, UnregisterBrokerRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::UnregisterBroker,
        API_VERSION_UNREGISTER_BROKER,
    );
    let request = UnregisterBrokerRequest::default()
        .with_broker_id(kafka_protocol::messages::BrokerId::from(broker_id));

    (header, request)
}

/// Build an `AssignReplicasToDirs` request.
pub fn build_assign_replicas_to_dirs_request(
    correlation_id: i32,
    client_id: &str,
    options: &AssignReplicasToDirsOptions,
) -> (RequestHeader, AssignReplicasToDirsRequest) {
    use kafka_protocol::messages::assign_replicas_to_dirs_request::{
        DirectoryData as KpDirectoryData, PartitionData as KpPartitionData,
        TopicData as KpTopicData,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::AssignReplicasToDirs,
        API_VERSION_ASSIGN_REPLICAS_TO_DIRS,
    );
    let directories: Vec<KpDirectoryData> = options
        .directories
        .iter()
        .map(|directory| {
            let topics: Vec<KpTopicData> = directory
                .topics
                .iter()
                .map(|topic| {
                    let partitions: Vec<KpPartitionData> = topic
                        .partitions
                        .iter()
                        .copied()
                        .map(|partition_index| {
                            KpPartitionData::default().with_partition_index(partition_index)
                        })
                        .collect();
                    KpTopicData::default()
                        .with_topic_id(topic.topic_id)
                        .with_partitions(partitions)
                })
                .collect();
            KpDirectoryData::default()
                .with_id(directory.directory_id)
                .with_topics(topics)
        })
        .collect();
    let request = AssignReplicasToDirsRequest::default()
        .with_broker_id(kafka_protocol::messages::BrokerId::from(options.broker_id))
        .with_broker_epoch(options.broker_epoch)
        .with_directories(directories);

    (header, request)
}

/// Build an `AddRaftVoter` request.
pub fn build_add_raft_voter_request(
    correlation_id: i32,
    client_id: &str,
    options: &AddRaftVoterOptions,
) -> (RequestHeader, AddRaftVoterRequest) {
    use kafka_protocol::messages::add_raft_voter_request::Listener as KpListener;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::AddRaftVoter,
        API_VERSION_ADD_RAFT_VOTER,
    );
    let listeners: Vec<KpListener> = options.listeners.iter().map(to_add_raft_listener).collect();
    let request = AddRaftVoterRequest::default()
        .with_cluster_id(optional_str_bytes(options.cluster_id.as_deref()))
        .with_timeout_ms(options.timeout_ms)
        .with_voter_id(options.voter_id)
        .with_voter_directory_id(options.voter_directory_id)
        .with_listeners(listeners);

    (header, request)
}

/// Build a `RemoveRaftVoter` request.
pub fn build_remove_raft_voter_request(
    correlation_id: i32,
    client_id: &str,
    options: &RemoveRaftVoterOptions,
) -> (RequestHeader, RemoveRaftVoterRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::RemoveRaftVoter,
        API_VERSION_REMOVE_RAFT_VOTER,
    );
    let request = RemoveRaftVoterRequest::default()
        .with_cluster_id(optional_str_bytes(options.cluster_id.as_deref()))
        .with_voter_id(options.voter_id)
        .with_voter_directory_id(options.voter_directory_id);

    (header, request)
}

/// Build an `UpdateRaftVoter` request.
pub fn build_update_raft_voter_request(
    correlation_id: i32,
    client_id: &str,
    options: &UpdateRaftVoterOptions,
) -> (RequestHeader, UpdateRaftVoterRequest) {
    use kafka_protocol::messages::update_raft_voter_request::{
        KRaftVersionFeature as KpKRaftVersionFeature, Listener as KpListener,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::UpdateRaftVoter,
        API_VERSION_UPDATE_RAFT_VOTER,
    );
    let listeners: Vec<KpListener> = options
        .listeners
        .iter()
        .map(|listener| {
            KpListener::default()
                .with_name(StrBytes::from_string(listener.name.clone()))
                .with_host(StrBytes::from_string(listener.host.clone()))
                .with_port(listener.port)
        })
        .collect();
    let version_feature = KpKRaftVersionFeature::default()
        .with_min_supported_version(options.raft_version_feature.min_supported_version)
        .with_max_supported_version(options.raft_version_feature.max_supported_version);
    let request = UpdateRaftVoterRequest::default()
        .with_cluster_id(optional_str_bytes(options.cluster_id.as_deref()))
        .with_current_leader_epoch(options.current_leader_epoch)
        .with_voter_id(options.voter_id)
        .with_voter_directory_id(options.voter_directory_id)
        .with_listeners(listeners)
        .with_k_raft_version_feature(version_feature);

    (header, request)
}

/// Build an `AlterShareGroupOffsets` request.
pub fn convert_describe_cluster_response(
    response: DescribeClusterResponse,
) -> DescribeClusterResponseData {
    DescribeClusterResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        endpoint_type: response.endpoint_type,
        cluster_id: response.cluster_id.to_string(),
        controller_id: i32::from(response.controller_id),
        brokers: response
            .brokers
            .into_iter()
            .map(|broker| ClusterBroker {
                broker_id: i32::from(broker.broker_id),
                host: broker.host.to_string(),
                port: broker.port,
                rack: broker.rack.map(|rack| rack.to_string()),
                is_fenced: broker.is_fenced,
            })
            .collect(),
        cluster_authorized_operations: response.cluster_authorized_operations,
    }
}

/// Convert a generated `ListGroupsResponse` into the crate's public shape.
pub fn convert_describe_quorum_response(
    response: DescribeQuorumResponse,
) -> DescribeQuorumResponseData {
    DescribeQuorumResponseData {
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        topics: response
            .topics
            .into_iter()
            .map(|topic| QuorumTopic {
                name: topic.topic_name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| QuorumPartition {
                        partition_index: partition.partition_index,
                        error_code: partition.error_code,
                        error_message: partition.error_message.map(|message| message.to_string()),
                        leader_id: i32::from(partition.leader_id),
                        leader_epoch: partition.leader_epoch,
                        high_watermark: partition.high_watermark,
                        current_voters: partition
                            .current_voters
                            .iter()
                            .map(convert_quorum_replica_state)
                            .collect(),
                        observers: partition
                            .observers
                            .iter()
                            .map(convert_quorum_replica_state)
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
        nodes: response
            .nodes
            .into_iter()
            .map(|node| QuorumNode {
                node_id: i32::from(node.node_id),
                listeners: node
                    .listeners
                    .into_iter()
                    .map(|listener| QuorumListener {
                        name: listener.name.to_string(),
                        host: listener.host.to_string(),
                        port: listener.port,
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn convert_quorum_replica_state(
    replica: &kafka_protocol::messages::describe_quorum_response::ReplicaState,
) -> QuorumReplicaState {
    QuorumReplicaState {
        replica_id: i32::from(replica.replica_id),
        replica_directory_id: replica.replica_directory_id.to_string(),
        log_end_offset: replica.log_end_offset,
        last_fetch_timestamp: replica.last_fetch_timestamp,
        last_caught_up_timestamp: replica.last_caught_up_timestamp,
    }
}

/// Convert a generated `ElectLeadersResponse` into the crate's public shape.
pub fn convert_elect_leaders_response(response: ElectLeadersResponse) -> ElectLeadersResponseData {
    ElectLeadersResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        replica_election_results: response
            .replica_election_results
            .into_iter()
            .map(|topic| ElectLeadersTopicResult {
                topic: topic.topic.to_string(),
                partition_results: topic
                    .partition_result
                    .into_iter()
                    .map(|partition| ElectLeadersPartitionResult {
                        partition_id: partition.partition_id,
                        error_code: partition.error_code,
                        error_message: partition.error_message.map(|message| message.to_string()),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `ConsumerGroupDescribeResponse` into the crate's public shape.
pub fn convert_update_features_response(
    response: UpdateFeaturesResponse,
) -> UpdateFeaturesResponseData {
    UpdateFeaturesResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|m| m.to_string()),
        results: response
            .results
            .into_iter()
            .map(|result| UpdateFeaturesResult {
                feature: result.feature.to_string(),
                error_code: result.error_code,
                error_message: result.error_message.map(|m| m.to_string()),
            })
            .collect(),
    }
}

/// Convert a generated `UnregisterBrokerResponse` into the crate's public shape.
pub fn convert_unregister_broker_response(
    response: UnregisterBrokerResponse,
) -> UnregisterBrokerResponseData {
    UnregisterBrokerResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|m| m.to_string()),
    }
}

/// Convert a generated `AssignReplicasToDirsResponse` into the crate's public shape.
pub fn convert_assign_replicas_to_dirs_response(
    response: AssignReplicasToDirsResponse,
) -> AssignReplicasToDirsResponseData {
    AssignReplicasToDirsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        directories: response
            .directories
            .into_iter()
            .map(|directory| ReplicaDirectoryAssignmentResult {
                directory_id: directory.id,
                topics: directory
                    .topics
                    .into_iter()
                    .map(|topic| ReplicaDirectoryTopicResult {
                        topic_id: topic.topic_id,
                        partitions: topic
                            .partitions
                            .into_iter()
                            .map(|partition| ReplicaDirectoryPartitionResult {
                                partition_index: partition.partition_index,
                                error_code: partition.error_code,
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `AddRaftVoterResponse` into the crate's public shape.
pub fn convert_add_raft_voter_response(response: AddRaftVoterResponse) -> RaftVoterResponseData {
    RaftVoterResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|m| m.to_string()),
    }
}

/// Convert a generated `RemoveRaftVoterResponse` into the crate's public shape.
pub fn convert_remove_raft_voter_response(
    response: RemoveRaftVoterResponse,
) -> RaftVoterResponseData {
    RaftVoterResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|m| m.to_string()),
    }
}

/// Convert a generated `UpdateRaftVoterResponse` into the crate's public shape.
pub fn convert_update_raft_voter_response(
    response: UpdateRaftVoterResponse,
) -> UpdateRaftVoterResponseData {
    let current_leader = response.current_leader;
    let current_leader = if current_leader
        == kafka_protocol::messages::update_raft_voter_response::CurrentLeader::default()
    {
        None
    } else {
        Some(RaftVoterCurrentLeader {
            leader_id: i32::from(current_leader.leader_id),
            leader_epoch: current_leader.leader_epoch,
            host: current_leader.host.to_string(),
            port: current_leader.port,
        })
    };

    UpdateRaftVoterResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        current_leader,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::assign_replicas_to_dirs_response::{
        DirectoryData as KpAssignDirectoryResult, PartitionData as KpAssignPartitionResult,
        TopicData as KpAssignTopicResult,
    };
    use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;
    use kafka_protocol::messages::describe_quorum_response::{
        Listener as KpQuorumListener, Node as KpQuorumNode, PartitionData as KpQuorumPartition,
        ReplicaState as KpQuorumReplica, TopicData as KpQuorumTopic,
    };
    use kafka_protocol::messages::elect_leaders_response::{
        PartitionResult as KpElectionPartitionResult,
        ReplicaElectionResult as KpReplicaElectionResult,
    };
    use kafka_protocol::messages::unregister_broker_response::UnregisterBrokerResponse as KpUnregisterBrokerResponse;
    use kafka_protocol::messages::update_features_response::UpdatableFeatureResult as KpUpdatableFeatureResult;
    use kafka_protocol::messages::update_raft_voter_response::CurrentLeader as KpRaftCurrentLeader;
    use kafka_protocol::messages::{ApiKey, BrokerId};
    use kafka_protocol::protocol::StrBytes;
    use uuid::Uuid;

    #[test]
    fn describe_cluster_request_uses_latest_supported_protocol_fields() {
        let (header, request) = build_describe_cluster_request(42, "client-a", true, true);

        assert_eq!(header.request_api_key, ApiKey::DescribeCluster as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_CLUSTER);
        assert_eq!(header.correlation_id, 42);
        assert_eq!(
            header.client_id.as_ref().map(ToString::to_string),
            Some("client-a".to_owned())
        );
        assert!(request.include_cluster_authorized_operations);
        assert_eq!(request.endpoint_type, DESCRIBE_CLUSTER_ENDPOINT_BROKERS);
        assert!(request.include_fenced_brokers);
    }
    #[test]
    fn describe_quorum_request_uses_topic_partition_filters() {
        let filter = [TopicPartitionFilter::new("cluster-metadata", [0])];
        let (header, request) = build_describe_quorum_request(19, "client-l", &filter);

        assert_eq!(header.request_api_key, ApiKey::DescribeQuorum as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_QUORUM);
        assert_eq!(request.topics[0].topic_name.to_string(), "cluster-metadata");
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
    }

    #[test]
    fn elect_leaders_request_preserves_type_timeout_and_optional_scope() {
        let scoped = ElectLeadersOptions::new(
            ELECTION_TYPE_UNCLEAN,
            [TopicPartitionFilter::new("topic-a", [0, 2])],
        )
        .with_timeout_ms(7_000);
        let (header, request) = build_elect_leaders_request(20, "client-m", &scoped);

        assert_eq!(header.request_api_key, ApiKey::ElectLeaders as i16);
        assert_eq!(header.request_api_version, API_VERSION_ELECT_LEADERS);
        assert_eq!(request.election_type, ELECTION_TYPE_UNCLEAN);
        assert_eq!(request.timeout_ms, 7_000);
        let topics = request.topic_partitions.as_ref().unwrap();
        assert_eq!(topics[0].topic.to_string(), "topic-a");
        assert_eq!(topics[0].partitions, vec![0, 2]);

        let (_, all_request) = build_elect_leaders_request(
            21,
            "client-n",
            &ElectLeadersOptions::all_partitions(ELECTION_TYPE_PREFERRED),
        );
        assert!(all_request.topic_partitions.is_none());
        assert_eq!(all_request.election_type, ELECTION_TYPE_PREFERRED);
    }
    #[test]
    fn convert_describe_cluster_response_preserves_new_fields() {
        let response = DescribeClusterResponse::default()
            .with_throttle_time_ms(10)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_endpoint_type(DESCRIBE_CLUSTER_ENDPOINT_BROKERS)
            .with_cluster_id(StrBytes::from_static_str("cluster-a"))
            .with_controller_id(BrokerId::from(1))
            .with_brokers(vec![
                DescribeClusterBroker::default()
                    .with_broker_id(BrokerId::from(1))
                    .with_host(StrBytes::from_static_str("broker-1"))
                    .with_port(9092)
                    .with_rack(Some(StrBytes::from_static_str("rack-a")))
                    .with_is_fenced(true),
            ])
            .with_cluster_authorized_operations(123);

        let converted = convert_describe_cluster_response(response);

        assert_eq!(converted.throttle_time_ms, 10);
        assert_eq!(converted.error_message, Some("ok".to_owned()));
        assert_eq!(converted.cluster_id, "cluster-a");
        assert_eq!(converted.controller_id, 1);
        assert_eq!(converted.cluster_authorized_operations, 123);
        assert_eq!(
            converted.brokers,
            vec![ClusterBroker {
                broker_id: 1,
                host: "broker-1".to_owned(),
                port: 9092,
                rack: Some("rack-a".to_owned()),
                is_fenced: true,
            }]
        );
    }
    #[test]
    fn convert_describe_quorum_response_preserves_kraft_state() {
        let response = DescribeQuorumResponse::default()
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_topics(vec![
                KpQuorumTopic::default()
                    .with_topic_name(StrBytes::from_static_str("cluster-metadata").into())
                    .with_partitions(vec![
                        KpQuorumPartition::default()
                            .with_partition_index(0)
                            .with_error_code(0)
                            .with_error_message(Some(StrBytes::from_static_str("ok")))
                            .with_leader_id(BrokerId::from(1))
                            .with_leader_epoch(7)
                            .with_high_watermark(128)
                            .with_current_voters(vec![
                                KpQuorumReplica::default()
                                    .with_replica_id(BrokerId::from(1))
                                    .with_log_end_offset(128)
                                    .with_last_fetch_timestamp(-1)
                                    .with_last_caught_up_timestamp(1_700_000),
                            ])
                            .with_observers(vec![
                                KpQuorumReplica::default()
                                    .with_replica_id(BrokerId::from(2))
                                    .with_log_end_offset(120)
                                    .with_last_fetch_timestamp(1_699_900)
                                    .with_last_caught_up_timestamp(1_699_800),
                            ]),
                    ]),
            ])
            .with_nodes(vec![
                KpQuorumNode::default()
                    .with_node_id(BrokerId::from(1))
                    .with_listeners(vec![
                        KpQuorumListener::default()
                            .with_name(StrBytes::from_static_str("CONTROLLER"))
                            .with_host(StrBytes::from_static_str("broker-1"))
                            .with_port(9093),
                    ]),
            ]);

        let converted = convert_describe_quorum_response(response);

        assert_eq!(converted.error_message, Some("ok".to_owned()));
        assert_eq!(converted.topics[0].name, "cluster-metadata");
        let partition = &converted.topics[0].partitions[0];
        assert_eq!(partition.leader_id, 1);
        assert_eq!(partition.leader_epoch, 7);
        assert_eq!(partition.high_watermark, 128);
        assert_eq!(partition.current_voters[0].replica_id, 1);
        assert_eq!(partition.observers[0].log_end_offset, 120);
        assert_eq!(
            partition.current_voters[0].replica_directory_id,
            "00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(converted.nodes[0].listeners[0].host, "broker-1");
        assert_eq!(converted.nodes[0].listeners[0].port, 9093);
    }

    #[test]
    fn convert_elect_leaders_response_preserves_partition_errors() {
        let response = ElectLeadersResponse::default()
            .with_throttle_time_ms(20)
            .with_error_code(0)
            .with_replica_election_results(vec![
                KpReplicaElectionResult::default()
                    .with_topic(StrBytes::from_static_str("topic-a").into())
                    .with_partition_result(vec![
                        KpElectionPartitionResult::default()
                            .with_partition_id(0)
                            .with_error_code(0)
                            .with_error_message(Some(StrBytes::from_static_str("ok"))),
                    ]),
            ]);

        let converted = convert_elect_leaders_response(response);

        assert_eq!(converted.throttle_time_ms, 20);
        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.replica_election_results[0].topic, "topic-a");
        assert_eq!(
            converted.replica_election_results[0].partition_results[0].partition_id,
            0
        );
        assert_eq!(
            converted.replica_election_results[0].partition_results[0].error_message,
            Some("ok".to_owned())
        );
    }
    #[test]
    fn update_features_request_includes_feature_updates_and_validate_only() {
        let updates = [
            FeatureUpdate::upgrade("kraft.version", 3),
            FeatureUpdate::safe_downgrade("test.feature", 1),
        ];
        let (header, request) = build_update_features_request(42, "client-a", &updates, true);

        assert_eq!(header.request_api_key, ApiKey::UpdateFeatures as i16);
        assert_eq!(header.request_api_version, API_VERSION_UPDATE_FEATURES);
        assert_eq!(header.correlation_id, 42);
        assert!(request.validate_only);
        assert_eq!(request.feature_updates.len(), 2);
        assert_eq!(
            request.feature_updates[0].feature,
            StrBytes::from_static_str("kraft.version")
        );
        assert_eq!(request.feature_updates[0].max_version_level, 3);
        assert_eq!(
            request.feature_updates[0].upgrade_type,
            FEATURE_UPGRADE_TYPE_UPGRADE
        );
        assert_eq!(
            request.feature_updates[1].feature,
            StrBytes::from_static_str("test.feature")
        );
        assert_eq!(request.feature_updates[1].max_version_level, 1);
        assert_eq!(
            request.feature_updates[1].upgrade_type,
            FEATURE_UPGRADE_TYPE_SAFE_DOWNGRADE
        );
    }

    #[test]
    fn unregister_broker_request_includes_broker_id() {
        let (header, request) = build_unregister_broker_request(99, "client-b", 42);

        assert_eq!(header.request_api_key, ApiKey::UnregisterBroker as i16);
        assert_eq!(header.request_api_version, API_VERSION_UNREGISTER_BROKER);
        assert_eq!(header.correlation_id, 99);
        assert_eq!(i32::from(request.broker_id), 42);
    }

    #[test]
    fn assign_replicas_to_dirs_request_preserves_directory_topic_partitions() {
        let directory_id = Uuid::from_u128(1);
        let topic_id = Uuid::from_u128(2);
        let options = AssignReplicasToDirsOptions::new(
            7,
            99,
            [ReplicaDirectoryAssignment::new(
                directory_id,
                [ReplicaDirectoryTopicAssignment::new(topic_id, [0, 2, 4])],
            )],
        );
        let (header, request) = build_assign_replicas_to_dirs_request(100, "client-c", &options);

        assert_eq!(header.request_api_key, ApiKey::AssignReplicasToDirs as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_ASSIGN_REPLICAS_TO_DIRS
        );
        assert_eq!(header.correlation_id, 100);
        assert_eq!(i32::from(request.broker_id), 7);
        assert_eq!(request.broker_epoch, 99);
        assert_eq!(request.directories[0].id, directory_id);
        assert_eq!(request.directories[0].topics[0].topic_id, topic_id);
        assert_eq!(
            request.directories[0].topics[0]
                .partitions
                .iter()
                .map(|partition| partition.partition_index)
                .collect::<Vec<_>>(),
            vec![0, 2, 4]
        );
    }

    #[test]
    fn raft_voter_requests_preserve_cluster_listener_and_version_fields() {
        let directory_id = Uuid::from_u128(3);
        let listener = RaftVoterListener::new("CONTROLLER", "controller-1", 9093);

        let add = AddRaftVoterOptions::new(11, directory_id, [listener.clone()])
            .with_cluster_id("cluster-a")
            .with_timeout_ms(30_000);
        let (add_header, add_request) = build_add_raft_voter_request(101, "client-d", &add);
        assert_eq!(add_header.request_api_key, ApiKey::AddRaftVoter as i16);
        assert_eq!(add_header.request_api_version, API_VERSION_ADD_RAFT_VOTER);
        assert_eq!(
            add_request.cluster_id.as_ref().map(ToString::to_string),
            Some("cluster-a".to_owned())
        );
        assert_eq!(add_request.timeout_ms, 30_000);
        assert_eq!(add_request.voter_id, 11);
        assert_eq!(add_request.voter_directory_id, directory_id);
        assert_eq!(add_request.listeners[0].name.to_string(), "CONTROLLER");
        assert_eq!(add_request.listeners[0].host.to_string(), "controller-1");
        assert_eq!(add_request.listeners[0].port, 9093);

        let remove = RemoveRaftVoterOptions::new(11, directory_id).with_cluster_id("cluster-a");
        let (remove_header, remove_request) =
            build_remove_raft_voter_request(102, "client-e", &remove);
        assert_eq!(
            remove_header.request_api_key,
            ApiKey::RemoveRaftVoter as i16
        );
        assert_eq!(
            remove_header.request_api_version,
            API_VERSION_REMOVE_RAFT_VOTER
        );
        assert_eq!(
            remove_request.cluster_id.as_ref().map(ToString::to_string),
            Some("cluster-a".to_owned())
        );
        assert_eq!(remove_request.voter_id, 11);
        assert_eq!(remove_request.voter_directory_id, directory_id);

        let update = UpdateRaftVoterOptions::new(
            11,
            directory_id,
            [listener],
            RaftVersionFeature::new(1, 3),
        )
        .with_cluster_id("cluster-a")
        .with_current_leader_epoch(5);
        let (update_header, update_request) =
            build_update_raft_voter_request(103, "client-f", &update);
        assert_eq!(
            update_header.request_api_key,
            ApiKey::UpdateRaftVoter as i16
        );
        assert_eq!(
            update_header.request_api_version,
            API_VERSION_UPDATE_RAFT_VOTER
        );
        assert_eq!(update_request.current_leader_epoch, 5);
        assert_eq!(update_request.voter_id, 11);
        assert_eq!(update_request.voter_directory_id, directory_id);
        assert_eq!(
            update_request.k_raft_version_feature.min_supported_version,
            1
        );
        assert_eq!(
            update_request.k_raft_version_feature.max_supported_version,
            3
        );
    }

    #[test]
    fn update_features_response_maps_all_fields() {
        let response = UpdateFeaturesResponse::default()
            .with_throttle_time_ms(50)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("top-level-ok")))
            .with_results(vec![
                KpUpdatableFeatureResult::default()
                    .with_feature(StrBytes::from_static_str("kraft.version"))
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok"))),
                KpUpdatableFeatureResult::default()
                    .with_feature(StrBytes::from_static_str("test.feature"))
                    .with_error_code(42)
                    .with_error_message(None),
            ]);

        let converted = convert_update_features_response(response);

        assert_eq!(converted.throttle_time_ms, 50);
        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.error_message, Some("top-level-ok".to_owned()));
        assert_eq!(converted.results.len(), 2);
        assert_eq!(converted.results[0].feature, "kraft.version");
        assert_eq!(converted.results[0].error_code, 0);
        assert_eq!(converted.results[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.results[1].feature, "test.feature");
        assert_eq!(converted.results[1].error_code, 42);
        assert_eq!(converted.results[1].error_message, None);
    }

    #[test]
    fn unregister_broker_response_maps_all_fields() {
        let response = KpUnregisterBrokerResponse::default()
            .with_throttle_time_ms(100)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("unregistered")));

        let converted = convert_unregister_broker_response(response);

        assert_eq!(converted.throttle_time_ms, 100);
        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.error_message, Some("unregistered".to_owned()));
    }

    #[test]
    fn assign_replicas_to_dirs_response_maps_nested_results() {
        let directory_id = Uuid::from_u128(4);
        let topic_id = Uuid::from_u128(5);
        let response = AssignReplicasToDirsResponse::default()
            .with_throttle_time_ms(10)
            .with_error_code(0)
            .with_directories(vec![
                KpAssignDirectoryResult::default()
                    .with_id(directory_id)
                    .with_topics(vec![
                        KpAssignTopicResult::default()
                            .with_topic_id(topic_id)
                            .with_partitions(vec![
                                KpAssignPartitionResult::default()
                                    .with_partition_index(0)
                                    .with_error_code(0),
                                KpAssignPartitionResult::default()
                                    .with_partition_index(1)
                                    .with_error_code(42),
                            ]),
                    ]),
            ]);

        let converted = convert_assign_replicas_to_dirs_response(response);

        assert_eq!(converted.throttle_time_ms, 10);
        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.directories[0].directory_id, directory_id);
        assert_eq!(converted.directories[0].topics[0].topic_id, topic_id);
        assert_eq!(
            converted.directories[0].topics[0].partitions,
            vec![
                ReplicaDirectoryPartitionResult {
                    partition_index: 0,
                    error_code: 0
                },
                ReplicaDirectoryPartitionResult {
                    partition_index: 1,
                    error_code: 42
                },
            ]
        );
    }

    #[test]
    fn raft_voter_responses_map_errors_and_optional_current_leader() {
        let add = AddRaftVoterResponse::default()
            .with_throttle_time_ms(11)
            .with_error_code(3)
            .with_error_message(Some(StrBytes::from_static_str("add-failed")));
        let remove = RemoveRaftVoterResponse::default()
            .with_throttle_time_ms(12)
            .with_error_code(4)
            .with_error_message(None);

        assert_eq!(
            convert_add_raft_voter_response(add),
            RaftVoterResponseData {
                throttle_time_ms: 11,
                error_code: 3,
                error_message: Some("add-failed".to_owned())
            }
        );
        assert_eq!(
            convert_remove_raft_voter_response(remove),
            RaftVoterResponseData {
                throttle_time_ms: 12,
                error_code: 4,
                error_message: None
            }
        );

        let without_leader = UpdateRaftVoterResponse::default()
            .with_throttle_time_ms(13)
            .with_error_code(5);
        let converted_without_leader = convert_update_raft_voter_response(without_leader);
        assert_eq!(converted_without_leader.throttle_time_ms, 13);
        assert_eq!(converted_without_leader.error_code, 5);
        assert_eq!(converted_without_leader.current_leader, None);

        let with_leader = UpdateRaftVoterResponse::default()
            .with_throttle_time_ms(14)
            .with_error_code(0)
            .with_current_leader(
                KpRaftCurrentLeader::default()
                    .with_leader_id(BrokerId::from(1))
                    .with_leader_epoch(9)
                    .with_host(StrBytes::from_static_str("controller-1"))
                    .with_port(9093),
            );
        let converted_with_leader = convert_update_raft_voter_response(with_leader);
        assert_eq!(
            converted_with_leader.current_leader,
            Some(RaftVoterCurrentLeader {
                leader_id: 1,
                leader_epoch: 9,
                host: "controller-1".to_owned(),
                port: 9093
            })
        );
    }
}
