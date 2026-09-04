//! Public re-exports for the client module.

pub use crate::compression::Compression;
pub use crate::protocol::admin::{
    ACL_OPERATION_ALL, ACL_OPERATION_ALTER, ACL_OPERATION_ALTER_CONFIGS, ACL_OPERATION_ANY,
    ACL_OPERATION_CLUSTER_ACTION, ACL_OPERATION_CREATE, ACL_OPERATION_CREATE_TOKENS,
    ACL_OPERATION_DELETE, ACL_OPERATION_DESCRIBE, ACL_OPERATION_DESCRIBE_CONFIGS,
    ACL_OPERATION_DESCRIBE_TOKENS, ACL_OPERATION_IDEMPOTENT_WRITE, ACL_OPERATION_READ,
    ACL_OPERATION_WRITE, ACL_PATTERN_TYPE_ANY, ACL_PATTERN_TYPE_LITERAL, ACL_PATTERN_TYPE_MATCH,
    ACL_PATTERN_TYPE_PREFIXED, ACL_PERMISSION_TYPE_ALLOW, ACL_PERMISSION_TYPE_ANY,
    ACL_PERMISSION_TYPE_DENY, ACL_RESOURCE_TYPE_ANY, ACL_RESOURCE_TYPE_CLUSTER,
    ACL_RESOURCE_TYPE_DELEGATION_TOKEN, ACL_RESOURCE_TYPE_GROUP, ACL_RESOURCE_TYPE_TOPIC,
    ACL_RESOURCE_TYPE_TRANSACTIONAL_ID, ACL_RESOURCE_TYPE_USER, AclBinding, AclDescription,
    AclResource, ActiveProducer, AddOffsetsToTxnResponseData, AddRaftVoterOptions,
    AlterClientQuotaEntryResult, AlterClientQuotasOptions, AlterClientQuotasResponseData,
    AlterConfigsEntry, AlterConfigsOptions, AlterConfigsResource, AlterConfigsResourceResult,
    AlterConfigsResponseData, AlterPartitionReassignmentsOptions,
    AlterPartitionReassignmentsPartitionResult, AlterPartitionReassignmentsResponseData,
    AlterPartitionReassignmentsTopicResult, AlterReplicaLogDir, AlterReplicaLogDirPartitionResult,
    AlterReplicaLogDirTopic, AlterReplicaLogDirTopicResult, AlterReplicaLogDirsResponseData,
    AlterShareGroupOffsetPartition, AlterShareGroupOffsetPartitionResult,
    AlterShareGroupOffsetTopic, AlterShareGroupOffsetTopicResult,
    AlterShareGroupOffsetsResponseData, AlterUserScramCredentialResult,
    AlterUserScramCredentialsOptions, AlterUserScramCredentialsResponseData,
    AssignReplicasToDirsOptions, AssignReplicasToDirsResponseData,
    CLIENT_QUOTA_MATCH_ANY_SPECIFIED, CLIENT_QUOTA_MATCH_DEFAULT, CLIENT_QUOTA_MATCH_EXACT,
    CONFIG_OPERATION_APPEND, CONFIG_OPERATION_DELETE, CONFIG_OPERATION_SET,
    CONFIG_OPERATION_SUBTRACT, CONFIG_RESOURCE_TYPE_BROKER, CONFIG_RESOURCE_TYPE_BROKER_LOGGER,
    CONFIG_RESOURCE_TYPE_TOPIC, ClientQuotaAlteration, ClientQuotaAlterationOp, ClientQuotaEntity,
    ClientQuotaEntityFilter, ClientQuotaEntitySpec, ClientQuotaEntry, ClientQuotaValue,
    ClusterBroker, ConfigEntry, ConfigResource, ConfigSynonym, ConsumerGroupAssignment,
    ConsumerGroupDescribeResponseData, ConsumerGroupDescription, ConsumerGroupMemberDescription,
    ConsumerGroupTopicPartitions, CreateAclResult, CreateAclsResponseData,
    CreateDelegationTokenOptions, CreateDelegationTokenResponseData, CreatePartitionsOptions,
    CreatePartitionsResponseData, CreatePartitionsTopicResult, CreatePartitionsTopicSpec,
    DelegationTokenDescription, DeleteAclsFilterResult, DeleteAclsResponseData,
    DeleteGroupsResponseData, DeleteRecordsPartitionResult, DeleteRecordsPartitionSpec,
    DeleteRecordsResponseData, DeleteRecordsTopicResult, DeleteRecordsTopicSpec,
    DeleteShareGroupOffsetTopic, DeleteShareGroupOffsetTopicResult,
    DeleteShareGroupOffsetsResponseData, DeletedAcl, DeletedGroup, DescribeAclsFilter,
    DescribeAclsResponseData, DescribeClientQuotasOptions, DescribeClientQuotasResponseData,
    DescribeClusterResponseData, DescribeConfigsResponseData, DescribeConfigsResult,
    DescribeDelegationTokenResponseData, DescribeGroupsResponseData, DescribeLogDirsResponseData,
    DescribeProducersResponseData, DescribeQuorumResponseData,
    DescribeShareGroupOffsetsResponseData, DescribeTopicPartitionsOptions,
    DescribeTopicPartitionsResponseData, DescribeTransactionsResponseData,
    DescribeUserScramCredentialsResponseData, DescribedGroup, DescribedGroupMember,
    DescribedTopicPartition, DescribedTopicPartitionsTopic, DescribedTransaction,
    ELECTION_TYPE_PREFERRED, ELECTION_TYPE_UNCLEAN, ElectLeadersOptions,
    ElectLeadersPartitionResult, ElectLeadersResponseData, ElectLeadersTopicResult,
    ExpireDelegationTokenResponseData, FEATURE_UPGRADE_TYPE_SAFE_DOWNGRADE,
    FEATURE_UPGRADE_TYPE_UNSAFE_DOWNGRADE, FEATURE_UPGRADE_TYPE_UPGRADE, FeatureUpdate,
    IncrementalAlterConfig, IncrementalAlterConfigsOptions, IncrementalAlterConfigsResource,
    IncrementalAlterConfigsResourceResult, IncrementalAlterConfigsResponseData, KafkaPrincipal,
    LeaderEpochPartitionOffset, LeaderEpochPartitionRequest, LeaderEpochTopicOffsets,
    LeaderEpochTopicRequest, ListConfigResourcesResponseData, ListGroupsResponseData,
    ListPartitionReassignmentsResponseData, ListTransactionsOptions, ListTransactionsResponseData,
    ListedConfigResource, ListedGroup, ListedTransaction, LogDirDescription, LogDirPartition,
    LogDirTopic, OffsetDeletePartitionResult, OffsetDeleteResponseData, OffsetDeleteTopicResult,
    OffsetForLeaderEpochResponseData, PartitionReassignment, PartitionReassignmentSpec,
    PartitionReassignmentTopicSpec, ProducerPartition, ProducerTopic, QuorumListener, QuorumNode,
    QuorumPartition, QuorumReplicaState, QuorumTopic, RaftVersionFeature, RaftVoterCurrentLeader,
    RaftVoterListener, RaftVoterResponseData, RemoveRaftVoterOptions,
    RenewDelegationTokenResponseData, ReplicaDirectoryAssignment, ReplicaDirectoryAssignmentResult,
    ReplicaDirectoryPartitionResult, ReplicaDirectoryTopicAssignment, ReplicaDirectoryTopicResult,
    SCRAM_MECHANISM_SHA_256, SCRAM_MECHANISM_SHA_512, ScramCredentialDeletion, ScramCredentialInfo,
    ScramCredentialUpsertion, ShareGroupAssignment, ShareGroupDescribeResponseData,
    ShareGroupDescription, ShareGroupMemberDescription, ShareGroupOffsetGroup,
    ShareGroupOffsetPartition, ShareGroupOffsetRequest, ShareGroupOffsetTopic,
    ShareGroupTopicPartitions, TopicPartitionFilter, TopicPartitionsCursor, TopicReassignment,
    TransactionTopic, TxnOffsetCommitPartitionResult, TxnOffsetCommitResponseData,
    TxnOffsetCommitTopicPartition, TxnOffsetCommitTopicResult, UnregisterBrokerResponseData,
    UpdateFeaturesResponseData, UpdateFeaturesResult, UpdateRaftVoterOptions,
    UpdateRaftVoterResponseData, UserScramCredentialsDescription,
};
pub use crate::protocol::api_versions::{
    ApiVersionCache, ApiVersions, ApiVersionsResponseData, BrokerApiVersion, api_key,
};
pub use crate::protocol::create_topics::{CreateTopicsResponseData, TopicConfig, TopicResult};
pub use crate::protocol::delete_topics::{DeleteTopicResult, DeleteTopicsResponseData};
#[cfg(feature = "producer_timestamp")]
pub use crate::protocol::produce::ProducerTimestamp;
pub use crate::protocol::share_consumer::{
    ConsumerGroupHeartbeatOptions, ConsumerGroupHeartbeatResponseData, ForgottenShareFetchTopic,
    HeartbeatAssignment, HeartbeatTopicPartitions, SHARE_ACK_TYPE_ACCEPT, SHARE_ACK_TYPE_GAP,
    SHARE_ACK_TYPE_REJECT, SHARE_ACK_TYPE_RELEASE, ShareAcknowledgeOptions,
    ShareAcknowledgePartition, ShareAcknowledgePartitionResponse, ShareAcknowledgeResponseData,
    ShareAcknowledgeTopic, ShareAcknowledgeTopicResponse, ShareAcknowledgeTopicResponseData,
    ShareAcknowledgementBatch, ShareAcquiredRecords, ShareAssignment, ShareConsumerSession,
    ShareFetchOptions, ShareFetchPartition, ShareFetchPartitionResponse, ShareFetchResponseData,
    ShareFetchSessionConfig, ShareFetchTopic, ShareFetchTopicResponse, ShareGroupHeartbeatOptions,
    ShareGroupHeartbeatResponseData, ShareHeartbeatResponseData, ShareLeader, ShareNodeEndpoint,
    ShareTopicPartitions,
};
pub use crate::protocol::telemetry::{
    GetTelemetrySubscriptionsOptions, GetTelemetrySubscriptionsResponseData, PushTelemetryOptions,
    PushTelemetryResponseData, TELEMETRY_COMPRESSION_GZIP, TELEMETRY_COMPRESSION_LZ4,
    TELEMETRY_COMPRESSION_NONE, TELEMETRY_COMPRESSION_SNAPPY, TELEMETRY_COMPRESSION_ZSTD,
    TelemetrySession, TelemetrySubscriptionsResponseData,
};
pub use crate::utils::PartitionOffset;

#[cfg(feature = "security")]
pub use crate::network::{SaslConfig, SecurityConfig};
#[cfg(feature = "security")]
pub use crate::tls::TlsConfig;
