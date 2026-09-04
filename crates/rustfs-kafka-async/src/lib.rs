//! Async Kafka client built on top of the tokio runtime.
//!
//! This crate provides native asynchronous Kafka clients built on tokio.
//! It exposes three primary types:
//!
//! - [`AsyncKafkaClient`]: bootstrap and connection management for async code.
//! - [`AsyncProducer`]: an async producer using non-blocking Kafka protocol I/O.
//! - [`AsyncProducerBuilder`]: async builder for configuring and creating an
//!   `AsyncProducer` without blocking the tokio scheduler.
//! - [`AsyncConsumer`]: an async consumer using non-blocking Kafka protocol I/O.
//! - [`AsyncConsumerBuilder`]: async builder for configuring and creating an
//!   `AsyncConsumer` without blocking the tokio scheduler.
//!
//! # Example
//!
//! ```no_run
//! use rustfs_kafka_async::{AsyncKafkaClient, AsyncProducer};
//! use rustfs_kafka::producer::Record;
//!
//! #[tokio::main]
//! async fn main() -> rustfs_kafka::error::Result<()> {
//!     // Create an async client from bootstrap hosts
//!     let client = AsyncKafkaClient::new(vec!["localhost:9092".to_owned()]).await?;
//!     // Create an async producer which manages a background task
//!     let mut producer = AsyncProducer::new(client).await?;
//!
//!     // Send a single message and close the producer
//!     producer.send(&Record::from_value("test-topic", &b"hello"[..])).await?;
//!     producer.close().await?;
//!     Ok(())
//! }
//! ```

mod client;
mod connection;
mod consumer;
mod consumer_observability;
mod metrics;
mod producer;
mod wire;

pub use client::AsyncKafkaClient;
pub use consumer::{AsyncConsumer, AsyncConsumerBuilder};
pub use consumer_observability::{NativeConsumerErrorSnapshot, NativeConsumerErrorStats};
pub use producer::{AsyncProducer, AsyncProducerBuilder, AsyncProducerConfig};

// Re-export core types from the sync crate for convenience
pub use rustfs_kafka::client::{
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
    AlterUserScramCredentialsOptions, AlterUserScramCredentialsResponseData, ApiVersionCache,
    ApiVersions, ApiVersionsResponseData, AssignReplicasToDirsOptions,
    AssignReplicasToDirsResponseData, BrokerApiVersion, CLIENT_QUOTA_MATCH_ANY_SPECIFIED,
    CLIENT_QUOTA_MATCH_DEFAULT, CLIENT_QUOTA_MATCH_EXACT, CONFIG_OPERATION_APPEND,
    CONFIG_OPERATION_DELETE, CONFIG_OPERATION_SET, CONFIG_OPERATION_SUBTRACT,
    CONFIG_RESOURCE_TYPE_BROKER, CONFIG_RESOURCE_TYPE_BROKER_LOGGER, CONFIG_RESOURCE_TYPE_TOPIC,
    ClientQuotaAlteration, ClientQuotaAlterationOp, ClientQuotaEntity, ClientQuotaEntityFilter,
    ClientQuotaEntitySpec, ClientQuotaEntry, ClientQuotaValue, ClusterBroker, ConfigEntry,
    ConfigResource, ConfigSynonym, ConsumerGroupAssignment, ConsumerGroupDescribeResponseData,
    ConsumerGroupDescription, ConsumerGroupHeartbeatOptions, ConsumerGroupHeartbeatResponseData,
    ConsumerGroupMemberDescription, ConsumerGroupTopicPartitions, CreateAclResult,
    CreateAclsResponseData, CreateDelegationTokenOptions, CreateDelegationTokenResponseData,
    CreatePartitionsOptions, CreatePartitionsResponseData, CreatePartitionsTopicResult,
    CreatePartitionsTopicSpec, CreateTopicsResponseData, DelegationTokenDescription,
    DeleteAclsFilterResult, DeleteAclsResponseData, DeleteGroupsResponseData,
    DeleteRecordsPartitionResult, DeleteRecordsPartitionSpec, DeleteRecordsResponseData,
    DeleteRecordsTopicResult, DeleteRecordsTopicSpec, DeleteShareGroupOffsetTopic,
    DeleteShareGroupOffsetTopicResult, DeleteShareGroupOffsetsResponseData, DeleteTopicResult,
    DeleteTopicsResponseData, DeletedAcl, DeletedGroup, DescribeAclsFilter,
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
    ForgottenShareFetchTopic, HeartbeatAssignment, HeartbeatTopicPartitions,
    IncrementalAlterConfig, IncrementalAlterConfigsOptions, IncrementalAlterConfigsResource,
    IncrementalAlterConfigsResourceResult, IncrementalAlterConfigsResponseData, KafkaPrincipal,
    LeaderEpochPartitionOffset, LeaderEpochPartitionRequest, LeaderEpochTopicOffsets,
    LeaderEpochTopicRequest, ListConfigResourcesResponseData, ListGroupsResponseData,
    ListPartitionReassignmentsResponseData, ListTransactionsOptions, ListTransactionsResponseData,
    ListedConfigResource, ListedGroup, ListedTransaction, LogDirDescription, LogDirPartition,
    LogDirTopic, OffsetDeletePartitionResult, OffsetDeleteResponseData, OffsetDeleteTopicResult,
    OffsetForLeaderEpochResponseData, PartitionReassignment, PartitionReassignmentSpec,
    PartitionReassignmentTopicSpec, ProducerPartition, ProducerTopic, PushTelemetryOptions,
    PushTelemetryResponseData, QuorumListener, QuorumNode, QuorumPartition, QuorumReplicaState,
    QuorumTopic, RaftVersionFeature, RaftVoterCurrentLeader, RaftVoterListener,
    RaftVoterResponseData, RemoveRaftVoterOptions, RenewDelegationTokenResponseData,
    ReplicaDirectoryAssignment, ReplicaDirectoryAssignmentResult, ReplicaDirectoryPartitionResult,
    ReplicaDirectoryTopicAssignment, ReplicaDirectoryTopicResult, RequiredAcks,
    SCRAM_MECHANISM_SHA_256, SCRAM_MECHANISM_SHA_512, SHARE_ACK_TYPE_ACCEPT, SHARE_ACK_TYPE_GAP,
    SHARE_ACK_TYPE_REJECT, SHARE_ACK_TYPE_RELEASE, SaslConfig, ScramCredentialDeletion,
    ScramCredentialInfo, ScramCredentialUpsertion, SecurityConfig, ShareAcknowledgeOptions,
    ShareAcknowledgePartition, ShareAcknowledgePartitionResponse, ShareAcknowledgeResponseData,
    ShareAcknowledgeTopic, ShareAcknowledgeTopicResponse, ShareAcknowledgeTopicResponseData,
    ShareAcknowledgementBatch, ShareAcquiredRecords, ShareAssignment, ShareConsumerSession,
    ShareFetchOptions, ShareFetchPartition, ShareFetchPartitionResponse, ShareFetchResponseData,
    ShareFetchSessionConfig, ShareFetchTopic, ShareFetchTopicResponse, ShareGroupAssignment,
    ShareGroupDescribeResponseData, ShareGroupDescription, ShareGroupHeartbeatOptions,
    ShareGroupHeartbeatResponseData, ShareGroupMemberDescription, ShareGroupOffsetGroup,
    ShareGroupOffsetPartition, ShareGroupOffsetRequest, ShareGroupOffsetTopic,
    ShareGroupTopicPartitions, ShareHeartbeatResponseData, ShareLeader, ShareNodeEndpoint,
    ShareTopicPartitions, TELEMETRY_COMPRESSION_GZIP, TELEMETRY_COMPRESSION_LZ4,
    TELEMETRY_COMPRESSION_NONE, TELEMETRY_COMPRESSION_SNAPPY, TELEMETRY_COMPRESSION_ZSTD,
    TelemetrySession, TelemetrySubscriptionsResponseData, TlsConfig, TopicConfig,
    TopicPartitionFilter, TopicPartitionsCursor, TopicReassignment, TransactionTopic,
    TxnOffsetCommitPartitionResult, TxnOffsetCommitResponseData, TxnOffsetCommitTopicPartition,
    TxnOffsetCommitTopicResult, UnregisterBrokerResponseData, UpdateFeaturesResponseData,
    UpdateFeaturesResult, UpdateRaftVoterOptions, UpdateRaftVoterResponseData,
    UserScramCredentialsDescription, api_key, build_create_topics_protocol_request,
    build_create_topics_request, build_delete_topics_protocol_request, build_delete_topics_request,
    convert_create_topics_response, convert_delete_topics_response,
};
pub use rustfs_kafka::error;
pub use rustfs_kafka::kafka_protocol;
pub use rustfs_kafka::producer::{AsBytes, Headers, Record};

#[cfg(test)]
mod public_reexports_tests {
    use super::*;

    #[test]
    fn admin_mutation_reexports_are_constructible() {
        let configs =
            IncrementalAlterConfigsOptions::new([IncrementalAlterConfigsResource::topic(
                "topic-a",
                [
                    IncrementalAlterConfig::set("retention.ms", "60000"),
                    IncrementalAlterConfig::delete("cleanup.policy"),
                ],
            )])
            .with_validate_only(true);
        assert!(configs.validate_only);

        let quota = AlterClientQuotasOptions::new([ClientQuotaAlteration::new(
            [
                ClientQuotaEntitySpec::named("user", "alice"),
                ClientQuotaEntitySpec::default_entity("client-id"),
            ],
            [
                ClientQuotaAlterationOp::set("producer_byte_rate", 1024.5),
                ClientQuotaAlterationOp::remove("consumer_byte_rate"),
            ],
        )])
        .with_validate_only(true);
        assert!(quota.validate_only);

        let create_partitions =
            CreatePartitionsOptions::new([CreatePartitionsTopicSpec::new("topic-a", 6)])
                .with_validate_only(true);
        assert_eq!(create_partitions.topics[0].count, 6);

        let reassignment =
            AlterPartitionReassignmentsOptions::new([PartitionReassignmentTopicSpec::new(
                "topic-a",
                [
                    PartitionReassignmentSpec::new(0, [1, 2]),
                    PartitionReassignmentSpec::cancel(1),
                ],
            )]);
        assert!(reassignment.topics[0].partitions[1].replicas.is_none());

        let leader_epoch =
            LeaderEpochTopicRequest::new("topic-a", [LeaderEpochPartitionRequest::new(0, -1, 7)]);
        assert_eq!(leader_epoch.partitions[0].leader_epoch, 7);

        let alter_configs = AlterConfigsOptions::new([AlterConfigsResource::topic(
            "topic-a",
            [AlterConfigsEntry::new("retention.ms", "60000")],
        )])
        .with_validate_only(true);
        assert!(alter_configs.validate_only);

        let log_dirs = AlterReplicaLogDir::new(
            "/kafka-logs-2",
            vec![AlterReplicaLogDirTopic::new("topic-a", [0, 1])],
        );
        assert_eq!(log_dirs.topics[0].partitions, vec![0, 1]);

        let token = CreateDelegationTokenOptions::new()
            .with_owner(KafkaPrincipal::user("alice"))
            .with_renewer(KafkaPrincipal::user("bob"))
            .with_max_lifetime_ms(60_000);
        assert_eq!(token.renewers[0].principal_name, "bob");

        let scram = AlterUserScramCredentialsOptions::new()
            .with_deletion(ScramCredentialDeletion::new(
                "old-user",
                SCRAM_MECHANISM_SHA_256,
            ))
            .with_upsertion(ScramCredentialUpsertion::new(
                "new-user",
                SCRAM_MECHANISM_SHA_512,
                8192,
                bytes::Bytes::from_static(b"salt"),
                bytes::Bytes::from_static(b"salted-password"),
            ));
        assert_eq!(scram.upsertions[0].iterations, 8192);

        let txn_offset =
            TxnOffsetCommitTopicPartition::new("topic-a", 0, 42).with_metadata("metadata");
        assert_eq!(txn_offset.metadata.as_deref(), Some("metadata"));

        let feature_update = FeatureUpdate::safe_downgrade("metadata.version", 20);
        assert_eq!(
            feature_update.upgrade_type,
            FEATURE_UPGRADE_TYPE_SAFE_DOWNGRADE
        );

        let directory_id = uuid::Uuid::from_u128(1);
        let topic_id = uuid::Uuid::from_u128(2);
        let assignment = ReplicaDirectoryAssignment::new(
            directory_id,
            [ReplicaDirectoryTopicAssignment::new(topic_id, [0, 1])],
        );
        let assign_dirs = AssignReplicasToDirsOptions::new(1, 10, [assignment]);
        assert_eq!(assign_dirs.directories[0].directory_id, directory_id);

        let listener = RaftVoterListener::new("CONTROLLER", "controller-1", 9093);
        let add_voter =
            AddRaftVoterOptions::new(2, directory_id, [listener.clone()]).with_timeout_ms(30_000);
        assert_eq!(add_voter.listeners[0].port, 9093);

        let remove_voter = RemoveRaftVoterOptions::new(2, directory_id);
        assert_eq!(remove_voter.voter_id, 2);

        let update_voter =
            UpdateRaftVoterOptions::new(2, directory_id, [listener], RaftVersionFeature::new(1, 3));
        assert_eq!(update_voter.raft_version_feature.max_supported_version, 3);

        let telemetry = PushTelemetryOptions::new(
            directory_id,
            7,
            bytes::Bytes::from_static(b"encoded-metrics"),
        )
        .with_compression_type(TELEMETRY_COMPRESSION_ZSTD)
        .with_terminating(true);
        assert!(telemetry.terminating);
        assert_eq!(telemetry.compression_type, TELEMETRY_COMPRESSION_ZSTD);

        let heartbeat = ConsumerGroupHeartbeatOptions::new("consumer-group", "member-a");
        assert_eq!(heartbeat.member_epoch, 0);

        let share_fetch =
            ShareFetchOptions::new("share-group", "member-a").with_topics([ShareFetchTopic::new(
                topic_id,
                [ShareFetchPartition::new(
                    0,
                    [ShareAcknowledgementBatch::new(
                        0,
                        0,
                        [SHARE_ACK_TYPE_ACCEPT],
                    )],
                )],
            )]);
        assert_eq!(share_fetch.topics[0].partitions[0].partition_index, 0);

        let share_alter = AlterShareGroupOffsetTopic::new(
            "topic-a",
            [AlterShareGroupOffsetPartition::new(0, 42)],
        );
        assert_eq!(share_alter.partitions[0].offset, 42);

        let share_delete = DeleteShareGroupOffsetTopic::new("topic-a");
        assert_eq!(share_delete.topic_name, "topic-a");

        let fallback_snapshot = ApiVersionCache::fallback_version(api_key::FETCH_SNAPSHOT);
        assert_eq!(fallback_snapshot, ApiVersions::default().fetch_snapshot);
        let _generated_request = kafka_protocol::messages::FetchSnapshotRequest::default();

        let telemetry_session = TelemetrySession::initial();
        assert_eq!(telemetry_session.subscription_id, 0);

        let share_session = ShareConsumerSession::new("share-group", "member-a")
            .with_fetch_config(ShareFetchSessionConfig::default());
        assert_eq!(share_session.heartbeat_options().group_id, "share-group");
    }
}
