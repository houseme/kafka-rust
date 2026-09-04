//! `KafkaClient` admin and generated protocol operations.

use std::time::Duration;

use super::{
    AclBinding, AddOffsetsToTxnResponseData, AddRaftVoterOptions, AlterClientQuotasOptions,
    AlterClientQuotasResponseData, AlterConfigsOptions, AlterConfigsResponseData,
    AlterPartitionReassignmentsOptions, AlterPartitionReassignmentsResponseData,
    AlterReplicaLogDir, AlterReplicaLogDirsResponseData, AlterShareGroupOffsetTopic,
    AlterShareGroupOffsetsResponseData, AlterUserScramCredentialsOptions,
    AlterUserScramCredentialsResponseData, ApiVersionsResponseData, AssignReplicasToDirsOptions,
    AssignReplicasToDirsResponseData, ConfigResource, ConsumerGroupDescribeResponseData,
    ConsumerGroupHeartbeatOptions, ConsumerGroupHeartbeatResponseData, CreateAclsResponseData,
    CreateDelegationTokenOptions, CreateDelegationTokenResponseData, CreatePartitionsOptions,
    CreatePartitionsResponseData, CreatePartitionsTopicSpec, DeleteAclsResponseData,
    DeleteGroupsResponseData, DeleteRecordsResponseData, DeleteRecordsTopicSpec,
    DeleteShareGroupOffsetTopic, DeleteShareGroupOffsetsResponseData, DescribeAclsFilter,
    DescribeAclsResponseData, DescribeClientQuotasOptions, DescribeClientQuotasResponseData,
    DescribeClusterResponseData, DescribeConfigsResponseData, DescribeDelegationTokenResponseData,
    DescribeGroupsResponseData, DescribeLogDirsResponseData, DescribeProducersResponseData,
    DescribeQuorumResponseData, DescribeShareGroupOffsetsResponseData,
    DescribeTopicPartitionsOptions, DescribeTopicPartitionsResponseData,
    DescribeTransactionsResponseData, DescribeUserScramCredentialsResponseData,
    ELECTION_TYPE_PREFERRED, ELECTION_TYPE_UNCLEAN, ElectLeadersOptions, ElectLeadersResponseData,
    Error, ExpireDelegationTokenResponseData, FeatureUpdate, GetTelemetrySubscriptionsOptions,
    IncrementalAlterConfigsOptions, IncrementalAlterConfigsResponseData, KafkaClient,
    KafkaPrincipal, LeaderEpochTopicRequest, ListConfigResourcesResponseData,
    ListGroupsResponseData, ListPartitionReassignmentsResponseData, ListTransactionsOptions,
    ListTransactionsResponseData, OffsetForLeaderEpochResponseData, PushTelemetryOptions,
    PushTelemetryResponseData, RaftVoterResponseData, RemoveRaftVoterOptions,
    RenewDelegationTokenResponseData, Result, ShareAcknowledgeOptions,
    ShareAcknowledgeResponseData, ShareFetchOptions, ShareFetchResponseData,
    ShareGroupDescribeResponseData, ShareGroupHeartbeatOptions, ShareGroupHeartbeatResponseData,
    ShareGroupOffsetRequest, TelemetrySubscriptionsResponseData, TopicPartitionFilter,
    TxnOffsetCommitResponseData, TxnOffsetCommitTopicPartition, UnregisterBrokerResponseData,
    UpdateFeaturesResponseData, UpdateRaftVoterOptions, UpdateRaftVoterResponseData, protocol,
};

impl KafkaClient {
    /// Fetches the Kafka API version ranges advertised by a broker.
    ///
    /// The request is attempted against configured brokers until one succeeds. On success,
    /// the client's internal version cache is refreshed for the responding broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn fetch_api_versions(&mut self) -> Result<ApiVersionsResponseData> {
        let correlation_id = self.state.next_correlation_id();
        let now = std::time::Instant::now();
        let hosts = self.config.hosts.clone();
        let mut last_err: Option<Error> = None;

        for host in hosts {
            let conn = match self.conn_pool.get_conn(&host, now) {
                Ok(conn) => conn,
                Err(e) => {
                    last_err = Some(e.with_broker_context(&host, "ApiVersions"));
                    continue;
                }
            };

            match protocol::api_versions::fetch_api_versions_data(
                conn,
                correlation_id,
                &self.config.client_id,
            ) {
                Ok(resp) => {
                    self.api_versions.insert_api_versions(host, &resp.api_keys);
                    return Ok(resp);
                }
                Err(e) => last_err = Some(e.with_broker_context(&host, "ApiVersions")),
            }
        }

        Err(last_err.unwrap_or_else(Error::no_host_reachable))
    }

    /// Fetches broker-side client telemetry subscription settings.
    ///
    /// The returned subscription ID, compression choices, interval, and metric
    /// filters are intended to drive subsequent `push_telemetry` calls.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn get_telemetry_subscriptions(
        &mut self,
        client_instance_id: uuid::Uuid,
    ) -> Result<TelemetrySubscriptionsResponseData> {
        self.try_admin_request(
            "GetTelemetrySubscriptions",
            protocol::API_VERSION_GET_TELEMETRY_SUBSCRIPTIONS,
            |correlation_id, client_id| {
                protocol::telemetry::build_get_telemetry_subscriptions_request(
                    correlation_id,
                    client_id,
                    GetTelemetrySubscriptionsOptions::for_client_instance(client_instance_id),
                )
            },
            protocol::telemetry::convert_get_telemetry_subscriptions_response,
        )
    }

    /// Pushes an encoded client telemetry payload to a broker.
    ///
    /// This low-level API does not encode metrics itself; callers should pass a
    /// payload that matches the broker's telemetry subscription requirements.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn push_telemetry(
        &mut self,
        options: &PushTelemetryOptions,
    ) -> Result<PushTelemetryResponseData> {
        self.try_admin_request(
            "PushTelemetry",
            protocol::API_VERSION_PUSH_TELEMETRY,
            |correlation_id, client_id| {
                protocol::telemetry::build_push_telemetry_request(
                    correlation_id,
                    client_id,
                    options,
                )
            },
            protocol::telemetry::convert_push_telemetry_response,
        )
    }

    /// Sends a low-level modern consumer-group heartbeat.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn consumer_group_heartbeat(
        &mut self,
        options: &ConsumerGroupHeartbeatOptions,
    ) -> Result<ConsumerGroupHeartbeatResponseData> {
        self.try_admin_request(
            "ConsumerGroupHeartbeat",
            protocol::API_VERSION_CONSUMER_GROUP_HEARTBEAT,
            |correlation_id, client_id| {
                protocol::share_consumer::build_consumer_group_heartbeat_request(
                    correlation_id,
                    client_id,
                    options,
                )
            },
            protocol::share_consumer::convert_consumer_group_heartbeat_response,
        )
    }

    /// Sends a low-level share-group heartbeat.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn share_group_heartbeat(
        &mut self,
        options: &ShareGroupHeartbeatOptions,
    ) -> Result<ShareGroupHeartbeatResponseData> {
        self.try_admin_request(
            "ShareGroupHeartbeat",
            protocol::API_VERSION_SHARE_GROUP_HEARTBEAT,
            |correlation_id, client_id| {
                protocol::share_consumer::build_share_group_heartbeat_request(
                    correlation_id,
                    client_id,
                    options,
                )
            },
            protocol::share_consumer::convert_share_group_heartbeat_response,
        )
    }

    /// Sends a low-level share fetch request.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn share_fetch(&mut self, options: &ShareFetchOptions) -> Result<ShareFetchResponseData> {
        self.try_admin_request(
            "ShareFetch",
            protocol::API_VERSION_SHARE_FETCH,
            |correlation_id, client_id| {
                protocol::share_consumer::build_share_fetch_request(
                    correlation_id,
                    client_id,
                    options,
                )
            },
            protocol::share_consumer::convert_share_fetch_response,
        )
    }

    /// Sends a low-level share acknowledgement request.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn share_acknowledge(
        &mut self,
        options: &ShareAcknowledgeOptions,
    ) -> Result<ShareAcknowledgeResponseData> {
        self.try_admin_request(
            "ShareAcknowledge",
            protocol::API_VERSION_SHARE_ACKNOWLEDGE,
            |correlation_id, client_id| {
                protocol::share_consumer::build_share_acknowledge_request(
                    correlation_id,
                    client_id,
                    options,
                )
            },
            protocol::share_consumer::convert_share_acknowledge_response,
        )
    }

    /// Describes the Kafka cluster, including cluster ID, controller ID, and brokers.
    ///
    /// The request is attempted against configured brokers until one succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_cluster(&mut self) -> Result<DescribeClusterResponseData> {
        self.describe_cluster_with_options(false, false)
    }

    /// Describes the Kafka cluster with optional authorized-operation and fenced-broker fields.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_cluster_with_options(
        &mut self,
        include_authorized_operations: bool,
        include_fenced_brokers: bool,
    ) -> Result<DescribeClusterResponseData> {
        self.try_admin_request(
            "DescribeCluster",
            protocol::API_VERSION_DESCRIBE_CLUSTER,
            |cid, cid_str| {
                protocol::admin::build_describe_cluster_request(
                    cid,
                    cid_str,
                    include_authorized_operations,
                    include_fenced_brokers,
                )
            },
            protocol::admin::convert_describe_cluster_response,
        )
    }

    /// Describes ACLs visible to the contacted broker.
    ///
    /// By default this matches all ACL resources, operations, and permission types.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_acls(&mut self) -> Result<DescribeAclsResponseData> {
        self.describe_acls_with_filter(&DescribeAclsFilter::default())
    }

    /// Describes ACLs using Kafka resource, principal, host, operation, or permission filters.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_acls_with_filter(
        &mut self,
        filter: &DescribeAclsFilter,
    ) -> Result<DescribeAclsResponseData> {
        self.try_admin_request(
            "DescribeAcls",
            protocol::API_VERSION_DESCRIBE_ACLS,
            |cid, cid_str| protocol::admin::build_describe_acls_request(cid, cid_str, filter),
            protocol::admin::convert_describe_acls_response,
        )
    }

    /// Creates ACL bindings on the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn create_acls(&mut self, bindings: &[AclBinding]) -> Result<CreateAclsResponseData> {
        self.try_admin_request(
            "CreateAcls",
            protocol::API_VERSION_CREATE_ACLS,
            |cid, cid_str| protocol::admin::build_create_acls_request(cid, cid_str, bindings),
            protocol::admin::convert_create_acls_response,
        )
    }

    /// Deletes ACLs matching the supplied filters.
    ///
    /// Each filter may match multiple ACL bindings.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn delete_acls(
        &mut self,
        filters: &[DescribeAclsFilter],
    ) -> Result<DeleteAclsResponseData> {
        self.try_admin_request(
            "DeleteAcls",
            protocol::API_VERSION_DELETE_ACLS,
            |cid, cid_str| protocol::admin::build_delete_acls_request(cid, cid_str, filters),
            protocol::admin::convert_delete_acls_response,
        )
    }

    /// Describes Kafka topic, broker, or broker logger configs.
    ///
    /// By default this fetches all config keys without synonyms or documentation.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_configs(
        &mut self,
        resources: &[ConfigResource],
    ) -> Result<DescribeConfigsResponseData> {
        self.describe_configs_with_options(resources, false, false)
    }

    /// Describes Kafka configs with optional synonyms and broker documentation.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_configs_with_options(
        &mut self,
        resources: &[ConfigResource],
        include_synonyms: bool,
        include_documentation: bool,
    ) -> Result<DescribeConfigsResponseData> {
        self.try_admin_request(
            "DescribeConfigs",
            protocol::API_VERSION_DESCRIBE_CONFIGS,
            |cid, cid_str| {
                protocol::admin::build_describe_configs_request(
                    cid,
                    cid_str,
                    resources,
                    include_synonyms,
                    include_documentation,
                )
            },
            protocol::admin::convert_describe_configs_response,
        )
    }

    /// Applies incremental config changes to Kafka topic, broker, or broker logger resources.
    ///
    /// Prefer this API over Kafka's legacy whole-resource `AlterConfigs` protocol.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn incremental_alter_configs(
        &mut self,
        options: &IncrementalAlterConfigsOptions,
    ) -> Result<IncrementalAlterConfigsResponseData> {
        self.try_admin_request(
            "IncrementalAlterConfigs",
            protocol::API_VERSION_INCREMENTAL_ALTER_CONFIGS,
            |cid, cid_str| {
                protocol::admin::build_incremental_alter_configs_request(cid, cid_str, options)
            },
            protocol::admin::convert_incremental_alter_configs_response,
        )
    }

    /// Alters broker or topic configs with Kafka's legacy whole-resource `AlterConfigs` API.
    ///
    /// Prefer [`incremental_alter_configs`](Self::incremental_alter_configs) for new code.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    #[deprecated(
        since = "1.2.0",
        note = "use incremental_alter_configs for incremental config mutation"
    )]
    pub fn alter_configs(
        &mut self,
        options: &AlterConfigsOptions,
    ) -> Result<AlterConfigsResponseData> {
        self.try_admin_request(
            "AlterConfigs",
            protocol::API_VERSION_ALTER_CONFIGS,
            |cid, cid_str| protocol::admin::build_alter_configs_request(cid, cid_str, options),
            protocol::admin::convert_alter_configs_response,
        )
    }

    /// Moves selected replicas to broker log directories.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn alter_replica_log_dirs(
        &mut self,
        dirs: &[AlterReplicaLogDir],
    ) -> Result<AlterReplicaLogDirsResponseData> {
        self.try_admin_request(
            "AlterReplicaLogDirs",
            protocol::API_VERSION_ALTER_REPLICA_LOG_DIRS,
            |cid, cid_str| {
                protocol::admin::build_alter_replica_log_dirs_request(cid, cid_str, dirs)
            },
            protocol::admin::convert_alter_replica_log_dirs_response,
        )
    }

    /// Describes all delegation tokens visible to the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_delegation_tokens(&mut self) -> Result<DescribeDelegationTokenResponseData> {
        self.describe_delegation_tokens_with_owners(None)
    }

    /// Describes delegation tokens owned by selected principals.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_delegation_tokens_for(
        &mut self,
        owners: &[KafkaPrincipal],
    ) -> Result<DescribeDelegationTokenResponseData> {
        self.describe_delegation_tokens_with_owners(Some(owners))
    }

    fn describe_delegation_tokens_with_owners(
        &mut self,
        owners: Option<&[KafkaPrincipal]>,
    ) -> Result<DescribeDelegationTokenResponseData> {
        self.try_admin_request(
            "DescribeDelegationToken",
            protocol::API_VERSION_DESCRIBE_DELEGATION_TOKEN,
            |cid, cid_str| {
                protocol::admin::build_describe_delegation_token_request(cid, cid_str, owners)
            },
            protocol::admin::convert_describe_delegation_token_response,
        )
    }

    /// Creates a Kafka delegation token.
    ///
    /// The returned HMAC is sensitive credential material and can be passed to
    /// [`renew_delegation_token`](Self::renew_delegation_token) or
    /// [`expire_delegation_token`](Self::expire_delegation_token).
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn create_delegation_token(
        &mut self,
        options: &CreateDelegationTokenOptions,
    ) -> Result<CreateDelegationTokenResponseData> {
        self.try_admin_request(
            "CreateDelegationToken",
            protocol::API_VERSION_CREATE_DELEGATION_TOKEN,
            |cid, cid_str| {
                protocol::admin::build_create_delegation_token_request(cid, cid_str, options)
            },
            protocol::admin::convert_create_delegation_token_response,
        )
    }

    /// Renews a Kafka delegation token by HMAC.
    ///
    /// # Errors
    ///
    /// Returns an error if the duration cannot fit Kafka's millisecond field,
    /// brokers are unreachable, or the broker response cannot be decoded.
    pub fn renew_delegation_token(
        &mut self,
        hmac: &[u8],
        renew_period: Duration,
    ) -> Result<RenewDelegationTokenResponseData> {
        let renew_period_ms = protocol::to_millis_i64(renew_period)?;
        let hmac_bytes = bytes::Bytes::copy_from_slice(hmac);
        self.try_admin_request(
            "RenewDelegationToken",
            protocol::API_VERSION_RENEW_DELEGATION_TOKEN,
            |cid, cid_str| {
                protocol::admin::build_renew_delegation_token_request(
                    cid,
                    cid_str,
                    hmac_bytes.clone(),
                    renew_period_ms,
                )
            },
            |resp| protocol::admin::convert_renew_delegation_token_response(&resp),
        )
    }

    /// Expires a Kafka delegation token by HMAC.
    ///
    /// # Errors
    ///
    /// Returns an error if the duration cannot fit Kafka's millisecond field,
    /// brokers are unreachable, or the broker response cannot be decoded.
    pub fn expire_delegation_token(
        &mut self,
        hmac: &[u8],
        expiry_period: Duration,
    ) -> Result<ExpireDelegationTokenResponseData> {
        let expiry_period_ms = protocol::to_millis_i64(expiry_period)?;
        let hmac_bytes = bytes::Bytes::copy_from_slice(hmac);
        self.try_admin_request(
            "ExpireDelegationToken",
            protocol::API_VERSION_EXPIRE_DELEGATION_TOKEN,
            |cid, cid_str| {
                protocol::admin::build_expire_delegation_token_request(
                    cid,
                    cid_str,
                    hmac_bytes.clone(),
                    expiry_period_ms,
                )
            },
            |resp| protocol::admin::convert_expire_delegation_token_response(&resp),
        )
    }

    /// Describes all broker log directories visible to the contacted broker.
    ///
    /// This returns per-log-dir topic and partition storage details, including the
    /// volume capacity fields exposed by Kafka's latest `DescribeLogDirs` version.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_log_dirs(&mut self) -> Result<DescribeLogDirsResponseData> {
        self.describe_log_dirs_with_filter(None)
    }

    /// Describes broker log directories for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_log_dirs_for(
        &mut self,
        topics: &[TopicPartitionFilter],
    ) -> Result<DescribeLogDirsResponseData> {
        self.describe_log_dirs_with_filter(Some(topics))
    }

    fn describe_log_dirs_with_filter(
        &mut self,
        topics: Option<&[TopicPartitionFilter]>,
    ) -> Result<DescribeLogDirsResponseData> {
        self.try_admin_request(
            "DescribeLogDirs",
            protocol::API_VERSION_DESCRIBE_LOG_DIRS,
            |cid, cid_str| protocol::admin::build_describe_log_dirs_request(cid, cid_str, topics),
            protocol::admin::convert_describe_log_dirs_response,
        )
    }

    /// Deletes records before the supplied offsets for selected topic partitions.
    ///
    /// Kafka keeps the partitions and offsets; this advances the partition low watermark
    /// so records before each requested offset are no longer readable.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout is too large, brokers are unreachable, or the
    /// broker response cannot be decoded.
    pub fn delete_records(
        &mut self,
        topics: &[DeleteRecordsTopicSpec],
        timeout: Duration,
    ) -> Result<DeleteRecordsResponseData> {
        let timeout_ms = protocol::to_millis_i32(timeout)?;
        self.try_admin_request(
            "DeleteRecords",
            protocol::API_VERSION_DELETE_RECORDS,
            |cid, cid_str| {
                protocol::admin::build_delete_records_request(cid, cid_str, topics, timeout_ms)
            },
            protocol::admin::convert_delete_records_response,
        )
    }

    /// Lists all ongoing partition reassignments visible to the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout is too large, brokers are unreachable, or the
    /// broker response cannot be decoded.
    pub fn list_partition_reassignments(
        &mut self,
        timeout: Duration,
    ) -> Result<ListPartitionReassignmentsResponseData> {
        self.list_partition_reassignments_with_filter(None, timeout)
    }

    /// Lists ongoing partition reassignments for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout is too large, brokers are unreachable, or the
    /// broker response cannot be decoded.
    pub fn list_partition_reassignments_for(
        &mut self,
        topics: &[TopicPartitionFilter],
        timeout: Duration,
    ) -> Result<ListPartitionReassignmentsResponseData> {
        self.list_partition_reassignments_with_filter(Some(topics), timeout)
    }

    fn list_partition_reassignments_with_filter(
        &mut self,
        topics: Option<&[TopicPartitionFilter]>,
        timeout: Duration,
    ) -> Result<ListPartitionReassignmentsResponseData> {
        let timeout_ms = protocol::to_millis_i32(timeout)?;
        self.try_admin_request(
            "ListPartitionReassignments",
            protocol::API_VERSION_LIST_PARTITION_REASSIGNMENTS,
            |cid, cid_str| {
                protocol::admin::build_list_partition_reassignments_request(
                    cid, cid_str, topics, timeout_ms,
                )
            },
            protocol::admin::convert_list_partition_reassignments_response,
        )
    }

    /// Alters or cancels partition reassignments.
    ///
    /// Use `PartitionReassignmentSpec::new` to assign a new replica set and
    /// `PartitionReassignmentSpec::cancel` to cancel an active reassignment.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn alter_partition_reassignments(
        &mut self,
        options: &AlterPartitionReassignmentsOptions,
    ) -> Result<AlterPartitionReassignmentsResponseData> {
        self.try_admin_request(
            "AlterPartitionReassignments",
            protocol::API_VERSION_ALTER_PARTITION_REASSIGNMENTS,
            |cid, cid_str| {
                protocol::admin::build_alter_partition_reassignments_request(cid, cid_str, options)
            },
            protocol::admin::convert_alter_partition_reassignments_response,
        )
    }

    /// Describes `KRaft` quorum state for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_quorum(
        &mut self,
        topics: &[TopicPartitionFilter],
    ) -> Result<DescribeQuorumResponseData> {
        self.try_admin_request(
            "DescribeQuorum",
            protocol::API_VERSION_DESCRIBE_QUORUM,
            |cid, cid_str| protocol::admin::build_describe_quorum_request(cid, cid_str, topics),
            protocol::admin::convert_describe_quorum_response,
        )
    }

    /// Updates finalized `KRaft` feature levels.
    ///
    /// This is a cluster-wide metadata mutation. Prefer calling with
    /// `validate_only = true` before applying feature upgrades or downgrades.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn update_features(
        &mut self,
        feature_updates: &[FeatureUpdate],
        validate_only: bool,
    ) -> Result<UpdateFeaturesResponseData> {
        self.try_admin_request(
            "UpdateFeatures",
            protocol::API_VERSION_UPDATE_FEATURES,
            |cid, cid_str| {
                protocol::admin::build_update_features_request(
                    cid,
                    cid_str,
                    feature_updates,
                    validate_only,
                )
            },
            protocol::admin::convert_update_features_response,
        )
    }

    /// Unregisters a broker from the `KRaft` cluster metadata.
    ///
    /// This is a destructive cluster lifecycle operation. Call it only after
    /// the broker has been intentionally removed from service.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn unregister_broker(&mut self, broker_id: i32) -> Result<UnregisterBrokerResponseData> {
        self.try_admin_request(
            "UnregisterBroker",
            protocol::API_VERSION_UNREGISTER_BROKER,
            |cid, cid_str| {
                protocol::admin::build_unregister_broker_request(cid, cid_str, broker_id)
            },
            protocol::admin::convert_unregister_broker_response,
        )
    }

    /// Assigns topic replicas to broker log directory IDs.
    ///
    /// This is a broker storage administration API intended for explicit JBOD
    /// directory placement workflows.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn assign_replicas_to_dirs(
        &mut self,
        options: &AssignReplicasToDirsOptions,
    ) -> Result<AssignReplicasToDirsResponseData> {
        self.try_admin_request(
            "AssignReplicasToDirs",
            protocol::API_VERSION_ASSIGN_REPLICAS_TO_DIRS,
            |cid, cid_str| {
                protocol::admin::build_assign_replicas_to_dirs_request(cid, cid_str, options)
            },
            protocol::admin::convert_assign_replicas_to_dirs_response,
        )
    }

    /// Adds a voter to the `KRaft` controller quorum.
    ///
    /// This is an explicit `KRaft` quorum administration API. Prefer verifying
    /// the target controller listener and directory ID before calling it.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn add_raft_voter(
        &mut self,
        options: &AddRaftVoterOptions,
    ) -> Result<RaftVoterResponseData> {
        self.try_admin_request(
            "AddRaftVoter",
            protocol::API_VERSION_ADD_RAFT_VOTER,
            |cid, cid_str| protocol::admin::build_add_raft_voter_request(cid, cid_str, options),
            protocol::admin::convert_add_raft_voter_response,
        )
    }

    /// Removes a voter from the `KRaft` controller quorum.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn remove_raft_voter(
        &mut self,
        options: &RemoveRaftVoterOptions,
    ) -> Result<RaftVoterResponseData> {
        self.try_admin_request(
            "RemoveRaftVoter",
            protocol::API_VERSION_REMOVE_RAFT_VOTER,
            |cid, cid_str| protocol::admin::build_remove_raft_voter_request(cid, cid_str, options),
            protocol::admin::convert_remove_raft_voter_response,
        )
    }

    /// Updates a voter in the `KRaft` controller quorum.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn update_raft_voter(
        &mut self,
        options: &UpdateRaftVoterOptions,
    ) -> Result<UpdateRaftVoterResponseData> {
        self.try_admin_request(
            "UpdateRaftVoter",
            protocol::API_VERSION_UPDATE_RAFT_VOTER,
            |cid, cid_str| protocol::admin::build_update_raft_voter_request(cid, cid_str, options),
            protocol::admin::convert_update_raft_voter_response,
        )
    }

    /// Elects leaders using the supplied Kafka election type and partition scope.
    ///
    /// Use `ElectLeadersOptions::all_partitions` to ask the broker to elect leaders
    /// for all eligible partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn elect_leaders(
        &mut self,
        options: &ElectLeadersOptions,
    ) -> Result<ElectLeadersResponseData> {
        self.try_admin_request(
            "ElectLeaders",
            protocol::API_VERSION_ELECT_LEADERS,
            |cid, cid_str| protocol::admin::build_elect_leaders_request(cid, cid_str, options),
            protocol::admin::convert_elect_leaders_response,
        )
    }

    /// Elects preferred leaders for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout is too large, brokers are unreachable, or the
    /// broker response cannot be decoded.
    pub fn elect_preferred_leaders(
        &mut self,
        topics: &[TopicPartitionFilter],
        timeout: Duration,
    ) -> Result<ElectLeadersResponseData> {
        let options = ElectLeadersOptions::new(ELECTION_TYPE_PREFERRED, topics.iter().cloned())
            .with_timeout_ms(protocol::to_millis_i32(timeout)?);
        self.elect_leaders(&options)
    }

    /// Elects unclean leaders for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout is too large, brokers are unreachable, or the
    /// broker response cannot be decoded.
    pub fn elect_unclean_leaders(
        &mut self,
        topics: &[TopicPartitionFilter],
        timeout: Duration,
    ) -> Result<ElectLeadersResponseData> {
        let options = ElectLeadersOptions::new(ELECTION_TYPE_UNCLEAN, topics.iter().cloned())
            .with_timeout_ms(protocol::to_millis_i32(timeout)?);
        self.elect_leaders(&options)
    }

    /// Lists config resources for the broker's default supported resource types.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_config_resources(&mut self) -> Result<ListConfigResourcesResponseData> {
        self.list_config_resources_for(&[])
    }

    /// Lists config resources for selected Kafka config resource types.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_config_resources_for(
        &mut self,
        resource_types: &[i8],
    ) -> Result<ListConfigResourcesResponseData> {
        self.try_admin_request(
            "ListConfigResources",
            protocol::API_VERSION_LIST_CONFIG_RESOURCES,
            |cid, cid_str| {
                protocol::admin::build_list_config_resources_request(cid, cid_str, resource_types)
            },
            protocol::admin::convert_list_config_resources_response,
        )
    }

    /// Expands partition counts for one or more topics.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn create_partitions(
        &mut self,
        topics: &[CreatePartitionsTopicSpec],
    ) -> Result<CreatePartitionsResponseData> {
        let options = CreatePartitionsOptions::new(topics.iter().cloned());
        self.create_partitions_with_options(&options)
    }

    /// Expands partition counts using timeout, validation, and assignment options.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn create_partitions_with_options(
        &mut self,
        options: &CreatePartitionsOptions,
    ) -> Result<CreatePartitionsResponseData> {
        self.try_admin_request(
            "CreatePartitions",
            protocol::API_VERSION_CREATE_PARTITIONS,
            |cid, cid_str| protocol::admin::build_create_partitions_request(cid, cid_str, options),
            protocol::admin::convert_create_partitions_response,
        )
    }

    /// Describes topic and partition metadata for one response page.
    ///
    /// Empty `topics` returns all topics visible to the broker, subject to `response_partition_limit`.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_topic_partitions(
        &mut self,
        topics: &[&str],
        response_partition_limit: i32,
    ) -> Result<DescribeTopicPartitionsResponseData> {
        let options = DescribeTopicPartitionsOptions::new(response_partition_limit)
            .with_topics(topics.iter().copied());
        self.describe_topic_partitions_with_options(&options)
    }

    /// Describes topic and partition metadata using Kafka pagination options.
    ///
    /// Use `DescribeTopicPartitionsResponseData::next_cursor` to request subsequent pages.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_topic_partitions_with_options(
        &mut self,
        options: &DescribeTopicPartitionsOptions,
    ) -> Result<DescribeTopicPartitionsResponseData> {
        self.try_admin_request(
            "DescribeTopicPartitions",
            protocol::API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
            |cid, cid_str| {
                protocol::admin::build_describe_topic_partitions_request(cid, cid_str, options)
            },
            protocol::admin::convert_describe_topic_partitions_response,
        )
    }

    /// Describes all client quota entities visible to the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_client_quotas(&mut self) -> Result<DescribeClientQuotasResponseData> {
        self.describe_client_quotas_with_options(&DescribeClientQuotasOptions::default())
    }

    /// Describes client quota entities using Kafka entity filters.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_client_quotas_with_options(
        &mut self,
        options: &DescribeClientQuotasOptions,
    ) -> Result<DescribeClientQuotasResponseData> {
        self.try_admin_request(
            "DescribeClientQuotas",
            protocol::API_VERSION_DESCRIBE_CLIENT_QUOTAS,
            |cid, cid_str| {
                protocol::admin::build_describe_client_quotas_request(cid, cid_str, options)
            },
            protocol::admin::convert_describe_client_quotas_response,
        )
    }

    /// Applies client quota changes for one or more quota entities.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn alter_client_quotas(
        &mut self,
        options: &AlterClientQuotasOptions,
    ) -> Result<AlterClientQuotasResponseData> {
        self.try_admin_request(
            "AlterClientQuotas",
            protocol::API_VERSION_ALTER_CLIENT_QUOTAS,
            |cid, cid_str| {
                protocol::admin::build_alter_client_quotas_request(cid, cid_str, options)
            },
            protocol::admin::convert_alter_client_quotas_response,
        )
    }

    /// Describes SCRAM credential metadata for all visible users.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_user_scram_credentials(
        &mut self,
    ) -> Result<DescribeUserScramCredentialsResponseData> {
        self.describe_user_scram_credentials_with_filter(None)
    }

    /// Describes SCRAM credential metadata for selected users.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_user_scram_credentials_for(
        &mut self,
        users: &[&str],
    ) -> Result<DescribeUserScramCredentialsResponseData> {
        self.describe_user_scram_credentials_with_filter(Some(users))
    }

    fn describe_user_scram_credentials_with_filter(
        &mut self,
        users: Option<&[&str]>,
    ) -> Result<DescribeUserScramCredentialsResponseData> {
        self.try_admin_request(
            "DescribeUserScramCredentials",
            protocol::API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
            |cid, cid_str| {
                protocol::admin::build_describe_user_scram_credentials_request(cid, cid_str, users)
            },
            protocol::admin::convert_describe_user_scram_credentials_response,
        )
    }

    /// Alters SCRAM credentials for Kafka users.
    ///
    /// Upsertions require precomputed `salt` and `salted_password` bytes for the selected SCRAM
    /// mechanism. This mirrors Kafka's protocol and avoids guessing password derivation policy in
    /// the client.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn alter_user_scram_credentials(
        &mut self,
        options: &AlterUserScramCredentialsOptions,
    ) -> Result<AlterUserScramCredentialsResponseData> {
        self.try_admin_request(
            "AlterUserScramCredentials",
            protocol::API_VERSION_ALTER_USER_SCRAM_CREDENTIALS,
            |cid, cid_str| {
                protocol::admin::build_alter_user_scram_credentials_request(cid, cid_str, options)
            },
            protocol::admin::convert_alter_user_scram_credentials_response,
        )
    }

    /// Describes active producers for selected topic partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_producers(
        &mut self,
        topics: &[TopicPartitionFilter],
    ) -> Result<DescribeProducersResponseData> {
        self.try_admin_request(
            "DescribeProducers",
            protocol::API_VERSION_DESCRIBE_PRODUCERS,
            |cid, cid_str| protocol::admin::build_describe_producers_request(cid, cid_str, topics),
            protocol::admin::convert_describe_producers_response,
        )
    }

    /// Looks up end offsets for specific topic-partition leader epochs.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn offsets_for_leader_epochs(
        &mut self,
        topics: &[LeaderEpochTopicRequest],
    ) -> Result<OffsetForLeaderEpochResponseData> {
        self.try_admin_request(
            "OffsetForLeaderEpoch",
            protocol::API_VERSION_OFFSET_FOR_LEADER_EPOCH,
            |cid, cid_str| {
                protocol::admin::build_offset_for_leader_epoch_request(cid, cid_str, topics)
            },
            protocol::admin::convert_offset_for_leader_epoch_response,
        )
    }

    /// Lists transactions visible to the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_transactions(&mut self) -> Result<ListTransactionsResponseData> {
        self.list_transactions_with_options(&ListTransactionsOptions::default())
    }

    /// Lists transactions using Kafka state, producer ID, duration, or ID pattern filters.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_transactions_with_options(
        &mut self,
        options: &ListTransactionsOptions,
    ) -> Result<ListTransactionsResponseData> {
        self.try_admin_request(
            "ListTransactions",
            protocol::API_VERSION_LIST_TRANSACTIONS,
            |cid, cid_str| protocol::admin::build_list_transactions_request(cid, cid_str, options),
            protocol::admin::convert_list_transactions_response,
        )
    }

    /// Describes detailed transaction state for the supplied transactional IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_transactions(
        &mut self,
        transactional_ids: &[&str],
    ) -> Result<DescribeTransactionsResponseData> {
        self.try_admin_request(
            "DescribeTransactions",
            protocol::API_VERSION_DESCRIBE_TRANSACTIONS,
            |cid, cid_str| {
                protocol::admin::build_describe_transactions_request(
                    cid,
                    cid_str,
                    transactional_ids,
                )
            },
            protocol::admin::convert_describe_transactions_response,
        )
    }

    /// Adds offsets for a consumer group to the current transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn add_offsets_to_txn(
        &mut self,
        txn_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<AddOffsetsToTxnResponseData> {
        self.try_admin_request(
            "AddOffsetsToTxn",
            protocol::API_VERSION_ADD_OFFSETS_TO_TXN,
            |cid, cid_str| {
                protocol::admin::build_add_offsets_to_txn_request(
                    cid,
                    cid_str,
                    txn_id,
                    producer_id,
                    producer_epoch,
                    group_id,
                )
            },
            |resp| protocol::admin::convert_add_offsets_to_txn_response(&resp),
        )
    }

    /// Commits consumer offsets as part of a transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn txn_offset_commit(
        &mut self,
        txn_id: &str,
        group_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        offsets: &[TxnOffsetCommitTopicPartition],
    ) -> Result<TxnOffsetCommitResponseData> {
        self.try_admin_request(
            "TxnOffsetCommit",
            protocol::API_VERSION_TXN_OFFSET_COMMIT,
            |cid, cid_str| {
                protocol::admin::build_txn_offset_commit_request(
                    cid,
                    cid_str,
                    txn_id,
                    group_id,
                    producer_id,
                    producer_epoch,
                    offsets,
                )
            },
            protocol::admin::convert_txn_offset_commit_response,
        )
    }

    /// Describes modern consumer group state for the supplied group IDs.
    ///
    /// This uses Kafka's `ConsumerGroupDescribe` API and returns structured member
    /// subscription and assignment data.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_consumer_groups(
        &mut self,
        groups: &[&str],
    ) -> Result<ConsumerGroupDescribeResponseData> {
        self.describe_consumer_groups_with_options(groups, false)
    }

    /// Describes modern consumer group state with optional authorized-operation fields.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_consumer_groups_with_options(
        &mut self,
        groups: &[&str],
        include_authorized_operations: bool,
    ) -> Result<ConsumerGroupDescribeResponseData> {
        self.try_admin_request(
            "ConsumerGroupDescribe",
            protocol::API_VERSION_CONSUMER_GROUP_DESCRIBE,
            |cid, cid_str| {
                protocol::admin::build_consumer_group_describe_request(
                    cid,
                    cid_str,
                    groups,
                    include_authorized_operations,
                )
            },
            protocol::admin::convert_consumer_group_describe_response,
        )
    }

    /// Describes Kafka share group state for the supplied group IDs.
    ///
    /// This uses Kafka's `ShareGroupDescribe` API and returns structured member
    /// subscription and assignment data.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_share_groups(
        &mut self,
        groups: &[&str],
    ) -> Result<ShareGroupDescribeResponseData> {
        self.describe_share_groups_with_options(groups, false)
    }

    /// Describes Kafka share group state with optional authorized-operation fields.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_share_groups_with_options(
        &mut self,
        groups: &[&str],
        include_authorized_operations: bool,
    ) -> Result<ShareGroupDescribeResponseData> {
        self.try_admin_request(
            "ShareGroupDescribe",
            protocol::API_VERSION_SHARE_GROUP_DESCRIBE,
            |cid, cid_str| {
                protocol::admin::build_share_group_describe_request(
                    cid,
                    cid_str,
                    groups,
                    include_authorized_operations,
                )
            },
            protocol::admin::convert_share_group_describe_response,
        )
    }

    /// Describes all visible share-partition offsets for the supplied share group IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_share_group_offsets(
        &mut self,
        groups: &[&str],
    ) -> Result<DescribeShareGroupOffsetsResponseData> {
        let requests: Vec<_> = groups
            .iter()
            .map(|group| ShareGroupOffsetRequest::all_partitions(*group))
            .collect();
        self.describe_share_group_offsets_with_options(&requests)
    }

    /// Describes share-partition offsets using per-group topic partition filters.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_share_group_offsets_with_options(
        &mut self,
        groups: &[ShareGroupOffsetRequest],
    ) -> Result<DescribeShareGroupOffsetsResponseData> {
        self.try_admin_request(
            "DescribeShareGroupOffsets",
            protocol::API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
            |cid, cid_str| {
                protocol::admin::build_describe_share_group_offsets_request(cid, cid_str, groups)
            },
            protocol::admin::convert_describe_share_group_offsets_response,
        )
    }

    /// Alters start offsets for partitions in a Kafka share group.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn alter_share_group_offsets(
        &mut self,
        group_id: &str,
        topics: &[AlterShareGroupOffsetTopic],
    ) -> Result<AlterShareGroupOffsetsResponseData> {
        self.try_admin_request(
            "AlterShareGroupOffsets",
            protocol::API_VERSION_ALTER_SHARE_GROUP_OFFSETS,
            |cid, cid_str| {
                protocol::admin::build_alter_share_group_offsets_request(
                    cid, cid_str, group_id, topics,
                )
            },
            protocol::admin::convert_alter_share_group_offsets_response,
        )
    }

    /// Deletes stored offsets for topics in a Kafka share group.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn delete_share_group_offsets(
        &mut self,
        group_id: &str,
        topics: &[DeleteShareGroupOffsetTopic],
    ) -> Result<DeleteShareGroupOffsetsResponseData> {
        self.try_admin_request(
            "DeleteShareGroupOffsets",
            protocol::API_VERSION_DELETE_SHARE_GROUP_OFFSETS,
            |cid, cid_str| {
                protocol::admin::build_delete_share_group_offsets_request(
                    cid, cid_str, group_id, topics,
                )
            },
            protocol::admin::convert_delete_share_group_offsets_response,
        )
    }

    /// Lists consumer groups known to the contacted broker.
    ///
    /// The request is attempted against configured brokers until one succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_groups(&mut self) -> Result<ListGroupsResponseData> {
        self.list_groups_with_filters(&[], &[])
    }

    /// Lists consumer groups filtered by group state and group type.
    ///
    /// Empty filters return all groups visible to the contacted broker.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn list_groups_with_filters(
        &mut self,
        states_filter: &[&str],
        types_filter: &[&str],
    ) -> Result<ListGroupsResponseData> {
        self.try_admin_request(
            "ListGroups",
            protocol::API_VERSION_LIST_GROUPS,
            |cid, cid_str| {
                protocol::admin::build_list_groups_request(
                    cid,
                    cid_str,
                    states_filter,
                    types_filter,
                )
            },
            protocol::admin::convert_list_groups_response,
        )
    }

    /// Deletes the supplied consumer groups.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn delete_groups(&mut self, groups: &[&str]) -> Result<DeleteGroupsResponseData> {
        self.try_admin_request(
            "DeleteGroups",
            protocol::API_VERSION_DELETE_GROUPS,
            |cid, cid_str| protocol::admin::build_delete_groups_request(cid, cid_str, groups),
            protocol::admin::convert_delete_groups_response,
        )
    }

    /// Describes the supplied consumer groups.
    ///
    /// The request is attempted against configured brokers until one succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_groups(&mut self, groups: &[&str]) -> Result<DescribeGroupsResponseData> {
        self.describe_groups_with_options(groups, false)
    }

    /// Describes the supplied consumer groups with optional authorized-operation fields.
    ///
    /// # Errors
    ///
    /// Returns an error if brokers are unreachable or the broker response cannot be decoded.
    pub fn describe_groups_with_options(
        &mut self,
        groups: &[&str],
        include_authorized_operations: bool,
    ) -> Result<DescribeGroupsResponseData> {
        self.try_admin_request(
            "DescribeGroups",
            protocol::API_VERSION_DESCRIBE_GROUPS,
            |cid, cid_str| {
                protocol::admin::build_describe_groups_request(
                    cid,
                    cid_str,
                    groups,
                    include_authorized_operations,
                )
            },
            protocol::admin::convert_describe_groups_response,
        )
    }
}
