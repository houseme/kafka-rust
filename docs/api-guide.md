# rustfs-kafka API Guide

This guide focuses on the current public APIs in `rustfs-kafka` (sync) and the most common usage patterns.

## 1. Quick Start

### 1.1 Producer

```rust,no_run
use std::time::Duration;
use rustfs_kafka::producer::{Producer, Record, RequiredAcks};

let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_client_id("app-producer".to_owned())
    .with_ack_timeout(Duration::from_secs(1))
    .with_required_acks(RequiredAcks::One)
    .create()
    .unwrap();

producer.send(&Record::from_value("my-topic", b"hello")).unwrap();
```

### 1.2 Consumer

```rust,no_run
use rustfs_kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_topic("my-topic".to_owned())
    .with_group("my-group".to_owned())
    .with_fallback_offset(FetchOffset::Earliest)
    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
    .create()
    .unwrap();

for ms in consumer.poll().unwrap() {
    for m in ms.messages() {
        println!("offset={} value={:?}", m.offset, m.value);
    }
    consumer.consume_messageset(&ms).unwrap();
}
consumer.commit_consumed().unwrap();
```

### 1.3 KafkaClient (mid-level)

```rust,no_run
use rustfs_kafka::client::KafkaClient;

let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
client.load_metadata_all().unwrap();
```

## 2. Producer APIs

### 2.1 Single and batch send

- `Producer::send(&Record)` sends one record.
- `Producer::send_all(&[Record])` sends a batch synchronously.

### 2.2 Partitioner selection

- `DefaultPartitioner` (default, key-hash based)
- `RoundRobinPartitioner`
- `StickyPartitioner`
- `UniformPartitioner`

### 2.3 Batch producer

```rust,no_run
use rustfs_kafka::producer::{BatchProducer, Record};
use std::time::Duration;

let mut batch = BatchProducer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_batch_size(100)
    .with_linger(Duration::from_millis(5))
    .create()
    .unwrap();

batch.send(Record::from_value("my-topic", b"msg-1")).unwrap();
batch.send(Record::from_value("my-topic", b"msg-2")).unwrap();
let _confirms = batch.flush().unwrap();
```

### 2.4 Transactional producer

Use `TransactionalProducer` for exactly-once style workflows.

```rust,no_run
use rustfs_kafka::producer::{TransactionalProducer, Record};

let mut tx = TransactionalProducer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_transactional_id("txn-demo".to_owned())
    .create()
    .unwrap();

tx.begin().unwrap();
tx.send(&Record::from_value("my-topic", b"in-txn")).unwrap();
tx.commit().unwrap();
```

## 3. Consumer APIs

### 3.1 Offset strategy

- `FetchOffset::Earliest`
- `FetchOffset::Latest`
- `FetchOffset::ByTime(i64)`

### 3.2 Pause/resume partitions

```rust,no_run
// consumer.pause("my-topic", &[0, 1]);
// consumer.resume("my-topic", &[1]);
```

### 3.3 Group offset APIs through `KafkaClient`

- `commit_offset`, `commit_offsets`
- `fetch_group_offsets`, `fetch_group_topic_offset`

## 4. KafkaClient Administrative APIs

### 4.1 Topic metadata and offsets

- `load_metadata_all`, `load_metadata`
- `fetch_offsets`, `list_offsets`, `fetch_topic_offsets`

### 4.2 Cluster and group inspection

```rust,no_run
use std::time::Duration;
use rustfs_kafka::client::{
    AclBinding, AlterClientQuotasOptions, AlterConfigsEntry, AlterConfigsOptions,
    AlterConfigsResource, AlterPartitionReassignmentsOptions, AlterReplicaLogDir,
    AlterReplicaLogDirTopic, AlterShareGroupOffsetPartition, AlterShareGroupOffsetTopic,
    AlterUserScramCredentialsOptions, ClientQuotaAlteration, ClientQuotaAlterationOp,
    ClientQuotaEntityFilter, ClientQuotaEntitySpec, ConfigResource, CreateDelegationTokenOptions,
    CreatePartitionsOptions, CreatePartitionsTopicSpec, DeleteRecordsPartitionSpec,
    DeleteRecordsTopicSpec, DeleteShareGroupOffsetTopic,
    DescribeAclsFilter, DescribeClientQuotasOptions, DescribeTopicPartitionsOptions,
    FeatureUpdate, IncrementalAlterConfig, IncrementalAlterConfigsOptions,
    IncrementalAlterConfigsResource, KafkaClient, KafkaPrincipal, LeaderEpochPartitionRequest,
    LeaderEpochTopicRequest, ListTransactionsOptions, PartitionReassignmentSpec,
    PartitionReassignmentTopicSpec, SCRAM_MECHANISM_SHA_512, ScramCredentialUpsertion,
    ShareConsumerSession, ShareGroupOffsetRequest, ShareFetchSessionConfig,
    TelemetrySession, TopicPartitionFilter, TopicPartitionsCursor, TxnOffsetCommitTopicPartition,
    ACL_OPERATION_READ, ACL_PATTERN_TYPE_LITERAL, ACL_PERMISSION_TYPE_ALLOW,
    ACL_RESOURCE_TYPE_TOPIC, CONFIG_RESOURCE_TYPE_BROKER, CONFIG_RESOURCE_TYPE_TOPIC,
};

let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);

let api_versions = client.fetch_api_versions().unwrap();
println!("broker supports {} Kafka APIs", api_versions.api_keys.len());

let cluster = client.describe_cluster().unwrap();
println!(
    "cluster={} controller={} brokers={}",
    cluster.cluster_id,
    cluster.controller_id,
    cluster.brokers.len()
);

let groups = client.list_groups().unwrap();
for group in groups.groups {
    println!(
        "group={} protocol={} state={} type={}",
        group.group_id,
        group.protocol_type,
        group.group_state,
        group.group_type
    );
}

let deleted_groups = client.delete_groups(&["old-group"]).unwrap();
for group in deleted_groups.results {
    println!("deleted_group={} error_code={}", group.group_id, group.error_code);
}

let described = client.describe_groups(&["my-group"]).unwrap();
for group in described.groups {
    println!("group={} members={}", group.group_id, group.members.len());
}

let consumer_groups = client
    .describe_consumer_groups_with_options(&["my-group"], true)
    .unwrap();
for group in consumer_groups.groups {
    println!(
        "consumer_group={} epoch={} members={}",
        group.group_id,
        group.group_epoch,
        group.members.len()
    );
}

let share_groups = client
    .describe_share_groups_with_options(&["my-share-group"], true)
    .unwrap();
for group in share_groups.groups {
    println!(
        "share_group={} epoch={} members={}",
        group.group_id,
        group.group_epoch,
        group.members.len()
    );
}

let topic_page = client.describe_topic_partitions(&["my-topic"], 100).unwrap();
for topic in topic_page.topics {
    println!(
        "topic={:?} partitions={}",
        topic.name,
        topic.partitions.len()
    );
}

let next_topic_page = client
    .describe_topic_partitions_with_options(
        &DescribeTopicPartitionsOptions::new(100)
            .with_topics(["my-topic"])
            .with_cursor(TopicPartitionsCursor::new("my-topic", 100)),
    )
    .unwrap();
println!("next_page_topics={}", next_topic_page.topics.len());

let acl_filter = DescribeAclsFilter::any()
    .with_resource_type(ACL_RESOURCE_TYPE_TOPIC)
    .with_resource_name("my-topic")
    .with_pattern_type(ACL_PATTERN_TYPE_LITERAL)
    .with_operation(ACL_OPERATION_READ)
    .with_permission_type(ACL_PERMISSION_TYPE_ALLOW);
let acls = client.describe_acls_with_filter(&acl_filter).unwrap();
for resource in acls.resources {
    println!("acl_resource={} acls={}", resource.resource_name, resource.acls.len());
}

let acl = AclBinding::new(
    ACL_RESOURCE_TYPE_TOPIC,
    "my-topic",
    ACL_PATTERN_TYPE_LITERAL,
    "User:alice",
    "*",
    ACL_OPERATION_READ,
    ACL_PERMISSION_TYPE_ALLOW,
);
let created_acls = client.create_acls(&[acl]).unwrap();
println!("created_acl_results={}", created_acls.results.len());

let deleted_acls = client.delete_acls(&[acl_filter]).unwrap();
println!("deleted_acl_filter_results={}", deleted_acls.filter_results.len());

let configs = client
    .describe_configs(&[
        ConfigResource::topic("my-topic").with_configuration_keys(["retention.ms"]),
        ConfigResource::broker("1"),
    ])
    .unwrap();
for resource in configs.results {
    println!("resource={} configs={}", resource.resource_name, resource.configs.len());
}

let altered_configs = client
    .incremental_alter_configs(
        &IncrementalAlterConfigsOptions::new([
            IncrementalAlterConfigsResource::topic(
                "my-topic",
                [IncrementalAlterConfig::set("retention.ms", "60000")],
            ),
        ])
        .with_validate_only(true),
    )
    .unwrap();
println!("altered_config_resources={}", altered_configs.responses.len());

#[allow(deprecated)]
let legacy_config_update = client
    .alter_configs(
        &AlterConfigsOptions::new([AlterConfigsResource::topic(
            "my-topic",
            [AlterConfigsEntry::new("retention.ms", "60000")],
        )])
        .with_validate_only(true),
    )
    .unwrap();
println!(
    "legacy_config_update_resources={}",
    legacy_config_update.responses.len()
);

let config_resources = client
    .list_config_resources_for(&[CONFIG_RESOURCE_TYPE_TOPIC, CONFIG_RESOURCE_TYPE_BROKER])
    .unwrap();
for resource in config_resources.resources {
    println!(
        "config_resource={} type={}",
        resource.resource_name,
        resource.resource_type
    );
}

let partitions = [TopicPartitionFilter::new("my-topic", [0, 1])];

let expanded_partitions = client
    .create_partitions_with_options(
        &CreatePartitionsOptions::new([CreatePartitionsTopicSpec::new("my-topic", 6)])
            .with_validate_only(true),
    )
    .unwrap();
println!("partition_expansion_results={}", expanded_partitions.results.len());

let deleted_records = client
    .delete_records(
        &[DeleteRecordsTopicSpec::new(
            "my-topic",
            [DeleteRecordsPartitionSpec::new(0, 42)],
        )],
        Duration::from_secs(10),
    )
    .unwrap();
for topic in deleted_records.topics {
    println!("deleted_records_topic={} partitions={}", topic.name, topic.partitions.len());
}

let deleted_offsets = client
    .delete_group_offsets("old-group", &partitions)
    .unwrap();
for topic in deleted_offsets.topics {
    println!("offset_delete_topic={} partitions={}", topic.name, topic.partitions.len());
}

let log_dirs = client.describe_log_dirs_for(&partitions).unwrap();
for log_dir in log_dirs.results {
    println!(
        "log_dir={} usable_bytes={} topics={}",
        log_dir.log_dir,
        log_dir.usable_bytes,
        log_dir.topics.len()
    );
}

let moved_replicas = client
    .alter_replica_log_dirs(&[AlterReplicaLogDir::new(
        "/kafka-logs-2",
        vec![AlterReplicaLogDirTopic::new("my-topic", [0, 1])],
    )])
    .unwrap();
println!("moved_replica_topics={}", moved_replicas.results.len());

let reassignments = client
    .list_partition_reassignments_for(&partitions, Duration::from_secs(10))
    .unwrap();
for topic in reassignments.topics {
    println!("topic={} reassignments={}", topic.name, topic.partitions.len());
}

let reassignment_update = client
    .alter_partition_reassignments(
        &AlterPartitionReassignmentsOptions::new([
            PartitionReassignmentTopicSpec::new(
                "my-topic",
                [PartitionReassignmentSpec::new(0, [1, 2])],
            ),
        ])
        .with_allow_replication_factor_change(false),
    )
    .unwrap();
println!(
    "reassignment_update_results={}",
    reassignment_update.responses.len()
);

let election = client
    .elect_preferred_leaders(&partitions, Duration::from_secs(10))
    .unwrap();
println!("leader_election_topics={}", election.replica_election_results.len());

let metadata_quorum = [TopicPartitionFilter::new("cluster-metadata", [0])];
let quorum = client.describe_quorum(&metadata_quorum).unwrap();
for topic in quorum.topics {
    println!("quorum_topic={} partitions={}", topic.name, topic.partitions.len());
}

let leader_epoch_offsets = client
    .offsets_for_leader_epochs(&[LeaderEpochTopicRequest::new(
        "my-topic",
        [LeaderEpochPartitionRequest::new(0, -1, 7)],
    )])
    .unwrap();
for topic in leader_epoch_offsets.topics {
    println!("leader_epoch_topic={} partitions={}", topic.topic, topic.partitions.len());
}

let tokens = client
    .describe_delegation_tokens_for(&[KafkaPrincipal::user("alice")])
    .unwrap();
for token in tokens.tokens {
    println!("token_id={} renewers={}", token.token_id, token.renewers.len());
}

let created_token = client
    .create_delegation_token(
        &CreateDelegationTokenOptions::new()
            .with_owner(KafkaPrincipal::user("alice"))
            .with_renewer(KafkaPrincipal::user("bob"))
            .with_max_lifetime_ms(86_400_000),
    )
    .unwrap();
let renewed_token = client
    .renew_delegation_token(&created_token.hmac, Duration::from_secs(3600))
    .unwrap();
println!("renewed_token_expiry={}", renewed_token.expiry_timestamp_ms);

let quota_filter = DescribeClientQuotasOptions::new()
    .with_component(ClientQuotaEntityFilter::exact("user", "alice"));
let quotas = client.describe_client_quotas_with_options(&quota_filter).unwrap();
for entry in quotas.entries.unwrap_or_default() {
    println!("quota_entity_parts={} values={}", entry.entity.len(), entry.values.len());
}

let altered_quotas = client
    .alter_client_quotas(
        &AlterClientQuotasOptions::new([ClientQuotaAlteration::new(
            [
                ClientQuotaEntitySpec::named("user", "alice"),
                ClientQuotaEntitySpec::default_entity("client-id"),
            ],
            [ClientQuotaAlterationOp::set("producer_byte_rate", 1024.5)],
        )])
        .with_validate_only(true),
    )
    .unwrap();
println!("altered_quota_entries={}", altered_quotas.entries.len());

let scram_credentials = client
    .describe_user_scram_credentials_for(&["alice"])
    .unwrap();
for user in scram_credentials.results {
    println!("user={} credentials={}", user.user, user.credential_infos.len());
}

let scram_update = client
    .alter_user_scram_credentials(
        &AlterUserScramCredentialsOptions::new()
            .with_upsertion(ScramCredentialUpsertion::new(
                "alice",
                SCRAM_MECHANISM_SHA_512,
                8192,
                bytes::Bytes::from_static(b"salt"),
                bytes::Bytes::from_static(b"salted-password"),
            )),
    )
    .unwrap();
println!("scram_update_results={}", scram_update.results.len());

let producers = client.describe_producers(&partitions).unwrap();
for topic in producers.topics {
    println!("topic={} producer_partitions={}", topic.name, topic.partitions.len());
}

let share_offsets = client
    .describe_share_group_offsets_with_options(&[
        ShareGroupOffsetRequest::with_topics("my-share-group", partitions.clone()),
    ])
    .unwrap();
for group in share_offsets.groups {
    println!("share_group={} offset_topics={}", group.group_id, group.topics.len());
}

let altered_share_offsets = client
    .alter_share_group_offsets(
        "my-share-group",
        &[AlterShareGroupOffsetTopic::new(
            "my-topic",
            [AlterShareGroupOffsetPartition::new(0, 42)],
        )],
    )
    .unwrap();
println!("altered_share_topics={}", altered_share_offsets.responses.len());

let deleted_share_offsets = client
    .delete_share_group_offsets(
        "my-share-group",
        &[DeleteShareGroupOffsetTopic::new("my-topic")],
    )
    .unwrap();
println!("deleted_share_topics={}", deleted_share_offsets.responses.len());

let transaction_filter = ListTransactionsOptions::new()
    .with_state_filters(["Ongoing"])
    .with_duration_filter_ms(30_000)
    .with_transactional_id_pattern("rustfs-.*");
let transactions = client
    .list_transactions_with_options(&transaction_filter)
    .unwrap();
for transaction in transactions.transaction_states {
    println!(
        "transactional_id={} state={}",
        transaction.transactional_id,
        transaction.transaction_state
    );
}

let described_transactions = client.describe_transactions(&["rustfs-txn-a"]).unwrap();
for transaction in described_transactions.transaction_states {
    println!(
        "transactional_id={} partitions={}",
        transaction.transactional_id,
        transaction.topics.len()
    );
}

let txn_offsets = client
    .add_offsets_to_txn("rustfs-txn-a", 42, 3, "consumer-group-a")
    .unwrap();
println!("add_offsets_to_txn_error={}", txn_offsets.error_code);

let committed_txn_offsets = client
    .txn_offset_commit(
        "rustfs-txn-a",
        "consumer-group-a",
        42,
        3,
        &[TxnOffsetCommitTopicPartition::new("my-topic", 0, 123)],
    )
    .unwrap();
println!("txn_offset_topics={}", committed_txn_offsets.topics.len());

let feature_preview = client
    .update_features(&[FeatureUpdate::upgrade("metadata.version", 20)], true)
    .unwrap();
println!("feature_update_results={}", feature_preview.results.len());

let mut telemetry = TelemetrySession::initial();
let subscription = client
    .get_telemetry_subscriptions(telemetry.client_instance_id)
    .unwrap();
if telemetry.apply_subscription(&subscription) {
    let push = telemetry.push_options(
        bytes::Bytes::from_static(b"encoded-otel-metrics"),
        &[rustfs_kafka::client::TELEMETRY_COMPRESSION_ZSTD],
    );
    let _ = client.push_telemetry(&push).unwrap();
}

let share_session = ShareConsumerSession::new("my-share-group", "member-a")
    .with_subscribed_topic_names(["my-topic"])
    .with_fetch_config(ShareFetchSessionConfig {
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1_048_576,
        max_records: 100,
        batch_size: 10,
    });
let heartbeat = share_session.heartbeat_options();
let _ = client.share_group_heartbeat(&heartbeat).unwrap();

```

Optional variants expose the highest fields currently wired from `kafka-protocol`:

- `describe_cluster_with_options(include_authorized_operations, include_fenced_brokers)`
- `list_groups_with_filters(states_filter, types_filter)`
- `delete_groups(groups)`
- `describe_groups_with_options(groups, include_authorized_operations)`
- `describe_consumer_groups_with_options(groups, include_authorized_operations)`
- `describe_share_groups_with_options(groups, include_authorized_operations)`
- `describe_share_group_offsets_with_options(groups)`
- `describe_topic_partitions_with_options(options)`
- `describe_acls_with_filter(filter)`
- `create_acls(bindings)`
- `delete_acls(filters)`
- `describe_configs_with_options(resources, include_synonyms, include_documentation)`
- `incremental_alter_configs(options)`
- `list_config_resources_for(resource_types)`
- `describe_delegation_tokens_for(owners)`
- `create_delegation_token(options)`
- `renew_delegation_token(hmac, renew_period)`
- `expire_delegation_token(hmac, expiry_period)`
- `create_partitions_with_options(options)`
- `delete_records(topics, timeout)`
- `describe_log_dirs_for(topic_partition_filters)`
- `alter_replica_log_dirs(log_dirs)`
- `delete_group_offsets(group, topic_partition_filters)`
- `alter_partition_reassignments(options)`
- `list_partition_reassignments_for(topic_partition_filters, timeout)`
- `elect_leaders(options)`
- `elect_preferred_leaders(topic_partition_filters, timeout)`
- `elect_unclean_leaders(topic_partition_filters, timeout)`
- `describe_quorum(topic_partition_filters)`
- `offsets_for_leader_epochs(topics)`
- `describe_client_quotas_with_options(options)`
- `alter_client_quotas(options)`
- `describe_user_scram_credentials_for(users)`
- `alter_user_scram_credentials(options)`
- `list_transactions_with_options(options)`
- `add_offsets_to_txn(transactional_id, producer_id, producer_epoch, group_id)`
- `txn_offset_commit(transactional_id, group_id, producer_id, producer_epoch, offsets)`
- `alter_share_group_offsets(group_id, topics)`
- `delete_share_group_offsets(group_id, topics)`
- `update_features(feature_updates, validate_only)`
- `unregister_broker(broker_id)` for explicitly planned KRaft broker removal.
- `assign_replicas_to_dirs(options)` for explicit broker log directory placement.
- `add_raft_voter(options)`, `remove_raft_voter(options)`, and `update_raft_voter(options)` for explicit KRaft
  quorum voter administration.
- `send_raw_protocol_request(api_key, api_version, request)` for advanced typed access to generated
  `kafka-protocol` requests that do not have a stable high-level client workflow.
- `AsyncKafkaClient::send_raw_protocol_request(api_key, api_version, request)` provides the same
  low-level generated protocol access for native tokio callers.
- `TelemetrySession` tracks successful telemetry subscription responses and builds broker-compatible
  `PushTelemetryOptions`.
- `ShareConsumerSession` composes share-group heartbeat, fetch, and acknowledgement options from the latest
  coordinator assignment.
- `alter_configs(options)` is deprecated; prefer `incremental_alter_configs(options)`.

### 4.3 Topic create/delete

```rust,no_run
use std::time::Duration;
use rustfs_kafka::client::{KafkaClient, TopicConfig};

let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
client.load_metadata_all().unwrap();

let topics = vec![TopicConfig::new("demo-topic").with_partitions(3)];
let _ = client.create_topics(&topics, Duration::from_secs(10)).unwrap();
let _ = client.delete_topics(&["demo-topic"], Duration::from_secs(10)).unwrap();
```

## 5. TLS

Enable TLS with default feature `security` (rustls + aws-lc-rs):

```toml
[dependencies]
rustfs-kafka = "1.3.0"
```

By default, TLS verification uses `webpki-roots`. Use `SecurityConfig::with_ca_cert` when Kafka brokers are signed by a
private or enterprise CA.

`security-ring` switches rustls crypto provider to `ring`:

```toml
[dependencies]
rustfs-kafka = { version = "1.3.0", default-features = false, features = ["security-ring"] }
```

## 6. Metrics

Enable metrics feature:

```toml
[dependencies]
rustfs-kafka = { version = "1.3.0", features = ["metrics"] }
```

Metrics include produce/fetch/metadata refresh and connection-level counters/gauges.

## 7. Feature Flags

- `security` (default)
- `security-ring`
- `compression` (default; enables gzip, snappy, lz4, and zstd)
- `gzip`
- `snappy`
- `lz4`
- `zstd`
- `producer_timestamp`
- `metrics`
- `nightly`
- `integration_tests`

Producer and consumer record batch compression is provided by `kafka-protocol`
codec features. Default builds enable all supported codecs. If default features
are disabled, enable each required codec explicitly, for example:

```toml
[dependencies]
rustfs-kafka = { version = "1.3.0", default-features = false, features = ["security", "zstd"] }
```

## 8. Async crate

For async wrappers built on tokio, see `crates/rustfs-kafka-async` and its README.
