//! Async consumer for fetching messages from Kafka.

use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    GroupId, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    RequestHeader, TopicName, fetch_request::FetchPartition as KpFetchPartition,
    fetch_request::FetchTopic as KpFetchTopic, list_offsets_request::ListOffsetsPartition,
    list_offsets_request::ListOffsetsTopic, metadata_request::MetadataRequestTopic,
    offset_commit_request::OffsetCommitRequestPartition,
    offset_commit_request::OffsetCommitRequestTopic, offset_fetch_request::OffsetFetchRequestTopic,
};
use kafka_protocol::protocol::StrBytes;
use rustfs_kafka::client::SecurityConfig;
use rustfs_kafka::client::fetch_kp::{OwnedFetchResponse, convert_fetch_response};
use rustfs_kafka::consumer::{FetchOffset, MessageSets};
use rustfs_kafka::error::{ConsumerError, Error, KafkaCode, ProtocolError, Result};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

use crate::AsyncKafkaClient;
use crate::consumer_observability::{
    DEFAULT_NATIVE_RECENT_ERROR_LIMIT, NativeConsumerErrorStats, NativeConsumerObservability,
};
use crate::wire::{get_kp_response, kafka_code_from_protocol as map_kafka_code, send_kp_request};

const API_VERSION_METADATA: i16 = 1;
const API_VERSION_FETCH: i16 = 12;
const API_VERSION_FIND_COORDINATOR: i16 = 3;
const API_VERSION_OFFSET_COMMIT: i16 = 2;
const API_VERSION_OFFSET_FETCH: i16 = 2;
const API_VERSION_LIST_OFFSETS: i16 = 1;
const DEFAULT_NATIVE_RETRY_ATTEMPTS: usize = 3;
const DEFAULT_NATIVE_RETRY_BACKOFF_MS: u64 = 100;
const FETCH_MIN_BYTES: i32 = 1;
const FETCH_MAX_WAIT_MS: i32 = 100;
const FETCH_PARTITION_MAX_BYTES: i32 = 1_048_576;

type TopicOffsets = HashMap<String, HashMap<i32, i64>>;

struct NativeConsumer {
    client: AsyncKafkaClient,
    group: String,
    topics: Vec<String>,
    fallback_offset: FetchOffset,
    offsets: TopicOffsets,
    dirty_offsets: TopicOffsets,
    leaders: HashMap<(String, i32), String>,
    coordinator: Option<String>,
    correlation: i32,
    retry_attempts: usize,
    retry_backoff: Duration,
    observability: NativeConsumerObservability,
}

enum AsyncConsumerMode {
    Native(Box<NativeConsumer>),
}

/// An async Kafka consumer.
pub struct AsyncConsumer {
    mode: AsyncConsumerMode,
}

/// Builder for constructing an [`AsyncConsumer`] asynchronously.
pub struct AsyncConsumerBuilder {
    hosts: Vec<String>,
    group: Option<String>,
    topics: Vec<String>,
    security: Option<SecurityConfig>,
    channel_capacity: usize,
    native_async: bool,
    fallback_offset: FetchOffset,
    native_retry_attempts: usize,
    native_retry_backoff: Duration,
    native_recent_error_limit: usize,
}

impl AsyncConsumerBuilder {
    /// Creates a new async consumer builder from bootstrap hosts.
    #[must_use]
    pub fn new(hosts: Vec<String>) -> Self {
        Self {
            hosts,
            group: None,
            topics: Vec::new(),
            security: None,
            channel_capacity: 64,
            native_async: true,
            fallback_offset: FetchOffset::Latest,
            native_retry_attempts: DEFAULT_NATIVE_RETRY_ATTEMPTS,
            native_retry_backoff: Duration::from_millis(DEFAULT_NATIVE_RETRY_BACKOFF_MS),
            native_recent_error_limit: DEFAULT_NATIVE_RECENT_ERROR_LIMIT,
        }
    }

    /// Sets the consumer group.
    #[must_use]
    pub fn with_group(mut self, group: String) -> Self {
        self.group = Some(group);
        self
    }

    /// Adds a topic subscription.
    #[must_use]
    pub fn with_topic(mut self, topic: String) -> Self {
        self.topics.push(topic);
        self
    }

    /// Adds multiple topic subscriptions.
    #[must_use]
    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics.extend(topics);
        self
    }

    /// Sets optional TLS security configuration for broker connections.
    #[must_use]
    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.security = Some(security);
        self
    }

    /// Backward-compatible no-op kept for API compatibility.
    #[deprecated(
        since = "1.2.0",
        note = "native async consumers no longer use an internal channel; this setting is ignored"
    )]
    #[must_use]
    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity.max(1);
        self
    }

    /// Backward-compatible setting kept for API compatibility.
    #[deprecated(
        since = "1.2.0",
        note = "native async consumers are always enabled; this setting is ignored"
    )]
    #[must_use]
    pub fn with_native_async(mut self, native_async: bool) -> Self {
        self.native_async = native_async;
        self
    }

    /// Sets fallback offset used when there is no committed group offset.
    #[must_use]
    pub fn with_fallback_offset(mut self, fallback_offset: FetchOffset) -> Self {
        self.fallback_offset = fallback_offset;
        self
    }

    /// Sets retry attempts for native async poll/commit recoverable errors.
    #[must_use]
    pub fn with_native_retry_attempts(mut self, attempts: usize) -> Self {
        self.native_retry_attempts = attempts.max(1);
        self
    }

    /// Sets retry backoff for native async poll/commit recoverable errors.
    #[must_use]
    pub fn with_native_retry_backoff(mut self, backoff: Duration) -> Self {
        self.native_retry_backoff = backoff;
        self
    }

    /// Sets the max number of native recent error snapshots retained in memory.
    #[must_use]
    pub fn with_native_recent_error_limit(mut self, limit: usize) -> Self {
        self.native_recent_error_limit = limit.max(1);
        self
    }

    /// Builds an async consumer.
    pub async fn build(self) -> Result<AsyncConsumer> {
        let AsyncConsumerBuilder {
            hosts,
            group,
            topics,
            security,
            channel_capacity,
            native_async,
            fallback_offset,
            native_retry_attempts,
            native_retry_backoff,
            native_recent_error_limit,
        } = self;

        let group = group.ok_or(Error::Consumer(ConsumerError::UnsetGroupId))?;
        if topics.is_empty() {
            return Err(Error::Consumer(ConsumerError::NoTopicsAssigned));
        }

        if !native_async {
            debug!(
                "AsyncConsumerBuilder::with_native_async(false) is ignored: consumer always uses native async I/O"
            );
        }
        let _ = channel_capacity;
        let client = AsyncKafkaClient::with_client_id_and_security(
            hosts,
            "rustfs-kafka-async".to_owned(),
            security,
        )
        .await?;

        Ok(AsyncConsumer {
            mode: AsyncConsumerMode::Native(Box::new(NativeConsumer {
                client,
                group,
                topics,
                fallback_offset,
                offsets: HashMap::new(),
                dirty_offsets: HashMap::new(),
                leaders: HashMap::new(),
                coordinator: None,
                correlation: 1,
                retry_attempts: native_retry_attempts,
                retry_backoff: native_retry_backoff,
                observability: NativeConsumerObservability::new(native_recent_error_limit),
            })),
        })
    }
}

impl AsyncConsumer {
    /// Starts building a new async consumer from bootstrap hosts.
    #[must_use]
    pub fn builder(hosts: Vec<String>) -> AsyncConsumerBuilder {
        AsyncConsumerBuilder::new(hosts)
    }

    /// Creates a new async consumer from bootstrap hosts.
    pub async fn from_hosts(
        hosts: Vec<String>,
        group: String,
        topics: Vec<String>,
    ) -> Result<Self> {
        Self::builder(hosts)
            .with_group(group)
            .with_topics(topics)
            .build()
            .await
    }

    /// Creates a new async consumer from an [`AsyncKafkaClient`].
    pub async fn from_client(
        client: AsyncKafkaClient,
        group: String,
        topics: Vec<String>,
    ) -> Result<Self> {
        if group.is_empty() {
            return Err(Error::Consumer(ConsumerError::UnsetGroupId));
        }
        if topics.is_empty() {
            return Err(Error::Consumer(ConsumerError::NoTopicsAssigned));
        }
        Ok(Self {
            mode: AsyncConsumerMode::Native(Box::new(NativeConsumer {
                client,
                group,
                topics,
                fallback_offset: FetchOffset::Latest,
                offsets: HashMap::new(),
                dirty_offsets: HashMap::new(),
                leaders: HashMap::new(),
                coordinator: None,
                correlation: 1,
                retry_attempts: DEFAULT_NATIVE_RETRY_ATTEMPTS,
                retry_backoff: Duration::from_millis(DEFAULT_NATIVE_RETRY_BACKOFF_MS),
                observability: NativeConsumerObservability::default(),
            })),
        })
    }

    /// Polls for new messages and returns fetched message sets.
    pub async fn poll(&mut self) -> Result<MessageSets> {
        match &mut self.mode {
            AsyncConsumerMode::Native(native) => native.poll().await,
        }
    }

    /// Commits the current consumed offsets.
    pub async fn commit(&mut self) -> Result<()> {
        match &mut self.mode {
            AsyncConsumerMode::Native(native) => native.commit().await,
        }
    }

    /// Gracefully closes the consumer.
    pub async fn close(self) -> Result<()> {
        Ok(())
    }

    /// Returns native consumer error statistics when running in native mode.
    #[must_use]
    pub fn native_error_stats(&self) -> Option<NativeConsumerErrorStats> {
        match &self.mode {
            AsyncConsumerMode::Native(native) => Some(native.error_stats()),
        }
    }

    /// Resets native consumer error statistics.
    ///
    /// Returns `true` when reset was performed (native mode), otherwise `false`.
    pub fn reset_native_error_stats(&mut self) -> bool {
        match &mut self.mode {
            AsyncConsumerMode::Native(native) => {
                native.reset_error_stats();
                true
            }
        }
    }
}

impl NativeConsumer {
    async fn poll(&mut self) -> Result<MessageSets> {
        for attempt in 1..=self.retry_attempts {
            match self.poll_once().await {
                Ok(data) => return Ok(data),
                Err(err) => {
                    self.record_error("poll", &err);
                    let coordinator_error = is_coordinator_error(&err);
                    if coordinator_error {
                        // Also invalidate on the final attempt, so a later
                        // poll can discover the coordinator after it recovers.
                        self.coordinator = None;
                    }
                    if attempt == self.retry_attempts || !should_retry_poll(&err) {
                        return Err(err);
                    }
                    if !coordinator_error {
                        self.leaders.clear();
                        self.refresh_metadata().await?;
                    }
                    tokio::time::sleep(self.retry_backoff).await;
                }
            }
        }
        Err(Error::Kafka(KafkaCode::Unknown))
    }

    async fn poll_once(&mut self) -> Result<MessageSets> {
        self.client.ensure_connected().await?;
        if self.leaders.is_empty() {
            self.refresh_metadata().await?;
        }
        self.ensure_start_offsets().await?;

        let correlation = self.next_correlation();
        let mut by_broker: HashMap<&str, Vec<(&str, i32, i64, i32)>> = HashMap::new();
        for (tp, leader_host) in &self.leaders {
            let offset = self
                .offsets
                .get(tp.0.as_str())
                .and_then(|partitions| partitions.get(&tp.1))
                .copied()
                .unwrap_or(0);
            by_broker.entry(leader_host.as_str()).or_default().push((
                tp.0.as_str(),
                tp.1,
                offset,
                FETCH_PARTITION_MAX_BYTES,
            ));
        }

        let client_id = self.client.client_id().to_owned();
        let mut owned_responses = Vec::with_capacity(by_broker.len());

        for (broker, tps) in by_broker {
            let conn = self.client.get_connection(broker).await?;
            let (header, request) = build_fetch_request(correlation, &client_id, &tps);
            send_kp_request(conn, &header, &request, API_VERSION_FETCH).await?;
            let response = get_kp_response::<FetchResponse>(conn, API_VERSION_FETCH).await?;
            let mut owned = convert_fetch_response(response, correlation);
            validate_and_trim_fetch_response(&mut owned, &tps)?;

            owned_responses.push(owned);
        }

        // A failed or cancelled poll must not consume messages it never returns.
        // Publish progress only once every broker response has been collected.
        publish_response_offsets(&mut self.offsets, &mut self.dirty_offsets, &owned_responses)?;
        Ok(MessageSets::from_fetch_responses(owned_responses))
    }

    fn next_correlation(&mut self) -> i32 {
        let cid = self.correlation;
        self.correlation = self.correlation.wrapping_add(1);
        cid
    }

    async fn commit(&mut self) -> Result<()> {
        for attempt in 1..=self.retry_attempts {
            match self.commit_once().await {
                Ok(()) => return Ok(()),
                Err(err) if attempt < self.retry_attempts && should_retry_commit(&err) => {
                    self.record_error("commit", &err);
                    self.coordinator = None;
                    self.refresh_coordinator().await?;
                    tokio::time::sleep(self.retry_backoff).await;
                    continue;
                }
                Err(err) => {
                    self.record_error("commit", &err);
                    return Err(err);
                }
            }
        }
        Err(Error::Kafka(KafkaCode::Unknown))
    }

    async fn commit_once(&mut self) -> Result<()> {
        if self.dirty_offsets.is_empty() {
            return Ok(());
        }

        self.client.ensure_connected().await?;
        if self.coordinator.is_none() {
            self.refresh_coordinator().await?;
        }
        let Some(coordinator) = self.coordinator.clone() else {
            return Err(Error::Kafka(KafkaCode::GroupCoordinatorNotAvailable));
        };

        let client_id = self.client.client_id().to_owned();
        let correlation = self.next_correlation();
        let payload: Vec<(&str, i32, i64)> = self
            .dirty_offsets
            .iter()
            .flat_map(|(topic, partitions)| {
                partitions
                    .iter()
                    .map(move |(partition, offset)| (topic.as_str(), *partition, *offset))
            })
            .collect();

        let conn = self.client.get_connection(&coordinator).await?;
        let (header, request) =
            build_offset_commit_request(correlation, &client_id, &self.group, &payload);
        send_kp_request(conn, &header, &request, API_VERSION_OFFSET_COMMIT).await?;
        let response =
            get_kp_response::<OffsetCommitResponse>(conn, API_VERSION_OFFSET_COMMIT).await?;
        if let Err(error) = validate_commit_acknowledgments(&response, &payload) {
            conn.invalidate();
            return Err(error);
        }

        for topic in response.topics {
            for partition in topic.partitions {
                if partition.error_code != 0 {
                    if let Some(code) = map_kafka_code(partition.error_code) {
                        return Err(Error::Kafka(code));
                    }
                    return Err(Error::Kafka(KafkaCode::Unknown));
                }
            }
        }

        self.dirty_offsets.clear();
        Ok(())
    }

    async fn refresh_metadata(&mut self) -> Result<()> {
        let request_host = if let Some(connected) = self.client.connected_hosts().first() {
            (*connected).to_owned()
        } else {
            self.client
                .bootstrap_hosts()
                .first()
                .cloned()
                .ok_or_else(no_host_reachable_error)?
        };

        let correlation = self.next_correlation();
        let client_id = self.client.client_id().to_owned();
        let conn = self.client.get_connection(&request_host).await?;
        let (header, request) = build_metadata_request(correlation, &client_id, &self.topics);
        send_kp_request(conn, &header, &request, API_VERSION_METADATA).await?;
        let response = get_kp_response::<MetadataResponse>(conn, API_VERSION_METADATA).await?;

        let mut brokers: HashMap<i32, String> = HashMap::new();
        for broker in response.brokers {
            brokers.insert(
                i32::from(broker.node_id),
                format!("{}:{}", broker.host, broker.port),
            );
        }

        self.leaders.clear();
        for topic in response.topics {
            let Some(topic_name) = topic.name else {
                continue;
            };
            for partition in topic.partitions {
                let leader = i32::from(partition.leader_id);
                if leader < 0 {
                    continue;
                }
                if let Some(host) = brokers.get(&leader) {
                    let tp = (topic_name.to_string(), partition.partition_index);
                    self.leaders.insert(tp, host.clone());
                }
            }
        }

        if self.leaders.is_empty() {
            return Err(Error::Kafka(KafkaCode::LeaderNotAvailable));
        }

        Ok(())
    }

    async fn refresh_coordinator(&mut self) -> Result<()> {
        let request_host = if let Some(connected) = self.client.connected_hosts().first() {
            (*connected).to_owned()
        } else {
            self.client
                .bootstrap_hosts()
                .first()
                .cloned()
                .ok_or_else(no_host_reachable_error)?
        };

        let correlation = self.next_correlation();
        let client_id = self.client.client_id().to_owned();
        let conn = self.client.get_connection(&request_host).await?;
        let (header, request) =
            build_find_coordinator_request(correlation, &client_id, &self.group);
        send_kp_request(conn, &header, &request, API_VERSION_FIND_COORDINATOR).await?;
        let response =
            get_kp_response::<FindCoordinatorResponse>(conn, API_VERSION_FIND_COORDINATOR).await?;

        let (error_code, host, port) = if let Some(c) = response.coordinators.first() {
            (c.error_code, c.host.to_string(), c.port)
        } else {
            (
                response.error_code,
                response.host.to_string(),
                response.port,
            )
        };

        if error_code != 0 {
            if let Some(code) = map_kafka_code(error_code) {
                return Err(Error::Kafka(code));
            }
            return Err(Error::Kafka(KafkaCode::Unknown));
        }

        self.coordinator = Some(format!("{host}:{port}"));
        Ok(())
    }

    async fn ensure_start_offsets(&mut self) -> Result<()> {
        let missing: Vec<(String, i32)> = self
            .leaders
            .keys()
            .filter(|tp| {
                !self
                    .offsets
                    .get(tp.0.as_str())
                    .is_some_and(|partitions| partitions.contains_key(&tp.1))
            })
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(());
        }

        self.client.ensure_connected().await?;
        if self.coordinator.is_none() {
            self.refresh_coordinator().await?;
        }

        let committed = self.fetch_committed_offsets(&missing).await?;
        for tp in missing {
            if let Some(offset) = committed
                .get(tp.0.as_str())
                .and_then(|partitions| partitions.get(&tp.1))
                && *offset >= 0
            {
                insert_topic_offset(&mut self.offsets, &tp.0, tp.1, *offset);
                continue;
            }

            let fallback = self.resolve_fallback_offset(&tp).await?;
            insert_topic_offset(&mut self.offsets, &tp.0, tp.1, fallback);
        }

        Ok(())
    }

    async fn fetch_committed_offsets(
        &mut self,
        partitions: &[(String, i32)],
    ) -> Result<TopicOffsets> {
        let Some(coordinator) = self.coordinator.clone() else {
            return Err(Error::Kafka(KafkaCode::GroupCoordinatorNotAvailable));
        };

        let client_id = self.client.client_id().to_owned();
        let correlation = self.next_correlation();
        let req_parts: Vec<(&str, i32)> = partitions
            .iter()
            .map(|(topic, partition)| (topic.as_str(), *partition))
            .collect();

        let conn = self.client.get_connection(&coordinator).await?;
        let (header, request) =
            build_offset_fetch_request(correlation, &client_id, &self.group, &req_parts);
        send_kp_request(conn, &header, &request, API_VERSION_OFFSET_FETCH).await?;
        let response =
            get_kp_response::<OffsetFetchResponse>(conn, API_VERSION_OFFSET_FETCH).await?;
        if response.error_code != 0 {
            return Err(Error::Kafka(
                map_kafka_code(response.error_code).unwrap_or(KafkaCode::Unknown),
            ));
        }
        if let Err(error) = validate_offset_fetch_acknowledgments(&response, &req_parts) {
            conn.invalidate();
            return Err(error);
        }

        let mut committed = HashMap::new();
        for topic in response.topics {
            for partition in topic.partitions {
                if partition.error_code != 0 {
                    if let Some(code) = map_kafka_code(partition.error_code) {
                        return Err(Error::Kafka(code));
                    }
                    return Err(Error::Kafka(KafkaCode::Unknown));
                }
                insert_topic_offset(
                    &mut committed,
                    topic.name.as_str(),
                    partition.partition_index,
                    partition.committed_offset,
                );
            }
        }
        Ok(committed)
    }

    async fn resolve_fallback_offset(&mut self, tp: &(String, i32)) -> Result<i64> {
        let Some(leader) = self.leaders.get(tp).cloned() else {
            return Err(Error::Kafka(KafkaCode::LeaderNotAvailable));
        };

        let timestamp = match self.fallback_offset {
            FetchOffset::Earliest => -2,
            FetchOffset::Latest => -1,
            FetchOffset::ByTime(t) => t,
        };

        let correlation = self.next_correlation();
        let client_id = self.client.client_id().to_owned();
        let conn = self.client.get_connection(&leader).await?;
        let (header, request) = build_list_offsets_request(
            correlation,
            &client_id,
            &[(tp.0.as_str(), tp.1, timestamp)],
        );
        send_kp_request(conn, &header, &request, API_VERSION_LIST_OFFSETS).await?;
        let response =
            get_kp_response::<ListOffsetsResponse>(conn, API_VERSION_LIST_OFFSETS).await?;

        for topic in response.topics {
            if topic.name.as_str() != tp.0.as_str() {
                continue;
            }
            for partition in topic.partitions {
                if partition.partition_index != tp.1 {
                    continue;
                }
                if partition.error_code != 0 {
                    if let Some(code) = map_kafka_code(partition.error_code) {
                        return Err(Error::Kafka(code));
                    }
                    return Err(Error::Kafka(KafkaCode::Unknown));
                }
                return Ok(partition.offset);
            }
        }

        Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition))
    }

    fn error_stats(&self) -> NativeConsumerErrorStats {
        self.observability.stats()
    }

    fn reset_error_stats(&mut self) {
        self.observability.clear();
    }

    fn record_error(&mut self, phase: &str, err: &Error) {
        self.observability.record_error(phase, err);
    }
}

fn build_metadata_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[String],
) -> (RequestHeader, MetadataRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::Metadata as i16)
        .with_request_api_version(API_VERSION_METADATA)
        .with_correlation_id(correlation_id);

    let request_topics: Vec<MetadataRequestTopic> = topics
        .iter()
        .map(|topic| {
            MetadataRequestTopic::default()
                .with_name(Some(TopicName::from(StrBytes::from_string(topic.clone()))))
        })
        .collect();

    let request = MetadataRequest::default().with_topics(Some(request_topics));
    (header, request)
}

fn build_fetch_request(
    correlation_id: i32,
    client_id: &str,
    partitions: &[(&str, i32, i64, i32)],
) -> (RequestHeader, FetchRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::Fetch as i16)
        .with_request_api_version(API_VERSION_FETCH)
        .with_correlation_id(correlation_id);

    let mut topic_map: HashMap<&str, Vec<KpFetchPartition>> = HashMap::new();
    for (topic, partition, offset, partition_max_bytes) in partitions {
        topic_map.entry(topic).or_default().push(
            KpFetchPartition::default()
                .with_partition(*partition)
                .with_fetch_offset(*offset)
                .with_partition_max_bytes(*partition_max_bytes),
        );
    }

    let topics: Vec<KpFetchTopic> = topic_map
        .into_iter()
        .map(|(topic_name, fetch_partitions)| {
            KpFetchTopic::default()
                .with_topic(TopicName::from(StrBytes::from_string(
                    topic_name.to_string(),
                )))
                .with_partitions(fetch_partitions)
        })
        .collect();

    let request = FetchRequest::default()
        .with_replica_id(kafka_protocol::messages::BrokerId::from(-1))
        .with_max_wait_ms(FETCH_MAX_WAIT_MS)
        .with_min_bytes(FETCH_MIN_BYTES)
        .with_max_bytes(i32::MAX)
        .with_isolation_level(0)
        .with_topics(topics);

    (header, request)
}

fn build_find_coordinator_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
) -> (RequestHeader, FindCoordinatorRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::FindCoordinator as i16)
        .with_request_api_version(API_VERSION_FIND_COORDINATOR)
        .with_correlation_id(correlation_id);

    let request = FindCoordinatorRequest::default()
        .with_key(StrBytes::from_string(group_id.to_owned()))
        .with_key_type(0);

    (header, request)
}

fn build_offset_commit_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    offsets: &[(&str, i32, i64)],
) -> (RequestHeader, OffsetCommitRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::OffsetCommit as i16)
        .with_request_api_version(API_VERSION_OFFSET_COMMIT)
        .with_correlation_id(correlation_id);

    let mut topic_map: HashMap<&str, Vec<OffsetCommitRequestPartition>> = HashMap::new();
    for (topic, partition, offset) in offsets {
        topic_map.entry(topic).or_default().push(
            OffsetCommitRequestPartition::default()
                .with_partition_index(*partition)
                .with_committed_offset(*offset)
                .with_committed_metadata(None),
        );
    }

    let topics: Vec<OffsetCommitRequestTopic> = topic_map
        .into_iter()
        .map(|(name, partitions)| {
            OffsetCommitRequestTopic::default()
                .with_name(TopicName::from(StrBytes::from_string(name.to_string())))
                .with_partitions(partitions)
        })
        .collect();

    let request = OffsetCommitRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_string(group_id.to_owned())))
        .with_generation_id_or_member_epoch(-1)
        .with_member_id(StrBytes::from_string(String::new()))
        .with_retention_time_ms(-1)
        .with_topics(topics);

    (header, request)
}

fn build_offset_fetch_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    partitions: &[(&str, i32)],
) -> (RequestHeader, OffsetFetchRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::OffsetFetch as i16)
        .with_request_api_version(API_VERSION_OFFSET_FETCH)
        .with_correlation_id(correlation_id);

    let mut topic_map: HashMap<&str, Vec<i32>> = HashMap::new();
    for (topic, partition) in partitions {
        topic_map.entry(topic).or_default().push(*partition);
    }

    let topics: Vec<OffsetFetchRequestTopic> = topic_map
        .into_iter()
        .map(|(topic, partition_indexes)| {
            OffsetFetchRequestTopic::default()
                .with_name(TopicName::from(StrBytes::from_string(topic.to_owned())))
                .with_partition_indexes(partition_indexes)
        })
        .collect();

    let request = OffsetFetchRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_string(group_id.to_owned())))
        .with_topics(Some(topics));
    (header, request)
}

fn build_list_offsets_request(
    correlation_id: i32,
    client_id: &str,
    partitions: &[(&str, i32, i64)],
) -> (RequestHeader, ListOffsetsRequest) {
    let header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(ApiKey::ListOffsets as i16)
        .with_request_api_version(API_VERSION_LIST_OFFSETS)
        .with_correlation_id(correlation_id);

    let mut topic_map: HashMap<&str, Vec<ListOffsetsPartition>> = HashMap::new();
    for (topic, partition, timestamp) in partitions {
        topic_map.entry(topic).or_default().push(
            ListOffsetsPartition::default()
                .with_partition_index(*partition)
                .with_timestamp(*timestamp),
        );
    }

    let topics: Vec<ListOffsetsTopic> = topic_map
        .into_iter()
        .map(|(topic, parts)| {
            ListOffsetsTopic::default()
                .with_name(TopicName::from(StrBytes::from_string(topic.to_owned())))
                .with_partitions(parts)
        })
        .collect();

    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_isolation_level(0)
        .with_topics(topics);
    (header, request)
}

fn insert_topic_offset(offsets: &mut TopicOffsets, topic: &str, partition: i32, offset: i64) {
    if let Some(partitions) = offsets.get_mut(topic) {
        partitions.insert(partition, offset);
    } else {
        offsets.insert(topic.to_owned(), HashMap::from([(partition, offset)]));
    }
}

fn fetch_partition_error(err: &Arc<Error>) -> Error {
    match &**err {
        Error::TopicPartitionError { error_code, .. } => Error::Kafka(*error_code),
        _ => Error::from(Arc::clone(err)),
    }
}

fn validate_commit_acknowledgments(
    response: &OffsetCommitResponse,
    requested: &[(&str, i32, i64)],
) -> Result<()> {
    validate_acknowledged_partitions(
        requested
            .iter()
            .map(|&(topic, partition, _)| (topic, partition)),
        response.topics.iter().map(|topic| {
            (
                topic.name.as_str(),
                topic
                    .partitions
                    .iter()
                    .map(|partition| partition.partition_index),
            )
        }),
    )
}

fn validate_offset_fetch_acknowledgments(
    response: &OffsetFetchResponse,
    requested: &[(&str, i32)],
) -> Result<()> {
    validate_acknowledged_partitions(
        requested.iter().copied(),
        response.topics.iter().map(|topic| {
            (
                topic.name.as_str(),
                topic
                    .partitions
                    .iter()
                    .map(|partition| partition.partition_index),
            )
        }),
    )?;
    // -1 denotes an explicitly unset position. Validate all successful-RPC
    // positions before the caller can initialize state or resolve fallbacks.
    if response.topics.iter().any(|topic| {
        topic
            .partitions
            .iter()
            .any(|partition| partition.committed_offset < -1)
    }) {
        return Err(Error::Protocol(ProtocolError::Codec));
    }
    Ok(())
}

fn validate_acknowledged_partitions<'a, 'b, P>(
    requested: impl IntoIterator<Item = (&'a str, i32)>,
    received: impl IntoIterator<Item = (&'b str, P)>,
) -> Result<()>
where
    P: IntoIterator<Item = i32>,
{
    let mut remaining: HashMap<&str, HashSet<i32>> = HashMap::new();
    for (topic, partition) in requested {
        if !remaining.entry(topic).or_default().insert(partition) {
            return Err(Error::Protocol(ProtocolError::Codec));
        }
    }
    for (topic, returned_partitions) in received {
        let mut partitions = remaining
            .remove(topic)
            .ok_or(Error::Protocol(ProtocolError::Codec))?;
        for partition in returned_partitions {
            if !partitions.remove(&partition) {
                return Err(Error::Protocol(ProtocolError::Codec));
            }
        }
        if !partitions.is_empty() {
            return Err(Error::Protocol(ProtocolError::Codec));
        }
    }
    if !remaining.is_empty() {
        return Err(Error::Protocol(ProtocolError::Codec));
    }
    Ok(())
}

enum RequestedFetchOffsets {
    Dense {
        offsets: Vec<Option<i64>>,
        remaining: usize,
    },
    Sparse(HashMap<i32, i64>),
}

impl RequestedFetchOffsets {
    fn new(requested: Vec<(i32, i64)>) -> Result<Self> {
        let dense_width = requested.iter().try_fold(0usize, |width, &(partition, _)| {
            let end = usize::try_from(partition).ok()?.checked_add(1)?;
            Some(width.max(end))
        });
        if let Some(width) = dense_width.filter(|width| *width <= requested.len().saturating_mul(2))
        {
            // Capacity is bounded by the request count, never a response ID.
            let mut offsets = vec![None; width];
            for &(partition, offset) in &requested {
                let slot = &mut offsets[usize::try_from(partition)
                    .map_err(|_| Error::Protocol(ProtocolError::Codec))?];
                if slot.replace(offset).is_some() {
                    return Err(Error::Protocol(ProtocolError::Codec));
                }
            }
            Ok(Self::Dense {
                offsets,
                remaining: requested.len(),
            })
        } else {
            let mut offsets = HashMap::with_capacity(requested.len());
            for (partition, offset) in requested {
                if offsets.insert(partition, offset).is_some() {
                    return Err(Error::Protocol(ProtocolError::Codec));
                }
            }
            Ok(Self::Sparse(offsets))
        }
    }

    fn take(&mut self, partition: i32) -> Option<i64> {
        match self {
            Self::Dense { offsets, remaining } => {
                let offset = offsets.get_mut(usize::try_from(partition).ok()?)?.take()?;
                *remaining -= 1;
                Some(offset)
            }
            Self::Sparse(offsets) => offsets.remove(&partition),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Dense { remaining, .. } => *remaining == 0,
            Self::Sparse(offsets) => offsets.is_empty(),
        }
    }
}

fn validate_and_trim_fetch_response(
    response: &mut OwnedFetchResponse,
    requested: &[(&str, i32, i64, i32)],
) -> Result<()> {
    let mut grouped: HashMap<&str, Vec<(i32, i64)>> = HashMap::new();
    for run in requested.chunk_by(|left, right| left.0 == right.0) {
        grouped.entry(run[0].0).or_default().extend(
            run.iter()
                .map(|&(_, partition, offset, _)| (partition, offset)),
        );
    }
    let mut remaining = HashMap::with_capacity(grouped.len());
    for (topic, partitions) in grouped {
        remaining.insert(topic, RequestedFetchOffsets::new(partitions)?);
    }
    for topic in &mut response.topics {
        let mut partitions = remaining
            .remove(topic.topic.as_str())
            .ok_or(Error::Protocol(ProtocolError::Codec))?;
        for partition in &mut topic.partitions {
            let requested_offset = partitions
                .take(partition.partition)
                .ok_or(Error::Protocol(ProtocolError::Codec))?;
            if let Ok(data) = &mut partition.data {
                // Validate even records that would be trimmed: malformed
                // offsets cannot be hidden inside a whole batch's prefix.
                if data
                    .messages
                    .iter()
                    .any(|message| !(0..i64::MAX).contains(&message.offset))
                {
                    return Err(Error::Protocol(ProtocolError::Codec));
                }
                // Brokers may return a whole batch before fetch_offset.
                data.messages
                    .retain(|message| message.offset >= requested_offset);
            }
        }
        if !partitions.is_empty() {
            return Err(Error::Protocol(ProtocolError::Codec));
        }
    }
    if !remaining.is_empty() {
        return Err(Error::Protocol(ProtocolError::Codec));
    }
    // Malformed identities or numeric offsets take precedence over a broker's
    // retriable error; retrying must not hide a malformed response envelope.
    for topic in &response.topics {
        for partition in &topic.partitions {
            partition.data().map_err(fetch_partition_error)?;
        }
    }
    Ok(())
}

fn publish_response_offsets(
    offsets: &mut TopicOffsets,
    dirty_offsets: &mut TopicOffsets,
    responses: &[OwnedFetchResponse],
) -> Result<()> {
    // Stage every next offset before mutating either map. No await or fallible
    // protocol validation remains once publication starts.
    let mut staged: HashMap<&str, Vec<(i32, i64)>> = HashMap::new();
    for response in responses {
        for topic in &response.topics {
            let mut updates = Vec::new();
            for partition in &topic.partitions {
                let data = partition.data().map_err(fetch_partition_error)?;
                if let Some(last) = data.messages.last() {
                    let next_offset = last
                        .offset
                        .checked_add(1)
                        .filter(|offset| *offset > 0)
                        .ok_or(Error::Protocol(ProtocolError::Codec))?;
                    if updates.is_empty() {
                        updates.reserve(topic.partitions.len());
                    }
                    updates.push((partition.partition, next_offset));
                }
            }
            if !updates.is_empty() {
                match staged.entry(topic.topic.as_str()) {
                    Entry::Occupied(mut entry) => entry.get_mut().extend(updates),
                    Entry::Vacant(entry) => {
                        entry.insert(updates);
                    }
                }
            }
        }
    }
    for (topic, updates) in staged {
        publish_topic_offsets(offsets, topic, &updates);
        publish_topic_offsets(dirty_offsets, topic, &updates);
    }
    Ok(())
}

fn publish_topic_offsets(offsets: &mut TopicOffsets, topic: &str, staged: &[(i32, i64)]) {
    if let Some(partitions) = offsets.get_mut(topic) {
        for &(partition, next_offset) in staged {
            partitions.insert(partition, next_offset);
        }
    } else {
        offsets.insert(topic.to_owned(), staged.iter().copied().collect());
    }
}

fn should_retry_poll(err: &Error) -> bool {
    if is_coordinator_error(err) {
        return true;
    }
    match err {
        Error::Kafka(code) => matches!(
            code,
            KafkaCode::LeaderNotAvailable
                | KafkaCode::NotLeaderForPartition
                | KafkaCode::RequestTimedOut
                | KafkaCode::NetworkException
        ),
        Error::Connection(_) => true,
        _ => false,
    }
}

fn is_coordinator_error(err: &Error) -> bool {
    matches!(
        err,
        Error::Kafka(
            KafkaCode::GroupCoordinatorNotAvailable
                | KafkaCode::NotCoordinatorForGroup
                | KafkaCode::GroupLoadInProgress
        )
    )
}

fn should_retry_commit(err: &Error) -> bool {
    match err {
        Error::Kafka(code) => matches!(
            code,
            KafkaCode::GroupCoordinatorNotAvailable
                | KafkaCode::NotCoordinatorForGroup
                | KafkaCode::GroupLoadInProgress
                | KafkaCode::RequestTimedOut
                | KafkaCode::NetworkException
        ),
        Error::Connection(_) => true,
        _ => false,
    }
}

fn no_host_reachable_error() -> Error {
    Error::Connection(rustfs_kafka::error::ConnectionError::NoHostReachable)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::{Buf, Bytes, BytesMut};
    use kafka_protocol::messages::ResponseHeader;
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
    use kafka_protocol::messages::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::offset_commit_response::{
        OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    };
    use kafka_protocol::messages::offset_fetch_response::{
        OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};
    use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};
    use rustfs_kafka::error::{ConnectionError, Error, ProtocolError};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Notify;

    use super::*;

    const TEST_TOPIC: &str = "offset-test";
    const TEST_GROUP: &str = "offset-group";
    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    async fn checked<F: Future>(future: F) -> F::Output {
        tokio::time::timeout(TEST_TIMEOUT, future)
            .await
            .expect("mock broker operation timed out")
    }

    fn test_topic() -> TopicName {
        TopicName::from(StrBytes::from_string(TEST_TOPIC.to_owned()))
    }

    async fn read_request<T>(
        socket: &mut TcpStream,
        api_key: ApiKey,
        version: i16,
    ) -> (RequestHeader, T)
    where
        T: Decodable + HeaderVersion,
    {
        let size = checked(socket.read_i32()).await.unwrap();
        let mut frame = vec![0; usize::try_from(size).unwrap()];
        checked(socket.read_exact(&mut frame)).await.unwrap();
        let mut frame = Bytes::from(frame);
        let header = RequestHeader::decode(&mut frame, T::header_version(version)).unwrap();
        assert_eq!(header.request_api_key, api_key as i16);
        assert_eq!(header.request_api_version, version);
        let request = T::decode(&mut frame, version).unwrap();
        assert!(!frame.has_remaining());
        (header, request)
    }

    async fn reply<T>(socket: &mut TcpStream, header: &RequestHeader, version: i16, response: T)
    where
        T: Encodable + HeaderVersion,
    {
        let mut frame = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(header.correlation_id)
            .encode(&mut frame, T::header_version(version))
            .unwrap();
        response.encode(&mut frame, version).unwrap();
        checked(socket.write_i32(i32::try_from(frame.len()).unwrap()))
            .await
            .unwrap();
        checked(socket.write_all(&frame)).await.unwrap();
    }

    async fn serve_metadata(socket: &mut TcpStream, brokers: &[SocketAddr]) {
        let (header, request) =
            read_request::<MetadataRequest>(socket, ApiKey::Metadata, API_VERSION_METADATA).await;
        assert_eq!(request.topics.unwrap()[0].name, Some(test_topic()));
        let response = MetadataResponse::default()
            .with_brokers(
                brokers
                    .iter()
                    .enumerate()
                    .map(|(index, addr)| {
                        MetadataResponseBroker::default()
                            .with_node_id(BrokerId::from(i32::try_from(index).unwrap()))
                            .with_host(StrBytes::from_string(addr.ip().to_string()))
                            .with_port(i32::from(addr.port()))
                    })
                    .collect(),
            )
            .with_topics(vec![
                MetadataResponseTopic::default()
                    .with_name(Some(test_topic()))
                    .with_partitions(
                        (0..brokers.len())
                            .map(|index| {
                                let index = i32::try_from(index).unwrap();
                                MetadataResponsePartition::default()
                                    .with_partition_index(index)
                                    .with_leader_id(BrokerId::from(index))
                            })
                            .collect(),
                    ),
            ]);
        reply(socket, &header, API_VERSION_METADATA, response).await;
    }

    async fn serve_initialization(
        socket: &mut TcpStream,
        brokers: &[SocketAddr],
        committed_offsets: &[i64],
        offset_fetch_error: i16,
    ) {
        serve_metadata(socket, brokers).await;
        serve_coordinator(socket, brokers[0]).await;
        serve_committed_offsets(socket, brokers.len(), committed_offsets, offset_fetch_error).await;
    }

    async fn serve_coordinator(socket: &mut TcpStream, coordinator: SocketAddr) {
        let (header, request) = read_request::<FindCoordinatorRequest>(
            socket,
            ApiKey::FindCoordinator,
            API_VERSION_FIND_COORDINATOR,
        )
        .await;
        assert_eq!(request.key.as_str(), TEST_GROUP);
        reply(
            socket,
            &header,
            API_VERSION_FIND_COORDINATOR,
            FindCoordinatorResponse::default()
                .with_host(StrBytes::from_string(coordinator.ip().to_string()))
                .with_port(i32::from(coordinator.port())),
        )
        .await;
    }

    async fn serve_committed_offsets(
        socket: &mut TcpStream,
        partition_count: usize,
        committed_offsets: &[i64],
        offset_fetch_error: i16,
    ) {
        let (header, request) = read_request::<OffsetFetchRequest>(
            socket,
            ApiKey::OffsetFetch,
            API_VERSION_OFFSET_FETCH,
        )
        .await;
        assert_eq!(request.group_id.as_str(), TEST_GROUP);
        let topics = request.topics.unwrap();
        assert_eq!(topics[0].name, test_topic());
        let mut partitions = topics[0].partition_indexes.clone();
        partitions.sort_unstable();
        assert_eq!(
            partitions,
            (0..i32::try_from(partition_count).unwrap()).collect::<Vec<_>>()
        );
        reply(
            socket,
            &header,
            API_VERSION_OFFSET_FETCH,
            OffsetFetchResponse::default()
                .with_error_code(offset_fetch_error)
                .with_topics(if offset_fetch_error == 0 {
                    vec![
                        OffsetFetchResponseTopic::default()
                            .with_name(test_topic())
                            .with_partitions(
                                committed_offsets
                                    .iter()
                                    .enumerate()
                                    .map(|(index, offset)| {
                                        OffsetFetchResponsePartition::default()
                                            .with_partition_index(i32::try_from(index).unwrap())
                                            .with_committed_offset(*offset)
                                    })
                                    .collect(),
                            ),
                    ]
                } else {
                    Vec::new()
                }),
        )
        .await;
    }

    fn encoded_record_batch(
        offset: i64,
        compression: kafka_protocol::records::Compression,
    ) -> Bytes {
        encoded_record_batch_offsets(&[offset], compression)
    }

    fn encoded_record_batch_offsets(
        offsets: &[i64],
        compression: kafka_protocol::records::Compression,
    ) -> Bytes {
        let records: Vec<_> = offsets
            .iter()
            .map(|&offset| Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset,
                sequence: -1,
                timestamp: 0,
                key: None,
                value: Some(Bytes::from_static(b"delivered")),
                headers: Default::default(),
            })
            .collect();
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .unwrap();
        encoded.freeze()
    }

    fn fetch_response(partition: i32, offset: i64, error_code: i16) -> FetchResponse {
        FetchResponse::default().with_responses(vec![
            FetchableTopicResponse::default()
                .with_topic(test_topic())
                .with_partitions(vec![
                    PartitionData::default()
                        .with_partition_index(partition)
                        .with_error_code(error_code)
                        .with_high_watermark(offset + 1)
                        .with_records(Some(encoded_record_batch(
                            offset,
                            kafka_protocol::records::Compression::None,
                        ))),
                ]),
        ])
    }

    fn response_for_requested_layout(topic: &str, partitions: &[i32]) -> OwnedFetchResponse {
        let partitions = partitions
            .iter()
            .rev()
            .map(|&partition| {
                PartitionData::default()
                    .with_partition_index(partition)
                    .with_high_watermark(42)
                    .with_records(Some(encoded_record_batch_offsets(
                        &[39, 41],
                        kafka_protocol::records::Compression::None,
                    )))
            })
            .collect();
        convert_fetch_response(
            FetchResponse::default().with_responses(vec![
                FetchableTopicResponse::default()
                    .with_topic(TopicName::from(StrBytes::from_string(topic.to_owned())))
                    .with_partitions(partitions),
            ]),
            7,
        )
    }

    #[test]
    fn dense_requested_offsets_are_bounded_and_consume_each_slot_once() {
        let mut requested = RequestedFetchOffsets::new(vec![(1, -1), (3, i64::MAX)]).unwrap();
        assert!(
            matches!(&requested, RequestedFetchOffsets::Dense { offsets, remaining } if offsets.len() == 4 && *remaining == 2)
        );
        assert_eq!(requested.take(i32::MAX), None);
        assert_eq!(requested.take(-1), None);
        assert_eq!(requested.take(0), None);
        assert_eq!(requested.take(1), Some(-1));
        assert_eq!(requested.take(1), None);
        assert!(!requested.is_empty());
        assert_eq!(requested.take(3), Some(i64::MAX));
        assert!(requested.is_empty());
    }

    #[test]
    fn sparse_or_large_requested_ids_do_not_allocate_a_dense_id_range() {
        for ids in [[1, 4], [0, i32::MAX], [-1, 0]] {
            let mut requested = RequestedFetchOffsets::new(
                ids.into_iter().map(|partition| (partition, 40)).collect(),
            )
            .unwrap();
            assert!(
                matches!(&requested, RequestedFetchOffsets::Sparse(offsets) if offsets.len() == 2)
            );
            for partition in ids {
                assert_eq!(requested.take(partition), Some(40));
                assert_eq!(requested.take(partition), None);
            }
            assert!(requested.is_empty());
        }
    }

    #[test]
    fn duplicate_requested_ids_are_rejected_for_dense_and_sparse_layouts() {
        for partition in [0, i32::MAX, -1] {
            assert!(matches!(
                RequestedFetchOffsets::new(vec![(partition, 40), (partition, 41)]),
                Err(Error::Protocol(ProtocolError::Codec))
            ));
        }
    }

    #[test]
    fn dense_sparse_and_large_layouts_validate_trim_and_publish_identical_progress() {
        for partitions in [
            vec![0, 1, 2],
            vec![1, 3],
            vec![0, 100],
            vec![i32::MAX - 1, i32::MAX],
            vec![-1, 4],
        ] {
            let requested: Vec<_> = partitions
                .iter()
                .map(|&partition| (TEST_TOPIC, partition, 40, FETCH_PARTITION_MAX_BYTES))
                .collect();
            let mut response = response_for_requested_layout(TEST_TOPIC, &partitions);
            validate_and_trim_fetch_response(&mut response, &requested).unwrap();
            for partition in &response.topics[0].partitions {
                assert_eq!(
                    partition
                        .data()
                        .unwrap()
                        .messages
                        .iter()
                        .map(|message| message.offset)
                        .collect::<Vec<_>>(),
                    [41]
                );
            }
            let mut offsets = HashMap::new();
            let mut dirty = HashMap::new();
            publish_response_offsets(&mut offsets, &mut dirty, std::slice::from_ref(&response))
                .unwrap();
            let expected = HashMap::from([(
                TEST_TOPIC.to_owned(),
                partitions
                    .iter()
                    .map(|&partition| (partition, 42))
                    .collect::<HashMap<_, _>>(),
            )]);
            assert_eq!(offsets, expected);
            assert_eq!(dirty, expected);
        }
    }

    #[test]
    fn adaptive_layouts_reject_missing_duplicate_extra_and_invalid_offsets() {
        for partitions in [vec![0, 2], vec![0, 100], vec![i32::MAX - 1, i32::MAX]] {
            let requested: Vec<_> = partitions
                .iter()
                .map(|&partition| (TEST_TOPIC, partition, 40, FETCH_PARTITION_MAX_BYTES))
                .collect();
            for fault in 0..4 {
                let mut response = response_for_requested_layout(TEST_TOPIC, &partitions);
                let returned = &mut response.topics[0].partitions;
                match fault {
                    0 => {
                        returned.pop();
                    }
                    1 => {
                        let duplicate = response_for_requested_layout(TEST_TOPIC, &partitions)
                            .topics
                            .remove(0)
                            .partitions
                            .remove(0);
                        returned.push(duplicate);
                    }
                    2 => {
                        returned[0].partition = i32::MIN;
                    }
                    _ => {
                        returned[0].data.as_mut().unwrap().messages[0].offset = -1;
                    }
                }
                assert!(matches!(
                    validate_and_trim_fetch_response(&mut response, &requested),
                    Err(Error::Protocol(ProtocolError::Codec))
                ));
            }
        }
    }

    #[test]
    fn interleaved_topics_and_repeated_topic_responses_publish_each_topic_together() {
        let requested = [
            (TEST_TOPIC, 0, 40, FETCH_PARTITION_MAX_BYTES),
            (OTHER_TOPIC, 0, 40, FETCH_PARTITION_MAX_BYTES),
            (TEST_TOPIC, 2, 40, FETCH_PARTITION_MAX_BYTES),
            (OTHER_TOPIC, i32::MAX, 40, FETCH_PARTITION_MAX_BYTES),
        ];
        let mut response = response_for_requested_layout(OTHER_TOPIC, &[0, i32::MAX]);
        response
            .topics
            .extend(response_for_requested_layout(TEST_TOPIC, &[0, 2]).topics);
        validate_and_trim_fetch_response(&mut response, &requested).unwrap();
        let mut other_broker = response_for_requested_layout(TEST_TOPIC, &[3]);
        validate_and_trim_fetch_response(
            &mut other_broker,
            &[(TEST_TOPIC, 3, 40, FETCH_PARTITION_MAX_BYTES)],
        )
        .unwrap();
        let mut offsets = HashMap::from([
            (
                TEST_TOPIC.to_owned(),
                HashMap::from([(0, 0), (2, 0), (3, 0)]),
            ),
            (
                OTHER_TOPIC.to_owned(),
                HashMap::from([(0, 0), (i32::MAX, 0)]),
            ),
        ]);
        let mut dirty = offsets.clone();
        publish_response_offsets(&mut offsets, &mut dirty, &[response, other_broker]).unwrap();
        assert_eq!(
            offsets[TEST_TOPIC],
            HashMap::from([(0, 42), (2, 42), (3, 42)])
        );
        assert_eq!(
            offsets[OTHER_TOPIC],
            HashMap::from([(0, 42), (i32::MAX, 42)])
        );
        assert_eq!(dirty, offsets);
    }

    #[test]
    fn grouped_publication_validates_every_topic_before_mutating_either_map() {
        let first = response_for_requested_layout(TEST_TOPIC, &[0, 1]);
        let mut later = response_for_requested_layout(OTHER_TOPIC, &[100]);
        later.topics[0].partitions[0]
            .data
            .as_mut()
            .unwrap()
            .messages
            .last_mut()
            .unwrap()
            .offset = i64::MAX;
        let mut offsets = HashMap::from([(TEST_TOPIC.to_owned(), HashMap::from([(0, 9), (1, 9)]))]);
        let mut dirty = offsets.clone();
        let original = offsets.clone();
        assert!(matches!(
            publish_response_offsets(&mut offsets, &mut dirty, &[first, later]),
            Err(Error::Protocol(ProtocolError::Codec))
        ));
        assert_eq!(offsets, original);
        assert_eq!(dirty, original);
    }

    async fn assert_poll_returns_batches(
        records: Bytes,
        start_offset: i64,
        expected_offsets: &[i64],
        next_offset: i64,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            serve_initialization(&mut socket, &[addr], &[start_offset], 0).await;
            let (header, request) =
                read_request::<FetchRequest>(&mut socket, ApiKey::Fetch, API_VERSION_FETCH).await;
            assert_eq!(request.topics[0].partitions[0].fetch_offset, start_offset);
            let mut response = fetch_response(0, start_offset, 0);
            let partition = &mut response.responses[0].partitions[0];
            partition.records = Some(records);
            partition.high_watermark = next_offset;
            reply(&mut socket, &header, API_VERSION_FETCH, response).await;
            serve_commit(&mut socket, &[(0, next_offset)]).await;
        });
        let mut consumer = consumer_at(addr, FetchOffset::Latest).await;
        let messages = checked(consumer.poll()).await.unwrap();
        let set = messages.iter().next().unwrap();
        assert_eq!(
            set.messages()
                .iter()
                .map(|message| message.offset)
                .collect::<Vec<_>>(),
            expected_offsets
        );
        assert!(
            set.messages().iter().all(
                |message| message.key.is_empty() && message.value.as_ref() == &b"delivered"[..]
            )
        );
        checked(consumer.commit()).await.unwrap();
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn poll_delivers_all_record_batches_and_commits_the_last_next_offset() {
        let mut records = BytesMut::new();
        for offset in [4, 5] {
            records.extend_from_slice(&encoded_record_batch(
                offset,
                kafka_protocol::records::Compression::None,
            ));
        }
        assert_poll_returns_batches(records.freeze(), 4, &[4, 5], 6).await;
    }

    #[tokio::test]
    async fn poll_trims_a_whole_batch_before_the_committed_offset() {
        let records = encoded_record_batch_offsets(
            &[0, 1, 2, 3, 4, 5],
            kafka_protocol::records::Compression::None,
        );
        assert_poll_returns_batches(records, 3, &[3, 4, 5], 6).await;
    }

    #[tokio::test]
    async fn maximum_legal_record_offset_commits_maximum_next_offset() {
        let records =
            encoded_record_batch(i64::MAX - 1, kafka_protocol::records::Compression::None);
        assert_poll_returns_batches(records, i64::MAX - 1, &[i64::MAX - 1], i64::MAX).await;
    }

    #[tokio::test]
    async fn a_whole_batch_before_the_requested_offset_does_not_create_dirty_progress() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            serve_initialization(&mut socket, &[addr], &[6], 0).await;
            let (header, request) =
                read_request::<FetchRequest>(&mut socket, ApiKey::Fetch, API_VERSION_FETCH).await;
            assert_eq!(request.topics[0].partitions[0].fetch_offset, 6);
            let mut response = fetch_response(0, 0, 0);
            response.responses[0].partitions[0].records = Some(encoded_record_batch_offsets(
                &[0, 1, 2, 3, 4, 5],
                kafka_protocol::records::Compression::None,
            ));
            reply(&mut socket, &header, API_VERSION_FETCH, response).await;
            assert_eq!(
                checked(socket.read_u8()).await.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof
            );
        });
        let mut consumer = consumer_at(addr, FetchOffset::Latest).await;
        assert!(checked(consumer.poll()).await.unwrap().is_empty());
        let native = native_consumer(&mut consumer);
        assert_eq!(native.offsets.get(TEST_TOPIC).unwrap().get(&0), Some(&6));
        assert!(native.dirty_offsets.is_empty());
        checked(consumer.commit()).await.unwrap();
        checked(consumer.close()).await.unwrap();
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn maximum_committed_next_offset_is_preserved_with_an_empty_fetch() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            serve_initialization(&mut socket, &[addr], &[i64::MAX], 0).await;
            let (header, request) =
                read_request::<FetchRequest>(&mut socket, ApiKey::Fetch, API_VERSION_FETCH).await;
            assert_eq!(request.topics[0].partitions[0].fetch_offset, i64::MAX);
            let mut response = fetch_response(0, 0, 0);
            response.responses[0].partitions[0].records = None;
            response.responses[0].partitions[0].high_watermark = i64::MAX;
            reply(&mut socket, &header, API_VERSION_FETCH, response).await;
            assert_eq!(
                checked(socket.read_u8()).await.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof
            );
        });
        let mut consumer = consumer_at(addr, FetchOffset::Latest).await;
        assert!(checked(consumer.poll()).await.unwrap().is_empty());
        let native = native_consumer(&mut consumer);
        assert_eq!(
            native.offsets.get(TEST_TOPIC).unwrap().get(&0),
            Some(&i64::MAX)
        );
        assert!(native.dirty_offsets.is_empty());
        checked(consumer.close()).await.unwrap();
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn poll_continues_past_an_empty_compacted_batch() {
        // A complete magic=2 batch with a valid CRC32C and recordsCount=0.
        let empty_batch: [u8; 61] = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 49, 255, 255, 255, 255, 2, 235, 224, 2, 3, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0,
        ];
        let mut records = BytesMut::from(&empty_batch[..]);
        records.extend_from_slice(&encoded_record_batch(
            5,
            kafka_protocol::records::Compression::None,
        ));
        assert_poll_returns_batches(records.freeze(), 0, &[5], 6).await;
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn poll_delivers_batches_with_mixed_compression() {
        use kafka_protocol::records::Compression;

        let mut records = BytesMut::new();
        for (offset, compression) in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ]
        .into_iter()
        .enumerate()
        {
            records.extend_from_slice(&encoded_record_batch(
                i64::try_from(offset).unwrap(),
                compression,
            ));
        }
        assert_poll_returns_batches(records.freeze(), 0, &[0, 1, 2, 3, 4], 5).await;
    }

    async fn serve_fetch(socket: &mut TcpStream, partition: i32, offset: i64) {
        let (header, request) =
            read_request::<FetchRequest>(socket, ApiKey::Fetch, API_VERSION_FETCH).await;
        assert_eq!(request.topics[0].topic, test_topic());
        assert_eq!(request.topics[0].partitions[0].partition, partition);
        assert_eq!(request.topics[0].partitions[0].fetch_offset, offset);
        reply(
            socket,
            &header,
            API_VERSION_FETCH,
            fetch_response(partition, offset, 0),
        )
        .await;
    }

    async fn serve_commit(socket: &mut TcpStream, expected: &[(i32, i64)]) {
        let (header, request) = read_request::<OffsetCommitRequest>(
            socket,
            ApiKey::OffsetCommit,
            API_VERSION_OFFSET_COMMIT,
        )
        .await;
        assert_eq!(request.group_id.as_str(), TEST_GROUP);
        assert_eq!(request.topics[0].name, test_topic());
        let mut committed: Vec<_> = request.topics[0]
            .partitions
            .iter()
            .map(|partition| (partition.partition_index, partition.committed_offset))
            .collect();
        committed.sort_unstable();
        assert_eq!(committed, expected);
        reply(
            socket,
            &header,
            API_VERSION_OFFSET_COMMIT,
            OffsetCommitResponse::default().with_topics(vec![
                OffsetCommitResponseTopic::default()
                    .with_name(test_topic())
                    .with_partitions(
                        expected
                            .iter()
                            .map(|(partition, _)| {
                                OffsetCommitResponsePartition::default()
                                    .with_partition_index(*partition)
                            })
                            .collect(),
                    ),
            ]),
        )
        .await;
    }

    async fn consumer_at(addr: SocketAddr, fallback: FetchOffset) -> AsyncConsumer {
        checked(
            AsyncConsumer::builder(vec![addr.to_string()])
                .with_group(TEST_GROUP.to_owned())
                .with_topic(TEST_TOPIC.to_owned())
                .with_fallback_offset(fallback)
                .with_native_retry_attempts(1)
                .build(),
        )
        .await
        .unwrap()
    }

    fn native_consumer(consumer: &mut AsyncConsumer) -> &mut NativeConsumer {
        match &mut consumer.mode {
            AsyncConsumerMode::Native(native) => native,
        }
    }

    async fn assert_start_offset(
        committed: i64,
        fallback: FetchOffset,
        expected_timestamp: Option<i64>,
        start_offset: i64,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            serve_initialization(&mut socket, &[addr], &[committed], 0).await;
            if let Some(timestamp) = expected_timestamp {
                let (header, request) = read_request::<ListOffsetsRequest>(
                    &mut socket,
                    ApiKey::ListOffsets,
                    API_VERSION_LIST_OFFSETS,
                )
                .await;
                assert_eq!(request.topics[0].partitions[0].timestamp, timestamp);
                reply(
                    &mut socket,
                    &header,
                    API_VERSION_LIST_OFFSETS,
                    ListOffsetsResponse::default().with_topics(vec![
                        ListOffsetsTopicResponse::default()
                            .with_name(test_topic())
                            .with_partitions(vec![
                                ListOffsetsPartitionResponse::default()
                                    .with_partition_index(0)
                                    .with_offset(start_offset),
                            ]),
                    ]),
                )
                .await;
            }
            serve_fetch(&mut socket, 0, start_offset).await;
            serve_commit(&mut socket, &[(0, start_offset + 1)]).await;
        });
        let mut consumer = consumer_at(addr, fallback).await;
        let messages = checked(consumer.poll()).await.unwrap();
        assert_eq!(
            messages.iter().next().unwrap().messages()[0].offset,
            start_offset
        );
        checked(consumer.commit()).await.unwrap();
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn poll_resumes_committed_offset_and_commits_next_delivered_offset() {
        assert_start_offset(42, FetchOffset::Latest, None, 42).await;
    }

    #[tokio::test]
    async fn poll_resolves_earliest_when_no_offset_is_committed() {
        assert_start_offset(-1, FetchOffset::Earliest, Some(-2), 5).await;
    }

    #[tokio::test]
    async fn poll_resolves_latest_when_no_offset_is_committed() {
        assert_start_offset(-1, FetchOffset::Latest, Some(-1), 27).await;
    }

    #[tokio::test]
    async fn poll_resolves_timestamp_when_no_offset_is_committed() {
        assert_start_offset(-1, FetchOffset::ByTime(1_234), Some(1_234), 16).await;
    }

    #[tokio::test]
    async fn offset_fetch_coordinator_error_does_not_fall_back_or_initialize_offsets() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            serve_initialization(&mut socket, &[addr], &[], 16).await;
            // Dropping the consumer must close the stream without sending a
            // ListOffsets or Fetch request after the failed OffsetFetch.
            assert_eq!(
                checked(socket.read_u8()).await.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof
            );
        });
        let mut consumer = consumer_at(addr, FetchOffset::Latest).await;
        assert!(matches!(
            checked(consumer.poll()).await,
            Err(Error::Kafka(KafkaCode::NotCoordinatorForGroup))
        ));
        let native = native_consumer(&mut consumer);
        assert!(native.coordinator.is_none());
        assert!(native.offsets.is_empty());
        assert!(native.dirty_offsets.is_empty());
        drop(consumer);
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn invalid_committed_offset_rejects_the_entire_response_before_any_fallback() {
        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_addr = first.local_addr().unwrap();
        let second_addr = second.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(first.accept()).await.unwrap();
            // The earlier unset partition must not trigger ListOffsets while
            // a later partition contains an invalid committed position.
            serve_initialization(&mut socket, &[first_addr, second_addr], &[-1, -2], 0).await;
            assert_eq!(
                checked(socket.read_u8()).await.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof
            );
        });
        let mut consumer = consumer_at(first_addr, FetchOffset::Latest).await;
        native_consumer(&mut consumer).retry_attempts = 3;
        assert!(matches!(
            checked(consumer.poll()).await,
            Err(Error::Protocol(ProtocolError::Codec))
        ));
        let native = native_consumer(&mut consumer);
        assert!(native.offsets.is_empty());
        assert!(native.dirty_offsets.is_empty());
        assert_eq!(consumer.native_error_stats().unwrap().total_errors, 1);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(second.poll_accept(cx).is_pending()))
                .await
        );
        checked(consumer.close()).await.unwrap();
        checked(server).await.unwrap();
    }

    fn malformed_offset_fetch_ack(kind: MalformedCommitAck) -> OffsetFetchResponse {
        let topics = [(TEST_TOPIC, &[0, 1][..]), (OTHER_TOPIC, &[0][..])]
            .into_iter()
            .map(|(topic, partitions)| {
                OffsetFetchResponseTopic::default()
                    .with_name(TopicName::from(StrBytes::from_string(topic.to_owned())))
                    .with_partitions(
                        partitions
                            .iter()
                            .map(|&partition| {
                                OffsetFetchResponsePartition::default()
                                    .with_partition_index(partition)
                                    .with_committed_offset(-1)
                            })
                            .collect(),
                    )
            })
            .collect();
        let mut response = OffsetFetchResponse::default().with_topics(topics);
        match kind {
            MalformedCommitAck::Empty => response.topics.clear(),
            MalformedCommitAck::MissingTopic => {
                response.topics.pop();
            }
            MalformedCommitAck::MissingPartition => {
                response.topics[0].partitions.pop();
            }
            MalformedCommitAck::ExtraTopic => {
                let mut topic = response.topics[0].clone();
                topic.name = TopicName::from(StrBytes::from_static_str("unrequested-offset-topic"));
                response.topics.push(topic);
            }
            MalformedCommitAck::ExtraPartition => {
                response.topics[0].partitions.push(
                    OffsetFetchResponsePartition::default()
                        .with_partition_index(99)
                        .with_committed_offset(-1),
                );
            }
            MalformedCommitAck::DuplicateTopic => response.topics.push(response.topics[0].clone()),
            MalformedCommitAck::DuplicatePartition => {
                let partition = response.topics[0].partitions[0].clone();
                response.topics[0].partitions.push(partition);
            }
            MalformedCommitAck::SparseWithRetriableError => {
                response.topics.pop();
                for partition in &mut response.topics[0].partitions {
                    partition.error_code = 16;
                }
            }
        }
        response
    }

    async fn assert_malformed_offset_fetch_cannot_initialize_or_fall_back(
        kind: MalformedCommitAck,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let (header, request) = read_request::<OffsetFetchRequest>(
                &mut socket,
                ApiKey::OffsetFetch,
                API_VERSION_OFFSET_FETCH,
            )
            .await;
            let mut requested = Vec::new();
            for topic in request.topics.unwrap() {
                requested.extend(
                    topic
                        .partition_indexes
                        .into_iter()
                        .map(|partition| (topic.name.to_string(), partition)),
                );
            }
            requested.sort_unstable();
            assert_eq!(requested, expected_topic_partitions());
            reply(
                &mut socket,
                &header,
                API_VERSION_OFFSET_FETCH,
                malformed_offset_fetch_ack(kind),
            )
            .await;
            // A malformed successful-RPC response must never send ListOffsets
            // for its apparent unset positions, nor automatically repeat RPCs.
            assert_eq!(
                checked(socket.read_u8()).await.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof
            );
        });
        let mut consumer = checked(
            AsyncConsumer::builder(vec![addr.to_string()])
                .with_group(TEST_GROUP.to_owned())
                .with_topics(vec![TEST_TOPIC.to_owned(), OTHER_TOPIC.to_owned()])
                .with_native_retry_attempts(3)
                .build(),
        )
        .await
        .unwrap();
        let native = native_consumer(&mut consumer);
        native.coordinator = Some(addr.to_string());
        native.leaders = expected_topic_partitions()
            .into_iter()
            .map(|tp| (tp, addr.to_string()))
            .collect();
        assert!(matches!(
            checked(consumer.poll()).await,
            Err(Error::Protocol(ProtocolError::Codec))
        ));
        let native = native_consumer(&mut consumer);
        assert!(native.offsets.is_empty());
        assert!(native.dirty_offsets.is_empty());
        assert!(native.client.connected_hosts().is_empty());
        assert_eq!(consumer.native_error_stats().unwrap().total_errors, 1);
        checked(consumer.close()).await.unwrap();
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn empty_successful_offset_fetch_cannot_be_interpreted_as_unset_positions() {
        assert_malformed_offset_fetch_cannot_initialize_or_fall_back(MalformedCommitAck::Empty)
            .await;
    }

    #[tokio::test]
    async fn missing_offset_fetch_topic_cannot_fall_back_or_publish_positions() {
        assert_malformed_offset_fetch_cannot_initialize_or_fall_back(
            MalformedCommitAck::MissingTopic,
        )
        .await;
    }

    #[tokio::test]
    async fn missing_offset_fetch_partition_cannot_fall_back_or_publish_positions() {
        assert_malformed_offset_fetch_cannot_initialize_or_fall_back(
            MalformedCommitAck::MissingPartition,
        )
        .await;
    }

    #[tokio::test]
    async fn extra_offset_fetch_topic_cannot_fall_back_or_publish_positions() {
        assert_malformed_offset_fetch_cannot_initialize_or_fall_back(
            MalformedCommitAck::ExtraTopic,
        )
        .await;
    }

    #[tokio::test]
    async fn extra_offset_fetch_partition_cannot_fall_back_or_publish_positions() {
        assert_malformed_offset_fetch_cannot_initialize_or_fall_back(
            MalformedCommitAck::ExtraPartition,
        )
        .await;
    }

    #[tokio::test]
    async fn duplicate_offset_fetch_topic_cannot_fall_back_or_publish_positions() {
        assert_malformed_offset_fetch_cannot_initialize_or_fall_back(
            MalformedCommitAck::DuplicateTopic,
        )
        .await;
    }

    #[tokio::test]
    async fn duplicate_offset_fetch_partition_cannot_fall_back_or_publish_positions() {
        assert_malformed_offset_fetch_cannot_initialize_or_fall_back(
            MalformedCommitAck::DuplicatePartition,
        )
        .await;
    }

    #[tokio::test]
    async fn sparse_offset_fetch_cannot_be_hidden_by_a_retriable_partition_error() {
        assert_malformed_offset_fetch_cannot_initialize_or_fall_back(
            MalformedCommitAck::SparseWithRetriableError,
        )
        .await;
    }

    async fn assert_coordinator_migration(error_code: i16) {
        let leader = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let leader_addr = leader.local_addr().unwrap();
        let coordinator_addr = coordinator.local_addr().unwrap();
        let leader_server = tokio::spawn(async move {
            let (mut socket, _) = checked(leader.accept()).await.unwrap();
            serve_initialization(&mut socket, &[leader_addr], &[], error_code).await;
            // A coordinator-only failure must rediscover it without another
            // Metadata request, and must not resolve the Latest fallback.
            serve_coordinator(&mut socket, coordinator_addr).await;
            serve_fetch(&mut socket, 0, 42).await;
        });
        let coordinator_server = tokio::spawn(async move {
            let (mut socket, _) = checked(coordinator.accept()).await.unwrap();
            serve_committed_offsets(&mut socket, 1, &[42], 0).await;
            serve_commit(&mut socket, &[(0, 43)]).await;
        });
        let mut consumer = checked(
            AsyncConsumer::builder(vec![leader_addr.to_string()])
                .with_group(TEST_GROUP.to_owned())
                .with_topic(TEST_TOPIC.to_owned())
                .with_fallback_offset(FetchOffset::Latest)
                .with_native_retry_attempts(2)
                .with_native_retry_backoff(Duration::ZERO)
                .build(),
        )
        .await
        .unwrap();
        let messages = checked(consumer.poll()).await.unwrap();
        assert_eq!(messages.iter().next().unwrap().messages()[0].offset, 42);
        assert_eq!(consumer.native_error_stats().unwrap().total_errors, 1);
        checked(consumer.commit()).await.unwrap();
        checked(leader_server).await.unwrap();
        checked(coordinator_server).await.unwrap();
    }

    #[tokio::test]
    async fn offset_initialization_rediscovers_a_migrated_coordinator() {
        assert_coordinator_migration(16).await;
    }

    #[tokio::test]
    async fn offset_initialization_retries_an_unavailable_coordinator() {
        assert_coordinator_migration(15).await;
    }

    #[tokio::test]
    async fn offset_initialization_retries_coordinator_load_in_progress() {
        assert_coordinator_migration(14).await;
    }

    #[tokio::test]
    async fn coordinator_retry_exhaustion_invalidates_cache_for_the_next_poll() {
        let leader = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let leader_addr = leader.local_addr().unwrap();
        let coordinator_addr = coordinator.local_addr().unwrap();
        let leader_server = tokio::spawn(async move {
            let (mut socket, _) = checked(leader.accept()).await.unwrap();
            serve_metadata(&mut socket, &[leader_addr]).await;
            for _ in 0..2 {
                serve_coordinator(&mut socket, leader_addr).await;
                serve_committed_offsets(&mut socket, 1, &[], 16).await;
            }
            // Exhaustion must return to the caller before a third attempt;
            // the caller's next poll starts with coordinator discovery.
            serve_coordinator(&mut socket, coordinator_addr).await;
            serve_fetch(&mut socket, 0, 42).await;
        });
        let coordinator_server = tokio::spawn(async move {
            let (mut socket, _) = checked(coordinator.accept()).await.unwrap();
            serve_committed_offsets(&mut socket, 1, &[42], 0).await;
            serve_commit(&mut socket, &[(0, 43)]).await;
        });
        let mut consumer = checked(
            AsyncConsumer::builder(vec![leader_addr.to_string()])
                .with_group(TEST_GROUP.to_owned())
                .with_topic(TEST_TOPIC.to_owned())
                .with_native_retry_attempts(2)
                .with_native_retry_backoff(Duration::ZERO)
                .build(),
        )
        .await
        .unwrap();
        assert!(matches!(
            checked(consumer.poll()).await,
            Err(Error::Kafka(KafkaCode::NotCoordinatorForGroup))
        ));
        let native = native_consumer(&mut consumer);
        assert!(native.coordinator.is_none());
        assert!(native.offsets.is_empty());
        assert!(native.dirty_offsets.is_empty());
        assert_eq!(
            native.leaders.get(&(TEST_TOPIC.to_owned(), 0)),
            Some(&leader_addr.to_string())
        );
        assert_eq!(consumer.native_error_stats().unwrap().total_errors, 2);
        let messages = checked(consumer.poll()).await.unwrap();
        assert_eq!(messages.iter().next().unwrap().messages()[0].offset, 42);
        checked(consumer.commit()).await.unwrap();
        checked(leader_server).await.unwrap();
        checked(coordinator_server).await.unwrap();
    }

    #[tokio::test]
    async fn metadata_refresh_preserves_progress_from_delivered_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            serve_initialization(&mut socket, &[addr], &[42], 0).await;
            serve_fetch(&mut socket, 0, 42).await;
            serve_metadata(&mut socket, &[addr]).await;
            // Existing progress must bypass OffsetFetch and ListOffsets.
            serve_fetch(&mut socket, 0, 43).await;
            serve_commit(&mut socket, &[(0, 44)]).await;
        });
        let mut consumer = consumer_at(addr, FetchOffset::Latest).await;
        checked(consumer.poll()).await.unwrap();
        checked(native_consumer(&mut consumer).refresh_metadata())
            .await
            .unwrap();
        let messages = checked(consumer.poll()).await.unwrap();
        assert_eq!(messages.iter().next().unwrap().messages()[0].offset, 43);
        checked(consumer.commit()).await.unwrap();
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn successful_multi_broker_poll_commits_next_offsets_for_all_delivered_records() {
        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_addr = first_listener.local_addr().unwrap();
        let second_addr = second_listener.local_addr().unwrap();
        let first_server = tokio::spawn(async move {
            let (mut socket, _) = checked(first_listener.accept()).await.unwrap();
            serve_initialization(&mut socket, &[first_addr, second_addr], &[10, 20], 0).await;
            serve_fetch(&mut socket, 0, 10).await;
            serve_commit(&mut socket, &[(0, 11), (1, 21)]).await;
        });
        let second_server = tokio::spawn(async move {
            let (mut socket, _) = checked(second_listener.accept()).await.unwrap();
            serve_fetch(&mut socket, 1, 20).await;
        });
        let mut consumer = consumer_at(first_addr, FetchOffset::Latest).await;
        let messages = checked(consumer.poll()).await.unwrap();
        let mut delivered: Vec<_> = messages
            .iter()
            .map(|set| (set.partition(), set.messages()[0].offset))
            .collect();
        delivered.sort_unstable();
        assert_eq!(delivered, [(0, 10), (1, 20)]);
        checked(consumer.commit()).await.unwrap();
        checked(first_server).await.unwrap();
        checked(second_server).await.unwrap();
    }

    const OTHER_TOPIC: &str = "another-offset-test";

    fn expected_topic_offsets(offset: i64) -> TopicOffsets {
        HashMap::from([
            (
                TEST_TOPIC.to_owned(),
                HashMap::from([(0, offset), (1, offset)]),
            ),
            (OTHER_TOPIC.to_owned(), HashMap::from([(0, offset)])),
        ])
    }

    fn expected_topic_partitions() -> Vec<(String, i32)> {
        let mut partitions = vec![
            (TEST_TOPIC.to_owned(), 0),
            (TEST_TOPIC.to_owned(), 1),
            (OTHER_TOPIC.to_owned(), 0),
        ];
        partitions.sort_unstable();
        partitions
    }

    async fn serve_multi_topic_commit(socket: &mut TcpStream, offset: i64, fail_one: bool) {
        let (header, request) = read_request::<OffsetCommitRequest>(
            socket,
            ApiKey::OffsetCommit,
            API_VERSION_OFFSET_COMMIT,
        )
        .await;
        let mut committed = Vec::new();
        for topic in &request.topics {
            for partition in &topic.partitions {
                assert_eq!(partition.committed_offset, offset);
                committed.push((topic.name.to_string(), partition.partition_index));
            }
        }
        committed.sort_unstable();
        assert_eq!(committed, expected_topic_partitions());
        let topics = request
            .topics
            .into_iter()
            .map(|topic| {
                let fail = fail_one && topic.name.as_str() == OTHER_TOPIC;
                OffsetCommitResponseTopic::default()
                    .with_name(topic.name)
                    .with_partitions(
                        topic
                            .partitions
                            .into_iter()
                            .map(|partition| {
                                OffsetCommitResponsePartition::default()
                                    .with_partition_index(partition.partition_index)
                                    .with_error_code(if fail { 30 } else { 0 })
                            })
                            .collect(),
                    )
            })
            .collect();
        reply(
            socket,
            &header,
            API_VERSION_OFFSET_COMMIT,
            OffsetCommitResponse::default().with_topics(topics),
        )
        .await;
    }

    #[derive(Clone, Copy)]
    enum MalformedCommitAck {
        Empty,
        MissingTopic,
        MissingPartition,
        ExtraTopic,
        ExtraPartition,
        DuplicateTopic,
        DuplicatePartition,
        SparseWithRetriableError,
    }

    fn malformed_commit_ack(
        request: &OffsetCommitRequest,
        kind: MalformedCommitAck,
    ) -> OffsetCommitResponse {
        let topics = request
            .topics
            .iter()
            .map(|topic| {
                OffsetCommitResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_partitions(
                        topic
                            .partitions
                            .iter()
                            .map(|partition| {
                                OffsetCommitResponsePartition::default()
                                    .with_partition_index(partition.partition_index)
                            })
                            .collect(),
                    )
            })
            .collect();
        let mut response = OffsetCommitResponse::default().with_topics(topics);
        match kind {
            MalformedCommitAck::Empty => response.topics.clear(),
            MalformedCommitAck::MissingTopic => {
                response.topics.pop();
            }
            MalformedCommitAck::MissingPartition => {
                response.topics[0].partitions.pop();
            }
            MalformedCommitAck::ExtraTopic => {
                let mut topic = response.topics[0].clone();
                topic.name = TopicName::from(StrBytes::from_static_str("unrequested-commit-topic"));
                response.topics.push(topic);
            }
            MalformedCommitAck::ExtraPartition => {
                response.topics[0]
                    .partitions
                    .push(OffsetCommitResponsePartition::default().with_partition_index(99));
            }
            MalformedCommitAck::DuplicateTopic => response.topics.push(response.topics[0].clone()),
            MalformedCommitAck::DuplicatePartition => {
                let partition = response.topics[0].partitions[0].clone();
                response.topics[0].partitions.push(partition);
            }
            MalformedCommitAck::SparseWithRetriableError => {
                response.topics.pop();
                for partition in &mut response.topics[0].partitions {
                    partition.error_code = 16;
                }
            }
        }
        response
    }

    async fn assert_malformed_commit_ack_preserves_dirty_and_reconnects(kind: MalformedCommitAck) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let (header, request) = read_request::<OffsetCommitRequest>(
                &mut socket,
                ApiKey::OffsetCommit,
                API_VERSION_OFFSET_COMMIT,
            )
            .await;
            let mut requested = Vec::new();
            for topic in &request.topics {
                for partition in &topic.partitions {
                    assert_eq!(partition.committed_offset, 2);
                    requested.push((topic.name.to_string(), partition.partition_index));
                }
            }
            requested.sort_unstable();
            assert_eq!(requested, expected_topic_partitions());
            reply(
                &mut socket,
                &header,
                API_VERSION_OFFSET_COMMIT,
                malformed_commit_ack(&request, kind),
            )
            .await;
            // No automatic replay is permitted after an ambiguous ACK. A
            // later explicit commit must retire this socket and resend all dirty.
            assert_eq!(
                checked(socket.read_u8()).await.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof
            );
            let (mut fresh, _) = checked(listener.accept()).await.unwrap();
            serve_multi_topic_commit(&mut fresh, 2, false).await;
        });
        let mut consumer = consumer_at(addr, FetchOffset::Latest).await;
        let native = native_consumer(&mut consumer);
        native.retry_attempts = 3;
        native.offsets = expected_topic_offsets(2);
        native.dirty_offsets = expected_topic_offsets(2);
        native.coordinator = Some(addr.to_string());
        assert!(matches!(
            checked(consumer.commit()).await,
            Err(Error::Protocol(ProtocolError::Codec))
        ));
        let native = native_consumer(&mut consumer);
        assert_eq!(native.offsets, expected_topic_offsets(2));
        assert_eq!(native.dirty_offsets, expected_topic_offsets(2));
        assert_eq!(consumer.native_error_stats().unwrap().total_errors, 1);
        checked(consumer.commit()).await.unwrap();
        assert!(native_consumer(&mut consumer).dirty_offsets.is_empty());
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn empty_commit_ack_keeps_all_dirty_offsets_and_poisons_connection() {
        assert_malformed_commit_ack_preserves_dirty_and_reconnects(MalformedCommitAck::Empty).await;
    }

    #[tokio::test]
    async fn missing_commit_topic_keeps_all_dirty_offsets_and_poisons_connection() {
        assert_malformed_commit_ack_preserves_dirty_and_reconnects(
            MalformedCommitAck::MissingTopic,
        )
        .await;
    }

    #[tokio::test]
    async fn missing_commit_partition_keeps_all_dirty_offsets_and_poisons_connection() {
        assert_malformed_commit_ack_preserves_dirty_and_reconnects(
            MalformedCommitAck::MissingPartition,
        )
        .await;
    }

    #[tokio::test]
    async fn extra_commit_topic_keeps_all_dirty_offsets_and_poisons_connection() {
        assert_malformed_commit_ack_preserves_dirty_and_reconnects(MalformedCommitAck::ExtraTopic)
            .await;
    }

    #[tokio::test]
    async fn extra_commit_partition_keeps_all_dirty_offsets_and_poisons_connection() {
        assert_malformed_commit_ack_preserves_dirty_and_reconnects(
            MalformedCommitAck::ExtraPartition,
        )
        .await;
    }

    #[tokio::test]
    async fn duplicate_commit_topic_keeps_all_dirty_offsets_and_poisons_connection() {
        assert_malformed_commit_ack_preserves_dirty_and_reconnects(
            MalformedCommitAck::DuplicateTopic,
        )
        .await;
    }

    #[tokio::test]
    async fn duplicate_commit_partition_keeps_all_dirty_offsets_and_poisons_connection() {
        assert_malformed_commit_ack_preserves_dirty_and_reconnects(
            MalformedCommitAck::DuplicatePartition,
        )
        .await;
    }

    #[tokio::test]
    async fn sparse_commit_ack_cannot_be_hidden_by_a_retriable_error() {
        assert_malformed_commit_ack_preserves_dirty_and_reconnects(
            MalformedCommitAck::SparseWithRetriableError,
        )
        .await;
    }

    #[tokio::test]
    async fn complete_commit_ack_with_coordinator_error_retains_bounded_recovery() {
        let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let new = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let old_addr = old.local_addr().unwrap();
        let new_addr = new.local_addr().unwrap();
        let old_server = tokio::spawn(async move {
            let (mut socket, _) = checked(old.accept()).await.unwrap();
            let (header, request) = read_request::<OffsetCommitRequest>(
                &mut socket,
                ApiKey::OffsetCommit,
                API_VERSION_OFFSET_COMMIT,
            )
            .await;
            let topics = request
                .topics
                .into_iter()
                .map(|topic| {
                    OffsetCommitResponseTopic::default()
                        .with_name(topic.name)
                        .with_partitions(
                            topic
                                .partitions
                                .into_iter()
                                .map(|partition| {
                                    OffsetCommitResponsePartition::default()
                                        .with_partition_index(partition.partition_index)
                                        .with_error_code(16)
                                })
                                .collect(),
                        )
                })
                .collect();
            reply(
                &mut socket,
                &header,
                API_VERSION_OFFSET_COMMIT,
                OffsetCommitResponse::default().with_topics(topics),
            )
            .await;
            serve_coordinator(&mut socket, new_addr).await;
        });
        let new_server = tokio::spawn(async move {
            let (mut socket, _) = checked(new.accept()).await.unwrap();
            serve_multi_topic_commit(&mut socket, 2, false).await;
        });
        let mut consumer = consumer_at(old_addr, FetchOffset::Latest).await;
        let native = native_consumer(&mut consumer);
        native.retry_attempts = 2;
        native.retry_backoff = Duration::ZERO;
        native.offsets = expected_topic_offsets(2);
        native.dirty_offsets = expected_topic_offsets(2);
        native.coordinator = Some(old_addr.to_string());
        checked(consumer.commit()).await.unwrap();
        assert_eq!(consumer.native_error_stats().unwrap().total_errors, 1);
        assert_eq!(
            native_consumer(&mut consumer).offsets,
            expected_topic_offsets(2)
        );
        assert!(native_consumer(&mut consumer).dirty_offsets.is_empty());
        checked(old_server).await.unwrap();
        checked(new_server).await.unwrap();
    }

    #[tokio::test]
    async fn repeated_multi_topic_poll_preserves_all_dirty_offsets_after_partial_commit_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let (header, request) = read_request::<MetadataRequest>(
                &mut socket,
                ApiKey::Metadata,
                API_VERSION_METADATA,
            )
            .await;
            let mut requested: Vec<_> = request
                .topics
                .unwrap()
                .into_iter()
                .map(|topic| topic.name.unwrap().to_string())
                .collect();
            requested.sort_unstable();
            let mut expected = vec![TEST_TOPIC.to_owned(), OTHER_TOPIC.to_owned()];
            expected.sort_unstable();
            assert_eq!(requested, expected);
            let routes: [(&str, &[i32]); 2] = [(TEST_TOPIC, &[0, 1]), (OTHER_TOPIC, &[0])];
            let topics = routes
                .iter()
                .map(|(topic, partitions)| {
                    MetadataResponseTopic::default()
                        .with_name(Some(TopicName::from(StrBytes::from_string(
                            (*topic).to_owned(),
                        ))))
                        .with_partitions(
                            partitions
                                .iter()
                                .map(|&partition| {
                                    MetadataResponsePartition::default()
                                        .with_partition_index(partition)
                                        .with_leader_id(BrokerId::from(0))
                                })
                                .collect(),
                        )
                })
                .collect();
            reply(
                &mut socket,
                &header,
                API_VERSION_METADATA,
                MetadataResponse::default()
                    .with_brokers(vec![
                        MetadataResponseBroker::default()
                            .with_node_id(BrokerId::from(0))
                            .with_host(StrBytes::from_string(addr.ip().to_string()))
                            .with_port(i32::from(addr.port())),
                    ])
                    .with_topics(topics),
            )
            .await;
            serve_coordinator(&mut socket, addr).await;
            let (header, request) = read_request::<OffsetFetchRequest>(
                &mut socket,
                ApiKey::OffsetFetch,
                API_VERSION_OFFSET_FETCH,
            )
            .await;
            let mut requested = Vec::new();
            for topic in request.topics.unwrap() {
                requested.extend(
                    topic
                        .partition_indexes
                        .into_iter()
                        .map(|partition| (topic.name.to_string(), partition)),
                );
            }
            requested.sort_unstable();
            assert_eq!(requested, expected_topic_partitions());
            let topics = routes
                .iter()
                .map(|(topic, partitions)| {
                    OffsetFetchResponseTopic::default()
                        .with_name(TopicName::from(StrBytes::from_string((*topic).to_owned())))
                        .with_partitions(
                            partitions
                                .iter()
                                .map(|&partition| {
                                    OffsetFetchResponsePartition::default()
                                        .with_partition_index(partition)
                                        .with_committed_offset(0)
                                })
                                .collect(),
                        )
                })
                .collect();
            reply(
                &mut socket,
                &header,
                API_VERSION_OFFSET_FETCH,
                OffsetFetchResponse::default().with_topics(topics),
            )
            .await;
            for offset in 0..3 {
                let (header, request) =
                    read_request::<FetchRequest>(&mut socket, ApiKey::Fetch, API_VERSION_FETCH)
                        .await;
                let mut requested = Vec::new();
                for topic in request.topics {
                    for partition in topic.partitions {
                        assert_eq!(partition.fetch_offset, offset);
                        requested.push((topic.topic.to_string(), partition.partition));
                    }
                }
                requested.sort_unstable();
                assert_eq!(requested, expected_topic_partitions());
                let responses = routes
                    .iter()
                    .map(|(topic, partitions)| {
                        FetchableTopicResponse::default()
                            .with_topic(TopicName::from(StrBytes::from_string((*topic).to_owned())))
                            .with_partitions(
                                partitions
                                    .iter()
                                    .map(|&partition| {
                                        PartitionData::default()
                                            .with_partition_index(partition)
                                            .with_high_watermark(offset + 1)
                                            .with_records(Some(encoded_record_batch(
                                                offset,
                                                kafka_protocol::records::Compression::None,
                                            )))
                                    })
                                    .collect(),
                            )
                    })
                    .collect();
                reply(
                    &mut socket,
                    &header,
                    API_VERSION_FETCH,
                    FetchResponse::default().with_responses(responses),
                )
                .await;
                if offset == 1 {
                    serve_multi_topic_commit(&mut socket, 2, true).await;
                    serve_multi_topic_commit(&mut socket, 2, false).await;
                } else if offset == 2 {
                    serve_multi_topic_commit(&mut socket, 3, false).await;
                }
            }
        });
        let mut consumer = checked(
            AsyncConsumer::builder(vec![addr.to_string()])
                .with_group(TEST_GROUP.to_owned())
                .with_topics(vec![TEST_TOPIC.to_owned(), OTHER_TOPIC.to_owned()])
                .with_native_retry_attempts(1)
                .build(),
        )
        .await
        .unwrap();
        for next in 1..=2 {
            assert_eq!(checked(consumer.poll()).await.unwrap().iter().count(), 3);
            let native = native_consumer(&mut consumer);
            assert_eq!(native.offsets, expected_topic_offsets(next));
            assert_eq!(native.dirty_offsets, expected_topic_offsets(next));
        }
        assert!(matches!(
            checked(consumer.commit()).await,
            Err(Error::Kafka(KafkaCode::GroupAuthorizationFailed))
        ));
        assert_eq!(
            native_consumer(&mut consumer).dirty_offsets,
            expected_topic_offsets(2)
        );
        checked(consumer.commit()).await.unwrap();
        assert!(native_consumer(&mut consumer).dirty_offsets.is_empty());
        checked(consumer.poll()).await.unwrap();
        assert_eq!(
            native_consumer(&mut consumer).offsets,
            expected_topic_offsets(3)
        );
        assert_eq!(
            native_consumer(&mut consumer).dirty_offsets,
            expected_topic_offsets(3)
        );
        checked(consumer.commit()).await.unwrap();
        checked(server).await.unwrap();
    }

    #[derive(Clone, Copy)]
    enum MalformedFetch {
        MissingTopic,
        MissingPartition,
        ExtraTopic,
        ExtraTopicWithRetriableError,
        ExtraPartition,
        DuplicateTopic,
        DuplicatePartition,
        NegativeOffset,
        MaximumOffset,
        MaximumPrefix,
        InvalidPrefix,
    }

    fn malformed_fetch_response(kind: MalformedFetch, partition: i32) -> FetchResponse {
        let mut response = fetch_response(partition, 1, 0);
        match kind {
            MalformedFetch::MissingTopic => response.responses.clear(),
            MalformedFetch::MissingPartition => response.responses[0].partitions.clear(),
            MalformedFetch::ExtraTopic | MalformedFetch::ExtraTopicWithRetriableError => {
                if matches!(kind, MalformedFetch::ExtraTopicWithRetriableError) {
                    response.responses[0].partitions[0].error_code = 6;
                }
                let mut extra = response.responses[0].clone();
                extra.topic = TopicName::from(StrBytes::from_static_str("unrequested-topic"));
                response.responses.push(extra);
            }
            MalformedFetch::ExtraPartition => {
                let mut extra = response.responses[0].partitions[0].clone();
                extra.partition_index = 99;
                response.responses[0].partitions.push(extra);
            }
            MalformedFetch::DuplicateTopic => {
                response.responses.push(response.responses[0].clone())
            }
            MalformedFetch::DuplicatePartition => {
                let extra = response.responses[0].partitions[0].clone();
                response.responses[0].partitions.push(extra);
            }
            MalformedFetch::NegativeOffset
            | MalformedFetch::MaximumOffset
            | MalformedFetch::InvalidPrefix => {
                let offsets = match kind {
                    MalformedFetch::NegativeOffset => vec![-1],
                    MalformedFetch::MaximumOffset => vec![i64::MAX],
                    _ => vec![-1, 1],
                };
                response.responses[0].partitions[0].records = Some(encoded_record_batch_offsets(
                    &offsets,
                    kafka_protocol::records::Compression::None,
                ));
            }
            MalformedFetch::MaximumPrefix => {
                let mut records = BytesMut::from(
                    &encoded_record_batch(i64::MAX, kafka_protocol::records::Compression::None)[..],
                );
                records.extend_from_slice(&encoded_record_batch(
                    1,
                    kafka_protocol::records::Compression::None,
                ));
                response.responses[0].partitions[0].records = Some(records.freeze());
            }
        }
        response
    }

    async fn assert_malformed_later_response_preserves_delivered_progress(kind: MalformedFetch) {
        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let brokers = Arc::new(vec![
            first.local_addr().unwrap(),
            second.local_addr().unwrap(),
        ]);
        let second_round = Arc::new(AtomicUsize::new(0));
        let mut servers = Vec::new();
        for (index, listener) in [first, second].into_iter().enumerate() {
            let brokers = Arc::clone(&brokers);
            let second_round = Arc::clone(&second_round);
            servers.push(tokio::spawn(async move {
                let (mut socket, _) = checked(listener.accept()).await.unwrap();
                if index == 0 {
                    serve_initialization(&mut socket, &brokers, &[0, 0], 0).await;
                }
                let partition = i32::try_from(index).unwrap();
                serve_fetch(&mut socket, partition, 0).await;
                let (header, request) =
                    read_request::<FetchRequest>(&mut socket, ApiKey::Fetch, API_VERSION_FETCH)
                        .await;
                assert_eq!(request.topics[0].partitions[0].fetch_offset, 1);
                let response = if second_round.fetch_add(1, Ordering::Relaxed) == 0 {
                    fetch_response(partition, 1, 0)
                } else {
                    malformed_fetch_response(kind, partition)
                };
                reply(&mut socket, &header, API_VERSION_FETCH, response).await;
                if index == 0 {
                    // The failed poll must leave both earlier delivered offsets
                    // dirty, rather than committing any of the failed poll.
                    serve_commit(&mut socket, &[(0, 1), (1, 1)]).await;
                }
            }));
        }
        let mut consumer = consumer_at(brokers[0], FetchOffset::Latest).await;
        native_consumer(&mut consumer).retry_attempts = 3;
        checked(consumer.poll()).await.unwrap();
        let native = native_consumer(&mut consumer);
        let offsets = native.offsets.clone();
        let dirty = native.dirty_offsets.clone();
        assert!(matches!(
            checked(consumer.poll()).await,
            Err(Error::Protocol(ProtocolError::Codec))
        ));
        let native = native_consumer(&mut consumer);
        assert_eq!(native.offsets, offsets);
        assert_eq!(native.dirty_offsets, dirty);
        assert_eq!(consumer.native_error_stats().unwrap().total_errors, 1);
        checked(consumer.commit()).await.unwrap();
        for server in servers {
            checked(server).await.unwrap();
        }
    }

    #[tokio::test]
    async fn missing_response_topic_does_not_publish_or_clear_delivered_progress() {
        assert_malformed_later_response_preserves_delivered_progress(MalformedFetch::MissingTopic)
            .await;
    }

    #[tokio::test]
    async fn missing_response_partition_does_not_publish_or_clear_delivered_progress() {
        assert_malformed_later_response_preserves_delivered_progress(
            MalformedFetch::MissingPartition,
        )
        .await;
    }

    #[tokio::test]
    async fn extra_response_topic_does_not_publish_or_clear_delivered_progress() {
        assert_malformed_later_response_preserves_delivered_progress(MalformedFetch::ExtraTopic)
            .await;
    }

    #[tokio::test]
    async fn extra_response_partition_does_not_publish_or_clear_delivered_progress() {
        assert_malformed_later_response_preserves_delivered_progress(
            MalformedFetch::ExtraPartition,
        )
        .await;
    }

    #[tokio::test]
    async fn malformed_response_is_not_hidden_by_a_retriable_partition_error() {
        assert_malformed_later_response_preserves_delivered_progress(
            MalformedFetch::ExtraTopicWithRetriableError,
        )
        .await;
    }

    #[tokio::test]
    async fn duplicate_response_topic_does_not_publish_or_clear_delivered_progress() {
        assert_malformed_later_response_preserves_delivered_progress(
            MalformedFetch::DuplicateTopic,
        )
        .await;
    }

    #[tokio::test]
    async fn duplicate_response_partition_does_not_publish_or_clear_delivered_progress() {
        assert_malformed_later_response_preserves_delivered_progress(
            MalformedFetch::DuplicatePartition,
        )
        .await;
    }

    #[tokio::test]
    async fn negative_message_offset_does_not_publish_or_clear_delivered_progress() {
        assert_malformed_later_response_preserves_delivered_progress(
            MalformedFetch::NegativeOffset,
        )
        .await;
    }

    #[tokio::test]
    async fn overflowing_message_offset_does_not_publish_or_clear_delivered_progress() {
        assert_malformed_later_response_preserves_delivered_progress(MalformedFetch::MaximumOffset)
            .await;
    }

    #[tokio::test]
    async fn malformed_old_prefix_cannot_be_hidden_by_trimming() {
        assert_malformed_later_response_preserves_delivered_progress(MalformedFetch::InvalidPrefix)
            .await;
    }

    #[tokio::test]
    async fn invalid_offset_before_the_last_record_cannot_escape_validation() {
        assert_malformed_later_response_preserves_delivered_progress(MalformedFetch::MaximumPrefix)
            .await;
    }

    #[derive(Clone, Copy)]
    enum LaterFetch {
        PartitionError,
        CorruptBatch,
        CorruptTail,
        TruncatedTail,
        #[cfg(not(feature = "gzip"))]
        UnsupportedTail,
        Disconnect,
        Block,
    }

    async fn assert_incomplete_poll_preserves_progress(later_fetch: LaterFetch) {
        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let brokers = Arc::new(vec![
            first_listener.local_addr().unwrap(),
            second_listener.local_addr().unwrap(),
        ]);
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let blocked = Arc::new(Notify::new());
        let mut servers = Vec::new();
        for (index, listener) in [first_listener, second_listener].into_iter().enumerate() {
            let brokers = Arc::clone(&brokers);
            let fetch_count = Arc::clone(&fetch_count);
            let blocked = Arc::clone(&blocked);
            servers.push(tokio::spawn(async move {
                let (mut socket, _) = checked(listener.accept()).await.unwrap();
                if index == 0 {
                    serve_initialization(&mut socket, &brokers, &[0, 0], 0).await;
                }
                let (header, request) =
                    read_request::<FetchRequest>(&mut socket, ApiKey::Fetch, API_VERSION_FETCH)
                        .await;
                assert_eq!(request.topics[0].partitions[0].fetch_offset, 0);
                let partition = i32::try_from(index).unwrap();
                assert_eq!(request.topics[0].partitions[0].partition, partition);
                // HashMap iteration can choose either broker first. Fail only
                // after the other broker has successfully returned its records.
                if fetch_count.fetch_add(1, Ordering::Relaxed) == 0 {
                    reply(
                        &mut socket,
                        &header,
                        API_VERSION_FETCH,
                        fetch_response(partition, 0, 0),
                    )
                    .await;
                } else {
                    match later_fetch {
                        LaterFetch::PartitionError => {
                            reply(
                                &mut socket,
                                &header,
                                API_VERSION_FETCH,
                                fetch_response(partition, 0, 29),
                            )
                            .await;
                        }
                        LaterFetch::CorruptBatch => {
                            let mut response = fetch_response(partition, 0, 0);
                            let records = &mut response.responses[0].partitions[0].records;
                            let mut corrupt_records = records.take().unwrap().to_vec();
                            // Preserve the valid Fetch frame and batch length,
                            // but invalidate the record batch's CRC.
                            *corrupt_records.last_mut().unwrap() ^= 1;
                            *records = Some(Bytes::from(corrupt_records));
                            reply(&mut socket, &header, API_VERSION_FETCH, response).await;
                        }
                        LaterFetch::CorruptTail | LaterFetch::TruncatedTail => {
                            let mut response = fetch_response(partition, 0, 0);
                            let records = &mut response.responses[0].partitions[0].records;
                            let mut combined = BytesMut::from(&records.take().unwrap()[..]);
                            if matches!(later_fetch, LaterFetch::CorruptTail) {
                                let mut tail = encoded_record_batch(
                                    1,
                                    kafka_protocol::records::Compression::None,
                                )
                                .to_vec();
                                *tail.last_mut().unwrap() ^= 1;
                                combined.extend_from_slice(&tail);
                            } else {
                                combined.extend_from_slice(&[0; 7]);
                            }
                            *records = Some(combined.freeze());
                            reply(&mut socket, &header, API_VERSION_FETCH, response).await;
                        }
                        #[cfg(not(feature = "gzip"))]
                        LaterFetch::UnsupportedTail => {
                            // Complete magic=2 batch with valid CRC32C and a
                            // GZIP-compressed record whose value is "delivered".
                            let mut gzip_batch = [
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 255, 255, 255, 255, 2, 239,
                                28, 19, 68, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                                255, 255, 255, 0, 0, 0, 1, 31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 147,
                                99, 96, 96, 96, 20, 74, 73, 205, 201, 44, 75, 45, 74, 77, 97, 0, 0,
                                184, 183, 161, 18, 16, 0, 0, 0,
                            ];
                            gzip_batch[..8].copy_from_slice(&1i64.to_be_bytes());
                            let mut response = fetch_response(partition, 0, 0);
                            let records = &mut response.responses[0].partitions[0].records;
                            let mut combined = BytesMut::from(&records.take().unwrap()[..]);
                            combined.extend_from_slice(&gzip_batch);
                            *records = Some(combined.freeze());
                            reply(&mut socket, &header, API_VERSION_FETCH, response).await;
                        }
                        LaterFetch::Disconnect => {}
                        LaterFetch::Block => {
                            blocked.notify_one();
                            std::future::pending::<()>().await;
                        }
                    }
                }
            }));
        }
        let mut consumer = consumer_at(brokers[0], FetchOffset::Latest).await;
        if matches!(
            later_fetch,
            LaterFetch::CorruptBatch | LaterFetch::CorruptTail | LaterFetch::TruncatedTail
        ) {
            // Codec errors must propagate even when recoverable errors would
            // receive multiple attempts.
            native_consumer(&mut consumer).retry_attempts = 3;
        }
        #[cfg(not(feature = "gzip"))]
        if matches!(later_fetch, LaterFetch::UnsupportedTail) {
            native_consumer(&mut consumer).retry_attempts = 3;
        }
        match later_fetch {
            LaterFetch::Block => {
                checked(async {
                    tokio::select! {
                        result = consumer.poll() => panic!("poll completed before cancellation: {result:?}"),
                        () = blocked.notified() => {}
                    }
                }).await;
            }
            LaterFetch::PartitionError => {
                assert!(matches!(
                    checked(consumer.poll()).await,
                    Err(Error::Kafka(KafkaCode::TopicAuthorizationFailed))
                ));
            }
            LaterFetch::CorruptBatch | LaterFetch::CorruptTail | LaterFetch::TruncatedTail => {
                assert!(matches!(
                    checked(consumer.poll()).await,
                    Err(Error::Protocol(ProtocolError::Codec))
                ));
                assert_eq!(consumer.native_error_stats().unwrap().total_errors, 1);
            }
            #[cfg(not(feature = "gzip"))]
            LaterFetch::UnsupportedTail => {
                assert!(matches!(
                    checked(consumer.poll()).await,
                    Err(Error::Protocol(ProtocolError::UnsupportedCompression))
                ));
                assert_eq!(consumer.native_error_stats().unwrap().total_errors, 1);
            }
            LaterFetch::Disconnect => {
                assert!(matches!(
                    checked(consumer.poll()).await,
                    Err(Error::Connection(_))
                ));
            }
        }
        let native = native_consumer(&mut consumer);
        let expected = HashMap::from([(TEST_TOPIC.to_owned(), HashMap::from([(0, 0), (1, 0)]))]);
        assert_eq!(native.offsets, expected);
        assert!(native.dirty_offsets.is_empty());
        checked(consumer.commit()).await.unwrap();
        for server in servers {
            if matches!(later_fetch, LaterFetch::Block) && !server.is_finished() {
                server.abort();
                assert!(checked(server).await.unwrap_err().is_cancelled());
            } else {
                checked(server).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn later_broker_partition_error_does_not_advance_undelivered_offsets() {
        assert_incomplete_poll_preserves_progress(LaterFetch::PartitionError).await;
    }

    #[tokio::test]
    async fn later_broker_corrupt_batch_does_not_advance_undelivered_offsets_or_retry() {
        assert_incomplete_poll_preserves_progress(LaterFetch::CorruptBatch).await;
    }

    #[tokio::test]
    async fn later_broker_corrupt_tail_does_not_advance_undelivered_offsets_or_retry() {
        assert_incomplete_poll_preserves_progress(LaterFetch::CorruptTail).await;
    }

    #[tokio::test]
    async fn later_broker_truncated_tail_does_not_advance_undelivered_offsets_or_retry() {
        assert_incomplete_poll_preserves_progress(LaterFetch::TruncatedTail).await;
    }

    #[cfg(not(feature = "gzip"))]
    #[tokio::test]
    async fn later_broker_unsupported_tail_does_not_advance_undelivered_offsets_or_retry() {
        assert_incomplete_poll_preserves_progress(LaterFetch::UnsupportedTail).await;
    }

    #[tokio::test]
    async fn later_broker_disconnect_does_not_advance_undelivered_offsets() {
        assert_incomplete_poll_preserves_progress(LaterFetch::Disconnect).await;
    }

    #[tokio::test]
    async fn cancelling_poll_at_later_broker_does_not_advance_undelivered_offsets() {
        assert_incomplete_poll_preserves_progress(LaterFetch::Block).await;
    }

    #[tokio::test]
    async fn from_hosts_fails_with_unreachable_hosts() {
        let result = AsyncConsumer::from_hosts(
            vec!["127.0.0.1:1".to_owned()],
            "test-group".to_owned(),
            vec!["test-topic".to_owned()],
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::NoHostReachable))
        ));
    }

    #[tokio::test]
    async fn from_client_fails_with_unreachable_hosts() {
        let client = AsyncKafkaClient::new(vec![]).await.unwrap();
        let result = AsyncConsumer::from_client(
            client,
            "test-group".to_owned(),
            vec!["test-topic".to_owned()],
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn drop_consumer_without_close_does_not_panic() {
        let result = AsyncConsumer::from_hosts(
            vec!["127.0.0.1:1".to_owned()],
            "test-drop-group".to_owned(),
            vec!["test-drop-topic".to_owned()],
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn builder_without_group_returns_error() {
        let result = AsyncConsumer::builder(vec![])
            .with_topic("t".to_owned())
            .build()
            .await;
        assert!(matches!(
            result,
            Err(Error::Consumer(ConsumerError::UnsetGroupId))
        ));
    }

    #[tokio::test]
    async fn builder_without_topics_returns_error() {
        let result = AsyncConsumer::builder(vec![])
            .with_group("g".to_owned())
            .build()
            .await;
        assert!(matches!(
            result,
            Err(Error::Consumer(ConsumerError::NoTopicsAssigned))
        ));
    }
}

#[cfg(test)]
#[path = "consumer_progress_bench.rs"]
mod consumer_progress_bench;
