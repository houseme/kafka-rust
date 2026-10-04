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
use rustfs_kafka::client::fetch_kp::convert_fetch_response;
use rustfs_kafka::consumer::{FetchOffset, MessageSets};
use rustfs_kafka::error::{ConsumerError, Error, KafkaCode, Result};
use std::collections::HashMap;
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

struct NativeConsumer {
    client: AsyncKafkaClient,
    group: String,
    topics: Vec<String>,
    fallback_offset: FetchOffset,
    offsets: HashMap<(String, i32), i64>,
    dirty_offsets: HashMap<(String, i32), i64>,
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
            let offset = *self.offsets.get(tp).unwrap_or(&0);
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
            let owned = convert_fetch_response(response, correlation);
            if let Some(err) = first_fetch_error(&owned) {
                return Err(err);
            }

            owned_responses.push(owned);
        }

        // A failed or cancelled poll must not consume messages it never returns.
        // Publish progress only once every broker response has been collected.
        for response in &owned_responses {
            self.advance_offsets(response);
        }
        Ok(MessageSets::from_fetch_responses(owned_responses))
    }

    fn next_correlation(&mut self) -> i32 {
        let cid = self.correlation;
        self.correlation = self.correlation.wrapping_add(1);
        cid
    }

    fn advance_offsets(&mut self, resp: &rustfs_kafka::client::fetch_kp::OwnedFetchResponse) {
        for topic in &resp.topics {
            for partition in &topic.partitions {
                if let Ok(data) = partition.data()
                    && let Some(last) = data.messages.last()
                {
                    let next_offset = last.offset + 1;
                    let tp = (topic.topic.clone(), partition.partition);
                    self.offsets.insert(tp.clone(), next_offset);
                    self.dirty_offsets.insert(tp, next_offset);
                }
            }
        }
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
            .map(|((topic, partition), offset)| (topic.as_str(), *partition, *offset))
            .collect();

        let conn = self.client.get_connection(&coordinator).await?;
        let (header, request) =
            build_offset_commit_request(correlation, &client_id, &self.group, &payload);
        send_kp_request(conn, &header, &request, API_VERSION_OFFSET_COMMIT).await?;
        let response =
            get_kp_response::<OffsetCommitResponse>(conn, API_VERSION_OFFSET_COMMIT).await?;

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
            .filter(|tp| !self.offsets.contains_key(*tp))
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
            if let Some(offset) = committed.get(&tp)
                && *offset >= 0
            {
                self.offsets.insert(tp.clone(), *offset);
                continue;
            }

            let fallback = self.resolve_fallback_offset(&tp).await?;
            self.offsets.insert(tp, fallback);
        }

        Ok(())
    }

    async fn fetch_committed_offsets(
        &mut self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<(String, i32), i64>> {
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

        let mut committed = HashMap::new();
        for topic in response.topics {
            for partition in topic.partitions {
                if partition.error_code != 0 {
                    if let Some(code) = map_kafka_code(partition.error_code) {
                        return Err(Error::Kafka(code));
                    }
                    return Err(Error::Kafka(KafkaCode::Unknown));
                }
                committed.insert(
                    (topic.name.to_string(), partition.partition_index),
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

fn first_fetch_error(resp: &rustfs_kafka::client::fetch_kp::OwnedFetchResponse) -> Option<Error> {
    for topic in &resp.topics {
        for partition in &topic.partitions {
            if let Err(err) = partition.data() {
                return Some(match &**err {
                    Error::TopicPartitionError { error_code, .. } => Error::Kafka(*error_code),
                    _ => Error::from(Arc::clone(err)),
                });
            }
        }
    }
    None
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
        let record = Record {
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
        };
        let mut records = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut records,
            &[record],
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .unwrap();
        records.freeze()
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
        let expected = HashMap::from([
            ((TEST_TOPIC.to_owned(), 0), 0),
            ((TEST_TOPIC.to_owned(), 1), 0),
        ]);
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
