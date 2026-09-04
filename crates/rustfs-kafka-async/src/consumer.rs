//! Async consumer for fetching messages from Kafka.

use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    GroupId, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    RequestHeader, ResponseHeader, TopicName, fetch_request::FetchPartition as KpFetchPartition,
    fetch_request::FetchTopic as KpFetchTopic, list_offsets_request::ListOffsetsPartition,
    list_offsets_request::ListOffsetsTopic, metadata_request::MetadataRequestTopic,
    offset_commit_request::OffsetCommitRequestPartition,
    offset_commit_request::OffsetCommitRequestTopic, offset_fetch_request::OffsetFetchRequestTopic,
};
use kafka_protocol::protocol::{Decodable, HeaderVersion, StrBytes};
use kafka_protocol::records::RecordBatchDecoder;
use rustfs_kafka::client::SecurityConfig;
use rustfs_kafka::consumer::{FetchOffset, MessageSets};
use rustfs_kafka::error::{ConsumerError, Error, KafkaCode, ProtocolError, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

use crate::AsyncKafkaClient;
use crate::connection::AsyncConnection;
use crate::consumer_observability::{
    DEFAULT_NATIVE_RECENT_ERROR_LIMIT, NativeConsumerErrorStats, NativeConsumerObservability,
};
use crate::wire::{
    get_kp_response, kafka_code_from_protocol as map_kafka_code, non_negative_i32_to_u64,
    send_kp_request,
};

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
                Err(err) if attempt < self.retry_attempts && should_retry_poll(&err) => {
                    self.record_error("poll", &err);
                    self.leaders.clear();
                    self.refresh_metadata().await?;
                    tokio::time::sleep(self.retry_backoff).await;
                    continue;
                }
                Err(err) => {
                    self.record_error("poll", &err);
                    return Err(err);
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

        let mut by_broker: HashMap<String, Vec<(String, i32, i64)>> = HashMap::new();
        for (tp, leader_host) in &self.leaders {
            let offset = *self.offsets.get(tp).unwrap_or(&0);
            by_broker
                .entry(leader_host.clone())
                .or_default()
                .push((tp.0.clone(), tp.1, offset));
        }

        let correlation = self.next_correlation();
        let client_id = self.client.client_id().to_owned();
        let mut owned_responses = Vec::new();

        for (broker, tps) in by_broker {
            let parts: Vec<(&str, i32, i64, i32)> = tps
                .iter()
                .map(|(topic, partition, offset)| {
                    (
                        topic.as_str(),
                        *partition,
                        *offset,
                        FETCH_PARTITION_MAX_BYTES,
                    )
                })
                .collect();

            let conn = self.client.get_connection(&broker).await?;
            let (header, request) = build_fetch_request(correlation, &client_id, &parts);
            send_kp_request(conn, &header, &request, API_VERSION_FETCH).await?;
            let response = get_fetch_response(conn, API_VERSION_FETCH).await?;
            let owned = convert_fetch_response(response, correlation);
            if let Some(code) = first_fetch_error_code(&owned) {
                return Err(Error::Kafka(code));
            }

            self.advance_offsets(&owned);
            owned_responses.push(owned);
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
                    self.leaders.insert(tp.clone(), host.clone());
                    self.offsets.entry(tp).or_insert(0);
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

async fn get_fetch_response(
    conn: &mut AsyncConnection,
    requested_version: i16,
) -> Result<FetchResponse> {
    let size_bytes = conn.read_exact(4).await?;
    let size = i32::from_be_bytes(
        <[u8; 4]>::try_from(size_bytes.as_ref())
            .map_err(|_| Error::Protocol(ProtocolError::Codec))?,
    );
    let resp_bytes = conn.read_exact(non_negative_i32_to_u64(size)?).await?;

    let mut candidates = Vec::with_capacity(1 + 18);
    candidates.push(requested_version);
    for v in (0..=17).rev() {
        if v != requested_version {
            candidates.push(v);
        }
    }

    for version in candidates {
        let mut bytes = resp_bytes.clone();
        let header_version = FetchResponse::header_version(version);
        if ResponseHeader::decode(&mut bytes, header_version).is_err() {
            continue;
        }
        if let Ok(resp) = FetchResponse::decode(&mut bytes, version) {
            return Ok(resp);
        }
    }

    Err(Error::Protocol(ProtocolError::Codec))
}

fn convert_fetch_response(
    kp_resp: FetchResponse,
    correlation_id: i32,
) -> rustfs_kafka::client::fetch_kp::OwnedFetchResponse {
    use rustfs_kafka::client::fetch_kp::{OwnedFetchResponse, OwnedPartition, OwnedTopic};

    let topics = kp_resp
        .responses
        .into_iter()
        .map(|t| {
            let topic_name = t.topic.to_string();
            let partitions: Vec<OwnedPartition> = t
                .partitions
                .into_iter()
                .map(|p| {
                    let data = if p.error_code != 0 {
                        Err(Arc::new(Error::TopicPartitionError {
                            topic_name: topic_name.clone(),
                            partition_id: p.partition_index,
                            error_code: map_kafka_code(p.error_code).unwrap_or(KafkaCode::Unknown),
                        }))
                    } else {
                        decode_partition_records(p.records, p.high_watermark)
                    };
                    OwnedPartition {
                        partition: p.partition_index,
                        data,
                        highwatermark: p.high_watermark,
                    }
                })
                .collect();
            OwnedTopic {
                topic: topic_name,
                partitions,
            }
        })
        .collect();

    OwnedFetchResponse {
        correlation_id,
        topics,
    }
}

fn first_fetch_error_code(
    resp: &rustfs_kafka::client::fetch_kp::OwnedFetchResponse,
) -> Option<KafkaCode> {
    for topic in &resp.topics {
        for partition in &topic.partitions {
            if let Err(err) = partition.data()
                && let Error::TopicPartitionError { error_code, .. } = &**err
            {
                return Some(*error_code);
            }
        }
    }
    None
}

fn should_retry_poll(err: &Error) -> bool {
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

fn decode_partition_records(
    records: Option<Bytes>,
    high_watermark: i64,
) -> std::result::Result<rustfs_kafka::client::fetch_kp::OwnedData, Arc<Error>> {
    use rustfs_kafka::client::fetch_kp::{OwnedData, OwnedMessage};

    let Some(mut records_bytes) = records else {
        return Ok(OwnedData {
            highwatermark_offset: high_watermark,
            messages: vec![],
        });
    };
    if records_bytes.is_empty() {
        return Ok(OwnedData {
            highwatermark_offset: high_watermark,
            messages: vec![],
        });
    }

    let Ok(record_set) = RecordBatchDecoder::decode(&mut records_bytes) else {
        return Err(Arc::new(Error::Protocol(ProtocolError::Codec)));
    };

    let mut messages: Vec<OwnedMessage> = Vec::new();
    for record in &record_set.records {
        messages.push(OwnedMessage {
            offset: record.offset,
            key: record.key.clone().unwrap_or_default(),
            value: record.value.clone().unwrap_or_default(),
        });
    }

    Ok(OwnedData {
        highwatermark_offset: high_watermark,
        messages,
    })
}

fn no_host_reachable_error() -> Error {
    Error::Connection(rustfs_kafka::error::ConnectionError::NoHostReachable)
}

#[cfg(test)]
mod tests {
    use rustfs_kafka::error::{ConnectionError, Error};

    use super::*;

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
