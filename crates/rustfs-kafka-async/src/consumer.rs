//! Async consumer for fetching messages from Kafka.

use bytes::{Bytes, BytesMut};
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
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::RecordBatchDecoder;
use rustfs_kafka::client::SecurityConfig;
use rustfs_kafka::consumer::{FetchOffset, MessageSets};
use rustfs_kafka::error::{ConsumerError, Error, KafkaCode, ProtocolError, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

use crate::AsyncKafkaClient;
use crate::connection::AsyncConnection;

const API_VERSION_METADATA: i16 = 1;
const API_VERSION_FETCH: i16 = 12;
const API_VERSION_FIND_COORDINATOR: i16 = 3;
const API_VERSION_OFFSET_COMMIT: i16 = 2;
const API_VERSION_OFFSET_FETCH: i16 = 2;
const API_VERSION_LIST_OFFSETS: i16 = 1;
const DEFAULT_NATIVE_RETRY_ATTEMPTS: usize = 3;
const DEFAULT_NATIVE_RETRY_BACKOFF_MS: u64 = 100;
const DEFAULT_NATIVE_RECENT_ERROR_LIMIT: usize = 32;
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

/// Native consumer error snapshot for diagnostics.
#[derive(Debug, Clone)]
pub struct NativeConsumerErrorSnapshot {
    pub phase: String,
    pub class: String,
    pub kafka_code: Option<KafkaCode>,
    pub message: String,
    pub timestamp_unix_ms: u128,
}

/// Native consumer error statistics.
#[derive(Debug, Clone)]
pub struct NativeConsumerErrorStats {
    pub total_errors: u64,
    pub kafka_code_counts: HashMap<String, u64>,
    pub class_counts: HashMap<String, u64>,
    pub last_error: Option<NativeConsumerErrorSnapshot>,
    pub recent_errors: Vec<NativeConsumerErrorSnapshot>,
}

#[derive(Debug, Clone)]
struct NativeConsumerObservability {
    total_errors: u64,
    kafka_code_counts: HashMap<String, u64>,
    class_counts: HashMap<String, u64>,
    last_error: Option<NativeConsumerErrorSnapshot>,
    recent_errors: VecDeque<NativeConsumerErrorSnapshot>,
    recent_error_limit: usize,
}

impl Default for NativeConsumerObservability {
    fn default() -> Self {
        Self::new(DEFAULT_NATIVE_RECENT_ERROR_LIMIT)
    }
}

impl NativeConsumerObservability {
    fn new(recent_error_limit: usize) -> Self {
        Self {
            total_errors: 0,
            kafka_code_counts: HashMap::new(),
            class_counts: HashMap::new(),
            last_error: None,
            recent_errors: VecDeque::new(),
            recent_error_limit: recent_error_limit.max(1),
        }
    }

    fn clear(&mut self) {
        self.total_errors = 0;
        self.kafka_code_counts.clear();
        self.class_counts.clear();
        self.last_error = None;
        self.recent_errors.clear();
    }
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
    #[must_use]
    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity.max(1);
        self
    }

    /// Backward-compatible setting kept for API compatibility.
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
        NativeConsumerErrorStats {
            total_errors: self.observability.total_errors,
            kafka_code_counts: self.observability.kafka_code_counts.clone(),
            class_counts: self.observability.class_counts.clone(),
            last_error: self.observability.last_error.clone(),
            recent_errors: self.observability.recent_errors.iter().cloned().collect(),
        }
    }

    fn reset_error_stats(&mut self) {
        self.observability.clear();
    }

    fn record_error(&mut self, phase: &str, err: &Error) {
        self.observability.total_errors = self.observability.total_errors.saturating_add(1);
        let class = error_class(err);
        let kafka_code = kafka_code_from_error(err).map(kafka_code_to_i16);
        let kafka_code_label = kafka_code.map(|code| code.to_string());

        *self
            .observability
            .class_counts
            .entry(class.clone())
            .or_insert(0) += 1;
        if let Some(code) = &kafka_code_label {
            *self
                .observability
                .kafka_code_counts
                .entry(code.clone())
                .or_insert(0) += 1;
        }
        let snapshot = NativeConsumerErrorSnapshot {
            phase: phase.to_owned(),
            class,
            kafka_code: kafka_code.and_then(map_kafka_code),
            message: err.to_string(),
            timestamp_unix_ms: now_unix_ms(),
        };
        self.observability.last_error = Some(snapshot.clone());
        self.observability.recent_errors.push_back(snapshot.clone());
        while self.observability.recent_errors.len() > self.observability.recent_error_limit {
            let _ = self.observability.recent_errors.pop_front();
        }
        crate::metrics::record_native_consumer_error(
            phase,
            &snapshot.class,
            kafka_code_label.as_deref(),
            self.observability.recent_errors.len(),
            snapshot.timestamp_unix_ms,
        );
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

async fn send_kp_request<T>(
    conn: &mut AsyncConnection,
    header: &RequestHeader,
    body: &T,
    api_version: i16,
) -> Result<()>
where
    T: Encodable + kafka_protocol::protocol::HeaderVersion,
{
    let header_version = T::header_version(api_version);

    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, header_version)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;

    let mut body_buf = BytesMut::new();
    body.encode(&mut body_buf, api_version)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;

    let total_len = usize_to_i32(header_buf.len() + body_buf.len())?;
    let mut out = BytesMut::with_capacity(4 + non_negative_i32_to_usize(total_len)?);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body_buf);

    conn.send(&out).await
}

async fn get_kp_response<R>(conn: &mut AsyncConnection, api_version: i16) -> Result<R>
where
    R: Decodable + kafka_protocol::protocol::HeaderVersion,
{
    let size_bytes = conn.read_exact(4).await?;
    let size = i32::from_be_bytes(
        <[u8; 4]>::try_from(size_bytes.as_ref())
            .map_err(|_| Error::Protocol(ProtocolError::Codec))?,
    );
    let mut bytes = conn.read_exact(non_negative_i32_to_u64(size)?).await?;

    let response_header_version = R::header_version(api_version);
    let _resp_header = ResponseHeader::decode(&mut bytes, response_header_version)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;

    R::decode(&mut bytes, api_version).map_err(|_| Error::Protocol(ProtocolError::Codec))
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

fn kafka_code_from_error(err: &Error) -> Option<KafkaCode> {
    match err {
        Error::Kafka(code) => Some(*code),
        Error::TopicPartitionError { error_code, .. } => Some(*error_code),
        Error::BrokerRequestError { source, .. } => kafka_code_from_error(source),
        _ => None,
    }
}

fn error_class(err: &Error) -> String {
    match err {
        Error::Kafka(_) => "kafka".to_owned(),
        Error::Connection(_) => "connection".to_owned(),
        Error::Protocol(_) => "protocol".to_owned(),
        Error::Config(_) => "config".to_owned(),
        Error::Consumer(_) => "consumer".to_owned(),
        Error::TopicPartitionError { .. } => "topic_partition".to_owned(),
        Error::BrokerRequestError { .. } => "broker_request".to_owned(),
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
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

fn map_kafka_code(code: i16) -> Option<KafkaCode> {
    match code {
        0 => None,
        1 => Some(KafkaCode::OffsetOutOfRange),
        2 => Some(KafkaCode::CorruptMessage),
        3 => Some(KafkaCode::UnknownTopicOrPartition),
        4 => Some(KafkaCode::InvalidMessageSize),
        5 => Some(KafkaCode::LeaderNotAvailable),
        6 => Some(KafkaCode::NotLeaderForPartition),
        7 => Some(KafkaCode::RequestTimedOut),
        8 => Some(KafkaCode::BrokerNotAvailable),
        9 => Some(KafkaCode::ReplicaNotAvailable),
        10 => Some(KafkaCode::MessageSizeTooLarge),
        11 => Some(KafkaCode::StaleControllerEpoch),
        12 => Some(KafkaCode::OffsetMetadataTooLarge),
        13 => Some(KafkaCode::NetworkException),
        14 => Some(KafkaCode::GroupLoadInProgress),
        15 => Some(KafkaCode::GroupCoordinatorNotAvailable),
        16 => Some(KafkaCode::NotCoordinatorForGroup),
        17 => Some(KafkaCode::InvalidTopic),
        18 => Some(KafkaCode::RecordListTooLarge),
        19 => Some(KafkaCode::NotEnoughReplicas),
        20 => Some(KafkaCode::NotEnoughReplicasAfterAppend),
        21 => Some(KafkaCode::InvalidRequiredAcks),
        22 => Some(KafkaCode::IllegalGeneration),
        23 => Some(KafkaCode::InconsistentGroupProtocol),
        24 => Some(KafkaCode::InvalidGroupId),
        25 => Some(KafkaCode::UnknownMemberId),
        26 => Some(KafkaCode::InvalidSessionTimeout),
        27 => Some(KafkaCode::RebalanceInProgress),
        28 => Some(KafkaCode::InvalidCommitOffsetSize),
        29 => Some(KafkaCode::TopicAuthorizationFailed),
        30 => Some(KafkaCode::GroupAuthorizationFailed),
        31 => Some(KafkaCode::ClusterAuthorizationFailed),
        32 => Some(KafkaCode::InvalidTimestamp),
        33 => Some(KafkaCode::UnsupportedSaslMechanism),
        34 => Some(KafkaCode::IllegalSaslState),
        35 => Some(KafkaCode::UnsupportedVersion),
        _ => Some(KafkaCode::Unknown),
    }
}

fn kafka_code_to_i16(code: KafkaCode) -> i16 {
    match code {
        KafkaCode::OffsetOutOfRange => 1,
        KafkaCode::CorruptMessage => 2,
        KafkaCode::UnknownTopicOrPartition => 3,
        KafkaCode::InvalidMessageSize => 4,
        KafkaCode::LeaderNotAvailable => 5,
        KafkaCode::NotLeaderForPartition => 6,
        KafkaCode::RequestTimedOut => 7,
        KafkaCode::BrokerNotAvailable => 8,
        KafkaCode::ReplicaNotAvailable => 9,
        KafkaCode::MessageSizeTooLarge => 10,
        KafkaCode::StaleControllerEpoch => 11,
        KafkaCode::OffsetMetadataTooLarge => 12,
        KafkaCode::NetworkException => 13,
        KafkaCode::GroupLoadInProgress => 14,
        KafkaCode::GroupCoordinatorNotAvailable => 15,
        KafkaCode::NotCoordinatorForGroup => 16,
        KafkaCode::InvalidTopic => 17,
        KafkaCode::RecordListTooLarge => 18,
        KafkaCode::NotEnoughReplicas => 19,
        KafkaCode::NotEnoughReplicasAfterAppend => 20,
        KafkaCode::InvalidRequiredAcks => 21,
        KafkaCode::IllegalGeneration => 22,
        KafkaCode::InconsistentGroupProtocol => 23,
        KafkaCode::InvalidGroupId => 24,
        KafkaCode::UnknownMemberId => 25,
        KafkaCode::InvalidSessionTimeout => 26,
        KafkaCode::RebalanceInProgress => 27,
        KafkaCode::InvalidCommitOffsetSize => 28,
        KafkaCode::TopicAuthorizationFailed => 29,
        KafkaCode::GroupAuthorizationFailed => 30,
        KafkaCode::ClusterAuthorizationFailed => 31,
        KafkaCode::InvalidTimestamp => 32,
        KafkaCode::UnsupportedSaslMechanism => 33,
        KafkaCode::IllegalSaslState => 34,
        KafkaCode::UnsupportedVersion => 35,
        KafkaCode::Unknown => -1,
    }
}

fn usize_to_i32(value: usize) -> Result<i32> {
    i32::try_from(value).map_err(|_| Error::Protocol(ProtocolError::Codec))
}

fn non_negative_i32_to_usize(value: i32) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::Protocol(ProtocolError::Codec))
}

fn non_negative_i32_to_u64(value: i32) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::Protocol(ProtocolError::Codec))
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

    #[tokio::test]
    async fn native_observability_tracks_recent_snapshots_and_numeric_codes() {
        let client = AsyncKafkaClient::new(vec![]).await.unwrap();
        let mut native = NativeConsumer {
            client,
            group: "g".to_owned(),
            topics: vec!["t".to_owned()],
            fallback_offset: FetchOffset::Latest,
            offsets: HashMap::new(),
            dirty_offsets: HashMap::new(),
            leaders: HashMap::new(),
            coordinator: None,
            correlation: 1,
            retry_attempts: DEFAULT_NATIVE_RETRY_ATTEMPTS,
            retry_backoff: Duration::from_millis(DEFAULT_NATIVE_RETRY_BACKOFF_MS),
            observability: NativeConsumerObservability::new(2),
        };

        native.record_error("poll", &Error::Kafka(KafkaCode::LeaderNotAvailable));
        native.record_error("poll", &Error::Kafka(KafkaCode::NotLeaderForPartition));
        native.record_error(
            "commit",
            &Error::Kafka(KafkaCode::GroupCoordinatorNotAvailable),
        );

        let stats = native.error_stats();
        assert_eq!(stats.total_errors, 3);
        assert_eq!(stats.kafka_code_counts.get("5").copied(), Some(1));
        assert_eq!(stats.kafka_code_counts.get("6").copied(), Some(1));
        assert_eq!(stats.kafka_code_counts.get("15").copied(), Some(1));
        assert_eq!(stats.recent_errors.len(), 2);
        assert_eq!(stats.recent_errors[0].phase, "poll");
        assert_eq!(stats.recent_errors[1].phase, "commit");

        native.reset_error_stats();
        let reset = native.error_stats();
        assert_eq!(reset.total_errors, 0);
        assert!(reset.kafka_code_counts.is_empty());
        assert!(reset.recent_errors.is_empty());
    }
}
