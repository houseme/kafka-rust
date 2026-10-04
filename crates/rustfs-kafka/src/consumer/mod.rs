//! Kafka Consumer - A higher-level API for consuming kafka topics.
//!
//! A consumer for Kafka topics on behalf of a specified group
//! providing help in offset management.  The consumer requires at
//! least one topic for consumption and allows consuming multiple
//! topics at the same time. Further, clients can restrict the
//! consumer to only specific topic partitions as demonstrated in the
//! following example.
//!
//! # Features
//!
//! - **Automatic offset management** with configurable storage (Kafka/Zookeeper)
//! - **Pause/Resume** support for individual partitions
//! - **Seek** to specific offsets within consumed partitions
//! - **Group-based consumption** with offset commit tracking
//!
//! # Examples
//!
//! ```no_run
//! use rustfs_kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
//!
//! let mut consumer =
//!    Consumer::from_hosts(vec!("localhost:9092".to_owned()))
//!       .with_topic_partitions("my-topic".to_owned(), &[0, 1])
//!       .with_fallback_offset(FetchOffset::Earliest)
//!       .with_group("my-group".to_owned())
//!       .with_offset_storage(Some(GroupOffsetStorage::Kafka))
//!       .create()
//!       .unwrap();
//! loop {
//!   for ms in consumer.poll().unwrap().iter() {
//!     for m in ms.messages() {
//!       println!("{:?}", m);
//!     }
//!     consumer.consume_messageset(&ms);
//!   }
//!   consumer.commit_consumed().unwrap();
//! }
//! ```
//!
//! Please refer to the documentation of the individual "with" methods
//! used to set up the consumer. These contain further information or
//! links to such.
//!
//! A call to `.poll()` on the consumer will ask for the next
//! available "chunk of data" for the client code to process.  The
//! returned data are `MessageSet`s. There is at most one for each partition
//! of the consumed topics. Individual messages are embedded in the
//! retrieved messagesets and can be processed using the `messages`
//! field.
//!
//! If the consumer is configured for a non-empty group, it helps in
//! keeping track of already consumed messages by maintaining a map of
//! the consumed offsets.  Messages can be told "consumed" either
//! through `consume_message` or `consume_messages` methods.  Once
//! these consumed messages are committed to Kafka using
//! `commit_consumed`, the consumer will start fetching messages from
//! here even after restart.  Since committing is a certain overhead,
//! it is up to the client to decide the frequency of the commits.
//! The consumer will *not* commit any messages to Kafka
//! automatically.
//!
//! The configuration of a group is optional.  If the consumer has no
//! group configured, it will behave as if it had one, only that
//! committing consumed message offsets resolves into a void operation.

use crate::client::fetch_kp;
use crate::client::{CommitOffset, FetchPartition, KafkaClient};
use crate::error::{Error, KafkaCode, Result};
use std::collections::hash_map::{Entry, HashMap};
use std::slice;
use std::sync::Arc;
use tracing::debug;

// public re-exports
pub use self::builder::Builder;
use self::state::TopicPartition;
pub use crate::client::FetchOffset;
pub use crate::client::GroupOffsetStorage;
pub use crate::protocol::fetch::OwnedMessage as Message;
pub use assignor::{PartitionAssignor, RangeAssignor, RoundRobinAssignor};
pub use group_coordinator::GroupCoordinator;
pub use rebalance::{RebalanceHandler, RebalanceListener};

mod assignment;
mod assignor;
mod builder;
mod config;
mod group_coordinator;
mod rebalance;
mod state;

/// The default value for `Builder::with_retry_max_bytes_limit`.
pub const DEFAULT_RETRY_MAX_BYTES_LIMIT: i32 = 0;

/// The default value for `Builder::with_fallback_offset`.
pub const DEFAULT_FALLBACK_OFFSET: FetchOffset = FetchOffset::Latest;

/// The Kafka Consumer
///
/// See module level documentation.
#[derive(Debug)]
pub struct Consumer {
    client: KafkaClient,
    state: state::State,
    config: config::Config,
}

// XXX 1) Allow returning to a previous offset (aka seeking)
// XXX 2) Issue IO in a separate (background) thread and pre-fetch message sets

impl Consumer {
    /// Starts building a consumer using the given kafka client.
    #[must_use]
    pub fn from_client(client: KafkaClient) -> Builder {
        builder::new(Some(client), Vec::new())
    }

    /// Starts building a consumer bootstrapping internally a new kafka
    /// client from the given kafka hosts.
    #[must_use]
    pub fn from_hosts(hosts: Vec<String>) -> Builder {
        builder::new(None, hosts)
    }

    /// Borrows the underlying kafka client.
    #[must_use]
    pub fn client(&self) -> &KafkaClient {
        &self.client
    }

    /// Borrows the underlying kafka client as mut.
    #[must_use]
    pub fn client_mut(&mut self) -> &mut KafkaClient {
        &mut self.client
    }

    /// Destroys this consumer returning back the underlying kafka client.
    #[must_use]
    pub fn into_client(self) -> KafkaClient {
        self.client
    }

    /// Pauses message fetching for the specified partitions of a topic.
    ///
    /// Paused partitions will not be included in future `poll()` results
    /// until they are resumed with `resume()`.
    pub fn pause(&mut self, topic: &str, partitions: &[i32]) {
        let topic_ref = self.state.topic_ref(topic);
        for &p in partitions {
            self.state.paused.insert((topic.to_owned(), p));
            if let Some(topic_ref) = topic_ref {
                self.state.paused_assignments.insert(TopicPartition {
                    topic_ref,
                    partition: p,
                });
            }
        }
        debug!("Paused partitions for topic '{}': {:?}", topic, partitions);
    }

    /// Resumes message fetching for the specified partitions of a topic.
    pub fn resume(&mut self, topic: &str, partitions: &[i32]) {
        let topic_ref = self.state.topic_ref(topic);
        for &p in partitions {
            self.state.paused.remove(&(topic.to_owned(), p));
            if let Some(topic_ref) = topic_ref {
                self.state.paused_assignments.remove(&TopicPartition {
                    topic_ref,
                    partition: p,
                });
            }
        }
        debug!("Resumed partitions for topic '{}': {:?}", topic, partitions);
    }

    /// Returns `true` if the specified partition is currently paused.
    #[must_use]
    pub fn is_paused(&self, topic: &str, partition: i32) -> bool {
        self.state.paused.contains(&(topic.to_owned(), partition))
    }

    /// Returns all currently paused partitions.
    pub fn paused_partitions(&self) -> impl Iterator<Item = &(String, i32)> {
        self.state.paused.iter()
    }

    /// Retrieves the topic partitions being currently consumed by
    /// this consumer.
    #[must_use]
    pub fn subscriptions(&self) -> HashMap<String, Vec<i32>> {
        let mut h: HashMap<String, Vec<i32>> =
            HashMap::with_capacity(self.state.assignments.as_slice().len());
        let tps = self
            .state
            .fetch_offsets
            .keys()
            .map(|tp| (self.state.topic_name(tp.topic_ref), tp.partition));
        for tp in tps {
            if let Some(ps) = h.get_mut(tp.0) {
                ps.push(tp.1);
                continue;
            }
            h.insert(tp.0.to_owned(), vec![tp.1]);
        }
        h
    }

    /// Polls for the next available message data.
    ///
    /// # Errors
    ///
    /// Returns an error if fetching messages from Kafka fails or if
    /// the response cannot be decoded.
    #[tracing::instrument(skip(self))]
    pub fn poll(&mut self) -> Result<MessageSets> {
        let (n, retry_partition, resps) = self.fetch_messages();
        let resps = resps?;
        self.process_fetch_responses_with_progress(n, retry_partition, resps)
    }

    #[must_use]
    fn single_partition_consumer(&self) -> bool {
        self.state.fetch_offsets.len() == 1
    }

    /// Retrieves the group on which behalf this consumer is acting.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.config.group
    }

    /// Convenient method to allow consumer to manually reposition to a set of
    /// topic, partition and offset.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustfs_kafka::client::{KafkaClient, FetchOffset};
    /// use rustfs_kafka::consumer::{Consumer, FetchOffset as ConsumerFetchOffset, GroupOffsetStorage};
    /// use rustfs_kafka::error::Result;
    ///
    /// let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_string()])
    ///     .with_topic("test-topic".to_string())
    ///     .with_fallback_offset(ConsumerFetchOffset::Latest)
    ///     .with_offset_storage(Some(GroupOffsetStorage::Kafka))
    ///     .create()
    ///     .unwrap();
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// let topics = vec!["test-topic".to_string()];
    /// let topic_offsets = client.list_offsets(&topics, FetchOffset::ByTime(1698425676797)).unwrap();
    ///
    /// // Seek to the offsets
    /// for (topic, partition_offsets) in topic_offsets {
    ///     for po in partition_offsets {
    ///         consumer.seek(&topic, po.partition, po.offset).unwrap();
    ///     }
    /// }
    /// ```
    /// # Errors
    ///
    /// Returns an error if the topic or partition is not assigned, or if the
    /// requested offset is negative.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        if offset < 0 {
            return Err(Error::Config("seek offset must be non-negative".into()));
        }
        let topic_ref = self.state.topic_ref(topic);
        match topic_ref {
            Some(topic_ref) => {
                let tp = TopicPartition {
                    topic_ref,
                    partition,
                };
                if !self.state.fetch_offsets.contains_key(&tp) {
                    return Err(Error::TopicPartitionError {
                        topic_name: topic.to_owned(),
                        partition_id: partition,
                        error_code: KafkaCode::UnknownTopicOrPartition,
                    });
                }
                let maybe_entry = self.state.fetch_offsets.entry(tp);
                match maybe_entry {
                    Entry::Occupied(mut e) => {
                        e.get_mut().offset = offset;
                        Ok(())
                    }
                    Entry::Vacant(_) => Err(Error::TopicPartitionError {
                        topic_name: topic.to_string(),
                        partition_id: partition,
                        error_code: KafkaCode::UnknownTopicOrPartition,
                    }),
                }
            }
            None => Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)),
        }
    }

    fn fetch_messages(
        &mut self,
    ) -> (
        u32,
        Option<TopicPartition>,
        Result<Vec<fetch_kp::FetchResponseWithProgress>>,
    ) {
        let retry_partition = self.state.next_retry_partition().map(|tp| TopicPartition {
            topic_ref: tp.topic_ref,
            partition: tp.partition,
        });
        if let Some(tp) = retry_partition {
            let Some(s) = self.state.fetch_offsets.get(&tp) else {
                return (
                    1,
                    Some(tp),
                    Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)),
                );
            };

            let topic = self.state.topic_name(tp.topic_ref);
            debug!(
                "fetching retry messages: (fetch-offset: {{\"{}:{}\": {:?}}})",
                topic, tp.partition, s
            );
            let result = self.client.fetch_messages_with_progress(std::iter::once(
                &FetchPartition::new(topic, tp.partition, s.offset).with_max_bytes(s.max_bytes),
            ));
            (1, Some(tp), result)
        } else {
            let client = &mut self.client;
            let state = &self.state;
            debug!(
                "fetching messages: (fetch-offsets: {:?})",
                state.fetch_offsets_debug()
            );
            let mut num_partitions = 0usize;
            let result =
                client.fetch_messages_with_progress(state.fetch_requests().inspect(|_| {
                    num_partitions += 1;
                }));
            #[allow(clippy::cast_possible_truncation)] // partition count won't exceed u32
            let num_partitions = num_partitions as u32;
            (num_partitions, None, result)
        }
    }

    fn process_fetch_responses_with_progress(
        &mut self,
        num_partitions_queried: u32,
        retry_partition: Option<TopicPartition>,
        responses: Vec<fetch_kp::FetchResponseWithProgress>,
    ) -> Result<MessageSets> {
        self.process_fetch_response_parts(
            num_partitions_queried,
            retry_partition,
            responses
                .into_iter()
                .map(|response| {
                    let (owned, progress) = response.into_parts();
                    (owned, Some(progress))
                })
                .collect(),
        )
    }

    #[cfg(test)]
    fn process_fetch_responses(
        &mut self,
        num_partitions_queried: u32,
        retry_partition: Option<TopicPartition>,
        responses: Vec<fetch_kp::OwnedFetchResponse>,
    ) -> Result<MessageSets> {
        self.process_fetch_response_parts(
            num_partitions_queried,
            retry_partition,
            responses
                .into_iter()
                .map(|response| (response, None))
                .collect(),
        )
    }

    fn process_fetch_response_parts(
        &mut self,
        num_partitions_queried: u32,
        retry_partition: Option<TopicPartition>,
        mut responses: Vec<(
            fetch_kp::OwnedFetchResponse,
            Option<fetch_kp::FetchProgress>,
        )>,
    ) -> Result<MessageSets> {
        self.validate_and_trim_fetch_responses(&mut responses)?;
        let mut empty = true;
        let mut fetch_updates: HashMap<TopicPartition, state::FetchState, state::PartitionHasher> =
            HashMap::default();
        let mut retry_updates = Vec::new();
        for (response, progress) in &mut responses {
            for (topic_index, topic) in response.topics.iter_mut().enumerate() {
                let topic_ref = self
                    .state
                    .assignments
                    .topic_ref(&topic.topic)
                    .ok_or_else(Error::codec)?;
                for (partition_index, partition) in topic.partitions.iter_mut().enumerate() {
                    let tp = TopicPartition {
                        topic_ref,
                        partition: partition.partition,
                    };
                    let current = self.state.fetch_offsets.get(&tp).ok_or_else(Error::codec)?;
                    let partition_id = partition.partition;
                    let Some(data) = Self::stage_partition_data(
                        &topic.topic,
                        &tp,
                        partition,
                        current,
                        &mut fetch_updates,
                    )?
                    else {
                        continue;
                    };
                    let fetch_state = fetch_updates
                        .entry(TopicPartition {
                            topic_ref,
                            partition: partition_id,
                        })
                        .or_insert_with(|| state::FetchState {
                            offset: current.offset,
                            max_bytes: current.max_bytes,
                        });
                    let batch_next = progress
                        .as_ref()
                        .and_then(|progress| progress.next_offset(topic_index, partition_index));
                    let message_next = data
                        .messages
                        .last()
                        .map(|message| {
                            Self::next_message_offset(message.offset).ok_or_else(Error::codec)
                        })
                        .transpose()?;
                    if let Some(next) = batch_next.or(message_next) {
                        fetch_state.offset = current.offset.max(next);
                        if !data.messages.is_empty() {
                            fetch_state.max_bytes = self.client.fetch_max_bytes_per_partition();
                            empty = false;
                        }
                    } else {
                        self.stage_empty_partition_retry(
                            &tp,
                            data.highwatermark_offset,
                            fetch_state,
                            num_partitions_queried,
                            &mut retry_updates,
                        )?;
                    }
                }
            }
        }
        // All broker/partition validation and local retry decisions succeeded.
        // Publish fetch cursors only; business messages remain explicitly consumed.
        let completed_retry = retry_partition.filter(|tp| fetch_updates.contains_key(tp));
        self.state.fetch_offsets.extend(fetch_updates);
        if let Some(tp) = completed_retry {
            self.state.complete_retry_partition(&tp);
        }
        self.state.retry_partitions.extend(retry_updates);
        Ok(MessageSets {
            responses: responses
                .into_iter()
                .map(|(response, _)| response)
                .collect(),
            empty,
        })
    }

    fn validate_and_trim_fetch_responses(
        &self,
        responses: &mut [(
            fetch_kp::OwnedFetchResponse,
            Option<fetch_kp::FetchProgress>,
        )],
    ) -> Result<()> {
        let mut seen = std::collections::HashSet::with_hasher(state::PartitionHasher::default());
        for (response, progress) in responses.iter_mut() {
            for (topic_index, topic) in response.topics.iter_mut().enumerate() {
                let topic_ref = self
                    .state
                    .assignments
                    .topic_ref(&topic.topic)
                    .ok_or_else(Error::codec)?;
                for (partition_index, partition) in topic.partitions.iter_mut().enumerate() {
                    let tp = TopicPartition {
                        topic_ref,
                        partition: partition.partition,
                    };
                    let current = self.state.fetch_offsets.get(&tp).ok_or_else(Error::codec)?;
                    if !seen.insert(tp) {
                        return Err(Error::codec());
                    }
                    if let Ok(data) = &mut partition.data {
                        let next = progress.as_ref().and_then(|progress| {
                            progress.next_offset(topic_index, partition_index)
                        });
                        if next.is_some_and(|cursor| cursor < 0)
                            || data.messages.iter().any(|message| {
                                Self::next_message_offset(message.offset).is_none()
                                    || next.is_some_and(|cursor| message.offset >= cursor)
                            })
                        {
                            return Err(Error::codec());
                        }
                        data.messages
                            .retain(|message| message.offset >= current.offset);
                    }
                }
            }
        }
        // Decoder errors describe a malformed response, so they must not be
        // hidden by an earlier partition's retriable broker error.
        for (response, _) in responses.iter() {
            for topic in &response.topics {
                for partition in &topic.partitions {
                    if let Err(error) = partition.data()
                        && !matches!(error.as_ref(), Error::TopicPartitionError { .. })
                    {
                        return Err(Error::from(Arc::clone(error)));
                    }
                }
            }
        }
        for (response, _) in responses {
            for topic in &response.topics {
                for partition in &topic.partitions {
                    if let Err(error) = partition.data()
                        && !matches!(
                            error.as_ref(),
                            Error::TopicPartitionError {
                                error_code: KafkaCode::OffsetOutOfRange,
                                ..
                            }
                        )
                    {
                        return Err(Error::from(Arc::clone(error)));
                    }
                }
            }
        }
        Ok(())
    }

    fn stage_empty_partition_retry(
        &self,
        tp: &TopicPartition,
        highwatermark: i64,
        fetch_state: &mut state::FetchState,
        num_partitions_queried: u32,
        retry_updates: &mut Vec<TopicPartition>,
    ) -> Result<()> {
        debug!(
            topic = self.state.topic_name(tp.topic_ref),
            partition = tp.partition,
            offset = fetch_state.offset,
            highwatermark,
            "No batches received for partition"
        );
        if fetch_state.offset < highwatermark {
            if fetch_state.max_bytes < self.config.retry_max_bytes_limit {
                fetch_state.max_bytes = fetch_state
                    .max_bytes
                    .saturating_mul(2)
                    .min(self.config.retry_max_bytes_limit);
            } else if num_partitions_queried == 1 {
                return Err(Error::Kafka(KafkaCode::MessageSizeTooLarge));
            }
            if !self.single_partition_consumer() {
                retry_updates.push(TopicPartition {
                    topic_ref: tp.topic_ref,
                    partition: tp.partition,
                });
            }
        }
        Ok(())
    }

    fn stage_partition_data<'a>(
        topic: &str,
        tp: &TopicPartition,
        partition: &'a mut fetch_kp::OwnedPartition,
        current: &state::FetchState,
        fetch_updates: &mut HashMap<TopicPartition, state::FetchState, state::PartitionHasher>,
    ) -> Result<Option<&'a mut fetch_kp::OwnedData>> {
        match partition.data.as_mut() {
            Ok(data) => Ok(Some(data)),
            Err(error) => {
                if let Error::TopicPartitionError {
                    error_code: KafkaCode::OffsetOutOfRange,
                    ..
                } = error.as_ref()
                {
                    if partition.highwatermark < 0 {
                        // The broker's OOR response can carry an unknown
                        // watermark. Do not publish that sentinel as a cursor.
                        return Err(Error::from(Arc::clone(error)));
                    }
                    let fetch_state = fetch_updates
                        .entry(TopicPartition {
                            topic_ref: tp.topic_ref,
                            partition: tp.partition,
                        })
                        .or_insert_with(|| state::FetchState {
                            offset: current.offset,
                            max_bytes: current.max_bytes,
                        });
                    debug!(
                        "OffsetOutOfRange for {}:{}, resetting to highwatermark {}",
                        topic, partition.partition, partition.highwatermark
                    );
                    fetch_state.offset = partition.highwatermark;
                    Ok(None)
                } else {
                    Err(Error::from(Arc::clone(error)))
                }
            }
        }
    }

    fn next_message_offset(offset: i64) -> Option<i64> {
        if offset < 0 {
            return None;
        }
        offset.checked_add(1)
    }

    /// Retrieves the offset of the last "consumed" message in the
    /// specified partition. Results in `None` if there is no such
    /// "consumed" message.
    #[must_use]
    pub fn last_consumed_message(&self, topic: &str, partition: i32) -> Option<i64> {
        self.state
            .topic_ref(topic)
            .and_then(|tref| {
                self.state.consumed_offsets.get(&TopicPartition {
                    topic_ref: tref,
                    partition,
                })
            })
            .map(|co| co.offset)
    }

    /// Marks the message at the specified offset in the specified
    /// topic partition as consumed by the caller.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic/partition is not assigned to this
    /// consumer, or the message offset is negative or cannot be advanced to a
    /// representable next offset.
    pub fn consume_message(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let topic_ref = self
            .state
            .topic_ref(topic)
            .ok_or(Error::Kafka(KafkaCode::UnknownTopicOrPartition))?;
        Self::next_message_offset(offset).ok_or_else(|| {
            Error::Config(
                "consumed message offset must be non-negative and less than i64::MAX".into(),
            )
        })?;

        let tp = TopicPartition {
            topic_ref,
            partition,
        };
        if !self.state.fetch_offsets.contains_key(&tp) {
            return Err(Error::TopicPartitionError {
                topic_name: topic.to_owned(),
                partition_id: partition,
                error_code: KafkaCode::UnknownTopicOrPartition,
            });
        }
        match self.state.consumed_offsets.entry(tp) {
            Entry::Vacant(v) => {
                v.insert(state::ConsumedOffset {
                    offset,
                    dirty: true,
                });
            }
            Entry::Occupied(mut v) => {
                let o = v.get_mut();
                if offset > o.offset {
                    o.offset = offset;
                    o.dirty = true;
                }
            }
        }
        Ok(())
    }

    /// A convenience method to mark the given message set consumed as a
    /// whole by the caller. This is equivalent to marking the last
    /// message of the given set as consumed.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic of the message set is not being consumed.
    pub fn consume_messageset(&mut self, msgs: &MessageSet) -> Result<()> {
        if let Some(last) = msgs.messages.last() {
            self.consume_message(&msgs.topic, msgs.partition, last.offset)
        } else {
            Ok(())
        }
    }

    /// Marks the last message in a borrowed message set as consumed.
    ///
    /// # Errors
    ///
    /// Returns the same topic and offset errors as `consume_message`.
    pub fn consume_messageset_ref(&mut self, msgs: &MessageSetRef<'_>) -> Result<()> {
        if let Some(last) = msgs.messages.last() {
            self.consume_message(msgs.topic, msgs.partition, last.offset)
        } else {
            Ok(())
        }
    }

    /// Persists the so-far "marked as consumed" messages (on behalf
    /// of this consumer's group for the underlying topic - if any.)
    ///
    /// # Errors
    ///
    /// Returns an error if no group is configured or if committing
    /// offsets to Kafka fails.
    pub fn commit_consumed(&mut self) -> Result<()> {
        if self.config.group.is_empty() {
            return Err(Error::unset_group_id());
        }
        // Check the entire dirty set before handing an iterator to the client.
        // Rejected offsets leave all dirty flags set and send no request.
        for offset in self
            .state
            .consumed_offsets
            .values()
            .filter(|offset| offset.dirty)
        {
            Self::next_message_offset(offset.offset).ok_or_else(|| {
                Error::Config(
                    "consumed message offset must be non-negative and less than i64::MAX".into(),
                )
            })?;
        }
        debug!(
            "commit_consumed: committing dirty-only consumer offsets (group: {} / offsets: {:?}",
            self.config.group,
            self.state.consumed_offsets_debug()
        );
        let (client, state) = (&mut self.client, &mut self.state);
        client.commit_offsets(
            &self.config.group,
            state
                .consumed_offsets
                .iter()
                .filter(|&(_, o)| o.dirty)
                .map(|(tp, o)| {
                    let topic = state.topic_name(tp.topic_ref);
                    CommitOffset::new(topic, tp.partition, o.offset.saturating_add(1))
                }),
        )?;
        for co in state.consumed_offsets.values_mut() {
            if co.dirty {
                co.dirty = false;
            }
        }
        Ok(())
    }
}

// --------------------------------------------------------------------

/// Messages retrieved from kafka in one fetch request.
#[derive(Debug)]
pub struct MessageSets {
    responses: Vec<fetch_kp::OwnedFetchResponse>,
    empty: bool,
}

impl MessageSets {
    /// Creates message sets from owned fetch responses.
    ///
    /// This is primarily useful for async wrappers that already obtained
    /// owned fetch responses and need to expose the standard consumer view.
    #[must_use]
    pub fn from_fetch_responses(responses: Vec<fetch_kp::OwnedFetchResponse>) -> Self {
        let empty = !responses
            .iter()
            .flat_map(|r| r.topics.iter())
            .flat_map(|t| t.partitions.iter())
            .filter_map(|p| p.data().ok())
            .any(|d| !d.messages.is_empty());
        Self { responses, empty }
    }

    /// Determines efficiently whether there are any consumeable
    /// messages in this data set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.empty
    }

    /// Returns an iterator over the contained `MessageSet`s.
    #[must_use]
    pub fn iter(&self) -> MessageSetsIter<'_> {
        MessageSetsIter::new(&self.responses)
    }

    /// Borrows message sets without copying topic names or message vectors.
    ///
    /// Use `MessageSetRef::to_owned` when a set must outlive this response.
    #[must_use]
    pub fn iter_ref(&self) -> MessageSetsRefIter<'_> {
        MessageSetsRefIter::new(&self.responses)
    }
}

impl<'a> IntoIterator for &'a MessageSets {
    type Item = MessageSet;
    type IntoIter = MessageSetsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A set of messages successfully retrieved from a specific topic
/// partition.
#[derive(Debug)]
pub struct MessageSet {
    topic: String,
    partition: i32,
    messages: Vec<Message>,
}

impl MessageSet {
    /// Returns the topic name for this message set.
    #[inline]
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition id for this message set.
    #[must_use]
    #[inline]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns a slice of messages contained in this set.
    #[must_use]
    #[inline]
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// Returns an iterator over the messages in this set.
    #[inline]
    pub fn iter(&self) -> slice::Iter<'_, Message> {
        self.messages.iter()
    }
}

impl<'a> IntoIterator for &'a MessageSet {
    type Item = &'a Message;
    type IntoIter = slice::Iter<'a, Message>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages.iter()
    }
}

/// An iterator over the consumed topic partition message sets.
pub struct MessageSetsIter<'a> {
    inner: MessageSetsRefIter<'a>,
}

impl<'a> MessageSetsIter<'a> {
    fn new(responses: &'a [fetch_kp::OwnedFetchResponse]) -> Self {
        Self {
            inner: MessageSetsRefIter::new(responses),
        }
    }
}

impl Iterator for MessageSetsIter<'_> {
    type Item = MessageSet;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|set| set.to_owned())
    }
}

/// A message set borrowed from the response that contains it.
#[derive(Debug, Clone, Copy)]
pub struct MessageSetRef<'a> {
    topic: &'a str,
    partition: i32,
    messages: &'a [Message],
}

impl<'a> MessageSetRef<'a> {
    /// Returns the topic name without allocating.
    #[must_use]
    pub fn topic(&self) -> &'a str {
        self.topic
    }

    /// Returns the partition ID.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the messages borrowed from the original response.
    #[must_use]
    pub fn messages(&self) -> &'a [Message] {
        self.messages
    }

    /// Copies the set into the existing owned representation.
    #[must_use]
    pub fn to_owned(&self) -> MessageSet {
        MessageSet {
            topic: self.topic.to_owned(),
            partition: self.partition,
            messages: self.messages.to_vec(),
        }
    }
}

/// Iterates over borrowed message sets, skipping errors and empty partitions.
pub struct MessageSetsRefIter<'a> {
    responses: slice::Iter<'a, fetch_kp::OwnedFetchResponse>,
    topics: Option<slice::Iter<'a, fetch_kp::OwnedTopic>>,
    curr_topic: Option<&'a str>,
    partitions: Option<slice::Iter<'a, fetch_kp::OwnedPartition>>,
}

impl<'a> MessageSetsRefIter<'a> {
    fn new(responses: &'a [fetch_kp::OwnedFetchResponse]) -> Self {
        Self {
            responses: responses.iter(),
            topics: None,
            curr_topic: None,
            partitions: None,
        }
    }
}

impl<'a> Iterator for MessageSetsRefIter<'a> {
    type Item = MessageSetRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try the next partition in the current topic
            if let Some(p) = self.partitions.as_mut().and_then(Iterator::next) {
                if let Ok(data) = p.data()
                    && !data.messages.is_empty()
                {
                    return Some(MessageSetRef {
                        topic: self.curr_topic.unwrap_or(""),
                        partition: p.partition,
                        messages: &data.messages,
                    });
                }
                continue;
            }
            // Advance to next topic
            if let Some(t) = self.topics.as_mut().and_then(Iterator::next) {
                self.curr_topic = Some(&t.topic);
                self.partitions = Some(t.partitions.iter());
                continue;
            }
            // Advance to next response
            if let Some(r) = self.responses.next() {
                self.topics = Some(r.topics.iter());
                self.curr_topic = None;
                continue;
            }
            return None;
        }
    }
}

#[cfg(test)]
mod pause_resume_tests {
    use super::*;
    use std::collections::{HashSet, VecDeque};

    fn make_consumer() -> Consumer {
        let assignments = assignment::from_map(HashMap::from([("t".to_owned(), vec![0, 1])]));
        let topic_ref = assignments.topic_ref("t").unwrap();
        let fetch_offsets = [0, 1]
            .into_iter()
            .map(|partition| {
                (
                    TopicPartition {
                        topic_ref,
                        partition,
                    },
                    state::FetchState {
                        offset: 10 + i64::from(partition),
                        max_bytes: 1024,
                    },
                )
            })
            .collect();
        Consumer {
            client: KafkaClient::new(Vec::new()),
            state: state::State {
                assignments,
                fetch_offsets,
                retry_partitions: VecDeque::new(),
                consumed_offsets: HashMap::default(),
                paused: HashSet::new(),
                paused_assignments: HashSet::default(),
            },
            config: config::Config {
                group: String::new(),
                fallback_offset: FetchOffset::Earliest,
                retry_max_bytes_limit: i32::MAX,
            },
        }
    }

    fn topic_partition(consumer: &Consumer, partition: i32) -> TopicPartition {
        TopicPartition {
            topic_ref: consumer.state.topic_ref("t").unwrap(),
            partition,
        }
    }

    fn requested_partitions(consumer: &Consumer) -> Vec<i32> {
        let mut partitions: Vec<_> = consumer
            .state
            .fetch_requests()
            .map(|request| request.partition)
            .collect();
        partitions.sort_unstable();
        partitions
    }

    #[test]
    fn seek_rejects_negative_and_unassigned_positions_without_mutation() {
        let mut consumer = make_consumer();
        let before = requested_partitions(&consumer);
        let offsets: Vec<_> = [0, 1]
            .map(|partition| {
                let state = consumer
                    .state
                    .fetch_offsets
                    .get(&topic_partition(&consumer, partition))
                    .unwrap();
                (state.offset, state.max_bytes)
            })
            .into();
        assert!(matches!(consumer.seek("t", 0, -1), Err(Error::Config(_))));
        assert!(matches!(
            consumer.seek("t", 2, 5),
            Err(Error::TopicPartitionError {
                error_code: KafkaCode::UnknownTopicOrPartition,
                ..
            })
        ));
        assert_eq!(requested_partitions(&consumer), before);
        let after: Vec<_> = [0, 1]
            .map(|partition| {
                let state = consumer
                    .state
                    .fetch_offsets
                    .get(&topic_partition(&consumer, partition))
                    .unwrap();
                (state.offset, state.max_bytes)
            })
            .into();
        assert_eq!(after, offsets);
    }

    #[test]
    fn seek_accepts_zero_and_maximum_cursor_offsets() {
        let mut consumer = make_consumer();
        consumer.seek("t", 0, 0).unwrap();
        consumer.seek("t", 0, i64::MAX).unwrap();
        assert_eq!(
            consumer
                .state
                .fetch_offsets
                .get(&topic_partition(&consumer, 0))
                .unwrap()
                .offset,
            i64::MAX
        );
    }

    #[test]
    fn consume_message_rejects_unassigned_partitions_before_dirtifying_offsets() {
        let mut consumer = make_consumer();
        assert!(matches!(
            consumer.consume_message("t", 2, 50),
            Err(Error::TopicPartitionError {
                error_code: KafkaCode::UnknownTopicOrPartition,
                ..
            })
        ));
        assert!(consumer.state.consumed_offsets.is_empty());

        consumer.consume_message("t", 0, 50).unwrap();
        assert_eq!(consumer.last_consumed_message("t", 0), Some(50));
        assert_eq!(consumer.last_consumed_message("t", 1), None);
    }

    #[test]
    fn negative_oor_high_watermark_does_not_publish_or_complete_a_retry() {
        let mut consumer = make_consumer();
        let tp = topic_partition(&consumer, 0);
        consumer.state.retry_partitions.push_back(tp);
        let before = consumer
            .state
            .fetch_offsets
            .get(&topic_partition(&consumer, 0))
            .unwrap()
            .offset;
        let response = fetch_kp::OwnedFetchResponse {
            correlation_id: 1,
            topics: vec![fetch_kp::OwnedTopic {
                topic: "t".to_owned(),
                partitions: vec![fetch_kp::OwnedPartition {
                    partition: 0,
                    highwatermark: -1,
                    data: Err(Arc::new(Error::TopicPartitionError {
                        topic_name: "t".to_owned(),
                        partition_id: 0,
                        error_code: KafkaCode::OffsetOutOfRange,
                    })),
                }],
            }],
        };

        assert!(matches!(
            consumer.process_fetch_responses(
                1,
                Some(topic_partition(&consumer, 0)),
                vec![response],
            ),
            Err(Error::TopicPartitionError {
                error_code: KafkaCode::OffsetOutOfRange,
                ..
            })
        ));
        assert_eq!(
            consumer
                .state
                .fetch_offsets
                .get(&topic_partition(&consumer, 0))
                .unwrap()
                .offset,
            before
        );
        assert_eq!(consumer.state.retry_partitions.len(), 1);
    }

    #[test]
    fn test_pause_and_resume() {
        let mut consumer = make_consumer();
        assert_eq!(requested_partitions(&consumer), vec![0, 1]);
        consumer.pause("t", &[0]);
        assert!(consumer.is_paused("t", 0));
        assert_eq!(requested_partitions(&consumer), vec![1]);
        consumer.resume("t", &[0]);
        assert!(!consumer.is_paused("t", 0));
        assert_eq!(requested_partitions(&consumer), vec![0, 1]);
        let state = consumer
            .state
            .fetch_offsets
            .get(&topic_partition(&consumer, 0))
            .unwrap();
        assert_eq!(state.offset, 10);
    }

    #[test]
    fn test_pause_multiple_partitions() {
        let mut consumer = make_consumer();
        consumer.pause("t", &[0, 1]);
        assert_eq!(consumer.paused_partitions().count(), 2);
        assert!(consumer.poll().unwrap().is_empty());
        let (count, retry, responses) = consumer.fetch_messages();
        assert_eq!(count, 0);
        assert!(retry.is_none());
        assert!(responses.unwrap().is_empty());
        consumer.resume("t", &[1]);
        assert_eq!(requested_partitions(&consumer), vec![1]);
    }

    #[test]
    fn test_pause_nonexistent_partition_no_panic() {
        let mut consumer = make_consumer();
        consumer.pause("t", &[999]);
        consumer.pause("unknown", &[0]);
        assert!(consumer.is_paused("t", 999));
        assert!(consumer.is_paused("unknown", 0));
        assert_eq!(requested_partitions(&consumer), vec![0, 1]);
        consumer.resume("t", &[999]);
        consumer.resume("unknown", &[0]);
        assert_eq!(consumer.paused_partitions().count(), 0);
    }

    #[test]
    fn paused_retry_is_retained_without_blocking_active_retries() {
        let mut consumer = make_consumer();
        consumer.state.retry_partitions =
            VecDeque::from([topic_partition(&consumer, 0), topic_partition(&consumer, 1)]);
        consumer.pause("t", &[0]);
        assert_eq!(
            consumer.state.next_retry_partition(),
            Some(&topic_partition(&consumer, 1))
        );
        let completed = topic_partition(&consumer, 1);
        consumer.state.complete_retry_partition(&completed);
        assert_eq!(consumer.state.retry_partitions.len(), 1);
        assert_eq!(consumer.state.next_retry_partition(), None);
        consumer.pause("t", &[1]);
        assert!(consumer.poll().unwrap().is_empty());
        assert_eq!(consumer.state.retry_partitions.len(), 1);
        consumer.resume("t", &[0]);
        assert_eq!(
            consumer.state.next_retry_partition(),
            Some(&topic_partition(&consumer, 0))
        );
        let completed = topic_partition(&consumer, 0);
        consumer.state.complete_retry_partition(&completed);
        assert!(consumer.state.retry_partitions.is_empty());
    }

    #[test]
    fn retry_byte_growth_saturates_at_configured_limit() {
        let mut consumer = make_consumer();
        let tp = topic_partition(&consumer, 0);
        consumer.state.fetch_offsets.get_mut(&tp).unwrap().max_bytes = 1_500_000_000;
        let response = fetch_response(vec![partition_data(0, 20, &[])]);
        assert!(
            consumer
                .process_fetch_responses(1, None, vec![response])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            consumer
                .state
                .fetch_offsets
                .get(&topic_partition(&consumer, 0))
                .unwrap()
                .max_bytes,
            i32::MAX
        );
    }

    fn fetch_progress(consumer: &Consumer) -> Vec<(i32, i64, i32)> {
        let mut progress: Vec<_> = consumer
            .state
            .fetch_offsets
            .iter()
            .map(|(tp, state)| (tp.partition, state.offset, state.max_bytes))
            .collect();
        progress.sort_unstable();
        progress
    }

    fn partition_data(
        partition: i32,
        highwatermark: i64,
        offsets: &[i64],
    ) -> fetch_kp::OwnedPartition {
        fetch_kp::OwnedPartition {
            partition,
            highwatermark,
            data: Ok(fetch_kp::OwnedData {
                highwatermark_offset: highwatermark,
                messages: offsets
                    .iter()
                    .map(|&offset| Message {
                        offset,
                        key: bytes::Bytes::new(),
                        value: bytes::Bytes::new(),
                    })
                    .collect(),
            }),
        }
    }

    fn partition_error(
        partition: i32,
        highwatermark: i64,
        error_code: KafkaCode,
    ) -> fetch_kp::OwnedPartition {
        fetch_kp::OwnedPartition {
            partition,
            highwatermark,
            data: Err(Arc::new(Error::TopicPartitionError {
                topic_name: "t".to_owned(),
                partition_id: partition,
                error_code,
            })),
        }
    }

    fn fetch_response(partitions: Vec<fetch_kp::OwnedPartition>) -> fetch_kp::OwnedFetchResponse {
        fetch_kp::OwnedFetchResponse {
            correlation_id: 1,
            topics: vec![fetch_kp::OwnedTopic {
                topic: "t".to_owned(),
                partitions,
            }],
        }
    }

    #[test]
    fn borrowed_iteration_preserves_owned_values_without_copying_storage() {
        let mut response = fetch_response(vec![
            partition_data(0, 30, &[10, 11]),
            partition_data(1, 30, &[]),
            partition_error(2, 30, KafkaCode::NotLeaderForPartition),
        ]);
        response.topics.push(fetch_kp::OwnedTopic {
            topic: "u".to_owned(),
            partitions: vec![partition_data(5, 50, &[42])],
        });
        let sets = MessageSets::from_fetch_responses(vec![response]);
        let views: Vec<_> = sets.iter_ref().collect();
        let owned: Vec<_> = sets.iter().collect();
        assert_eq!(views.len(), 2);
        assert_eq!(owned.len(), views.len());
        for (view, owned) in views.iter().zip(&owned) {
            assert_eq!(view.topic(), owned.topic());
            assert_eq!(view.partition(), owned.partition());
            let offsets =
                |messages: &[Message]| messages.iter().map(|m| m.offset).collect::<Vec<_>>();
            assert_eq!(offsets(view.messages()), offsets(owned.messages()));
        }
        let source = &sets.responses[0].topics[0];
        assert_eq!(views[0].topic().as_ptr(), source.topic.as_ptr());
        assert_eq!(
            views[0].messages().as_ptr(),
            source.partitions[0].data().unwrap().messages.as_ptr()
        );
        let retained = views[0].to_owned();
        drop(views);
        drop(sets);
        assert_eq!(retained.topic(), "t");
        assert_eq!(
            retained
                .messages()
                .iter()
                .map(|m| m.offset)
                .collect::<Vec<_>>(),
            [10, 11]
        );
    }

    #[test]
    fn borrowed_message_set_can_mark_consumed_without_materializing_an_owned_set() {
        let mut consumer = make_consumer();
        let sets = MessageSets::from_fetch_responses(vec![fetch_response(vec![partition_data(
            0,
            30,
            &[10, 11],
        )])]);
        let view = sets.iter_ref().next().unwrap();
        consumer.consume_messageset_ref(&view).unwrap();
        assert_eq!(consumer.last_consumed_message("t", 0), Some(11));
    }

    #[test]
    fn later_partition_error_does_not_skip_undelivered_messages() {
        let mut consumer = make_consumer();
        let before = fetch_progress(&consumer);
        let response = fetch_response(vec![
            partition_data(0, 30, &[10, 11]),
            partition_error(1, 30, KafkaCode::NotLeaderForPartition),
        ]);

        assert!(
            consumer
                .process_fetch_responses(2, None, vec![response])
                .is_err()
        );
        assert_eq!(fetch_progress(&consumer), before);
        assert!(consumer.state.retry_partitions.is_empty());
    }

    #[test]
    fn unexpected_response_members_return_errors_without_publishing_progress() {
        for (topic, partition, error_code) in [
            ("unexpected", 0, None),
            ("t", 99, None),
            ("t", 99, Some(KafkaCode::OffsetOutOfRange)),
        ] {
            let mut consumer = make_consumer();
            let retry = topic_partition(&consumer, 0);
            consumer.state.retry_partitions.push_back(retry);
            let before = fetch_progress(&consumer);
            let unexpected = error_code.map_or_else(
                || partition_data(partition, 30, &[20]),
                |code| partition_error(partition, 30, code),
            );
            let mut response = fetch_response(vec![partition_data(0, 30, &[10, 11])]);
            response.topics.push(fetch_kp::OwnedTopic {
                topic: topic.to_owned(),
                partitions: vec![unexpected],
            });
            let retry_for_poll = topic_partition(&consumer, 0);
            assert!(matches!(
                consumer.process_fetch_responses(2, Some(retry_for_poll), vec![response]),
                Err(Error::Protocol(crate::error::ProtocolError::Codec))
            ));
            assert_eq!(fetch_progress(&consumer), before);
            assert_eq!(consumer.state.retry_partitions.len(), 1);
            let expected_retry = topic_partition(&consumer, 0);
            assert_eq!(consumer.state.next_retry_partition(), Some(&expected_retry));
        }
    }

    #[test]
    fn later_message_size_error_does_not_publish_retry_growth() {
        let mut consumer = make_consumer();
        let before = fetch_progress(&consumer);
        let response = fetch_response(vec![
            partition_data(0, 30, &[]),
            partition_error(1, 30, KafkaCode::MessageSizeTooLarge),
        ]);

        assert!(
            consumer
                .process_fetch_responses(2, None, vec![response])
                .is_err()
        );
        assert_eq!(fetch_progress(&consumer), before);
        assert!(consumer.state.retry_partitions.is_empty());
    }

    #[test]
    fn local_message_size_error_retains_retry_obligation() {
        let mut consumer = make_consumer();
        let tp = topic_partition(&consumer, 0);
        consumer.state.retry_partitions.push_back(tp);
        consumer.config.retry_max_bytes_limit = 1024;
        let before = fetch_progress(&consumer);
        let retry = Some(topic_partition(&consumer, 0));
        let response = fetch_response(vec![partition_data(0, 30, &[])]);

        assert!(matches!(
            consumer.process_fetch_responses(1, retry, vec![response]),
            Err(Error::Kafka(KafkaCode::MessageSizeTooLarge))
        ));
        assert_eq!(fetch_progress(&consumer), before);
        assert_eq!(consumer.state.retry_partitions.len(), 1);
    }

    #[test]
    fn poll_failure_and_missing_response_retain_retry_obligation() {
        let mut consumer = make_consumer();
        let missing = topic_partition(&consumer, 999);
        consumer.state.retry_partitions.push_back(missing);
        let before = fetch_progress(&consumer);

        assert!(consumer.poll().is_err());
        assert_eq!(fetch_progress(&consumer), before);
        assert_eq!(consumer.state.retry_partitions.len(), 1);

        consumer.state.retry_partitions.clear();
        let tp = topic_partition(&consumer, 0);
        consumer.state.retry_partitions.push_back(tp);
        // Missing metadata is rejected before the client can issue this retry.
        assert!(matches!(
            consumer.poll(),
            Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition))
        ));
        assert_eq!(fetch_progress(&consumer), before);
        assert_eq!(consumer.state.retry_partitions.len(), 1);
        let retry = Some(topic_partition(&consumer, 0));
        assert!(
            consumer
                .process_fetch_responses(1, retry, Vec::new())
                .unwrap()
                .is_empty()
        );
        assert_eq!(fetch_progress(&consumer), before);
        assert_eq!(consumer.state.retry_partitions.len(), 1);
    }

    #[test]
    fn partition_error_keeps_retry_until_successful_publication() {
        let mut consumer = make_consumer();
        let tp = topic_partition(&consumer, 0);
        consumer.state.retry_partitions.push_back(tp);
        let before = fetch_progress(&consumer);
        let failed = fetch_response(vec![partition_error(
            0,
            30,
            KafkaCode::NotLeaderForPartition,
        )]);
        let retry = Some(topic_partition(&consumer, 0));
        assert!(
            consumer
                .process_fetch_responses(1, retry, vec![failed])
                .is_err()
        );
        assert_eq!(fetch_progress(&consumer), before);
        assert_eq!(consumer.state.retry_partitions.len(), 1);

        let retry = Some(topic_partition(&consumer, 0));
        let response = fetch_response(vec![partition_data(0, 30, &[10, 11])]);

        let messages = consumer
            .process_fetch_responses(1, retry, vec![response])
            .unwrap();
        assert!(!messages.is_empty());
        let progress = consumer
            .state
            .fetch_offsets
            .get(&topic_partition(&consumer, 0))
            .unwrap();
        assert_eq!(progress.offset, 12);
        assert_eq!(
            progress.max_bytes,
            consumer.client.fetch_max_bytes_per_partition()
        );
        assert!(consumer.state.retry_partitions.is_empty());
    }

    #[test]
    fn successful_empty_retry_replaces_obligation_after_growth() {
        let mut consumer = make_consumer();
        let tp = topic_partition(&consumer, 0);
        consumer.state.retry_partitions.push_back(tp);
        let retry = Some(topic_partition(&consumer, 0));
        let response = fetch_response(vec![partition_data(0, 30, &[])]);

        assert!(
            consumer
                .process_fetch_responses(1, retry, vec![response])
                .unwrap()
                .is_empty()
        );
        let progress = consumer
            .state
            .fetch_offsets
            .get(&topic_partition(&consumer, 0))
            .unwrap();
        assert_eq!(progress.offset, 10);
        assert_eq!(progress.max_bytes, 2048);
        assert_eq!(consumer.state.retry_partitions.len(), 1);
        assert_eq!(
            consumer.state.next_retry_partition(),
            Some(&topic_partition(&consumer, 0))
        );
    }

    #[test]
    fn offset_out_of_range_reset_is_published_only_with_successful_poll() {
        let mut consumer = make_consumer();
        let before = fetch_progress(&consumer);
        let failed = fetch_response(vec![
            partition_error(0, 20, KafkaCode::OffsetOutOfRange),
            partition_error(1, 30, KafkaCode::NotLeaderForPartition),
        ]);
        assert!(
            consumer
                .process_fetch_responses(2, None, vec![failed])
                .is_err()
        );
        assert_eq!(fetch_progress(&consumer), before);

        let successful = fetch_response(vec![
            partition_error(0, 20, KafkaCode::OffsetOutOfRange),
            partition_data(1, 30, &[11, 12]),
        ]);
        assert!(
            !consumer
                .process_fetch_responses(2, None, vec![successful])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            consumer
                .state
                .fetch_offsets
                .get(&topic_partition(&consumer, 0))
                .unwrap()
                .offset,
            20
        );
        assert_eq!(
            consumer
                .state
                .fetch_offsets
                .get(&topic_partition(&consumer, 1))
                .unwrap()
                .offset,
            13
        );
    }

    #[test]
    fn seek_inside_a_batch_discards_only_its_prefix() {
        let mut consumer = make_consumer();
        consumer.seek("t", 0, 3).unwrap();
        let response = fetch_response(vec![partition_data(0, 6, &[0, 1, 2, 3, 4, 5])]);
        let sets = consumer
            .process_fetch_responses(1, None, vec![response])
            .unwrap();
        let owned = sets.iter().next().unwrap();
        let borrowed = sets.iter_ref().next().unwrap();
        assert_eq!(
            owned
                .messages()
                .iter()
                .map(|message| message.offset)
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert_eq!(
            borrowed
                .messages()
                .iter()
                .map(|message| message.offset)
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert_eq!(
            consumer
                .state
                .fetch_offsets
                .get(&topic_partition(&consumer, 0))
                .unwrap()
                .offset,
            6
        );
    }

    #[test]
    fn fully_filtered_batch_keeps_position_and_completes_a_successful_retry() {
        let mut consumer = make_consumer();
        let retry = topic_partition(&consumer, 0);
        consumer.state.retry_partitions.push_back(retry);
        let before = fetch_progress(&consumer);
        let response = fetch_response(vec![partition_data(0, 3, &[0, 1, 2])]);
        let token = Some(topic_partition(&consumer, 0));
        let sets = consumer
            .process_fetch_responses(1, token, vec![response])
            .unwrap();
        assert!(sets.is_empty());
        assert_eq!(fetch_progress(&consumer), before);
        assert!(consumer.state.retry_partitions.is_empty());
    }

    #[test]
    fn invalid_offsets_are_rejected_before_filtering_batch_prefixes() {
        for invalid in [-1, i64::MIN, i64::MAX] {
            let mut consumer = make_consumer();
            let before = fetch_progress(&consumer);
            let response = fetch_response(vec![partition_data(0, 30, &[invalid, 11])]);
            assert!(matches!(
                consumer.process_fetch_responses(1, None, vec![response]),
                Err(Error::Protocol(crate::error::ProtocolError::Codec))
            ));
            assert_eq!(fetch_progress(&consumer), before);
            assert!(consumer.state.retry_partitions.is_empty());
        }
    }

    #[test]
    fn later_invalid_offset_does_not_publish_earlier_partition_progress() {
        for invalid in [-1, i64::MAX] {
            let mut consumer = make_consumer();
            let retry = topic_partition(&consumer, 0);
            consumer.state.retry_partitions.push_back(retry);
            let before = fetch_progress(&consumer);
            let response = fetch_response(vec![
                partition_data(0, 30, &[10, 11]),
                partition_data(1, 30, &[11, invalid]),
            ]);
            let retry = Some(topic_partition(&consumer, 0));
            assert!(matches!(
                consumer.process_fetch_responses(2, retry, vec![response]),
                Err(Error::Protocol(crate::error::ProtocolError::Codec))
            ));
            assert_eq!(fetch_progress(&consumer), before);
            assert_eq!(consumer.state.retry_partitions.len(), 1);
        }
    }

    #[test]
    fn last_representable_message_advances_to_the_maximum_fetch_offset() {
        let mut consumer = make_consumer();
        let response = fetch_response(vec![partition_data(0, i64::MAX, &[i64::MAX - 1])]);
        let sets = consumer
            .process_fetch_responses(1, None, vec![response])
            .unwrap();
        assert_eq!(
            sets.iter_ref().next().unwrap().messages()[0].offset,
            i64::MAX - 1
        );
        assert_eq!(
            consumer
                .state
                .fetch_offsets
                .get(&topic_partition(&consumer, 0))
                .unwrap()
                .offset,
            i64::MAX
        );
    }

    #[test]
    fn invalid_consumed_offsets_do_not_create_or_replace_dirty_progress() {
        let mut consumer = make_consumer();
        for invalid in [-1, -2, i64::MIN, i64::MAX] {
            assert!(matches!(
                consumer.consume_message("t", 0, invalid),
                Err(Error::Config(_))
            ));
            assert!(consumer.state.consumed_offsets.is_empty());
        }
        consumer.consume_message("t", 0, 0).unwrap();
        for invalid in [-1, i64::MAX] {
            assert!(matches!(
                consumer.consume_message("t", 0, invalid),
                Err(Error::Config(_))
            ));
            assert_eq!(consumer.last_consumed_message("t", 0), Some(0));
        }
        consumer.consume_message("t", 0, i64::MAX - 1).unwrap();
        assert_eq!(consumer.last_consumed_message("t", 0), Some(i64::MAX - 1));
        assert_eq!(Consumer::next_message_offset(i64::MAX - 1), Some(i64::MAX));
    }

    #[test]
    fn invalid_dirty_offsets_reject_the_whole_commit_before_any_io() {
        use std::net::TcpListener;
        for invalid in [-1, i64::MIN, i64::MAX] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let mut consumer = make_consumer();
            consumer.client = KafkaClient::new(vec![listener.local_addr().unwrap().to_string()]);
            consumer.config.group = "test-group".into();
            consumer.consume_message("t", 0, 10).unwrap();
            let partition = topic_partition(&consumer, 1);
            consumer.state.consumed_offsets.insert(
                partition,
                state::ConsumedOffset {
                    offset: invalid,
                    dirty: true,
                },
            );
            assert!(matches!(consumer.commit_consumed(), Err(Error::Config(_))));
            assert!(
                consumer
                    .state
                    .consumed_offsets
                    .values()
                    .all(|offset| offset.dirty)
            );
            assert_eq!(consumer.last_consumed_message("t", 0), Some(10));
            assert_eq!(consumer.last_consumed_message("t", 1), Some(invalid));
            assert!(
                listener
                    .accept()
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
            );
        }
    }
}

#[cfg(test)]
mod offset_wire_tests {
    use super::*;
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::{
        ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse,
        FindCoordinatorRequest, FindCoordinatorResponse, ListOffsetsRequest, ListOffsetsResponse,
        MetadataRequest, MetadataResponse, OffsetCommitRequest, OffsetCommitResponse,
        OffsetFetchRequest, OffsetFetchResponse, RequestHeader, ResponseHeader, TopicName,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::thread::JoinHandle;
    use std::time::Duration;

    enum Action {
        Fetch(Vec<i64>, i64),
        RawFetch(Bytes, i64),
        Commit(i16),
        MalformedCommit(bool),
    }

    #[derive(Debug, Default)]
    struct ObservedOffsets {
        fetched: Vec<i64>,
        committed: Vec<i64>,
    }

    fn mock_consumer(
        committed: Option<i64>,
        actions: Vec<Action>,
    ) -> (Result<Consumer>, JoinHandle<ObservedOffsets>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut observed = ObservedOffsets::default();
            if !serve_startup(&mut stream, address, committed) {
                return observed;
            }
            for action in actions {
                match action {
                    Action::Fetch(offsets, highwatermark) => {
                        let (header, request) =
                            read_request::<FetchRequest>(&mut stream, ApiKey::Fetch);
                        observed
                            .fetched
                            .push(request.topics[0].partitions[0].fetch_offset);
                        write_response(
                            &mut stream,
                            &header,
                            &fetch_response(offsets, highwatermark),
                        );
                    }
                    Action::RawFetch(records, highwatermark) => {
                        let (header, request) =
                            read_request::<FetchRequest>(&mut stream, ApiKey::Fetch);
                        observed
                            .fetched
                            .push(request.topics[0].partitions[0].fetch_offset);
                        write_response(
                            &mut stream,
                            &header,
                            &raw_fetch_response(records, highwatermark),
                        );
                    }
                    Action::Commit(error) => {
                        let (header, request) =
                            read_request::<OffsetCommitRequest>(&mut stream, ApiKey::OffsetCommit);
                        observed
                            .committed
                            .push(request.topics[0].partitions[0].committed_offset);
                        write_response(&mut stream, &header, &commit_response(error));
                    }
                    Action::MalformedCommit(sparse) => {
                        let (header, request) =
                            read_request::<OffsetCommitRequest>(&mut stream, ApiKey::OffsetCommit);
                        observed
                            .committed
                            .push(request.topics[0].partitions[0].committed_offset);
                        let response = if sparse {
                            OffsetCommitResponse::default().with_topics(vec![kafka_protocol::messages::offset_commit_response::OffsetCommitResponseTopic::default().with_name(topic_name())])
                        } else {
                            OffsetCommitResponse::default()
                        };
                        write_response(&mut stream, &header, &response);
                        let mut marker = [0];
                        match stream.read(&mut marker) {
                            Ok(0) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                            other => panic!("unexpected automatic commit replay: {other:?}"),
                        }
                        let (replacement, _) = listener.accept().unwrap();
                        stream = replacement;
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                    }
                }
            }
            observed
        });
        let builder = Consumer::from_hosts(vec![address.to_string()])
            .with_topic_partitions("t".into(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_fetch_max_wait_time(Duration::from_millis(100));
        let builder = if committed.is_some() {
            builder.with_group("test-group".into())
        } else {
            builder
        };
        (builder.create(), server)
    }

    fn serve_startup(stream: &mut TcpStream, address: SocketAddr, committed: Option<i64>) -> bool {
        let (header, _) = read_request::<ApiVersionsRequest>(stream, ApiKey::ApiVersions);
        write_response(stream, &header, &ApiVersionsResponse::default());
        let (header, _) = read_request::<MetadataRequest>(stream, ApiKey::Metadata);
        write_response(stream, &header, &metadata_response(address));
        if let Some(offset) = committed {
            let (header, _) =
                read_request::<FindCoordinatorRequest>(stream, ApiKey::FindCoordinator);
            let response = FindCoordinatorResponse::default()
                .with_node_id(BrokerId::from(1))
                .with_host(StrBytes::from_string(address.ip().to_string()))
                .with_port(i32::from(address.port()));
            write_response(stream, &header, &response);
            let (header, _) = read_request::<OffsetFetchRequest>(stream, ApiKey::OffsetFetch);
            write_response(stream, &header, &offset_fetch_response(offset));
            if offset < -1 {
                return false;
            }
        }
        let requests = if committed.is_some_and(|offset| offset >= 0) {
            2
        } else {
            1
        };
        for _ in 0..requests {
            let (header, request) = read_request::<ListOffsetsRequest>(stream, ApiKey::ListOffsets);
            let latest = committed.unwrap_or(6).max(6);
            let offset = if request.topics[0].partitions[0].timestamp == -2 {
                0
            } else {
                latest
            };
            write_response(stream, &header, &list_offset_response(offset));
        }
        true
    }

    fn read_request<T: Decodable + HeaderVersion>(
        stream: &mut TcpStream,
        key: ApiKey,
    ) -> (RequestHeader, T) {
        let mut size = [0; 4];
        stream.read_exact(&mut size).unwrap();
        let mut frame = vec![0; usize::try_from(i32::from_be_bytes(size)).unwrap()];
        stream.read_exact(&mut frame).unwrap();
        let version = i16::from_be_bytes(frame[2..4].try_into().unwrap());
        let mut frame = Bytes::from(frame);
        let header = RequestHeader::decode(&mut frame, T::header_version(version)).unwrap();
        assert_eq!(header.request_api_key, key as i16);
        let request = T::decode(&mut frame, version).unwrap();
        (header, request)
    }

    fn write_response<T: Encodable + HeaderVersion>(
        stream: &mut TcpStream,
        request: &RequestHeader,
        response: &T,
    ) {
        let version = request.request_api_version;
        let mut payload = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(request.correlation_id)
            .encode(&mut payload, T::header_version(version))
            .unwrap();
        response.encode(&mut payload, version).unwrap();
        stream
            .write_all(&i32::try_from(payload.len()).unwrap().to_be_bytes())
            .unwrap();
        stream.write_all(&payload).unwrap();
    }

    fn topic_name() -> TopicName {
        TopicName::from(StrBytes::from_static_str("t"))
    }

    fn metadata_response(address: SocketAddr) -> MetadataResponse {
        use kafka_protocol::messages::metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        };
        MetadataResponse::default()
            .with_brokers(vec![
                MetadataResponseBroker::default()
                    .with_node_id(BrokerId::from(1))
                    .with_host(StrBytes::from_string(address.ip().to_string()))
                    .with_port(i32::from(address.port())),
            ])
            .with_topics(vec![
                MetadataResponseTopic::default()
                    .with_name(Some(topic_name()))
                    .with_partitions(vec![
                        MetadataResponsePartition::default()
                            .with_partition_index(0)
                            .with_leader_id(BrokerId::from(1))
                            .with_replica_nodes(vec![BrokerId::from(1)])
                            .with_isr_nodes(vec![BrokerId::from(1)]),
                    ]),
            ])
    }

    fn offset_fetch_response(offset: i64) -> OffsetFetchResponse {
        use kafka_protocol::messages::offset_fetch_response::{
            OffsetFetchResponsePartition, OffsetFetchResponseTopic,
        };
        OffsetFetchResponse::default().with_topics(vec![
            OffsetFetchResponseTopic::default()
                .with_name(topic_name())
                .with_partitions(vec![
                    OffsetFetchResponsePartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(offset),
                ]),
        ])
    }

    fn list_offset_response(offset: i64) -> ListOffsetsResponse {
        use kafka_protocol::messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
        };
        ListOffsetsResponse::default().with_topics(vec![
            ListOffsetsTopicResponse::default()
                .with_name(topic_name())
                .with_partitions(vec![
                    ListOffsetsPartitionResponse::default()
                        .with_partition_index(0)
                        .with_offset(offset),
                ]),
        ])
    }

    fn commit_response(error: i16) -> OffsetCommitResponse {
        use kafka_protocol::messages::offset_commit_response::{
            OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        };
        OffsetCommitResponse::default().with_topics(vec![
            OffsetCommitResponseTopic::default()
                .with_name(topic_name())
                .with_partitions(vec![
                    OffsetCommitResponsePartition::default()
                        .with_partition_index(0)
                        .with_error_code(error),
                ]),
        ])
    }

    fn fetch_response(offsets: Vec<i64>, highwatermark: i64) -> FetchResponse {
        use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
        let records = if offsets.is_empty() {
            None
        } else {
            let records: Vec<_> = offsets
                .into_iter()
                .enumerate()
                .map(|(index, offset)| Record {
                    transactional: false,
                    control: false,
                    delete_horizon: false,
                    partition_leader_epoch: -1,
                    producer_id: -1,
                    producer_epoch: -1,
                    timestamp_type: TimestampType::Creation,
                    offset,
                    sequence: i32::try_from(index).unwrap(),
                    timestamp: 0,
                    key: None,
                    value: Some(Bytes::from_static(b"value")),
                    headers: indexmap::IndexMap::default(),
                })
                .collect();
            let mut data = BytesMut::new();
            RecordBatchEncoder::encode(
                &mut data,
                &records,
                &RecordEncodeOptions {
                    version: 2,
                    compression: Compression::None,
                },
            )
            .unwrap();
            Some(data.freeze())
        };
        FetchResponse::default().with_responses(vec![
            FetchableTopicResponse::default()
                .with_topic(topic_name())
                .with_partitions(vec![
                    PartitionData::default()
                        .with_partition_index(0)
                        .with_high_watermark(highwatermark)
                        .with_last_stable_offset(highwatermark)
                        .with_log_start_offset(0)
                        .with_records(records),
                ]),
        ])
    }

    fn raw_fetch_response(records: Bytes, highwatermark: i64) -> FetchResponse {
        let mut response = fetch_response(Vec::new(), highwatermark);
        response.responses[0].partitions[0].records = Some(records);
        response
    }

    fn control_batch(offset: i64) -> Bytes {
        let record = Record {
            transactional: true,
            control: true,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: 7,
            producer_epoch: 0,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: -1,
            timestamp: 0,
            key: Some(Bytes::from_static(&[0, 0, 0, 0])),
            value: Some(Bytes::from_static(&[0, 0, 0, 0, 0, 0])),
            headers: indexmap::IndexMap::default(),
        };
        let mut bytes = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut bytes,
            &[record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();
        bytes.freeze()
    }

    fn empty_compacted_batch(base: i64, last_delta: i32) -> Bytes {
        let response = fetch_response(vec![base], base + i64::from(last_delta) + 1);
        let encoded = response.responses[0].partitions[0]
            .records
            .as_ref()
            .unwrap();
        assert!(encoded.len() >= 61 && encoded[16] == 2);
        let mut bytes = encoded[..61].to_vec();
        bytes[8..12].copy_from_slice(&49_i32.to_be_bytes());
        bytes[23..27].copy_from_slice(&last_delta.to_be_bytes());
        bytes[27..35].copy_from_slice(&(-1_i64).to_be_bytes());
        bytes[57..61].copy_from_slice(&0_i32.to_be_bytes());
        set_fixture_batch_crc(&mut bytes);
        Bytes::from(bytes)
    }

    fn set_fixture_batch_crc(bytes: &mut [u8]) {
        let mut crc = !0_u32;
        for byte in &bytes[21..] {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0_u32.wrapping_sub(crc & 1) & 0x82f6_3b78);
            }
        }
        bytes[17..21].copy_from_slice(&(!crc).to_be_bytes());
    }

    #[test]
    fn tcp_control_empty_and_compacted_tail_advance_fetch_only_by_verified_batch_ends() {
        let mut records = BytesMut::new();
        records.extend_from_slice(
            fetch_response(vec![16], 100).responses[0].partitions[0]
                .records
                .as_ref()
                .unwrap(),
        );
        records.extend_from_slice(&empty_compacted_batch(17, 5));
        let (consumer, server) = mock_consumer(
            Some(-1),
            vec![
                Action::RawFetch(control_batch(10), 100),
                Action::RawFetch(empty_compacted_batch(11, 4), 100),
                Action::RawFetch(records.freeze(), 100),
                Action::Commit(0),
                Action::Fetch(Vec::new(), 23),
            ],
        );
        let mut consumer = consumer.unwrap();
        consumer.seek("t", 0, 10).unwrap();
        let initial_bytes = consumer.client.fetch_max_bytes_per_partition();
        consumer.config.retry_max_bytes_limit = initial_bytes;
        for cursor in [11, 16] {
            assert!(consumer.poll().unwrap().is_empty());
            let tp = TopicPartition {
                topic_ref: consumer.state.topic_ref("t").unwrap(),
                partition: 0,
            };
            assert_eq!(consumer.state.fetch_offsets[&tp].offset, cursor);
            assert_eq!(consumer.state.fetch_offsets[&tp].max_bytes, initial_bytes);
            assert_eq!(consumer.last_consumed_message("t", 0), None);
            assert!(consumer.state.consumed_offsets.is_empty());
            // Empty/control progress must not turn into a business commit RPC.
            consumer.commit_consumed().unwrap();
        }
        let messages = consumer.poll().unwrap();
        let set = messages.iter_ref().next().unwrap();
        assert_eq!(
            set.messages()
                .iter()
                .map(|message| message.offset)
                .collect::<Vec<_>>(),
            [16]
        );
        assert_eq!(consumer.last_consumed_message("t", 0), None);
        consumer.consume_messageset_ref(&set).unwrap();
        consumer.commit_consumed().unwrap();
        assert_eq!(consumer.last_consumed_message("t", 0), Some(16));
        assert!(consumer.poll().unwrap().is_empty());
        let observed = server.join().unwrap();
        assert_eq!(observed.fetched, [10, 11, 16, 23]);
        assert_eq!(observed.committed, [17]);
    }

    #[test]
    fn tcp_valid_batch_before_requested_offset_neither_rolls_back_nor_grows_buffer() {
        let (consumer, server) = mock_consumer(
            None,
            vec![
                Action::RawFetch(empty_compacted_batch(10, 4), 100),
                Action::Fetch(Vec::new(), 20),
            ],
        );
        let mut consumer = consumer.unwrap();
        consumer.seek("t", 0, 20).unwrap();
        consumer.config.retry_max_bytes_limit = consumer.client.fetch_max_bytes_per_partition();
        assert!(consumer.poll().unwrap().is_empty());
        assert!(consumer.poll().unwrap().is_empty());
        assert_eq!(server.join().unwrap().fetched, [20, 20]);
    }

    #[test]
    fn tcp_no_batches_preserve_the_original_buffer_growth_and_size_error_budget() {
        let (consumer, server) = mock_consumer(
            None,
            vec![
                Action::Fetch(Vec::new(), 100),
                Action::Fetch(Vec::new(), 100),
            ],
        );
        let mut consumer = consumer.unwrap();
        let initial_bytes = consumer.client.fetch_max_bytes_per_partition();
        consumer.config.retry_max_bytes_limit = initial_bytes * 2;
        assert!(consumer.poll().unwrap().is_empty());
        let tp = TopicPartition {
            topic_ref: consumer.state.topic_ref("t").unwrap(),
            partition: 0,
        };
        assert_eq!(consumer.state.fetch_offsets[&tp].offset, 0);
        assert_eq!(
            consumer.state.fetch_offsets[&tp].max_bytes,
            initial_bytes * 2
        );
        assert!(matches!(
            consumer.poll(),
            Err(Error::Kafka(KafkaCode::MessageSizeTooLarge))
        ));
        assert_eq!(consumer.state.fetch_offsets[&tp].offset, 0);
        assert_eq!(
            consumer.state.fetch_offsets[&tp].max_bytes,
            initial_bytes * 2
        );
        assert!(consumer.state.consumed_offsets.is_empty());
        assert_eq!(server.join().unwrap().fetched, [0, 0]);
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum LaterProgressFailure {
        Broker,
        UnknownTarget,
        CorruptTail,
    }

    #[derive(Clone)]
    struct ProgressScenario {
        addresses: [SocketAddr; 2],
        failure: LaterProgressFailure,
        first_has_batch: bool,
        order: Arc<std::sync::atomic::AtomicUsize>,
    }

    fn two_broker_metadata(addresses: [SocketAddr; 2]) -> MetadataResponse {
        use kafka_protocol::messages::metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition,
        };
        let mut metadata = metadata_response(addresses[0]);
        metadata.brokers.push(
            MetadataResponseBroker::default()
                .with_node_id(2.into())
                .with_host(StrBytes::from_string(addresses[1].ip().to_string()))
                .with_port(i32::from(addresses[1].port())),
        );
        metadata.topics[0].partitions.push(
            MetadataResponsePartition::default()
                .with_partition_index(1)
                .with_leader_id(2.into())
                .with_replica_nodes(vec![2.into()])
                .with_isr_nodes(vec![2.into()]),
        );
        metadata
    }

    fn progress_response(partition: i32, records: Option<Bytes>, error: i16) -> FetchResponse {
        let mut response = fetch_response(Vec::new(), 100);
        let data = &mut response.responses[0].partitions[0];
        data.partition_index = partition;
        data.records = records;
        data.error_code = error;
        response
    }

    fn serve_progress_broker(
        listener: &TcpListener,
        scenario: &ProgressScenario,
        partition: i32,
    ) -> Vec<(i64, i32)> {
        use std::sync::atomic::Ordering;
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        if partition == 0 {
            let (header, _) = read_request::<ApiVersionsRequest>(&mut stream, ApiKey::ApiVersions);
            write_response(&mut stream, &header, &ApiVersionsResponse::default());
            let (header, _) = read_request::<MetadataRequest>(&mut stream, ApiKey::Metadata);
            write_response(
                &mut stream,
                &header,
                &two_broker_metadata(scenario.addresses),
            );
        }
        let (header, request) = read_request::<FetchRequest>(&mut stream, ApiKey::Fetch);
        let requested = &request.topics[0].partitions[0];
        assert_eq!(requested.partition, partition);
        let first = scenario.order.fetch_add(1, Ordering::SeqCst) == 0;
        let mut observed = vec![(requested.fetch_offset, requested.partition_max_bytes)];
        let mut response = if first {
            progress_response(
                partition,
                scenario
                    .first_has_batch
                    .then(|| control_batch(requested.fetch_offset)),
                0,
            )
        } else {
            match scenario.failure {
                LaterProgressFailure::Broker => {
                    progress_response(partition, None, KafkaCode::NotLeaderForPartition as i16)
                }
                LaterProgressFailure::UnknownTarget => {
                    let mut response = progress_response(
                        partition,
                        Some(control_batch(requested.fetch_offset)),
                        0,
                    );
                    response.responses[0].topic = StrBytes::from_static_str("unrequested").into();
                    response
                }
                LaterProgressFailure::CorruptTail => {
                    let mut records =
                        BytesMut::from(control_batch(requested.fetch_offset).as_ref());
                    records.extend_from_slice(&[0]);
                    progress_response(partition, Some(records.freeze()), 0)
                }
            }
        };
        write_response(&mut stream, &header, &response);
        if !first && scenario.failure != LaterProgressFailure::Broker {
            let mut marker = [0];
            assert_eq!(
                stream.read(&mut marker).unwrap(),
                0,
                "malformed response connection remained reusable"
            );
            stream = listener.accept().unwrap().0;
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
        }
        let (header, request) = read_request::<FetchRequest>(&mut stream, ApiKey::Fetch);
        let requested = &request.topics[0].partitions[0];
        observed.push((requested.fetch_offset, requested.partition_max_bytes));
        response = fetch_response(vec![requested.fetch_offset], 100);
        response.responses[0].partitions[0].partition_index = partition;
        write_response(&mut stream, &header, &response);
        observed
    }

    fn initialized_progress_consumer(address: SocketAddr) -> Consumer {
        use std::collections::{HashSet, VecDeque};
        let mut client = KafkaClient::builder()
            .with_hosts(vec![address.to_string()])
            .with_conn_rw_timeout(2)
            .build();
        client.load_metadata_all().unwrap();
        let assignments = assignment::from_map(HashMap::from([("t".to_owned(), vec![0, 1])]));
        let topic_ref = assignments.topic_ref("t").unwrap();
        let fetch_offsets = [0, 1]
            .into_iter()
            .map(|partition| {
                (
                    TopicPartition {
                        topic_ref,
                        partition,
                    },
                    state::FetchState {
                        offset: 10 + i64::from(partition),
                        max_bytes: 1024,
                    },
                )
            })
            .collect();
        Consumer {
            client,
            state: state::State {
                assignments,
                fetch_offsets,
                retry_partitions: VecDeque::new(),
                consumed_offsets: HashMap::default(),
                paused: HashSet::new(),
                paused_assignments: HashSet::default(),
            },
            config: config::Config {
                group: "atomic-progress".into(),
                fallback_offset: FetchOffset::Earliest,
                retry_max_bytes_limit: 2048,
            },
        }
    }

    fn assert_later_progress_failure_is_atomic(
        failure: LaterProgressFailure,
        first_has_batch: bool,
    ) {
        let first = TcpListener::bind("127.0.0.1:0").unwrap();
        let second = TcpListener::bind("127.0.0.1:0").unwrap();
        let addresses = [first.local_addr().unwrap(), second.local_addr().unwrap()];
        let scenario = ProgressScenario {
            addresses,
            failure,
            first_has_batch,
            order: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let first_scenario = scenario.clone();
        let second_scenario = scenario;
        let first_server =
            std::thread::spawn(move || serve_progress_broker(&first, &first_scenario, 0));
        let second_server =
            std::thread::spawn(move || serve_progress_broker(&second, &second_scenario, 1));
        let mut consumer = initialized_progress_consumer(addresses[0]);
        consumer.consume_message("t", 0, 7).unwrap();
        assert!(consumer.poll().is_err());
        for (tp, state) in &consumer.state.fetch_offsets {
            assert_eq!(state.offset, 10 + i64::from(tp.partition));
            assert_eq!(state.max_bytes, 1024);
        }
        assert!(consumer.state.retry_partitions.is_empty());
        assert_eq!(consumer.last_consumed_message("t", 0), Some(7));
        assert!(
            consumer
                .state
                .consumed_offsets
                .values()
                .all(|offset| offset.dirty)
        );
        let messages = consumer.poll().unwrap();
        assert_eq!(
            messages
                .iter_ref()
                .map(|set| set.messages().len())
                .sum::<usize>(),
            2
        );
        assert_eq!(first_server.join().unwrap(), [(10, 1024), (10, 1024)]);
        assert_eq!(second_server.join().unwrap(), [(11, 1024), (11, 1024)]);
        assert_eq!(consumer.last_consumed_message("t", 0), Some(7));
    }

    #[test]
    fn tcp_later_broker_failure_preserves_all_batch_cursors_and_retry_growth() {
        for failure in [
            LaterProgressFailure::Broker,
            LaterProgressFailure::UnknownTarget,
            LaterProgressFailure::CorruptTail,
        ] {
            for first_has_batch in [false, true] {
                assert_later_progress_failure_is_atomic(failure, first_has_batch);
            }
        }
    }

    fn malformed_crc_valid_batch(defect: &str) -> Bytes {
        let offsets = if defect == "order" {
            vec![12, 11]
        } else {
            vec![11, 12]
        };
        let response = fetch_response(offsets, 100);
        let mut bytes = response.responses[0].partitions[0]
            .records
            .as_ref()
            .unwrap()
            .to_vec();
        if defect == "bounds" {
            // Real record delta 1 lies beyond the claimed last delta 0.
            bytes[23..27].copy_from_slice(&0_i32.to_be_bytes());
            set_fixture_batch_crc(&mut bytes);
        } else if defect == "count" {
            // Header claims zero records while the valid encoded body remains.
            bytes[57..61].copy_from_slice(&0_i32.to_be_bytes());
            set_fixture_batch_crc(&mut bytes);
        }
        Bytes::from(bytes)
    }

    fn serve_broker_error_then_structural_error(listener: &TcpListener, defect: &str) {
        use kafka_protocol::messages::metadata_response::MetadataResponsePartition;
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let (header, _) = read_request::<ApiVersionsRequest>(&mut stream, ApiKey::ApiVersions);
        write_response(&mut stream, &header, &ApiVersionsResponse::default());
        let (header, _) = read_request::<MetadataRequest>(&mut stream, ApiKey::Metadata);
        let mut metadata = metadata_response(listener.local_addr().unwrap());
        metadata.topics[0].partitions.push(
            MetadataResponsePartition::default()
                .with_partition_index(1)
                .with_leader_id(1.into())
                .with_replica_nodes(vec![1.into()])
                .with_isr_nodes(vec![1.into()]),
        );
        write_response(&mut stream, &header, &metadata);
        let (header, request) = read_request::<FetchRequest>(&mut stream, ApiKey::Fetch);
        let mut requested: Vec<_> = request.topics[0]
            .partitions
            .iter()
            .map(|partition| {
                (
                    partition.partition,
                    partition.fetch_offset,
                    partition.partition_max_bytes,
                )
            })
            .collect();
        requested.sort_unstable();
        assert_eq!(requested, [(0, 10, 1024), (1, 11, 1024)]);
        let mut response = progress_response(0, None, KafkaCode::NotLeaderForPartition as i16);
        let mut later = progress_response(1, Some(malformed_crc_valid_batch(defect)), 0);
        response.responses[0]
            .partitions
            .push(later.responses[0].partitions.pop().unwrap());
        write_response(&mut stream, &header, &response);
        let mut marker = [0];
        assert_eq!(
            stream.read(&mut marker).unwrap(),
            0,
            "structural error stream was not retired"
        );
        listener.set_nonblocking(true).unwrap();
        assert!(
            listener
                .accept()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock),
            "malformed response triggered a replacement retry connection"
        );
    }

    #[test]
    fn tcp_later_crc_valid_structural_errors_override_earlier_retriable_broker_errors() {
        for defect in ["bounds", "order", "count"] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                serve_broker_error_then_structural_error(&listener, defect);
            });
            let mut consumer = initialized_progress_consumer(address);
            consumer.consume_message("t", 0, 7).unwrap();
            assert!(
                matches!(
                    consumer.poll(),
                    Err(Error::Protocol(crate::error::ProtocolError::Codec))
                ),
                "retriable broker error hid structural {defect} corruption"
            );
            for (tp, state) in &consumer.state.fetch_offsets {
                assert_eq!(state.offset, 10 + i64::from(tp.partition));
                assert_eq!(state.max_bytes, 1024);
            }
            assert!(consumer.state.retry_partitions.is_empty());
            assert_eq!(consumer.last_consumed_message("t", 0), Some(7));
            assert!(
                consumer
                    .state
                    .consumed_offsets
                    .values()
                    .all(|offset| offset.dirty)
            );
            server.join().unwrap();
        }
    }

    #[test]
    fn tcp_poll_filters_a_whole_batch_and_next_request_uses_its_end() {
        let (consumer, server) = mock_consumer(
            None,
            vec![
                Action::Fetch(vec![0, 1, 2, 3, 4, 5], 6),
                Action::Fetch(Vec::new(), 6),
            ],
        );
        let mut consumer = consumer.unwrap();
        consumer.seek("t", 0, 3).unwrap();
        let sets = consumer.poll().unwrap();
        assert_eq!(
            sets.iter_ref()
                .next()
                .unwrap()
                .messages()
                .iter()
                .map(|message| message.offset)
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert!(consumer.poll().unwrap().is_empty());
        assert_eq!(server.join().unwrap().fetched, vec![3, 6]);
    }

    #[test]
    fn tcp_invalid_record_offsets_are_rejected_even_in_a_batch_prefix() {
        for invalid in [-1, i64::MIN, i64::MAX] {
            let (consumer, server) =
                mock_consumer(None, vec![Action::Fetch(vec![invalid, 11], 30)]);
            let mut consumer = consumer.unwrap();
            consumer.seek("t", 0, 10).unwrap();
            assert!(matches!(
                consumer.poll(),
                Err(Error::Protocol(crate::error::ProtocolError::Codec))
            ));
            let tp = TopicPartition {
                topic_ref: consumer.state.topic_ref("t").unwrap(),
                partition: 0,
            };
            assert_eq!(consumer.state.fetch_offsets[&tp].offset, 10);
            assert_eq!(server.join().unwrap().fetched, vec![10]);
        }
    }

    #[test]
    fn tcp_maximum_commit_is_sent_without_overflow_and_errors_keep_dirty_flags() {
        let (consumer, server) = mock_consumer(
            Some(-1),
            vec![
                Action::Commit(KafkaCode::TopicAuthorizationFailed as i16),
                Action::Commit(0),
            ],
        );
        let mut consumer = consumer.unwrap();
        consumer.consume_message("t", 0, i64::MAX - 1).unwrap();
        assert!(consumer.commit_consumed().is_err());
        assert!(
            consumer
                .state
                .consumed_offsets
                .values()
                .all(|offset| offset.dirty)
        );
        consumer.commit_consumed().unwrap();
        assert!(
            consumer
                .state
                .consumed_offsets
                .values()
                .all(|offset| !offset.dirty)
        );
        assert_eq!(server.join().unwrap().committed, vec![i64::MAX, i64::MAX]);
    }

    #[test]
    fn tcp_committed_offset_domain_preserves_unset_zero_and_maximum() {
        for (offset, consumed) in [(-1, None), (0, Some(-1)), (i64::MAX, Some(i64::MAX - 1))] {
            let (consumer, server) = mock_consumer(Some(offset), Vec::new());
            let consumer = consumer.unwrap();
            assert_eq!(consumer.last_consumed_message("t", 0), consumed);
            assert!(
                consumer
                    .state
                    .consumed_offsets
                    .values()
                    .all(|offset| !offset.dirty)
            );
            server.join().unwrap();
        }
        for offset in [-2, i64::MIN] {
            let (consumer, server) = mock_consumer(Some(offset), Vec::new());
            let Err(error) = consumer else {
                panic!("invalid committed offset must be rejected");
            };
            assert!(is_codec_error(&error));
            server.join().unwrap();
        }
    }

    fn is_codec_error(error: &Error) -> bool {
        match error {
            Error::Protocol(crate::error::ProtocolError::Codec) => true,
            Error::BrokerRequestError { source, .. } => is_codec_error(source),
            _ => false,
        }
    }

    #[test]
    fn tcp_malformed_success_ack_keeps_dirty_until_an_explicit_successful_commit() {
        for sparse in [false, true] {
            let (consumer, server) = mock_consumer(
                Some(-1),
                vec![Action::MalformedCommit(sparse), Action::Commit(0)],
            );
            let mut consumer = consumer.unwrap();
            consumer.consume_message("t", 0, 10).unwrap();
            assert!(consumer.commit_consumed().is_err());
            assert!(
                consumer
                    .state
                    .consumed_offsets
                    .values()
                    .all(|offset| offset.dirty)
            );
            assert_eq!(consumer.last_consumed_message("t", 0), Some(10));
            consumer.commit_consumed().unwrap();
            assert!(
                consumer
                    .state
                    .consumed_offsets
                    .values()
                    .all(|offset| !offset.dirty)
            );
            assert_eq!(server.join().unwrap().committed, vec![11, 11]);
        }
    }
}
