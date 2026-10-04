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
        self.process_fetch_responses(n, retry_partition, resps)
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
    /// Returns an error if the topic or partition is not being consumed.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let topic_ref = self.state.topic_ref(topic);
        match topic_ref {
            Some(topic_ref) => {
                let tp = TopicPartition {
                    topic_ref,
                    partition,
                };
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
        Result<Vec<fetch_kp::OwnedFetchResponse>>,
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
            let result = self.client.fetch_messages_kp(std::iter::once(
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
            let reqs: Vec<FetchPartition<'_>> = state.fetch_requests().collect();
            if reqs.is_empty() {
                return (0, None, Ok(Vec::new()));
            }
            #[allow(clippy::cast_possible_truncation)] // partition count won't exceed u32
            let num_partitions = reqs.len() as u32;
            (num_partitions, None, client.fetch_messages_kp(reqs.iter()))
        }
    }

    fn process_fetch_responses(
        &mut self,
        num_partitions_queried: u32,
        retry_partition: Option<TopicPartition>,
        resps: Vec<fetch_kp::OwnedFetchResponse>,
    ) -> Result<MessageSets> {
        let single_partition_consumer = self.single_partition_consumer();
        let mut empty = true;
        let mut fetch_updates: HashMap<TopicPartition, state::FetchState, state::PartitionHasher> =
            HashMap::default();
        let mut retry_updates = Vec::new();

        for resp in &resps {
            for t in &resp.topics {
                let topic_ref = self
                    .state
                    .assignments
                    .topic_ref(&t.topic)
                    .ok_or_else(Error::codec)?;

                for p in &t.partitions {
                    let tp = TopicPartition {
                        topic_ref,
                        partition: p.partition,
                    };
                    let current = self.state.fetch_offsets.get(&tp).ok_or_else(Error::codec)?;

                    let Some(data) =
                        self.stage_partition_data(&t.topic, &tp, p, &mut fetch_updates)?
                    else {
                        continue;
                    };

                    let fetch_state =
                        fetch_updates
                            .entry(tp)
                            .or_insert_with(|| state::FetchState {
                                offset: current.offset,
                                max_bytes: current.max_bytes,
                            });
                    if let Some(last_msg) = data.messages.last() {
                        fetch_state.offset = last_msg.offset + 1;
                        empty = false;

                        if fetch_state.max_bytes != self.client.fetch_max_bytes_per_partition() {
                            let prev_max_bytes = fetch_state.max_bytes;
                            fetch_state.max_bytes = self.client.fetch_max_bytes_per_partition();
                            debug!(
                                "reset max_bytes for {}:{} from {} to {}",
                                &t.topic, p.partition, prev_max_bytes, fetch_state.max_bytes
                            );
                        }
                    } else {
                        debug!(
                            "no data received for {}:{} (max_bytes: {} / fetch_offset: {} / \
                                highwatermark_offset: {})",
                            &t.topic,
                            p.partition,
                            fetch_state.max_bytes,
                            fetch_state.offset,
                            data.highwatermark_offset
                        );

                        if fetch_state.offset < data.highwatermark_offset {
                            if fetch_state.max_bytes < self.config.retry_max_bytes_limit {
                                let prev_max_bytes = fetch_state.max_bytes;
                                fetch_state.max_bytes = prev_max_bytes
                                    .saturating_mul(2)
                                    .min(self.config.retry_max_bytes_limit);
                                debug!(
                                    "increased max_bytes for {}:{} from {} to {}",
                                    &t.topic, p.partition, prev_max_bytes, fetch_state.max_bytes
                                );
                            } else if num_partitions_queried == 1 {
                                return Err(Error::Kafka(KafkaCode::MessageSizeTooLarge));
                            }
                            if !single_partition_consumer {
                                debug!("rescheduled for retry: {}:{}", &t.topic, p.partition);
                                retry_updates.push(TopicPartition {
                                    topic_ref,
                                    partition: p.partition,
                                });
                            }
                        }
                    }
                }
            }
        }

        // Publish progress only after every partition has been validated. Failed polls
        // must leave messages available and retain any outstanding retry obligation.
        let completed_retry = retry_partition.filter(|tp| fetch_updates.contains_key(tp));
        self.state.fetch_offsets.extend(fetch_updates);
        if let Some(tp) = completed_retry {
            self.state.complete_retry_partition(&tp);
        }
        self.state.retry_partitions.extend(retry_updates);

        Ok(MessageSets {
            responses: resps,
            empty,
        })
    }

    fn stage_partition_data<'a>(
        &self,
        topic: &str,
        tp: &TopicPartition,
        partition: &'a fetch_kp::OwnedPartition,
        fetch_updates: &mut HashMap<TopicPartition, state::FetchState, state::PartitionHasher>,
    ) -> Result<Option<&'a fetch_kp::OwnedData>> {
        match partition.data() {
            Ok(data) => Ok(Some(data)),
            Err(error) => {
                if let Error::TopicPartitionError {
                    error_code: KafkaCode::OffsetOutOfRange,
                    ..
                } = error.as_ref()
                {
                    if let Some(current) = self.state.fetch_offsets.get(tp) {
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
                    }
                    Ok(None)
                } else {
                    Err(Error::from(Arc::clone(error)))
                }
            }
        }
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
    /// Returns an error if the topic is not being consumed.
    pub fn consume_message(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let topic_ref = self
            .state
            .topic_ref(topic)
            .ok_or(Error::Kafka(KafkaCode::UnknownTopicOrPartition))?;

        let tp = TopicPartition {
            topic_ref,
            partition,
        };
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
                    CommitOffset::new(topic, tp.partition, o.offset + 1)
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
    responses: slice::Iter<'a, fetch_kp::OwnedFetchResponse>,
    topics: Option<slice::Iter<'a, fetch_kp::OwnedTopic>>,
    curr_topic: Option<&'a str>,
    partitions: Option<slice::Iter<'a, fetch_kp::OwnedPartition>>,
}

impl<'a> MessageSetsIter<'a> {
    fn new(responses: &'a [fetch_kp::OwnedFetchResponse]) -> Self {
        Self {
            responses: responses.iter(),
            topics: None,
            curr_topic: None,
            partitions: None,
        }
    }
}

impl Iterator for MessageSetsIter<'_> {
    type Item = MessageSet;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try the next partition in the current topic
            if let Some(p) = self.partitions.as_mut().and_then(Iterator::next) {
                if let Ok(data) = p.data()
                    && !data.messages.is_empty()
                {
                    let topic = self.curr_topic.unwrap_or("").to_owned();
                    return Some(MessageSet {
                        topic,
                        partition: p.partition,
                        messages: data.messages.clone(),
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
}
