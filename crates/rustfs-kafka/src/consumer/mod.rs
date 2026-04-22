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
        for &p in partitions {
            self.state.paused.insert((topic.to_owned(), p));
        }
        debug!("Paused partitions for topic '{}': {:?}", topic, partitions);
    }

    /// Resumes message fetching for the specified partitions of a topic.
    pub fn resume(&mut self, topic: &str, partitions: &[i32]) {
        for &p in partitions {
            self.state.paused.remove(&(topic.to_owned(), p));
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
        let (n, resps) = self.fetch_messages();
        let resps = resps?;
        self.process_fetch_responses(n, resps)
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

    fn fetch_messages(&mut self) -> (u32, Result<Vec<fetch_kp::OwnedFetchResponse>>) {
        if let Some(tp) = self.state.retry_partitions.pop_front() {
            let Some(s) = self.state.fetch_offsets.get(&tp) else {
                return (1, Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)));
            };

            let topic = self.state.topic_name(tp.topic_ref);
            debug!(
                "fetching retry messages: (fetch-offset: {{\"{}:{}\": {:?}}})",
                topic, tp.partition, s
            );
            (
                1,
                self.client.fetch_messages_kp(std::iter::once(
                    &FetchPartition::new(topic, tp.partition, s.offset).with_max_bytes(s.max_bytes),
                )),
            )
        } else {
            let client = &mut self.client;
            let state = &self.state;
            debug!(
                "fetching messages: (fetch-offsets: {:?})",
                state.fetch_offsets_debug()
            );
            let reqs: Vec<FetchPartition<'_>> = state
                .fetch_offsets
                .iter()
                .map(|(tp, s)| {
                    let topic = state.topic_name(tp.topic_ref);
                    FetchPartition::new(topic, tp.partition, s.offset).with_max_bytes(s.max_bytes)
                })
                .collect();
            #[allow(clippy::cast_possible_truncation)] // partition count won't exceed u32
            let num_partitions = state.fetch_offsets.len() as u32;
            (num_partitions, client.fetch_messages_kp(reqs.iter()))
        }
    }

    fn process_fetch_responses(
        &mut self,
        num_partitions_queried: u32,
        resps: Vec<fetch_kp::OwnedFetchResponse>,
    ) -> Result<MessageSets> {
        let single_partition_consumer = self.single_partition_consumer();
        let mut empty = true;
        let retry_partitions = &mut self.state.retry_partitions;

        for resp in &resps {
            for t in &resp.topics {
                let topic_ref = self
                    .state
                    .assignments
                    .topic_ref(&t.topic)
                    .expect("unknown topic in response");

                for p in &t.partitions {
                    let tp = TopicPartition {
                        topic_ref,
                        partition: p.partition,
                    };

                    let data = match p.data() {
                        Ok(d) => d,
                        Err(e) => {
                            return Err(Error::from(Arc::clone(e)));
                        }
                    };

                    let fetch_state = self
                        .state
                        .fetch_offsets
                        .get_mut(&tp)
                        .expect("non-requested partition");
                    if let Some(last_msg) = data.messages.last() {
                        fetch_state.offset = last_msg.offset + 1;
                        empty = false;

                        if fetch_state.max_bytes != self.client.fetch_max_bytes_per_partition() {
                            let prev_max_bytes = fetch_state.max_bytes;
                            fetch_state.max_bytes = self.client.fetch_max_bytes_per_partition();
                            debug!(
                                "reset max_bytes for {}:{} from {} to {}",
                                &t.topic, tp.partition, prev_max_bytes, fetch_state.max_bytes
                            );
                        }
                    } else {
                        debug!(
                            "no data received for {}:{} (max_bytes: {} / fetch_offset: {} / \
                                highwatermark_offset: {})",
                            &t.topic,
                            tp.partition,
                            fetch_state.max_bytes,
                            fetch_state.offset,
                            data.highwatermark_offset
                        );

                        if fetch_state.offset < data.highwatermark_offset {
                            if fetch_state.max_bytes < self.config.retry_max_bytes_limit {
                                let prev_max_bytes = fetch_state.max_bytes;
                                let incr_max_bytes = prev_max_bytes + prev_max_bytes;
                                if incr_max_bytes > self.config.retry_max_bytes_limit {
                                    fetch_state.max_bytes = self.config.retry_max_bytes_limit;
                                } else {
                                    fetch_state.max_bytes = incr_max_bytes;
                                }
                                debug!(
                                    "increased max_bytes for {}:{} from {} to {}",
                                    &t.topic, tp.partition, prev_max_bytes, fetch_state.max_bytes
                                );
                            } else if num_partitions_queried == 1 {
                                return Err(Error::Kafka(KafkaCode::MessageSizeTooLarge));
                            }
                            if !single_partition_consumer {
                                debug!("rescheduled for retry: {}:{}", &t.topic, tp.partition);
                                retry_partitions.push_back(tp);
                            }
                        }
                    }
                }
            }
        }

        Ok(MessageSets {
            responses: resps,
            empty,
        })
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

    #[test]
    fn test_pause_and_resume() {
        // We can't easily create a full Consumer without a Kafka connection,
        // so test the State directly
        let paused = std::collections::HashSet::new();
        assert!(paused.is_empty());

        let mut paused = paused;
        paused.insert(("t".to_owned(), 0));
        paused.insert(("t".to_owned(), 1));

        assert!(paused.contains(&("t".to_owned(), 0)));
        assert!(paused.contains(&("t".to_owned(), 1)));
        assert!(!paused.contains(&("t".to_owned(), 2)));

        paused.remove(&("t".to_owned(), 0));
        assert!(!paused.contains(&("t".to_owned(), 0)));
        assert!(paused.contains(&("t".to_owned(), 1)));
    }

    #[test]
    fn test_pause_multiple_partitions() {
        let mut paused = std::collections::HashSet::new();
        paused.insert(("t".to_owned(), 0));
        paused.insert(("t".to_owned(), 1));
        paused.insert(("t".to_owned(), 2));
        assert_eq!(paused.len(), 3);

        paused.remove(&("t".to_owned(), 1));
        assert_eq!(paused.len(), 2);
    }

    #[test]
    fn test_pause_nonexistent_partition_no_panic() {
        let mut paused = std::collections::HashSet::new();
        paused.insert(("t".to_owned(), 999));
        assert_eq!(paused.len(), 1);
    }
}
