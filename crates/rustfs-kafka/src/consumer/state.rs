use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::BuildHasherDefault;

use crate::client::metadata::Topics;
use crate::client::{FetchGroupOffset, FetchOffset, FetchPartition, KafkaClient};
use crate::error::{Error, KafkaCode, Result};
use fnv::FnvHasher;
use tracing::debug;

use super::assignment::{Assignment, AssignmentRef, Assignments};
use super::config::Config;

pub type PartitionHasher = BuildHasherDefault<FnvHasher>;

// The "fetch state" for a particular topic partition.
#[derive(Debug)]
pub struct FetchState {
    /// ~ specifies the offset which to fetch from
    pub offset: i64,
    /// ~ specifies the `max_bytes` to be fetched
    pub max_bytes: i32,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    /// ~ indirect reference to the topic through config.topic(..)
    pub topic_ref: AssignmentRef,
    /// ~ the partition to retry
    pub partition: i32,
}

#[derive(Debug)]
pub struct ConsumedOffset {
    /// ~ the consumed offset
    pub offset: i64,
    /// ~ true if the consumed offset is changed but not committed to
    /// kafka yet
    pub dirty: bool,
}

pub struct State {
    /// Contains the topic partitions the consumer is assigned to
    /// consume; this is a _read-only_ data structure
    pub assignments: Assignments,

    /// Contains the information relevant for the next fetch operation
    /// on the corresponding partitions
    pub fetch_offsets: HashMap<TopicPartition, FetchState, PartitionHasher>,

    /// Specifies partitions to be fetched on their own in the next
    /// poll request.
    pub retry_partitions: VecDeque<TopicPartition>,

    /// Contains the offsets of messages marked as "consumed" (to be
    /// committed)
    pub consumed_offsets: HashMap<TopicPartition, ConsumedOffset, PartitionHasher>,

    /// Set of (topic, partition) pairs that are currently paused.
    pub paused: HashSet<(String, i32)>,

    /// Internal assignment references for allocation-free pause checks while fetching.
    pub paused_assignments: HashSet<TopicPartition, PartitionHasher>,
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "State {{ assignments: {:?}, fetch_offsets: {:?}, retry_partitions: {:?}, \
                consumed_offsets: {:?} }}",
            self.assignments,
            self.fetch_offsets_debug(),
            TopicPartitionsDebug {
                state: self,
                tps: &self.retry_partitions,
            },
            self.consumed_offsets_debug()
        )
    }
}

impl State {
    pub fn new(
        client: &mut KafkaClient,
        config: &Config,
        assignments: Assignments,
    ) -> Result<State> {
        let (consumed_offsets, fetch_offsets) = {
            let subscriptions = {
                let xs = assignments.as_slice();
                let mut subs = Vec::with_capacity(xs.len());
                for x in xs {
                    subs.push(determine_partitions(x, &client.topics())?);
                }
                subs
            };
            let n = subscriptions.iter().map(|s| s.partitions.len()).sum();
            let mut consumed =
                load_consumed_offsets(client, &config.group, &assignments, &subscriptions, n)?;

            let fetch_next =
                load_fetch_states(client, config, &assignments, &subscriptions, &consumed, n)?;
            // A fallback can move the starting cursor away from the stored commit.
            // Reset its consumed marker only after every offset lookup succeeds.
            consumed.retain(|partition, offset| {
                fetch_next
                    .get(partition)
                    .is_some_and(|fetch| offset.offset.checked_add(1) == Some(fetch.offset))
            });
            (consumed, fetch_next)
        };
        Ok(State {
            assignments,
            fetch_offsets,
            retry_partitions: VecDeque::new(),
            consumed_offsets,
            paused: HashSet::new(),
            paused_assignments: HashSet::default(),
        })
    }

    pub fn topic_name(&self, assignment: AssignmentRef) -> &str {
        self.assignments[assignment].topic()
    }

    pub fn topic_ref(&self, name: &str) -> Option<AssignmentRef> {
        self.assignments.topic_ref(name)
    }

    pub fn fetch_requests(&self) -> impl Iterator<Item = FetchPartition<'_>> {
        self.fetch_offsets
            .iter()
            .filter(|(tp, _)| !self.paused_assignments.contains(*tp))
            .map(|(tp, state)| {
                FetchPartition::new(self.topic_name(tp.topic_ref), tp.partition, state.offset)
                    .with_max_bytes(state.max_bytes)
            })
    }

    pub fn next_retry_partition(&self) -> Option<&TopicPartition> {
        self.retry_partitions
            .iter()
            .find(|tp| !self.paused_assignments.contains(*tp))
    }

    pub fn complete_retry_partition(&mut self, partition: &TopicPartition) {
        if let Some(index) = self.retry_partitions.iter().position(|tp| tp == partition) {
            self.retry_partitions.remove(index);
        }
    }

    /// Returns a wrapper around `self.fetch_offsets` for nice dumping
    /// in debug messages
    pub fn fetch_offsets_debug(&self) -> OffsetsMapDebug<'_, FetchState> {
        OffsetsMapDebug {
            state: self,
            offsets: &self.fetch_offsets,
        }
    }

    pub fn consumed_offsets_debug(&self) -> OffsetsMapDebug<'_, ConsumedOffset> {
        OffsetsMapDebug {
            state: self,
            offsets: &self.consumed_offsets,
        }
    }
}

// Specifies the actual partitions of a topic to be consumed
struct Subscription<'a> {
    assignment: &'a Assignment, // the assignment - user configuration
    partitions: Vec<i32>,       // the actual partitions to be consumed
}

/// Determines the partitions to be consumed according to the
/// specified topic and requested partitions configuration. Returns an
/// ordered list of the partition ids to consume.
fn determine_partitions<'a>(
    assignment: &'a Assignment,
    metadata: &Topics<'_>,
) -> Result<Subscription<'a>> {
    let topic = assignment.topic();
    let req_partitions = assignment.partitions();

    let avail_partitions = metadata.partitions(topic).ok_or_else(|| {
        debug!(
            "determine_partitions: no such topic: {} (all metadata: {:?})",
            topic, metadata
        );
        Error::Kafka(KafkaCode::UnknownTopicOrPartition)
    })?;

    let ps = if req_partitions.is_empty() {
        // ~ no partitions configured ... use all available
        let mut ps: Vec<i32> = Vec::with_capacity(avail_partitions.len());

        for partition in &avail_partitions {
            if !partition.is_available() {
                return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
            }
            ps.push(partition.id());
        }
        ps
    } else {
        // ~ validate that all partitions we're going to consume are
        // available
        let mut ps: Vec<i32> = Vec::with_capacity(req_partitions.len());
        for &p in req_partitions {
            match avail_partitions.partition(p) {
                None => {
                    debug!(
                        "determine_partitions: no such partition: \"{}:{}\" \
                            (all metadata: {:?})",
                        topic, p, metadata
                    );
                    return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
                }
                Some(partition) if partition.is_available() => ps.push(p),
                Some(_) => return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)),
            }
        }
        ps
    };
    Ok(Subscription {
        assignment,
        partitions: ps,
    })
}

// Fetches the so-far commited/consumed offsets for the configured
// group/topic/partitions.
fn load_consumed_offsets(
    client: &mut KafkaClient,
    group: &str,
    assignments: &Assignments,
    subscriptions: &[Subscription<'_>],
    result_capacity: usize,
) -> Result<HashMap<TopicPartition, ConsumedOffset, PartitionHasher>> {
    assert!(!subscriptions.is_empty());
    // ~ pre-allocate the right size
    let mut offs = HashMap::with_capacity_and_hasher(result_capacity, PartitionHasher::default());
    // ~ no group, no persisted consumed offsets
    if group.is_empty() {
        return Ok(offs);
    }
    // ~ otherwise try load them for the group
    let topic_offsets = client.fetch_group_offsets(
        group,
        subscriptions.iter().flat_map(|s| {
            let topic = s.assignment.topic();
            s.partitions
                .iter()
                .map(move |&p| FetchGroupOffset::new(topic, p))
        }),
    )?;
    for (topic, pos) in topic_offsets {
        for po in pos {
            if let Some(offset) = consumed_offset_from_committed(po.offset)? {
                offs.insert(
                    TopicPartition {
                        topic_ref: assignments.topic_ref(&topic).expect("non-assigned topic"),
                        partition: po.partition,
                    },
                    // the committed offset is the next message to be fetched, so
                    // the last consumed message is that - 1
                    ConsumedOffset {
                        offset,
                        dirty: false,
                    },
                );
            }
        }
    }

    debug!("load_consumed_offsets: constructed consumed: {:#?}", offs);

    Ok(offs)
}

fn consumed_offset_from_committed(committed: i64) -> Result<Option<i64>> {
    if committed == -1 {
        return Ok(None);
    }
    if committed < 0 {
        return Err(Error::codec());
    }
    Ok(Some(committed.checked_sub(1).ok_or_else(Error::codec)?))
}

/// Fetches the "next fetch" offsets/states based on the specified
/// subscriptions and the given consumed offsets.
fn load_fetch_states(
    client: &mut KafkaClient,
    config: &Config,
    assignments: &Assignments,
    subscriptions: &[Subscription<'_>],
    consumed_offsets: &HashMap<TopicPartition, ConsumedOffset, PartitionHasher>,
    result_capacity: usize,
) -> Result<HashMap<TopicPartition, FetchState, PartitionHasher>> {
    let max_bytes = client.fetch_max_bytes_per_partition();
    let subscription_topics: Vec<_> = subscriptions.iter().map(|s| s.assignment.topic()).collect();
    if consumed_offsets.is_empty() {
        let offsets = load_partition_offsets(client, &subscription_topics, config.fallback_offset)?;
        return fallback_fetch_states(
            assignments,
            subscriptions,
            &offsets,
            max_bytes,
            result_capacity,
        );
    }

    let latest = load_partition_offsets(client, &subscription_topics, FetchOffset::Latest)?;
    let earliest = load_partition_offsets(client, &subscription_topics, FetchOffset::Earliest)?;
    let mut by_time = None;
    let mut fetch_offsets =
        HashMap::with_capacity_and_hasher(result_capacity, PartitionHasher::default());
    for subscription in subscriptions {
        let topic = subscription.assignment.topic();
        let topic_ref = assignments.topic_ref(topic).ok_or_else(Error::codec)?;
        for &partition in &subscription.partitions {
            let tp = TopicPartition {
                topic_ref,
                partition,
            };
            let committed = match consumed_offsets.get(&tp) {
                Some(consumed) => next_fetch_offset(
                    consumed.offset,
                    concrete_partition_offset(&earliest, topic, partition)?,
                    concrete_partition_offset(&latest, topic, partition)?,
                ),
                None => None,
            };
            let offset = match committed {
                Some(offset) => offset,
                None => match config.fallback_offset {
                    FetchOffset::Latest => concrete_partition_offset(&latest, topic, partition)?,
                    FetchOffset::Earliest => {
                        concrete_partition_offset(&earliest, topic, partition)?
                    }
                    FetchOffset::ByTime(_) => {
                        if by_time.is_none() {
                            by_time = Some(load_partition_offsets(
                                client,
                                &subscription_topics,
                                config.fallback_offset,
                            )?);
                        }
                        concrete_partition_offset(
                            by_time.as_ref().ok_or_else(Error::codec)?,
                            topic,
                            partition,
                        )?
                    }
                },
            };
            fetch_offsets.insert(tp, FetchState { offset, max_bytes });
        }
    }
    Ok(fetch_offsets)
}

type PartitionOffsets = HashMap<String, HashMap<i32, i64, PartitionHasher>>;

fn load_partition_offsets(
    client: &mut KafkaClient,
    topics: &[&str],
    offset: FetchOffset,
) -> Result<PartitionOffsets> {
    let offsets = client.fetch_offsets(topics, offset)?;
    let mut result = HashMap::with_capacity(offsets.len());
    for (topic, partitions) in offsets {
        let mut indexed =
            HashMap::with_capacity_and_hasher(partitions.len(), PartitionHasher::default());
        for partition in partitions {
            if partition.offset < -1
                || indexed
                    .insert(partition.partition, partition.offset)
                    .is_some()
            {
                return Err(Error::codec());
            }
        }
        result.insert(topic, indexed);
    }
    Ok(result)
}

fn concrete_partition_offset(
    offsets: &PartitionOffsets,
    topic: &str,
    partition: i32,
) -> Result<i64> {
    let offset = *offsets
        .get(topic)
        .and_then(|partitions| partitions.get(&partition))
        .ok_or(Error::Kafka(KafkaCode::UnknownTopicOrPartition))?;
    match offset {
        -1 => Err(Error::Kafka(KafkaCode::OffsetOutOfRange)),
        offset if offset < -1 => Err(Error::codec()),
        offset => Ok(offset),
    }
}

fn fallback_fetch_states(
    assignments: &Assignments,
    subscriptions: &[Subscription<'_>],
    offsets: &PartitionOffsets,
    max_bytes: i32,
    capacity: usize,
) -> Result<HashMap<TopicPartition, FetchState, PartitionHasher>> {
    let mut result = HashMap::with_capacity_and_hasher(capacity, PartitionHasher::default());
    for subscription in subscriptions {
        let topic = subscription.assignment.topic();
        let topic_ref = assignments.topic_ref(topic).ok_or_else(Error::codec)?;
        for &partition in &subscription.partitions {
            let offset = concrete_partition_offset(offsets, topic, partition)?;
            result.insert(
                TopicPartition {
                    topic_ref,
                    partition,
                },
                FetchState { offset, max_bytes },
            );
        }
    }
    Ok(result)
}

fn next_fetch_offset(consumed_offset: i64, earliest: i64, latest: i64) -> Option<i64> {
    // Kafka stores the next offset to fetch; the consumer tracks the previous one.
    consumed_offset
        .checked_add(1)
        .filter(|&offset| offset >= earliest && offset <= latest)
}

pub struct OffsetsMapDebug<'a, T> {
    state: &'a State,
    offsets: &'a HashMap<TopicPartition, T, PartitionHasher>,
}

impl<'a, T: fmt::Debug + 'a> fmt::Debug for OffsetsMapDebug<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        for (i, (tp, v)) in self.offsets.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            let topic = self.state.topic_name(tp.topic_ref);
            write!(f, "\"{}:{}\": {:?}", topic, tp.partition, v)?;
        }
        write!(f, "}}")
    }
}

struct TopicPartitionsDebug<'a> {
    state: &'a State,
    tps: &'a VecDeque<TopicPartition>,
}

impl fmt::Debug for TopicPartitionsDebug<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, tp) in self.tps.iter().enumerate() {
            if i != 0 {
                write!(f, " ,")?;
            }
            write!(
                f,
                "\"{}:{}\"",
                self.state.topic_name(tp.topic_ref),
                tp.partition
            )?;
        }
        write!(f, "]")
    }
}

#[cfg(test)]
mod offset_tests {
    use super::{consumed_offset_from_committed, next_fetch_offset};
    use crate::error::{Error, ProtocolError};

    #[test]
    fn committed_offsets_preserve_unset_zero_and_maximum_boundaries() {
        assert_eq!(consumed_offset_from_committed(-1).unwrap(), None);
        assert_eq!(consumed_offset_from_committed(0).unwrap(), Some(-1));
        assert_eq!(consumed_offset_from_committed(1).unwrap(), Some(0));
        assert_eq!(
            consumed_offset_from_committed(i64::MAX).unwrap(),
            Some(i64::MAX - 1)
        );
    }

    #[test]
    fn malformed_committed_offsets_return_codec_errors() {
        for offset in [-2, i64::MIN] {
            assert!(matches!(
                consumed_offset_from_committed(offset),
                Err(Error::Protocol(ProtocolError::Codec))
            ));
        }
    }

    #[test]
    fn committed_offset_at_earliest_is_valid() {
        assert_eq!(next_fetch_offset(-1, 0, 10), Some(0));
        assert_eq!(next_fetch_offset(99, 100, 110), Some(100));
    }

    #[test]
    fn committed_offset_at_latest_is_valid_including_empty_log() {
        assert_eq!(next_fetch_offset(109, 100, 110), Some(110));
        assert_eq!(next_fetch_offset(99, 100, 100), Some(100));
    }

    #[test]
    fn committed_offsets_outside_log_range_require_fallback() {
        assert_eq!(next_fetch_offset(98, 100, 110), None);
        assert_eq!(next_fetch_offset(110, 100, 110), None);
    }

    #[test]
    fn committed_offset_overflow_does_not_panic() {
        assert_eq!(next_fetch_offset(i64::MAX, 0, i64::MAX), None);
        assert_eq!(next_fetch_offset(i64::MAX - 1, 0, i64::MAX), Some(i64::MAX));
    }
}

#[cfg(test)]
#[path = "offset_initialization_tests.rs"]
mod initialization_tests;
