use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::client;

use super::{Partitioner, Topics};

#[derive(Default)]
struct StickyState {
    partition: Option<i32>,
    message_count: usize,
}

/// A partitioner that "sticks" to one partition per topic for a batch of messages,
/// then randomly selects a new partition for the next batch.
///
/// This reduces the number of batch requests by grouping messages
/// to the same partition, improving throughput.
///
/// Best for: high-throughput scenarios with many small messages.
pub struct StickyPartitioner {
    topics: HashMap<String, StickyState>,
    batch_size: usize,
}

impl StickyPartitioner {
    /// Create a new `StickyPartitioner` that sticks to a chosen partition for
    /// `batch_size` messages per topic before selecting a new partition.
    #[must_use]
    pub fn new(batch_size: usize) -> Self {
        Self {
            topics: HashMap::new(),
            batch_size,
        }
    }

    fn select_partition(state: &mut StickyState, available: &[i32], batch_size: usize) -> i32 {
        if state.message_count >= batch_size
            || state
                .partition
                .is_none_or(|partition| !available.contains(&partition))
        {
            let seed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let idx = usize::try_from(seed % available.len() as u128).unwrap_or_default();
            state.partition = Some(available[idx]);
            state.message_count = 0;
        }

        state.message_count += 1;
        state.partition.unwrap_or(available[0])
    }
}

impl Default for StickyPartitioner {
    fn default() -> Self {
        Self::new(64)
    }
}

impl Partitioner for StickyPartitioner {
    fn partition(&mut self, topics: Topics<'_>, rec: &mut client::ProduceMessage<'_, '_>) {
        if rec.partition >= 0 {
            return;
        }

        let Some(partitions) = topics.partitions(rec.topic) else {
            return;
        };

        let avail = partitions.available_ids();
        if avail.is_empty() {
            return;
        }

        // Partitioner receives exclusive access, so topic state needs no locks.
        rec.partition = if let Some(state) = self.topics.get_mut(rec.topic) {
            Self::select_partition(state, avail, self.batch_size)
        } else {
            let mut state = StickyState::default();
            let partition = Self::select_partition(&mut state, avail, self.batch_size);
            self.topics.insert(rec.topic.to_owned(), state);
            partition
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer::partitioner::Partitions;

    fn topic_partitions(entries: &[(&str, &[i32])]) -> HashMap<String, Partitions> {
        entries
            .iter()
            .map(|(topic, available)| {
                (
                    (*topic).to_owned(),
                    Partitions {
                        available_ids: available.to_vec(),
                        num_all_partitions: u32::try_from(available.len()).unwrap(),
                    },
                )
            })
            .collect()
    }

    fn partition_record(
        partitioner: &mut StickyPartitioner,
        topics: &HashMap<String, Partitions>,
        topic: &str,
        partition: i32,
    ) -> i32 {
        let mut record = client::ProduceMessage {
            topic,
            partition,
            key: None,
            value: None,
            headers: &[],
        };
        partitioner.partition(Topics::new(topics), &mut record);
        record.partition
    }

    #[test]
    fn sticky_partitions_and_batch_counts_are_per_topic() {
        let mut partitioner = StickyPartitioner::new(2);
        let topics = topic_partitions(&[("a", &[2]), ("b", &[0])]);

        assert_eq!(partition_record(&mut partitioner, &topics, "a", -1), 2);
        assert_eq!(partition_record(&mut partitioner, &topics, "b", -1), 0);
        assert_eq!(partition_record(&mut partitioner, &topics, "a", -1), 2);
        assert_eq!(partitioner.topics["a"].message_count, 2);
        assert_eq!(partitioner.topics["b"].message_count, 1);
        assert_eq!(partition_record(&mut partitioner, &topics, "a", -1), 2);
        assert_eq!(partitioner.topics["a"].message_count, 1);
        assert_eq!(partitioner.topics["b"].message_count, 1);
    }

    #[test]
    fn sticky_partition_remains_constant_within_batch() {
        let mut partitioner = StickyPartitioner::new(3);
        let topics = topic_partitions(&[("t", &[0, 1, 2])]);
        let selected = partition_record(&mut partitioner, &topics, "t", -1);

        assert_eq!(
            partition_record(&mut partitioner, &topics, "t", -1),
            selected
        );
        assert_eq!(
            partition_record(&mut partitioner, &topics, "t", -1),
            selected
        );
    }

    #[test]
    fn unavailable_sticky_partition_is_reselected_before_batch_ends() {
        let mut partitioner = StickyPartitioner::new(10);
        let initial = topic_partitions(&[("t", &[2])]);
        assert_eq!(partition_record(&mut partitioner, &initial, "t", -1), 2);

        let updated = topic_partitions(&[("t", &[0])]);
        assert_eq!(partition_record(&mut partitioner, &updated, "t", -1), 0);
        assert_eq!(partitioner.topics["t"].message_count, 1);
    }

    #[test]
    fn explicit_partition_and_missing_metadata_do_not_change_sticky_state() {
        let mut partitioner = StickyPartitioner::default();
        let topics = topic_partitions(&[("t", &[0]), ("empty", &[])]);

        assert_eq!(partition_record(&mut partitioner, &topics, "t", 7), 7);
        assert_eq!(
            partition_record(&mut partitioner, &topics, "unknown", -1),
            -1
        );
        assert_eq!(partition_record(&mut partitioner, &topics, "empty", -1), -1);
        assert!(partitioner.topics.is_empty());
    }

    #[test]
    fn zero_batch_size_selects_a_valid_partition_on_every_record() {
        let mut partitioner = StickyPartitioner::new(0);
        let topics = topic_partitions(&[("t", &[3])]);

        for _ in 0..3 {
            assert_eq!(partition_record(&mut partitioner, &topics, "t", -1), 3);
            assert_eq!(partitioner.topics["t"].message_count, 1);
        }
    }
}
