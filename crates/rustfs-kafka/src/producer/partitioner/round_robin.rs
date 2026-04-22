use std::sync::atomic::{AtomicUsize, Ordering};

use super::{Partitioner, Topics};
use crate::client;

/// A partitioner that cycles through partitions in order.
///
/// Best for: messages without keys that need even distribution.
pub struct RoundRobinPartitioner {
    counter: AtomicUsize,
}

impl RoundRobinPartitioner {
    /// Create a new `RoundRobinPartitioner` with an initial counter of zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobinPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner for RoundRobinPartitioner {
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

        let idx = self.counter.fetch_add(1, Ordering::Relaxed);
        rec.partition = avail[idx % avail.len()];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin_counter_increments() {
        let p = RoundRobinPartitioner::new();
        let v1 = p.counter.load(Ordering::Relaxed);
        let _ = p.counter.fetch_add(1, Ordering::Relaxed);
        let v2 = p.counter.load(Ordering::Relaxed);
        assert!(v2 > v1);
    }

    #[test]
    fn test_round_robin_default() {
        let _ = RoundRobinPartitioner::default();
    }
}
