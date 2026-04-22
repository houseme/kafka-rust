use crate::client;
use std::time::{SystemTime, UNIX_EPOCH};

use super::{Partitioner, Topics};

/// A partitioner that selects partitions uniformly at random.
///
/// Simplest strategy for even distribution without ordering guarantees.
///
/// Best for: messages without keys needing simple random distribution.
pub struct UniformPartitioner;

impl Default for UniformPartitioner {
    fn default() -> Self {
        Self
    }
}

impl UniformPartitioner {
    /// Create a new `UniformPartitioner` instance.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Partitioner for UniformPartitioner {
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

        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as usize;
        let idx = seed % avail.len();
        rec.partition = avail[idx];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniform_default() {
        let _ = UniformPartitioner;
    }

    #[test]
    fn test_uniform_new() {
        let _ = UniformPartitioner::new();
    }
}
