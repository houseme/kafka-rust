use std::sync::Mutex;

use crate::client;

use super::{Partitioner, Topics};

/// A partitioner that "sticks" to one partition for a batch of messages,
/// then randomly selects a new partition for the next batch.
///
/// This reduces the number of batch requests by grouping messages
/// to the same partition, improving throughput.
///
/// Best for: high-throughput scenarios with many small messages.
pub struct StickyPartitioner {
    current_partition: Mutex<Option<i32>>,
    batch_count: Mutex<usize>,
    batch_size: usize,
}

impl StickyPartitioner {
    pub fn new(batch_size: usize) -> Self {
        Self {
            current_partition: Mutex::new(None),
            batch_count: Mutex::new(0),
            batch_size,
        }
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

        let partitions = match topics.partitions(rec.topic) {
            None => return,
            Some(partitions) => partitions,
        };

        let avail = partitions.available_ids();
        if avail.is_empty() {
            return;
        }

        let num_avail = avail.len();

        let mut count = self.batch_count.lock().unwrap();
        let mut current = self.current_partition.lock().unwrap();

        if current.is_none() || *count >= self.batch_size {
            use std::time::{SystemTime, UNIX_EPOCH};
            let seed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as usize;
            let idx = seed % num_avail;
            *current = Some(avail[idx]);
            *count = 0;
        }

        *count += 1;
        rec.partition = current.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sticky_default() {
        let _ = StickyPartitioner::default();
    }

    #[test]
    fn test_sticky_new() {
        let p = StickyPartitioner::new(10);
        assert_eq!(p.batch_size, 10);
    }
}
