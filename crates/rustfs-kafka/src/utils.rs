//! Some utility structures
//!
//! This module is _not_ exposed to the public directly.

/// A retrieved offset for a particular partition in the context of an
/// already known topic.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct PartitionOffset {
    /// The offset value for the partition.
    pub offset: i64,
    /// The partition identifier.
    pub partition: i32,
}

/// A retrieved offset for a particular partition in the context of an
/// already known topic, specific to a timestamp.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct TimestampedPartitionOffset {
    /// The offset value for the partition.
    pub offset: i64,
    /// The partition identifier.
    pub partition: i32,
    /// The timestamp (ms since epoch) associated with the offset.
    pub time: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_offset_construction() {
        let po = PartitionOffset {
            offset: 42,
            partition: 3,
        };
        assert_eq!(po.offset, 42);
        assert_eq!(po.partition, 3);
    }

    #[test]
    fn test_partition_offset_zero_values() {
        let po = PartitionOffset {
            offset: 0,
            partition: 0,
        };
        assert_eq!(po.offset, 0);
        assert_eq!(po.partition, 0);
    }

    #[test]
    fn test_partition_offset_negative_values() {
        let po = PartitionOffset {
            offset: -1,
            partition: -5,
        };
        assert_eq!(po.offset, -1);
        assert_eq!(po.partition, -5);
    }

    #[test]
    fn test_partition_offset_equality() {
        let a = PartitionOffset {
            offset: 100,
            partition: 2,
        };
        let b = PartitionOffset {
            offset: 100,
            partition: 2,
        };
        let c = PartitionOffset {
            offset: 100,
            partition: 3,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_timestamped_partition_offset_construction() {
        let tpo = TimestampedPartitionOffset {
            offset: 99,
            partition: 1,
            time: 1_700_000_000_000,
        };
        assert_eq!(tpo.offset, 99);
        assert_eq!(tpo.partition, 1);
        assert_eq!(tpo.time, 1_700_000_000_000);
    }

    #[test]
    fn test_timestamped_partition_offset_zero_values() {
        let tpo = TimestampedPartitionOffset {
            offset: 0,
            partition: 0,
            time: 0,
        };
        assert_eq!(tpo.offset, 0);
        assert_eq!(tpo.partition, 0);
        assert_eq!(tpo.time, 0);
    }

    #[test]
    fn test_timestamped_partition_offset_negative_time() {
        let tpo = TimestampedPartitionOffset {
            offset: -10,
            partition: 0,
            time: -1,
        };
        assert_eq!(tpo.offset, -10);
        assert_eq!(tpo.time, -1);
    }

    #[test]
    fn test_timestamped_partition_offset_equality() {
        let a = TimestampedPartitionOffset {
            offset: 50,
            partition: 0,
            time: 123,
        };
        let b = TimestampedPartitionOffset {
            offset: 50,
            partition: 0,
            time: 123,
        };
        let c = TimestampedPartitionOffset {
            offset: 50,
            partition: 0,
            time: 124,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_timestamped_partition_offset_differs_from_partition_offset() {
        // Ensure they are distinct types — this compiles only if the types are separate.
        let po = PartitionOffset {
            offset: 1,
            partition: 0,
        };
        let tpo = TimestampedPartitionOffset {
            offset: 1,
            partition: 0,
            time: 0,
        };
        assert_eq!(po.offset, tpo.offset);
        assert_eq!(po.partition, tpo.partition);
    }
}
