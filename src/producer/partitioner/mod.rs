//! Partitioning strategies for message distribution across topic partitions.
//!
//! Provides the [`Partitioner`] trait and several built-in implementations:
//!
//! - [`DefaultPartitioner`] — hash-based key partitioning (default)
//! - [`RoundRobinPartitioner`] — round-robin for keyless messages
//! - [`StickyPartitioner`] — sticks to one partition per batch
//! - [`UniformPartitioner`] — random uniform distribution

mod default;
mod round_robin;
mod sticky;
mod uniform;

pub use default::DefaultPartitioner;
pub use default::Partitions;
pub use default::Topics;
pub use round_robin::RoundRobinPartitioner;
pub use sticky::StickyPartitioner;
pub use uniform::UniformPartitioner;

use crate::client;

// --------------------------------------------------------------------

/// A partitioner is given a chance to choose/redefine a partition for
/// a message to be sent to Kafka.  See also
/// `Record#with_partition`.
///
/// Implementations can be stateful.
pub trait Partitioner: Send + Sync {
    /// Supposed to inspect the given message and if desired re-assign
    /// the message's target partition.
    fn partition(&mut self, topics: Topics<'_>, msg: &mut client::ProduceMessage<'_, '_>);
}
