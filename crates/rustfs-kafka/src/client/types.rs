//! Public client request and response DTOs.

use crate::error::KafkaCode;

/// Possible values when querying a topic's offset.
/// See `KafkaClient::fetch_offsets`.
#[derive(Debug, Copy, Clone)]
pub enum FetchOffset {
    /// Receive the earliest available offset.
    Earliest,
    /// Receive the latest offset.
    Latest,
    /// Used to ask for all messages before a certain time (ms); unix
    /// timestamp in milliseconds.
    ByTime(i64),
}

impl FetchOffset {
    pub(super) fn to_kafka_value(self) -> i64 {
        match self {
            FetchOffset::Earliest => -2,
            FetchOffset::Latest => -1,
            FetchOffset::ByTime(n) => n,
        }
    }
}

/// Defines the available storage types to utilize when fetching or
/// committing group offsets.  See also `KafkaClient::set_group_offset_storage`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum GroupOffsetStorage {
    /// Zookeeper based storage (available as of kafka 0.8.1)
    Zookeeper,
    /// Kafka based storage (available as of Kafka 0.8.2). This is the
    /// preferred method for groups to store their offsets at.
    Kafka,
}

/// Data point identifying a topic partition to fetch a group's offset
/// for.  See `KafkaClient::fetch_group_offsets`.
#[derive(Debug)]
pub struct FetchGroupOffset<'a> {
    /// The topic to fetch the group offset for
    pub topic: &'a str,
    /// The partition to fetch the group offset for
    pub partition: i32,
}

impl<'a> FetchGroupOffset<'a> {
    /// Create a new `FetchGroupOffset` which identifies a topic partition
    /// to query a group's offset for.
    ///
    /// The returned value borrows the provided `topic` string slice.
    #[inline]
    #[must_use]
    pub fn new(topic: &'a str, partition: i32) -> Self {
        FetchGroupOffset { topic, partition }
    }
}

impl<'a> AsRef<FetchGroupOffset<'a>> for FetchGroupOffset<'a> {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// Data point identifying a particular topic partition offset to be
/// committed.
/// See `KafkaClient::commit_offsets`.
#[derive(Debug)]
pub struct CommitOffset<'a> {
    /// The offset to be committed
    pub offset: i64,
    /// The topic to commit the offset for
    pub topic: &'a str,
    /// The partition to commit the offset for
    pub partition: i32,
}

impl<'a> CommitOffset<'a> {
    /// Construct a `CommitOffset` for the given topic partition and offset.
    ///
    /// This is a convenience constructor used when committing consumer
    /// offsets on behalf of a group.
    #[must_use]
    pub fn new(topic: &'a str, partition: i32, offset: i64) -> Self {
        CommitOffset {
            offset,
            topic,
            partition,
        }
    }
}

impl<'a> AsRef<CommitOffset<'a>> for CommitOffset<'a> {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// Possible choices on acknowledgement requirements when
/// producing/sending messages to Kafka. See
/// `KafkaClient::produce_messages`.
#[derive(Debug, Copy, Clone)]
pub enum RequiredAcks {
    /// Indicates to the receiving Kafka broker not to acknowledge
    /// messages sent to it at all.
    None = 0,
    /// Requires the receiving Kafka broker to wait until the sent
    /// messages are written to local disk.
    One = 1,
    /// Requires the sent messages to be acknowledged by all in-sync
    /// replicas of the targeted topic partitions.
    All = -1,
}

/// Message data to be sent/produced to a particular topic partition.
/// See `KafkaClient::produce_messages` and `Producer::send`.
#[derive(Debug)]
pub struct ProduceMessage<'a, 'b> {
    /// The "key" data of this message.
    pub key: Option<&'b [u8]>,
    /// The "value" data of this message.
    pub value: Option<&'b [u8]>,
    /// The topic to produce this message to.
    pub topic: &'a str,
    /// The partition (of the corresponding topic) to produce this
    /// message to.
    pub partition: i32,
    /// Optional headers for this message.
    pub headers: &'b [(String, bytes::Bytes)],
}

impl<'a, 'b> AsRef<ProduceMessage<'a, 'b>> for ProduceMessage<'a, 'b> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<'a, 'b> ProduceMessage<'a, 'b> {
    /// A convenient constructor method to create a new produce
    /// message with all attributes specified.
    #[must_use]
    pub fn new(
        topic: &'a str,
        partition: i32,
        key: Option<&'b [u8]>,
        value: Option<&'b [u8]>,
    ) -> Self {
        ProduceMessage {
            key,
            value,
            topic,
            partition,
            headers: &[],
        }
    }
}

/// Partition related request data for fetching messages.
/// See `KafkaClient::fetch_messages`.
#[derive(Debug)]
pub struct FetchPartition<'a> {
    /// The topic to fetch messages from.
    pub topic: &'a str,
    /// The offset as of which to fetch messages.
    pub offset: i64,
    /// The partition to fetch messages from.
    pub partition: i32,
    /// Specifies the max. amount of data to fetch (for this
    /// partition.)
    pub max_bytes: i32,
}

impl<'a> FetchPartition<'a> {
    /// Creates a new "fetch messages" request structure with an
    /// unspecified `max_bytes`.
    #[must_use]
    pub fn new(topic: &'a str, partition: i32, offset: i64) -> Self {
        FetchPartition {
            topic,
            partition,
            offset,
            max_bytes: -1,
        }
    }

    /// Sets the `max_bytes` value for the "fetch messages" request.
    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: i32) -> Self {
        self.max_bytes = max_bytes;
        self
    }
}

impl<'a> AsRef<FetchPartition<'a>> for FetchPartition<'a> {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// A confirmation of messages sent back by the Kafka broker
/// to confirm delivery of producer messages.
#[derive(Debug)]
pub struct ProduceConfirm {
    /// The topic the messages were sent to.
    pub topic: String,
    /// The list of individual confirmations for each offset and partition.
    pub partition_confirms: Vec<ProducePartitionConfirm>,
}

/// A confirmation of messages sent back by the Kafka broker
/// to confirm delivery of producer messages for a particular topic.
#[derive(Debug)]
pub struct ProducePartitionConfirm {
    /// The offset assigned to the first message in the message set appended
    /// to this partition, or an error if one occurred.
    pub offset: std::result::Result<i64, KafkaCode>,
    /// The partition to which the message(s) were appended.
    pub partition: i32,
}
