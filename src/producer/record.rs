use std::fmt;

/// A trait used by `Producer` to obtain the bytes `Record::key` and
/// `Record::value` represent.  This leaves the choice of the types
/// for `key` and `value` with the client.
pub trait AsBytes {
    fn as_bytes(&self) -> &[u8];
}

impl AsBytes for () {
    fn as_bytes(&self) -> &[u8] {
        &[]
    }
}

impl AsBytes for String {
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}
impl AsBytes for Vec<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

impl AsBytes for &[u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}
impl AsBytes for &str {
    fn as_bytes(&self) -> &[u8] {
        str::as_bytes(self)
    }
}

// --------------------------------------------------------------------

/// A structure representing a message to be sent to Kafka through the
/// `Producer` API.  Such a message is basically a key/value pair
/// specifying the target topic and optionally the topic's partition.
pub struct Record<'a, K, V> {
    /// Key data of this (message) record.
    pub key: K,

    /// Value data of this (message) record.
    pub value: V,

    /// Name of the topic this message is supposed to be delivered to.
    pub topic: &'a str,

    /// The partition id of the topic to deliver this message to.
    /// This partition may be `< 0` in which case it is considered
    /// "unspecified".  A `Producer` will then typically try to derive
    /// a partition on its own.
    pub partition: i32,
}

impl<'a, K, V> Record<'a, K, V> {
    /// Convenience function to create a new key/value record with an
    /// "unspecified" partition - this is, a partition set to a negative
    /// value.
    #[inline]
    pub fn from_key_value(topic: &'a str, key: K, value: V) -> Record<'a, K, V> {
        Record {
            key,
            value,
            topic,
            partition: -1,
        }
    }

    /// Convenience method to set the partition.
    #[inline]
    #[must_use]
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }
}

impl<'a, V> Record<'a, (), V> {
    /// Convenience function to create a new value only record with an
    /// "unspecified" partition - this is, a partition set to a negative
    /// value.
    #[inline]
    pub fn from_value(topic: &'a str, value: V) -> Record<'a, (), V> {
        Record {
            key: (),
            value,
            topic,
            partition: -1,
        }
    }
}

impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for Record<'_, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Record {{ topic: {}, partition: {}, key: {:?}, value: {:?} }}",
            self.topic, self.partition, self.key, self.value
        )
    }
}
