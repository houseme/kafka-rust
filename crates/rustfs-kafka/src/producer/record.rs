use bytes::Bytes;
use std::fmt;

/// A collection of key-value headers attached to a Kafka record.
///
/// Headers are supported since Kafka 0.11.
///
/// Header values are stored as `Bytes` to enable zero-copy sharing
/// when records are batched and encoded for the wire protocol.
#[derive(Default, Clone, Debug)]
pub struct Headers(pub(crate) Vec<(String, Bytes)>);

impl Headers {
    /// Creates an empty headers collection.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a header key-value pair.
    pub fn insert(&mut self, key: impl Into<String>, value: impl AsRef<[u8]>) {
        self.0
            .push((key.into(), Bytes::copy_from_slice(value.as_ref())));
    }

    /// Returns the number of headers.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if there are no headers.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns an iterator over the headers.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &(String, Bytes)> {
        self.0.iter()
    }
}

impl IntoIterator for Headers {
    type Item = (String, Bytes);
    type IntoIter = std::vec::IntoIter<(String, Bytes)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

// --------------------------------------------------------------------

/// A trait used by `Producer` to obtain the bytes `Record::key` and
/// `Record::value` represent.  This leaves the choice of the types
/// for `key` and `value` with the client.
pub trait AsBytes {
    /// Return the byte slice representation of this value.
    ///
    /// Implementors should return a stable slice view over the underlying
    /// data. Empty values may return an empty slice.
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

    /// Optional headers attached to this record.
    pub headers: Headers,
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
            headers: Headers::new(),
        }
    }

    /// Convenience method to set the partition.
    #[inline]
    #[must_use]
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }

    /// Sets the headers for this record.
    #[inline]
    #[must_use]
    pub fn with_headers(mut self, headers: Headers) -> Self {
        self.headers = headers;
        self
    }

    /// Adds a single header to this record.
    #[inline]
    #[must_use]
    pub fn with_header(mut self, key: impl Into<String>, value: impl AsRef<[u8]>) -> Self {
        self.headers.insert(key, value);
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
            headers: Headers::new(),
        }
    }
}

impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for Record<'_, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Record {{ topic: {}, partition: {}, key: {:?}, value: {:?}, headers: {:?} }}",
            self.topic, self.partition, self.key, self.value, self.headers
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headers_empty_default() {
        let h = Headers::new();
        assert!(h.is_empty());
        assert_eq!(h.len(), 0);
    }

    #[test]
    fn test_headers_insert_and_iter() {
        let mut h = Headers::new();
        h.insert("key1", b"value1");
        h.insert("key2", b"value2");

        assert_eq!(h.len(), 2);
        assert!(!h.is_empty());

        let pairs: Vec<_> = h.iter().collect();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, "key1");
        assert_eq!(pairs[1].0, "key2");
    }

    #[test]
    fn test_headers_into_iterator() {
        let mut h = Headers::new();
        h.insert("a", b"1");
        h.insert("b", b"2");

        assert_eq!(h.into_iter().count(), 2);
    }

    #[test]
    fn test_record_default_no_headers() {
        let r = Record::from_value("topic", b"value");
        assert!(r.headers.is_empty());
        assert_eq!(r.partition, -1);
    }

    #[test]
    fn test_record_with_headers() {
        let mut headers = Headers::new();
        headers.insert("trace-id", b"abc123");

        let r = Record::from_value("topic", b"value").with_headers(headers);
        assert_eq!(r.headers.len(), 1);
    }

    #[test]
    fn test_record_with_single_header() {
        let r = Record::from_value("topic", b"value").with_header("content-type", b"json");
        assert_eq!(r.headers.len(), 1);
    }

    #[test]
    fn test_record_from_key_value_with_headers() {
        let r = Record::from_key_value("topic", "key", b"value").with_header("h1", b"v1");
        assert_eq!(r.key, "key");
        assert_eq!(r.headers.len(), 1);
    }
}
