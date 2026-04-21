//! Network related functionality for `KafkaClient`.
//!
//! This module is crate private and not exposed to the public except
//! through re-exports of individual items from within
//! `rustfs_kafka::client`.

mod connection;
mod pool;

pub(crate) use self::connection::KafkaConnection;
pub use self::pool::Connections;

#[cfg(feature = "security")]
pub use self::connection::SecurityConfig;

#[cfg(not(feature = "security"))]
pub type SecurityConfig = ();

/// A wrapper to track the last checkout time of a pooled item.
pub(crate) struct Pooled<T> {
    pub item: T,
    pub last_checkout: std::time::Instant,
}

impl<T> Pooled<T> {
    pub(crate) fn new(last_checkout: std::time::Instant, item: T) -> Self {
        Pooled { item, last_checkout }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Pooled<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Pooled {{ last_checkout: {:?}, item: {:?} }}",
            self.last_checkout, self.item
        )
    }
}
