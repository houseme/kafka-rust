//! Async Kafka client for metadata and connection management.

use rustfs_kafka::error::{ConnectionError, Error, Result};
use tokio::task::JoinSet;
use tracing::{debug, info};

use crate::connection::{AsyncConnection, AsyncConnectionPool};

/// An async Kafka client for bootstrap and connection management.
///
/// This lightweight client manages a pool of [`AsyncConnection`]s and is
/// intended to be used by other async wrappers (producer/consumer) to obtain
/// connections to brokers without imposing `Sync`/`'static` constraints on the
/// higher-level code. It will attempt to connect to the provided bootstrap
/// hosts on creation (unless the host list is empty), but will not continuously
/// maintain metadata — callers can use [`ensure_connected`] to trigger a
/// reconnection to bootstrap hosts when necessary.
pub struct AsyncKafkaClient {
    pool: AsyncConnectionPool,
    bootstrap_hosts: Vec<String>,
    client_id: String,
}

impl AsyncKafkaClient {
    /// Creates a new async client and connects to the bootstrap brokers.
    pub async fn new(hosts: Vec<String>) -> Result<Self> {
        Self::with_client_id(hosts, "rustfs-kafka-async".to_owned()).await
    }

    /// Creates a new async client with a specific client ID.
    ///
    /// Attempts to connect to the provided `hosts` in order until a
    /// connection succeeds. If no hosts are reachable and the `hosts` list is
    /// non-empty, an error `Error::Connection(ConnectionError::NoHostReachable)`
    /// is returned.
    pub async fn with_client_id(hosts: Vec<String>, client_id: String) -> Result<Self> {
        let mut pool = AsyncConnectionPool::new();
        let connected = connect_any_bootstrap(&mut pool, &hosts).await;

        if !connected && !hosts.is_empty() {
            return Err(Error::Connection(ConnectionError::NoHostReachable));
        }

        info!(
            "AsyncKafkaClient created with {} bootstrap hosts",
            hosts.len()
        );

        Ok(Self {
            pool,
            bootstrap_hosts: hosts,
            client_id,
        })
    }

    /// Returns the client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the bootstrap hosts.
    #[must_use]
    pub fn bootstrap_hosts(&self) -> &[String] {
        &self.bootstrap_hosts
    }

    /// Gets (or creates) a mutable reference to a connection for `host`.
    ///
    /// If a connection for `host` does not yet exist, the underlying
    /// [`AsyncConnection::connect`] is attempted and the connection is stored in
    /// the internal pool. The returned reference is tied to the mutable
    /// borrow of `self` and therefore short-lived.
    pub async fn get_connection(&mut self, host: &str) -> Result<&mut AsyncConnection> {
        self.pool.get(host).await
    }

    /// Gets the list of currently connected hosts.
    ///
    /// This returns the host addresses for which there is an established
    /// connection in the internal pool. The returned `Vec<&str>` is a snapshot
    /// of the current keys and does not hold any borrow on `self` afterwards.
    #[must_use]
    pub fn connected_hosts(&self) -> Vec<&str> {
        self.pool.hosts()
    }

    /// Ensures the client has at least one active connection.
    ///
    /// If the client was created with bootstrap hosts and the internal pool is
    /// currently empty, this will attempt to connect to the bootstrap hosts in
    /// order until one succeeds. It is a no-op when `bootstrap_hosts` is empty
    /// or when the pool already contains connections.
    pub async fn ensure_connected(&mut self) -> Result<()> {
        if !self.bootstrap_hosts.is_empty() && self.pool.hosts().is_empty() {
            connect_any_bootstrap(&mut self.pool, &self.bootstrap_hosts).await;
        }
        Ok(())
    }
}

async fn connect_any_bootstrap(pool: &mut AsyncConnectionPool, hosts: &[String]) -> bool {
    let mut set = JoinSet::new();
    for host in hosts {
        let host = host.clone();
        set.spawn(async move {
            let connection = crate::connection::AsyncConnection::connect(&host).await;
            (host, connection)
        });
    }

    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((host, Ok(connection))) => {
                pool.insert(host, connection);
                return true;
            }
            Ok((host, Err(e))) => {
                debug!("Failed to connect to {}: {}", host, e);
            }
            Err(e) => {
                debug!("Bootstrap connect task failed to join: {}", e);
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use rustfs_kafka::error::{ConnectionError, Error};

    use super::*;

    #[tokio::test]
    async fn new_with_empty_hosts_succeeds() {
        let result = AsyncKafkaClient::new(vec![]).await;
        assert!(result.is_ok());
        let client = result.unwrap();
        assert!(client.bootstrap_hosts().is_empty());
        assert!(client.connected_hosts().is_empty());
    }

    #[tokio::test]
    async fn new_with_unreachable_hosts_returns_error() {
        let result = AsyncKafkaClient::new(vec!["127.0.0.1:1".to_owned()]).await;
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::NoHostReachable))
        ));
    }

    #[tokio::test]
    async fn with_client_id_unreachable_returns_error() {
        let result = AsyncKafkaClient::with_client_id(
            vec!["127.0.0.1:1".to_owned()],
            "my-custom-client".to_owned(),
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::NoHostReachable))
        ));
    }

    #[tokio::test]
    async fn ensure_connected_with_empty_hosts_is_ok() {
        let client = AsyncKafkaClient {
            pool: AsyncConnectionPool::new(),
            bootstrap_hosts: vec![],
            client_id: "test".to_owned(),
        };
        // ensure_connected with empty bootstrap_hosts is a no-op
        assert!(client.bootstrap_hosts.is_empty());
        assert!(client.connected_hosts().is_empty());
    }
}
