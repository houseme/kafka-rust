//! Async Kafka client for metadata and connection management.

use rustfs_kafka::error::{ConnectionError, Error, Result};
use tracing::{debug, info};

use crate::connection::{AsyncConnection, AsyncConnectionPool};

/// An async Kafka client for bootstrap and metadata operations.
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
    pub async fn with_client_id(hosts: Vec<String>, client_id: String) -> Result<Self> {
        let mut pool = AsyncConnectionPool::new();

        let mut connected = false;
        for host in &hosts {
            match pool.get(host).await {
                Ok(_) => {
                    connected = true;
                    break;
                }
                Err(e) => {
                    debug!("Failed to connect to {}: {}", host, e);
                }
            }
        }

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

    /// Gets a mutable reference to a connection by host.
    pub async fn get_connection(&mut self, host: &str) -> Result<&mut AsyncConnection> {
        self.pool.get(host).await
    }

    /// Gets the list of currently connected hosts.
    #[must_use]
    pub fn connected_hosts(&self) -> Vec<&str> {
        self.pool.hosts()
    }

    /// Refreshes metadata by connecting to bootstrap hosts if needed.
    pub async fn ensure_connected(&mut self) -> Result<()> {
        if !self.bootstrap_hosts.is_empty() && self.pool.hosts().is_empty() {
            for host in &self.bootstrap_hosts {
                if self.pool.get(host).await.is_ok() {
                    break;
                }
            }
        }
        Ok(())
    }
}
