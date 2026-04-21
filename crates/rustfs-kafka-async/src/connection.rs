//! Async Kafka connection wrapping tokio TcpStream.

use std::collections::HashMap;
use std::io;

use rustfs_kafka::error::{Error, ProtocolError, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

/// An async TCP connection to a Kafka broker.
pub struct AsyncConnection {
    stream: TcpStream,
    host: String,
}

impl AsyncConnection {
    /// Connects to a Kafka broker asynchronously.
    pub async fn connect(host: &str) -> Result<Self> {
        debug!("Connecting to {}", host);
        let stream = TcpStream::connect(host).await.map_err(|e| {
            Error::Connection(rustfs_kafka::error::ConnectionError::Io(io::Error::new(
                e.kind(),
                format!("connect to {host}: {e}"),
            )))
        })?;

        debug!("Connected to {}", host);
        Ok(Self {
            stream,
            host: host.to_owned(),
        })
    }

    /// Returns the host this connection is connected to.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Sends raw bytes to the broker.
    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.stream
            .write_all(data)
            .await
            .map_err(|e| Error::Connection(rustfs_kafka::error::ConnectionError::Io(e)))?;
        self.stream
            .flush()
            .await
            .map_err(|e| Error::Connection(rustfs_kafka::error::ConnectionError::Io(e)))?;
        Ok(())
    }

    /// Reads exactly `n` bytes from the broker.
    pub async fn read_exact(&mut self, n: u64) -> Result<Vec<u8>> {
        let n = usize::try_from(n).map_err(|_| Error::Protocol(ProtocolError::Codec))?;
        let mut buf = vec![0u8; n];
        self.stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| Error::Connection(rustfs_kafka::error::ConnectionError::Io(e)))?;
        Ok(buf)
    }

    /// Sends a Kafka request frame and reads the response frame.
    pub async fn request_response(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        self.send(request).await?;

        let mut size_buf = [0u8; 4];
        self.stream
            .read_exact(&mut size_buf)
            .await
            .map_err(|e| Error::Connection(rustfs_kafka::error::ConnectionError::Io(e)))?;
        let size = i32::from_be_bytes(size_buf);
        if size < 0 {
            return Err(Error::Protocol(ProtocolError::Codec));
        }

        self.read_exact(size as u64).await
    }
}

/// A pool of async connections to Kafka brokers.
pub struct AsyncConnectionPool {
    connections: HashMap<String, AsyncConnection>,
}

impl AsyncConnectionPool {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }

    /// Gets or creates a connection to the specified host.
    pub async fn get(&mut self, host: &str) -> Result<&mut AsyncConnection> {
        if !self.connections.contains_key(host) {
            let conn = AsyncConnection::connect(host).await?;
            self.connections.insert(host.to_owned(), conn);
        }
        // This is safe because we just ensured the key exists
        Ok(self.connections.get_mut(host).unwrap())
    }

    /// Returns the list of connected hosts.
    #[must_use]
    pub fn hosts(&self) -> Vec<&str> {
        self.connections.keys().map(String::as_str).collect()
    }
}

impl Default for AsyncConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}
