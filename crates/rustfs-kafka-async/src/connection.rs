//! Async Kafka connection wrapping tokio TcpStream.

use std::collections::HashMap;
use std::io;

use rustfs_kafka::error::{Error, ProtocolError, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

/// An async TCP connection to a Kafka broker.
///
/// This type wraps a `tokio::net::TcpStream` and provides convenient
/// read/write helpers that map IO errors into the crate's `Error` type. It is
/// used by higher-level components to perform request/response interactions
/// with a Kafka broker.
pub struct AsyncConnection {
    stream: TcpStream,
    host: String,
}

impl AsyncConnection {
    /// Connects to a Kafka broker asynchronously.
    ///
    /// On error the returned `Error::Connection` will contain an underlying
    /// IO error describing the failure. The `host` argument is the socket
    /// address of the broker (for example `"127.0.0.1:9092"`).
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

    /// Sends raw bytes to the broker and flushes the stream.
    ///
    /// This writes all bytes in `data` and then flushes the socket. IO errors
    /// are mapped to `Error::Connection(... Io(...))`.
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

    /// Reads exactly `n` bytes from the broker and returns them as `Bytes`.
    ///
    /// If `n` does not fit into a `usize` the call returns
    /// `Error::Protocol(ProtocolError::Codec)`. Any IO error while reading is
    /// mapped to `Error::Connection(... Io(...))`.
    pub async fn read_exact(&mut self, n: u64) -> Result<bytes::Bytes> {
        let n = usize::try_from(n).map_err(|_| Error::Protocol(ProtocolError::Codec))?;
        let mut buf = bytes::BytesMut::with_capacity(n);
        buf.resize(n, 0);
        self.stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| Error::Connection(rustfs_kafka::error::ConnectionError::Io(e)))?;
        Ok(buf.freeze())
    }

    /// Sends a Kafka request frame and reads the response frame.
    ///
    /// The wire format expected is a 4-byte big-endian length followed by the
    /// payload. This function writes the provided `request` bytes (which
    /// should already include any framing required by the caller), then reads
    /// a 4-byte response size and finally reads exactly that many bytes and
    /// returns them.
    pub async fn request_response(&mut self, request: &[u8]) -> Result<bytes::Bytes> {
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
///
/// This is a simple, in-memory map of host -> `AsyncConnection`. It is not
/// concurrent; callers should hold a mutable reference to the pool while
/// performing operations. The pool lazily creates connections when `get` is
/// called.
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
    ///
    /// If a connection for `host` does not exist it will be created and
    /// inserted into the pool. The returned mutable reference is valid as long
    /// as the borrow on `&mut self` is held by the caller.
    pub async fn get(&mut self, host: &str) -> Result<&mut AsyncConnection> {
        if !self.connections.contains_key(host) {
            let conn = AsyncConnection::connect(host).await?;
            self.insert(host.to_owned(), conn);
        }
        // This is safe because we just ensured the key exists
        Ok(self.connections.get_mut(host).unwrap())
    }

    pub(crate) fn insert(&mut self, host: String, connection: AsyncConnection) {
        self.connections.insert(host, connection);
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

#[cfg(test)]
mod tests {
    use rustfs_kafka::error::ConnectionError;

    use super::*;

    #[test]
    fn pool_new_creates_empty_pool() {
        let pool = AsyncConnectionPool::new();
        assert!(pool.hosts().is_empty());
    }

    #[test]
    fn pool_default_matches_new() {
        let pool = AsyncConnectionPool::default();
        assert!(pool.hosts().is_empty());
    }

    #[tokio::test]
    async fn connect_unreachable_host_returns_io_error() {
        let result = AsyncConnection::connect("127.0.0.1:1").await;
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::Io(_)))
        ));
    }

    #[tokio::test]
    async fn pool_get_unreachable_host_propagates_error() {
        let mut pool = AsyncConnectionPool::new();
        let result = pool.get("127.0.0.1:1").await;
        assert!(result.is_err());
        // Pool should remain empty after failed connect
        assert!(pool.hosts().is_empty());
    }
}
