//! TLS abstraction layer for Kafka connections.
//!
//! This module provides a backend-agnostic interface for TLS connections,
//! supporting both rustls (default, pure-Rust) and OpenSSL (deprecated)
//! backends.

use std::io;
use std::net::TcpStream;

// Re-export the appropriate connector based on enabled features
#[cfg(feature = "security-rustls")]
pub mod rustls_connector;
#[cfg(feature = "security-rustls")]
pub use rustls_connector::RustlsConnector;

#[cfg(feature = "security-openssl")]
pub mod openssl_connector;
#[cfg(feature = "security-openssl")]
pub use openssl_connector::OpenSslConnector;

/// Configuration for TLS connections
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Whether to verify the server's hostname
    pub verify_hostname: bool,
    /// Optional path to a CA certificate file
    pub ca_cert_path: Option<String>,
    /// Optional path to a client certificate file
    pub client_cert_path: Option<String>,
    /// Optional path to a client private key file
    pub client_key_path: Option<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        TlsConfig {
            verify_hostname: true,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
        }
    }
}

impl TlsConfig {
    /// Create a new TLS configuration with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to verify the server's hostname
    #[must_use]
    pub fn with_hostname_verification(mut self, verify: bool) -> Self {
        self.verify_hostname = verify;
        self
    }

    /// Set the CA certificate path
    #[must_use]
    pub fn with_ca_cert(mut self, path: String) -> Self {
        self.ca_cert_path = Some(path);
        self
    }

    /// Set the client certificate and key paths
    #[must_use]
    pub fn with_client_cert(mut self, cert_path: String, key_path: String) -> Self {
        self.client_cert_path = Some(cert_path);
        self.client_key_path = Some(key_path);
        self
    }
}

/// Trait for TLS stream implementations
///
/// This trait abstracts over different TLS backends (rustls, OpenSSL)
/// and plain TCP streams.
pub trait TlsStream: io::Read + io::Write + Send {
    /// Returns true if this is a secured (TLS) connection
    fn is_secured(&self) -> bool;

    /// Set the read timeout
    fn set_read_timeout(&mut self, dur: Option<std::time::Duration>) -> io::Result<()>;

    /// Set the write timeout
    fn set_write_timeout(&mut self, dur: Option<std::time::Duration>) -> io::Result<()>;

    /// Shutdown the connection
    fn shutdown(&mut self) -> io::Result<()>;
}

/// Plain TCP stream wrapper implementing TlsStream
pub struct PlainStream {
    inner: TcpStream,
}

impl PlainStream {
    pub fn new(stream: TcpStream) -> Self {
        PlainStream { inner: stream }
    }
}

impl TlsStream for PlainStream {
    fn is_secured(&self) -> bool {
        false
    }

    fn set_read_timeout(&mut self, dur: Option<std::time::Duration>) -> io::Result<()> {
        self.inner.set_read_timeout(dur)
    }

    fn set_write_timeout(&mut self, dur: Option<std::time::Duration>) -> io::Result<()> {
        self.inner.set_write_timeout(dur)
    }

    fn shutdown(&mut self) -> io::Result<()> {
        use std::net::Shutdown;
        self.inner.shutdown(Shutdown::Both)
    }
}

impl io::Read for PlainStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl io::Write for PlainStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
