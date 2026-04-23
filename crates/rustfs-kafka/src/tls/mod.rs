//! TLS abstraction layer for Kafka connections.
//!
//! This module provides TLS connections via rustls (pure-Rust).

use std::io;
use std::net::TcpStream;

pub mod rustls_connector;
pub use rustls_connector::RustlsConnector;

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
    #[must_use] 
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
#[allow(dead_code)] // Methods may not be used in all configurations
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

/// Plain TCP stream wrapper implementing `TlsStream`
#[allow(dead_code)] // May not be used in all configurations
pub struct PlainStream {
    inner: TcpStream,
}

#[allow(dead_code)] // May not be used in all configurations
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
