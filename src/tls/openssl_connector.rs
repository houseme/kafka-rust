//! OpenSSL-based TLS connector implementation.
//!
//! ⚠️ **DEPRECATED**: This backend will be removed in the next major version.
//! Please migrate to the rustls backend (enabled by default with the `security-rustls` feature).
//!
//! This module provides TLS support using OpenSSL for backward compatibility.

use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslStream, SslVerifyMode};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::time::Duration;
use tracing::warn;

use super::{TlsConfig, TlsStream};

/// OpenSSL-based TLS stream
#[allow(dead_code)] // May not be used when rustls is the default
pub struct OpenSslStream {
    inner: SslStream<TcpStream>,
}

#[allow(dead_code)] // May not be used when rustls is the default
impl OpenSslStream {
    fn new(stream: SslStream<TcpStream>) -> Self {
        OpenSslStream { inner: stream }
    }

    fn get_ref(&self) -> &TcpStream {
        self.inner.get_ref()
    }
}

impl TlsStream for OpenSslStream {
    fn is_secured(&self) -> bool {
        true
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        self.get_ref().set_read_timeout(dur)
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        self.get_ref().set_write_timeout(dur)
    }

    fn shutdown(&mut self) -> io::Result<()> {
        self.get_ref().shutdown(Shutdown::Both)
    }
}

impl Read for OpenSslStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for OpenSslStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// OpenSSL-based TLS connector
///
/// ⚠️ **DEPRECATED**: Use `RustlsConnector` instead.
#[allow(dead_code)] // May not be used when both backends are enabled
#[deprecated(
    since = "0.10.0",
    note = "OpenSSL backend is deprecated. Use rustls backend instead (enabled by default)."
)]
pub struct OpenSslConnector {
    connector: SslConnector,
    verify_hostname: bool,
}

impl OpenSslConnector {
    /// Create a new OpenSSL connector with the given configuration
    #[allow(dead_code)] // May not be used when both backends are enabled
    pub fn new(tls_config: &TlsConfig) -> io::Result<Self> {
        let mut builder = SslConnector::builder(SslMethod::tls())
            .map_err(|e| io::Error::other(format!("Failed to create SSL connector: {e}")))?;

        builder
            .set_cipher_list("DEFAULT")
            .map_err(|e| io::Error::other(format!("Failed to set cipher list: {e}")))?;

        builder.set_verify(SslVerifyMode::PEER);

        // Load client certificate if provided
        if let (Some(cert_path), Some(key_path)) =
            (&tls_config.client_cert_path, &tls_config.client_key_path)
        {
            builder
                .set_certificate_file(cert_path, SslFiletype::PEM)
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to load client cert: {e}"),
                    )
                })?;

            builder
                .set_private_key_file(key_path, SslFiletype::PEM)
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to load client key: {e}"),
                    )
                })?;

            builder.check_private_key().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Private key check failed: {e}"),
                )
            })?;
        }

        // Load CA certificate if provided
        if let Some(ca_cert_path) = &tls_config.ca_cert_path {
            builder.set_ca_file(ca_cert_path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to load CA cert: {e}"),
                )
            })?;
        } else {
            builder.set_default_verify_paths().map_err(|e| {
                io::Error::other(format!("Failed to set default verify paths: {e}"))
            })?;
        }

        Ok(OpenSslConnector {
            connector: builder.build(),
            verify_hostname: tls_config.verify_hostname,
        })
    }

    /// Connect to a server using TLS
    #[allow(dead_code)] // May not be used when both backends are enabled
    pub fn connect(&self, domain: &str, tcp_stream: TcpStream) -> io::Result<Box<dyn TlsStream>> {
        let mut connector_config = self
            .connector
            .configure()
            .map_err(|e| io::Error::other(format!("Failed to configure SSL: {e}")))?;

        if !self.verify_hostname {
            warn!(
                "Hostname verification is disabled! This is insecure and should only be used for testing."
            );
            connector_config.set_verify_hostname(false);
        }

        let stream = connector_config
            .connect(domain, tcp_stream)
            .map_err(|e| io::Error::other(format!("SSL connection failed: {e}")))?;

        Ok(Box::new(OpenSslStream::new(stream)))
    }
}

impl std::fmt::Debug for OpenSslConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OpenSslConnector {{ verify_hostname: {} }}",
            self.verify_hostname
        )
    }
}
