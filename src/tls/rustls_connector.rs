//! rustls-based TLS connector implementation.
//!
//! This module provides a pure-Rust TLS implementation using rustls.
//! It is the default and recommended TLS backend for kafka-rust.

use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use super::{TlsConfig, TlsStream};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use tracing::{debug, warn};

/// rustls-based TLS stream
pub struct RustlsStream {
    inner: StreamOwned<ClientConnection, TcpStream>,
}

impl RustlsStream {
    fn new(stream: StreamOwned<ClientConnection, TcpStream>) -> Self {
        RustlsStream { inner: stream }
    }

    fn get_ref(&self) -> &TcpStream {
        self.inner.get_ref()
    }
}

impl TlsStream for RustlsStream {
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

impl Read for RustlsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for RustlsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// rustls-based TLS connector
pub struct RustlsConnector {
    config: Arc<ClientConfig>,
    verify_hostname: bool,
}

impl RustlsConnector {
    /// Create a new rustls connector with the given configuration
    pub fn new(tls_config: &TlsConfig) -> io::Result<Self> {
        let root_store = Self::load_root_store(tls_config)?;

        let provider = {
            #[cfg(feature = "security-ring")]
            {
                rustls::crypto::ring::default_provider()
            }
            #[cfg(not(feature = "security-ring"))]
            {
                rustls::crypto::aws_lc_rs::default_provider()
            }
        };

        let config_builder = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to set protocol versions: {e}"),
                )
            })?
            .with_root_certificates(root_store);

        let config = if let (Some(cert_path), Some(key_path)) =
            (&tls_config.client_cert_path, &tls_config.client_key_path)
        {
            Self::load_client_auth(config_builder, cert_path, key_path)?
        } else {
            config_builder.with_no_client_auth()
        };

        Ok(RustlsConnector {
            config: Arc::new(config),
            verify_hostname: tls_config.verify_hostname,
        })
    }

    fn load_root_store(tls_config: &TlsConfig) -> io::Result<RootCertStore> {
        let mut root_store = RootCertStore::empty();

        // Load CA certificates
        if let Some(ca_cert_path) = &tls_config.ca_cert_path {
            // Load custom CA certificate
            let ca_file = File::open(ca_cert_path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Failed to open CA cert file: {e}"),
                )
            })?;
            let mut ca_reader = BufReader::new(ca_file);

            let certs: Vec<CertificateDer> = CertificateDer::pem_reader_iter(&mut ca_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to parse CA cert: {e}"),
                    )
                })?;

            for cert in certs {
                root_store.add(cert).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to add CA cert: {e}"),
                    )
                })?;
            }
        } else {
            // Use webpki-roots for default trusted CAs
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

            // Also try to load system certificates
            let native_certs = rustls_native_certs::load_native_certs();
            for cert in native_certs.certs {
                let _ = root_store.add(cert);
            }
            if let Some(e) = native_certs.errors.first() {
                debug!(
                    "Failed to load some native certs (using webpki-roots as fallback): {}",
                    e
                );
            }
        }
        Ok(root_store)
    }

    fn load_client_auth(
        builder: rustls::ConfigBuilder<ClientConfig, rustls::client::WantsClientCert>,
        cert_path: &str,
        key_path: &str,
    ) -> io::Result<ClientConfig> {
        // Load client certificate if provided
        let cert_file = File::open(cert_path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Failed to open client cert file: {e}"),
            )
        })?;
        let mut cert_reader = BufReader::new(cert_file);

        let certs: Vec<CertificateDer> = CertificateDer::pem_reader_iter(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse client cert: {e}"),
                )
            })?;

        let key_file = File::open(key_path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Failed to open client key file: {e}"),
            )
        })?;
        let mut key_reader = BufReader::new(key_file);

        // Try to parse as different key types
        let key = PrivateKeyDer::from_pem_reader(&mut key_reader).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse private key: {e}"),
            )
        })?;

        builder.with_client_auth_cert(certs, key).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to set client auth: {e}"),
            )
        })
    }

    /// Connect to a server using TLS
    pub fn connect(&self, domain: &str, tcp_stream: TcpStream) -> io::Result<Box<dyn TlsStream>> {
        let server_name = ServerName::try_from(domain)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Invalid DNS name: {domain}"),
                )
            })?
            .to_owned();

        let conn = ClientConnection::new(self.config.clone(), server_name)
            .map_err(|e| io::Error::other(format!("TLS connection error: {e}")))?;

        // Disable hostname verification if requested (insecure!)
        if !self.verify_hostname {
            warn!(
                "Hostname verification is disabled! This is insecure and should only be used for testing."
            );
            // Note: rustls doesn't provide a direct way to disable hostname verification
            // after ClientConnection is created. The verification happens during
            // ServerName creation, so we've already validated it above.
            // For true insecure mode, users should use a custom verifier.
        }

        let mut stream = StreamOwned::new(conn, tcp_stream);

        // Complete the TLS handshake
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|e| io::Error::other(format!("TLS handshake failed: {e}")))?;

        Ok(Box::new(RustlsStream::new(stream)))
    }
}

impl std::fmt::Debug for RustlsConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RustlsConnector {{ verify_hostname: {} }}",
            self.verify_hostname
        )
    }
}
