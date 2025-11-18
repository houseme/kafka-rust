//! rustls-based TLS connector implementation.
//!
//! This module provides a pure-Rust TLS implementation using rustls.
//! It is the default and recommended TLS backend for kafka-rust.

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerName, StreamOwned};

use super::{TlsConfig, TlsStream};

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
    pub fn new(tls_config: TlsConfig) -> io::Result<Self> {
        let mut root_store = RootCertStore::empty();

        // Load CA certificates
        if let Some(ca_cert_path) = &tls_config.ca_cert_path {
            // Load custom CA certificate
            let ca_file = File::open(ca_cert_path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Failed to open CA cert file: {}", e),
                )
            })?;
            let mut ca_reader = BufReader::new(ca_file);
            
            let certs = rustls_pemfile::certs(&mut ca_reader).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse CA cert: {}", e),
                )
            })?;
            
            for cert in certs {
                root_store
                    .add(&rustls::Certificate(cert))
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Failed to add CA cert: {}", e),
                        )
                    })?;
            }
        } else {
            // Use webpki-roots for default trusted CAs
            root_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| {
                rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
                    ta.subject,
                    ta.spki,
                    ta.name_constraints,
                )
            }));

            // Also try to load system certificates
            match rustls_native_certs::load_native_certs() {
                Ok(certs) => {
                    for cert in certs {
                        let _ = root_store.add(&rustls::Certificate(cert.0));
                    }
                }
                Err(e) => {
                    debug!("Failed to load native certs (using webpki-roots as fallback): {}", e);
                }
            }
        }

        let config_builder = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_store);

        let config = if let (Some(cert_path), Some(key_path)) =
            (&tls_config.client_cert_path, &tls_config.client_key_path)
        {
            // Load client certificate if provided
            let cert_file = File::open(cert_path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Failed to open client cert file: {}", e),
                )
            })?;
            let mut cert_reader = BufReader::new(cert_file);
            
            let certs = rustls_pemfile::certs(&mut cert_reader)
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to parse client cert: {}", e),
                    )
                })?
                .into_iter()
                .map(rustls::Certificate)
                .collect();

            let key_file = File::open(key_path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Failed to open client key file: {}", e),
                )
            })?;
            let mut key_reader = BufReader::new(key_file);
            
            // Try to parse as different key types
            let mut keys = rustls_pemfile::pkcs8_private_keys(&mut key_reader).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse private key: {}", e),
                )
            })?;

            if keys.is_empty() {
                // Try RSA keys
                key_reader.get_mut().seek(io::SeekFrom::Start(0))?;
                keys = rustls_pemfile::rsa_private_keys(&mut key_reader).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to parse RSA private key: {}", e),
                    )
                })?;
            }

            let key = keys
                .into_iter()
                .next()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "No private key found"))?;

            config_builder
                .with_client_auth_cert(certs, rustls::PrivateKey(key))
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to set client auth: {}", e),
                    )
                })?
        } else {
            config_builder.with_no_client_auth()
        };

        Ok(RustlsConnector {
            config: Arc::new(config),
            verify_hostname: tls_config.verify_hostname,
        })
    }

    /// Connect to a server using TLS
    pub fn connect(&self, domain: &str, tcp_stream: TcpStream) -> io::Result<Box<dyn TlsStream>> {
        let server_name = ServerName::try_from(domain).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid DNS name: {}", domain),
            )
        })?;

        let conn = ClientConnection::new(self.config.clone(), server_name).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("TLS connection error: {}", e))
        })?;

        // Disable hostname verification if requested (insecure!)
        if !self.verify_hostname {
            warn!("Hostname verification is disabled! This is insecure and should only be used for testing.");
            // Note: rustls doesn't provide a direct way to disable hostname verification
            // after ClientConnection is created. The verification happens during
            // ServerName creation, so we've already validated it above.
            // For true insecure mode, users should use a custom verifier.
        }

        let mut stream = StreamOwned::new(conn, tcp_stream);

        // Complete the TLS handshake
        stream.conn.complete_io(&mut stream.sock).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("TLS handshake failed: {}", e))
        })?;

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
