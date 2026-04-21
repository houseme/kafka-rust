//! Network related functionality for `KafkaClient`.
//!
//! This module is crate private and not exposed to the public except
//! through re-exports of individual items from within
//! `rustfs_kafka::client`.

use crate::error::Result;
use std::collections::HashMap;
use std::fmt;
use std::io::{Read, Write};
use std::mem;
use std::net::{Shutdown, TcpStream};
use std::time::{Duration, Instant};
use tracing::{debug, trace, warn};

#[cfg(feature = "security")]
use crate::tls::{RustlsConnector, TlsConfig, TlsStream};

// --------------------------------------------------------------------

/// Security relevant configuration options for `KafkaClient`.
#[cfg(feature = "security")]
pub struct SecurityConfig {
    tls_config: TlsConfig,
}

#[cfg(feature = "security")]
impl SecurityConfig {
    /// Create a new `SecurityConfig` with default TLS settings.
    #[must_use]
    pub fn new() -> Self {
        SecurityConfig {
            tls_config: TlsConfig::new(),
        }
    }

    /// Create a `SecurityConfig` from a `TlsConfig`
    #[must_use]
    pub fn from_tls_config(tls_config: TlsConfig) -> SecurityConfig {
        SecurityConfig { tls_config }
    }

    /// Initiates a client-side TLS session with/without performing hostname verification.
    #[must_use]
    pub fn with_hostname_verification(mut self, verify_hostname: bool) -> SecurityConfig {
        self.tls_config.verify_hostname = verify_hostname;
        self
    }

    /// Set a custom CA certificate file path
    #[must_use]
    pub fn with_ca_cert(mut self, path: String) -> SecurityConfig {
        self.tls_config.ca_cert_path = Some(path);
        self
    }

    /// Set client certificate and key file paths
    #[must_use]
    pub fn with_client_cert(mut self, cert_path: String, key_path: String) -> SecurityConfig {
        self.tls_config.client_cert_path = Some(cert_path);
        self.tls_config.client_key_path = Some(key_path);
        self
    }
}

#[cfg(feature = "security")]
impl Default for SecurityConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "security")]
impl fmt::Debug for SecurityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SecurityConfig {{ verify_hostname: {} }}",
            self.tls_config.verify_hostname
        )
    }
}

// --------------------------------------------------------------------

struct Pooled<T> {
    last_checkout: Instant,
    item: T,
}

impl<T> Pooled<T> {
    fn new(last_checkout: Instant, item: T) -> Self {
        Pooled {
            last_checkout,
            item,
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Pooled<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Pooled {{ last_checkout: {:?}, item: {:?} }}",
            self.last_checkout, self.item
        )
    }
}

#[derive(Debug)]
pub struct Config {
    rw_timeout: Option<Duration>,
    idle_timeout: Duration,
    #[cfg(feature = "security")]
    security_config: Option<SecurityConfig>,
}

impl Config {
    #[cfg(not(feature = "security"))]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        KafkaConnection::new(id, host, self.rw_timeout).map(|c| {
            debug!("Established: {:?}", c);
            c
        })
    }

    #[cfg(feature = "security")]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        KafkaConnection::new(
            id,
            host,
            self.rw_timeout,
            self.security_config.as_ref().map(|c| &c.tls_config),
        )
        .map(|c| {
            debug!("Established: {:?}", c);
            c
        })
    }
}

#[derive(Debug)]
struct State {
    num_conns: u32,
}

impl State {
    fn new() -> State {
        State { num_conns: 0 }
    }

    fn next_conn_id(&mut self) -> u32 {
        let c = self.num_conns;
        self.num_conns = self.num_conns.wrapping_add(1);
        c
    }
}

#[derive(Debug)]
pub struct Connections {
    conns: HashMap<String, Pooled<KafkaConnection>>,
    state: State,
    config: Config,
}

impl Connections {
    #[cfg(not(feature = "security"))]
    pub fn new(rw_timeout: Option<Duration>, idle_timeout: Duration) -> Connections {
        Connections {
            conns: HashMap::new(),
            state: State::new(),
            config: Config {
                rw_timeout,
                idle_timeout,
            },
        }
    }

    #[cfg(feature = "security")]
    pub fn new(rw_timeout: Option<Duration>, idle_timeout: Duration) -> Connections {
        Self::new_with_security(rw_timeout, idle_timeout, None)
    }

    #[cfg(feature = "security")]
    pub fn new_with_security(
        rw_timeout: Option<Duration>,
        idle_timeout: Duration,
        security: Option<SecurityConfig>,
    ) -> Connections {
        Connections {
            conns: HashMap::new(),
            state: State::new(),
            config: Config {
                rw_timeout,
                idle_timeout,
                security_config: security,
            },
        }
    }

    pub fn set_idle_timeout(&mut self, idle_timeout: Duration) {
        self.config.idle_timeout = idle_timeout;
    }

    pub fn idle_timeout(&self) -> Duration {
        self.config.idle_timeout
    }

    pub fn get_conn(&mut self, host: &str, now: Instant) -> Result<&mut KafkaConnection> {
        if let Some(conn) = self.conns.get_mut(host) {
            let needs_reconnect =
                now.duration_since(conn.last_checkout) >= self.config.idle_timeout
                    || conn.item.is_terminated();
            if needs_reconnect {
                let reason = if conn.item.is_terminated() {
                    "connection terminated"
                } else {
                    "idle timeout"
                };
                debug!("Reconnecting ({}) to: {:?}", reason, conn.item);
                let new_conn = self.config.new_conn(self.state.next_conn_id(), host)?;
                let _ = conn.item.shutdown();
                conn.item = new_conn;
            }
            conn.last_checkout = now;
            let kconn: &mut KafkaConnection = &mut conn.item;
            return Ok(unsafe { mem::transmute(kconn) });
        }
        let cid = self.state.next_conn_id();
        self.conns.insert(
            host.to_owned(),
            Pooled::new(now, self.config.new_conn(cid, host)?),
        );
        Ok(&mut self.conns.get_mut(host).unwrap().item)
    }

    pub fn get_conn_any(&mut self, now: Instant) -> Option<&mut KafkaConnection> {
        for (host, conn) in &mut self.conns {
            let needs_reconnect =
                now.duration_since(conn.last_checkout) >= self.config.idle_timeout
                    || conn.item.is_terminated();
            if needs_reconnect {
                let reason = if conn.item.is_terminated() {
                    "connection terminated"
                } else {
                    "idle timeout"
                };
                debug!("Reconnecting ({}) to: {:?}", reason, conn.item);
                let new_conn_id = self.state.next_conn_id();
                let new_conn = match self.config.new_conn(new_conn_id, host.as_str()) {
                    Ok(new_conn) => {
                        let _ = conn.item.shutdown();
                        new_conn
                    }
                    Err(e) => {
                        warn!("Failed to reconnect to {}: {:?}", host, e);
                        continue;
                    }
                };
                conn.item = new_conn;
            }
            conn.last_checkout = now;
            let kconn: &mut KafkaConnection = &mut conn.item;
            return Some(kconn);
        }
        None
    }
}

// --------------------------------------------------------------------

#[cfg(not(feature = "security"))]
type KafkaStream = TcpStream;

#[cfg(feature = "security")]
enum KafkaStream {
    Plain(TcpStream),
    Tls(Box<dyn TlsStream>),
}

trait StreamOps {
    fn is_secured(&self) -> bool;
    fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()>;
    fn set_write_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()>;
    fn shutdown(&mut self, how: Shutdown) -> std::io::Result<()>;
}

#[cfg(not(feature = "security"))]
impl StreamOps for KafkaStream {
    fn is_secured(&self) -> bool {
        false
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        self.set_read_timeout(dur)
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        self.set_write_timeout(dur)
    }

    fn shutdown(&mut self, how: Shutdown) -> std::io::Result<()> {
        self.shutdown(how)
    }
}

#[cfg(feature = "security")]
impl StreamOps for KafkaStream {
    fn is_secured(&self) -> bool {
        match self {
            KafkaStream::Plain(_) => false,
            KafkaStream::Tls(_) => true,
        }
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.set_read_timeout(dur),
            KafkaStream::Tls(s) => s.set_read_timeout(dur),
        }
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.set_write_timeout(dur),
            KafkaStream::Tls(s) => s.set_write_timeout(dur),
        }
    }

    fn shutdown(&mut self, how: Shutdown) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.shutdown(how),
            KafkaStream::Tls(s) => s.shutdown(),
        }
    }
}

#[cfg(feature = "security")]
impl Read for KafkaStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            KafkaStream::Plain(s) => s.read(buf),
            KafkaStream::Tls(s) => s.read(buf),
        }
    }
}

#[cfg(feature = "security")]
impl Write for KafkaStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            KafkaStream::Plain(s) => s.write(buf),
            KafkaStream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.flush(),
            KafkaStream::Tls(s) => s.flush(),
        }
    }
}

/// A TCP stream to a remote Kafka broker.
pub struct KafkaConnection {
    id: u32,
    host: String,
    stream: KafkaStream,
    state: ConnectionState,
}

/// Connection health state for detecting broken connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    /// Connection is healthy and ready for I/O.
    Connected,
    /// Connection was terminated (e.g., by broker, timeout, or error).
    /// Next operation should trigger a reconnection.
    Terminated,
}

impl fmt::Debug for KafkaConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KafkaConnection {{ id: {}, secured: {}, state: {:?}, host: \"{}\" }}",
            self.id,
            self.stream.is_secured(),
            self.state,
            self.host
        )
    }
}

/// Configure a TCP socket with keepalive and nodelay for Kafka compatibility.
fn configure_tcp_socket(socket: &socket2::Socket) -> std::io::Result<()> {
    use socket2::TcpKeepalive;

    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(10))
        .with_interval(Duration::from_secs(20));
    socket.set_tcp_keepalive(&keepalive)?;
    socket.set_tcp_nodelay(true)?;
    Ok(())
}

impl KafkaConnection {
    pub fn send(&mut self, msg: &[u8]) -> Result<usize> {
        let r = self.stream.write(msg).map_err(|e| {
            self.state = ConnectionState::Terminated;
            From::from(e)
        });
        trace!("Sent {} bytes to: {:?} => {:?}", msg.len(), self, r);
        r
    }

    fn is_terminated(&self) -> bool {
        self.state == ConnectionState::Terminated
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let r = self.stream.read_exact(buf).map_err(|e| {
            self.state = ConnectionState::Terminated;
            From::from(e)
        });
        trace!("Read {} bytes from: {:?} => {:?}", buf.len(), self, r);
        r
    }

    pub fn read_exact_alloc(&mut self, size: u64) -> Result<Vec<u8>> {
        let mut buffer = vec![0; size as usize];
        self.read_exact(buffer.as_mut_slice())?;
        Ok(buffer)
    }

    fn shutdown(&mut self) -> Result<()> {
        self.state = ConnectionState::Terminated;
        let r = self.stream.shutdown(Shutdown::Both);
        debug!("Shut down: {:?} => {:?}", self, r);
        r.map_err(From::from)
    }

    fn from_stream(
        mut stream: KafkaStream,
        id: u32,
        host: &str,
        rw_timeout: Option<Duration>,
    ) -> Result<KafkaConnection> {
        stream.set_read_timeout(rw_timeout)?;
        stream.set_write_timeout(rw_timeout)?;
        Ok(KafkaConnection {
            id,
            host: host.to_owned(),
            stream,
            state: ConnectionState::Connected,
        })
    }

    fn new_tcp_stream(host: &str) -> std::io::Result<TcpStream> {
        let addr: std::net::SocketAddr = host
            .parse()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let domain = match addr {
            std::net::SocketAddr::V4(_) => socket2::Domain::IPV4,
            std::net::SocketAddr::V6(_) => socket2::Domain::IPV6,
        };
        let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;

        socket.connect(&socket2::SockAddr::from(addr))?;
        configure_tcp_socket(&socket)?;

        Ok(socket.into())
    }

    #[cfg(not(feature = "security"))]
    fn new(id: u32, host: &str, rw_timeout: Option<Duration>) -> Result<KafkaConnection> {
        KafkaConnection::from_stream(Self::new_tcp_stream(host)?, id, host, rw_timeout)
    }

    #[cfg(feature = "security")]
    fn new(
        id: u32,
        host: &str,
        rw_timeout: Option<Duration>,
        tls_config: Option<&TlsConfig>,
    ) -> Result<KafkaConnection> {
        let tcp_stream = Self::new_tcp_stream(host)?;

        let stream = match tls_config {
            Some(config) => {
                let domain = match host.rfind(':') {
                    None => host,
                    Some(i) => &host[..i],
                };
                let connector = RustlsConnector::new(config)?;
                let tls_stream = connector.connect(domain, tcp_stream)?;
                KafkaStream::Tls(tls_stream)
            }
            None => KafkaStream::Plain(tcp_stream),
        };

        KafkaConnection::from_stream(stream, id, host, rw_timeout)
    }
}
