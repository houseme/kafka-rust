use crate::error::Result;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use super::Pooled;
#[cfg(any(feature = "security", feature = "security-ring"))]
use super::SecurityConfig;
use super::connection::KafkaConnection;

#[derive(Debug)]
pub struct PoolConfig {
    rw_timeout: Option<Duration>,
    idle_timeout: Duration,
    #[cfg(any(feature = "security", feature = "security-ring"))]
    security_config: Option<SecurityConfig>,
}

impl PoolConfig {
    #[cfg(not(any(feature = "security", feature = "security-ring")))]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        KafkaConnection::new(id, host, self.rw_timeout).map(|c| {
            debug!("Established: {:?}", c);
            c
        })
    }

    #[cfg(any(feature = "security", feature = "security-ring"))]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        KafkaConnection::new(id, host, self.rw_timeout, self.security_config.as_ref()).map(|c| {
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
    conns: Vec<Pooled<KafkaConnection>>,
    host_index: HashMap<String, usize>,
    free_indices: Vec<usize>,
    state: State,
    config: PoolConfig,
}

impl Connections {
    #[cfg(not(any(feature = "security", feature = "security-ring")))]
    pub fn new(rw_timeout: Option<Duration>, idle_timeout: Duration) -> Connections {
        Connections {
            conns: Vec::new(),
            host_index: HashMap::new(),
            free_indices: Vec::new(),
            state: State::new(),
            config: PoolConfig {
                rw_timeout,
                idle_timeout,
            },
        }
    }

    #[cfg(any(feature = "security", feature = "security-ring"))]
    pub fn new(rw_timeout: Option<Duration>, idle_timeout: Duration) -> Connections {
        Self::new_with_security(rw_timeout, idle_timeout, None)
    }

    #[cfg(any(feature = "security", feature = "security-ring"))]
    pub fn new_with_security(
        rw_timeout: Option<Duration>,
        idle_timeout: Duration,
        security: Option<SecurityConfig>,
    ) -> Connections {
        Connections {
            conns: Vec::new(),
            host_index: HashMap::new(),
            free_indices: Vec::new(),
            state: State::new(),
            config: PoolConfig {
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

    fn allocate_slot(&mut self, host: &str, now: Instant) -> Result<usize> {
        let cid = self.state.next_conn_id();
        let conn = Pooled::new(now, self.config.new_conn(cid, host)?);

        if let Some(idx) = self.free_indices.pop() {
            self.conns[idx] = conn;
            self.host_index.insert(host.to_owned(), idx);
            Ok(idx)
        } else {
            let idx = self.conns.len();
            self.conns.push(conn);
            self.host_index.insert(host.to_owned(), idx);
            Ok(idx)
        }
    }

    fn ensure_connected(
        config: &PoolConfig,
        state: &mut State,
        conn: &mut Pooled<KafkaConnection>,
        host: &str,
        now: Instant,
    ) -> Result<()> {
        let needs_reconnect = now.duration_since(conn.last_checkout) >= config.idle_timeout
            || conn.item.is_terminated();
        if needs_reconnect {
            let reason = if conn.item.is_terminated() {
                "connection terminated"
            } else {
                "idle timeout"
            };
            debug!("Reconnecting ({}) to: {:?}", reason, conn.item);
            let new_conn = config.new_conn(state.next_conn_id(), host)?;
            let _ = conn.item.shutdown();
            conn.item = new_conn;
        }
        conn.last_checkout = now;
        Ok(())
    }

    #[tracing::instrument(skip(self, now), fields(broker = %host))]
    pub fn get_conn(&mut self, host: &str, now: Instant) -> Result<&mut KafkaConnection> {
        let (idx, result) = if let Some(&idx) = self.host_index.get(host) {
            let result = Self::ensure_connected(
                &self.config,
                &mut self.state,
                &mut self.conns[idx],
                host,
                now,
            );
            (idx, result)
        } else {
            (self.allocate_slot(host, now)?, Ok(()))
        };

        #[cfg(feature = "metrics")]
        {
            crate::metrics::update_connection_count(self.conns.len());
            if let Err(ref e) = result {
                crate::metrics::record_connection_error(host, &e.to_string());
            }
        }

        result?;
        Ok(&mut self.conns[idx].item)
    }

    pub fn get_conn_any(&mut self, now: Instant) -> Option<&mut KafkaConnection> {
        let mut failed_candidate = None;
        loop {
            let (host, &idx) = self
                .host_index
                .iter()
                .filter(|(_, idx)| {
                    failed_candidate
                        .is_none_or(|failed| (self.conns[**idx].last_checkout, **idx) > failed)
                })
                .min_by_key(|(_, idx)| (self.conns[**idx].last_checkout, **idx))?;
            let candidate = (self.conns[idx].last_checkout, idx);
            match Self::ensure_connected(
                &self.config,
                &mut self.state,
                &mut self.conns[idx],
                host,
                now,
            ) {
                Ok(()) => return Some(&mut self.conns[idx].item),
                Err(error) => {
                    warn!("Failed to reconnect to {}: {:?}", host, error);
                    // Failed candidates keep their checkout time, so this boundary
                    // advances without allocating or retrying a broken broker.
                    failed_candidate = Some(candidate);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::net::{TcpListener, TcpStream};

    use super::*;

    fn listening_broker() -> (String, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        (listener.local_addr().unwrap().to_string(), listener)
    }

    fn connect(
        pool: &mut Connections,
        host: &str,
        listener: &TcpListener,
        now: Instant,
    ) -> TcpStream {
        pool.get_conn(host, now).unwrap();
        accept_connection(listener)
    }

    fn accept_connection(listener: &TcpListener) -> TcpStream {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            match listener.accept() {
                Ok((stream, _)) => return stream,
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("expected an established loopback connection: {error}"),
            }
        }
    }

    fn assert_no_new_connection(listener: &TcpListener) {
        assert_eq!(listener.accept().unwrap_err().kind(), ErrorKind::WouldBlock);
    }

    #[test]
    fn get_conn_reuses_connection_before_idle_timeout() {
        let (host, listener) = listening_broker();
        let mut pool = Connections::new(None, Duration::from_secs(60));
        let start = Instant::now();
        let _peer = connect(&mut pool, &host, &listener, start);
        let checkout = start + Duration::from_secs(1);

        assert_eq!(pool.get_conn(&host, checkout).unwrap().host(), host);

        assert_eq!(pool.state.num_conns, 1);
        assert_eq!(pool.conns[pool.host_index[&host]].last_checkout, checkout);
        assert_no_new_connection(&listener);
    }

    #[test]
    fn get_conn_any_selects_oldest_and_updates_only_its_checkout() {
        let (first_host, first_listener) = listening_broker();
        let (second_host, second_listener) = listening_broker();
        let mut pool = Connections::new(None, Duration::from_secs(60));
        let start = Instant::now();
        let _first_peer = connect(&mut pool, &first_host, &first_listener, start);
        let _second_peer = connect(
            &mut pool,
            &second_host,
            &second_listener,
            start + Duration::from_secs(1),
        );
        let first_checkout = start + Duration::from_secs(2);
        pool.get_conn(&first_host, first_checkout).unwrap();
        let checkout = start + Duration::from_secs(3);

        assert_eq!(pool.get_conn_any(checkout).unwrap().host(), second_host);

        assert_eq!(pool.state.num_conns, 2);
        assert_eq!(
            pool.conns[pool.host_index[&first_host]].last_checkout,
            first_checkout
        );
        assert_eq!(
            pool.conns[pool.host_index[&second_host]].last_checkout,
            checkout
        );
        assert_eq!(
            pool.get_conn_any(checkout + Duration::from_secs(1))
                .unwrap()
                .host(),
            first_host
        );
        assert_no_new_connection(&first_listener);
        assert_no_new_connection(&second_listener);
    }

    #[test]
    fn get_conn_any_reconnects_only_selected_idle_connection() {
        let (first_host, first_listener) = listening_broker();
        let (second_host, second_listener) = listening_broker();
        let mut pool = Connections::new(None, Duration::from_secs(10));
        let start = Instant::now();
        let _first_peer = connect(&mut pool, &first_host, &first_listener, start);
        let second_checkout = start + Duration::from_secs(1);
        let _second_peer = connect(&mut pool, &second_host, &second_listener, second_checkout);
        let checkout = start + Duration::from_secs(30);

        assert_eq!(pool.get_conn_any(checkout).unwrap().host(), first_host);

        let _replacement_peer = accept_connection(&first_listener);
        assert_eq!(pool.state.num_conns, 3);
        assert_eq!(
            pool.conns[pool.host_index[&first_host]].last_checkout,
            checkout
        );
        assert_eq!(
            pool.conns[pool.host_index[&second_host]].last_checkout,
            second_checkout
        );
        assert_no_new_connection(&second_listener);
    }

    #[test]
    fn get_conn_any_recovers_terminated_connection() {
        let (host, listener) = listening_broker();
        let mut pool = Connections::new(None, Duration::from_secs(60));
        let start = Instant::now();
        let _peer = connect(&mut pool, &host, &listener, start);
        pool.conns[pool.host_index[&host]].item.shutdown().unwrap();
        let checkout = start + Duration::from_secs(1);

        let connection = pool.get_conn_any(checkout).unwrap();

        assert_eq!(connection.host(), host);
        assert!(!connection.is_terminated());
        let _replacement_peer = accept_connection(&listener);
        assert_eq!(pool.state.num_conns, 2);
        assert_eq!(pool.conns[pool.host_index[&host]].last_checkout, checkout);
    }

    #[test]
    fn get_conn_any_falls_back_after_failed_reconnection() {
        let (first_host, first_listener) = listening_broker();
        let (second_host, second_listener) = listening_broker();
        let mut pool = Connections::new(None, Duration::from_secs(60));
        let start = Instant::now();
        let _first_peer = connect(&mut pool, &first_host, &first_listener, start);
        let _second_peer = connect(
            &mut pool,
            &second_host,
            &second_listener,
            start + Duration::from_secs(1),
        );
        pool.conns[pool.host_index[&first_host]]
            .item
            .shutdown()
            .unwrap();
        drop(first_listener);
        let checkout = start + Duration::from_secs(2);

        assert_eq!(pool.get_conn_any(checkout).unwrap().host(), second_host);

        assert_eq!(pool.state.num_conns, 3);
        assert_eq!(
            pool.conns[pool.host_index[&first_host]].last_checkout,
            start
        );
        assert_eq!(
            pool.conns[pool.host_index[&second_host]].last_checkout,
            checkout
        );
        assert_no_new_connection(&second_listener);
    }

    #[test]
    fn get_conn_any_returns_none_after_all_reconnections_fail() {
        let (first_host, first_listener) = listening_broker();
        let (second_host, second_listener) = listening_broker();
        let mut pool = Connections::new(None, Duration::from_secs(60));
        let start = Instant::now();
        let _first_peer = connect(&mut pool, &first_host, &first_listener, start);
        let _second_peer = connect(&mut pool, &second_host, &second_listener, start);
        for connection in &mut pool.conns {
            connection.item.shutdown().unwrap();
        }
        drop(first_listener);
        drop(second_listener);

        assert!(pool.get_conn_any(start + Duration::from_secs(1)).is_none());

        assert_eq!(pool.state.num_conns, 4);
        assert!(pool.conns.iter().all(|conn| conn.last_checkout == start));
    }

    #[test]
    fn get_conn_any_returns_none_for_empty_pool() {
        let mut pool = Connections::new(None, Duration::from_secs(60));

        assert!(pool.get_conn_any(Instant::now()).is_none());

        assert_eq!(pool.state.num_conns, 0);
    }

    #[test]
    fn zero_idle_timeout_creates_once_then_reconnects_on_next_checkout() {
        let (host, listener) = listening_broker();
        let mut pool = Connections::new(None, Duration::ZERO);
        let start = Instant::now();
        let _first_peer = connect(&mut pool, &host, &listener, start);

        assert_eq!(pool.state.num_conns, 1);
        assert_no_new_connection(&listener);

        let _replacement_peer = connect(&mut pool, &host, &listener, start);

        assert_eq!(pool.state.num_conns, 2);
        assert_no_new_connection(&listener);
    }
}
