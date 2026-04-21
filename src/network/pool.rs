use crate::error::Result;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use super::connection::KafkaConnection;
use super::{Pooled, SecurityConfig};

#[derive(Debug)]
pub struct PoolConfig {
    rw_timeout: Option<Duration>,
    idle_timeout: Duration,
    #[cfg(feature = "security")]
    security_config: Option<SecurityConfig>,
}

impl PoolConfig {
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
    conns: Vec<Pooled<KafkaConnection>>,
    host_index: HashMap<String, usize>,
    free_indices: Vec<usize>,
    state: State,
    config: PoolConfig,
}

impl Connections {
    #[cfg(not(feature = "security"))]
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
        &mut self,
        idx: usize,
        host: &str,
        now: Instant,
    ) -> Result<()> {
        let conn = &mut self.conns[idx];
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
        Ok(())
    }

    pub fn get_conn(&mut self, host: &str, now: Instant) -> Result<&mut KafkaConnection> {
        let idx = if let Some(&idx) = self.host_index.get(host) {
            idx
        } else {
            self.allocate_slot(host, now)?
        };

        self.ensure_connected(idx, host, now)?;
        Ok(&mut self.conns[idx].item)
    }

    pub fn get_conn_any(&mut self, now: Instant) -> Option<&mut KafkaConnection> {
        let mut best_idx: Option<usize> = None;
        let mut best_checkout: Option<Instant> = None;

        let hosts: Vec<String> = self.host_index.keys().cloned().collect();
        for host in &hosts {
            let idx = self.host_index[host];
            if let Err(e) = self.ensure_connected(idx, host, now) {
                warn!("Failed to reconnect to {}: {:?}", host, e);
                continue;
            }
            let conn = &self.conns[idx];
            if best_checkout.is_none()
                || conn.last_checkout < best_checkout.unwrap()
            {
                best_idx = Some(idx);
                best_checkout = Some(conn.last_checkout);
            }
        }

        best_idx.map(|idx| &mut self.conns[idx].item)
    }
}
