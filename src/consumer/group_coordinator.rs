//! Consumer Group coordinator for managing group lifecycle.
//!
//! Handles `JoinGroup`, `SyncGroup`, Heartbeat, and `LeaveGroup` operations
//! for a consumer participating in a consumer group.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use tracing::{debug, info, warn};

use super::assignor::{PartitionAssignor, SimpleTopicPartitions};
use crate::client::KafkaClient;
use crate::error::{Error, KafkaCode, Result};
use crate::protocol::group::{
    self, GroupAssignment, GroupMember, MemberAssignment, ProtocolMetadata,
};

/// Manages the consumer group lifecycle.
pub struct GroupCoordinator {
    client: KafkaClient,
    group_id: String,
    member_id: Option<String>,
    generation_id: Option<i32>,
    leader_id: Option<String>,
    protocol_name: Option<String>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    heartbeat_interval_ms: u64,
    heartbeat_thread: Option<thread::JoinHandle<()>>,
    heartbeat_stop: Arc<AtomicBool>,
    max_poll_interval_ms: i32,
}

impl GroupCoordinator {
    /// Creates a new `GroupCoordinator`.
    #[must_use]
    pub fn new(
        client: KafkaClient,
        group_id: String,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        heartbeat_interval_ms: u64,
        max_poll_interval_ms: i32,
    ) -> Self {
        GroupCoordinator {
            client,
            group_id,
            member_id: None,
            generation_id: None,
            leader_id: None,
            protocol_name: None,
            session_timeout_ms,
            rebalance_timeout_ms,
            heartbeat_interval_ms,
            heartbeat_thread: None,
            heartbeat_stop: Arc::new(AtomicBool::new(false)),
            max_poll_interval_ms,
        }
    }

    #[must_use]
    pub fn member_id(&self) -> Option<&str> {
        self.member_id.as_deref()
    }

    #[must_use]
    pub fn generation_id(&self) -> Option<i32> {
        self.generation_id
    }

    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.leader_id
            .as_deref()
            .zip(self.member_id.as_deref())
            .is_some_and(|(leader, member)| leader == member)
    }

    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Joins the consumer group and returns the assigned partitions.
    pub fn join_group<A: PartitionAssignor + ?Sized>(
        &mut self,
        assignor: &A,
        subscribed_topics: &[String],
    ) -> Result<MemberAssignment> {
        let coordinator_host = self.find_coordinator()?;
        let correlation_id = self.client.next_correlation_id();
        let client_id = self.client.client_id().to_owned();

        let protocols = vec![ProtocolMetadata {
            name: assignor.name().to_owned(),
            metadata: Vec::new(),
        }];

        debug!(
            "Joining group '{}' (coordinator: {}, session_timeout: {}ms)",
            self.group_id, coordinator_host, self.session_timeout_ms
        );

        let mid = self.member_id.as_deref().unwrap_or("");
        let join_resp = group::fetch_join_group(
            self.client.get_conn_mut(&coordinator_host)?,
            correlation_id,
            &client_id,
            &self.group_id,
            self.session_timeout_ms,
            self.rebalance_timeout_ms,
            mid,
            None,
            "consumer",
            &protocols,
        )?;

        if join_resp.error_code != 0 {
            let err = Error::from_protocol(join_resp.error_code)
                .unwrap_or(Error::Kafka(KafkaCode::Unknown));
            return Err(err);
        }

        self.member_id = Some(join_resp.member_id.clone());
        self.generation_id = Some(join_resp.generation_id);
        self.leader_id = Some(join_resp.leader_id.clone());
        self.protocol_name.clone_from(&join_resp.protocol_name);
        if let Some(protocol_type) = join_resp.protocol_type.as_deref()
            && protocol_type != "consumer"
        {
            return Err(Error::Config(format!(
                "unsupported group protocol type: {protocol_type}"
            )));
        }

        info!(
            "Joined group '{}' (generation: {}, member_id: {}, leader: {})",
            self.group_id, join_resp.generation_id, join_resp.member_id, join_resp.leader_id
        );

        let correlation_id = self.client.next_correlation_id();
        let client_id = self.client.client_id().to_owned();
        let group_assignment = if self.is_leader() {
            self.compute_assignment(assignor, &join_resp.members, subscribed_topics)
        } else {
            vec![GroupAssignment {
                member_id: self.member_id.clone().unwrap_or_default(),
                group_instance_id: None,
                assignment: Vec::new(),
            }]
        };

        let sync_resp = group::fetch_sync_group(
            self.client.get_conn_mut(&coordinator_host)?,
            correlation_id,
            &client_id,
            &self.group_id,
            join_resp.generation_id,
            &join_resp.member_id,
            None,
            &group_assignment,
        )?;

        if sync_resp.error_code != 0 {
            let err = Error::from_protocol(sync_resp.error_code)
                .unwrap_or(Error::Kafka(KafkaCode::Unknown));
            return Err(err);
        }

        self.start_heartbeat_thread();

        let assignment = MemberAssignment::from_bytes(&sync_resp.assignment)?;
        info!(
            "Synced group '{}': assigned {:?}",
            self.group_id,
            assignment
                .topic_partitions
                .iter()
                .map(|tp| format!("{}[{:?}]", tp.topic, tp.partitions))
                .collect::<Vec<_>>()
        );

        Ok(assignment)
    }

    /// Sends a heartbeat to the group coordinator.
    pub fn heartbeat(&mut self) -> Result<()> {
        let coordinator_host = self.find_coordinator()?;
        let correlation_id = self.client.next_correlation_id();
        let client_id = self.client.client_id().to_owned();

        let resp = group::fetch_heartbeat(
            self.client.get_conn_mut(&coordinator_host)?,
            correlation_id,
            &client_id,
            &self.group_id,
            self.generation_id.unwrap_or(0),
            self.member_id.as_deref().unwrap_or(""),
            None,
        )?;

        if resp.error_code != 0 {
            if resp.error_code == KafkaCode::RebalanceInProgress as i16 {
                warn!("Heartbeat: rebalance in progress");
            }
            let err =
                Error::from_protocol(resp.error_code).unwrap_or(Error::Kafka(KafkaCode::Unknown));
            return Err(err);
        }

        debug!("Heartbeat sent to group '{}'", self.group_id);
        Ok(())
    }

    /// Leaves the consumer group gracefully.
    pub fn leave_group(&mut self) -> Result<()> {
        self.stop_heartbeat_thread();

        if self.member_id.is_none() {
            return Ok(());
        }

        let coordinator_host = self.find_coordinator()?;
        let correlation_id = self.client.next_correlation_id();
        let client_id = self.client.client_id().to_owned();

        let leave_members = vec![group::LeaveMemberRequest {
            member_id: self.member_id.clone().unwrap_or_default(),
            group_instance_id: None,
        }];

        let resp = group::fetch_leave_group(
            self.client.get_conn_mut(&coordinator_host)?,
            correlation_id,
            &client_id,
            &self.group_id,
            &leave_members,
        )?;

        if resp.error_code != 0 {
            let err =
                Error::from_protocol(resp.error_code).unwrap_or(Error::Kafka(KafkaCode::Unknown));
            warn!("Leave group error: {:?}", err);
        } else {
            info!("Left group '{}'", self.group_id);
        }

        self.member_id = None;
        self.generation_id = None;
        self.leader_id = None;
        self.protocol_name = None;

        Ok(())
    }

    /// Re-joins the group (for rebalancing).
    pub fn rejoin<A: PartitionAssignor + ?Sized>(
        &mut self,
        assignor: &A,
        subscribed_topics: &[String],
    ) -> Result<MemberAssignment> {
        self.stop_heartbeat_thread();
        self.join_group(assignor, subscribed_topics)
    }

    fn compute_assignment<A: PartitionAssignor + ?Sized>(
        &self,
        assignor: &A,
        members: &[GroupMember],
        subscribed_topics: &[String],
    ) -> Vec<GroupAssignment> {
        let tp_data = self.build_topic_partitions(subscribed_topics);

        let member_ids: Vec<&str> = members.iter().map(|m| m.member_id.as_str()).collect();
        let member_subscriptions: Vec<(String, Vec<String>)> = members
            .iter()
            .map(|m| {
                let topics = decode_member_subscription_topics(&m.metadata)
                    .filter(|topics| !topics.is_empty())
                    .unwrap_or_else(|| subscribed_topics.to_vec());
                (m.member_id.clone(), topics)
            })
            .collect();

        let assignments = assignor.assign(&member_ids, &member_subscriptions, &tp_data);

        members
            .iter()
            .zip(assignments)
            .map(|(member, ga)| GroupAssignment {
                member_id: member.member_id.clone(),
                group_instance_id: member.group_instance_id.clone(),
                assignment: ga.assignment,
            })
            .collect()
    }

    fn build_topic_partitions(&self, subscribed_topics: &[String]) -> SimpleTopicPartitions {
        let topics: Vec<(String, Vec<i32>)> = subscribed_topics
            .iter()
            .filter_map(|topic_name| {
                self.client.topics().partitions(topic_name).map(|tp| {
                    let ps: Vec<i32> = tp.iter().map(|p| p.id()).collect();
                    (topic_name.clone(), ps)
                })
            })
            .collect();
        SimpleTopicPartitions::new(&topics)
    }

    fn find_coordinator(&mut self) -> Result<String> {
        if let Some(host) = self.client.group_coordinator_host(&self.group_id) {
            return Ok(host);
        }

        self.client.load_metadata_all()?;

        self.client
            .group_coordinator_host(&self.group_id)
            .ok_or(Error::Kafka(KafkaCode::GroupCoordinatorNotAvailable))
    }

    #[allow(clippy::disallowed_methods)]
    fn start_heartbeat_thread(&mut self) {
        self.stop_heartbeat_thread();

        self.heartbeat_stop.store(false, Ordering::Relaxed);
        let stop = Arc::clone(&self.heartbeat_stop);
        let interval_ms = self.heartbeat_interval_ms;
        let max_poll_interval_ms = u64::try_from(self.max_poll_interval_ms).unwrap_or(0);
        let effective_interval_ms = if max_poll_interval_ms > 0 {
            let max_heartbeat = std::cmp::max(1, max_poll_interval_ms / 3);
            interval_ms.min(max_heartbeat)
        } else {
            interval_ms
        };
        let group_id = self.group_id.clone();

        let handle = thread::spawn(move || {
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }

                let sleep_dur = Duration::from_millis(effective_interval_ms);
                thread::sleep(sleep_dur);

                if stop.load(Ordering::Relaxed) {
                    break;
                }

                debug!(
                    "Heartbeat thread tick for group '{}' (interval: {}ms, max_poll_interval: {}ms)",
                    group_id, effective_interval_ms, max_poll_interval_ms
                );
            }
        });

        self.heartbeat_thread = Some(handle);
    }

    fn stop_heartbeat_thread(&mut self) {
        self.heartbeat_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.heartbeat_thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for GroupCoordinator {
    fn drop(&mut self) {
        self.stop_heartbeat_thread();
    }
}

fn decode_member_subscription_topics(metadata: &[u8]) -> Option<Vec<String>> {
    if metadata.len() < 6 {
        return None;
    }
    let mut i = 0usize;
    i += 2; // version
    let topic_count = i32::from_be_bytes([
        metadata[i],
        metadata[i + 1],
        metadata[i + 2],
        metadata[i + 3],
    ]);
    if topic_count < 0 {
        return None;
    }
    i += 4;

    let mut topics = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        if i + 2 > metadata.len() {
            return None;
        }
        let len = i16::from_be_bytes([metadata[i], metadata[i + 1]]);
        i += 2;
        if len < 0 {
            return None;
        }
        let len = len as usize;
        if i + len > metadata.len() {
            return None;
        }
        let topic = std::str::from_utf8(&metadata[i..i + len]).ok()?.to_owned();
        i += len;
        topics.push(topic);
    }
    Some(topics)
}

#[cfg(test)]
mod tests {
    use super::super::assignor::TopicPartitions;
    use super::*;

    #[test]
    fn test_simple_topic_partitions_basic() {
        let tp = SimpleTopicPartitions::new(&[
            ("t1".to_owned(), vec![0, 1, 2]),
            ("t2".to_owned(), vec![0, 1]),
        ]);

        let mut topics = tp.topics();
        topics.sort_unstable();
        assert_eq!(topics, vec!["t1", "t2"]);

        assert_eq!(tp.partitions_for("t1"), vec![0, 1, 2]);
        assert_eq!(tp.partitions_for("t2"), vec![0, 1]);
        assert_eq!(tp.partitions_for("unknown"), Vec::<i32>::new());
    }
}
