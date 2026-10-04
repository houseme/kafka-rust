//! Consumer Group coordinator for managing group lifecycle.
//!
//! Handles `JoinGroup`, `SyncGroup`, Heartbeat, and `LeaveGroup` operations
//! for a consumer participating in a consumer group.

use std::collections::{BTreeSet, HashMap, HashSet};

use tracing::{debug, info, warn};

use super::assignor::{PartitionAssignor, SimpleTopicPartitions};
use crate::client::KafkaClient;
use crate::error::{Error, KafkaCode, Result};
use crate::protocol::group::{
    self, GroupAssignment, GroupMember, MemberAssignment, ProtocolMetadata,
};

/// Manages the consumer group lifecycle.
///
/// Operations are synchronous; call [`Self::heartbeat`] manually while joined.
/// Automatic heartbeat scheduling and max-poll enforcement are not implemented.
pub struct GroupCoordinator {
    client: KafkaClient,
    group_id: String,
    member_id: Option<String>,
    generation_id: Option<i32>,
    leader_id: Option<String>,
    protocol_name: Option<String>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
}

impl GroupCoordinator {
    /// Creates a new `GroupCoordinator`.
    ///
    /// Heartbeat interval and max-poll interval arguments are retained for API
    /// compatibility; scheduling and max-poll enforcement require manual handling.
    #[must_use]
    pub fn new(
        client: KafkaClient,
        group_id: String,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        _heartbeat_interval_ms: u64,
        _max_poll_interval_ms: i32,
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
        }
    }

    /// Returns the current member id assigned by the group coordinator, if any.
    #[must_use]
    pub fn member_id(&self) -> Option<&str> {
        self.member_id.as_deref()
    }

    /// Returns the current generation id for this consumer group membership, if any.
    #[must_use]
    pub fn generation_id(&self) -> Option<i32> {
        self.generation_id
    }

    /// Returns `true` if this member is the group leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.leader_id
            .as_deref()
            .zip(self.member_id.as_deref())
            .is_some_and(|(leader, member)| leader == member)
    }

    /// Returns the identifier of the consumer group managed by this coordinator.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Joins the consumer group and returns the assigned partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if coordinator lookup, join/sync requests, or assignment decoding fails.
    pub fn join_group<A: PartitionAssignor + ?Sized>(
        &mut self,
        assignor: &A,
        subscribed_topics: &[String],
    ) -> Result<MemberAssignment> {
        let metadata = group::encode_member_subscription(subscribed_topics)?;
        let coordinator_host = self.find_coordinator()?;
        let correlation_id = self.client.next_correlation_id();
        let client_id = self.client.client_id().to_owned();

        let protocols = vec![ProtocolMetadata {
            name: assignor.name().to_owned(),
            metadata,
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
            return Err(self.group_response_error(join_resp.error_code));
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
        if join_resp.protocol_name.as_deref() != Some(assignor.name()) {
            return Err(Error::Config(
                "coordinator selected an unsupported assignment protocol".into(),
            ));
        }

        info!(
            "Joined group '{}' (generation: {}, member_id: {}, leader: {})",
            self.group_id, join_resp.generation_id, join_resp.member_id, join_resp.leader_id
        );

        let correlation_id = self.client.next_correlation_id();
        let client_id = self.client.client_id().to_owned();
        let group_assignment = if self.is_leader() {
            self.compute_assignment(assignor, &join_resp.members)?
        } else {
            Vec::new()
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
            return Err(self.group_response_error(sync_resp.error_code));
        }

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
    ///
    /// # Errors
    ///
    /// Returns an error if coordinator lookup fails, heartbeat I/O fails, or broker returns a heartbeat error.
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
            return Err(self.group_response_error(resp.error_code));
        }

        debug!("Heartbeat sent to group '{}'", self.group_id);
        Ok(())
    }

    /// Leaves the consumer group gracefully.
    ///
    /// # Errors
    ///
    /// Returns an error if coordinator lookup, network I/O, or the broker leave request fails.
    pub fn leave_group(&mut self) -> Result<()> {
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
            let err = self.group_response_error(resp.error_code);
            warn!("Leave group error: {:?}", err);
            return Err(err);
        }
        info!("Left group '{}'", self.group_id);

        self.member_id = None;
        self.generation_id = None;
        self.leader_id = None;
        self.protocol_name = None;

        Ok(())
    }

    /// Re-joins the group (for rebalancing).
    ///
    /// # Errors
    ///
    /// Returns an error if leaving or re-joining the group fails.
    pub fn rejoin<A: PartitionAssignor + ?Sized>(
        &mut self,
        assignor: &A,
        subscribed_topics: &[String],
    ) -> Result<MemberAssignment> {
        self.join_group(assignor, subscribed_topics)
    }

    fn compute_assignment<A: PartitionAssignor + ?Sized>(
        &mut self,
        assignor: &A,
        members: &[GroupMember],
    ) -> Result<Vec<GroupAssignment>> {
        let mut member_lookup = HashMap::with_capacity(members.len());
        for member in members {
            if member.member_id.is_empty()
                || member_lookup
                    .insert(member.member_id.as_str(), member)
                    .is_some()
            {
                return Err(Error::codec());
            }
        }
        let member_ids: Vec<&str> = members.iter().map(|m| m.member_id.as_str()).collect();
        let member_subscriptions: Vec<(String, Vec<String>)> = members
            .iter()
            .map(|member| {
                Ok((
                    member.member_id.clone(),
                    group::decode_member_subscription_topics(&member.metadata)?,
                ))
            })
            .collect::<Result<_>>()?;
        let subscribed_topics: Vec<String> = member_subscriptions
            .iter()
            .flat_map(|(_, topics)| topics.iter().map(String::as_str))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(str::to_owned)
            .collect();
        if !subscribed_topics.is_empty() {
            self.client.load_metadata(&subscribed_topics)?;
        }
        let tp_data = self.build_topic_partitions(&subscribed_topics)?;
        let mut assignments = assignor.assign(&member_ids, &member_subscriptions, &tp_data);
        if assignments.len() != members.len() {
            return Err(Error::Config(
                "assignor must return exactly one assignment per group member".into(),
            ));
        }
        let mut assigned_members = HashSet::with_capacity(assignments.len());
        for assignment in &mut assignments {
            let member = member_lookup
                .get(assignment.member_id.as_str())
                .ok_or_else(|| Error::Config("assignor returned an unknown group member".into()))?;
            if !assigned_members.insert(assignment.member_id.clone()) {
                return Err(Error::Config(
                    "assignor returned duplicate group member assignments".into(),
                ));
            }
            MemberAssignment::from_bytes(&assignment.assignment)?;
            assignment
                .group_instance_id
                .clone_from(&member.group_instance_id);
        }
        Ok(assignments)
    }

    fn build_topic_partitions(
        &self,
        subscribed_topics: &[String],
    ) -> Result<SimpleTopicPartitions> {
        let topics: Vec<(String, Vec<i32>)> = subscribed_topics
            .iter()
            .map(|topic_name| {
                let topic_metadata = self.client.topics();
                let partitions = topic_metadata
                    .partitions(topic_name)
                    .filter(|partitions| !partitions.is_empty())
                    .ok_or(Error::Kafka(KafkaCode::UnknownTopicOrPartition))?;
                Ok((
                    topic_name.clone(),
                    partitions.iter().map(|partition| partition.id()).collect(),
                ))
            })
            .collect::<Result<_>>()?;
        Ok(SimpleTopicPartitions::new(&topics))
    }

    fn find_coordinator(&mut self) -> Result<String> {
        self.client.find_group_coordinator(&self.group_id)
    }

    fn group_response_error(&mut self, error_code: i16) -> Error {
        if matches!(
            KafkaCode::from_protocol(error_code),
            Some(
                KafkaCode::GroupLoadInProgress
                    | KafkaCode::GroupCoordinatorNotAvailable
                    | KafkaCode::NotCoordinatorForGroup
            )
        ) {
            self.client.invalidate_group_coordinator(&self.group_id);
        }
        Error::from_protocol(error_code).unwrap_or(Error::Kafka(KafkaCode::Unknown))
    }
}

#[cfg(test)]
mod tests {
    use super::super::assignor::{RangeAssignor, TopicPartitions};
    use super::*;
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::{
        ApiKey, ApiVersionsRequest, ApiVersionsResponse, FindCoordinatorRequest,
        FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
        JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, MetadataRequest,
        MetadataResponse, RequestHeader, ResponseHeader, SyncGroupRequest, SyncGroupResponse,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::time::Duration;

    struct FixedAssignor(Vec<GroupAssignment>);

    impl PartitionAssignor for FixedAssignor {
        fn name(&self) -> &'static str {
            "range"
        }
        fn assign(
            &self,
            _: &[&str],
            _: &[(String, Vec<String>)],
            _: &dyn TopicPartitions,
        ) -> Vec<GroupAssignment> {
            self.0.clone()
        }
    }

    fn assignment(member: &str, topic: Option<&str>) -> GroupAssignment {
        GroupAssignment {
            member_id: member.to_owned(),
            group_instance_id: None,
            assignment: MemberAssignment {
                version: 0,
                topic_partitions: topic
                    .into_iter()
                    .map(|topic| group::TopicAssignment {
                        topic: topic.to_owned(),
                        partitions: vec![0, 1],
                    })
                    .collect(),
                user_data: None,
            }
            .to_bytes(),
        }
    }

    fn expected_assignments() -> Vec<GroupAssignment> {
        vec![
            assignment("leader", Some("topic-a")),
            assignment("peer", Some("topic-b")),
            assignment("empty", None),
        ]
    }

    fn coordinator(address: SocketAddr) -> GroupCoordinator {
        GroupCoordinator::new(
            KafkaClient::builder()
                .with_hosts(vec![address.to_string()])
                .with_conn_rw_timeout(3)
                .build(),
            "group-test".into(),
            10_000,
            10_000,
            0,
            10_000,
        )
    }

    fn members() -> Vec<JoinGroupResponseMember> {
        [
            ("leader", vec!["topic-a".to_owned()]),
            ("peer", vec!["topic-b".to_owned()]),
            ("empty", vec![]),
        ]
        .into_iter()
        .map(|(member, topics)| {
            JoinGroupResponseMember::default()
                .with_member_id(StrBytes::from_string(member.to_owned()))
                .with_metadata(group::encode_member_subscription(&topics).unwrap())
        })
        .collect()
    }

    fn read_request<T: Decodable>(stream: &mut TcpStream, api: ApiKey) -> (RequestHeader, T) {
        let mut size = [0; 4];
        stream.read_exact(&mut size).unwrap();
        let mut payload = vec![0; usize::try_from(i32::from_be_bytes(size)).unwrap()];
        stream.read_exact(&mut payload).unwrap();
        let version = i16::from_be_bytes(payload[2..4].try_into().unwrap());
        let mut payload = Bytes::from(payload);
        let header =
            RequestHeader::decode(&mut payload, api.request_header_version(version)).unwrap();
        assert_eq!(header.request_api_key, api as i16);
        let request = T::decode(&mut payload, version).unwrap();
        assert!(payload.is_empty());
        (header, request)
    }

    fn write_response<T: Encodable + HeaderVersion>(
        stream: &mut TcpStream,
        request: &RequestHeader,
        response: &T,
    ) {
        let version = request.request_api_version;
        let mut payload = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(request.correlation_id)
            .encode(&mut payload, T::header_version(version))
            .unwrap();
        response.encode(&mut payload, version).unwrap();
        stream
            .write_all(&i32::try_from(payload.len()).unwrap().to_be_bytes())
            .unwrap();
        stream.write_all(&payload).unwrap();
    }

    fn metadata_response(address: SocketAddr, topics: &[&str]) -> MetadataResponse {
        MetadataResponse::default()
            .with_brokers(vec![
                MetadataResponseBroker::default()
                    .with_node_id(1.into())
                    .with_host(StrBytes::from_static_str("127.0.0.1"))
                    .with_port(i32::from(address.port())),
            ])
            .with_topics(
                topics
                    .iter()
                    .map(|topic| {
                        MetadataResponseTopic::default()
                            .with_name(Some(StrBytes::from_string((*topic).to_owned()).into()))
                            .with_partitions(
                                (0..2)
                                    .map(|partition| {
                                        MetadataResponsePartition::default()
                                            .with_partition_index(partition)
                                            .with_leader_id(1.into())
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            )
    }

    fn fresh_join(
        stream: &mut TcpStream,
        address: SocketAddr,
        member: &str,
        group_members: Vec<JoinGroupResponseMember>,
    ) {
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let (header, _) = read_request::<ApiVersionsRequest>(stream, ApiKey::ApiVersions);
        write_response(stream, &header, &ApiVersionsResponse::default());
        let (header, request) = read_request::<MetadataRequest>(stream, ApiKey::Metadata);
        assert!(request.topics.is_none());
        write_response(stream, &header, &metadata_response(address, &["topic-a"]));
        let (header, request) =
            read_request::<FindCoordinatorRequest>(stream, ApiKey::FindCoordinator);
        assert_eq!(request.key.as_str(), "group-test");
        assert_eq!(request.key_type, 0);
        write_response(
            stream,
            &header,
            &FindCoordinatorResponse::default()
                .with_node_id(1.into())
                .with_host(StrBytes::from_static_str("127.0.0.1"))
                .with_port(i32::from(address.port())),
        );
        let (header, request) = read_request::<JoinGroupRequest>(stream, ApiKey::JoinGroup);
        assert_eq!(request.protocol_type.as_str(), "consumer");
        assert_eq!(request.protocols.len(), 1);
        assert_eq!(request.protocols[0].name.as_str(), "range");
        assert_eq!(
            group::decode_member_subscription_topics(&request.protocols[0].metadata).unwrap(),
            ["topic-a"]
        );
        write_response(
            stream,
            &header,
            &JoinGroupResponse::default()
                .with_generation_id(10)
                .with_protocol_name(Some(StrBytes::from_static_str("range")))
                .with_leader(StrBytes::from_static_str("leader"))
                .with_member_id(StrBytes::from_string(member.to_owned()))
                .with_members(group_members),
        );
    }

    fn acknowledge_union_metadata(stream: &mut TcpStream, address: SocketAddr) {
        let (header, request) = read_request::<MetadataRequest>(stream, ApiKey::Metadata);
        let topics: Vec<_> = request
            .topics
            .unwrap()
            .into_iter()
            .map(|topic| topic.name.unwrap().to_string())
            .collect();
        assert_eq!(topics, ["topic-a", "topic-b"]);
        write_response(
            stream,
            &header,
            &metadata_response(address, &["topic-a", "topic-b"]),
        );
    }

    #[test]
    fn fresh_group_join_sends_subscriptions_and_assigns_heterogeneous_members_by_id() {
        for reversed in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                fresh_join(&mut stream, address, "leader", members());
                acknowledge_union_metadata(&mut stream, address);
                let (header, request) =
                    read_request::<SyncGroupRequest>(&mut stream, ApiKey::SyncGroup);
                assert_eq!(request.assignments.len(), 3);
                for assignment in request.assignments {
                    let decoded = MemberAssignment::from_bytes(&assignment.assignment).unwrap();
                    match assignment.member_id.as_str() {
                        "leader" => {
                            assert_eq!(decoded.topic_partitions[0].topic, "topic-a");
                            assert_eq!(decoded.topic_partitions[0].partitions, [0, 1]);
                        }
                        "peer" => {
                            assert_eq!(decoded.topic_partitions[0].topic, "topic-b");
                            assert_eq!(decoded.topic_partitions[0].partitions, [0, 1]);
                        }
                        "empty" => assert!(decoded.topic_partitions.is_empty()),
                        other => panic!("unexpected member {other}"),
                    }
                }
                write_response(
                    &mut stream,
                    &header,
                    &SyncGroupResponse::default()
                        .with_assignment(expected_assignments().remove(0).assignment),
                );
                let (header, request) =
                    read_request::<HeartbeatRequest>(&mut stream, ApiKey::Heartbeat);
                assert_eq!(request.member_id.as_str(), "leader");
                assert_eq!(request.generation_id, 10);
                write_response(&mut stream, &header, &HeartbeatResponse::default());
                let (header, request) =
                    read_request::<LeaveGroupRequest>(&mut stream, ApiKey::LeaveGroup);
                assert_eq!(request.member_id.as_str(), "leader");
                write_response(&mut stream, &header, &LeaveGroupResponse::default());
            });
            let mut coordinator = coordinator(address);
            let assignor: Box<dyn PartitionAssignor> = if reversed {
                let mut assignments = expected_assignments();
                assignments.reverse();
                Box::new(FixedAssignor(assignments))
            } else {
                Box::new(RangeAssignor)
            };
            let assignment = coordinator
                .join_group(assignor.as_ref(), &["topic-a".to_owned()])
                .unwrap();
            assert_eq!(assignment.topic_partitions[0].topic, "topic-a");
            coordinator.heartbeat().unwrap();
            coordinator.leave_group().unwrap();
            server.join().unwrap();
        }
    }

    #[test]
    fn follower_sync_sends_no_member_assignments() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            fresh_join(&mut stream, address, "follower", vec![]);
            let (header, request) =
                read_request::<SyncGroupRequest>(&mut stream, ApiKey::SyncGroup);
            assert_eq!(request.assignments, Vec::<kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment>::new());
            write_response(
                &mut stream,
                &header,
                &SyncGroupResponse::default()
                    .with_assignment(assignment("follower", Some("topic-a")).assignment),
            );
        });
        let mut coordinator = coordinator(address);
        coordinator
            .join_group(&RangeAssignor, &["topic-a".to_owned()])
            .unwrap();
        server.join().unwrap();
    }

    #[test]
    fn coordinator_errors_allow_the_next_manual_rpc_to_discover_a_new_host() {
        for leave in [false, true] {
            for code in [
                KafkaCode::GroupLoadInProgress,
                KafkaCode::GroupCoordinatorNotAvailable,
                KafkaCode::NotCoordinatorForGroup,
            ] {
                let error_code = code as i16;
                let old_listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let old_address = old_listener.local_addr().unwrap();
                let new_listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let new_address = new_listener.local_addr().unwrap();
                let old_server = std::thread::spawn(move || {
                    migration_old_broker(&old_listener, new_address, error_code, leave);
                });
                let new_server =
                    std::thread::spawn(move || migration_new_broker(&new_listener, leave));
                let mut coordinator = coordinator(old_address);
                coordinator
                    .join_group(&RangeAssignor, &["topic-a".to_owned()])
                    .unwrap();
                coordinator
                    .client
                    .find_group_coordinator("other-group")
                    .unwrap();
                let result = if leave {
                    coordinator.leave_group()
                } else {
                    coordinator.heartbeat()
                };
                assert!(matches!(result, Err(Error::Kafka(code)) if code as i16 == error_code));
                assert!(
                    coordinator
                        .client
                        .group_coordinator_host("group-test")
                        .is_none()
                );
                assert_eq!(coordinator.member_id(), Some("follower"));
                assert_eq!(coordinator.generation_id(), Some(10));
                assert_eq!(
                    coordinator.client.group_coordinator_host("other-group"),
                    Some(old_address.to_string())
                );
                if leave {
                    coordinator.leave_group().unwrap();
                } else {
                    coordinator.heartbeat().unwrap();
                    coordinator.leave_group().unwrap();
                }
                assert_eq!(
                    coordinator.client.group_coordinator_host("group-test"),
                    Some(new_address.to_string())
                );
                assert_eq!(
                    coordinator.client.group_coordinator_host("other-group"),
                    Some(old_address.to_string())
                );
                assert!(coordinator.member_id().is_none());
                old_server.join().unwrap();
                new_server.join().unwrap();
            }
        }
    }

    fn migration_old_broker(
        listener: &TcpListener,
        new_address: SocketAddr,
        error_code: i16,
        leave: bool,
    ) {
        let address = listener.local_addr().unwrap();
        let (mut stream, _) = listener.accept().unwrap();
        fresh_join(&mut stream, address, "follower", vec![]);
        let (header, _) = read_request::<SyncGroupRequest>(&mut stream, ApiKey::SyncGroup);
        write_response(
            &mut stream,
            &header,
            &SyncGroupResponse::default()
                .with_assignment(assignment("follower", Some("topic-a")).assignment),
        );
        let (header, _) = read_request::<MetadataRequest>(&mut stream, ApiKey::Metadata);
        write_response(
            &mut stream,
            &header,
            &metadata_response(address, &["topic-a"]),
        );
        let (header, request) =
            read_request::<FindCoordinatorRequest>(&mut stream, ApiKey::FindCoordinator);
        assert_eq!(request.key.as_str(), "other-group");
        write_response(
            &mut stream,
            &header,
            &FindCoordinatorResponse::default()
                .with_node_id(1.into())
                .with_host(StrBytes::from_static_str("127.0.0.1"))
                .with_port(i32::from(address.port())),
        );
        if leave {
            let (header, _) = read_request::<LeaveGroupRequest>(&mut stream, ApiKey::LeaveGroup);
            write_response(
                &mut stream,
                &header,
                &LeaveGroupResponse::default().with_error_code(error_code),
            );
        } else {
            let (header, _) = read_request::<HeartbeatRequest>(&mut stream, ApiKey::Heartbeat);
            write_response(
                &mut stream,
                &header,
                &HeartbeatResponse::default().with_error_code(error_code),
            );
        }
        let (header, _) = read_request::<MetadataRequest>(&mut stream, ApiKey::Metadata);
        // Only a new broker is listed: another group's existing BrokerRef must
        // remain attached to its old broker until that group reports a change.
        let metadata = metadata_response(address, &["topic-a"]).with_brokers(vec![
            MetadataResponseBroker::default()
                .with_node_id(2.into())
                .with_host(StrBytes::from_static_str("127.0.0.1"))
                .with_port(i32::from(new_address.port())),
        ]);
        write_response(&mut stream, &header, &metadata);
        let (header, request) =
            read_request::<FindCoordinatorRequest>(&mut stream, ApiKey::FindCoordinator);
        assert_eq!(request.key.as_str(), "group-test");
        write_response(
            &mut stream,
            &header,
            &FindCoordinatorResponse::default()
                .with_node_id(2.into())
                .with_host(StrBytes::from_static_str("127.0.0.1"))
                .with_port(i32::from(new_address.port())),
        );
    }

    fn migration_new_broker(listener: &TcpListener, leave: bool) {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        if !leave {
            let (header, request) =
                read_request::<HeartbeatRequest>(&mut stream, ApiKey::Heartbeat);
            assert_eq!(request.member_id.as_str(), "follower");
            assert_eq!(request.generation_id, 10);
            write_response(&mut stream, &header, &HeartbeatResponse::default());
        }
        let (header, request) = read_request::<LeaveGroupRequest>(&mut stream, ApiKey::LeaveGroup);
        assert_eq!(request.member_id.as_str(), "follower");
        write_response(&mut stream, &header, &LeaveGroupResponse::default());
    }

    #[test]
    fn malformed_members_or_assignor_results_fail_before_sync() {
        for failure in ["metadata", "unknown", "duplicate", "missing", "payload"] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let (check_tx, check_rx) = std::sync::mpsc::channel();
            let (checked_tx, checked_rx) = std::sync::mpsc::channel();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let mut group_members = members();
                if failure == "metadata" {
                    group_members[1].metadata = Bytes::new();
                }
                fresh_join(&mut stream, address, "leader", group_members);
                if failure != "metadata" {
                    acknowledge_union_metadata(&mut stream, address);
                }
                check_rx.recv().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_millis(100)))
                    .unwrap();
                let mut byte = [0];
                let error = stream.peek(&mut byte).unwrap_err();
                assert!(matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ));
                checked_tx.send(()).unwrap();
            });
            let mut assignments = expected_assignments();
            match failure {
                "unknown" => assignments[1].member_id = "unknown".to_owned(),
                "duplicate" => assignments[1].member_id = "leader".to_owned(),
                "missing" => {
                    assignments.pop();
                }
                "payload" => assignments[1].assignment = Bytes::new(),
                _ => {}
            }
            let mut coordinator = coordinator(address);
            let result =
                coordinator.join_group(&FixedAssignor(assignments), &["topic-a".to_owned()]);
            assert!(
                result.is_err(),
                "{failure} must reject the join before SyncGroup"
            );
            check_tx.send(()).unwrap();
            checked_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            server.join().unwrap();
        }
    }

    #[test]
    fn custom_member_ids_preserve_the_corresponding_group_instance_id() {
        let client = KafkaClient::new(vec![]);
        let mut coordinator =
            GroupCoordinator::new(client, "group-test".into(), 10_000, 10_000, 5, 10_000);
        let metadata = group::encode_member_subscription(&[]).unwrap();
        let members = vec![
            GroupMember {
                member_id: "leader".into(),
                group_instance_id: Some("instance-a".into()),
                metadata: metadata.clone(),
            },
            GroupMember {
                member_id: "peer".into(),
                group_instance_id: Some("instance-b".into()),
                metadata,
            },
        ];
        let assignments = coordinator
            .compute_assignment(
                &FixedAssignor(vec![assignment("peer", None), assignment("leader", None)]),
                &members,
            )
            .unwrap();
        assert_eq!(assignments[0].member_id, "peer");
        assert_eq!(
            assignments[0].group_instance_id.as_deref(),
            Some("instance-b")
        );
        assert_eq!(
            assignments[1].group_instance_id.as_deref(),
            Some("instance-a")
        );
    }

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
