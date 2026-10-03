//! Consumer Group protocol implementations.
//!
//! Provides request builders and response parsers for the four core
//! consumer group management protocols: `JoinGroup`, `SyncGroup`, Heartbeat,
//! and `LeaveGroup`.

use bytes::{Buf, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader};
use kafka_protocol::protocol::Encodable;
use kafka_protocol::protocol::StrBytes;

use crate::error::{Error, Result};
use crate::network::KafkaConnection;

pub const API_KEY_JOIN_GROUP: i16 = ApiKey::JoinGroup as i16;
pub const API_KEY_SYNC_GROUP: i16 = ApiKey::SyncGroup as i16;
pub const API_KEY_HEARTBEAT: i16 = ApiKey::Heartbeat as i16;
pub const API_KEY_LEAVE_GROUP: i16 = ApiKey::LeaveGroup as i16;

pub const API_VERSION_JOIN_GROUP: i16 = 1;
pub const API_VERSION_SYNC_GROUP: i16 = 1;
pub const API_VERSION_HEARTBEAT: i16 = 1;
pub const API_VERSION_LEAVE_GROUP: i16 = 1;

// --------------------------------------------------------------------
// Shared encoding helpers
// --------------------------------------------------------------------

fn encode_string(buf: &mut BytesMut, s: &str) {
    let len = crate::protocol::usize_to_i16(s.len())
        .expect("Kafka string length must fit in i16 for protocol encoding");
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn encode_bytes(buf: &mut BytesMut, data: &[u8]) {
    let len = crate::protocol::usize_to_i32(data.len())
        .expect("Kafka bytes length must fit in i32 for protocol encoding");
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(data);
}

fn decode_string(bytes: &mut bytes::Bytes) -> Result<String> {
    if bytes.len() < 2 {
        return Err(Error::codec());
    }
    let len = crate::protocol::non_negative_i16_to_usize(i16::from_be_bytes([bytes[0], bytes[1]]))?;
    bytes.advance(2);
    if bytes.len() < len {
        return Err(Error::codec());
    }
    let s = std::str::from_utf8(&bytes[..len])
        .map_err(|_| Error::codec())?
        .to_owned();
    bytes.advance(len);
    Ok(s)
}

fn build_frame(header: &RequestHeader, body: &[u8], api_version: i16) -> Result<bytes::Bytes> {
    let mut header_buf = BytesMut::new();
    let header_version = ApiKey::try_from(header.request_api_key)
        .map_err(|()| Error::codec())?
        .request_header_version(api_version);
    header
        .encode(&mut header_buf, header_version)
        .map_err(|_| Error::codec())?;

    let total_len = crate::protocol::usize_to_i32(header_buf.len() + body.len())?;
    let out_len = crate::protocol::non_negative_i32_to_usize(total_len)?;
    let mut out = BytesMut::with_capacity(4 + out_len);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(body);

    Ok(out.freeze())
}

// --------------------------------------------------------------------
// JoinGroup
// --------------------------------------------------------------------

/// Metadata about a protocol supported by a group member.
#[derive(Debug, Clone)]
pub struct ProtocolMetadata {
    pub name: String,
    pub metadata: bytes::Bytes,
}

/// A member of a consumer group.
#[derive(Debug, Clone)]
pub struct GroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: bytes::Bytes,
}

/// Parsed response from a `JoinGroup` request.
#[derive(Debug, Clone)]
pub struct JoinGroupResponseData {
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader_id: String,
    pub member_id: String,
    pub members: Vec<GroupMember>,
}

/// Build a `JoinGroup` request (v1).
#[allow(clippy::too_many_arguments)]
pub fn build_join_group_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    member_id: &str,
    _group_instance_id: Option<&str>,
    protocol_type: &str,
    protocols: &[ProtocolMetadata],
) -> Result<bytes::Bytes> {
    let version = API_VERSION_JOIN_GROUP;
    let mut body = BytesMut::new();

    encode_string(&mut body, group_id);
    body.extend_from_slice(&session_timeout_ms.to_be_bytes());
    body.extend_from_slice(&rebalance_timeout_ms.to_be_bytes());
    encode_string(&mut body, member_id);
    encode_string(&mut body, protocol_type);
    body.extend_from_slice(&crate::protocol::usize_to_i32(protocols.len())?.to_be_bytes());
    for p in protocols {
        encode_string(&mut body, &p.name);
        encode_bytes(&mut body, &p.metadata);
    }

    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_JOIN_GROUP)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    build_frame(&header, &body, version)
}

/// Send a `JoinGroup` request and parse the response.
#[allow(clippy::too_many_arguments)]
pub fn fetch_join_group(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
    protocol_type: &str,
    protocols: &[ProtocolMetadata],
) -> Result<JoinGroupResponseData> {
    let version = API_VERSION_JOIN_GROUP;
    let request_bytes = build_join_group_request(
        correlation_id,
        client_id,
        group_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        member_id,
        group_instance_id,
        protocol_type,
        protocols,
    )?;
    conn.send_request(&request_bytes, correlation_id, version)?;
    let response = conn.read_response::<kafka_protocol::messages::JoinGroupResponse>(version)?;
    Ok(JoinGroupResponseData {
        error_code: response.error_code,
        generation_id: response.generation_id,
        protocol_type: response.protocol_type.map(|value| value.to_string()),
        protocol_name: response.protocol_name.map(|value| value.to_string()),
        leader_id: response.leader.to_string(),
        member_id: response.member_id.to_string(),
        members: response
            .members
            .into_iter()
            .map(|member| GroupMember {
                member_id: member.member_id.to_string(),
                group_instance_id: member.group_instance_id.map(|value| value.to_string()),
                metadata: member.metadata,
            })
            .collect(),
    })
}

// --------------------------------------------------------------------
// SyncGroup
// --------------------------------------------------------------------

/// Parsed response from a `SyncGroup` request.
#[derive(Debug, Clone)]
pub struct SyncGroupResponseData {
    pub error_code: i16,
    pub assignment: bytes::Bytes,
}

/// Assignment for a specific group member (used by leader in `SyncGroup`).
#[derive(Debug, Clone)]
pub struct GroupAssignment {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub assignment: bytes::Bytes,
}

/// Build a `SyncGroup` request (v1).
pub fn build_sync_group_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    _group_instance_id: Option<&str>,
    group_assignment: &[GroupAssignment],
) -> Result<bytes::Bytes> {
    let version = API_VERSION_SYNC_GROUP;
    let mut body = BytesMut::new();

    encode_string(&mut body, group_id);
    body.extend_from_slice(&generation_id.to_be_bytes());
    encode_string(&mut body, member_id);
    body.extend_from_slice(&crate::protocol::usize_to_i32(group_assignment.len())?.to_be_bytes());
    for ga in group_assignment {
        encode_string(&mut body, &ga.member_id);
        encode_bytes(&mut body, &ga.assignment);
    }

    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_SYNC_GROUP)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    build_frame(&header, &body, version)
}

/// Send a `SyncGroup` request and parse the response.
#[allow(clippy::too_many_arguments)]
pub fn fetch_sync_group(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
    group_assignment: &[GroupAssignment],
) -> Result<SyncGroupResponseData> {
    let version = API_VERSION_SYNC_GROUP;
    let request_bytes = build_sync_group_request(
        correlation_id,
        client_id,
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        group_assignment,
    )?;
    conn.send_request(&request_bytes, correlation_id, version)?;
    let response = conn.read_response::<kafka_protocol::messages::SyncGroupResponse>(version)?;
    Ok(SyncGroupResponseData {
        error_code: response.error_code,
        assignment: response.assignment,
    })
}

// --------------------------------------------------------------------
// Heartbeat
// --------------------------------------------------------------------

/// Parsed response from a Heartbeat request.
#[derive(Debug, Clone)]
pub struct HeartbeatResponseData {
    pub error_code: i16,
}

/// Build a Heartbeat request (v1).
pub fn build_heartbeat_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    _group_instance_id: Option<&str>,
) -> Result<bytes::Bytes> {
    let version = API_VERSION_HEARTBEAT;
    let mut body = BytesMut::new();

    encode_string(&mut body, group_id);
    body.extend_from_slice(&generation_id.to_be_bytes());
    encode_string(&mut body, member_id);

    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_HEARTBEAT)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    build_frame(&header, &body, version)
}

/// Send a Heartbeat request and parse the response.
pub fn fetch_heartbeat(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> Result<HeartbeatResponseData> {
    let version = API_VERSION_HEARTBEAT;
    let request_bytes = build_heartbeat_request(
        correlation_id,
        client_id,
        group_id,
        generation_id,
        member_id,
        group_instance_id,
    )?;
    conn.send_request(&request_bytes, correlation_id, version)?;
    let response = conn.read_response::<kafka_protocol::messages::HeartbeatResponse>(version)?;
    Ok(HeartbeatResponseData {
        error_code: response.error_code,
    })
}

// --------------------------------------------------------------------
// LeaveGroup
// --------------------------------------------------------------------

/// Parsed response from a `LeaveGroup` request.
#[derive(Debug, Clone)]
pub struct LeaveGroupResponseData {
    pub error_code: i16,
}

/// Member leave request data.
#[derive(Debug, Clone)]
pub struct LeaveMemberRequest {
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

/// Build a `LeaveGroup` request (v1).
pub fn build_leave_group_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    members: &[LeaveMemberRequest],
) -> Result<bytes::Bytes> {
    if members.is_empty() {
        return Err(Error::Config(
            "leave-group requires at least one member".into(),
        ));
    }
    if members.iter().any(|m| m.group_instance_id.is_some()) {
        return Err(Error::Config(
            "group_instance_id in LeaveGroup is not supported for protocol v1".into(),
        ));
    }

    let version = API_VERSION_LEAVE_GROUP;
    let mut body = BytesMut::new();

    encode_string(&mut body, group_id);
    encode_string(&mut body, &members[0].member_id);

    let header = RequestHeader::default()
        .with_request_api_key(API_KEY_LEAVE_GROUP)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    build_frame(&header, &body, version)
}

/// Send a `LeaveGroup` request and parse the response.
pub fn fetch_leave_group(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    members: &[LeaveMemberRequest],
) -> Result<LeaveGroupResponseData> {
    let version = API_VERSION_LEAVE_GROUP;
    let request_bytes = build_leave_group_request(correlation_id, client_id, group_id, members)?;
    conn.send_request(&request_bytes, correlation_id, version)?;
    let response = conn.read_response::<kafka_protocol::messages::LeaveGroupResponse>(version)?;
    Ok(LeaveGroupResponseData {
        error_code: response.error_code,
    })
}

// --------------------------------------------------------------------
// Assignment encoding/decoding
// --------------------------------------------------------------------

/// Represents a partition assignment for a topic.
#[derive(Debug, Clone)]
pub struct TopicAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

/// Represents the full assignment for a consumer group member.
#[derive(Debug, Clone)]
pub struct MemberAssignment {
    pub version: i16,
    pub topic_partitions: Vec<TopicAssignment>,
    pub user_data: Option<Vec<u8>>,
}

impl MemberAssignment {
    /// Decode a member assignment from raw bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut bytes = bytes::Bytes::from(data.to_vec());
        if bytes.len() < 2 {
            return Err(Error::codec());
        }
        let version = i16::from_be_bytes([bytes[0], bytes[1]]);
        bytes.advance(2);

        let num_topics = if bytes.len() < 4 {
            return Err(Error::codec());
        } else {
            let v = crate::protocol::non_negative_i32_to_usize(i32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ]))?;
            bytes.advance(4);
            v
        };

        let mut topic_partitions = Vec::with_capacity(num_topics);
        for _ in 0..num_topics {
            let topic = decode_string(&mut bytes)?;
            let num_partitions = if bytes.len() < 4 {
                return Err(Error::codec());
            } else {
                let v = crate::protocol::non_negative_i32_to_usize(i32::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                ]))?;
                bytes.advance(4);
                v
            };
            let mut partitions = Vec::with_capacity(num_partitions);
            for _ in 0..num_partitions {
                if bytes.len() < 4 {
                    return Err(Error::codec());
                }
                let p = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                bytes.advance(4);
                partitions.push(p);
            }
            topic_partitions.push(TopicAssignment { topic, partitions });
        }

        let user_data = if !bytes.is_empty() && bytes.len() >= 4 {
            let len = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            bytes.advance(4);
            if len >= 0 {
                let len = crate::protocol::non_negative_i32_to_usize(len)?;
                if bytes.len() >= len {
                    Some(bytes[..len].to_vec())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(MemberAssignment {
            version,
            topic_partitions,
            user_data,
        })
    }

    /// Encode this member assignment to raw bytes.
    pub fn to_bytes(&self) -> bytes::Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.version.to_be_bytes());
        let topic_count = crate::protocol::usize_to_i32(self.topic_partitions.len())
            .expect("topic count must fit in i32 for protocol encoding");
        buf.extend_from_slice(&topic_count.to_be_bytes());
        for ta in &self.topic_partitions {
            encode_string(&mut buf, &ta.topic);
            let partition_count = crate::protocol::usize_to_i32(ta.partitions.len())
                .expect("partition count must fit in i32 for protocol encoding");
            buf.extend_from_slice(&partition_count.to_be_bytes());
            for &p in &ta.partitions {
                buf.extend_from_slice(&p.to_be_bytes());
            }
        }
        if let Some(ref ud) = self.user_data {
            encode_bytes(&mut buf, ud);
        } else {
            buf.extend_from_slice(&(-1i32).to_be_bytes());
        }
        buf.freeze()
    }
}

// --------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::{
        HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
        LeaveGroupRequest, LeaveGroupResponse, ResponseHeader, SyncGroupRequest, SyncGroupResponse,
    };
    use kafka_protocol::protocol::{Decodable, HeaderVersion};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    #[test]
    fn test_member_assignment_roundtrip() {
        let assignment = MemberAssignment {
            version: 1,
            topic_partitions: vec![
                TopicAssignment {
                    topic: "test-topic".to_owned(),
                    partitions: vec![0, 1, 2],
                },
                TopicAssignment {
                    topic: "other-topic".to_owned(),
                    partitions: vec![0],
                },
            ],
            user_data: None,
        };

        let bytes = assignment.to_bytes();
        let decoded = MemberAssignment::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.topic_partitions.len(), 2);
        assert_eq!(decoded.topic_partitions[0].topic, "test-topic");
        assert_eq!(decoded.topic_partitions[0].partitions, vec![0, 1, 2]);
        assert_eq!(decoded.topic_partitions[1].topic, "other-topic");
        assert_eq!(decoded.topic_partitions[1].partitions, vec![0]);
        assert!(decoded.user_data.is_none());
    }

    #[test]
    fn test_member_assignment_with_user_data() {
        let assignment = MemberAssignment {
            version: 1,
            topic_partitions: vec![TopicAssignment {
                topic: "t".to_owned(),
                partitions: vec![0],
            }],
            user_data: Some(b"custom-data".to_vec()),
        };

        let bytes = assignment.to_bytes();
        let decoded = MemberAssignment::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.user_data, Some(b"custom-data".to_vec()));
    }

    #[test]
    fn test_member_assignment_empty() {
        let assignment = MemberAssignment {
            version: 1,
            topic_partitions: vec![],
            user_data: None,
        };

        let bytes = assignment.to_bytes();
        let decoded = MemberAssignment::from_bytes(&bytes).unwrap();
        assert!(decoded.topic_partitions.is_empty());
    }

    #[test]
    fn test_build_join_group_request() {
        let protocols = vec![ProtocolMetadata {
            name: "range".to_owned(),
            metadata: vec![0, 1, 2].into(),
        }];
        let req = build_join_group_request(
            1, "client", "group", 10000, 300_000, "", None, "consumer", &protocols,
        );
        assert!(
            req.is_ok(),
            "build_join_group_request failed: {:?}",
            req.err()
        );
        assert!(req.unwrap().len() > 4);
    }

    #[test]
    fn test_build_sync_group_request() {
        let assignments = vec![GroupAssignment {
            member_id: "member-1".to_owned(),
            group_instance_id: None,
            assignment: vec![0, 1].into(),
        }];
        let req = build_sync_group_request(1, "client", "group", 1, "member-1", None, &assignments);
        assert!(req.is_ok());
    }

    #[test]
    fn test_build_heartbeat_request() {
        let req = build_heartbeat_request(1, "client", "group", 1, "member-1", None);
        assert!(req.is_ok());
    }

    #[test]
    fn test_build_leave_group_request() {
        let members = vec![LeaveMemberRequest {
            member_id: "member-1".to_owned(),
            group_instance_id: None,
        }];
        let req = build_leave_group_request(1, "client", "group", &members);
        assert!(req.is_ok());
    }

    fn group_response<R: Encodable + HeaderVersion>(correlation: i32, body: &R) -> BytesMut {
        let mut bytes = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(correlation)
            .encode(&mut bytes, R::header_version(1))
            .unwrap();
        body.encode(&mut bytes, 1).unwrap();
        bytes
    }

    fn group_request_reply(key: ApiKey, correlation: i32, bytes: &mut bytes::Bytes) -> BytesMut {
        let reply = match key {
            ApiKey::JoinGroup => {
                let request = JoinGroupRequest::decode(bytes, 1).unwrap();
                assert_eq!(request.protocol_type.as_str(), "consumer");
                group_response(correlation, &JoinGroupResponse::default()
                    .with_generation_id(7)
                    .with_protocol_name(Some(StrBytes::from_static_str("range")))
                    .with_leader(StrBytes::from_static_str("leader"))
                    .with_member_id(StrBytes::from_static_str("member"))
                    .with_members(vec![kafka_protocol::messages::join_group_response::JoinGroupResponseMember::default()
                        .with_member_id(StrBytes::from_static_str("member"))
                        .with_metadata(bytes::Bytes::from_static(b"metadata"))]))
            }
            ApiKey::SyncGroup => {
                SyncGroupRequest::decode(bytes, 1).unwrap();
                group_response(
                    correlation,
                    &SyncGroupResponse::default()
                        .with_throttle_time_ms(37)
                        .with_error_code(25)
                        .with_assignment(bytes::Bytes::from_static(b"assignment")),
                )
            }
            ApiKey::Heartbeat => {
                HeartbeatRequest::decode(bytes, 1).unwrap();
                group_response(
                    correlation,
                    &HeartbeatResponse::default()
                        .with_throttle_time_ms(37)
                        .with_error_code(25),
                )
            }
            ApiKey::LeaveGroup => {
                LeaveGroupRequest::decode(bytes, 1).unwrap();
                group_response(
                    correlation,
                    &LeaveGroupResponse::default()
                        .with_throttle_time_ms(37)
                        .with_error_code(25),
                )
            }
            _ => unreachable!(),
        };
        assert!(bytes.is_empty());
        reply
    }

    fn serve_group_v1_requests(listener: &TcpListener) {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        for key in [
            ApiKey::JoinGroup,
            ApiKey::SyncGroup,
            ApiKey::Heartbeat,
            ApiKey::LeaveGroup,
        ] {
            let mut size = [0; 4];
            stream.read_exact(&mut size).unwrap();
            let mut bytes = vec![0; usize::try_from(i32::from_be_bytes(size)).unwrap()];
            stream.read_exact(&mut bytes).unwrap();
            let mut bytes = bytes::Bytes::from(bytes);
            let header = RequestHeader::decode(&mut bytes, key.request_header_version(1)).unwrap();
            assert_eq!(header.request_api_key, key as i16);
            assert_eq!(header.request_api_version, 1);
            let reply = group_request_reply(key, header.correlation_id, &mut bytes);
            stream
                .write_all(&i32::try_from(reply.len()).unwrap().to_be_bytes())
                .unwrap();
            stream.write_all(&reply).unwrap();
        }
    }

    #[test]
    fn group_v1_responses_use_generated_layouts_and_request_correlation() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || serve_group_v1_requests(&listener));
        let mut client = crate::client::KafkaClient::builder()
            .with_conn_rw_timeout(2)
            .build();
        let conn = client.get_conn_mut(&host).unwrap();
        let joined = fetch_join_group(
            conn,
            1,
            "client",
            "group",
            10_000,
            30_000,
            "",
            None,
            "consumer",
            &[],
        )
        .unwrap();
        assert_eq!(joined.generation_id, 7);
        assert_eq!(joined.protocol_type, None);
        assert_eq!(joined.protocol_name.as_deref(), Some("range"));
        assert_eq!(joined.leader_id, "leader");
        assert_eq!(joined.member_id, "member");
        assert_eq!(joined.members[0].metadata.as_ref(), b"metadata");
        let synced = fetch_sync_group(conn, 2, "client", "group", 7, "member", None, &[]).unwrap();
        assert_eq!(synced.error_code, 25);
        assert_eq!(synced.assignment.as_ref(), b"assignment");
        assert_eq!(
            fetch_heartbeat(conn, 3, "client", "group", 7, "member", None)
                .unwrap()
                .error_code,
            25
        );
        let members = [LeaveMemberRequest {
            member_id: "member".into(),
            group_instance_id: None,
        }];
        assert_eq!(
            fetch_leave_group(conn, 4, "client", "group", &members)
                .unwrap()
                .error_code,
            25
        );
        assert!(!conn.is_terminated());
        server.join().unwrap();
    }
}
