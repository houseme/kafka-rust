//! Consumer group offset operations for [`KafkaClient`].
//!
//! Implements committing and fetching consumer group offsets via the group
//! coordinator, with retry logic for transient errors such as
//! `GroupLoadInProgress` and `NotCoordinatorForGroup`.

use std::collections::HashMap;
use std::time::Instant;
use tracing::debug;

use crate::error::{Error, KafkaCode, Result};
use crate::protocol;
use crate::protocol::api_versions::ApiVersionCache;
use crate::utils::PartitionOffset;

use super::{config::ClientConfig, transport};

pub(crate) struct OffsetRequestContext<'a> {
    pub(crate) correlation_id: i32,
    pub(crate) client_id: &'a str,
    pub(crate) state: &'a mut super::state::ClientState,
    pub(crate) conn_pool: &'a mut crate::network::Connections,
    pub(crate) config: &'a ClientConfig,
    pub(crate) api_versions: &'a ApiVersionCache,
}

fn decode_find_coordinator_response(
    conn: &mut crate::network::KafkaConnection,
    requested_version: i16,
) -> Result<kafka_protocol::messages::FindCoordinatorResponse> {
    use kafka_protocol::messages::{FindCoordinatorResponse, ResponseHeader};
    use kafka_protocol::protocol::{Decodable, HeaderVersion};

    let size = transport::get_response_size(conn)?;
    let resp_bytes = conn.read_exact_alloc(crate::protocol::non_negative_i32_to_u64(size)?)?;

    let mut candidate_versions = vec![requested_version, 6, 5, 4, 3, 2, 1, 0];
    candidate_versions.dedup();

    for version in candidate_versions {
        let mut bytes = resp_bytes.clone();
        let header_version = FindCoordinatorResponse::header_version(version);
        if ResponseHeader::decode(&mut bytes, header_version).is_err() {
            continue;
        }
        if let Ok(resp) = FindCoordinatorResponse::decode(&mut bytes, version) {
            return Ok(resp);
        }
    }

    Err(Error::codec())
}

pub(crate) fn commit_offsets_kp<'a, J, I>(
    offsets: I,
    group: &str,
    mut ctx: OffsetRequestContext<'_>,
) -> Result<()>
where
    J: AsRef<super::CommitOffset<'a>>,
    I: IntoIterator<Item = J>,
{
    let mut offset_vec: Vec<(&str, i32, i64, Option<&str>)> = Vec::new();
    for o in offsets {
        let o = o.as_ref();
        if ctx.state.contains_topic_partition(o.topic, o.partition) {
            offset_vec.push((o.topic, o.partition, o.offset, None));
        } else {
            return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
        }
    }
    if offset_vec.is_empty() {
        debug!("commit_offsets_kp: no offsets provided");
        Ok(())
    } else {
        commit_offsets_inner(&offset_vec, group, &mut ctx)
    }
}

pub(crate) fn fetch_group_offsets_kp<'a, J, I>(
    partitions: I,
    group: &str,
    mut ctx: OffsetRequestContext<'_>,
) -> Result<HashMap<String, Vec<PartitionOffset>>>
where
    J: AsRef<super::FetchGroupOffset<'a>>,
    I: IntoIterator<Item = J>,
{
    let mut partition_vec: Vec<(&str, i32)> = Vec::new();
    for p in partitions {
        let p = p.as_ref();
        if ctx.state.contains_topic_partition(p.topic, p.partition) {
            partition_vec.push((p.topic, p.partition));
        } else {
            return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
        }
    }
    fetch_group_offsets_inner(&partition_vec, group, &mut ctx)
}

fn get_group_coordinator(
    group: &str,
    ctx: &mut OffsetRequestContext<'_>,
    now: Instant,
) -> Result<String> {
    if let Some(host) = ctx.state.group_coordinator(group) {
        return Ok(host.to_owned());
    }
    let correlation_id = ctx.state.next_correlation_id();
    let (mut header, request) = crate::protocol::consumer::build_find_coordinator_request(
        correlation_id,
        &ctx.config.client_id,
        group,
    );
    let mut attempt = 1;
    loop {
        let conn = ctx
            .conn_pool
            .get_conn_any(now)
            .ok_or_else(Error::no_host_reachable)?;
        let host = conn.host().to_owned();
        let api_version = transport::apply_request_api_version(
            ctx.api_versions,
            &host,
            &mut header,
            protocol::API_VERSION_FIND_COORDINATOR,
        );
        debug!(
            "get_group_coordinator_kp: asking for coordinator of '{}' on: {:?}",
            group, conn
        );
        transport::kp_send_request(conn, &header, &request, api_version)
            .map_err(|e| e.with_broker_context(&host, "FindCoordinator"))?;
        let kp_resp = decode_find_coordinator_response(conn, api_version)
            .map_err(|e| e.with_broker_context(&host, "FindCoordinator"))?;
        let r =
            crate::protocol::consumer::convert_find_coordinator_response(&kp_resp, correlation_id);
        let retry_code = match r.error {
            0 => {
                let gc = protocol::consumer::GroupCoordinatorResponse {
                    header: protocol::HeaderResponse {
                        correlation: correlation_id,
                    },
                    error: r.error,
                    broker_id: r.broker_id,
                    port: r.port,
                    host: r.host,
                };
                return Ok(ctx.state.set_group_coordinator(group, &gc).to_owned());
            }
            e if KafkaCode::from_protocol(e) == Some(KafkaCode::GroupCoordinatorNotAvailable) => e,
            e => {
                if let Some(code) = KafkaCode::from_protocol(e) {
                    return Err(Error::Kafka(code));
                }
                return Err(Error::Kafka(KafkaCode::Unknown));
            }
        };
        if attempt < ctx.config.retry_max_attempts() {
            debug!(
                "get_group_coordinator_kp: will retry request (c: {}) due to: {:?}",
                correlation_id, retry_code
            );
            attempt += 1;
            retry_sleep(ctx.config, attempt);
        } else {
            return Err(Error::Kafka(
                KafkaCode::from_protocol(retry_code).unwrap_or(KafkaCode::Unknown),
            ));
        }
    }
}

fn commit_offsets_inner(
    offsets: &[(&str, i32, i64, Option<&str>)],
    group: &str,
    ctx: &mut OffsetRequestContext<'_>,
) -> Result<()> {
    let mut attempt = 1;
    loop {
        let now = Instant::now();
        let host = get_group_coordinator(group, ctx, now)?;
        debug!("commit_offsets_kp: sending request to: {}", host);

        let conn = ctx
            .conn_pool
            .get_conn(&host, now)
            .map_err(|e| e.with_broker_context(&host, "OffsetCommit"))?;
        let (mut header, request) = crate::protocol::consumer::build_offset_commit_request(
            ctx.correlation_id,
            ctx.client_id,
            group,
            -1,
            "",
            -1,
            offsets,
        );
        let api_version = transport::apply_request_api_version(
            ctx.api_versions,
            &host,
            &mut header,
            protocol::API_VERSION_OFFSET_COMMIT,
        );
        transport::kp_send_request(conn, &header, &request, api_version)
            .map_err(|e| e.with_broker_context(&host, "OffsetCommit"))?;
        let kp_resp = transport::kp_get_response::<kafka_protocol::messages::OffsetCommitResponse>(
            conn,
            api_version,
        )
        .map_err(|e| e.with_broker_context(&host, "OffsetCommit"))?;
        let our_resp =
            crate::protocol::consumer::convert_offset_commit_response(kp_resp, ctx.correlation_id);

        let mut retry_code = None;
        'rproc: for tp in &our_resp.topic_partitions {
            for p in &tp.partitions {
                match KafkaCode::from_protocol(p.error) {
                    None => {}
                    Some(e @ KafkaCode::GroupLoadInProgress) => {
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Some(e @ KafkaCode::NotCoordinatorForGroup) => {
                        debug!(
                            "commit_offsets_kp: resetting group coordinator for '{}'",
                            group
                        );
                        ctx.state.remove_group_coordinator(group);
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Some(code) => return Err(Error::Kafka(code)),
                }
            }
        }
        match retry_code {
            Some(e) => {
                if attempt < ctx.config.retry_max_attempts() {
                    debug!(
                        "commit_offsets_kp: will retry request (c: {}) due to: {:?}",
                        ctx.correlation_id, e
                    );
                    attempt += 1;
                    retry_sleep(ctx.config, attempt);
                } else {
                    return Err(Error::Kafka(e));
                }
            }
            None => return Ok(()),
        }
    }
}

fn fetch_group_offsets_inner(
    partitions: &[(&str, i32)],
    group: &str,
    ctx: &mut OffsetRequestContext<'_>,
) -> Result<HashMap<String, Vec<PartitionOffset>>> {
    let mut attempt = 1;
    loop {
        let now = Instant::now();
        let host = get_group_coordinator(group, ctx, now)?;
        debug!("fetch_group_offsets_kp: sending request to: {}", host);

        let conn = ctx
            .conn_pool
            .get_conn(&host, now)
            .map_err(|e| e.with_broker_context(&host, "OffsetFetch"))?;
        let (mut header, request) = crate::protocol::consumer::build_offset_fetch_request(
            ctx.correlation_id,
            ctx.client_id,
            group,
            partitions,
        );
        let api_version = transport::apply_request_api_version(
            ctx.api_versions,
            &host,
            &mut header,
            protocol::API_VERSION_OFFSET_FETCH,
        );
        transport::kp_send_request(conn, &header, &request, api_version)
            .map_err(|e| e.with_broker_context(&host, "OffsetFetch"))?;
        let kp_resp = transport::kp_get_response::<kafka_protocol::messages::OffsetFetchResponse>(
            conn,
            api_version,
        )
        .map_err(|e| e.with_broker_context(&host, "OffsetFetch"))?;
        let mut retry_code = offset_fetch_retry_code(kp_resp.error_code, group, ctx.state)?;
        let our_resp =
            crate::protocol::consumer::convert_offset_fetch_response(kp_resp, ctx.correlation_id);

        let mut topic_map = HashMap::with_capacity(our_resp.topic_partitions.len());

        if retry_code.is_none() {
            'rproc: for tp in our_resp.topic_partitions {
                let mut partition_offsets = Vec::with_capacity(tp.partitions.len());
                for p in tp.partitions {
                    if let Some(code) = offset_fetch_retry_code(p.error, group, ctx.state)? {
                        retry_code = Some(code);
                        break 'rproc;
                    }
                    partition_offsets.push(PartitionOffset {
                        offset: p.offset,
                        partition: p.partition,
                    });
                }
                topic_map.insert(tp.topic, partition_offsets);
            }
        }

        match retry_code {
            Some(e) => {
                if attempt < ctx.config.retry_max_attempts() {
                    debug!(
                        "fetch_group_offsets_kp: will retry request (c: {}) due to: {:?}",
                        ctx.correlation_id, e
                    );
                    attempt += 1;
                    retry_sleep(ctx.config, attempt);
                } else {
                    return Err(Error::Kafka(e));
                }
            }
            None => return Ok(topic_map),
        }
    }
}

fn offset_fetch_retry_code(
    error_code: i16,
    group: &str,
    state: &mut super::state::ClientState,
) -> Result<Option<KafkaCode>> {
    match KafkaCode::from_protocol(error_code) {
        None => Ok(None),
        Some(code @ KafkaCode::GroupLoadInProgress) => Ok(Some(code)),
        Some(code @ KafkaCode::NotCoordinatorForGroup) => {
            debug!(
                "fetch_group_offsets_kp: resetting group coordinator for '{}'",
                group
            );
            state.remove_group_coordinator(group);
            Ok(Some(code))
        }
        Some(code) => Err(Error::Kafka(code)),
    }
}

#[allow(clippy::disallowed_methods)]
fn retry_sleep(cfg: &ClientConfig, attempt: u32) {
    if let Some(delay) = cfg.retry_policy().next_delay(attempt) {
        std::thread::sleep(delay);
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::{
        ApiKey, BrokerId, FindCoordinatorRequest, FindCoordinatorResponse, OffsetFetchRequest,
        OffsetFetchResponse, RequestHeader, ResponseHeader, TopicName,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

    use super::*;
    use crate::client::{KafkaClient, RetryPolicy};

    #[test]
    fn group_coordinator_returns_no_host_for_an_empty_connection_pool() {
        let mut client = KafkaClient::new(vec![]);
        let mut ctx = OffsetRequestContext {
            correlation_id: 1,
            client_id: &client.config.client_id,
            state: &mut client.state,
            conn_pool: &mut client.conn_pool,
            config: &client.config,
            api_versions: &client.api_versions,
        };

        assert!(matches!(
            get_group_coordinator("test-group", &mut ctx, Instant::now()),
            Err(Error::Connection(
                crate::error::ConnectionError::NoHostReachable
            ))
        ));
    }

    #[test]
    fn group_offsets_retry_a_top_level_loading_error() {
        let (result, _) = fetch_offsets_from_mock(&[KafkaCode::GroupLoadInProgress as i16, 0], 2);
        let offsets = result.unwrap();
        assert_eq!(offsets["test-topic"][0].offset, 42);
    }

    #[test]
    fn group_offsets_rediscover_coordinator_after_a_top_level_error() {
        let (result, coordinator) =
            fetch_offsets_from_mock(&[KafkaCode::NotCoordinatorForGroup as i16, 0], 2);
        assert_eq!(result.unwrap()["test-topic"][0].offset, 42);
        assert!(coordinator.is_some());
    }

    #[test]
    fn group_offsets_return_top_level_errors_when_retry_is_exhausted() {
        for code in [
            KafkaCode::GroupLoadInProgress,
            KafkaCode::NotCoordinatorForGroup,
        ] {
            let (result, coordinator) = fetch_offsets_from_mock(&[code as i16], 1);
            assert!(matches!(result, Err(Error::Kafka(error)) if error == code));
            if code == KafkaCode::NotCoordinatorForGroup {
                assert!(coordinator.is_none());
            }
        }
    }

    #[test]
    fn group_offsets_return_a_terminal_top_level_error() {
        let (result, _) = fetch_offsets_from_mock(&[KafkaCode::GroupAuthorizationFailed as i16], 2);
        assert!(matches!(
            result,
            Err(Error::Kafka(KafkaCode::GroupAuthorizationFailed))
        ));
    }

    fn fetch_offsets_from_mock(
        errors: &[i16],
        max_attempts: u32,
    ) -> (
        Result<HashMap<String, Vec<PartitionOffset>>>,
        Option<String>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let errors = errors.to_vec();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            for (index, &error_code) in errors.iter().enumerate() {
                if index > 0 && errors[index - 1] == KafkaCode::NotCoordinatorForGroup as i16 {
                    let (header, version) = read_request(&mut stream, ApiKey::FindCoordinator);
                    let response = FindCoordinatorResponse::default()
                        .with_node_id(BrokerId::from(1))
                        .with_host(StrBytes::from_string(address.ip().to_string()))
                        .with_port(i32::from(address.port()));
                    write_response(&mut stream, &header, version, &response);
                }
                let (header, version) = read_request(&mut stream, ApiKey::OffsetFetch);
                let mut response = OffsetFetchResponse::default().with_error_code(error_code);
                if error_code == 0 {
                    response = response.with_topics(vec![
                        kafka_protocol::messages::offset_fetch_response::OffsetFetchResponseTopic::default()
                            .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
                            .with_partitions(vec![
                                kafka_protocol::messages::offset_fetch_response::OffsetFetchResponsePartition::default()
                                    .with_partition_index(0)
                                    .with_committed_offset(42),
                            ]),
                    ]);
                }
                write_response(&mut stream, &header, version, &response);
            }
        });

        let mut client = KafkaClient::new(vec![address.to_string()]);
        client.config.retry.policy = RetryPolicy::Fixed {
            interval: Duration::ZERO,
            max_attempts,
        };
        client.config.connection.rw_timeout = Duration::from_secs(5);
        client.state.set_group_coordinator(
            "test-group",
            &protocol::consumer::GroupCoordinatorResponse {
                broker_id: 1,
                host: address.ip().to_string(),
                port: i32::from(address.port()),
                ..Default::default()
            },
        );
        let mut ctx = OffsetRequestContext {
            correlation_id: 1,
            client_id: &client.config.client_id,
            state: &mut client.state,
            conn_pool: &mut client.conn_pool,
            config: &client.config,
            api_versions: &client.api_versions,
        };
        let result = fetch_group_offsets_inner(&[("test-topic", 0)], "test-group", &mut ctx);
        let coordinator = client
            .state
            .group_coordinator("test-group")
            .map(str::to_owned);
        server.join().unwrap();
        (result, coordinator)
    }

    fn read_request(stream: &mut TcpStream, api_key: ApiKey) -> (RequestHeader, i16) {
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let mut bytes = vec![0; usize::try_from(i32::from_be_bytes(length)).unwrap()];
        stream.read_exact(&mut bytes).unwrap();
        let version = i16::from_be_bytes(bytes[2..4].try_into().unwrap());
        let header_version = match api_key {
            ApiKey::FindCoordinator => FindCoordinatorRequest::header_version(version),
            ApiKey::OffsetFetch => OffsetFetchRequest::header_version(version),
            _ => unreachable!(),
        };
        let header = RequestHeader::decode(&mut Bytes::from(bytes), header_version).unwrap();
        assert_eq!(header.request_api_key, api_key as i16);
        (header, version)
    }

    fn write_response<T: Encodable + HeaderVersion>(
        stream: &mut TcpStream,
        request: &RequestHeader,
        version: i16,
        response: &T,
    ) {
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
}
