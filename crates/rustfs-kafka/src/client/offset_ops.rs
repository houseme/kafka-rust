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
use crate::utils::PartitionOffset;

use super::config::ClientConfig;

pub(crate) fn commit_offsets_kp<'a, J, I>(
    offsets: I,
    group: &str,
    correlation_id: i32,
    client_id: &str,
    state: &mut super::state::ClientState,
    conn_pool: &mut crate::network::Connections,
    config: &ClientConfig,
) -> Result<()>
where
    J: AsRef<super::CommitOffset<'a>>,
    I: IntoIterator<Item = J>,
{
    let mut offset_vec: Vec<(&str, i32, i64, Option<&str>)> = Vec::new();
    for o in offsets {
        let o = o.as_ref();
        if state.contains_topic_partition(o.topic, o.partition) {
            offset_vec.push((o.topic, o.partition, o.offset, None));
        } else {
            return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
        }
    }
    if offset_vec.is_empty() {
        debug!("commit_offsets_kp: no offsets provided");
        Ok(())
    } else {
        commit_offsets_inner(
            &offset_vec,
            group,
            correlation_id,
            client_id,
            state,
            conn_pool,
            config,
        )
    }
}

pub(crate) fn fetch_group_offsets_kp<'a, J, I>(
    partitions: I,
    group: &str,
    correlation_id: i32,
    client_id: &str,
    state: &mut super::state::ClientState,
    conn_pool: &mut crate::network::Connections,
    config: &ClientConfig,
) -> Result<HashMap<String, Vec<PartitionOffset>>>
where
    J: AsRef<super::FetchGroupOffset<'a>>,
    I: IntoIterator<Item = J>,
{
    let mut partition_vec: Vec<(&str, i32)> = Vec::new();
    for p in partitions {
        let p = p.as_ref();
        if state.contains_topic_partition(p.topic, p.partition) {
            partition_vec.push((p.topic, p.partition));
        } else {
            return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
        }
    }
    fetch_group_offsets_inner(
        &partition_vec,
        group,
        correlation_id,
        client_id,
        state,
        conn_pool,
        config,
    )
}

fn get_group_coordinator(
    group: &str,
    state: &mut super::state::ClientState,
    conn_pool: &mut crate::network::Connections,
    config: &ClientConfig,
    now: Instant,
) -> Result<String> {
    if let Some(host) = state.group_coordinator(group) {
        return Ok(host.to_owned());
    }
    let correlation_id = state.next_correlation_id();
    let (header, request) = crate::protocol::consumer::build_find_coordinator_request(
        correlation_id,
        &config.client_id,
        group,
    );
    let mut attempt = 1;
    loop {
        let conn = conn_pool.get_conn_any(now).expect("available connection");
        debug!(
            "get_group_coordinator_kp: asking for coordinator of '{}' on: {:?}",
            group, conn
        );
        kp_send_request(
            conn,
            &header,
            &request,
            crate::protocol::API_VERSION_FIND_COORDINATOR,
        )
        .map_err(|e| e.with_broker_context("any", "FindCoordinator"))?;
        let kp_resp = kp_get_response::<kafka_protocol::messages::FindCoordinatorResponse>(
            conn,
            crate::protocol::API_VERSION_FIND_COORDINATOR,
        )
        .map_err(|e| e.with_broker_context("any", "FindCoordinator"))?;
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
                return Ok(state.set_group_coordinator(group, &gc).to_owned());
            }
            e if KafkaCode::from_protocol(e) == Some(KafkaCode::GroupCoordinatorNotAvailable) => e,
            e => {
                if let Some(code) = KafkaCode::from_protocol(e) {
                    return Err(Error::Kafka(code));
                }
                return Err(Error::Kafka(KafkaCode::Unknown));
            }
        };
        if attempt < config.retry_max_attempts() {
            debug!(
                "get_group_coordinator_kp: will retry request (c: {}) due to: {:?}",
                correlation_id, retry_code
            );
            attempt += 1;
            retry_sleep(config, attempt);
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
    correlation_id: i32,
    client_id: &str,
    state: &mut super::state::ClientState,
    conn_pool: &mut crate::network::Connections,
    config: &ClientConfig,
) -> Result<()> {
    let mut attempt = 1;
    loop {
        let now = Instant::now();
        let host = get_group_coordinator(group, state, conn_pool, config, now)?;
        debug!("commit_offsets_kp: sending request to: {}", host);

        let conn = conn_pool
            .get_conn(&host, now)
            .map_err(|e| e.with_broker_context(&host, "OffsetCommit"))?;
        let (header, request) = crate::protocol::consumer::build_offset_commit_request(
            correlation_id,
            client_id,
            group,
            -1,
            "",
            -1,
            offsets,
        );
        kp_send_request(
            conn,
            &header,
            &request,
            crate::protocol::API_VERSION_OFFSET_COMMIT,
        )
        .map_err(|e| e.with_broker_context(&host, "OffsetCommit"))?;
        let kp_resp = kp_get_response::<kafka_protocol::messages::OffsetCommitResponse>(
            conn,
            crate::protocol::API_VERSION_OFFSET_COMMIT,
        )
        .map_err(|e| e.with_broker_context(&host, "OffsetCommit"))?;
        let our_resp =
            crate::protocol::consumer::convert_offset_commit_response(kp_resp, correlation_id);

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
                        state.remove_group_coordinator(group);
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Some(code) => return Err(Error::Kafka(code)),
                }
            }
        }
        match retry_code {
            Some(e) => {
                if attempt < config.retry_max_attempts() {
                    debug!(
                        "commit_offsets_kp: will retry request (c: {}) due to: {:?}",
                        correlation_id, e
                    );
                    attempt += 1;
                    retry_sleep(config, attempt);
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
    correlation_id: i32,
    client_id: &str,
    state: &mut super::state::ClientState,
    conn_pool: &mut crate::network::Connections,
    config: &ClientConfig,
) -> Result<HashMap<String, Vec<PartitionOffset>>> {
    let mut attempt = 1;
    loop {
        let now = Instant::now();
        let host = get_group_coordinator(group, state, conn_pool, config, now)?;
        debug!("fetch_group_offsets_kp: sending request to: {}", host);

        let conn = conn_pool
            .get_conn(&host, now)
            .map_err(|e| e.with_broker_context(&host, "OffsetFetch"))?;
        let (header, request) = crate::protocol::consumer::build_offset_fetch_request(
            correlation_id,
            client_id,
            group,
            partitions,
        );
        kp_send_request(
            conn,
            &header,
            &request,
            crate::protocol::API_VERSION_OFFSET_FETCH,
        )
        .map_err(|e| e.with_broker_context(&host, "OffsetFetch"))?;
        let kp_resp = kp_get_response::<kafka_protocol::messages::OffsetFetchResponse>(
            conn,
            crate::protocol::API_VERSION_OFFSET_FETCH,
        )
        .map_err(|e| e.with_broker_context(&host, "OffsetFetch"))?;
        let our_resp =
            crate::protocol::consumer::convert_offset_fetch_response(kp_resp, correlation_id);

        let mut retry_code = None;
        let mut topic_map = HashMap::with_capacity(our_resp.topic_partitions.len());

        'rproc: for tp in our_resp.topic_partitions {
            let mut partition_offsets = Vec::with_capacity(tp.partitions.len());
            for p in tp.partitions {
                match KafkaCode::from_protocol(p.error) {
                    None => {
                        partition_offsets.push(PartitionOffset {
                            offset: p.offset,
                            partition: p.partition,
                        });
                    }
                    Some(e @ KafkaCode::GroupLoadInProgress) => {
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Some(e @ KafkaCode::NotCoordinatorForGroup) => {
                        debug!(
                            "fetch_group_offsets_kp: resetting group coordinator for '{}'",
                            group
                        );
                        state.remove_group_coordinator(group);
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Some(e) => return Err(Error::Kafka(e)),
                }
            }
            topic_map.insert(tp.topic, partition_offsets);
        }

        match retry_code {
            Some(e) => {
                if attempt < config.retry_max_attempts() {
                    debug!(
                        "fetch_group_offsets_kp: will retry request (c: {}) due to: {:?}",
                        correlation_id, e
                    );
                    attempt += 1;
                    retry_sleep(config, attempt);
                } else {
                    return Err(Error::Kafka(e));
                }
            }
            None => return Ok(topic_map),
        }
    }
}

fn kp_send_request(
    conn: &mut crate::network::KafkaConnection,
    header: &kafka_protocol::messages::RequestHeader,
    body: &impl kafka_protocol::protocol::Encodable,
    api_version: i16,
) -> Result<()> {
    use bytes::BytesMut;
    use kafka_protocol::protocol::Encodable;

    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, api_version)
        .map_err(|_| Error::codec())?;

    let mut body_buf = BytesMut::new();
    body.encode(&mut body_buf, api_version)
        .map_err(|_| Error::codec())?;

    let total_len = crate::protocol::usize_to_i32(header_buf.len() + body_buf.len())?;
    let out_len = crate::protocol::non_negative_i32_to_usize(total_len)?;
    let mut out = BytesMut::with_capacity(4 + out_len);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body_buf);

    conn.send(&out)?;
    Ok(())
}

fn kp_get_response<R: kafka_protocol::protocol::Decodable>(
    conn: &mut crate::network::KafkaConnection,
    api_version: i16,
) -> Result<R> {
    use bytes::Bytes;
    use kafka_protocol::messages::ResponseHeader;
    use kafka_protocol::protocol::Decodable;

    let size = get_response_size(conn)?;
    let resp_bytes = conn.read_exact_alloc(crate::protocol::non_negative_i32_to_u64(size)?)?;

    let mut bytes = Bytes::from(resp_bytes);
    let _resp_header =
        ResponseHeader::decode(&mut bytes, api_version).map_err(|_| Error::codec())?;

    R::decode(&mut bytes, api_version).map_err(|_| Error::codec())
}

fn get_response_size(conn: &mut crate::network::KafkaConnection) -> Result<i32> {
    let mut buf = [0u8; 4];
    conn.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

#[allow(clippy::disallowed_methods)]
fn retry_sleep(cfg: &ClientConfig, attempt: u32) {
    if let Some(delay) = cfg.retry_policy().next_delay(attempt) {
        std::thread::sleep(delay);
    }
}
