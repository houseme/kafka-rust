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
            .expect("available connection");
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
        let our_resp =
            crate::protocol::consumer::convert_offset_fetch_response(kp_resp, ctx.correlation_id);

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
                        ctx.state.remove_group_coordinator(group);
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

#[allow(clippy::disallowed_methods)]
fn retry_sleep(cfg: &ClientConfig, attempt: u32) {
    if let Some(delay) = cfg.retry_policy().next_delay(attempt) {
        std::thread::sleep(delay);
    }
}
