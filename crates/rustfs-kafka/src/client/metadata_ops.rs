//! Metadata and offset operations for [`KafkaClient`].
//!
//! Handles loading cluster metadata (topic and partition info), fetching
//! topic offsets (earliest, latest, by timestamp), and API version negotiation
//! with brokers.

use std::collections::hash_map;
use std::collections::hash_map::HashMap;
use std::time::Instant;
use tracing::debug;

use crate::error::{Error, KafkaCode, Result};
use crate::protocol;
use crate::utils::{PartitionOffset, TimestampedPartitionOffset};

use super::transport;
use super::{FetchOffset, KafkaClient};

#[tracing::instrument(skip(client))]
pub fn load_metadata_all(client: &mut KafkaClient) -> Result<()> {
    client.reset_metadata();
    load_metadata_kp(client, &[] as &[&str])
}

#[tracing::instrument(skip(client, topics), fields(topic_count = topics.len()))]
pub fn load_metadata<T: AsRef<str>>(client: &mut KafkaClient, topics: &[T]) -> Result<()> {
    load_metadata_kp(client, topics)
}

pub fn load_metadata_kp<T: AsRef<str>>(client: &mut KafkaClient, topics: &[T]) -> Result<()> {
    #[cfg(feature = "metrics")]
    let start = Instant::now();
    let resp = fetch_metadata_kp(client, topics)?;
    client.state.update_metadata(resp);
    #[cfg(feature = "metrics")]
    crate::metrics::record_metadata_refresh(start.elapsed().as_secs_f64() * 1000.0);
    Ok(())
}

pub fn reset_metadata(client: &mut KafkaClient) {
    client.state.clear_metadata();
}

#[tracing::instrument(skip(client, topics))]
pub fn fetch_offsets<T: AsRef<str>>(
    client: &mut KafkaClient,
    topics: &[T],
    offset: FetchOffset,
) -> Result<HashMap<String, Vec<PartitionOffset>>> {
    fetch_offsets_kp(client, topics, offset)
}

pub fn list_offsets<T: AsRef<str>>(
    client: &mut KafkaClient,
    topics: &[T],
    offset: FetchOffset,
) -> Result<HashMap<String, Vec<TimestampedPartitionOffset>>> {
    fetch_offsets_kp(client, topics, offset).map(|m| {
        m.into_iter()
            .map(|(topic, offsets)| {
                let timestamped = offsets
                    .into_iter()
                    .map(|po| TimestampedPartitionOffset {
                        offset: po.offset,
                        partition: po.partition,
                        time: 0,
                    })
                    .collect();
                (topic, timestamped)
            })
            .collect()
    })
}

pub fn fetch_topic_offsets<T: AsRef<str>>(
    client: &mut KafkaClient,
    topic: T,
    offset: FetchOffset,
) -> Result<Vec<PartitionOffset>> {
    let topic = topic.as_ref();
    let mut m = fetch_offsets(client, &[topic], offset)?;
    let offs = m.remove(topic).unwrap_or_default();
    if offs.is_empty() {
        Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition))
    } else {
        Ok(offs)
    }
}

pub fn fetch_offsets_kp<T: AsRef<str>>(
    client: &mut KafkaClient,
    topics: &[T],
    offset: FetchOffset,
) -> Result<HashMap<String, Vec<PartitionOffset>>> {
    let time = offset.to_kafka_value();
    let n_topics = topics.len();
    let state = &mut client.state;
    let correlation = state.next_correlation_id();
    let config = &client.config;

    let mut broker_partitions: HashMap<&str, Vec<(&str, i32, i64)>> = HashMap::new();
    for topic in topics {
        let topic = topic.as_ref();
        if let Some(ps) = state.partitions_for(topic) {
            for (id, host) in ps
                .iter()
                .filter_map(|(id, p)| p.broker(state).map(|b| (id, b.host())))
            {
                broker_partitions
                    .entry(host)
                    .or_default()
                    .push((topic, id, time));
            }
        }
    }

    let now = Instant::now();
    let mut res: HashMap<String, Vec<PartitionOffset>> = HashMap::with_capacity(n_topics);
    for (host, partitions) in broker_partitions {
        let conn = client
            .conn_pool
            .get_conn(host, now)
            .map_err(|e| e.with_broker_context(host, "ListOffsets"))?;
        let (header, request) = crate::protocol::offset::build_list_offsets_request(
            correlation,
            &config.client_id,
            &partitions,
        );
        transport::kp_send_request(
            conn,
            &header,
            &request,
            crate::protocol::API_VERSION_LIST_OFFSETS,
        )
        .map_err(|e| e.with_broker_context(host, "ListOffsets"))?;
        let kp_resp = transport::kp_get_response::<kafka_protocol::messages::ListOffsetsResponse>(
            conn,
            crate::protocol::API_VERSION_LIST_OFFSETS,
        )
        .map_err(|e| e.with_broker_context(host, "ListOffsets"))?;
        let our_resp = crate::protocol::offset::convert_list_offsets_response(kp_resp, correlation);

        for tp in our_resp.topic_partitions {
            let mut entry = res.entry(tp.topic);
            let mut new_resp_offsets = None;
            let mut err = None;
            {
                let resp_offsets = match entry {
                    hash_map::Entry::Occupied(ref mut e) => e.get_mut(),
                    hash_map::Entry::Vacant(_) => {
                        new_resp_offsets.get_or_insert(Vec::with_capacity(tp.partitions.len()))
                    }
                };
                for p in tp.partitions {
                    let partition_offset = match p.to_offset() {
                        Ok(po) => po,
                        Err(code) => {
                            err = Some((p.partition, code));
                            break;
                        }
                    };
                    resp_offsets.push(partition_offset);
                }
            }
            if let Some((partition, code)) = err {
                let topic = get_key_from_entry(entry);
                return Err(Error::TopicPartitionError {
                    topic_name: topic,
                    partition_id: partition,
                    error_code: code,
                });
            }
            if let hash_map::Entry::Vacant(e) = entry {
                e.insert(new_resp_offsets.unwrap());
            }
        }
    }

    Ok(res)
}

fn fetch_metadata_kp<T: AsRef<str>>(
    client: &mut KafkaClient,
    topics: &[T],
) -> Result<protocol::metadata::MetadataResponseData> {
    let correlation = client.state.next_correlation_id();
    let now = Instant::now();
    let topic_strs: Vec<&str> = topics.iter().map(std::convert::AsRef::as_ref).collect();

    for host in &client.config.hosts {
        debug!("fetch_metadata_kp: requesting metadata from {}", host);
        match client.conn_pool.get_conn(host, now) {
            Ok(conn) => {
                if !client.api_versions.contains(host) {
                    let av_correlation = client.state.next_correlation_id();
                    match crate::protocol::api_versions::fetch_api_versions(
                        conn,
                        av_correlation,
                        &client.config.client_id,
                    ) {
                        Ok(versions) => {
                            client.api_versions.insert(host.clone(), versions);
                        }
                        Err(e) => debug!(
                            "fetch_metadata_kp: API version negotiation failed for {}: {}",
                            host, e
                        ),
                    }
                }

                let (header, request) = crate::protocol::metadata::build_metadata_request(
                    correlation,
                    &client.config.client_id,
                    if topic_strs.is_empty() {
                        None
                    } else {
                        Some(&topic_strs)
                    },
                );
                match transport::kp_send_request(
                    conn,
                    &header,
                    &request,
                    crate::protocol::API_VERSION_METADATA,
                ) {
                    Ok(()) => {
                        match transport::kp_get_response::<kafka_protocol::messages::MetadataResponse>(
                            conn,
                            crate::protocol::API_VERSION_METADATA,
                        ) {
                            Ok(kp_resp) => {
                                return Ok(crate::protocol::metadata::convert_metadata_response(
                                    kp_resp,
                                    correlation,
                                ));
                            }
                            Err(e) => debug!(
                                "fetch_metadata_kp: failed to decode metadata from {}: {}",
                                host, e
                            ),
                        }
                    }
                    Err(e) => debug!(
                        "fetch_metadata_kp: failed to request metadata from {}: {}",
                        host, e
                    ),
                }
            }
            Err(e) => {
                debug!("fetch_metadata_kp: failed to connect to {}: {}", host, e);
            }
        }
    }
    Err(Error::no_host_reachable())
}

pub(crate) fn get_key_from_entry<'a, K: 'a, V: 'a>(entry: hash_map::Entry<'a, K, V>) -> K {
    match entry {
        hash_map::Entry::Occupied(e) => e.remove_entry().0,
        hash_map::Entry::Vacant(e) => e.into_key(),
    }
}
