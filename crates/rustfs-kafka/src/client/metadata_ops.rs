//! Metadata and offset operations for [`KafkaClient`].
//!
//! Handles loading cluster metadata (topic and partition info), fetching
//! topic offsets (earliest, latest, by timestamp), and API version negotiation
//! with brokers.

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
    request_offsets_kp(client, topics, offset, |partition| {
        TimestampedPartitionOffset {
            offset: partition.offset,
            partition: partition.partition_index,
            time: partition.timestamp,
        }
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
    request_offsets_kp(client, topics, offset, |partition| PartitionOffset {
        offset: partition.offset,
        partition: partition.partition_index,
    })
}

fn request_offsets_kp<T: AsRef<str>, O>(
    client: &mut KafkaClient,
    topics: &[T],
    offset: FetchOffset,
    map_partition: impl Fn(
        &kafka_protocol::messages::list_offsets_response::ListOffsetsPartitionResponse,
    ) -> O,
) -> Result<HashMap<String, Vec<O>>> {
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
    let mut res: HashMap<String, Vec<O>> = HashMap::with_capacity(n_topics);
    for (host, partitions) in broker_partitions {
        let conn = client
            .conn_pool
            .get_conn(host, now)
            .map_err(|e| e.with_broker_context(host, "ListOffsets"))?;
        let (header, request) = protocol::offset::build_list_offsets_request(
            correlation,
            &config.client_id,
            &partitions,
        );
        let mut header = header;
        let api_version = transport::apply_request_api_version(
            &client.api_versions,
            host,
            &mut header,
            protocol::API_VERSION_LIST_OFFSETS,
        );
        transport::kp_send_request(conn, &header, &request, api_version)
            .map_err(|e| e.with_broker_context(host, "ListOffsets"))?;
        let kp_resp = transport::kp_get_response::<kafka_protocol::messages::ListOffsetsResponse>(
            conn,
            api_version,
        )
        .map_err(|e| e.with_broker_context(host, "ListOffsets"))?;
        for topic in kp_resp.topics {
            let topic_name = topic.name;
            let resp_offsets = res
                .entry(topic_name.to_string())
                .or_insert_with(|| Vec::with_capacity(topic.partitions.len()));
            resp_offsets.reserve(topic.partitions.len());
            for partition in topic.partitions {
                if let Some(error_code) = KafkaCode::from_protocol(partition.error_code) {
                    return Err(Error::TopicPartitionError {
                        topic_name: topic_name.to_string(),
                        partition_id: partition.partition_index,
                        error_code,
                    });
                }
                resp_offsets.push(map_partition(&partition));
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
    let topic_strs: Vec<&str> = topics.iter().map(AsRef::as_ref).collect();

    for host in &client.config.hosts {
        debug!("fetch_metadata_kp: requesting metadata from {}", host);
        match client.conn_pool.get_conn(host, now) {
            Ok(conn) => {
                if !client.api_versions.contains(host) {
                    let av_correlation = client.state.next_correlation_id();
                    match protocol::api_versions::fetch_api_versions(
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

                let (mut header, request) = protocol::metadata::build_metadata_request(
                    correlation,
                    &client.config.client_id,
                    if topic_strs.is_empty() {
                        None
                    } else {
                        Some(&topic_strs)
                    },
                );
                let api_version = transport::apply_request_api_version(
                    &client.api_versions,
                    host,
                    &mut header,
                    protocol::API_VERSION_METADATA,
                );
                match transport::kp_send_request(conn, &header, &request, api_version) {
                    Ok(()) => {
                        match transport::kp_get_response::<kafka_protocol::messages::MetadataResponse>(
                            conn,
                            api_version,
                        ) {
                            Ok(kp_resp) => {
                                return Ok(protocol::metadata::convert_metadata_response(
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

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };
    use kafka_protocol::messages::{
        ApiKey, ListOffsetsRequest, ListOffsetsResponse, RequestHeader, ResponseHeader, TopicName,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

    use super::*;
    use crate::protocol::metadata::{
        BrokerMetadata, MetadataResponseData, PartitionMetadata, TopicMetadata,
    };

    #[test]
    fn list_offsets_preserves_the_broker_timestamp_for_by_time() {
        let requested_time = 1_700_000_000_000;
        let returned_time = requested_time + 500;
        let response = offsets_response(&[("topic-a", 0, 42, returned_time, 0)]);
        let (mut client, server) =
            mock_offsets_client(vec![(requested_time, response)], &[("topic-a", 1)]);

        let offsets = client
            .list_offsets(&["topic-a"], FetchOffset::ByTime(requested_time))
            .unwrap();
        assert_eq!(
            offsets["topic-a"],
            [TimestampedPartitionOffset {
                offset: 42,
                partition: 0,
                time: returned_time,
            }]
        );
        server.join().unwrap();
    }

    #[test]
    fn list_offsets_preserves_unknown_timestamps_for_earliest_latest_and_no_match() {
        let (mut client, server) = mock_offsets_client(
            vec![
                (-2, offsets_response(&[("topic-a", 0, 3, -1, 0)])),
                (-1, offsets_response(&[("topic-a", 0, 12, -1, 0)])),
                (9_999, offsets_response(&[("topic-a", 0, -1, -1, 0)])),
            ],
            &[("topic-a", 1)],
        );

        for (query, expected_offset) in [
            (FetchOffset::Earliest, 3),
            (FetchOffset::Latest, 12),
            (FetchOffset::ByTime(9_999), -1),
        ] {
            let offsets = client.list_offsets(&["topic-a"], query).unwrap();
            assert_eq!(offsets["topic-a"][0].offset, expected_offset);
            assert_eq!(offsets["topic-a"][0].time, -1);
        }
        server.join().unwrap();
    }

    #[test]
    fn list_offsets_keeps_each_topic_and_partition_timestamp() {
        let response = offsets_response(&[
            ("topic-a", 0, 11, 1_000, 0),
            ("topic-a", 1, 23, 2_000, 0),
            ("topic-b", 0, 31, 3_000, 0),
        ]);
        let (mut client, server) = mock_offsets_client(
            vec![(100, response.clone()), (100, response)],
            &[("topic-a", 2), ("topic-b", 1)],
        );

        let timestamped = client
            .list_offsets(&["topic-a", "topic-b"], FetchOffset::ByTime(100))
            .unwrap();
        assert_eq!(
            timestamped["topic-a"],
            [
                TimestampedPartitionOffset {
                    offset: 11,
                    partition: 0,
                    time: 1_000
                },
                TimestampedPartitionOffset {
                    offset: 23,
                    partition: 1,
                    time: 2_000
                },
            ]
        );
        assert_eq!(
            timestamped["topic-b"],
            [TimestampedPartitionOffset {
                offset: 31,
                partition: 0,
                time: 3_000
            },]
        );

        let offsets = client
            .fetch_offsets(&["topic-a", "topic-b"], FetchOffset::ByTime(100))
            .unwrap();
        assert_eq!(
            offsets["topic-a"],
            [
                PartitionOffset {
                    offset: 11,
                    partition: 0
                },
                PartitionOffset {
                    offset: 23,
                    partition: 1
                },
            ]
        );
        assert_eq!(
            offsets["topic-b"],
            [PartitionOffset {
                offset: 31,
                partition: 0
            }]
        );
        server.join().unwrap();
    }

    #[test]
    fn both_offset_apis_report_the_broker_partition_error() {
        let response = offsets_response(&[
            ("topic-a", 0, 11, 1_000, 0),
            (
                "topic-a",
                1,
                -1,
                -1,
                KafkaCode::NotLeaderForPartition as i16,
            ),
        ]);
        let (mut client, server) = mock_offsets_client(
            vec![(100, response.clone()), (100, response)],
            &[("topic-a", 2)],
        );

        let errors = [
            client
                .list_offsets(&["topic-a"], FetchOffset::ByTime(100))
                .unwrap_err(),
            client
                .fetch_offsets(&["topic-a"], FetchOffset::ByTime(100))
                .unwrap_err(),
        ];
        for error in errors {
            assert!(matches!(error, Error::TopicPartitionError {
                topic_name,
                partition_id: 1,
                error_code: KafkaCode::NotLeaderForPartition,
            } if topic_name == "topic-a"));
        }
        server.join().unwrap();
    }

    fn offsets_response(partitions: &[(&str, i32, i64, i64, i16)]) -> ListOffsetsResponse {
        let mut topics: Vec<ListOffsetsTopicResponse> = Vec::new();
        for &(topic, partition, offset, timestamp, error_code) in partitions {
            let topic_index = topics
                .iter()
                .position(|entry| entry.name.as_str() == topic)
                .unwrap_or_else(|| {
                    topics.push(
                        ListOffsetsTopicResponse::default()
                            .with_name(TopicName::from(StrBytes::from_string(topic.to_owned()))),
                    );
                    topics.len() - 1
                });
            topics[topic_index].partitions.push(
                ListOffsetsPartitionResponse::default()
                    .with_partition_index(partition)
                    .with_offset(offset)
                    .with_timestamp(timestamp)
                    .with_error_code(error_code),
            );
        }
        ListOffsetsResponse::default().with_topics(topics)
    }

    fn mock_offsets_client(
        script: Vec<(i64, ListOffsetsResponse)>,
        topics: &[(&str, i32)],
    ) -> (KafkaClient, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let expected_partitions: usize = topics
            .iter()
            .map(|(_, count)| usize::try_from(*count).unwrap())
            .sum();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            for (expected_time, response) in script {
                let (header, request) = read_offsets_request(&mut stream);
                assert_eq!(
                    request
                        .topics
                        .iter()
                        .map(|topic| topic.partitions.len())
                        .sum::<usize>(),
                    expected_partitions
                );
                for topic in request.topics {
                    for partition in topic.partitions {
                        assert_eq!(partition.timestamp, expected_time);
                    }
                }
                let version = header.request_api_version;
                let mut payload = BytesMut::new();
                ResponseHeader::default()
                    .with_correlation_id(header.correlation_id)
                    .encode(&mut payload, ListOffsetsResponse::header_version(version))
                    .unwrap();
                response.encode(&mut payload, version).unwrap();
                stream
                    .write_all(&i32::try_from(payload.len()).unwrap().to_be_bytes())
                    .unwrap();
                stream.write_all(&payload).unwrap();
            }
        });
        let mut client = KafkaClient::builder()
            .with_hosts(vec![address.to_string()])
            .with_conn_rw_timeout(5)
            .build();
        client.state.update_metadata(MetadataResponseData {
            brokers: vec![BrokerMetadata {
                node_id: 1,
                host: address.ip().to_string(),
                port: i32::from(address.port()),
            }],
            topics: topics
                .iter()
                .map(|&(topic, count)| TopicMetadata {
                    topic: topic.to_owned(),
                    partitions: (0..count)
                        .map(|id| PartitionMetadata {
                            id,
                            leader: 1,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        });
        (client, server)
    }

    fn read_offsets_request(stream: &mut TcpStream) -> (RequestHeader, ListOffsetsRequest) {
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let mut payload = vec![0; usize::try_from(i32::from_be_bytes(length)).unwrap()];
        stream.read_exact(&mut payload).unwrap();
        let version = i16::from_be_bytes(payload[2..4].try_into().unwrap());
        let mut payload = Bytes::from(payload);
        let header =
            RequestHeader::decode(&mut payload, ListOffsetsRequest::header_version(version))
                .unwrap();
        assert_eq!(header.request_api_key, ApiKey::ListOffsets as i16);
        let request = ListOffsetsRequest::decode(&mut payload, version).unwrap();
        assert!(payload.is_empty());
        (header, request)
    }
}
