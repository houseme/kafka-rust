//! Fetch message operations for [`KafkaClient`].
//!
//! Implements the logic for sending fetch requests to Kafka brokers,
//! grouping partitions by their leader broker, and aggregating responses.

use std::collections::HashMap;
use std::time::Instant;

use crate::error::{Error, KafkaCode, Result};

use super::FetchPartition;
use super::config::ClientConfig;
use super::state::ClientState;
use super::transport;
use crate::network::Connections;
use crate::protocol::api_versions::ApiVersionCache;

#[tracing::instrument(skip(conn_pool, state, config, input))]
pub fn fetch_messages_kp<'a, I, J>(
    conn_pool: &mut Connections,
    state: &mut ClientState,
    config: &ClientConfig,
    api_versions: &ApiVersionCache,
    correlation: i32,
    input: I,
) -> Result<Vec<super::fetch_kp::OwnedFetchResponse>>
where
    J: AsRef<FetchPartition<'a>>,
    I: IntoIterator<Item = J>,
{
    #[cfg(feature = "metrics")]
    let start = Instant::now();

    let mut broker_partitions: HashMap<&str, Vec<(&str, i32, i64, i32)>> = HashMap::new();
    for inp in input {
        let inp = inp.as_ref();
        let broker = state
            .find_broker(inp.topic, inp.partition)
            .ok_or(Error::Kafka(KafkaCode::UnknownTopicOrPartition))?;
        broker_partitions.entry(broker).or_default().push((
            inp.topic,
            inp.partition,
            inp.offset,
            if inp.max_bytes > 0 {
                inp.max_bytes
            } else {
                config.fetch_max_bytes_per_partition()
            },
        ));
    }

    let result = fetch_messages_inner(
        conn_pool,
        correlation,
        &config.client_id,
        config.fetch_max_wait_time(),
        config.fetch_min_bytes(),
        api_versions,
        broker_partitions,
    );

    #[cfg(feature = "metrics")]
    {
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        match &result {
            Ok(responses) => {
                for resp in responses {
                    for t in &resp.topics {
                        let mut total_bytes: usize = 0;
                        let mut total_messages: usize = 0;
                        for p in &t.partitions {
                            if let Ok(data) = p.data() {
                                total_messages += data.messages.len();
                                for msg in &data.messages {
                                    total_bytes += msg.key.len() + msg.value.len();
                                }
                            }
                        }
                        crate::metrics::record_fetch(
                            &t.topic,
                            total_bytes,
                            total_messages,
                            elapsed,
                        );
                    }
                }
            }
            Err(e) => {
                let error_type = format!("{e:?}");
                crate::metrics::record_fetch_error("_unknown", &error_type);
            }
        }
    }

    result
}

fn fetch_messages_inner(
    conn_pool: &mut Connections,
    correlation_id: i32,
    client_id: &str,
    max_wait_ms: i32,
    min_bytes: i32,
    api_versions: &ApiVersionCache,
    broker_partitions: HashMap<&str, Vec<(&str, i32, i64, i32)>>,
) -> Result<Vec<crate::protocol::fetch::OwnedFetchResponse>> {
    let now = Instant::now();
    let mut res = Vec::with_capacity(broker_partitions.len());
    for (host, partitions) in broker_partitions {
        let conn = conn_pool
            .get_conn(host, now)
            .map_err(|e| e.with_broker_context(host, "Fetch"))?;
        let (mut header, request) = crate::protocol::fetch::build_fetch_request(
            correlation_id,
            client_id,
            -1,
            max_wait_ms,
            min_bytes,
            0x7fff_ffff,
            &partitions,
        );
        let api_version = transport::apply_request_api_version(
            api_versions,
            host,
            &mut header,
            crate::protocol::API_VERSION_FETCH,
        );
        transport::kp_send_request(conn, &header, &request, api_version)
            .map_err(|e| e.with_broker_context(host, "Fetch"))?;
        let kp_resp = transport::kp_get_response::<kafka_protocol::messages::FetchResponse>(
            conn,
            api_version,
        )
        .map_err(|e| e.with_broker_context(host, "Fetch"))?;
        let owned = crate::protocol::fetch::convert_fetch_response(kp_resp, correlation_id);
        res.push(owned);
    }
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::metadata::{
        BrokerMetadata, MetadataResponseData, PartitionMetadata, TopicMetadata,
    };
    use std::io::ErrorKind;
    use std::net::TcpListener;

    #[test]
    fn unknown_fetch_input_is_rejected_before_contacting_any_valid_broker() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut client = crate::client::KafkaClient::builder()
            .with_conn_rw_timeout(1)
            .build();
        client.state.update_metadata(MetadataResponseData {
            brokers: vec![BrokerMetadata {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: i32::from(address.port()),
            }],
            topics: vec![TopicMetadata {
                topic: "known".into(),
                partitions: vec![PartitionMetadata {
                    id: 0,
                    leader: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        for unknown in [
            FetchPartition::new("unknown", 0, 0),
            FetchPartition::new("known", 1, 0),
        ] {
            let result = client.fetch_messages([FetchPartition::new("known", 0, 0), unknown]);
            assert!(matches!(
                result,
                Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition))
            ));
            assert_eq!(listener.accept().unwrap_err().kind(), ErrorKind::WouldBlock);
        }
    }
}
