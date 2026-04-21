use std::collections::HashMap;
use std::time::Instant;

use crate::error::Result;

use super::FetchPartition;
use super::config::ClientConfig;
use super::state::ClientState;
use super::transport;
use crate::network::Connections;

#[tracing::instrument(skip(conn_pool, state, config, input))]
pub fn fetch_messages_kp<'a, I, J>(
    conn_pool: &mut Connections,
    state: &mut ClientState,
    config: &ClientConfig,
    correlation: i32,
    input: I,
) -> Result<Vec<super::fetch_kp::OwnedFetchResponse>>
where
    J: AsRef<FetchPartition<'a>>,
    I: IntoIterator<Item = J>,
{
    let start = Instant::now();

    let mut broker_partitions: HashMap<&str, Vec<(&str, i32, i64, i32)>> = HashMap::new();
    for inp in input {
        let inp = inp.as_ref();
        if let Some(broker) = state.find_broker(inp.topic, inp.partition) {
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
    }

    let result = __fetch_messages_kp(
        conn_pool,
        correlation,
        &config.client_id,
        config.fetch_max_wait_time(),
        config.fetch_min_bytes(),
        broker_partitions,
    );

    #[cfg(feature = "metrics")]
    {
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        match &result {
            Ok(responses) => {
                let mut total_bytes: usize = 0;
                let mut total_messages: usize = 0;
                for resp in responses {
                    for t in &resp.topics {
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
                let error_type = format!("{:?}", e);
                crate::metrics::record_fetch_error("_unknown", &error_type);
            }
        }
    }

    result
}

fn __fetch_messages_kp(
    conn_pool: &mut Connections,
    correlation_id: i32,
    client_id: &str,
    max_wait_ms: i32,
    min_bytes: i32,
    broker_partitions: HashMap<&str, Vec<(&str, i32, i64, i32)>>,
) -> Result<Vec<crate::protocol::fetch::OwnedFetchResponse>> {
    let now = Instant::now();
    let mut res = Vec::with_capacity(broker_partitions.len());
    for (host, partitions) in broker_partitions {
        let conn = conn_pool
            .get_conn(host, now)
            .map_err(|e| e.with_broker_context(host, "Fetch"))?;
        let (header, request) = crate::protocol::fetch::build_fetch_request(
            correlation_id,
            client_id,
            -1,
            max_wait_ms,
            min_bytes,
            0x7fff_ffff,
            &partitions,
        );
        transport::kp_send_request(conn, &header, &request, crate::protocol::API_VERSION_FETCH)
            .map_err(|e| e.with_broker_context(host, "Fetch"))?;
        let kp_resp = transport::kp_get_response::<kafka_protocol::messages::FetchResponse>(
            conn,
            crate::protocol::API_VERSION_FETCH,
        )
        .map_err(|e| e.with_broker_context(host, "Fetch"))?;
        let owned = crate::protocol::fetch::convert_fetch_response(kp_resp, correlation_id);
        res.push(owned);
    }
    Ok(res)
}
