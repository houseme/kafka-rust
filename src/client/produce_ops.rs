//! Produce message operations for [`KafkaClient`].
//!
//! Handles sending messages to Kafka brokers, grouping messages by their
//! target broker, and supporting both fire-and-forget (acks=0) and
//! acknowledged produce modes with optional metrics recording.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::compression::Compression;
use crate::error::{Error, KafkaCode, Result};
use crate::protocol;

use super::config::ClientConfig;
use super::state::ClientState;
use super::transport;
use super::{ProduceConfirm, ProduceMessage, RequiredAcks};
use crate::network::Connections;

#[tracing::instrument(skip(conn_pool, state, config, messages), fields(acks = ?acks))]
pub(crate) fn internal_produce_messages_kp<'a, 'b, I, J>(
    conn_pool: &mut Connections,
    state: &mut ClientState,
    config: &ClientConfig,
    acks: RequiredAcks,
    ack_timeout: Duration,
    messages: I,
) -> Result<Vec<ProduceConfirm>>
where
    J: AsRef<ProduceMessage<'a, 'b>>,
    I: IntoIterator<Item = J>,
{
    #[cfg(feature = "metrics")]
    let start = Instant::now();
    let correlation = state.next_correlation_id();

    // Collect messages into (broker, Vec<(topic, partition, key, value, headers)>)
    // We extract broker info first, then bundle with header references.
    let mut broker_msgs: HashMap<
        String,
        Vec<(
            &str,
            i32,
            Option<&'b [u8]>,
            Option<&'b [u8]>,
            &'b [(String, Vec<u8>)],
        )>,
    > = HashMap::new();
    #[cfg(feature = "metrics")]
    let mut total_bytes: usize = 0;
    #[cfg(feature = "metrics")]
    let mut message_count: usize = 0;
    for msg in messages {
        let msg = msg.as_ref();
        #[cfg(feature = "metrics")]
        {
            total_bytes += msg.value.map(|v| v.len()).unwrap_or(0);
            message_count += 1;
        }
        let broker = match state.find_broker(msg.topic, msg.partition) {
            None => {
                #[cfg(feature = "metrics")]
                crate::metrics::record_produce_error(msg.topic, "UnknownTopicOrPartition");
                return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
            }
            Some(b) => b.to_owned(),
        };
        broker_msgs.entry(broker).or_default().push((
            msg.topic,
            msg.partition,
            msg.key,
            msg.value,
            msg.headers,
        ));
    }

    let result = produce_messages_inner(
        conn_pool,
        correlation,
        &config.client_id,
        acks as i16,
        protocol::to_millis_i32(ack_timeout)?,
        config.compression,
        broker_msgs,
        acks as i16 == 0,
    );

    #[cfg(feature = "metrics")]
    {
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        match &result {
            Ok(confirms) => {
                for confirm in confirms {
                    crate::metrics::record_produce(
                        &confirm.topic,
                        total_bytes,
                        message_count,
                        elapsed,
                    );
                }
                if confirms.is_empty() && message_count > 0 {
                    // no-acks mode: record without specific topic
                    crate::metrics::record_produce("_unknown", total_bytes, message_count, elapsed);
                }
            }
            Err(e) => {
                let error_type = format!("{:?}", e);
                crate::metrics::record_produce_error("_unknown", &error_type);
            }
        }
    }

    result
}

#[allow(clippy::too_many_arguments)]
fn produce_messages_inner(
    conn_pool: &mut Connections,
    correlation_id: i32,
    client_id: &str,
    required_acks: i16,
    ack_timeout_ms: i32,
    compression: Compression,
    broker_msgs: HashMap<
        String,
        Vec<(
            &str,
            i32,
            Option<&[u8]>,
            Option<&[u8]>,
            &[(String, Vec<u8>)],
        )>,
    >,
    no_acks: bool,
) -> Result<Vec<ProduceConfirm>> {
    let now = Instant::now();
    if no_acks {
        for (host, msgs) in broker_msgs {
            let conn = conn_pool
                .get_conn(&host, now)
                .map_err(|e| e.with_broker_context(&host, "Produce"))?;
            let (header, request) = crate::protocol::produce::build_produce_request(
                correlation_id,
                client_id,
                required_acks,
                ack_timeout_ms,
                compression,
                &msgs,
            );
            transport::kp_send_request(
                conn,
                &header,
                &request,
                crate::protocol::API_VERSION_PRODUCE,
            )
            .map_err(|e| e.with_broker_context(&host, "Produce"))?;
        }
        Ok(vec![])
    } else {
        let mut res: Vec<ProduceConfirm> = vec![];
        for (host, msgs) in broker_msgs {
            let conn = conn_pool
                .get_conn(&host, now)
                .map_err(|e| e.with_broker_context(&host, "Produce"))?;
            let (header, request) = crate::protocol::produce::build_produce_request(
                correlation_id,
                client_id,
                required_acks,
                ack_timeout_ms,
                compression,
                &msgs,
            );
            transport::kp_send_request(
                conn,
                &header,
                &request,
                crate::protocol::API_VERSION_PRODUCE,
            )
            .map_err(|e| e.with_broker_context(&host, "Produce"))?;
            let kp_resp = transport::kp_get_response::<kafka_protocol::messages::ProduceResponse>(
                conn,
                crate::protocol::API_VERSION_PRODUCE,
            )
            .map_err(|e| e.with_broker_context(&host, "Produce"))?;
            let our_resp =
                crate::protocol::produce::convert_produce_response(kp_resp, correlation_id);
            for tpo in our_resp.get_response() {
                res.push(tpo);
            }
        }
        Ok(res)
    }
}
