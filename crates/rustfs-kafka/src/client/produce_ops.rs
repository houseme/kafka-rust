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
use crate::protocol::api_versions::ApiVersionCache;

use super::config::ClientConfig;
use super::state::ClientState;
use super::transport;
use super::{ProduceConfirm, ProduceMessage, RequiredAcks};
use crate::network::Connections;

type BrokerMessage<'a, 'b> = (
    &'a str,
    i32,
    Option<&'b [u8]>,
    Option<&'b [u8]>,
    &'b [(String, bytes::Bytes)],
);
type BrokerMessages<'a, 'b> = HashMap<String, Vec<BrokerMessage<'a, 'b>>>;

struct ProduceRequestContext<'a> {
    conn_pool: &'a mut Connections,
    correlation_id: i32,
    client_id: &'a str,
    required_acks: i16,
    ack_timeout_ms: i32,
    compression: Compression,
    api_versions: &'a ApiVersionCache,
    no_acks: bool,
}

#[tracing::instrument(skip(conn_pool, state, config, messages), fields(acks = ?acks))]
pub(crate) fn internal_produce_messages_kp<'a, 'b, I, J>(
    conn_pool: &mut Connections,
    state: &mut ClientState,
    config: &ClientConfig,
    api_versions: &ApiVersionCache,
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
    let mut broker_msgs: BrokerMessages<'a, 'b> = HashMap::new();
    #[cfg(feature = "metrics")]
    let mut total_bytes: usize = 0;
    #[cfg(feature = "metrics")]
    let mut message_count: usize = 0;
    for msg in messages {
        let msg = msg.as_ref();
        #[cfg(feature = "metrics")]
        {
            total_bytes += msg.value.map_or(0, <[u8]>::len);
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

    let mut ctx = ProduceRequestContext {
        conn_pool,
        correlation_id: correlation,
        client_id: &config.client_id,
        required_acks: acks as i16,
        ack_timeout_ms: protocol::to_millis_i32(ack_timeout)?,
        compression: config.compression,
        api_versions,
        no_acks: acks as i16 == 0,
    };
    let result = produce_messages_inner(&mut ctx, broker_msgs);

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
                let error_type = format!("{e:?}");
                crate::metrics::record_produce_error("_unknown", &error_type);
            }
        }
    }

    result
}

fn produce_messages_inner(
    ctx: &mut ProduceRequestContext<'_>,
    broker_msgs: BrokerMessages<'_, '_>,
) -> Result<Vec<ProduceConfirm>> {
    let now = Instant::now();
    let mut res: Vec<ProduceConfirm> = Vec::new();

    for (host, msgs) in broker_msgs {
        let conn = ctx
            .conn_pool
            .get_conn(&host, now)
            .map_err(|e| e.with_broker_context(&host, "Produce"))?;
        let (mut header, request) = crate::protocol::produce::build_produce_request(
            ctx.correlation_id,
            ctx.client_id,
            ctx.required_acks,
            ctx.ack_timeout_ms,
            ctx.compression,
            &msgs,
        )?;
        let api_version = transport::apply_request_api_version(
            ctx.api_versions,
            &host,
            &mut header,
            crate::protocol::API_VERSION_PRODUCE,
        );
        transport::kp_send_request(conn, &header, &request, api_version)
            .map_err(|e| e.with_broker_context(&host, "Produce"))?;

        if ctx.no_acks {
            continue;
        }

        let kp_resp = transport::kp_get_response::<kafka_protocol::messages::ProduceResponse>(
            conn,
            api_version,
        )
        .map_err(|e| e.with_broker_context(&host, "Produce"))?;
        let our_resp =
            crate::protocol::produce::convert_produce_response(kp_resp, ctx.correlation_id);
        res.extend(our_resp.get_response());
    }

    Ok(res)
}
