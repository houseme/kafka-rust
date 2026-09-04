//! Low-level Kafka protocol transport utilities.
//!
//! Provides functions for sending requests and receiving responses over
//! a [`KafkaConnection`], handling the Kafka wire protocol frame format
//! (4-byte length prefix + header + body).

use tracing::trace;

use crate::error::Result;

use crate::network::KafkaConnection;

pub(crate) fn apply_request_api_version(
    api_versions: &crate::protocol::api_versions::ApiVersionCache,
    host: &str,
    header: &mut kafka_protocol::messages::RequestHeader,
    fallback: i16,
) -> i16 {
    let api_version = api_versions.negotiate(host, header.request_api_key, fallback);
    header.request_api_version = api_version;
    api_version
}

pub(crate) fn kp_send_request<T>(
    conn: &mut KafkaConnection,
    header: &kafka_protocol::messages::RequestHeader,
    body: &T,
    api_version: i16,
) -> Result<()>
where
    T: kafka_protocol::protocol::Encodable + kafka_protocol::protocol::HeaderVersion,
{
    let out = crate::protocol::encode_request_frame(header, body, api_version)?;
    trace!("kp_send_request: sending {} bytes", out.len());
    conn.send(&out)?;
    Ok(())
}

pub(crate) fn kp_get_response<
    R: kafka_protocol::protocol::Decodable + kafka_protocol::protocol::HeaderVersion,
>(
    conn: &mut KafkaConnection,
    api_version: i16,
) -> Result<R> {
    let size = get_response_size(conn)?;
    let bytes = conn.read_exact_alloc(crate::protocol::non_negative_i32_to_u64(size)?)?;
    crate::protocol::decode_response_payload(bytes, api_version)
}

pub(crate) fn get_response_size(conn: &mut KafkaConnection) -> Result<i32> {
    let mut buf = [0u8; 4];
    conn.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}
