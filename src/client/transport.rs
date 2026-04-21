//! Low-level Kafka protocol transport utilities.
//!
//! Provides functions for sending requests and receiving responses over
//! a [`KafkaConnection`], handling the Kafka wire protocol frame format
//! (4-byte length prefix + header + body).

use tracing::trace;

use crate::error::{Error, Result};

use crate::network::KafkaConnection;

pub(crate) fn kp_send_request(
    conn: &mut KafkaConnection,
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

    let total_len = (header_buf.len() + body_buf.len()) as i32;
    let mut out = BytesMut::with_capacity(4 + total_len as usize);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body_buf);

    trace!("kp_send_request: sending {} bytes", out.len());
    conn.send(&out)?;
    Ok(())
}

pub(crate) fn kp_get_response<R: kafka_protocol::protocol::Decodable>(
    conn: &mut KafkaConnection,
    api_version: i16,
) -> Result<R> {
    use bytes::Bytes;
    use kafka_protocol::messages::ResponseHeader;
    use kafka_protocol::protocol::Decodable;

    let size = get_response_size(conn)?;
    let resp_bytes = conn.read_exact_alloc(size as u64)?;

    let mut bytes = Bytes::from(resp_bytes);
    let _resp_header =
        ResponseHeader::decode(&mut bytes, api_version).map_err(|_| Error::codec())?;

    R::decode(&mut bytes, api_version).map_err(|_| Error::codec())
}

pub(crate) fn get_response_size(conn: &mut KafkaConnection) -> Result<i32> {
    let mut buf = [0u8; 4];
    conn.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}
