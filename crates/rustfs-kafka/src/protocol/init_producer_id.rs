//! `InitProducerId` protocol (API key 22) for idempotent/transactional producer support.

use bytes::BytesMut;
use kafka_protocol::messages::{
    InitProducerIdRequest, InitProducerIdResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::protocol::{Decodable, Encodable};

use crate::error::{Error, Result};
use crate::network::KafkaConnection;

pub const API_VERSION_INIT_PRODUCER_ID: i16 = 2;

/// Parsed response from an `InitProducerId` request.
#[derive(Debug, Clone)]
pub struct InitProducerIdResponseData {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub error_code: i16,
}

impl InitProducerIdResponseData {
    pub fn from_response(resp: &InitProducerIdResponse) -> Self {
        Self {
            producer_id: i64::from(resp.producer_id),
            producer_epoch: resp.producer_epoch,
            error_code: resp.error_code,
        }
    }
}

/// Build and send an `InitProducerId` request, returning the parsed response.
pub fn fetch_init_producer_id(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
    transactional_id: Option<&str>,
) -> Result<InitProducerIdResponseData> {
    let version = API_VERSION_INIT_PRODUCER_ID;

    let mut req = InitProducerIdRequest::default();
    if let Some(tid) = transactional_id {
        req = req.with_transactional_id(Some(kafka_protocol::messages::TransactionalId(
            StrBytes::from_string(tid.to_owned()),
        )));
    }

    let header = RequestHeader::default()
        .with_request_api_key(22)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, version)
        .map_err(|_| Error::codec())?;

    let mut body_buf = BytesMut::new();
    req.encode(&mut body_buf, version)
        .map_err(|_| Error::codec())?;

    let total_len = crate::protocol::usize_to_i32(header_buf.len() + body_buf.len())?;
    let out_len = crate::protocol::non_negative_i32_to_usize(total_len)?;
    let mut out = BytesMut::with_capacity(4 + out_len);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body_buf);

    conn.send(&out)?;

    let size = {
        let mut buf = [0u8; 4];
        conn.read_exact(&mut buf)?;
        i32::from_be_bytes(buf)
    };
    let resp_bytes = conn.read_exact_alloc(crate::protocol::non_negative_i32_to_u64(size)?)?;
    let mut bytes = resp_bytes;
    let _resp_header = ResponseHeader::decode(&mut bytes, version).map_err(|_| Error::codec())?;

    let kp_resp =
        InitProducerIdResponse::decode(&mut bytes, version).map_err(|_| Error::codec())?;
    Ok(InitProducerIdResponseData::from_response(&kp_resp))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_producer_id_response_data_from_response() {
        let resp = InitProducerIdResponse::default()
            .with_producer_id(kafka_protocol::messages::ProducerId(12345))
            .with_producer_epoch(1)
            .with_error_code(0);
        let data = InitProducerIdResponseData::from_response(&resp);
        assert_eq!(data.producer_id, 12345);
        assert_eq!(data.producer_epoch, 1);
        assert_eq!(data.error_code, 0);
    }
}
