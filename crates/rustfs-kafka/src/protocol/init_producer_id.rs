//! `InitProducerId` protocol (API key 22) for idempotent/transactional producer support.

use kafka_protocol::messages::{
    ApiKey, InitProducerIdRequest, InitProducerIdResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use crate::error::Result;
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
        .with_request_api_key(ApiKey::InitProducerId as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    let out = crate::protocol::encode_request_frame(&header, &req, version)?;

    conn.send(&out)?;

    let size = {
        let mut buf = [0u8; 4];
        conn.read_exact(&mut buf)?;
        i32::from_be_bytes(buf)
    };
    let resp_bytes = conn.read_exact_alloc(crate::protocol::non_negative_i32_to_u64(size)?)?;
    let kp_resp =
        crate::protocol::decode_response_payload::<InitProducerIdResponse>(resp_bytes, version)?;
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
