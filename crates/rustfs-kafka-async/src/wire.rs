//! Shared async Kafka wire-protocol helpers.

use bytes::Bytes;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};
use rustfs_kafka::client::{decode_response_payload, encode_request_frame};
use rustfs_kafka::error::{Error, KafkaCode, ProtocolError, Result};

use crate::connection::AsyncConnection;

pub(crate) async fn send_kp_request<T>(
    conn: &mut AsyncConnection,
    header: &RequestHeader,
    body: &T,
    api_version: i16,
) -> Result<()>
where
    T: Encodable + HeaderVersion,
{
    let out = encode_kp_request(header, body, api_version)?;
    conn.send(&out).await
}

pub(crate) async fn get_kp_response<R>(conn: &mut AsyncConnection, api_version: i16) -> Result<R>
where
    R: Decodable + HeaderVersion,
{
    let size_bytes = conn.read_exact(4).await?;
    let size = i32::from_be_bytes(
        <[u8; 4]>::try_from(size_bytes.as_ref())
            .map_err(|_| Error::Protocol(ProtocolError::Codec))?,
    );
    let bytes = conn.read_exact(non_negative_i32_to_u64(size)?).await?;
    decode_kp_response(bytes, api_version)
}

pub(crate) fn encode_kp_request<T>(
    header: &RequestHeader,
    body: &T,
    api_version: i16,
) -> Result<Bytes>
where
    T: Encodable + HeaderVersion,
{
    encode_request_frame(header, body, api_version)
}

pub(crate) fn decode_kp_response<R>(bytes: Bytes, api_version: i16) -> Result<R>
where
    R: Decodable + HeaderVersion,
{
    decode_response_payload(bytes, api_version)
}

pub(crate) fn non_negative_i32_to_usize(value: i32) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::Protocol(ProtocolError::Codec))
}

pub(crate) fn non_negative_i32_to_u64(value: i32) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::Protocol(ProtocolError::Codec))
}

pub(crate) fn kafka_code_from_protocol(code: i16) -> Option<KafkaCode> {
    match code {
        0 => None,
        1 => Some(KafkaCode::OffsetOutOfRange),
        2 => Some(KafkaCode::CorruptMessage),
        3 => Some(KafkaCode::UnknownTopicOrPartition),
        4 => Some(KafkaCode::InvalidMessageSize),
        5 => Some(KafkaCode::LeaderNotAvailable),
        6 => Some(KafkaCode::NotLeaderForPartition),
        7 => Some(KafkaCode::RequestTimedOut),
        8 => Some(KafkaCode::BrokerNotAvailable),
        9 => Some(KafkaCode::ReplicaNotAvailable),
        10 => Some(KafkaCode::MessageSizeTooLarge),
        11 => Some(KafkaCode::StaleControllerEpoch),
        12 => Some(KafkaCode::OffsetMetadataTooLarge),
        13 => Some(KafkaCode::NetworkException),
        14 => Some(KafkaCode::GroupLoadInProgress),
        15 => Some(KafkaCode::GroupCoordinatorNotAvailable),
        16 => Some(KafkaCode::NotCoordinatorForGroup),
        17 => Some(KafkaCode::InvalidTopic),
        18 => Some(KafkaCode::RecordListTooLarge),
        19 => Some(KafkaCode::NotEnoughReplicas),
        20 => Some(KafkaCode::NotEnoughReplicasAfterAppend),
        21 => Some(KafkaCode::InvalidRequiredAcks),
        22 => Some(KafkaCode::IllegalGeneration),
        23 => Some(KafkaCode::InconsistentGroupProtocol),
        24 => Some(KafkaCode::InvalidGroupId),
        25 => Some(KafkaCode::UnknownMemberId),
        26 => Some(KafkaCode::InvalidSessionTimeout),
        27 => Some(KafkaCode::RebalanceInProgress),
        28 => Some(KafkaCode::InvalidCommitOffsetSize),
        29 => Some(KafkaCode::TopicAuthorizationFailed),
        30 => Some(KafkaCode::GroupAuthorizationFailed),
        31 => Some(KafkaCode::ClusterAuthorizationFailed),
        32 => Some(KafkaCode::InvalidTimestamp),
        33 => Some(KafkaCode::UnsupportedSaslMechanism),
        34 => Some(KafkaCode::IllegalSaslState),
        35 => Some(KafkaCode::UnsupportedVersion),
        _ => Some(KafkaCode::Unknown),
    }
}

pub(crate) fn kafka_code_to_protocol(code: KafkaCode) -> i16 {
    match code {
        KafkaCode::OffsetOutOfRange => 1,
        KafkaCode::CorruptMessage => 2,
        KafkaCode::UnknownTopicOrPartition => 3,
        KafkaCode::InvalidMessageSize => 4,
        KafkaCode::LeaderNotAvailable => 5,
        KafkaCode::NotLeaderForPartition => 6,
        KafkaCode::RequestTimedOut => 7,
        KafkaCode::BrokerNotAvailable => 8,
        KafkaCode::ReplicaNotAvailable => 9,
        KafkaCode::MessageSizeTooLarge => 10,
        KafkaCode::StaleControllerEpoch => 11,
        KafkaCode::OffsetMetadataTooLarge => 12,
        KafkaCode::NetworkException => 13,
        KafkaCode::GroupLoadInProgress => 14,
        KafkaCode::GroupCoordinatorNotAvailable => 15,
        KafkaCode::NotCoordinatorForGroup => 16,
        KafkaCode::InvalidTopic => 17,
        KafkaCode::RecordListTooLarge => 18,
        KafkaCode::NotEnoughReplicas => 19,
        KafkaCode::NotEnoughReplicasAfterAppend => 20,
        KafkaCode::InvalidRequiredAcks => 21,
        KafkaCode::IllegalGeneration => 22,
        KafkaCode::InconsistentGroupProtocol => 23,
        KafkaCode::InvalidGroupId => 24,
        KafkaCode::UnknownMemberId => 25,
        KafkaCode::InvalidSessionTimeout => 26,
        KafkaCode::RebalanceInProgress => 27,
        KafkaCode::InvalidCommitOffsetSize => 28,
        KafkaCode::TopicAuthorizationFailed => 29,
        KafkaCode::GroupAuthorizationFailed => 30,
        KafkaCode::ClusterAuthorizationFailed => 31,
        KafkaCode::InvalidTimestamp => 32,
        KafkaCode::UnsupportedSaslMechanism => 33,
        KafkaCode::IllegalSaslState => 34,
        KafkaCode::UnsupportedVersion => 35,
        KafkaCode::Unknown => -1,
    }
}

pub(crate) fn kafka_error_from_protocol_code(code: i16) -> Error {
    Error::Kafka(kafka_code_from_protocol(code).unwrap_or(KafkaCode::Unknown))
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use kafka_protocol::messages::{ApiKey, ApiVersionsRequest};
    use kafka_protocol::protocol::StrBytes;

    use super::*;

    #[test]
    fn encode_kp_request_prefixes_size_and_header() {
        let header = RequestHeader::default()
            .with_client_id(Some(StrBytes::from_static_str("test-client")))
            .with_request_api_key(ApiKey::ApiVersions as i16)
            .with_request_api_version(0)
            .with_correlation_id(42);
        let request = ApiVersionsRequest::default();

        let frame = encode_kp_request(&header, &request, 0).unwrap();
        let mut bytes = frame.clone();
        let declared_len = bytes.get_i32();

        assert_eq!(declared_len as usize, frame.len() - 4);

        let decoded_header =
            RequestHeader::decode(&mut bytes, ApiVersionsRequest::header_version(0)).unwrap();
        assert_eq!(decoded_header.request_api_key, ApiKey::ApiVersions as i16);
        assert_eq!(decoded_header.request_api_version, 0);
        assert_eq!(decoded_header.correlation_id, 42);
        assert_eq!(
            decoded_header.client_id.as_ref().map(ToString::to_string),
            Some("test-client".to_owned())
        );

        assert!(!bytes.has_remaining());
    }

    #[test]
    fn negative_lengths_are_rejected() {
        assert!(non_negative_i32_to_usize(-1).is_err());
        assert!(non_negative_i32_to_u64(-1).is_err());
    }

    #[test]
    fn kafka_code_mapping_handles_success_known_and_unknown_codes() {
        assert_eq!(kafka_code_from_protocol(0), None);
        assert_eq!(
            kafka_code_from_protocol(3),
            Some(KafkaCode::UnknownTopicOrPartition)
        );
        assert_eq!(kafka_code_from_protocol(99), Some(KafkaCode::Unknown));
        assert_eq!(kafka_code_to_protocol(KafkaCode::NotLeaderForPartition), 6);
        assert_eq!(kafka_code_to_protocol(KafkaCode::Unknown), -1);
    }
}
