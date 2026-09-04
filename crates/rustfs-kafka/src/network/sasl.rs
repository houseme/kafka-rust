//! SASL authentication logic for Kafka connections.
//!
//! Supports PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 mechanisms.

use std::collections::HashMap;
use std::io::{Read, Write};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::{Bytes, BytesMut};
use hmac::{Hmac, Mac};
use kafka_protocol::messages::{
    ApiKey, RequestHeader, ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use pbkdf2::pbkdf2_hmac;
use rand::distr::{Alphanumeric, SampleString};
use sha2::{Digest, Sha256, Sha512};

use super::connection::SaslConfig;
use super::connection::KafkaStream;
use crate::error::{Error, KafkaCode, ProtocolError, Result};

const API_VERSION_SASL_HANDSHAKE: i16 = 1;
const API_VERSION_SASL_AUTHENTICATE: i16 = 1;
const DEFAULT_CLIENT_ID: &str = "rustfs-kafka";

#[derive(Clone, Copy)]
enum ScramAlgorithm {
    Sha256,
    Sha512,
}

pub(crate) fn perform_sasl_authentication(stream: &mut KafkaStream, sasl: &SaslConfig) -> Result<()> {
    let mechanism = sasl.mechanism().to_owned();
    let correlation_id = 1;

    let handshake_header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(DEFAULT_CLIENT_ID.to_owned())))
        .with_request_api_key(ApiKey::SaslHandshake as i16)
        .with_request_api_version(API_VERSION_SASL_HANDSHAKE)
        .with_correlation_id(correlation_id);
    let handshake_request =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_string(mechanism.clone()));

    send_kp_request_on_stream(
        stream,
        &handshake_header,
        &handshake_request,
        API_VERSION_SASL_HANDSHAKE,
    )?;
    let handshake_response: SaslHandshakeResponse =
        get_kp_response_from_stream(stream, API_VERSION_SASL_HANDSHAKE)?;

    if handshake_response.error_code != 0 {
        return Err(map_kafka_code_or_unknown(handshake_response.error_code));
    }

    if !handshake_response.mechanisms.is_empty()
        && !handshake_response
            .mechanisms
            .iter()
            .any(|m| m.as_str().eq_ignore_ascii_case(&mechanism))
    {
        return Err(Error::Kafka(KafkaCode::UnsupportedSaslMechanism));
    }

    if mechanism.eq_ignore_ascii_case("PLAIN") {
        return perform_sasl_plain_authenticate(stream, sasl, correlation_id + 1);
    }
    if mechanism.eq_ignore_ascii_case("SCRAM-SHA-256") {
        return perform_sasl_scram_authenticate(
            stream,
            sasl,
            ScramAlgorithm::Sha256,
            correlation_id + 1,
        );
    }
    if mechanism.eq_ignore_ascii_case("SCRAM-SHA-512") {
        return perform_sasl_scram_authenticate(
            stream,
            sasl,
            ScramAlgorithm::Sha512,
            correlation_id + 1,
        );
    }

    Err(Error::Config(format!(
        "unsupported SASL mechanism for sync path: {}",
        sasl.mechanism()
    )))
}

fn perform_sasl_plain_authenticate(
    stream: &mut KafkaStream,
    sasl: &SaslConfig,
    correlation_id: i32,
) -> Result<()> {
    let auth_header = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(DEFAULT_CLIENT_ID.to_owned())))
        .with_request_api_key(ApiKey::SaslAuthenticate as i16)
        .with_request_api_version(API_VERSION_SASL_AUTHENTICATE)
        .with_correlation_id(correlation_id);
    let auth_request =
        SaslAuthenticateRequest::default().with_auth_bytes(build_sasl_plain_auth_bytes(sasl));

    send_kp_request_on_stream(
        stream,
        &auth_header,
        &auth_request,
        API_VERSION_SASL_AUTHENTICATE,
    )?;
    let auth_response: SaslAuthenticateResponse =
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE)?;

    if auth_response.error_code != 0 {
        return Err(map_kafka_code_or_unknown(auth_response.error_code));
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
fn perform_sasl_scram_authenticate(
    stream: &mut KafkaStream,
    sasl: &SaslConfig,
    algorithm: ScramAlgorithm,
    correlation_id: i32,
) -> Result<()> {
    let client_nonce = generate_scram_nonce();
    let user = scram_escape_username(sasl.username());
    let client_first_bare = format!("n={user},r={client_nonce}");
    let client_first = format!("n,,{client_first_bare}");

    let auth_header_1 = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(DEFAULT_CLIENT_ID.to_owned())))
        .with_request_api_key(ApiKey::SaslAuthenticate as i16)
        .with_request_api_version(API_VERSION_SASL_AUTHENTICATE)
        .with_correlation_id(correlation_id);
    let auth_request_1 =
        SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(client_first));
    send_kp_request_on_stream(
        stream,
        &auth_header_1,
        &auth_request_1,
        API_VERSION_SASL_AUTHENTICATE,
    )?;
    let auth_response_1: SaslAuthenticateResponse =
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE)?;
    if auth_response_1.error_code != 0 {
        return Err(map_kafka_code_or_unknown(auth_response_1.error_code));
    }

    let server_first =
        std::str::from_utf8(&auth_response_1.auth_bytes).map_err(|_| Error::codec())?;
    let server_first_attrs = parse_scram_attributes(server_first)?;
    if let Some(err_msg) = server_first_attrs.get("e") {
        return Err(Error::Config(format!("SCRAM server error: {err_msg}")));
    }

    let server_nonce = server_first_attrs
        .get("r")
        .ok_or_else(|| Error::Config("SCRAM challenge missing nonce".to_owned()))?;
    if !server_nonce.starts_with(&client_nonce) {
        return Err(Error::Config(
            "SCRAM server nonce does not include client nonce prefix".to_owned(),
        ));
    }
    let salt_b64 = server_first_attrs
        .get("s")
        .ok_or_else(|| Error::Config("SCRAM challenge missing salt".to_owned()))?;
    let salt = BASE64
        .decode(salt_b64)
        .map_err(|e| Error::Config(format!("invalid SCRAM salt encoding: {e}")))?;
    let iterations = server_first_attrs
        .get("i")
        .ok_or_else(|| Error::Config("SCRAM challenge missing iterations".to_owned()))?
        .parse::<u32>()
        .map_err(|e| Error::Config(format!("invalid SCRAM iterations: {e}")))?;
    if iterations == 0 {
        return Err(Error::Config(
            "invalid SCRAM iterations: must be > 0".to_owned(),
        ));
    }

    let client_final_without_proof = format!("c=biws,r={server_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");
    let (client_proof, expected_server_signature) = compute_scram_proof_and_server_signature(
        algorithm,
        sasl.password(),
        &salt,
        iterations,
        &auth_message,
    )?;

    let client_final = format!(
        "{client_final_without_proof},p={}",
        BASE64.encode(client_proof)
    );
    let auth_header_2 = RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(DEFAULT_CLIENT_ID.to_owned())))
        .with_request_api_key(ApiKey::SaslAuthenticate as i16)
        .with_request_api_version(API_VERSION_SASL_AUTHENTICATE)
        .with_correlation_id(correlation_id + 1);
    let auth_request_2 =
        SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(client_final));
    send_kp_request_on_stream(
        stream,
        &auth_header_2,
        &auth_request_2,
        API_VERSION_SASL_AUTHENTICATE,
    )?;
    let auth_response_2: SaslAuthenticateResponse =
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE)?;
    if auth_response_2.error_code != 0 {
        return Err(map_kafka_code_or_unknown(auth_response_2.error_code));
    }

    let server_final =
        std::str::from_utf8(&auth_response_2.auth_bytes).map_err(|_| Error::codec())?;
    let server_final_attrs = parse_scram_attributes(server_final)?;
    if let Some(err_msg) = server_final_attrs.get("e") {
        return Err(Error::Config(format!(
            "SCRAM authentication failed: {err_msg}"
        )));
    }
    let server_signature_b64 = server_final_attrs
        .get("v")
        .ok_or_else(|| Error::Config("SCRAM final message missing server signature".to_owned()))?;
    let server_signature = BASE64
        .decode(server_signature_b64)
        .map_err(|e| Error::Config(format!("invalid SCRAM server signature encoding: {e}")))?;
    if server_signature != expected_server_signature {
        return Err(Error::Config(
            "SCRAM server signature verification failed".to_owned(),
        ));
    }

    Ok(())
}

fn build_sasl_plain_auth_bytes(sasl: &SaslConfig) -> Bytes {
    let mut payload = Vec::with_capacity(sasl.username().len() + sasl.password().len() + 2);
    payload.push(0);
    payload.extend_from_slice(sasl.username().as_bytes());
    payload.push(0);
    payload.extend_from_slice(sasl.password().as_bytes());
    Bytes::from(payload)
}

fn compute_scram_proof_and_server_signature(
    algorithm: ScramAlgorithm,
    password: &str,
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Result<(Vec<u8>, Vec<u8>)> {
    match algorithm {
        ScramAlgorithm::Sha256 => compute_scram_sha256(password, salt, iterations, auth_message),
        ScramAlgorithm::Sha512 => compute_scram_sha512(password, salt, iterations, auth_message),
    }
}

fn compute_scram_sha256(
    password: &str,
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Result<(Vec<u8>, Vec<u8>)> {
    type HmacSha256 = Hmac<Sha256>;

    let mut salted_password = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted_password);
    let client_key = hmac_bytes::<HmacSha256>(&salted_password, b"Client Key")?;
    let stored_key = Sha256::digest(&client_key).to_vec();
    let client_signature = hmac_bytes::<HmacSha256>(&stored_key, auth_message.as_bytes())?;
    let client_proof = xor_bytes(&client_key, &client_signature)?;
    let server_key = hmac_bytes::<HmacSha256>(&salted_password, b"Server Key")?;
    let server_signature = hmac_bytes::<HmacSha256>(&server_key, auth_message.as_bytes())?;
    Ok((client_proof, server_signature))
}

fn compute_scram_sha512(
    password: &str,
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Result<(Vec<u8>, Vec<u8>)> {
    type HmacSha512 = Hmac<Sha512>;

    let mut salted_password = [0u8; 64];
    pbkdf2_hmac::<Sha512>(password.as_bytes(), salt, iterations, &mut salted_password);
    let client_key = hmac_bytes::<HmacSha512>(&salted_password, b"Client Key")?;
    let stored_key = Sha512::digest(&client_key).to_vec();
    let client_signature = hmac_bytes::<HmacSha512>(&stored_key, auth_message.as_bytes())?;
    let client_proof = xor_bytes(&client_key, &client_signature)?;
    let server_key = hmac_bytes::<HmacSha512>(&salted_password, b"Server Key")?;
    let server_signature = hmac_bytes::<HmacSha512>(&server_key, auth_message.as_bytes())?;
    Ok((client_proof, server_signature))
}

fn hmac_bytes<M>(key: &[u8], data: &[u8]) -> Result<Vec<u8>>
where
    M: Mac + hmac::digest::KeyInit,
{
    let mut mac = <M as hmac::digest::KeyInit>::new_from_slice(key)
        .map_err(|e| Error::Config(format!("hmac init failed: {e}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn xor_bytes(left: &[u8], right: &[u8]) -> Result<Vec<u8>> {
    if left.len() != right.len() {
        return Err(Error::Config(
            "SCRAM proof construction failed: buffer length mismatch".to_owned(),
        ));
    }
    Ok(left.iter().zip(right.iter()).map(|(a, b)| a ^ b).collect())
}

fn parse_scram_attributes(input: &str) -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    for part in input.split(',') {
        if part.is_empty() {
            continue;
        }
        let Some((k, v)) = part.split_once('=') else {
            return Err(Error::Config(format!(
                "invalid SCRAM attribute segment: {part}"
            )));
        };
        out.insert(k.to_owned(), v.to_owned());
    }
    Ok(out)
}

fn generate_scram_nonce() -> String {
    Alphanumeric.sample_string(&mut rand::rng(), 24)
}

fn scram_escape_username(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

fn send_kp_request_on_stream<T>(
    stream: &mut KafkaStream,
    header: &RequestHeader,
    body: &T,
    api_version: i16,
) -> Result<()>
where
    T: Encodable + HeaderVersion,
{
    let header_version = T::header_version(api_version);

    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, header_version)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;

    let mut body_buf = BytesMut::new();
    body.encode(&mut body_buf, api_version)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;

    let total_len = i32::try_from(header_buf.len() + body_buf.len())
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;
    let mut out = BytesMut::with_capacity(
        4 + usize::try_from(total_len).map_err(|_| Error::Protocol(ProtocolError::Codec))?,
    );
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body_buf);

    stream.write_all(&out).map_err(Error::from)?;
    stream.flush().map_err(Error::from)
}

fn get_kp_response_from_stream<R>(stream: &mut KafkaStream, api_version: i16) -> Result<R>
where
    R: Decodable + HeaderVersion,
{
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).map_err(Error::from)?;
    let size = i32::from_be_bytes(size_buf);
    if size < 0 {
        return Err(Error::Protocol(ProtocolError::Codec));
    }

    let mut payload = vec![0u8; usize::try_from(size).map_err(|_| Error::codec())?];
    stream.read_exact(&mut payload).map_err(Error::from)?;
    let mut bytes = Bytes::from(payload);

    let response_header_version = R::header_version(api_version);
    let _resp_header = ResponseHeader::decode(&mut bytes, response_header_version)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;

    R::decode(&mut bytes, api_version).map_err(|_| Error::Protocol(ProtocolError::Codec))
}

fn map_kafka_code_or_unknown(code: i16) -> Error {
    Error::from_protocol(code).unwrap_or(Error::Kafka(KafkaCode::Unknown))
}
