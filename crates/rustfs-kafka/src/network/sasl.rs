//! SASL authentication logic for Kafka connections.
//!
//! Supports PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 mechanisms.

use std::collections::HashMap;
use std::io::{Read, Write};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use kafka_protocol::messages::{
    ApiKey, RequestHeader, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use pbkdf2::pbkdf2_hmac_array;
use rand::distr::{Alphanumeric, SampleString};
use sha2::{Digest, Sha256, Sha512};

use super::connection::SaslConfig;
use super::connection::{KafkaStream, StreamOps};
use crate::error::{Error, KafkaCode, Result};

const API_VERSION_SASL_HANDSHAKE: i16 = 1;
const API_VERSION_SASL_AUTHENTICATE: i16 = 1;
const DEFAULT_CLIENT_ID: &str = "rustfs-kafka";

#[derive(Clone, Copy)]
enum ScramAlgorithm {
    Sha256,
    Sha512,
}

pub(crate) fn perform_sasl_authentication(
    stream: &mut KafkaStream,
    sasl: &SaslConfig,
) -> Result<()> {
    let result = perform_sasl_authentication_inner(stream, sasl);
    if result.is_err() {
        // Failed authentication never hands a reusable stream to its caller,
        // including broker rejections and errors outside the frame decoder.
        let _ = StreamOps::shutdown(stream, std::net::Shutdown::Both);
    }
    result
}

fn perform_sasl_authentication_inner(stream: &mut KafkaStream, sasl: &SaslConfig) -> Result<()> {
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
        get_kp_response_from_stream(stream, API_VERSION_SASL_HANDSHAKE, correlation_id)?;

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
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE, correlation_id)?;

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
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE, correlation_id)?;
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
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE, correlation_id + 1)?;
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

    let salted_password = pbkdf2_hmac_array::<Sha256, 32>(password.as_bytes(), salt, iterations);
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

    let salted_password = pbkdf2_hmac_array::<Sha512, 64>(password.as_bytes(), salt, iterations);
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
    let out = crate::protocol::encode_request_frame(header, body, api_version)?;
    stream.write_all(&out).map_err(Error::from)?;
    stream.flush().map_err(Error::from)
}

fn get_kp_response_from_stream<R>(
    stream: &mut KafkaStream,
    api_version: i16,
    correlation_id: i32,
) -> Result<R>
where
    R: Decodable + HeaderVersion,
{
    let result = (|| {
        let mut size_buf = [0u8; 4];
        stream.read_exact(&mut size_buf)?;
        let size = crate::protocol::non_negative_i32_to_usize(i32::from_be_bytes(size_buf))?;
        let mut payload = vec![0u8; size];
        stream.read_exact(&mut payload)?;
        crate::protocol::decode_response_payload_checked(
            Bytes::from(payload),
            api_version,
            correlation_id,
        )
    })();
    if result.is_err() {
        let _ = StreamOps::shutdown(stream, std::net::Shutdown::Both);
    }
    result
}

fn map_kafka_code_or_unknown(code: i16) -> Error {
    Error::from_protocol(code).unwrap_or(Error::Kafka(KafkaCode::Unknown))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use kafka_protocol::messages::ResponseHeader;
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;

    #[test]
    fn invalid_handshake_frames_close_stream_without_sending_credentials() {
        for defect in [
            "wrong-correlation",
            "trailing-data",
            "negative-size",
            "short-body",
            "truncated-frame",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut size = [0; 4];
                stream.read_exact(&mut size).unwrap();
                let mut bytes = vec![0; usize::try_from(i32::from_be_bytes(size)).unwrap()];
                stream.read_exact(&mut bytes).unwrap();
                let mut bytes = Bytes::from(bytes);
                let header = RequestHeader::decode(
                    &mut bytes,
                    SaslHandshakeRequest::header_version(API_VERSION_SASL_HANDSHAKE),
                )
                .unwrap();
                assert_eq!(header.request_api_key, ApiKey::SaslHandshake as i16);
                let request =
                    SaslHandshakeRequest::decode(&mut bytes, API_VERSION_SASL_HANDSHAKE).unwrap();
                assert_eq!(request.mechanism.as_str(), "PLAIN");
                assert!(bytes.is_empty());
                let mut response = BytesMut::new();
                ResponseHeader::default()
                    .with_correlation_id(
                        header.correlation_id + i32::from(defect == "wrong-correlation"),
                    )
                    .encode(
                        &mut response,
                        SaslHandshakeResponse::header_version(API_VERSION_SASL_HANDSHAKE),
                    )
                    .unwrap();
                if defect != "short-body" {
                    SaslHandshakeResponse::default()
                        .with_mechanisms(vec![StrBytes::from_static_str("PLAIN")])
                        .encode(&mut response, API_VERSION_SASL_HANDSHAKE)
                        .unwrap();
                }
                if defect == "trailing-data" {
                    response.extend_from_slice(&[0]);
                }
                let size = if defect == "negative-size" {
                    -1
                } else {
                    i32::try_from(response.len()).unwrap() + i32::from(defect == "truncated-frame")
                };
                stream.write_all(&size.to_be_bytes()).unwrap();
                if size >= 0 {
                    stream.write_all(&response).unwrap();
                }
                if defect == "truncated-frame" {
                    stream.shutdown(std::net::Shutdown::Write).unwrap();
                }
                let mut credential_byte = [0];
                assert_eq!(
                    stream
                        .read(&mut credential_byte)
                        .unwrap_or_else(|err| panic!(
                            "{defect}: expected closed handshake stream: {err}"
                        )),
                    0,
                    "invalid handshake caused credential transmission"
                );
            });
            let stream = TcpStream::connect(address).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut stream = KafkaStream::Plain(stream);
            let sasl = SaslConfig::plain("username".into(), "password".into());
            let result = perform_sasl_authentication(&mut stream, &sasl);
            assert!(result.is_err(), "accepted invalid {defect}");
            // Match KafkaConnection::new: failed authentication drops its owned
            // stream, including when the peer half-closes a truncated response.
            drop(stream);
            server.join().unwrap();
        }
    }
}
