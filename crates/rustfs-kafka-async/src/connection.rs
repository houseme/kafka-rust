//! Async Kafka connection with native tokio I/O.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use kafka_protocol::messages::{
    ApiKey, RequestHeader, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse,
};
use kafka_protocol::protocol::StrBytes;
use pbkdf2::pbkdf2_hmac_array;
use rand::distr::{Alphanumeric, SampleString};
use rustfs_kafka::client::{SaslConfig, SecurityConfig, TlsConfig};
use rustfs_kafka::error::{ConnectionError, Error, KafkaCode, ProtocolError, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore};
use sha2::{Digest, Sha256, Sha512};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};
use tracing::debug;

use crate::wire::{
    decode_kp_response, encode_kp_request, kafka_error_from_protocol_code,
    non_negative_i32_to_usize,
};

const API_VERSION_SASL_HANDSHAKE: i16 = 1;
const API_VERSION_SASL_AUTHENTICATE: i16 = 1;
const DEFAULT_CLIENT_ID: &str = "rustfs-kafka-async";

enum ScramAlgorithm {
    Sha256,
    Sha512,
}

enum AsyncKafkaStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

/// Certificate verifier that accepts any server certificate (for testing).
#[derive(Debug)]
struct InsecureVerifier;

impl ServerCertVerifier for InsecureVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

/// An async TCP/TLS connection to a Kafka broker.
///
/// This type wraps a tokio stream and provides convenient read/write helpers
/// that map IO errors into the crate's `Error` type.
pub struct AsyncConnection {
    stream: AsyncKafkaStream,
    host: String,
    healthy: bool,
    pending_correlation_id: Option<i32>,
}

impl AsyncConnection {
    /// Connects to a Kafka broker asynchronously.
    pub async fn connect(host: &str, security: Option<&SecurityConfig>) -> Result<Self> {
        debug!("Connecting to {}", host);
        let tcp_stream = TcpStream::connect(host)
            .await
            .map_err(to_io_connection_error)?;
        configure_tcp_stream(&tcp_stream).map_err(to_io_connection_error)?;

        let mut stream = if let Some(security) = security {
            let domain = host.split(':').next().unwrap_or(host).to_owned();
            let tls_config = build_tls_config(security.tls_config()).await?;
            let connector = TlsConnector::from(tls_config);
            let server_name = ServerName::try_from(domain).map_err(|_| {
                Error::Connection(ConnectionError::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid DNS name",
                )))
            })?;
            let tls = connector
                .connect(server_name, tcp_stream)
                .await
                .map_err(|e| {
                    Error::Connection(ConnectionError::Io(io::Error::other(format!(
                        "TLS handshake failed: {e}"
                    ))))
                })?;
            AsyncKafkaStream::Tls(Box::new(tls))
        } else {
            AsyncKafkaStream::Plain(tcp_stream)
        };

        if let Some(sasl) = security.and_then(SecurityConfig::sasl_config) {
            perform_sasl_authentication(&mut stream, sasl).await?;
        }

        debug!("Connected to {}", host);
        Ok(Self {
            stream,
            host: host.to_owned(),
            healthy: true,
            pending_correlation_id: None,
        })
    }

    /// Returns the host this connection is connected to.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Sends raw bytes to the broker and flushes the stream.
    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.ensure_healthy()?;
        // An IO future can be dropped after transferring only part of a frame.
        // Restore health only when the complete operation has succeeded.
        self.healthy = false;
        stream_send(&mut self.stream, data).await?;
        self.healthy = true;
        Ok(())
    }

    /// Reads exactly `n` bytes from the broker and returns them as `Bytes`.
    pub async fn read_exact(&mut self, n: u64) -> Result<Bytes> {
        let n = usize::try_from(n).map_err(|_| Error::Protocol(ProtocolError::Codec))?;
        self.ensure_healthy()?;
        self.healthy = false;
        let bytes = stream_read_exact(&mut self.stream, n).await?;
        self.healthy = true;
        Ok(bytes)
    }

    /// Sends a Kafka request frame and reads the response frame.
    pub async fn request_response(&mut self, request: &[u8]) -> Result<Bytes> {
        self.ensure_healthy()?;
        if self.pending_correlation_id.is_some() {
            return Err(unusable_connection_error());
        }
        self.healthy = false;
        stream_send(&mut self.stream, request).await?;
        let response = stream_read_frame(&mut self.stream).await?;
        self.healthy = true;
        Ok(response)
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.healthy {
            Ok(())
        } else {
            Err(unusable_connection_error())
        }
    }

    fn is_reusable(&self) -> bool {
        self.healthy && self.pending_correlation_id.is_none()
    }

    pub(crate) async fn send_request(&mut self, data: &[u8], correlation_id: i32) -> Result<()> {
        if !self.is_reusable() {
            return Err(unusable_connection_error());
        }
        // Keep the request pending across the gap between send and receive.
        self.pending_correlation_id = Some(correlation_id);
        self.send(data).await
    }

    pub(crate) fn pending_correlation_id(&self) -> Result<i32> {
        self.pending_correlation_id
            .ok_or(Error::Protocol(ProtocolError::Codec))
    }

    pub(crate) async fn read_response_frame(&mut self) -> Result<Bytes> {
        self.ensure_healthy()?;
        self.healthy = false;
        let response = stream_read_frame(&mut self.stream).await?;
        self.healthy = true;
        Ok(response)
    }

    pub(crate) fn complete_request(&mut self) {
        self.pending_correlation_id = None;
    }

    pub(crate) fn invalidate(&mut self) {
        self.healthy = false;
    }
}

fn unusable_connection_error() -> Error {
    to_io_connection_error(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "connection contains failed or incomplete IO",
    ))
}

async fn stream_send(stream: &mut AsyncKafkaStream, data: &[u8]) -> Result<()> {
    match stream {
        AsyncKafkaStream::Plain(stream) => {
            stream
                .write_all(data)
                .await
                .map_err(to_io_connection_error)?;
            stream.flush().await.map_err(to_io_connection_error)?;
        }
        AsyncKafkaStream::Tls(stream) => {
            stream
                .write_all(data)
                .await
                .map_err(to_io_connection_error)?;
            stream.flush().await.map_err(to_io_connection_error)?;
        }
    }
    Ok(())
}

async fn stream_read_exact(stream: &mut AsyncKafkaStream, n: usize) -> Result<Bytes> {
    match stream {
        AsyncKafkaStream::Plain(stream) => read_exact_bytes(stream, n).await,
        AsyncKafkaStream::Tls(stream) => read_exact_bytes(stream, n).await,
    }
}

async fn read_exact_bytes(reader: &mut (impl AsyncRead + Unpin), n: usize) -> Result<Bytes> {
    if n == 0 {
        return Ok(Bytes::new());
    }
    let limit = u64::try_from(n).map_err(|_| Error::Protocol(ProtocolError::Codec))?;
    let mut buf = Vec::new();
    buf.try_reserve_exact(n).map_err(|error| {
        to_io_connection_error(io::Error::new(io::ErrorKind::OutOfMemory, error))
    })?;
    // Allocator capacity may exceed n. Limit the reader rather than exposing
    // all spare capacity to the socket and consuming bytes from the next frame.
    let mut reader = reader.take(limit);
    while buf.len() < n {
        match reader.read_buf(&mut buf).await {
            Ok(0) => {
                return Err(to_io_connection_error(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection ended before the requested bytes were read",
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(to_io_connection_error(error)),
        }
    }
    Ok(Bytes::from(buf))
}

async fn stream_read_frame(stream: &mut AsyncKafkaStream) -> Result<Bytes> {
    let mut size = [0u8; 4];
    match stream {
        AsyncKafkaStream::Plain(stream) => {
            stream
                .read_exact(&mut size)
                .await
                .map_err(to_io_connection_error)?;
        }
        AsyncKafkaStream::Tls(stream) => {
            stream
                .read_exact(&mut size)
                .await
                .map_err(to_io_connection_error)?;
        }
    }
    let size = non_negative_i32_to_usize(i32::from_be_bytes(size))?;
    stream_read_exact(stream, size).await
}

async fn perform_sasl_authentication(
    stream: &mut AsyncKafkaStream,
    sasl: &SaslConfig,
) -> Result<()> {
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
    )
    .await?;
    let handshake_response: SaslHandshakeResponse =
        get_kp_response_from_stream(stream, API_VERSION_SASL_HANDSHAKE, correlation_id).await?;

    if handshake_response.error_code != 0 {
        return Err(kafka_error_from_protocol_code(
            handshake_response.error_code,
        ));
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
        return perform_sasl_plain_authenticate(stream, sasl, correlation_id + 1).await;
    }
    if mechanism.eq_ignore_ascii_case("SCRAM-SHA-256") {
        return perform_sasl_scram_authenticate(
            stream,
            sasl,
            ScramAlgorithm::Sha256,
            correlation_id + 1,
        )
        .await;
    }
    if mechanism.eq_ignore_ascii_case("SCRAM-SHA-512") {
        return perform_sasl_scram_authenticate(
            stream,
            sasl,
            ScramAlgorithm::Sha512,
            correlation_id + 1,
        )
        .await;
    }

    Err(Error::Config(format!(
        "unsupported SASL mechanism for native async path: {}",
        sasl.mechanism()
    )))
}

async fn perform_sasl_plain_authenticate(
    stream: &mut AsyncKafkaStream,
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
    )
    .await?;
    let auth_response: SaslAuthenticateResponse =
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE, correlation_id).await?;

    if auth_response.error_code != 0 {
        return Err(kafka_error_from_protocol_code(auth_response.error_code));
    }

    Ok(())
}

async fn perform_sasl_scram_authenticate(
    stream: &mut AsyncKafkaStream,
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
        SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(client_first.clone()));
    send_kp_request_on_stream(
        stream,
        &auth_header_1,
        &auth_request_1,
        API_VERSION_SASL_AUTHENTICATE,
    )
    .await?;
    let auth_response_1: SaslAuthenticateResponse =
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE, correlation_id).await?;
    if auth_response_1.error_code != 0 {
        return Err(kafka_error_from_protocol_code(auth_response_1.error_code));
    }

    let server_first = std::str::from_utf8(&auth_response_1.auth_bytes)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;
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
        &algorithm,
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
    )
    .await?;
    let auth_response_2: SaslAuthenticateResponse =
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE, correlation_id + 1)
            .await?;
    if auth_response_2.error_code != 0 {
        return Err(kafka_error_from_protocol_code(auth_response_2.error_code));
    }

    let server_final = std::str::from_utf8(&auth_response_2.auth_bytes)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;
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
    // SASL/PLAIN initial response: authzid (empty) + username + password.
    let mut payload = Vec::with_capacity(sasl.username().len() + sasl.password().len() + 2);
    payload.push(0);
    payload.extend_from_slice(sasl.username().as_bytes());
    payload.push(0);
    payload.extend_from_slice(sasl.password().as_bytes());
    Bytes::from(payload)
}

fn compute_scram_proof_and_server_signature(
    algorithm: &ScramAlgorithm,
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

async fn send_kp_request_on_stream<T>(
    stream: &mut AsyncKafkaStream,
    header: &RequestHeader,
    body: &T,
    api_version: i16,
) -> Result<()>
where
    T: kafka_protocol::protocol::Encodable + kafka_protocol::protocol::HeaderVersion,
{
    let out = encode_kp_request(header, body, api_version)?;
    stream_send(stream, &out).await
}

async fn get_kp_response_from_stream<R>(
    stream: &mut AsyncKafkaStream,
    api_version: i16,
    expected_correlation_id: i32,
) -> Result<R>
where
    R: kafka_protocol::protocol::Decodable + kafka_protocol::protocol::HeaderVersion,
{
    let bytes = stream_read_frame(stream).await?;
    decode_kp_response(bytes, api_version, expected_correlation_id)
}

fn configure_tcp_stream(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    Ok(())
}

async fn build_tls_config(tls_config: &TlsConfig) -> Result<Arc<ClientConfig>> {
    let provider = rustls::crypto::aws_lc_rs::default_provider();

    let config = if tls_config.verify_hostname {
        let root_store = load_root_store(tls_config).await?;
        let builder = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .map_err(|e| {
                Error::Connection(ConnectionError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to set protocol versions: {e}"),
                )))
            })?
            .with_root_certificates(root_store);

        if let (Some(cert_path), Some(key_path)) =
            (&tls_config.client_cert_path, &tls_config.client_key_path)
        {
            load_client_auth(builder, cert_path, key_path).await?
        } else {
            builder.with_no_client_auth()
        }
    } else {
        ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .map_err(|e| {
                Error::Connection(ConnectionError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to set protocol versions: {e}"),
                )))
            })?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(InsecureVerifier))
            .with_no_client_auth()
    };

    Ok(Arc::new(config))
}

async fn load_root_store(tls_config: &TlsConfig) -> Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    if let Some(ca_cert_path) = &tls_config.ca_cert_path {
        let bytes = tokio::fs::read(ca_cert_path)
            .await
            .map_err(to_io_connection_error)?;
        let mut reader = std::io::Cursor::new(bytes);
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_reader_iter(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                Error::Connection(ConnectionError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse CA cert: {e}"),
                )))
            })?;

        for cert in certs {
            root_store.add(cert).map_err(|e| {
                Error::Connection(ConnectionError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to add CA cert: {e}"),
                )))
            })?;
        }
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    Ok(root_store)
}

async fn load_client_auth(
    builder: rustls::ConfigBuilder<ClientConfig, rustls::client::WantsClientCert>,
    cert_path: &str,
    key_path: &str,
) -> Result<ClientConfig> {
    let cert_bytes = tokio::fs::read(cert_path)
        .await
        .map_err(to_io_connection_error)?;
    let mut cert_reader = std::io::Cursor::new(cert_bytes);
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_reader_iter(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            Error::Connection(ConnectionError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse client cert: {e}"),
            )))
        })?;

    let key_bytes = tokio::fs::read(key_path)
        .await
        .map_err(to_io_connection_error)?;
    let mut key_reader = std::io::Cursor::new(key_bytes);
    let key = PrivateKeyDer::from_pem_reader(&mut key_reader).map_err(|e| {
        Error::Connection(ConnectionError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to parse private key: {e}"),
        )))
    })?;

    builder.with_client_auth_cert(certs, key).map_err(|e| {
        Error::Connection(ConnectionError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to set client auth: {e}"),
        )))
    })
}

fn to_io_connection_error(e: io::Error) -> Error {
    Error::Connection(ConnectionError::Io(e))
}

/// A pool of async connections to Kafka brokers.
///
/// This is a simple, in-memory map of host -> `AsyncConnection`.
pub struct AsyncConnectionPool {
    connections: HashMap<String, AsyncConnection>,
    security: Option<SecurityConfig>,
}

impl AsyncConnectionPool {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            security: None,
        }
    }

    pub fn new_with_security(security: Option<SecurityConfig>) -> Self {
        Self {
            connections: HashMap::new(),
            security,
        }
    }

    /// Gets or creates a connection to the specified host.
    pub async fn get(&mut self, host: &str) -> Result<&mut AsyncConnection> {
        if self
            .connections
            .get(host)
            .is_none_or(|connection| !connection.is_reusable())
        {
            self.connections.remove(host);
            let conn = AsyncConnection::connect(host, self.security.as_ref()).await?;
            self.insert(host.to_owned(), conn);
        }
        Ok(self.connections.get_mut(host).expect("key must exist"))
    }

    pub(crate) fn insert(&mut self, host: String, connection: AsyncConnection) {
        self.connections.insert(host, connection);
    }

    /// Returns whether a connection can begin a request without reconnecting.
    pub(crate) fn has_reusable_connection(&self) -> bool {
        self.connections.values().any(AsyncConnection::is_reusable)
    }

    /// Returns the list of connected hosts.
    #[must_use]
    pub fn hosts(&self) -> Vec<&str> {
        self.connections
            .iter()
            .filter(|(_, connection)| connection.is_reusable())
            .map(|(host, _)| host.as_str())
            .collect()
    }
}

impl Default for AsyncConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use bytes::{Buf, BytesMut};
    use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse, ResponseHeader};
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};
    use rustfs_kafka::error::ConnectionError;
    use tokio::io::ReadBuf;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;

    use super::*;
    use crate::wire::{get_kp_response, send_kp_request};

    async fn checked<F: Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .expect("mock broker operation timed out")
    }

    async fn assert_cancelled_socket_closed(socket: &mut TcpStream) {
        let error = checked(socket.read_u8()).await.unwrap_err();
        // A cancelled read may leave inbound bytes unread. Linux can close
        // that discarded connection with RST while other platforms send FIN.
        // Both mean the old socket was retired; fresh replies must succeed.
        assert!(matches!(
            error.kind(),
            io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
        ));
    }

    async fn read_api_versions_request(socket: &mut TcpStream) -> RequestHeader {
        let size = checked(socket.read_i32()).await.unwrap();
        let mut bytes = vec![0; usize::try_from(size).unwrap()];
        checked(socket.read_exact(&mut bytes)).await.unwrap();
        let mut bytes = Bytes::from(bytes);
        let header =
            RequestHeader::decode(&mut bytes, ApiVersionsRequest::header_version(0)).unwrap();
        assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
        assert_eq!(header.request_api_version, 0);
        ApiVersionsRequest::decode(&mut bytes, 0).unwrap();
        assert!(!bytes.has_remaining());
        header
    }

    fn response_frame(correlation_id: i32, trailing_bytes: bool) -> Bytes {
        let mut payload = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(correlation_id)
            .encode(&mut payload, ApiVersionsResponse::header_version(0))
            .unwrap();
        ApiVersionsResponse::default()
            .encode(&mut payload, 0)
            .unwrap();
        if trailing_bytes {
            payload.extend_from_slice(&[0]);
        }
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&i32::try_from(payload.len()).unwrap().to_be_bytes());
        frame.extend_from_slice(&payload);
        frame.freeze()
    }

    async fn send_api_versions(conn: &mut AsyncConnection, correlation_id: i32) -> Result<()> {
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::ApiVersions as i16)
            .with_request_api_version(0)
            .with_correlation_id(correlation_id);
        send_kp_request(conn, &header, &ApiVersionsRequest::default(), 0).await
    }

    #[derive(Clone, Copy)]
    enum ResponseFault {
        CancelLength,
        CancelBody,
        Eof,
        NegativeLength,
        WrongCorrelation,
        TrailingBytes,
        AbandonedRequest,
    }

    async fn assert_response_fault_reconnects(fault: ResponseFault) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let fragment_sent = Arc::new(Notify::new());
        let sent = Arc::clone(&fragment_sent);
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let header = read_api_versions_request(&mut socket).await;
            let wrong_correlation = matches!(fault, ResponseFault::WrongCorrelation);
            let frame = response_frame(
                header.correlation_id + i32::from(wrong_correlation),
                matches!(fault, ResponseFault::TrailingBytes),
            );
            match fault {
                ResponseFault::CancelLength => {
                    checked(socket.write_all(&frame[..2])).await.unwrap();
                    sent.notify_one();
                }
                ResponseFault::CancelBody => {
                    checked(socket.write_all(&frame[..6])).await.unwrap();
                    sent.notify_one();
                }
                ResponseFault::Eof => {
                    checked(socket.write_all(&frame[..6])).await.unwrap();
                }
                ResponseFault::NegativeLength => {
                    checked(socket.write_i32(-1)).await.unwrap();
                }
                ResponseFault::WrongCorrelation | ResponseFault::TrailingBytes => {
                    checked(socket.write_all(&frame)).await.unwrap();
                }
                ResponseFault::AbandonedRequest => {}
            }
            if !matches!(fault, ResponseFault::Eof) {
                if matches!(
                    fault,
                    ResponseFault::CancelLength | ResponseFault::CancelBody
                ) {
                    assert_cancelled_socket_closed(&mut socket).await;
                } else {
                    assert_eq!(
                        checked(socket.read_u8()).await.unwrap_err().kind(),
                        io::ErrorKind::UnexpectedEof,
                    );
                }
            }
            drop(socket);
            // Recovery creates a new connection; neither the incomplete frame
            // nor the old request is replayed on it.
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            for correlation_id in [2, 3] {
                let header = read_api_versions_request(&mut socket).await;
                assert_eq!(header.correlation_id, correlation_id);
                checked(socket.write_all(&response_frame(correlation_id, false)))
                    .await
                    .unwrap();
            }
        });
        let mut pool = AsyncConnectionPool::new();
        let conn = checked(pool.get(&host)).await.unwrap();
        checked(send_api_versions(conn, 1)).await.unwrap();
        match fault {
            ResponseFault::CancelLength | ResponseFault::CancelBody => {
                checked(fragment_sent.notified()).await;
                let mut response = Box::pin(get_kp_response::<ApiVersionsResponse>(conn, 0));
                // Poll the actual framed receive after the fragment was sent,
                // then drop it while the rest of the frame is unavailable.
                poll_fn(|cx| match response.as_mut().poll(cx) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(result) => panic!("partial response completed: {result:?}"),
                })
                .await;
                drop(response);
            }
            ResponseFault::AbandonedRequest => {}
            ResponseFault::Eof => {
                assert!(matches!(
                    checked(get_kp_response::<ApiVersionsResponse>(conn, 0)).await,
                    Err(Error::Connection(ConnectionError::Io(_))),
                ));
            }
            _ => {
                assert!(matches!(
                    checked(get_kp_response::<ApiVersionsResponse>(conn, 0)).await,
                    Err(Error::Protocol(ProtocolError::Codec)),
                ));
            }
        }
        assert!(pool.hosts().is_empty());
        assert!(!pool.has_reusable_connection());
        for correlation_id in [2, 3] {
            let conn = checked(pool.get(&host)).await.unwrap();
            checked(send_api_versions(conn, correlation_id))
                .await
                .unwrap();
            assert_eq!(
                checked(get_kp_response::<ApiVersionsResponse>(conn, 0))
                    .await
                    .unwrap()
                    .error_code,
                0,
            );
            assert_eq!(pool.hosts(), [host.as_str()]);
            assert!(pool.has_reusable_connection());
        }
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_response_length_is_reconnected() {
        assert_response_fault_reconnects(ResponseFault::CancelLength).await;
    }

    #[tokio::test]
    async fn cancelled_response_body_is_reconnected() {
        assert_response_fault_reconnects(ResponseFault::CancelBody).await;
    }

    #[tokio::test]
    async fn eof_mid_response_is_reconnected() {
        assert_response_fault_reconnects(ResponseFault::Eof).await;
    }

    #[tokio::test]
    async fn negative_response_length_is_reconnected() {
        assert_response_fault_reconnects(ResponseFault::NegativeLength).await;
    }

    #[tokio::test]
    async fn mismatched_response_correlation_is_reconnected() {
        assert_response_fault_reconnects(ResponseFault::WrongCorrelation).await;
    }

    #[tokio::test]
    async fn response_with_trailing_bytes_is_reconnected() {
        assert_response_fault_reconnects(ResponseFault::TrailingBytes).await;
    }

    #[tokio::test]
    async fn request_abandoned_between_send_and_receive_is_reconnected() {
        assert_response_fault_reconnects(ResponseFault::AbandonedRequest).await;
    }

    #[tokio::test]
    async fn cancelled_partial_raw_send_is_reconnected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let partial_write = Arc::new(Notify::new());
        let received = Arc::clone(&partial_write);
        let resume = Arc::new(Notify::new());
        let resume_drain = Arc::clone(&resume);
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let mut bytes = [0; 1024];
            checked(socket.read_exact(&mut bytes)).await.unwrap();
            assert_eq!(bytes, [0x5a; 1024]);
            received.notify_one();
            resume_drain.notified().await;
            // Drain the partial write after the client replaces the connection.
            checked(socket.read_to_end(&mut Vec::new())).await.unwrap();
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let header = read_api_versions_request(&mut socket).await;
            assert_eq!(header.correlation_id, 2);
            checked(socket.write_all(&response_frame(2, false)))
                .await
                .unwrap();
        });
        let mut pool = AsyncConnectionPool::new();
        let conn = checked(pool.get(&host)).await.unwrap();
        let bytes = vec![0x5a; 16 * 1024 * 1024];
        checked(async {
            tokio::select! {
                result = conn.send(&bytes) => panic!("send completed before cancellation: {result:?}"),
                () = partial_write.notified() => {}
            }
        }).await;
        assert!(pool.hosts().is_empty());
        resume.notify_one();
        let conn = checked(pool.get(&host)).await.unwrap();
        checked(send_api_versions(conn, 2)).await.unwrap();
        checked(get_kp_response::<ApiVersionsResponse>(conn, 0))
            .await
            .unwrap();
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_raw_request_response_is_reconnected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let request_received = Arc::new(Notify::new());
        let received = Arc::clone(&request_received);
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let header = read_api_versions_request(&mut socket).await;
            assert_eq!(header.correlation_id, 1);
            // Cancel while waiting for the response's first byte. Closing with
            // unread response bytes can produce RST on Linux, so it cannot
            // support a portable EOF assertion. Mid-frame cancellation has
            // separate coverage below the raw request/response API.
            received.notify_one();
            assert_eq!(
                checked(socket.read_u8()).await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof,
            );
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let header = read_api_versions_request(&mut socket).await;
            assert_eq!(header.correlation_id, 2);
            checked(socket.write_all(&response_frame(2, false)))
                .await
                .unwrap();
        });
        let mut pool = AsyncConnectionPool::new();
        let conn = checked(pool.get(&host)).await.unwrap();
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::ApiVersions as i16)
            .with_correlation_id(1);
        let request = encode_kp_request(&header, &ApiVersionsRequest::default(), 0).unwrap();
        checked(async {
            tokio::select! {
                result = conn.request_response(&request) => panic!("response completed before cancellation: {result:?}"),
                () = request_received.notified() => {}
            }
        }).await;
        assert!(pool.hosts().is_empty());
        let conn = checked(pool.get(&host)).await.unwrap();
        checked(send_api_versions(conn, 2)).await.unwrap();
        checked(get_kp_response::<ApiVersionsResponse>(conn, 0))
            .await
            .unwrap();
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_partial_raw_read_is_reconnected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let fragment = Arc::new(Notify::new());
        let sent = Arc::clone(&fragment);
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            checked(socket.write_all(b"ab")).await.unwrap();
            sent.notify_one();
            assert_cancelled_socket_closed(&mut socket).await;
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let header = read_api_versions_request(&mut socket).await;
            assert_eq!(header.correlation_id, 1);
            checked(socket.write_all(&response_frame(1, false)))
                .await
                .unwrap();
        });
        let mut pool = AsyncConnectionPool::new();
        let conn = checked(pool.get(&host)).await.unwrap();
        checked(fragment.notified()).await;
        let mut read = Box::pin(conn.read_exact(4));
        poll_fn(|cx| match read.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(result) => panic!("partial raw read completed: {result:?}"),
        })
        .await;
        drop(read);
        assert!(pool.hosts().is_empty());
        let conn = checked(pool.get(&host)).await.unwrap();
        checked(send_api_versions(conn, 1)).await.unwrap();
        checked(get_kp_response::<ApiVersionsResponse>(conn, 0))
            .await
            .unwrap();
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn complete_raw_reads_and_writes_preserve_pool_reuse() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let mut request = [0; 3];
            checked(socket.read_exact(&mut request)).await.unwrap();
            assert_eq!(&request, b"raw");
            checked(socket.write_all(b"ab")).await.unwrap();
            checked(socket.write_all(b"cd")).await.unwrap();
            let header = read_api_versions_request(&mut socket).await;
            assert_eq!(header.correlation_id, 1);
            checked(socket.write_all(&response_frame(1, false)))
                .await
                .unwrap();
        });
        let mut pool = AsyncConnectionPool::new();
        let conn = checked(pool.get(&host)).await.unwrap();
        assert!(checked(conn.read_exact(0)).await.unwrap().is_empty());
        checked(conn.send(b"raw")).await.unwrap();
        assert_eq!(checked(conn.read_exact(2)).await.unwrap(), &b"ab"[..]);
        assert!(checked(conn.read_exact(0)).await.unwrap().is_empty());
        assert_eq!(checked(conn.read_exact(2)).await.unwrap(), &b"cd"[..]);
        assert_eq!(pool.hosts(), [host.as_str()]);
        let conn = checked(pool.get(&host)).await.unwrap();
        checked(send_api_versions(conn, 1)).await.unwrap();
        checked(get_kp_response::<ApiVersionsResponse>(conn, 0))
            .await
            .unwrap();
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn exact_raw_read_assembles_tcp_fragments_without_consuming_following_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let sent = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let server_sent = Arc::clone(&sent);
        let server_resume = Arc::clone(&resume);
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            for fragment in [&b"ab"[..], &b"c"[..]] {
                checked(socket.write_all(fragment)).await.unwrap();
                server_sent.notify_one();
                checked(server_resume.notified()).await;
            }
            checked(socket.write_all(b"defghij")).await.unwrap();
        });
        let mut conn = checked(AsyncConnection::connect(&host, None))
            .await
            .unwrap();
        checked(sent.notified()).await;
        let mut read = Box::pin(conn.read_exact(7));
        for index in 0..2 {
            if index != 0 {
                checked(sent.notified()).await;
            }
            poll_fn(|cx| match read.as_mut().poll(cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(result) => panic!("partial raw read completed: {result:?}"),
            })
            .await;
            resume.notify_one();
        }
        assert_eq!(checked(read).await.unwrap(), &b"abcdefg"[..]);
        assert_eq!(checked(conn.read_exact(3)).await.unwrap(), &b"hij"[..]);
        assert!(conn.is_reusable());
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn adjacent_short_response_frames_are_not_overread() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let mut request = [0; 3];
            checked(socket.read_exact(&mut request)).await.unwrap();
            assert_eq!(&request, b"one");
            // Both frames are available to a single socket read; each declared
            // payload must leave the following frame untouched.
            let mut frames = Vec::new();
            for payload in [b"abc", b"def"] {
                frames.extend_from_slice(&3i32.to_be_bytes());
                frames.extend_from_slice(payload);
            }
            checked(socket.write_all(&frames)).await.unwrap();
            checked(socket.read_exact(&mut request)).await.unwrap();
            assert_eq!(&request, b"two");
        });
        let mut conn = checked(AsyncConnection::connect(&host, None))
            .await
            .unwrap();
        assert_eq!(
            checked(conn.request_response(b"one")).await.unwrap(),
            &b"abc"[..]
        );
        assert_eq!(
            checked(conn.request_response(b"two")).await.unwrap(),
            &b"def"[..]
        );
        assert!(conn.is_reusable());
        checked(server).await.unwrap();
    }

    struct InterruptedTcpReader {
        stream: TcpStream,
        interrupt_once: bool,
    }

    impl AsyncRead for InterruptedTcpReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if std::mem::take(&mut self.interrupt_once) {
                return Poll::Ready(Err(io::ErrorKind::Interrupted.into()));
            }
            Pin::new(&mut self.stream).poll_read(cx, buf)
        }
    }

    #[tokio::test]
    async fn interrupted_exact_read_retries_and_preserves_following_tcp_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            checked(socket.write_all(b"abcdef")).await.unwrap();
        });
        let mut reader = InterruptedTcpReader {
            stream: checked(TcpStream::connect(host)).await.unwrap(),
            interrupt_once: true,
        };
        assert_eq!(
            checked(read_exact_bytes(&mut reader, 3)).await.unwrap(),
            &b"abc"[..]
        );
        assert_eq!(
            checked(read_exact_bytes(&mut reader, 3)).await.unwrap(),
            &b"def"[..]
        );
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn exact_read_capacity_overflow_returns_an_error_without_panicking() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            assert_eq!(
                checked(socket.read_u8()).await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
        });
        let mut conn = checked(AsyncConnection::connect(&host, None))
            .await
            .unwrap();
        let error = checked(conn.read_exact(u64::try_from(usize::MAX).unwrap()))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Connection(ConnectionError::Io(ref io_error)) if io_error.kind() == io::ErrorKind::OutOfMemory)
        );
        assert!(!conn.is_reusable());
        drop(conn);
        checked(server).await.unwrap();
    }

    async fn assert_sasl_rejects_invalid_handshake(fault: ResponseFault) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = checked(listener.accept()).await.unwrap();
            let size = checked(socket.read_i32()).await.unwrap();
            let mut frame = vec![0; usize::try_from(size).unwrap()];
            checked(socket.read_exact(&mut frame)).await.unwrap();
            let mut frame = Bytes::from(frame);
            let header = RequestHeader::decode(
                &mut frame,
                SaslHandshakeRequest::header_version(API_VERSION_SASL_HANDSHAKE),
            )
            .unwrap();
            assert_eq!(header.request_api_key, ApiKey::SaslHandshake as i16);
            SaslHandshakeRequest::decode(&mut frame, API_VERSION_SASL_HANDSHAKE).unwrap();
            if matches!(fault, ResponseFault::NegativeLength) {
                checked(socket.write_i32(-1)).await.unwrap();
            } else {
                let mut payload = BytesMut::new();
                ResponseHeader::default()
                    .with_correlation_id(
                        header.correlation_id
                            + i32::from(matches!(fault, ResponseFault::WrongCorrelation)),
                    )
                    .encode(
                        &mut payload,
                        SaslHandshakeResponse::header_version(API_VERSION_SASL_HANDSHAKE),
                    )
                    .unwrap();
                SaslHandshakeResponse::default()
                    .with_mechanisms(vec![StrBytes::from_static_str("PLAIN")])
                    .encode(&mut payload, API_VERSION_SASL_HANDSHAKE)
                    .unwrap();
                if matches!(fault, ResponseFault::TrailingBytes) {
                    payload.extend_from_slice(&[0]);
                }
                checked(socket.write_i32(i32::try_from(payload.len()).unwrap()))
                    .await
                    .unwrap();
                checked(socket.write_all(&payload)).await.unwrap();
            }
            // Invalid handshake frames must never reach the credential step.
            assert_eq!(
                checked(socket.read_u8()).await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
        });
        let mut stream = AsyncKafkaStream::Plain(checked(TcpStream::connect(&host)).await.unwrap());
        let result = checked(perform_sasl_authentication(
            &mut stream,
            &SaslConfig::plain("u".to_owned(), "p".to_owned()),
        ))
        .await;
        assert!(matches!(result, Err(Error::Protocol(ProtocolError::Codec))));
        drop(stream);
        checked(server).await.unwrap();
    }

    #[tokio::test]
    async fn sasl_rejects_mismatched_handshake_correlation_before_credentials() {
        assert_sasl_rejects_invalid_handshake(ResponseFault::WrongCorrelation).await;
    }

    #[tokio::test]
    async fn sasl_rejects_handshake_trailing_bytes_before_credentials() {
        assert_sasl_rejects_invalid_handshake(ResponseFault::TrailingBytes).await;
    }

    #[tokio::test]
    async fn sasl_rejects_negative_handshake_length_before_credentials() {
        assert_sasl_rejects_invalid_handshake(ResponseFault::NegativeLength).await;
    }

    #[test]
    fn pool_new_creates_empty_pool() {
        let pool = AsyncConnectionPool::new();
        assert!(pool.hosts().is_empty());
        assert!(!pool.has_reusable_connection());
    }

    #[test]
    fn pool_default_matches_new() {
        let pool = AsyncConnectionPool::default();
        assert!(pool.hosts().is_empty());
        assert!(!pool.has_reusable_connection());
    }

    #[tokio::test]
    async fn connect_unreachable_host_returns_io_error() {
        let result = AsyncConnection::connect("127.0.0.1:1", None).await;
        assert!(matches!(
            result,
            Err(Error::Connection(ConnectionError::Io(_)))
        ));
    }

    #[tokio::test]
    async fn pool_get_unreachable_host_propagates_error() {
        let mut pool = AsyncConnectionPool::new();
        let result = pool.get("127.0.0.1:1").await;
        assert!(result.is_err());
        assert!(pool.hosts().is_empty());
    }

    #[test]
    fn sasl_plain_auth_bytes_format() {
        let bytes = build_sasl_plain_auth_bytes(&SaslConfig::plain("u".to_owned(), "p".to_owned()));
        assert_eq!(bytes.as_ref(), &[0, b'u', 0, b'p']);
    }

    #[test]
    fn scram_username_escape() {
        assert_eq!(scram_escape_username("a,b=c"), "a=2Cb=3Dc");
    }
}
