//! Async Kafka connection with native tokio I/O.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

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
use rustfs_kafka::client::{SaslConfig, SecurityConfig, TlsConfig};
use rustfs_kafka::error::{ConnectionError, Error, KafkaCode, ProtocolError, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore};
use sha2::{Digest, Sha256, Sha512};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};
use tracing::debug;

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
        })
    }

    /// Returns the host this connection is connected to.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Sends raw bytes to the broker and flushes the stream.
    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        stream_send(&mut self.stream, data).await
    }

    /// Reads exactly `n` bytes from the broker and returns them as `Bytes`.
    pub async fn read_exact(&mut self, n: u64) -> Result<Bytes> {
        let n = usize::try_from(n).map_err(|_| Error::Protocol(ProtocolError::Codec))?;
        stream_read_exact(&mut self.stream, n).await
    }

    /// Sends a Kafka request frame and reads the response frame.
    pub async fn request_response(&mut self, request: &[u8]) -> Result<Bytes> {
        self.send(request).await?;

        let size_bytes = stream_read_exact(&mut self.stream, 4).await?;
        let size = i32::from_be_bytes(
            <[u8; 4]>::try_from(size_bytes.as_ref())
                .map_err(|_| Error::Protocol(ProtocolError::Codec))?,
        );
        if size < 0 {
            return Err(Error::Protocol(ProtocolError::Codec));
        }

        stream_read_exact(
            &mut self.stream,
            usize::try_from(size).map_err(|_| Error::Protocol(ProtocolError::Codec))?,
        )
        .await
    }
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
    let mut buf = BytesMut::with_capacity(n);
    buf.resize(n, 0);

    match stream {
        AsyncKafkaStream::Plain(stream) => {
            stream
                .read_exact(&mut buf)
                .await
                .map_err(to_io_connection_error)?;
        }
        AsyncKafkaStream::Tls(stream) => {
            stream
                .read_exact(&mut buf)
                .await
                .map_err(to_io_connection_error)?;
        }
    }

    Ok(buf.freeze())
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
        get_kp_response_from_stream(stream, API_VERSION_SASL_HANDSHAKE).await?;

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
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE).await?;

    if auth_response.error_code != 0 {
        return Err(map_kafka_code_or_unknown(auth_response.error_code));
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
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE).await?;
    if auth_response_1.error_code != 0 {
        return Err(map_kafka_code_or_unknown(auth_response_1.error_code));
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
        get_kp_response_from_stream(stream, API_VERSION_SASL_AUTHENTICATE).await?;
    if auth_response_2.error_code != 0 {
        return Err(map_kafka_code_or_unknown(auth_response_2.error_code));
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

async fn send_kp_request_on_stream<T>(
    stream: &mut AsyncKafkaStream,
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

    stream_send(stream, &out).await
}

async fn get_kp_response_from_stream<R>(
    stream: &mut AsyncKafkaStream,
    api_version: i16,
) -> Result<R>
where
    R: Decodable + HeaderVersion,
{
    let size_bytes = stream_read_exact(stream, 4).await?;
    let size = i32::from_be_bytes(
        <[u8; 4]>::try_from(size_bytes.as_ref())
            .map_err(|_| Error::Protocol(ProtocolError::Codec))?,
    );
    if size < 0 {
        return Err(Error::Protocol(ProtocolError::Codec));
    }

    let mut bytes = stream_read_exact(
        stream,
        usize::try_from(size).map_err(|_| Error::Protocol(ProtocolError::Codec))?,
    )
    .await?;

    let response_header_version = R::header_version(api_version);
    let _resp_header = ResponseHeader::decode(&mut bytes, response_header_version)
        .map_err(|_| Error::Protocol(ProtocolError::Codec))?;

    R::decode(&mut bytes, api_version).map_err(|_| Error::Protocol(ProtocolError::Codec))
}

fn map_kafka_code_or_unknown(code: i16) -> Error {
    let kafka_code = match code {
        33 => KafkaCode::UnsupportedSaslMechanism,
        34 => KafkaCode::IllegalSaslState,
        35 => KafkaCode::UnsupportedVersion,
        _ => KafkaCode::Unknown,
    };
    Error::Kafka(kafka_code)
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
        let native_certs = rustls_native_certs::load_native_certs();
        for cert in native_certs.certs {
            let _ = root_store.add(cert);
        }
        if let Some(e) = native_certs.errors.first() {
            debug!(
                "Failed to load some native certs (using webpki-roots as fallback): {}",
                e
            );
        }
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
        if !self.connections.contains_key(host) {
            let conn = AsyncConnection::connect(host, self.security.as_ref()).await?;
            self.insert(host.to_owned(), conn);
        }
        Ok(self.connections.get_mut(host).expect("key must exist"))
    }

    pub(crate) fn insert(&mut self, host: String, connection: AsyncConnection) {
        self.connections.insert(host, connection);
    }

    /// Returns the list of connected hosts.
    #[must_use]
    pub fn hosts(&self) -> Vec<&str> {
        self.connections.keys().map(String::as_str).collect()
    }
}

impl Default for AsyncConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use rustfs_kafka::error::ConnectionError;

    use super::*;

    #[test]
    fn pool_new_creates_empty_pool() {
        let pool = AsyncConnectionPool::new();
        assert!(pool.hosts().is_empty());
    }

    #[test]
    fn pool_default_matches_new() {
        let pool = AsyncConnectionPool::default();
        assert!(pool.hosts().is_empty());
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
