#[cfg(feature = "security")]
use std::collections::HashMap;
use std::fmt;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::time::Duration;
use tracing::debug;

#[cfg(feature = "security")]
use base64::Engine as _;
#[cfg(feature = "security")]
use base64::engine::general_purpose::STANDARD as BASE64;
#[cfg(feature = "security")]
use bytes::{Bytes, BytesMut};
#[cfg(feature = "security")]
use hmac::{Hmac, Mac};
#[cfg(feature = "security")]
use kafka_protocol::messages::{
    ApiKey, RequestHeader, ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse,
};
#[cfg(feature = "security")]
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
#[cfg(feature = "security")]
use pbkdf2::pbkdf2_hmac;
#[cfg(feature = "security")]
use rand::distr::{Alphanumeric, SampleString};
#[cfg(feature = "security")]
use sha2::{Digest, Sha256, Sha512};

use crate::error::Result;
#[cfg(feature = "security")]
use crate::error::{Error, KafkaCode, ProtocolError};
#[cfg(feature = "security")]
use crate::tls::{RustlsConnector, TlsConfig, TlsStream};

// --------------------------------------------------------------------

/// Security relevant configuration options for `KafkaClient`.
#[cfg(feature = "security")]
#[derive(Clone)]
pub struct SecurityConfig {
    pub(crate) tls_config: TlsConfig,
    pub(crate) sasl_config: Option<SaslConfig>,
}

/// SASL configuration options for `KafkaClient`.
#[cfg(feature = "security")]
#[derive(Clone, Debug)]
pub struct SaslConfig {
    pub(crate) mechanism: String,
    pub(crate) username: String,
    pub(crate) password: String,
}

#[cfg(feature = "security")]
impl SaslConfig {
    /// Creates a SASL configuration with explicit mechanism and credentials.
    #[must_use]
    pub fn new(mechanism: String, username: String, password: String) -> Self {
        Self {
            mechanism,
            username,
            password,
        }
    }

    /// Creates a SASL/PLAIN configuration with username and password.
    #[must_use]
    pub fn plain(username: String, password: String) -> Self {
        Self::new("PLAIN".to_owned(), username, password)
    }

    /// Returns SASL mechanism.
    #[must_use]
    pub fn mechanism(&self) -> &str {
        &self.mechanism
    }

    /// Returns SASL username.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns SASL password.
    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }
}

#[cfg(feature = "security")]
impl SecurityConfig {
    /// Create a new `SecurityConfig` with default TLS settings.
    #[must_use]
    pub fn new() -> Self {
        SecurityConfig {
            tls_config: TlsConfig::new(),
            sasl_config: None,
        }
    }

    /// Create a `SecurityConfig` from a `TlsConfig`
    #[must_use]
    pub fn from_tls_config(tls_config: TlsConfig) -> SecurityConfig {
        SecurityConfig {
            tls_config,
            sasl_config: None,
        }
    }

    /// Initiates a client-side TLS session with/without performing hostname verification.
    #[must_use]
    pub fn with_hostname_verification(mut self, verify_hostname: bool) -> SecurityConfig {
        self.tls_config.verify_hostname = verify_hostname;
        self
    }

    /// Set a custom CA certificate file path
    #[must_use]
    pub fn with_ca_cert(mut self, path: String) -> SecurityConfig {
        self.tls_config.ca_cert_path = Some(path);
        self
    }

    /// Set client certificate and key file paths
    #[must_use]
    pub fn with_client_cert(mut self, cert_path: String, key_path: String) -> SecurityConfig {
        self.tls_config.client_cert_path = Some(cert_path);
        self.tls_config.client_key_path = Some(key_path);
        self
    }

    /// Sets SASL configuration.
    #[must_use]
    pub fn with_sasl(mut self, sasl_config: SaslConfig) -> SecurityConfig {
        self.sasl_config = Some(sasl_config);
        self
    }

    /// Sets SASL/PLAIN username and password.
    #[must_use]
    pub fn with_sasl_plain(mut self, username: String, password: String) -> SecurityConfig {
        self.sasl_config = Some(SaslConfig::plain(username, password));
        self
    }

    /// Returns the underlying TLS configuration.
    #[must_use]
    pub fn tls_config(&self) -> &TlsConfig {
        &self.tls_config
    }

    /// Returns optional SASL configuration.
    #[must_use]
    pub fn sasl_config(&self) -> Option<&SaslConfig> {
        self.sasl_config.as_ref()
    }
}

#[cfg(feature = "security")]
impl Default for SecurityConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "security")]
impl fmt::Debug for SecurityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SecurityConfig {{ verify_hostname: {} }}",
            self.tls_config.verify_hostname
        )
    }
}

// --------------------------------------------------------------------

#[cfg(not(feature = "security"))]
pub(crate) type KafkaStream = TcpStream;

#[cfg(feature = "security")]
pub(crate) enum KafkaStream {
    Plain(TcpStream),
    Tls(Box<dyn TlsStream>),
}

pub(crate) trait StreamOps {
    fn is_secured(&self) -> bool;
    fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()>;
    fn set_write_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()>;
    fn shutdown(&mut self, how: Shutdown) -> std::io::Result<()>;
}

#[cfg(not(feature = "security"))]
impl StreamOps for KafkaStream {
    fn is_secured(&self) -> bool {
        false
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        TcpStream::set_read_timeout(self, dur)
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        TcpStream::set_write_timeout(self, dur)
    }

    fn shutdown(&mut self, how: Shutdown) -> std::io::Result<()> {
        TcpStream::shutdown(self, how)
    }
}

#[cfg(feature = "security")]
impl StreamOps for KafkaStream {
    fn is_secured(&self) -> bool {
        match self {
            KafkaStream::Plain(_) => false,
            KafkaStream::Tls(_) => true,
        }
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.set_read_timeout(dur),
            KafkaStream::Tls(s) => s.set_read_timeout(dur),
        }
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.set_write_timeout(dur),
            KafkaStream::Tls(s) => s.set_write_timeout(dur),
        }
    }

    fn shutdown(&mut self, how: Shutdown) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.shutdown(how),
            KafkaStream::Tls(s) => s.shutdown(),
        }
    }
}

#[cfg(feature = "security")]
impl Read for KafkaStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            KafkaStream::Plain(s) => s.read(buf),
            KafkaStream::Tls(s) => s.read(buf),
        }
    }
}

#[cfg(feature = "security")]
impl Write for KafkaStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            KafkaStream::Plain(s) => s.write(buf),
            KafkaStream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.flush(),
            KafkaStream::Tls(s) => s.flush(),
        }
    }
}

// --------------------------------------------------------------------

/// A TCP stream to a remote Kafka broker.
pub struct KafkaConnection {
    id: u32,
    host: String,
    stream: KafkaStream,
    state: ConnectionState,
}

/// Connection health state for detecting broken connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionState {
    Connected,
    Terminated,
}

impl fmt::Debug for KafkaConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KafkaConnection {{ id: {}, secured: {}, state: {:?}, host: \"{}\" }}",
            self.id,
            self.stream.is_secured(),
            self.state,
            self.host
        )
    }
}

/// Configure a TCP socket with keepalive and nodelay for Kafka compatibility.
fn configure_tcp_socket(socket: &socket2::Socket) -> std::io::Result<()> {
    use socket2::TcpKeepalive;

    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(10))
        .with_interval(Duration::from_secs(20));
    socket.set_tcp_keepalive(&keepalive)?;
    socket.set_tcp_nodelay(true)?;
    Ok(())
}

#[cfg(feature = "security")]
const API_VERSION_SASL_HANDSHAKE: i16 = 1;
#[cfg(feature = "security")]
const API_VERSION_SASL_AUTHENTICATE: i16 = 1;
#[cfg(feature = "security")]
const DEFAULT_CLIENT_ID: &str = "rustfs-kafka";

#[cfg(feature = "security")]
#[derive(Clone, Copy)]
enum ScramAlgorithm {
    Sha256,
    Sha512,
}

#[cfg(feature = "security")]
fn perform_sasl_authentication(stream: &mut KafkaStream, sasl: &SaslConfig) -> Result<()> {
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

#[cfg(feature = "security")]
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

#[cfg(feature = "security")]
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

#[cfg(feature = "security")]
fn build_sasl_plain_auth_bytes(sasl: &SaslConfig) -> Bytes {
    let mut payload = Vec::with_capacity(sasl.username().len() + sasl.password().len() + 2);
    payload.push(0);
    payload.extend_from_slice(sasl.username().as_bytes());
    payload.push(0);
    payload.extend_from_slice(sasl.password().as_bytes());
    Bytes::from(payload)
}

#[cfg(feature = "security")]
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

#[cfg(feature = "security")]
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

#[cfg(feature = "security")]
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

#[cfg(feature = "security")]
fn hmac_bytes<M>(key: &[u8], data: &[u8]) -> Result<Vec<u8>>
where
    M: Mac + hmac::digest::KeyInit,
{
    let mut mac = <M as hmac::digest::KeyInit>::new_from_slice(key)
        .map_err(|e| Error::Config(format!("hmac init failed: {e}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

#[cfg(feature = "security")]
fn xor_bytes(left: &[u8], right: &[u8]) -> Result<Vec<u8>> {
    if left.len() != right.len() {
        return Err(Error::Config(
            "SCRAM proof construction failed: buffer length mismatch".to_owned(),
        ));
    }
    Ok(left.iter().zip(right.iter()).map(|(a, b)| a ^ b).collect())
}

#[cfg(feature = "security")]
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

#[cfg(feature = "security")]
fn generate_scram_nonce() -> String {
    Alphanumeric.sample_string(&mut rand::rng(), 24)
}

#[cfg(feature = "security")]
fn scram_escape_username(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

#[cfg(feature = "security")]
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

#[cfg(feature = "security")]
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

#[cfg(feature = "security")]
fn map_kafka_code_or_unknown(code: i16) -> Error {
    Error::from_protocol(code).unwrap_or(Error::Kafka(KafkaCode::Unknown))
}

impl KafkaConnection {
    pub fn send(&mut self, msg: &[u8]) -> Result<usize> {
        self.stream.write(msg).map_err(|e| {
            self.state = ConnectionState::Terminated;
            From::from(e)
        })
    }

    pub(crate) fn is_terminated(&self) -> bool {
        self.state == ConnectionState::Terminated
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.stream.read_exact(buf).map_err(|e| {
            self.state = ConnectionState::Terminated;
            From::from(e)
        })
    }

    pub fn read_exact_alloc(&mut self, size: u64) -> Result<bytes::Bytes> {
        let len = usize::try_from(size).expect("response size exceeds usize");
        let mut buf = bytes::BytesMut::with_capacity(len);
        buf.resize(len, 0);
        self.read_exact(&mut buf)?;
        Ok(buf.freeze())
    }

    pub(crate) fn shutdown(&mut self) -> Result<()> {
        self.state = ConnectionState::Terminated;
        let r = StreamOps::shutdown(&mut self.stream, Shutdown::Both);
        debug!("Shut down: {:?} => {:?}", self, r);
        r.map_err(From::from)
    }

    fn from_stream(
        mut stream: KafkaStream,
        id: u32,
        host: &str,
        rw_timeout: Option<Duration>,
    ) -> Result<KafkaConnection> {
        StreamOps::set_read_timeout(&mut stream, rw_timeout)?;
        StreamOps::set_write_timeout(&mut stream, rw_timeout)?;
        Ok(KafkaConnection {
            id,
            host: host.to_owned(),
            stream,
            state: ConnectionState::Connected,
        })
    }

    fn new_tcp_stream(host: &str) -> std::io::Result<TcpStream> {
        let mut last_err: Option<std::io::Error> = None;
        for addr in host.to_socket_addrs()? {
            let domain = match addr {
                std::net::SocketAddr::V4(_) => socket2::Domain::IPV4,
                std::net::SocketAddr::V6(_) => socket2::Domain::IPV6,
            };
            let socket =
                socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;

            match socket.connect(&socket2::SockAddr::from(addr)) {
                Ok(()) => {
                    configure_tcp_socket(&socket)?;
                    return Ok(socket.into());
                }
                Err(e) => last_err = Some(e),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                format!("unable to resolve broker address: {host}"),
            )
        }))
    }

    #[cfg(not(feature = "security"))]
    pub(crate) fn new(
        id: u32,
        host: &str,
        rw_timeout: Option<Duration>,
    ) -> Result<KafkaConnection> {
        KafkaConnection::from_stream(Self::new_tcp_stream(host)?, id, host, rw_timeout)
    }

    #[cfg(feature = "security")]
    pub(crate) fn new(
        id: u32,
        host: &str,
        rw_timeout: Option<Duration>,
        security: Option<&SecurityConfig>,
    ) -> Result<KafkaConnection> {
        let tcp_stream = Self::new_tcp_stream(host)?;

        let mut stream = match security.map(SecurityConfig::tls_config) {
            Some(config) => {
                let domain = match host.rfind(':') {
                    None => host,
                    Some(i) => &host[..i],
                };
                let connector = RustlsConnector::new(config)?;
                let tls_stream = connector.connect(domain, tcp_stream)?;
                KafkaStream::Tls(tls_stream)
            }
            None => KafkaStream::Plain(tcp_stream),
        };

        if let Some(sasl) = security.and_then(SecurityConfig::sasl_config) {
            perform_sasl_authentication(&mut stream, sasl)?;
        }

        KafkaConnection::from_stream(stream, id, host, rw_timeout)
    }
}
