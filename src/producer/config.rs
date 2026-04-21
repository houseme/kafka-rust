use crate::client::RequiredAcks;

/// The default value for `Builder::with_ack_timeout`.
pub const DEFAULT_ACK_TIMEOUT_MILLIS: u64 = 30 * 1000;

/// The default value for `Builder::with_required_acks`.
pub const DEFAULT_REQUIRED_ACKS: RequiredAcks = RequiredAcks::One;

pub(crate) struct Config {
    pub ack_timeout: i32,
    pub required_acks: i16,
}
