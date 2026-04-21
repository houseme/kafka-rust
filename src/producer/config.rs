use crate::client::RequiredAcks;

/// The default value for `Builder::with_ack_timeout`.
pub const DEFAULT_ACK_TIMEOUT_MILLIS: u64 = 30 * 1000;

/// The default value for `Builder::with_required_acks`.
pub const DEFAULT_REQUIRED_ACKS: RequiredAcks = RequiredAcks::One;

/// The default batch size for `BatchProducer`.
pub const DEFAULT_BATCH_SIZE: usize = 16_384;

/// The default linger time in milliseconds for `BatchProducer`.
pub const DEFAULT_LINGER_MS: u64 = 5;

/// The default max bytes per batch for `BatchProducer`.
pub const DEFAULT_MAX_BATCH_BYTES: usize = 1_048_576;

pub(crate) struct Config {
    pub ack_timeout: i32,
    pub required_acks: i16,
    pub enable_idempotence: bool,
    pub transactional_id: Option<String>,
}

/// Configuration for batch producing.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of messages per batch.
    pub batch_size: usize,
    /// Maximum time to wait before flushing a batch (milliseconds).
    pub linger_ms: u64,
    /// Maximum total bytes per batch (across all partitions).
    pub max_bytes: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            linger_ms: DEFAULT_LINGER_MS,
            max_bytes: DEFAULT_MAX_BATCH_BYTES,
        }
    }
}
