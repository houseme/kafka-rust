//! Low-level helpers for modern consumer/share-consumer protocol messages.
//!
//! These helpers expose generated Kafka request/response shapes through crate
//! DTOs and a small client-side session helper. They do not run a background
//! fetch or acknowledgement loop.

mod acknowledge;
mod fetch;
mod heartbeat;
mod session;

pub use acknowledge::*;
pub use fetch::*;
pub use heartbeat::*;
pub use session::{ShareConsumerSession, ShareFetchSessionConfig};

use kafka_protocol::messages::{ApiKey, GroupId, RequestHeader, TopicName};
use kafka_protocol::protocol::StrBytes;

use super::{
    API_VERSION_CONSUMER_GROUP_HEARTBEAT, API_VERSION_SHARE_ACKNOWLEDGE, API_VERSION_SHARE_FETCH,
    API_VERSION_SHARE_GROUP_HEARTBEAT,
};

/// Share acknowledgement type for a gap.
pub const SHARE_ACK_TYPE_GAP: i8 = 0;
/// Share acknowledgement type for accepted records.
pub const SHARE_ACK_TYPE_ACCEPT: i8 = 1;
/// Share acknowledgement type for released records.
pub const SHARE_ACK_TYPE_RELEASE: i8 = 2;
/// Share acknowledgement type for rejected records.
pub const SHARE_ACK_TYPE_REJECT: i8 = 3;

/// One share acknowledgement batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgementBatch {
    /// First offset in the batch.
    pub first_offset: i64,
    /// Last offset in the batch, inclusive.
    pub last_offset: i64,
    /// One Kafka share acknowledgement type code per acknowledged record.
    pub acknowledge_types: Vec<i8>,
}

impl ShareAcknowledgementBatch {
    /// Create an acknowledgement batch.
    #[must_use]
    pub fn new<I>(first_offset: i64, last_offset: i64, acknowledge_types: I) -> Self
    where
        I: IntoIterator<Item = i8>,
    {
        Self {
            first_offset,
            last_offset,
            acknowledge_types: acknowledge_types.into_iter().collect(),
        }
    }
}

/// Current leader reference returned in share responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareLeader {
    /// Current leader broker ID.
    pub leader_id: i32,
    /// Current leader epoch.
    pub leader_epoch: i32,
}

/// Broker endpoint returned in share responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareNodeEndpoint {
    /// Broker ID.
    pub node_id: i32,
    /// Broker host.
    pub host: String,
    /// Broker port.
    pub port: i32,
    /// Optional broker rack.
    pub rack: Option<String>,
}

// ---------------------------------------------------------------------------
// Shared private helpers
// ---------------------------------------------------------------------------

fn request_header(
    correlation_id: i32,
    client_id: &str,
    api_key: ApiKey,
    api_version: i16,
) -> RequestHeader {
    RequestHeader::default()
        .with_client_id(Some(str_bytes(client_id.to_owned())))
        .with_request_api_key(api_key as i16)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id)
}

fn str_bytes(value: String) -> StrBytes {
    StrBytes::from_string(value)
}

fn group_id(value: String) -> GroupId {
    GroupId(str_bytes(value))
}

fn optional_group_id(value: Option<String>) -> Option<GroupId> {
    value.map(group_id)
}

fn optional_str_bytes(value: Option<String>) -> Option<StrBytes> {
    value.map(str_bytes)
}

fn optional_string(value: Option<StrBytes>) -> Option<String> {
    value.map(|value| value.to_string())
}

fn optional_topic_names(values: Option<Vec<String>>) -> Option<Vec<TopicName>> {
    values.map(|items| {
        items
            .into_iter()
            .map(|topic| TopicName(str_bytes(topic)))
            .collect()
    })
}
