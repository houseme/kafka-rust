//! Kafka administration protocol helpers.

mod acl;
mod cluster;
mod config;
mod group;
mod log_dir;
mod quota;
mod reassignment;
mod share_group;
mod token;
mod topic;
mod transaction;

pub use acl::*;
pub use cluster::*;
pub use config::*;
pub use group::*;
pub use log_dir::*;
pub use quota::*;
pub use reassignment::*;
pub use share_group::*;
pub use token::*;
pub use topic::*;
pub use transaction::*;

use kafka_protocol::messages::{ApiKey, GroupId, RequestHeader};
use kafka_protocol::protocol::StrBytes;

pub(crate) fn request_header(
    correlation_id: i32,
    client_id: &str,
    api_key: ApiKey,
    api_version: i16,
) -> RequestHeader {
    RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(api_key as i16)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id)
}

pub(crate) fn optional_str_bytes(value: Option<&str>) -> Option<StrBytes> {
    value.map(|value| StrBytes::from_string(value.to_owned()))
}

pub(crate) fn to_add_raft_listener(
    listener: &RaftVoterListener,
) -> kafka_protocol::messages::add_raft_voter_request::Listener {
    kafka_protocol::messages::add_raft_voter_request::Listener::default()
        .with_name(StrBytes::from_string(listener.name.clone()))
        .with_host(StrBytes::from_string(listener.host.clone()))
        .with_port(listener.port)
}

pub(crate) fn str_bytes_vec(values: &[&str]) -> Vec<StrBytes> {
    values
        .iter()
        .map(|value| StrBytes::from_string((*value).to_owned()))
        .collect()
}

pub(crate) fn group_id(value: &str) -> GroupId {
    GroupId::from(StrBytes::from_string(value.to_owned()))
}

pub(crate) fn transactional_id(value: &str) -> kafka_protocol::messages::TransactionalId {
    kafka_protocol::messages::TransactionalId::from(StrBytes::from_string(value.to_owned()))
}
