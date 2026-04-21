use super::HeaderResponse;
use crate::error::{Error, KafkaCode, Result};
use crate::utils::PartitionOffset;

#[derive(Debug, Default)]
pub struct GroupCoordinatorResponse {
    pub header: HeaderResponse,
    pub error: i16,
    pub broker_id: i32,
    pub port: i32,
    pub host: String,
}

impl GroupCoordinatorResponse {
    pub fn into_result(self) -> Result<Self> {
        match Error::from_protocol(self.error) {
            Some(e) => Err(e),
            None => Ok(self),
        }
    }
}

// --------------------------------------------------------------------

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OffsetFetchVersion {
    V0 = 0,
    V1 = 1,
}

#[derive(Default, Debug)]
pub struct OffsetFetchResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetFetchResponse>,
}

#[derive(Default, Debug)]
pub struct TopicPartitionOffsetFetchResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetFetchResponse>,
}

#[derive(Default, Debug)]
pub struct PartitionOffsetFetchResponse {
    pub partition: i32,
    pub offset: i64,
    pub metadata: String,
    pub error: i16,
}

impl PartitionOffsetFetchResponse {
    pub fn get_offsets(&self) -> Result<PartitionOffset> {
        match Error::from_protocol(self.error) {
            Some(Error::Kafka(KafkaCode::UnknownTopicOrPartition)) => {
                Ok(PartitionOffset {
                    partition: self.partition,
                    offset: -1,
                })
            }
            Some(e) => Err(e),
            None => Ok(PartitionOffset {
                partition: self.partition,
                offset: self.offset,
            }),
        }
    }
}

// --------------------------------------------------------------------

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OffsetCommitVersion {
    V0 = 0,
    V1 = 1,
    V2 = 2,
}

#[derive(Default, Debug)]
pub struct OffsetCommitResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetCommitResponse>,
}

#[derive(Default, Debug)]
pub struct TopicPartitionOffsetCommitResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetCommitResponse>,
}

#[derive(Default, Debug)]
pub struct PartitionOffsetCommitResponse {
    pub partition: i32,
    pub error: i16,
}

impl PartitionOffsetCommitResponse {
    pub fn to_error(&self) -> Option<KafkaCode> {
        KafkaCode::from_protocol(self.error)
    }
}
