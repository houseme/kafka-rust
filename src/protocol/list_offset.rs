use super::HeaderResponse;
use crate::error::KafkaCode;
use crate::utils::TimestampedPartitionOffset;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ListOffsetVersion {
    V1 = 1,
}

#[derive(Default, Debug)]
pub struct ListOffsetsResponse {
    pub header: HeaderResponse,
    pub topics: Vec<TopicListOffsetsResponse>,
}

#[derive(Default, Debug)]
pub struct TopicListOffsetsResponse {
    pub topic: String,
    pub partitions: Vec<TimestampedPartitionOffsetListOffsetsResponse>,
}

#[derive(Default, Debug)]
pub struct TimestampedPartitionOffsetListOffsetsResponse {
    pub partition: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
}

impl TimestampedPartitionOffsetListOffsetsResponse {
    pub fn to_offset(&self) -> std::result::Result<TimestampedPartitionOffset, KafkaCode> {
        match KafkaCode::from_protocol(self.error_code) {
            Some(code) => Err(code),
            None => Ok(TimestampedPartitionOffset {
                partition: self.partition,
                offset: self.offset,
                time: self.timestamp,
            }),
        }
    }
}
