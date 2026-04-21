use super::HeaderResponse;
use crate::error::KafkaCode;
use crate::utils::PartitionOffset;

#[derive(Default, Debug)]
pub struct OffsetResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetResponse>,
}

#[derive(Default, Debug)]
pub struct TopicPartitionOffsetResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetResponse>,
}

#[derive(Default, Debug)]
pub struct PartitionOffsetResponse {
    pub partition: i32,
    pub error: i16,
    pub offset: Vec<i64>,
}

impl PartitionOffsetResponse {
    pub fn to_offset(&self) -> std::result::Result<PartitionOffset, KafkaCode> {
        if let Some(code) = KafkaCode::from_protocol(self.error) {
            Err(code)
        } else {
            let offset = self.offset.first().copied().unwrap_or(-1);
            Ok(PartitionOffset {
                partition: self.partition,
                offset,
            })
        }
    }
}
