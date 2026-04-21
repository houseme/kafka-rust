use super::HeaderResponse;
use crate::error::KafkaCode;
use crate::producer::{ProduceConfirm, ProducePartitionConfirm};

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum ProducerTimestamp {
    CreateTime = 0,
    LogAppendTime = 8,
}

// --------------------------------------------------------------------

#[derive(Default, Debug, Clone)]
pub struct ProduceResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionProduceResponse>,
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionProduceResponse {
    pub topic: String,
    pub partitions: Vec<PartitionProduceResponse>,
}

#[derive(Default, Debug, Clone)]
pub struct PartitionProduceResponse {
    pub partition: i32,
    pub error: i16,
    pub offset: i64,
}

impl ProduceResponse {
    pub fn get_response(self) -> Vec<ProduceConfirm> {
        self.topic_partitions
            .into_iter()
            .map(TopicPartitionProduceResponse::get_response)
            .collect()
    }
}

impl TopicPartitionProduceResponse {
    pub fn get_response(self) -> ProduceConfirm {
        let Self { topic, partitions } = self;
        let partition_confirms = partitions
            .iter()
            .map(PartitionProduceResponse::get_response)
            .collect();
        ProduceConfirm {
            topic,
            partition_confirms,
        }
    }
}

impl PartitionProduceResponse {
    pub fn get_response(&self) -> ProducePartitionConfirm {
        ProducePartitionConfirm {
            partition: self.partition,
            offset: match KafkaCode::from_protocol(self.error) {
                None => Ok(self.offset),
                Some(code) => Err(code),
            },
        }
    }
}
