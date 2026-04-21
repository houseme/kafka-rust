use super::HeaderResponse;

#[derive(Default, Debug)]
pub struct MetadataResponse {
    pub header: HeaderResponse,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Default, Debug)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Default, Debug)]
pub struct TopicMetadata {
    pub error: i16,
    pub topic: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Default, Debug)]
pub struct PartitionMetadata {
    pub error: i16,
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}
