#![allow(clippy::wildcard_imports)]
//! Log directory administration helpers.

use kafka_protocol::messages::{
    AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse, ApiKey, DescribeLogDirsRequest,
    DescribeLogDirsResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{API_VERSION_ALTER_REPLICA_LOG_DIRS, API_VERSION_DESCRIBE_LOG_DIRS};
use super::request_header;
use super::*;

/// Partition storage details returned by `DescribeLogDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Size of log segments in bytes.
    pub partition_size: i64,
    /// Log end offset lag relative to the partition watermark or replica log.
    pub offset_lag: i64,
    /// Whether this is a future log created by replica movement.
    pub is_future_key: bool,
}

/// Per-topic storage details inside one log directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirTopic {
    /// Topic name.
    pub name: String,
    /// Partitions present in the log directory.
    pub partitions: Vec<LogDirPartition>,
}

/// One log directory returned by `DescribeLogDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirDescription {
    /// Per-log-directory broker error code.
    pub error_code: i16,
    /// Absolute broker log directory path.
    pub log_dir: String,
    /// Topics present in the log directory.
    pub topics: Vec<LogDirTopic>,
    /// Total bytes on the backing volume, or Kafka's `-1` sentinel before v4.
    pub total_bytes: i64,
    /// Usable bytes on the backing volume, or Kafka's `-1` sentinel before v4.
    pub usable_bytes: i64,
}

/// Parsed response from a `DescribeLogDirs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Log directories returned by the broker.
    pub results: Vec<LogDirDescription>,
}

/// A log directory path with topic partitions to move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to move.
    pub partitions: Vec<i32>,
}

impl AlterReplicaLogDirTopic {
    /// Create a topic partition spec for log dir alteration.
    #[must_use]
    pub fn new<I>(topic: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// A log directory with topic partitions to move there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDir {
    /// Absolute directory path.
    pub path: String,
    /// Topics with partitions to move to this directory.
    pub topics: Vec<AlterReplicaLogDirTopic>,
}

impl AlterReplicaLogDir {
    /// Create a log directory spec.
    #[must_use]
    pub fn new(path: impl Into<String>, topics: Vec<AlterReplicaLogDirTopic>) -> Self {
        Self {
            path: path.into(),
            topics,
        }
    }
}

/// Per-partition result in an `AlterReplicaLogDirs` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
}

/// Per-topic result in an `AlterReplicaLogDirs` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirTopicResult {
    /// Topic name.
    pub topic_name: String,
    /// Per-partition results.
    pub partitions: Vec<AlterReplicaLogDirPartitionResult>,
}

/// Parsed response from an `AlterReplicaLogDirs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-topic results.
    pub results: Vec<AlterReplicaLogDirTopicResult>,
}

pub fn build_alter_replica_log_dirs_request(
    correlation_id: i32,
    client_id: &str,
    dirs: &[AlterReplicaLogDir],
) -> (RequestHeader, AlterReplicaLogDirsRequest) {
    use kafka_protocol::messages::alter_replica_log_dirs_request::{
        AlterReplicaLogDir as KpAlterReplicaLogDir,
        AlterReplicaLogDirTopic as KpAlterReplicaLogDirTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::AlterReplicaLogDirs,
        API_VERSION_ALTER_REPLICA_LOG_DIRS,
    );
    let log_dirs: Vec<KpAlterReplicaLogDir> = dirs
        .iter()
        .map(|dir| {
            KpAlterReplicaLogDir::default()
                .with_path(StrBytes::from_string(dir.path.clone()))
                .with_topics(
                    dir.topics
                        .iter()
                        .map(|topic| {
                            KpAlterReplicaLogDirTopic::default()
                                .with_name(StrBytes::from_string(topic.topic.clone()).into())
                                .with_partitions(topic.partitions.clone())
                        })
                        .collect(),
                )
        })
        .collect();
    let request = AlterReplicaLogDirsRequest::default().with_dirs(log_dirs);

    (header, request)
}

/// Build a `DescribeDelegationToken` request.
pub fn build_describe_log_dirs_request(
    correlation_id: i32,
    client_id: &str,
    topics: Option<&[TopicPartitionFilter]>,
) -> (RequestHeader, DescribeLogDirsRequest) {
    use kafka_protocol::messages::describe_log_dirs_request::DescribableLogDirTopic;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeLogDirs,
        API_VERSION_DESCRIBE_LOG_DIRS,
    );
    let topics = topics.map(|topics| {
        topics
            .iter()
            .map(|topic| {
                DescribableLogDirTopic::default()
                    .with_topic(StrBytes::from_string(topic.topic.clone()).into())
                    .with_partitions(topic.partitions.clone())
            })
            .collect()
    });
    let request = DescribeLogDirsRequest::default().with_topics(topics);

    (header, request)
}

/// Build a `DeleteRecords` request.
pub fn convert_alter_replica_log_dirs_response(
    response: AlterReplicaLogDirsResponse,
) -> AlterReplicaLogDirsResponseData {
    AlterReplicaLogDirsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        results: response
            .results
            .into_iter()
            .map(|topic| AlterReplicaLogDirTopicResult {
                topic_name: topic.topic_name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|p| AlterReplicaLogDirPartitionResult {
                        partition_index: p.partition_index,
                        error_code: p.error_code,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeLogDirsResponse` into the crate's public shape.
pub fn convert_describe_log_dirs_response(
    response: DescribeLogDirsResponse,
) -> DescribeLogDirsResponseData {
    DescribeLogDirsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        results: response
            .results
            .into_iter()
            .map(|result| LogDirDescription {
                error_code: result.error_code,
                log_dir: result.log_dir.to_string(),
                topics: result
                    .topics
                    .into_iter()
                    .map(|topic| LogDirTopic {
                        name: topic.name.to_string(),
                        partitions: topic
                            .partitions
                            .into_iter()
                            .map(|partition| LogDirPartition {
                                partition_index: partition.partition_index,
                                partition_size: partition.partition_size,
                                offset_lag: partition.offset_lag,
                                is_future_key: partition.is_future_key,
                            })
                            .collect(),
                    })
                    .collect(),
                total_bytes: result.total_bytes,
                usable_bytes: result.usable_bytes,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::messages::alter_replica_log_dirs_response::{
        AlterReplicaLogDirPartitionResult as KpAlterReplicaLogDirPartitionResult,
        AlterReplicaLogDirTopicResult as KpAlterReplicaLogDirTopicResult,
    };
    use kafka_protocol::messages::describe_log_dirs_response::{
        DescribeLogDirsPartition as KpDescribeLogDirsPartition,
        DescribeLogDirsResult as KpDescribeLogDirsResult,
        DescribeLogDirsTopic as KpDescribeLogDirsTopic,
    };
    use kafka_protocol::protocol::StrBytes;

    #[test]
    fn describe_log_dirs_request_fetches_all_topics_when_filter_is_absent() {
        let (header, request) = build_describe_log_dirs_request(14, "client-i", None);

        assert_eq!(header.request_api_key, ApiKey::DescribeLogDirs as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_LOG_DIRS);
        assert!(request.topics.is_none());
    }

    #[test]
    fn describe_log_dirs_request_fetches_selected_partitions() {
        let filter = [TopicPartitionFilter::new("topic-a", [0, 2])];
        let (_, request) = build_describe_log_dirs_request(15, "client-j", Some(&filter));

        let topic = &request.topics.as_ref().unwrap()[0];
        assert_eq!(topic.topic.to_string(), "topic-a");
        assert_eq!(topic.partitions, vec![0, 2]);
    }
    #[test]
    fn convert_describe_log_dirs_response_preserves_storage_details() {
        let response = DescribeLogDirsResponse::default()
            .with_throttle_time_ms(16)
            .with_error_code(0)
            .with_results(vec![
                KpDescribeLogDirsResult::default()
                    .with_error_code(0)
                    .with_log_dir(StrBytes::from_static_str("/kafka-logs"))
                    .with_total_bytes(1_000)
                    .with_usable_bytes(750)
                    .with_topics(vec![
                        KpDescribeLogDirsTopic::default()
                            .with_name(StrBytes::from_static_str("topic-a").into())
                            .with_partitions(vec![
                                KpDescribeLogDirsPartition::default()
                                    .with_partition_index(0)
                                    .with_partition_size(256)
                                    .with_offset_lag(3)
                                    .with_is_future_key(true),
                            ]),
                    ]),
            ]);

        let converted = convert_describe_log_dirs_response(response);

        assert_eq!(converted.throttle_time_ms, 16);
        assert_eq!(converted.results[0].log_dir, "/kafka-logs");
        assert_eq!(converted.results[0].total_bytes, 1_000);
        assert_eq!(converted.results[0].topics[0].name, "topic-a");
        assert_eq!(converted.results[0].topics[0].partitions[0].offset_lag, 3);
        assert!(converted.results[0].topics[0].partitions[0].is_future_key);
    }
    #[test]
    fn alter_replica_log_dirs_request_includes_paths_and_topics() {
        let dirs = [AlterReplicaLogDir::new(
            "/kafka-logs-2",
            vec![AlterReplicaLogDirTopic::new("topic-a", [0, 1])],
        )];
        let (header, request) = build_alter_replica_log_dirs_request(31, "client-y", &dirs);

        assert_eq!(header.request_api_key, ApiKey::AlterReplicaLogDirs as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_ALTER_REPLICA_LOG_DIRS
        );
        assert_eq!(request.dirs[0].path.to_string(), "/kafka-logs-2");
        assert_eq!(request.dirs[0].topics[0].name.to_string(), "topic-a");
        assert_eq!(request.dirs[0].topics[0].partitions, vec![0, 1]);
    }
    #[test]
    fn alter_replica_log_dirs_response_maps_all_fields() {
        let response = AlterReplicaLogDirsResponse::default()
            .with_throttle_time_ms(20)
            .with_results(vec![
                KpAlterReplicaLogDirTopicResult::default()
                    .with_topic_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpAlterReplicaLogDirPartitionResult::default()
                            .with_partition_index(0)
                            .with_error_code(0),
                        KpAlterReplicaLogDirPartitionResult::default()
                            .with_partition_index(1)
                            .with_error_code(15),
                    ]),
            ]);

        let converted = convert_alter_replica_log_dirs_response(response);

        assert_eq!(converted.throttle_time_ms, 20);
        assert_eq!(converted.results[0].topic_name, "topic-a");
        assert_eq!(
            converted.results[0].partitions,
            vec![
                AlterReplicaLogDirPartitionResult {
                    partition_index: 0,
                    error_code: 0,
                },
                AlterReplicaLogDirPartitionResult {
                    partition_index: 1,
                    error_code: 15,
                },
            ]
        );
    }
}
