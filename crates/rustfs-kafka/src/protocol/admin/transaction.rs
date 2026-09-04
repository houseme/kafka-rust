#![allow(clippy::wildcard_imports)]
//! Transaction administration helpers.

use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, ApiKey, DescribeTransactionsRequest,
    DescribeTransactionsResponse, ListTransactionsRequest, ListTransactionsResponse, ProducerId,
    RequestHeader, TxnOffsetCommitRequest, TxnOffsetCommitResponse,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{
    API_VERSION_ADD_OFFSETS_TO_TXN, API_VERSION_DESCRIBE_TRANSACTIONS,
    API_VERSION_LIST_TRANSACTIONS, API_VERSION_TXN_OFFSET_COMMIT,
};
use super::{group_id, request_header, transactional_id};

/// Filters for a `ListTransactions` request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListTransactionsOptions {
    /// Transaction states to include, or empty to include all states.
    pub state_filters: Vec<String>,
    /// Producer IDs to include, or empty to include all producer IDs.
    pub producer_id_filters: Vec<i64>,
    /// Minimum running duration in milliseconds, or `None` to include all durations.
    pub duration_filter_ms: Option<i64>,
    /// Optional transactional ID regular expression pattern.
    pub transactional_id_pattern: Option<String>,
}

impl ListTransactionsOptions {
    /// Create default options that list all transactions visible to the broker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Restrict the request to the supplied transaction states.
    #[must_use]
    pub fn with_state_filters<I, S>(mut self, states: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.state_filters = states.into_iter().map(Into::into).collect();
        self
    }

    /// Restrict the request to the supplied producer IDs.
    #[must_use]
    pub fn with_producer_id_filters<I>(mut self, producer_ids: I) -> Self
    where
        I: IntoIterator<Item = i64>,
    {
        self.producer_id_filters = producer_ids.into_iter().collect();
        self
    }

    /// Restrict the request to transactions running longer than the supplied duration.
    #[must_use]
    pub fn with_duration_filter_ms(mut self, duration_ms: i64) -> Self {
        self.duration_filter_ms = Some(duration_ms);
        self
    }

    /// Restrict the request to transactional IDs matching the supplied pattern.
    #[must_use]
    pub fn with_transactional_id_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.transactional_id_pattern = Some(pattern.into());
        self
    }
}

/// Summary state for one transaction returned by `ListTransactions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedTransaction {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID currently associated with the transaction.
    pub producer_id: i64,
    /// Current transaction state.
    pub transaction_state: String,
}

/// Parsed response from a `ListTransactions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTransactionsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Requested state filters unknown to the transaction coordinator.
    pub unknown_state_filters: Vec<String>,
    /// Transaction summaries returned by the broker.
    pub transaction_states: Vec<ListedTransaction>,
}

/// Topic partitions included in a described transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionTopic {
    /// Topic name.
    pub topic: String,
    /// Partition IDs included in the transaction.
    pub partitions: Vec<i32>,
}

/// Detailed state for one transaction returned by `DescribeTransactions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTransaction {
    /// Per-transaction broker error code.
    pub error_code: i16,
    /// Transactional ID.
    pub transactional_id: String,
    /// Current transaction state.
    pub transaction_state: String,
    /// Transaction timeout in milliseconds.
    pub transaction_timeout_ms: i32,
    /// Transaction start time in milliseconds since Unix epoch.
    pub transaction_start_time_ms: i64,
    /// Producer ID currently associated with the transaction.
    pub producer_id: i64,
    /// Producer epoch currently associated with the transaction.
    pub producer_epoch: i16,
    /// Topic partitions included in the current transaction.
    pub topics: Vec<TransactionTopic>,
}

/// Parsed response from a `DescribeTransactions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTransactionsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Detailed transaction states returned by the broker.
    pub transaction_states: Vec<DescribedTransaction>,
}

/// Parsed response from an `AddOffsetsToTxn` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddOffsetsToTxnResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Broker error code.
    pub error_code: i16,
}

/// Per-partition result in a `TxnOffsetCommit` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
}

/// Per-topic result in a `TxnOffsetCommit` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitTopicResult {
    /// Topic name.
    pub topic: String,
    /// Per-partition commit results.
    pub partitions: Vec<TxnOffsetCommitPartitionResult>,
}

/// Parsed response from a `TxnOffsetCommit` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-topic offset commit results.
    pub topics: Vec<TxnOffsetCommitTopicResult>,
}

/// A topic/partition/offset tuple for `TxnOffsetCommit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitTopicPartition {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Offset to commit.
    pub offset: i64,
    /// Optional leader epoch.
    pub leader_epoch: Option<i32>,
    /// Optional metadata string.
    pub metadata: Option<String>,
}

impl TxnOffsetCommitTopicPartition {
    /// Create a transactional offset commit entry.
    #[must_use]
    pub fn new(topic: impl Into<String>, partition: i32, offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            leader_epoch: None,
            metadata: None,
        }
    }

    /// Set the committed leader epoch.
    #[must_use]
    pub fn with_leader_epoch(mut self, leader_epoch: i32) -> Self {
        self.leader_epoch = Some(leader_epoch);
        self
    }

    /// Set committed offset metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: impl Into<String>) -> Self {
        self.metadata = Some(metadata.into());
        self
    }
}

pub fn build_list_transactions_request(
    correlation_id: i32,
    client_id: &str,
    options: &ListTransactionsOptions,
) -> (RequestHeader, ListTransactionsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ListTransactions,
        API_VERSION_LIST_TRANSACTIONS,
    );
    let request = ListTransactionsRequest::default()
        .with_state_filters(
            options
                .state_filters
                .iter()
                .map(|state| StrBytes::from_string(state.clone()))
                .collect(),
        )
        .with_producer_id_filters(
            options
                .producer_id_filters
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
        )
        .with_duration_filter(options.duration_filter_ms.unwrap_or(-1))
        .with_transactional_id_pattern(
            options
                .transactional_id_pattern
                .as_ref()
                .map(|pattern| StrBytes::from_string(pattern.clone())),
        );

    (header, request)
}

/// Build a `DescribeTransactions` request.
pub fn build_describe_transactions_request(
    correlation_id: i32,
    client_id: &str,
    transactional_ids: &[&str],
) -> (RequestHeader, DescribeTransactionsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeTransactions,
        API_VERSION_DESCRIBE_TRANSACTIONS,
    );
    let request = DescribeTransactionsRequest::default().with_transactional_ids(
        transactional_ids
            .iter()
            .map(|id| transactional_id(id))
            .collect(),
    );

    (header, request)
}

/// Build an `AddOffsetsToTxn` request.
pub fn build_add_offsets_to_txn_request(
    correlation_id: i32,
    client_id: &str,
    txn_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group_id_str: &str,
) -> (RequestHeader, AddOffsetsToTxnRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::AddOffsetsToTxn,
        API_VERSION_ADD_OFFSETS_TO_TXN,
    );
    let request = AddOffsetsToTxnRequest::default()
        .with_transactional_id(transactional_id(txn_id))
        .with_producer_id(ProducerId::from(producer_id))
        .with_producer_epoch(producer_epoch)
        .with_group_id(group_id(group_id_str));

    (header, request)
}

/// Build a `TxnOffsetCommit` request.
pub fn build_txn_offset_commit_request(
    correlation_id: i32,
    client_id: &str,
    txn_id: &str,
    group_id_str: &str,
    producer_id: i64,
    producer_epoch: i16,
    topics: &[TxnOffsetCommitTopicPartition],
) -> (RequestHeader, TxnOffsetCommitRequest) {
    use kafka_protocol::messages::txn_offset_commit_request::{
        TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::TxnOffsetCommit,
        API_VERSION_TXN_OFFSET_COMMIT,
    );

    // Group topics by name
    let mut topic_map: std::collections::BTreeMap<String, Vec<&TxnOffsetCommitTopicPartition>> =
        std::collections::BTreeMap::new();
    for tp in topics {
        topic_map.entry(tp.topic.clone()).or_default().push(tp);
    }

    let topic_list: Vec<TxnOffsetCommitRequestTopic> = topic_map
        .into_iter()
        .map(|(name, partitions)| {
            TxnOffsetCommitRequestTopic::default()
                .with_name(StrBytes::from_string(name).into())
                .with_partitions(
                    partitions
                        .iter()
                        .map(|p| {
                            let mut part = TxnOffsetCommitRequestPartition::default()
                                .with_partition_index(p.partition)
                                .with_committed_offset(p.offset);
                            if let Some(epoch) = p.leader_epoch {
                                part = part.with_committed_leader_epoch(epoch);
                            }
                            match p.metadata {
                                Some(ref meta) => {
                                    part = part.with_committed_metadata(Some(
                                        StrBytes::from_string(meta.clone()),
                                    ));
                                }
                                None => {
                                    part = part.with_committed_metadata(None);
                                }
                            }
                            part
                        })
                        .collect(),
                )
        })
        .collect();

    let request = TxnOffsetCommitRequest::default()
        .with_transactional_id(transactional_id(txn_id))
        .with_group_id(group_id(group_id_str))
        .with_producer_id(ProducerId::from(producer_id))
        .with_producer_epoch(producer_epoch)
        .with_topics(topic_list);

    (header, request)
}

/// Build a `CreateDelegationToken` request.
pub fn convert_list_transactions_response(
    response: ListTransactionsResponse,
) -> ListTransactionsResponseData {
    ListTransactionsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        unknown_state_filters: response
            .unknown_state_filters
            .into_iter()
            .map(|state| state.to_string())
            .collect(),
        transaction_states: response
            .transaction_states
            .into_iter()
            .map(|transaction| ListedTransaction {
                transactional_id: transaction.transactional_id.to_string(),
                producer_id: i64::from(transaction.producer_id),
                transaction_state: transaction.transaction_state.to_string(),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeTransactionsResponse` into the crate's public shape.
pub fn convert_describe_transactions_response(
    response: DescribeTransactionsResponse,
) -> DescribeTransactionsResponseData {
    DescribeTransactionsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        transaction_states: response
            .transaction_states
            .into_iter()
            .map(|transaction| DescribedTransaction {
                error_code: transaction.error_code,
                transactional_id: transaction.transactional_id.to_string(),
                transaction_state: transaction.transaction_state.to_string(),
                transaction_timeout_ms: transaction.transaction_timeout_ms,
                transaction_start_time_ms: transaction.transaction_start_time_ms,
                producer_id: i64::from(transaction.producer_id),
                producer_epoch: transaction.producer_epoch,
                topics: transaction
                    .topics
                    .into_iter()
                    .map(|topic| TransactionTopic {
                        topic: topic.topic.to_string(),
                        partitions: topic.partitions,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `AddOffsetsToTxnResponse` into the crate's public shape.
pub fn convert_add_offsets_to_txn_response(
    response: &AddOffsetsToTxnResponse,
) -> AddOffsetsToTxnResponseData {
    AddOffsetsToTxnResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
    }
}

/// Convert a generated `TxnOffsetCommitResponse` into the crate's public shape.
pub fn convert_txn_offset_commit_response(
    response: TxnOffsetCommitResponse,
) -> TxnOffsetCommitResponseData {
    TxnOffsetCommitResponseData {
        throttle_time_ms: response.throttle_time_ms,
        topics: response
            .topics
            .into_iter()
            .map(|topic| TxnOffsetCommitTopicResult {
                topic: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|p| TxnOffsetCommitPartitionResult {
                        partition_index: p.partition_index,
                        error_code: p.error_code,
                    })
                    .collect(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::describe_transactions_response::{
        TopicData as KpDescribeTransactionTopic, TransactionState as KpDescribedTransactionState,
    };
    use kafka_protocol::messages::list_transactions_response::TransactionState as KpListedTransactionState;
    use kafka_protocol::messages::{ApiKey, ProducerId};

    #[test]
    fn list_transactions_request_accepts_all_filters() {
        let options = ListTransactionsOptions::new()
            .with_state_filters(["Ongoing", "PrepareCommit"])
            .with_producer_id_filters([42, 43])
            .with_duration_filter_ms(30_000)
            .with_transactional_id_pattern("rustfs-.*");
        let (header, request) = build_list_transactions_request(28, "client-w", &options);

        assert_eq!(header.request_api_key, ApiKey::ListTransactions as i16);
        assert_eq!(header.request_api_version, API_VERSION_LIST_TRANSACTIONS);
        assert_eq!(
            request.state_filters,
            vec![
                StrBytes::from_static_str("Ongoing"),
                StrBytes::from_static_str("PrepareCommit"),
            ]
        );
        assert_eq!(
            request
                .producer_id_filters
                .into_iter()
                .map(i64::from)
                .collect::<Vec<_>>(),
            vec![42, 43]
        );
        assert_eq!(request.duration_filter, 30_000);
        assert_eq!(
            request
                .transactional_id_pattern
                .map(|value| value.to_string()),
            Some("rustfs-.*".to_owned())
        );
    }

    #[test]
    fn describe_transactions_request_includes_transactional_ids() {
        let (header, request) =
            build_describe_transactions_request(26, "client-u", &["txn-a", "txn-b"]);

        assert_eq!(header.request_api_key, ApiKey::DescribeTransactions as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_DESCRIBE_TRANSACTIONS
        );
        assert_eq!(request.transactional_ids[0].to_string(), "txn-a");
        assert_eq!(request.transactional_ids[1].to_string(), "txn-b");
    }
    #[test]
    fn convert_list_transactions_response_preserves_state_filters_and_transactions() {
        let response = ListTransactionsResponse::default()
            .with_throttle_time_ms(22)
            .with_error_code(0)
            .with_unknown_state_filters(vec![StrBytes::from_static_str("UnknownState")])
            .with_transaction_states(vec![
                KpListedTransactionState::default()
                    .with_transactional_id(transactional_id("txn-a"))
                    .with_producer_id(ProducerId::from(42))
                    .with_transaction_state(StrBytes::from_static_str("Ongoing")),
            ]);

        let converted = convert_list_transactions_response(response);

        assert_eq!(converted.throttle_time_ms, 22);
        assert_eq!(converted.unknown_state_filters, vec!["UnknownState"]);
        assert_eq!(
            converted.transaction_states,
            vec![ListedTransaction {
                transactional_id: "txn-a".to_owned(),
                producer_id: 42,
                transaction_state: "Ongoing".to_owned(),
            }]
        );
    }

    #[test]
    fn convert_describe_transactions_response_preserves_transaction_details() {
        let response = DescribeTransactionsResponse::default()
            .with_throttle_time_ms(23)
            .with_transaction_states(vec![
                KpDescribedTransactionState::default()
                    .with_error_code(0)
                    .with_transactional_id(transactional_id("txn-a"))
                    .with_transaction_state(StrBytes::from_static_str("Ongoing"))
                    .with_transaction_timeout_ms(60_000)
                    .with_transaction_start_time_ms(1_700_000)
                    .with_producer_id(ProducerId::from(42))
                    .with_producer_epoch(3)
                    .with_topics(vec![
                        KpDescribeTransactionTopic::default()
                            .with_topic(StrBytes::from_static_str("topic-a").into())
                            .with_partitions(vec![0, 1]),
                    ]),
            ]);

        let converted = convert_describe_transactions_response(response);

        assert_eq!(converted.throttle_time_ms, 23);
        assert_eq!(converted.transaction_states[0].transactional_id, "txn-a");
        assert_eq!(
            converted.transaction_states[0].transaction_timeout_ms,
            60_000
        );
        assert_eq!(converted.transaction_states[0].producer_id, 42);
        assert_eq!(converted.transaction_states[0].producer_epoch, 3);
        assert_eq!(converted.transaction_states[0].topics[0].topic, "topic-a");
        assert_eq!(
            converted.transaction_states[0].topics[0].partitions,
            vec![0, 1]
        );
    }

    #[test]
    fn add_offsets_to_txn_request_includes_transaction_and_group() {
        let (header, request) =
            build_add_offsets_to_txn_request(30, "client-x", "txn-a", 42, 3, "group-a");

        assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
        assert_eq!(header.request_api_version, API_VERSION_ADD_OFFSETS_TO_TXN);
        assert_eq!(header.correlation_id, 30);
        assert_eq!(
            header.client_id.as_ref().map(ToString::to_string),
            Some("client-x".to_owned())
        );
        assert_eq!(request.transactional_id.to_string(), "txn-a");
        assert_eq!(i64::from(request.producer_id), 42);
        assert_eq!(request.producer_epoch, 3);
        assert_eq!(request.group_id.to_string(), "group-a");
    }

    #[test]
    fn txn_offset_commit_request_groups_topics() {
        let offsets = vec![
            TxnOffsetCommitTopicPartition {
                topic: "topic-a".to_owned(),
                partition: 0,
                offset: 10,
                leader_epoch: Some(5),
                metadata: Some("meta-a".to_owned()),
            },
            TxnOffsetCommitTopicPartition {
                topic: "topic-a".to_owned(),
                partition: 1,
                offset: 20,
                leader_epoch: None,
                metadata: None,
            },
            TxnOffsetCommitTopicPartition {
                topic: "topic-b".to_owned(),
                partition: 0,
                offset: 30,
                leader_epoch: Some(7),
                metadata: None,
            },
        ];
        let (header, request) =
            build_txn_offset_commit_request(31, "client-y", "txn-b", "group-b", 43, 4, &offsets);

        assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
        assert_eq!(header.request_api_version, API_VERSION_TXN_OFFSET_COMMIT);
        assert_eq!(request.transactional_id.to_string(), "txn-b");
        assert_eq!(request.group_id.to_string(), "group-b");
        assert_eq!(i64::from(request.producer_id), 43);
        assert_eq!(request.producer_epoch, 4);

        // Topics are grouped by name (BTreeMap sorts alphabetically)
        assert_eq!(request.topics.len(), 2);
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[0].partitions.len(), 2);
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
        assert_eq!(request.topics[0].partitions[0].committed_offset, 10);
        assert_eq!(request.topics[0].partitions[0].committed_leader_epoch, 5);
        assert_eq!(
            request.topics[0].partitions[0]
                .committed_metadata
                .as_ref()
                .map(ToString::to_string),
            Some("meta-a".to_owned())
        );
        assert_eq!(request.topics[0].partitions[1].partition_index, 1);
        assert_eq!(request.topics[0].partitions[1].committed_offset, 20);
        assert_eq!(request.topics[0].partitions[1].committed_leader_epoch, -1);
        assert!(request.topics[0].partitions[1].committed_metadata.is_none());

        assert_eq!(request.topics[1].name.to_string(), "topic-b");
        assert_eq!(request.topics[1].partitions.len(), 1);
        assert_eq!(request.topics[1].partitions[0].partition_index, 0);
        assert_eq!(request.topics[1].partitions[0].committed_offset, 30);
    }

    #[test]
    fn add_offsets_to_txn_response_maps_all_fields() {
        use kafka_protocol::messages::add_offsets_to_txn_response::AddOffsetsToTxnResponse as KpAddOffsetsToTxnResponse;

        let response = KpAddOffsetsToTxnResponse::default()
            .with_throttle_time_ms(100)
            .with_error_code(0);

        let converted = convert_add_offsets_to_txn_response(&response);

        assert_eq!(
            converted,
            AddOffsetsToTxnResponseData {
                throttle_time_ms: 100,
                error_code: 0,
            }
        );
    }

    #[test]
    fn txn_offset_commit_response_maps_all_fields() {
        use kafka_protocol::messages::txn_offset_commit_response::{
            TxnOffsetCommitResponsePartition as KpTxnOffsetCommitPartition,
            TxnOffsetCommitResponseTopic as KpTxnOffsetCommitTopic,
        };

        let response = TxnOffsetCommitResponse::default()
            .with_throttle_time_ms(200)
            .with_topics(vec![
                KpTxnOffsetCommitTopic::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpTxnOffsetCommitPartition::default()
                            .with_partition_index(0)
                            .with_error_code(0),
                        KpTxnOffsetCommitPartition::default()
                            .with_partition_index(1)
                            .with_error_code(15),
                    ]),
                KpTxnOffsetCommitTopic::default()
                    .with_name(StrBytes::from_static_str("topic-b").into())
                    .with_partitions(vec![
                        KpTxnOffsetCommitPartition::default()
                            .with_partition_index(0)
                            .with_error_code(0),
                    ]),
            ]);

        let converted = convert_txn_offset_commit_response(response);

        assert_eq!(converted.throttle_time_ms, 200);
        assert_eq!(converted.topics.len(), 2);
        assert_eq!(converted.topics[0].topic, "topic-a");
        assert_eq!(converted.topics[0].partitions.len(), 2);
        assert_eq!(converted.topics[0].partitions[0].partition_index, 0);
        assert_eq!(converted.topics[0].partitions[0].error_code, 0);
        assert_eq!(converted.topics[0].partitions[1].partition_index, 1);
        assert_eq!(converted.topics[0].partitions[1].error_code, 15);
        assert_eq!(converted.topics[1].topic, "topic-b");
        assert_eq!(converted.topics[1].partitions.len(), 1);
        assert_eq!(converted.topics[1].partitions[0].partition_index, 0);
        assert_eq!(converted.topics[1].partitions[0].error_code, 0);
    }
}
