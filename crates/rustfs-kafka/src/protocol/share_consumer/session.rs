//! Client-side share-consumer session helpers.

use super::{
    HeartbeatAssignment, ShareAcknowledgeOptions, ShareAcknowledgePartition, ShareAcknowledgeTopic,
    ShareAcknowledgementBatch, ShareFetchOptions, ShareFetchPartition, ShareFetchPartitionResponse,
    ShareFetchResponseData, ShareFetchTopic, ShareFetchTopicResponse, ShareGroupHeartbeatOptions,
    ShareGroupHeartbeatResponseData,
};

/// Fetch tuning used by [`ShareConsumerSession`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareFetchSessionConfig {
    /// Maximum wait in milliseconds.
    pub max_wait_ms: i32,
    /// Minimum response bytes.
    pub min_bytes: i32,
    /// Maximum response bytes.
    pub max_bytes: i32,
    /// Maximum records to fetch.
    pub max_records: i32,
    /// Optimal acquired-record/acknowledgement batch size.
    pub batch_size: i32,
}

impl Default for ShareFetchSessionConfig {
    fn default() -> Self {
        Self {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            max_records: 500,
            batch_size: 1,
        }
    }
}

/// Client-side state for composing share-consumer protocol calls.
///
/// This helper keeps the coordinator-assigned member metadata and assignment
/// needed to build follow-up `ShareGroupHeartbeat`, `ShareFetch`, and
/// `ShareAcknowledge` options. It intentionally does not perform network I/O
/// or run a background fetch/acknowledgement loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareConsumerSession {
    /// Share group ID.
    pub group_id: String,
    /// Share group member ID.
    pub member_id: String,
    /// Current member epoch.
    pub member_epoch: i32,
    /// Current share session epoch.
    pub share_session_epoch: i32,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: i32,
    /// Optional rack ID sent on heartbeat requests.
    pub rack_id: Option<String>,
    /// Topic subscriptions sent on join heartbeats.
    pub subscribed_topic_names: Vec<String>,
    /// Last assignment returned by the coordinator.
    pub assignment: Option<HeartbeatAssignment>,
    /// Fetch tuning copied into generated `ShareFetchOptions`.
    pub fetch: ShareFetchSessionConfig,
}

impl ShareConsumerSession {
    /// Create a share-consumer session.
    #[must_use]
    pub fn new(group_id: impl Into<String>, member_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            member_id: member_id.into(),
            member_epoch: 0,
            share_session_epoch: 0,
            heartbeat_interval_ms: 0,
            rack_id: None,
            subscribed_topic_names: Vec::new(),
            assignment: None,
            fetch: ShareFetchSessionConfig::default(),
        }
    }

    /// Set the rack ID to include in share-group heartbeats.
    #[must_use]
    pub fn with_rack_id(mut self, rack_id: impl Into<String>) -> Self {
        self.rack_id = Some(rack_id.into());
        self
    }

    /// Set topic subscriptions to include in share-group heartbeats.
    #[must_use]
    pub fn with_subscribed_topic_names<I, S>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.subscribed_topic_names = topics.into_iter().map(Into::into).collect();
        self
    }

    /// Set fetch tuning for generated `ShareFetchOptions`.
    #[must_use]
    pub fn with_fetch_config(mut self, fetch: ShareFetchSessionConfig) -> Self {
        self.fetch = fetch;
        self
    }

    /// Set the current share session epoch.
    pub fn set_share_session_epoch(&mut self, share_session_epoch: i32) {
        self.share_session_epoch = share_session_epoch;
    }

    /// Increment the current share session epoch.
    pub fn advance_share_session_epoch(&mut self) {
        self.share_session_epoch = self.share_session_epoch.saturating_add(1);
    }

    /// Build heartbeat options from the current session state.
    #[must_use]
    pub fn heartbeat_options(&self) -> ShareGroupHeartbeatOptions {
        ShareGroupHeartbeatOptions {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: self.member_epoch,
            rack_id: self.rack_id.clone(),
            subscribed_topic_names: (!self.subscribed_topic_names.is_empty())
                .then(|| self.subscribed_topic_names.clone()),
        }
    }

    /// Merge a successful heartbeat response into this session.
    ///
    /// Returns `true` when the response was successful and state changed to
    /// match it. Non-zero Kafka error responses do not modify the session.
    #[must_use]
    pub fn apply_heartbeat_response(&mut self, response: &ShareGroupHeartbeatResponseData) -> bool {
        if response.error_code != 0 {
            return false;
        }

        if let Some(member_id) = &response.member_id {
            self.member_id.clone_from(member_id);
        }
        self.member_epoch = response.member_epoch;
        self.heartbeat_interval_ms = response.heartbeat_interval_ms;
        if let Some(assignment) = &response.assignment {
            self.assignment = Some(assignment.clone());
        }
        true
    }

    /// Build fetch options for the current assignment.
    #[must_use]
    pub fn fetch_options(&self) -> ShareFetchOptions {
        let topics = self
            .assignment
            .as_ref()
            .map_or_else(Vec::new, |assignment| {
                assignment
                    .topic_partitions
                    .iter()
                    .map(|topic| {
                        ShareFetchTopic::new(
                            topic.topic_id,
                            topic
                                .partitions
                                .iter()
                                .copied()
                                .map(|partition| ShareFetchPartition::new(partition, [])),
                        )
                    })
                    .collect()
            });
        self.fetch_options_with_topics(topics)
    }

    /// Build fetch options using explicit topic fetch data.
    #[must_use]
    pub fn fetch_options_with_topics<I>(&self, topics: I) -> ShareFetchOptions
    where
        I: IntoIterator<Item = ShareFetchTopic>,
    {
        let mut options = ShareFetchOptions::new(self.group_id.clone(), self.member_id.clone())
            .with_topics(topics);
        options.share_session_epoch = self.share_session_epoch;
        options.max_wait_ms = self.fetch.max_wait_ms;
        options.min_bytes = self.fetch.min_bytes;
        options.max_bytes = self.fetch.max_bytes;
        options.max_records = self.fetch.max_records;
        options.batch_size = self.fetch.batch_size;
        options
    }

    /// Build acknowledgement options from the current session state.
    #[must_use]
    pub fn acknowledge_options<I, T>(&self, topics: I) -> ShareAcknowledgeOptions
    where
        I: IntoIterator<Item = T>,
        T: Into<ShareAcknowledgeTopic>,
    {
        let mut options =
            ShareAcknowledgeOptions::new(self.group_id.clone(), self.member_id.clone())
                .with_topics(topics);
        options.share_session_epoch = self.share_session_epoch;
        options
    }

    /// Build acknowledgement options for all successful acquired ranges in a
    /// `ShareFetch` response.
    ///
    /// The caller chooses the acknowledgement type because accepting,
    /// releasing, or rejecting records is application-specific.
    #[must_use]
    pub fn acknowledge_fetch_response(
        &self,
        response: &ShareFetchResponseData,
        acknowledge_type: i8,
    ) -> ShareAcknowledgeOptions {
        if response.error_code != 0 {
            return self.acknowledge_options(Vec::<ShareAcknowledgeTopic>::new());
        }

        let topics = response
            .responses
            .iter()
            .filter_map(|topic| acknowledge_topic_from_fetch(topic, acknowledge_type))
            .collect::<Vec<_>>();
        self.acknowledge_options(topics)
    }
}

fn acknowledge_topic_from_fetch(
    topic: &ShareFetchTopicResponse,
    acknowledge_type: i8,
) -> Option<ShareAcknowledgeTopic> {
    let partitions = topic
        .partitions
        .iter()
        .filter_map(|partition| acknowledge_partition_from_fetch(partition, acknowledge_type))
        .collect::<Vec<_>>();

    (!partitions.is_empty()).then(|| ShareAcknowledgeTopic::new(topic.topic_id, partitions))
}

fn acknowledge_partition_from_fetch(
    partition: &ShareFetchPartitionResponse,
    acknowledge_type: i8,
) -> Option<ShareAcknowledgePartition> {
    if partition.error_code != 0 || partition.acknowledge_error_code != 0 {
        return None;
    }

    let batches = partition
        .acquired_records
        .iter()
        .filter_map(|records| {
            let count = records
                .last_offset
                .checked_sub(records.first_offset)?
                .checked_add(1)?;
            let count = usize::try_from(count).ok()?;
            Some(ShareAcknowledgementBatch::new(
                records.first_offset,
                records.last_offset,
                std::iter::repeat_n(acknowledge_type, count),
            ))
        })
        .collect::<Vec<_>>();

    (!batches.is_empty())
        .then(|| ShareAcknowledgePartition::new(partition.partition_index, batches))
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::protocol::share_consumer::{
        HeartbeatTopicPartitions, SHARE_ACK_TYPE_ACCEPT, ShareAcknowledgePartition,
        ShareAcknowledgementBatch, ShareAcquiredRecords, ShareFetchPartitionResponse,
        ShareFetchResponseData, ShareFetchTopicResponse, ShareLeader,
    };

    #[test]
    fn share_consumer_session_builds_join_heartbeat_options() {
        let session = ShareConsumerSession::new("share-group", "member-a")
            .with_rack_id("rack-a")
            .with_subscribed_topic_names(["topic-a", "topic-b"]);

        let options = session.heartbeat_options();

        assert_eq!(options.group_id, "share-group");
        assert_eq!(options.member_id, "member-a");
        assert_eq!(options.member_epoch, 0);
        assert_eq!(options.rack_id.as_deref(), Some("rack-a"));
        assert_eq!(
            options.subscribed_topic_names,
            Some(vec!["topic-a".to_owned(), "topic-b".to_owned()])
        );
    }

    #[test]
    fn share_consumer_session_applies_successful_heartbeat() {
        let topic_id = Uuid::from_u128(8);
        let mut session = ShareConsumerSession::new("share-group", "pending-member");
        let response = ShareGroupHeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Some("member-a".to_owned()),
            member_epoch: 4,
            heartbeat_interval_ms: 3_000,
            assignment: Some(HeartbeatAssignment {
                topic_partitions: vec![HeartbeatTopicPartitions::new(topic_id, [0, 1])],
            }),
        };

        assert!(session.apply_heartbeat_response(&response));

        assert_eq!(session.member_id, "member-a");
        assert_eq!(session.member_epoch, 4);
        assert_eq!(session.heartbeat_interval_ms, 3_000);
        assert_eq!(
            session.assignment.unwrap().topic_partitions,
            vec![HeartbeatTopicPartitions::new(topic_id, [0, 1])]
        );
    }

    #[test]
    fn share_consumer_session_ignores_failed_heartbeat() {
        let mut session = ShareConsumerSession::new("share-group", "member-a");
        let response = ShareGroupHeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: 15,
            error_message: Some("failed".to_owned()),
            member_id: Some("member-b".to_owned()),
            member_epoch: 99,
            heartbeat_interval_ms: 10,
            assignment: None,
        };

        assert!(!session.apply_heartbeat_response(&response));

        assert_eq!(session.member_id, "member-a");
        assert_eq!(session.member_epoch, 0);
        assert_eq!(session.heartbeat_interval_ms, 0);
    }

    #[test]
    fn share_consumer_session_builds_fetch_options_from_assignment() {
        let topic_id = Uuid::from_u128(9);
        let mut session = ShareConsumerSession::new("share-group", "member-a").with_fetch_config(
            ShareFetchSessionConfig {
                max_wait_ms: 1_000,
                min_bytes: 2,
                max_bytes: 4_096,
                max_records: 20,
                batch_size: 5,
            },
        );
        session.member_epoch = 3;
        session.set_share_session_epoch(7);
        session.assignment = Some(HeartbeatAssignment {
            topic_partitions: vec![HeartbeatTopicPartitions::new(topic_id, [2, 4])],
        });

        let options = session.fetch_options();

        assert_eq!(options.group_id.as_deref(), Some("share-group"));
        assert_eq!(options.member_id.as_deref(), Some("member-a"));
        assert_eq!(options.share_session_epoch, 7);
        assert_eq!(options.max_wait_ms, 1_000);
        assert_eq!(options.min_bytes, 2);
        assert_eq!(options.max_bytes, 4_096);
        assert_eq!(options.max_records, 20);
        assert_eq!(options.batch_size, 5);
        assert_eq!(options.topics[0].topic_id, topic_id);
        assert_eq!(options.topics[0].partitions[0].partition_index, 2);
        assert_eq!(options.topics[0].partitions[1].partition_index, 4);
    }

    #[test]
    fn share_consumer_session_builds_acknowledge_options() {
        let topic_id = Uuid::from_u128(10);
        let mut session = ShareConsumerSession::new("share-group", "member-a");
        session.advance_share_session_epoch();

        let options = session.acknowledge_options([ShareAcknowledgeTopic::new(
            topic_id,
            [ShareAcknowledgePartition::new(
                0,
                [ShareAcknowledgementBatch::new(
                    15,
                    16,
                    [SHARE_ACK_TYPE_ACCEPT, SHARE_ACK_TYPE_ACCEPT],
                )],
            )],
        )]);

        assert_eq!(options.group_id.as_deref(), Some("share-group"));
        assert_eq!(options.member_id.as_deref(), Some("member-a"));
        assert_eq!(options.share_session_epoch, 1);
        assert_eq!(options.topics[0].topic_id, topic_id);
        assert_eq!(
            options.topics[0].partitions[0].acknowledgement_batches[0].acknowledge_types,
            vec![SHARE_ACK_TYPE_ACCEPT, SHARE_ACK_TYPE_ACCEPT]
        );
    }

    #[test]
    fn share_consumer_session_acknowledges_successful_fetch_ranges() {
        let topic_id = Uuid::from_u128(11);
        let mut session = ShareConsumerSession::new("share-group", "member-a");
        session.set_share_session_epoch(3);
        let response = ShareFetchResponseData {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            acquisition_lock_timeout_ms: 1_000,
            responses: vec![ShareFetchTopicResponse {
                topic_id,
                partitions: vec![
                    ShareFetchPartitionResponse {
                        partition_index: 0,
                        error_code: 0,
                        error_message: None,
                        acknowledge_error_code: 0,
                        acknowledge_error_message: None,
                        current_leader: ShareLeader {
                            leader_id: 1,
                            leader_epoch: 2,
                        },
                        records: None,
                        acquired_records: vec![
                            ShareAcquiredRecords {
                                first_offset: 10,
                                last_offset: 12,
                                delivery_count: 1,
                            },
                            ShareAcquiredRecords {
                                first_offset: 20,
                                last_offset: 20,
                                delivery_count: 2,
                            },
                        ],
                    },
                    ShareFetchPartitionResponse {
                        partition_index: 1,
                        error_code: 3,
                        error_message: Some("missing".to_owned()),
                        acknowledge_error_code: 0,
                        acknowledge_error_message: None,
                        current_leader: ShareLeader {
                            leader_id: 1,
                            leader_epoch: 2,
                        },
                        records: None,
                        acquired_records: vec![ShareAcquiredRecords {
                            first_offset: 99,
                            last_offset: 99,
                            delivery_count: 1,
                        }],
                    },
                ],
            }],
            node_endpoints: Vec::new(),
        };

        let options = session.acknowledge_fetch_response(&response, SHARE_ACK_TYPE_ACCEPT);

        assert_eq!(options.group_id.as_deref(), Some("share-group"));
        assert_eq!(options.member_id.as_deref(), Some("member-a"));
        assert_eq!(options.share_session_epoch, 3);
        assert_eq!(options.topics.len(), 1);
        assert_eq!(options.topics[0].topic_id, topic_id);
        assert_eq!(options.topics[0].partitions.len(), 1);
        assert_eq!(options.topics[0].partitions[0].partition_index, 0);
        assert_eq!(
            options.topics[0].partitions[0].acknowledgement_batches[0].acknowledge_types,
            vec![
                SHARE_ACK_TYPE_ACCEPT,
                SHARE_ACK_TYPE_ACCEPT,
                SHARE_ACK_TYPE_ACCEPT
            ]
        );
        assert_eq!(
            options.topics[0].partitions[0].acknowledgement_batches[1].acknowledge_types,
            vec![SHARE_ACK_TYPE_ACCEPT]
        );
    }

    #[test]
    fn share_consumer_session_does_not_ack_failed_fetch_response() {
        let session = ShareConsumerSession::new("share-group", "member-a");
        let response = ShareFetchResponseData {
            throttle_time_ms: 0,
            error_code: 15,
            error_message: Some("coordinator unavailable".to_owned()),
            acquisition_lock_timeout_ms: 0,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
        };

        let options = session.acknowledge_fetch_response(&response, SHARE_ACK_TYPE_ACCEPT);

        assert!(options.topics.is_empty());
    }
}
