#![allow(clippy::wildcard_imports)]
//! Consumer group administration helpers.

use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, DeleteGroupsRequest,
    DeleteGroupsResponse, DescribeGroupsRequest, DescribeGroupsResponse, ListGroupsRequest,
    ListGroupsResponse, RequestHeader, ShareGroupDescribeRequest, ShareGroupDescribeResponse,
};

use super::super::{
    API_VERSION_CONSUMER_GROUP_DESCRIBE, API_VERSION_DELETE_GROUPS, API_VERSION_DESCRIBE_GROUPS,
    API_VERSION_LIST_GROUPS, API_VERSION_SHARE_GROUP_DESCRIBE,
};
use super::*;

/// A group returned by `ListGroups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroup {
    /// Group ID.
    pub group_id: String,
    /// Group protocol type, for example `consumer`.
    pub protocol_type: String,
    /// Group state name when returned by the broker.
    pub group_state: String,
    /// Group type name when returned by the broker.
    pub group_type: String,
}

/// Parsed response from a `ListGroups` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Groups returned by the broker.
    pub groups: Vec<ListedGroup>,
}

/// Result of one group deletion returned by `DeleteGroups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedGroup {
    /// Group ID.
    pub group_id: String,
    /// Broker error code for this group deletion.
    pub error_code: i16,
}

/// Parsed response from a `DeleteGroups` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteGroupsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-group deletion results returned by the broker.
    pub results: Vec<DeletedGroup>,
}

/// A member returned by `DescribeGroups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroupMember {
    /// Member ID assigned by the group coordinator.
    pub member_id: String,
    /// Static membership instance ID, when configured.
    pub group_instance_id: Option<String>,
    /// Client ID reported by the member.
    pub client_id: String,
    /// Client host reported by the broker.
    pub client_host: String,
    /// Opaque protocol metadata for the member.
    pub member_metadata: Bytes,
    /// Opaque assignment payload for the member.
    pub member_assignment: Bytes,
}

/// A group returned by `DescribeGroups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroup {
    /// Per-group broker error code.
    pub error_code: i16,
    /// Optional per-group broker error message.
    pub error_message: Option<String>,
    /// Group ID.
    pub group_id: String,
    /// Group state name.
    pub group_state: String,
    /// Group protocol type.
    pub protocol_type: String,
    /// Active protocol name/data selected by the group.
    pub protocol_data: String,
    /// Members in the group.
    pub members: Vec<DescribedGroupMember>,
    /// Authorized operations bitfield, or Kafka's sentinel when not requested.
    pub authorized_operations: i32,
}

/// Parsed response from a `DescribeGroups` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeGroupsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Described groups returned by the broker.
    pub groups: Vec<DescribedGroup>,
}

/// Topic partitions in a modern consumer group assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupTopicPartitions {
    /// Topic UUID as a string.
    pub topic_id: String,
    /// Topic name.
    pub topic_name: String,
    /// Assigned partition indexes.
    pub partitions: Vec<i32>,
}

/// Assignment returned by `ConsumerGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupAssignment {
    /// Topic partitions in the assignment.
    pub topic_partitions: Vec<ConsumerGroupTopicPartitions>,
}

/// Member state returned by `ConsumerGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMemberDescription {
    /// Member ID assigned by the group coordinator.
    pub member_id: String,
    /// Static membership instance ID, when configured.
    pub instance_id: Option<String>,
    /// Rack ID reported by the member, when configured.
    pub rack_id: Option<String>,
    /// Current member epoch.
    pub member_epoch: i32,
    /// Client ID reported by the member.
    pub client_id: String,
    /// Client host reported by the broker.
    pub client_host: String,
    /// Subscribed topic names.
    pub subscribed_topic_names: Vec<String>,
    /// Subscribed topic regex, when provided.
    pub subscribed_topic_regex: Option<String>,
    /// Current assignment.
    pub assignment: ConsumerGroupAssignment,
    /// Target assignment during rebalancing.
    pub target_assignment: ConsumerGroupAssignment,
    /// Kafka member type code, or `-1` for unknown on older response versions.
    pub member_type: i8,
}

/// Consumer group state returned by `ConsumerGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescription {
    /// Per-group broker error code.
    pub error_code: i16,
    /// Optional per-group broker error message.
    pub error_message: Option<String>,
    /// Group ID.
    pub group_id: String,
    /// Current group state.
    pub group_state: String,
    /// Current group epoch.
    pub group_epoch: i32,
    /// Current assignment epoch.
    pub assignment_epoch: i32,
    /// Selected assignor name.
    pub assignor_name: String,
    /// Members in the group.
    pub members: Vec<ConsumerGroupMemberDescription>,
    /// Authorized operations bitfield, or Kafka's sentinel when not requested.
    pub authorized_operations: i32,
}

/// Parsed response from a `ConsumerGroupDescribe` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescribeResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Described consumer groups returned by the broker.
    pub groups: Vec<ConsumerGroupDescription>,
}

pub fn build_list_groups_request(
    correlation_id: i32,
    client_id: &str,
    states_filter: &[&str],
    types_filter: &[&str],
) -> (RequestHeader, ListGroupsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ListGroups,
        API_VERSION_LIST_GROUPS,
    );
    let request = ListGroupsRequest::default()
        .with_states_filter(str_bytes_vec(states_filter))
        .with_types_filter(str_bytes_vec(types_filter));

    (header, request)
}

/// Build a `DeleteGroups` request.
pub fn build_delete_groups_request(
    correlation_id: i32,
    client_id: &str,
    groups: &[&str],
) -> (RequestHeader, DeleteGroupsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DeleteGroups,
        API_VERSION_DELETE_GROUPS,
    );
    let request = DeleteGroupsRequest::default()
        .with_groups_names(groups.iter().map(|group| group_id(group)).collect());

    (header, request)
}

/// Build a `DescribeGroups` request.
pub fn build_describe_groups_request(
    correlation_id: i32,
    client_id: &str,
    groups: &[&str],
    include_authorized_operations: bool,
) -> (RequestHeader, DescribeGroupsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeGroups,
        API_VERSION_DESCRIBE_GROUPS,
    );
    let request = DescribeGroupsRequest::default()
        .with_groups(groups.iter().map(|g| group_id(g)).collect())
        .with_include_authorized_operations(include_authorized_operations);

    (header, request)
}

/// Build a `DescribeAcls` request.
pub fn build_consumer_group_describe_request(
    correlation_id: i32,
    client_id: &str,
    groups: &[&str],
    include_authorized_operations: bool,
) -> (RequestHeader, ConsumerGroupDescribeRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ConsumerGroupDescribe,
        API_VERSION_CONSUMER_GROUP_DESCRIBE,
    );
    let request = ConsumerGroupDescribeRequest::default()
        .with_group_ids(groups.iter().map(|g| group_id(g)).collect())
        .with_include_authorized_operations(include_authorized_operations);

    (header, request)
}

/// Build a `ShareGroupDescribe` request.
pub fn build_share_group_describe_request(
    correlation_id: i32,
    client_id: &str,
    groups: &[&str],
    include_authorized_operations: bool,
) -> (RequestHeader, ShareGroupDescribeRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ShareGroupDescribe,
        API_VERSION_SHARE_GROUP_DESCRIBE,
    );
    let request = ShareGroupDescribeRequest::default()
        .with_group_ids(groups.iter().map(|g| group_id(g)).collect())
        .with_include_authorized_operations(include_authorized_operations);

    (header, request)
}

/// Build a `ListConfigResources` request.
pub fn convert_list_groups_response(response: ListGroupsResponse) -> ListGroupsResponseData {
    ListGroupsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        groups: response
            .groups
            .into_iter()
            .map(|group| ListedGroup {
                group_id: group.group_id.to_string(),
                protocol_type: group.protocol_type.to_string(),
                group_state: group.group_state.to_string(),
                group_type: group.group_type.to_string(),
            })
            .collect(),
    }
}

/// Convert a generated `DeleteGroupsResponse` into the crate's public shape.
pub fn convert_delete_groups_response(response: DeleteGroupsResponse) -> DeleteGroupsResponseData {
    DeleteGroupsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        results: response
            .results
            .into_iter()
            .map(|result| DeletedGroup {
                group_id: result.group_id.to_string(),
                error_code: result.error_code,
            })
            .collect(),
    }
}

/// Convert a generated `DescribeGroupsResponse` into the crate's public shape.
pub fn convert_describe_groups_response(
    response: DescribeGroupsResponse,
) -> DescribeGroupsResponseData {
    DescribeGroupsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        groups: response
            .groups
            .into_iter()
            .map(|group| DescribedGroup {
                error_code: group.error_code,
                error_message: group.error_message.map(|message| message.to_string()),
                group_id: group.group_id.to_string(),
                group_state: group.group_state.to_string(),
                protocol_type: group.protocol_type.to_string(),
                protocol_data: group.protocol_data.to_string(),
                members: group
                    .members
                    .into_iter()
                    .map(|member| DescribedGroupMember {
                        member_id: member.member_id.to_string(),
                        group_instance_id: member
                            .group_instance_id
                            .map(|instance_id| instance_id.to_string()),
                        client_id: member.client_id.to_string(),
                        client_host: member.client_host.to_string(),
                        member_metadata: member.member_metadata,
                        member_assignment: member.member_assignment,
                    })
                    .collect(),
                authorized_operations: group.authorized_operations,
            })
            .collect(),
    }
}

/// Convert a generated `DescribeAclsResponse` into the crate's public shape.
pub fn convert_consumer_group_describe_response(
    response: ConsumerGroupDescribeResponse,
) -> ConsumerGroupDescribeResponseData {
    ConsumerGroupDescribeResponseData {
        throttle_time_ms: response.throttle_time_ms,
        groups: response
            .groups
            .into_iter()
            .map(|group| ConsumerGroupDescription {
                error_code: group.error_code,
                error_message: group.error_message.map(|message| message.to_string()),
                group_id: group.group_id.to_string(),
                group_state: group.group_state.to_string(),
                group_epoch: group.group_epoch,
                assignment_epoch: group.assignment_epoch,
                assignor_name: group.assignor_name.to_string(),
                members: group
                    .members
                    .into_iter()
                    .map(|member| ConsumerGroupMemberDescription {
                        member_id: member.member_id.to_string(),
                        instance_id: member
                            .instance_id
                            .map(|instance_id| instance_id.to_string()),
                        rack_id: member.rack_id.map(|rack_id| rack_id.to_string()),
                        member_epoch: member.member_epoch,
                        client_id: member.client_id.to_string(),
                        client_host: member.client_host.to_string(),
                        subscribed_topic_names: member
                            .subscribed_topic_names
                            .into_iter()
                            .map(|topic_name| topic_name.to_string())
                            .collect(),
                        subscribed_topic_regex: member
                            .subscribed_topic_regex
                            .map(|regex| regex.to_string()),
                        assignment: convert_consumer_group_assignment(member.assignment),
                        target_assignment: convert_consumer_group_assignment(
                            member.target_assignment,
                        ),
                        member_type: member.member_type,
                    })
                    .collect(),
                authorized_operations: group.authorized_operations,
            })
            .collect(),
    }
}

fn convert_consumer_group_assignment(
    assignment: kafka_protocol::messages::consumer_group_describe_response::Assignment,
) -> ConsumerGroupAssignment {
    ConsumerGroupAssignment {
        topic_partitions: assignment
            .topic_partitions
            .into_iter()
            .map(|topic| ConsumerGroupTopicPartitions {
                topic_id: topic.topic_id.to_string(),
                topic_name: topic.topic_name.to_string(),
                partitions: topic.partitions,
            })
            .collect(),
    }
}

/// Convert a generated `ShareGroupDescribeResponse` into the crate's public shape.
pub fn convert_share_group_describe_response(
    response: ShareGroupDescribeResponse,
) -> ShareGroupDescribeResponseData {
    ShareGroupDescribeResponseData {
        throttle_time_ms: response.throttle_time_ms,
        groups: response
            .groups
            .into_iter()
            .map(|group| ShareGroupDescription {
                error_code: group.error_code,
                error_message: group.error_message.map(|message| message.to_string()),
                group_id: group.group_id.to_string(),
                group_state: group.group_state.to_string(),
                group_epoch: group.group_epoch,
                assignment_epoch: group.assignment_epoch,
                assignor_name: group.assignor_name.to_string(),
                members: group
                    .members
                    .into_iter()
                    .map(|member| ShareGroupMemberDescription {
                        member_id: member.member_id.to_string(),
                        rack_id: member.rack_id.map(|rack_id| rack_id.to_string()),
                        member_epoch: member.member_epoch,
                        client_id: member.client_id.to_string(),
                        client_host: member.client_host.to_string(),
                        subscribed_topic_names: member
                            .subscribed_topic_names
                            .into_iter()
                            .map(|topic_name| topic_name.to_string())
                            .collect(),
                        assignment: convert_share_group_assignment(member.assignment),
                    })
                    .collect(),
                authorized_operations: group.authorized_operations,
            })
            .collect(),
    }
}

fn convert_share_group_assignment(
    assignment: kafka_protocol::messages::share_group_describe_response::Assignment,
) -> ShareGroupAssignment {
    ShareGroupAssignment {
        topic_partitions: assignment
            .topic_partitions
            .into_iter()
            .map(|topic| ShareGroupTopicPartitions {
                topic_id: topic.topic_id.to_string(),
                topic_name: topic.topic_name.to_string(),
                partitions: topic.partitions,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::messages::consumer_group_describe_response::{
        Assignment as KpConsumerGroupAssignment, DescribedGroup as KpConsumerGroupDescription,
        Member as KpConsumerGroupMember, TopicPartitions as KpConsumerGroupTopicPartitions,
    };
    use kafka_protocol::messages::delete_groups_response::DeletableGroupResult as KpDeletableGroupResult;
    use kafka_protocol::messages::describe_groups_response::{
        DescribedGroup as KpDescribedGroup, DescribedGroupMember as KpDescribedGroupMember,
    };
    use kafka_protocol::messages::list_groups_response::ListedGroup as KpListedGroup;
    use kafka_protocol::messages::share_group_describe_response::{
        Assignment as KpShareGroupAssignment, DescribedGroup as KpShareGroupDescription,
        Member as KpShareGroupMember, TopicPartitions as KpShareGroupTopicPartitions,
    };
    use kafka_protocol::protocol::StrBytes;

    #[test]
    fn list_groups_request_includes_state_and_type_filters() {
        let (header, request) =
            build_list_groups_request(7, "client-b", &["Stable"], &["consumer"]);

        assert_eq!(header.request_api_key, ApiKey::ListGroups as i16);
        assert_eq!(header.request_api_version, API_VERSION_LIST_GROUPS);
        assert_eq!(
            request.states_filter,
            vec![StrBytes::from_static_str("Stable")]
        );
        assert_eq!(
            request.types_filter,
            vec![StrBytes::from_static_str("consumer")]
        );
    }

    #[test]
    fn delete_groups_request_includes_group_ids() {
        let (header, request) = build_delete_groups_request(8, "client-c", &["group-a", "group-b"]);

        assert_eq!(header.request_api_key, ApiKey::DeleteGroups as i16);
        assert_eq!(header.request_api_version, API_VERSION_DELETE_GROUPS);
        assert_eq!(request.groups_names[0].to_string(), "group-a");
        assert_eq!(request.groups_names[1].to_string(), "group-b");
    }

    #[test]
    fn describe_groups_request_includes_authorized_operations_flag() {
        let (header, request) =
            build_describe_groups_request(9, "client-c", &["group-a", "group-b"], true);

        assert_eq!(header.request_api_key, ApiKey::DescribeGroups as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_GROUPS);
        assert!(request.include_authorized_operations);
        assert_eq!(request.groups[0].to_string(), "group-a");
        assert_eq!(request.groups[1].to_string(), "group-b");
    }
    #[test]
    fn consumer_group_describe_request_includes_authorized_operations_flag() {
        let (header, request) =
            build_consumer_group_describe_request(18, "client-m", &["group-a"], true);

        assert_eq!(header.request_api_key, ApiKey::ConsumerGroupDescribe as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_CONSUMER_GROUP_DESCRIBE
        );
        assert!(request.include_authorized_operations);
        assert_eq!(request.group_ids[0].to_string(), "group-a");
    }

    #[test]
    fn share_group_describe_request_includes_authorized_operations_flag() {
        let (header, request) =
            build_share_group_describe_request(22, "client-n", &["share-a"], true);

        assert_eq!(header.request_api_key, ApiKey::ShareGroupDescribe as i16);
        assert_eq!(header.request_api_version, API_VERSION_SHARE_GROUP_DESCRIBE);
        assert!(request.include_authorized_operations);
        assert_eq!(request.group_ids[0].to_string(), "share-a");
    }
    #[test]
    fn convert_list_groups_response_preserves_state_and_type() {
        let response = ListGroupsResponse::default()
            .with_throttle_time_ms(11)
            .with_error_code(0)
            .with_groups(vec![
                KpListedGroup::default()
                    .with_group_id(group_id("group-a"))
                    .with_protocol_type(StrBytes::from_static_str("consumer"))
                    .with_group_state(StrBytes::from_static_str("Stable"))
                    .with_group_type(StrBytes::from_static_str("classic")),
            ]);

        let converted = convert_list_groups_response(response);

        assert_eq!(
            converted,
            ListGroupsResponseData {
                throttle_time_ms: 11,
                error_code: 0,
                groups: vec![ListedGroup {
                    group_id: "group-a".to_owned(),
                    protocol_type: "consumer".to_owned(),
                    group_state: "Stable".to_owned(),
                    group_type: "classic".to_owned(),
                }],
            }
        );
    }

    #[test]
    fn convert_delete_groups_response_preserves_results() {
        let response = DeleteGroupsResponse::default()
            .with_throttle_time_ms(12)
            .with_results(vec![
                KpDeletableGroupResult::default()
                    .with_group_id(group_id("group-a"))
                    .with_error_code(0),
                KpDeletableGroupResult::default()
                    .with_group_id(group_id("group-b"))
                    .with_error_code(15),
            ]);

        let converted = convert_delete_groups_response(response);

        assert_eq!(
            converted,
            DeleteGroupsResponseData {
                throttle_time_ms: 12,
                results: vec![
                    DeletedGroup {
                        group_id: "group-a".to_owned(),
                        error_code: 0,
                    },
                    DeletedGroup {
                        group_id: "group-b".to_owned(),
                        error_code: 15,
                    },
                ],
            }
        );
    }

    #[test]
    fn convert_describe_groups_response_preserves_members_and_authorizations() {
        let response = DescribeGroupsResponse::default()
            .with_throttle_time_ms(12)
            .with_groups(vec![
                KpDescribedGroup::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_group_id(group_id("group-a"))
                    .with_group_state(StrBytes::from_static_str("Stable"))
                    .with_protocol_type(StrBytes::from_static_str("consumer"))
                    .with_protocol_data(StrBytes::from_static_str("range"))
                    .with_members(vec![
                        KpDescribedGroupMember::default()
                            .with_member_id(StrBytes::from_static_str("member-a"))
                            .with_group_instance_id(Some(StrBytes::from_static_str("instance-a")))
                            .with_client_id(StrBytes::from_static_str("client-a"))
                            .with_client_host(StrBytes::from_static_str("/127.0.0.1"))
                            .with_member_metadata(Bytes::from_static(b"metadata"))
                            .with_member_assignment(Bytes::from_static(b"assignment")),
                    ])
                    .with_authorized_operations(456),
            ]);

        let converted = convert_describe_groups_response(response);

        assert_eq!(converted.throttle_time_ms, 12);
        assert_eq!(converted.groups.len(), 1);
        assert_eq!(converted.groups[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.groups[0].group_id, "group-a");
        assert_eq!(converted.groups[0].authorized_operations, 456);
        assert_eq!(converted.groups[0].members[0].member_id, "member-a");
        assert_eq!(
            converted.groups[0].members[0].group_instance_id,
            Some("instance-a".to_owned())
        );
        assert_eq!(
            converted.groups[0].members[0].member_metadata,
            Bytes::from_static(b"metadata")
        );
    }
    #[test]
    fn convert_consumer_group_describe_response_preserves_assignments() {
        let assignment = KpConsumerGroupAssignment::default().with_topic_partitions(vec![
            KpConsumerGroupTopicPartitions::default()
                .with_topic_name(StrBytes::from_static_str("topic-a").into())
                .with_partitions(vec![0, 2]),
        ]);
        let response = ConsumerGroupDescribeResponse::default()
            .with_throttle_time_ms(24)
            .with_groups(vec![
                KpConsumerGroupDescription::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_group_id(group_id("group-a"))
                    .with_group_state(StrBytes::from_static_str("Stable"))
                    .with_group_epoch(7)
                    .with_assignment_epoch(8)
                    .with_assignor_name(StrBytes::from_static_str("range"))
                    .with_members(vec![
                        KpConsumerGroupMember::default()
                            .with_member_id(StrBytes::from_static_str("member-a"))
                            .with_instance_id(Some(StrBytes::from_static_str("instance-a")))
                            .with_rack_id(Some(StrBytes::from_static_str("rack-a")))
                            .with_member_epoch(9)
                            .with_client_id(StrBytes::from_static_str("client-a"))
                            .with_client_host(StrBytes::from_static_str("/127.0.0.1"))
                            .with_subscribed_topic_names(vec![
                                StrBytes::from_static_str("topic-a").into(),
                            ])
                            .with_subscribed_topic_regex(Some(StrBytes::from_static_str(
                                "topic-.*",
                            )))
                            .with_assignment(assignment.clone())
                            .with_target_assignment(assignment)
                            .with_member_type(1),
                    ])
                    .with_authorized_operations(321),
            ]);

        let converted = convert_consumer_group_describe_response(response);

        assert_eq!(converted.throttle_time_ms, 24);
        assert_eq!(converted.groups[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.groups[0].group_id, "group-a");
        assert_eq!(converted.groups[0].group_epoch, 7);
        assert_eq!(converted.groups[0].assignment_epoch, 8);
        assert_eq!(converted.groups[0].assignor_name, "range");
        assert_eq!(converted.groups[0].authorized_operations, 321);
        let member = &converted.groups[0].members[0];
        assert_eq!(member.member_id, "member-a");
        assert_eq!(member.instance_id, Some("instance-a".to_owned()));
        assert_eq!(member.rack_id, Some("rack-a".to_owned()));
        assert_eq!(member.member_epoch, 9);
        assert_eq!(member.subscribed_topic_names, vec!["topic-a"]);
        assert_eq!(member.subscribed_topic_regex, Some("topic-.*".to_owned()));
        assert_eq!(member.member_type, 1);
        assert_eq!(member.assignment.topic_partitions[0].topic_name, "topic-a");
        assert_eq!(member.assignment.topic_partitions[0].partitions, vec![0, 2]);
        assert_eq!(
            member.assignment.topic_partitions[0].topic_id,
            "00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn convert_share_group_describe_response_preserves_assignments() {
        let assignment = KpShareGroupAssignment::default().with_topic_partitions(vec![
            KpShareGroupTopicPartitions::default()
                .with_topic_name(StrBytes::from_static_str("topic-a").into())
                .with_partitions(vec![1, 3]),
        ]);
        let response = ShareGroupDescribeResponse::default()
            .with_throttle_time_ms(26)
            .with_groups(vec![
                KpShareGroupDescription::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_group_id(group_id("share-a"))
                    .with_group_state(StrBytes::from_static_str("Stable"))
                    .with_group_epoch(4)
                    .with_assignment_epoch(5)
                    .with_assignor_name(StrBytes::from_static_str("share"))
                    .with_members(vec![
                        KpShareGroupMember::default()
                            .with_member_id(StrBytes::from_static_str("member-a"))
                            .with_rack_id(Some(StrBytes::from_static_str("rack-a")))
                            .with_member_epoch(6)
                            .with_client_id(StrBytes::from_static_str("client-a"))
                            .with_client_host(StrBytes::from_static_str("/127.0.0.1"))
                            .with_subscribed_topic_names(vec![
                                StrBytes::from_static_str("topic-a").into(),
                            ])
                            .with_assignment(assignment),
                    ])
                    .with_authorized_operations(777),
            ]);

        let converted = convert_share_group_describe_response(response);

        assert_eq!(converted.throttle_time_ms, 26);
        assert_eq!(converted.groups[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.groups[0].group_id, "share-a");
        assert_eq!(converted.groups[0].group_state, "Stable");
        assert_eq!(converted.groups[0].group_epoch, 4);
        assert_eq!(converted.groups[0].assignment_epoch, 5);
        assert_eq!(converted.groups[0].assignor_name, "share");
        assert_eq!(converted.groups[0].authorized_operations, 777);
        let member = &converted.groups[0].members[0];
        assert_eq!(member.member_id, "member-a");
        assert_eq!(member.rack_id, Some("rack-a".to_owned()));
        assert_eq!(member.member_epoch, 6);
        assert_eq!(member.client_id, "client-a");
        assert_eq!(member.client_host, "/127.0.0.1");
        assert_eq!(member.subscribed_topic_names, vec!["topic-a"]);
        assert_eq!(member.assignment.topic_partitions[0].topic_name, "topic-a");
        assert_eq!(member.assignment.topic_partitions[0].partitions, vec![1, 3]);
        assert_eq!(
            member.assignment.topic_partitions[0].topic_id,
            "00000000-0000-0000-0000-000000000000"
        );
    }
}
