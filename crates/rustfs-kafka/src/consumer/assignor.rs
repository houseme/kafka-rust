//! Partition assignment strategies for consumer groups.
//!
//! Provides traits and implementations for partition assignment algorithms
//! used during consumer group rebalancing.

use std::collections::HashMap;

use crate::protocol::group::{GroupAssignment, MemberAssignment, TopicAssignment};

/// Trait for providing topic partition information to assignors.
pub trait TopicPartitions {
    /// Returns the list of topics with available partitions.
    fn topics(&self) -> Vec<&str>;

    /// Returns the partition IDs for a given topic.
    fn partitions_for(&self, topic: &str) -> Vec<i32>;
}

/// Trait for partition assignment strategies.
///
/// Implementations distribute topic partitions among consumer group members.
pub trait PartitionAssignor: Send + Sync {
    /// Returns the name of this assignor (e.g., "range", "roundrobin").
    fn name(&self) -> &str;

    /// Assign partitions to group members.
    fn assign(
        &self,
        members: &[&str],
        member_subscriptions: &[(String, Vec<String>)],
        topic_partitions: &dyn TopicPartitions,
    ) -> Vec<GroupAssignment>;
}

/// Range assignor: assigns consecutive partition ranges to each consumer.
#[derive(Debug, Default)]
pub struct RangeAssignor;

impl PartitionAssignor for RangeAssignor {
    fn name(&self) -> &'static str {
        "range"
    }

    fn assign(
        &self,
        members: &[&str],
        member_subscriptions: &[(String, Vec<String>)],
        topic_partitions: &dyn TopicPartitions,
    ) -> Vec<GroupAssignment> {
        if members.is_empty() {
            return Vec::new();
        }

        let sub_map: HashMap<&str, &[String]> = member_subscriptions
            .iter()
            .map(|(mid, topics)| (mid.as_str(), topics.as_slice()))
            .collect();

        let mut assignments: HashMap<&str, Vec<TopicAssignment>> =
            members.iter().map(|&m| (m, Vec::new())).collect();

        for topic in topic_partitions.topics() {
            let partitions = topic_partitions.partitions_for(topic);
            if partitions.is_empty() {
                continue;
            }

            let subscribed_members: Vec<&str> = members
                .iter()
                .filter(|&&m| {
                    sub_map
                        .get(m)
                        .is_some_and(|topics| topics.iter().any(|t| t == topic))
                })
                .copied()
                .collect();

            if subscribed_members.is_empty() {
                continue;
            }

            let num_subscribers = subscribed_members.len();
            let partitions_per_member = partitions.len() / num_subscribers;
            let extra = partitions.len() % num_subscribers;

            let mut partition_idx = 0;
            for (i, &member) in subscribed_members.iter().enumerate() {
                let count = partitions_per_member + usize::from(i < extra);
                if count == 0 {
                    continue;
                }
                let assigned: Vec<i32> = (partition_idx..partition_idx + count)
                    .map(|idx| partitions[idx])
                    .collect();
                partition_idx += count;

                if let Some(assignments) = assignments.get_mut(member) {
                    assignments.push(TopicAssignment {
                        topic: topic.to_owned(),
                        partitions: assigned,
                    });
                }
            }
        }

        members
            .iter()
            .map(|&member| {
                let member_assignment = MemberAssignment {
                    version: 2,
                    topic_partitions: assignments.remove(member).unwrap_or_default(),
                    user_data: None,
                };
                GroupAssignment {
                    member_id: member.to_owned(),
                    group_instance_id: None,
                    assignment: member_assignment.to_bytes(),
                }
            })
            .collect()
    }
}

/// `RoundRobin` assignor: assigns partitions in round-robin fashion.
#[derive(Debug, Default)]
pub struct RoundRobinAssignor;

impl PartitionAssignor for RoundRobinAssignor {
    fn name(&self) -> &'static str {
        "roundrobin"
    }

    fn assign(
        &self,
        members: &[&str],
        member_subscriptions: &[(String, Vec<String>)],
        topic_partitions: &dyn TopicPartitions,
    ) -> Vec<GroupAssignment> {
        if members.is_empty() {
            return Vec::new();
        }

        let sub_map: HashMap<&str, &[String]> = member_subscriptions
            .iter()
            .map(|(mid, topics)| (mid.as_str(), topics.as_slice()))
            .collect();

        let mut all_partitions: Vec<(String, i32)> = Vec::new();
        for topic in topic_partitions.topics() {
            let partitions = topic_partitions.partitions_for(topic);
            if partitions.is_empty() {
                continue;
            }
            let has_subscriber = members.iter().any(|m| {
                sub_map
                    .get(m)
                    .is_some_and(|topics| topics.iter().any(|t| t == topic))
            });
            if !has_subscriber {
                continue;
            }
            for &p in &partitions {
                all_partitions.push((topic.to_owned(), p));
            }
        }

        all_partitions.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let mut member_topics: HashMap<&str, HashMap<String, Vec<i32>>> =
            members.iter().map(|&m| (m, HashMap::new())).collect();

        let num_members = members.len();
        for (i, (topic, partition)) in all_partitions.into_iter().enumerate() {
            let member_idx = i % num_members;
            let member = members[member_idx];

            let is_subscribed = sub_map
                .get(member)
                .is_some_and(|topics| topics.iter().any(|t| t.as_str() == topic));

            if is_subscribed {
                if let Some(topics) = member_topics.get_mut(member) {
                    topics.entry(topic).or_default().push(partition);
                }
            } else {
                for offset in 1..num_members {
                    let alt_idx = (member_idx + offset) % num_members;
                    let alt_member = members[alt_idx];
                    let alt_subscribed = sub_map
                        .get(alt_member)
                        .is_some_and(|topics| topics.iter().any(|t| t.as_str() == topic));
                    if alt_subscribed {
                        if let Some(topics) = member_topics.get_mut(alt_member) {
                            topics.entry(topic).or_default().push(partition);
                        }
                        break;
                    }
                }
            }
        }

        members
            .iter()
            .map(|&member| {
                let topic_assignments: Vec<TopicAssignment> = member_topics
                    .remove(member)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(topic, partitions)| TopicAssignment { topic, partitions })
                    .collect();
                let member_assignment = MemberAssignment {
                    version: 2,
                    topic_partitions: topic_assignments,
                    user_data: None,
                };
                GroupAssignment {
                    member_id: member.to_owned(),
                    group_instance_id: None,
                    assignment: member_assignment.to_bytes(),
                }
            })
            .collect()
    }
}

/// Simple concrete implementation of `TopicPartitions` for testing and use.
pub struct SimpleTopicPartitions {
    data: HashMap<String, Vec<i32>>,
}

impl SimpleTopicPartitions {
    pub fn new(topics: &[(String, Vec<i32>)]) -> Self {
        let data = topics.iter().cloned().collect();
        SimpleTopicPartitions { data }
    }
}

impl TopicPartitions for SimpleTopicPartitions {
    fn topics(&self) -> Vec<&str> {
        let mut topics: Vec<&str> = self.data.keys().map(String::as_str).collect();
        topics.sort_unstable();
        topics
    }

    fn partitions_for(&self, topic: &str) -> Vec<i32> {
        self.data.get(topic).cloned().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tp(topics: &[(String, Vec<i32>)]) -> SimpleTopicPartitions {
        SimpleTopicPartitions::new(topics)
    }

    #[test]
    fn test_range_assignor_single_topic() {
        let assignor = RangeAssignor;
        let members = vec!["m1", "m2"];
        let subs = vec![
            ("m1".to_owned(), vec!["t1".to_owned()]),
            ("m2".to_owned(), vec!["t1".to_owned()]),
        ];
        let tp = make_tp(&[("t1".to_owned(), vec![0, 1, 2, 3])]);

        let assignments = assignor.assign(&members, &subs, &tp);
        assert_eq!(assignments.len(), 2);

        let m1 = MemberAssignment::from_bytes(
            &assignments
                .iter()
                .find(|a| a.member_id == "m1")
                .unwrap()
                .assignment,
        )
        .unwrap();
        let m2 = MemberAssignment::from_bytes(
            &assignments
                .iter()
                .find(|a| a.member_id == "m2")
                .unwrap()
                .assignment,
        )
        .unwrap();

        assert_eq!(m1.topic_partitions.len(), 1);
        assert_eq!(m2.topic_partitions.len(), 1);

        let total: usize =
            m1.topic_partitions[0].partitions.len() + m2.topic_partitions[0].partitions.len();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_range_assignor_no_members() {
        let assignor = RangeAssignor;
        let tp = make_tp(&[("t1".to_owned(), vec![0, 1])]);
        let assignments = assignor.assign(&[], &[], &tp);
        assert!(assignments.is_empty());
    }

    #[test]
    fn test_range_assignor_more_partitions_than_members() {
        let assignor = RangeAssignor;
        let members = vec!["m1"];
        let subs = vec![("m1".to_owned(), vec!["t1".to_owned()])];
        let tp = make_tp(&[("t1".to_owned(), vec![0, 1, 2, 3, 4, 5])]);

        let assignments = assignor.assign(&members, &subs, &tp);
        assert_eq!(assignments.len(), 1);
        let decoded = MemberAssignment::from_bytes(&assignments[0].assignment).unwrap();
        assert_eq!(decoded.topic_partitions[0].partitions.len(), 6);
    }

    #[test]
    fn test_round_robin_assignor_basic() {
        let assignor = RoundRobinAssignor;
        let members = vec!["m1", "m2"];
        let subs = vec![
            ("m1".to_owned(), vec!["t1".to_owned()]),
            ("m2".to_owned(), vec!["t1".to_owned()]),
        ];
        let tp = make_tp(&[("t1".to_owned(), vec![0, 1, 2, 3])]);

        let assignments = assignor.assign(&members, &subs, &tp);
        assert_eq!(assignments.len(), 2);

        let total: usize = assignments
            .iter()
            .map(|a| {
                MemberAssignment::from_bytes(&a.assignment)
                    .unwrap()
                    .topic_partitions
                    .iter()
                    .map(|t| t.partitions.len())
                    .sum::<usize>()
            })
            .sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_round_robin_deterministic() {
        let assignor = RoundRobinAssignor;
        let members = vec!["m1", "m2", "m3"];
        let subs = vec![
            ("m1".to_owned(), vec!["t1".to_owned()]),
            ("m2".to_owned(), vec!["t1".to_owned()]),
            ("m3".to_owned(), vec!["t1".to_owned()]),
        ];
        let tp = make_tp(&[("t1".to_owned(), vec![0, 1, 2, 3, 4, 5])]);

        let a1 = assignor.assign(&members, &subs, &tp);
        let a2 = assignor.assign(&members, &subs, &tp);

        for i in 0..a1.len() {
            assert_eq!(a1[i].member_id, a2[i].member_id);
            assert_eq!(a1[i].assignment, a2[i].assignment);
        }
    }
}
