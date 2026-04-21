//! Consumer group rebalance handling.
//!
//! Provides the `RebalanceHandler` for managing consumer group rebalance
//! events and `RebalanceListener` for user-defined callbacks.

use tracing::{debug, warn};

use super::assignor::PartitionAssignor;
use super::group_coordinator::GroupCoordinator;
use crate::error::{Error, KafkaCode, Result};
use crate::protocol::group::MemberAssignment;

/// Callback interface for rebalance events.
///
/// Implementations can be used to commit offsets before a rebalance
/// starts and to initialize state after partitions are assigned.
pub trait RebalanceListener: Send + Sync {
    /// Called before a rebalance begins, providing the set of partitions
    /// being revoked from this consumer.
    fn on_partitions_revoked(&self, revoked: &[(String, Vec<i32>)]);

    /// Called after a rebalance completes, providing the set of partitions
    /// newly assigned to this consumer.
    fn on_partitions_assigned(&self, assigned: &[(String, Vec<i32>)]);
}

/// Handles consumer group rebalance events.
pub struct RebalanceHandler {
    coordinator: GroupCoordinator,
    assignor: Box<dyn PartitionAssignor>,
    listener: Option<Box<dyn RebalanceListener>>,
    subscribed_topics: Vec<String>,
    current_assignment: Option<MemberAssignment>,
}

impl RebalanceHandler {
    /// Creates a new rebalance handler.
    pub fn new(
        coordinator: GroupCoordinator,
        assignor: Box<dyn PartitionAssignor>,
        subscribed_topics: Vec<String>,
    ) -> Self {
        RebalanceHandler {
            coordinator,
            assignor,
            listener: None,
            subscribed_topics,
            current_assignment: None,
        }
    }

    /// Sets a rebalance listener for receiving rebalance event callbacks.
    pub fn with_listener(mut self, listener: Box<dyn RebalanceListener>) -> Self {
        self.listener = Some(listener);
        self
    }

    /// Returns the current partition assignment, if any.
    #[must_use]
    pub fn current_assignment(&self) -> Option<&MemberAssignment> {
        self.current_assignment.as_ref()
    }

    /// Performs the initial join to the consumer group.
    pub fn join_group(&mut self) -> Result<MemberAssignment> {
        let assignment = self
            .coordinator
            .join_group(self.assignor.as_ref(), &self.subscribed_topics)?;
        self.current_assignment = Some(assignment.clone());

        if let Some(ref listener) = self.listener {
            let assigned: Vec<(String, Vec<i32>)> = assignment
                .topic_partitions
                .iter()
                .map(|tp| (tp.topic.clone(), tp.partitions.clone()))
                .collect();
            listener.on_partitions_assigned(&assigned);
        }

        Ok(assignment)
    }

    /// Handles a rebalance triggered by a heartbeat error or other event.
    pub fn handle_rebalance(&mut self) -> Result<MemberAssignment> {
        debug!(
            "Starting rebalance for group '{}'",
            self.coordinator.group_id()
        );

        // Notify listener of revocation
        if let Some(ref listener) = self.listener {
            if let Some(ref assignment) = self.current_assignment {
                let revoked: Vec<(String, Vec<i32>)> = assignment
                    .topic_partitions
                    .iter()
                    .map(|tp| (tp.topic.clone(), tp.partitions.clone()))
                    .collect();
                if !revoked.is_empty() {
                    listener.on_partitions_revoked(&revoked);
                }
            }
        }

        // Re-join the group
        let assignment = self
            .coordinator
            .rejoin(self.assignor.as_ref(), &self.subscribed_topics)?;
        self.current_assignment = Some(assignment.clone());

        // Notify listener of new assignment
        if let Some(ref listener) = self.listener {
            let assigned: Vec<(String, Vec<i32>)> = assignment
                .topic_partitions
                .iter()
                .map(|tp| (tp.topic.clone(), tp.partitions.clone()))
                .collect();
            listener.on_partitions_assigned(&assigned);
        }

        debug!(
            "Rebalance complete for group '{}'",
            self.coordinator.group_id()
        );
        Ok(assignment)
    }

    /// Sends a heartbeat and triggers rebalance if needed.
    ///
    /// Returns `Ok(true)` if the heartbeat was successful,
    /// `Ok(false)` if a rebalance is needed.
    pub fn heartbeat(&mut self) -> Result<bool> {
        match self.coordinator.heartbeat() {
            Ok(()) => Ok(true),
            Err(e) => {
                if is_rebalance_error(&e) {
                    warn!(
                        "Heartbeat failed with rebalance error: {:?}. Rebalance needed.",
                        e
                    );
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Gracefully leaves the consumer group.
    pub fn leave_group(&mut self) -> Result<()> {
        if let Some(ref listener) = self.listener {
            if let Some(ref assignment) = self.current_assignment {
                let revoked: Vec<(String, Vec<i32>)> = assignment
                    .topic_partitions
                    .iter()
                    .map(|tp| (tp.topic.clone(), tp.partitions.clone()))
                    .collect();
                if !revoked.is_empty() {
                    listener.on_partitions_revoked(&revoked);
                }
            }
        }
        self.current_assignment = None;
        self.coordinator.leave_group()
    }

    /// Returns a reference to the underlying group coordinator.
    pub fn coordinator(&self) -> &GroupCoordinator {
        &self.coordinator
    }

    /// Returns a mutable reference to the underlying group coordinator.
    pub fn coordinator_mut(&mut self) -> &mut GroupCoordinator {
        &mut self.coordinator
    }
}

impl Drop for RebalanceHandler {
    fn drop(&mut self) {
        if self.current_assignment.is_some() {
            // Best-effort leave on drop
            let _ = self.coordinator.leave_group();
        }
    }
}

fn is_rebalance_error(err: &Error) -> bool {
    matches!(
        err,
        Error::Kafka(KafkaCode::RebalanceInProgress)
            | Error::Kafka(KafkaCode::IllegalGeneration)
            | Error::Kafka(KafkaCode::UnknownMemberId)
            | Error::Kafka(KafkaCode::GroupCoordinatorNotAvailable)
    )
}

// --------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    struct TestListener;

    impl RebalanceListener for TestListener {
        fn on_partitions_revoked(&self, revoked: &[(String, Vec<i32>)]) {
            let _ = revoked;
        }

        fn on_partitions_assigned(&self, assigned: &[(String, Vec<i32>)]) {
            let _ = assigned;
        }
    }

    #[test]
    fn test_is_rebalance_error() {
        assert!(is_rebalance_error(&Error::Kafka(
            KafkaCode::RebalanceInProgress
        )));
        assert!(is_rebalance_error(&Error::Kafka(
            KafkaCode::IllegalGeneration
        )));
        assert!(is_rebalance_error(&Error::Kafka(
            KafkaCode::UnknownMemberId
        )));
        assert!(is_rebalance_error(&Error::Kafka(
            KafkaCode::GroupCoordinatorNotAvailable
        )));
        assert!(!is_rebalance_error(&Error::Kafka(
            KafkaCode::UnknownTopicOrPartition
        )));
        assert!(!is_rebalance_error(&Error::Kafka(
            KafkaCode::LeaderNotAvailable
        )));
    }

    // Note: Integration tests for join_group/rebalance require a live Kafka cluster.
    // Unit tests for the assignors are in the assignor module.
}
