use crate::models::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a proposed rebalancing action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    /// Move a replica from one broker to another
    MoveReplica {
        topic: TopicName,
        partition: PartitionId,
        from_broker: BrokerId,
        to_broker: BrokerId,
        replica_size_mb: u64,
    },

    /// Elect a new leader for a partition
    ElectLeader {
        topic: TopicName,
        partition: PartitionId,
        new_leader: BrokerId,
    },

    /// Add a new replica to a partition
    AddReplica {
        topic: TopicName,
        partition: PartitionId,
        broker: BrokerId,
    },

    /// Remove a replica from a partition
    RemoveReplica {
        topic: TopicName,
        partition: PartitionId,
        broker: BrokerId,
    },

    /// Swap positions of two replicas in the replica list
    SwapReplicas {
        topic: TopicName,
        partition: PartitionId,
        broker1: BrokerId,
        broker2: BrokerId,
    },
}

impl Action {
    /// Estimate the cost/impact of this action
    pub fn cost(&self) -> ActionCost {
        match self {
            Action::MoveReplica { replica_size_mb, .. } => ActionCost {
                data_transfer_mb: *replica_size_mb,
                leader_movement: false,
                estimated_duration_secs: (*replica_size_mb as f64 / 100.0).ceil() as u64, // Assume 100 MB/s
            },
            Action::ElectLeader { .. } => ActionCost {
                data_transfer_mb: 0,
                leader_movement: true,
                estimated_duration_secs: 1, // Leader elections are fast
            },
            Action::AddReplica { .. } => ActionCost {
                data_transfer_mb: 1000, // Estimate
                leader_movement: false,
                estimated_duration_secs: 10,
            },
            Action::RemoveReplica { .. } => ActionCost {
                data_transfer_mb: 0,
                leader_movement: false,
                estimated_duration_secs: 1,
            },
            Action::SwapReplicas { .. } => ActionCost {
                data_transfer_mb: 0,
                leader_movement: false,
                estimated_duration_secs: 1,
            },
        }
    }

    /// Get the brokers affected by this action
    pub fn affected_brokers(&self) -> Vec<BrokerId> {
        match self {
            Action::MoveReplica {
                from_broker,
                to_broker,
                ..
            } => vec![*from_broker, *to_broker],
            Action::ElectLeader { new_leader, .. } => vec![*new_leader],
            Action::AddReplica { broker, .. } => vec![*broker],
            Action::RemoveReplica { broker, .. } => vec![*broker],
            Action::SwapReplicas {
                broker1, broker2, ..
            } => vec![*broker1, *broker2],
        }
    }

    /// Get a human-readable description
    pub fn description(&self) -> String {
        match self {
            Action::MoveReplica {
                topic,
                partition,
                from_broker,
                to_broker,
                replica_size_mb,
            } => format!(
                "Move replica of {}/{} from broker {} to {} ({} MB)",
                topic, partition, from_broker, to_broker, replica_size_mb
            ),
            Action::ElectLeader {
                topic,
                partition,
                new_leader,
            } => format!(
                "Elect broker {} as leader for {}/{}",
                new_leader, topic, partition
            ),
            Action::AddReplica {
                topic,
                partition,
                broker,
            } => format!("Add replica of {}/{} to broker {}", topic, partition, broker),
            Action::RemoveReplica {
                topic,
                partition,
                broker,
            } => format!(
                "Remove replica of {}/{} from broker {}",
                topic, partition, broker
            ),
            Action::SwapReplicas {
                topic,
                partition,
                broker1,
                broker2,
            } => format!(
                "Swap replica positions for {}/{} between brokers {} and {}",
                topic, partition, broker1, broker2
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ActionCost {
    pub data_transfer_mb: u64,
    pub leader_movement: bool,
    pub estimated_duration_secs: u64,
}

/// A complete rebalancing plan with ordered actions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancePlan {
    pub actions: Vec<Action>,
    pub estimated_duration_secs: u64,
    pub total_data_transfer_mb: u64,
    pub goal_improvements: HashMap<String, f64>,
    pub metadata: PlanMetadata,
}

impl RebalancePlan {
    pub fn new(actions: Vec<Action>) -> Self {
        let total_data_transfer_mb = actions
            .iter()
            .map(|a| a.cost().data_transfer_mb)
            .sum();

        let estimated_duration_secs = actions
            .iter()
            .map(|a| a.cost().estimated_duration_secs)
            .sum();

        Self {
            actions,
            estimated_duration_secs,
            total_data_transfer_mb,
            goal_improvements: HashMap::new(),
            metadata: PlanMetadata::default(),
        }
    }

    /// Group actions into batches that can be executed concurrently
    pub fn batch_actions(&self, max_concurrent: usize) -> Vec<Vec<&Action>> {
        let mut batches = Vec::new();
        let mut current_batch = Vec::new();
        let mut affected_brokers = std::collections::HashSet::new();

        for action in &self.actions {
            let action_brokers = action.affected_brokers();
            
            // Check if any broker in this action is already in current batch
            let has_conflict = action_brokers
                .iter()
                .any(|b| affected_brokers.contains(b));

            if has_conflict || current_batch.len() >= max_concurrent {
                // Start new batch
                if !current_batch.is_empty() {
                    batches.push(current_batch);
                    current_batch = Vec::new();
                    affected_brokers.clear();
                }
            }

            current_batch.push(action);
            affected_brokers.extend(action_brokers);
        }

        if !current_batch.is_empty() {
            batches.push(current_batch);
        }

        batches
    }

    /// Calculate total improvement score
    pub fn total_improvement(&self) -> f64 {
        self.goal_improvements.values().sum()
    }

    /// Check if this plan is empty
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    /// Get summary statistics
    pub fn summary(&self) -> PlanSummary {
        let mut move_count = 0;
        let mut leader_election_count = 0;
        let mut add_replica_count = 0;
        let mut remove_replica_count = 0;

        for action in &self.actions {
            match action {
                Action::MoveReplica { .. } => move_count += 1,
                Action::ElectLeader { .. } => leader_election_count += 1,
                Action::AddReplica { .. } => add_replica_count += 1,
                Action::RemoveReplica { .. } => remove_replica_count += 1,
                Action::SwapReplicas { .. } => {}
            }
        }

        PlanSummary {
            total_actions: self.actions.len(),
            move_count,
            leader_election_count,
            add_replica_count,
            remove_replica_count,
            estimated_duration_secs: self.estimated_duration_secs,
            total_data_transfer_mb: self.total_data_transfer_mb,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanMetadata {
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    pub goals_used: Vec<String>,
    pub cluster_stats: Option<ClusterStats>,
}

impl Default for PlanMetadata {
    fn default() -> Self {
        Self {
            created_at: Some(chrono::Utc::now()),
            goals_used: Vec::new(),
            cluster_stats: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStats {
    pub broker_count: usize,
    pub topic_count: usize,
    pub partition_count: usize,
    pub replica_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanSummary {
    pub total_actions: usize,
    pub move_count: usize,
    pub leader_election_count: usize,
    pub add_replica_count: usize,
    pub remove_replica_count: usize,
    pub estimated_duration_secs: u64,
    pub total_data_transfer_mb: u64,
}

impl std::fmt::Display for PlanSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Total Actions: {}, Moves: {}, Leader Elections: {}, Data Transfer: {} MB, Duration: {}s",
            self.total_actions,
            self.move_count,
            self.leader_election_count,
            self.total_data_transfer_mb,
            self.estimated_duration_secs
        )
    }
}

/// Strategy for ordering actions in a plan
#[derive(Debug, Clone, Copy)]
pub enum ActionOrderingStrategy {
    /// Execute actions in the order they were generated
    Sequential,
    
    /// Prioritize small data transfers first
    SmallReplicasFirst,
    
    /// Prioritize large data transfers first
    LargeReplicasFirst,
    
    /// Prioritize partitions that are under min ISR
    UnderMinIsrFirst,
    
    /// Minimize leader movements
    MinimizeLeaderMovement,
}

impl ActionOrderingStrategy {
    pub fn sort_actions(&self, actions: &mut Vec<Action>) {
        match self {
            ActionOrderingStrategy::Sequential => {
                // No sorting needed
            }
            ActionOrderingStrategy::SmallReplicasFirst => {
                actions.sort_by_key(|a| a.cost().data_transfer_mb);
            }
            ActionOrderingStrategy::LargeReplicasFirst => {
                actions.sort_by_key(|a| std::cmp::Reverse(a.cost().data_transfer_mb));
            }
            ActionOrderingStrategy::UnderMinIsrFirst => {
                // Would need additional partition metadata
                // Placeholder for now
            }
            ActionOrderingStrategy::MinimizeLeaderMovement => {
                actions.sort_by_key(|a| {
                    if a.cost().leader_movement {
                        std::cmp::Reverse(1)
                    } else {
                        std::cmp::Reverse(0)
                    }
                });
            }
        }
    }
}