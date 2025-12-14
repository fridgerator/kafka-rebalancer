use crate::models::ClusterModel;
use super::{Goal, GoalPriority, GoalViolation, ViolationSeverity, OptimizationResult};

/// Preferred leader election - move leaders to first replica
pub struct PreferredLeaderElectionGoal;

impl Goal for PreferredLeaderElectionGoal {
    fn name(&self) -> &str {
        "PreferredLeaderElectionGoal"
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Low
    }

    fn check(&self, cluster: &ClusterModel) -> Vec<GoalViolation> {
        let mut violations = Vec::new();

        for partition in cluster.all_partitions() {
            if let Some(first_replica) = partition.replicas.first() {
                if let Some(leader_id) = partition.leader {
                    if leader_id != first_replica.broker_id {
                        violations.push(GoalViolation {
                            goal_name: self.name().to_string(),
                            severity: ViolationSeverity::Low,
                            description: format!(
                                "Partition {}/{} leader is on broker {} instead of preferred broker {}",
                                partition.topic, partition.id, leader_id, first_replica.broker_id
                            ),
                            affected_brokers: vec![leader_id, first_replica.broker_id],
                        });
                    }
                }
            }
        }

        violations
    }

    fn score(&self, cluster: &ClusterModel) -> f64 {
        let total_partitions = cluster.all_partitions().count();
        if total_partitions == 0 {
            return 1.0;
        }

        let correct_leaders = cluster
            .all_partitions()
            .filter(|p| {
                p.replicas
                    .first()
                    .and_then(|r| p.leader.map(|l| l == r.broker_id))
                    .unwrap_or(false)
            })
            .count();

        correct_leaders as f64 / total_partitions as f64
    }

    fn optimize(&self, cluster: &ClusterModel) -> OptimizationResult {
        let violations = self.check(cluster);
        let actions = Vec::new();

        // TODO: Implement optimize() for PreferredLeaderElectionGoal
        // For each violation, check if the preferred replica (first in list) is in-sync
        // If yes, generate an ElectLeader action to promote it to leader
        // If no, either wait for it to sync or consider it a hard constraint violation
        for _violation in &violations {
            // Create actions to elect preferred leader
        }

        OptimizationResult {
            goal_name: self.name().to_string(),
            violations,
            proposed_actions: actions,
            score: self.score(cluster),
        }
    }
}
