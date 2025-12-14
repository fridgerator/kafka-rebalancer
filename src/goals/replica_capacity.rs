use crate::models::ClusterModel;
use super::{Goal, GoalPriority, GoalViolation, ViolationSeverity, OptimizationResult};

/// Ensures that the number of replicas per broker is below a threshold
pub struct ReplicaCapacityGoal {
    pub max_replicas_per_broker: usize,
}

impl Goal for ReplicaCapacityGoal {
    fn name(&self) -> &str {
        "ReplicaCapacityGoal"
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::High
    }

    fn check(&self, cluster: &ClusterModel) -> Vec<GoalViolation> {
        let mut violations = Vec::new();

        for broker in cluster.brokers.values() {
            let replica_count = cluster.replicas_on_broker(broker.id).len();

            if replica_count > self.max_replicas_per_broker {
                violations.push(GoalViolation {
                    goal_name: self.name().to_string(),
                    severity: ViolationSeverity::High,
                    description: format!(
                        "Broker {} has {} replicas (max: {})",
                        broker.id, replica_count, self.max_replicas_per_broker
                    ),
                    affected_brokers: vec![broker.id],
                });
            }
        }

        violations
    }

    fn score(&self, cluster: &ClusterModel) -> f64 {
        let violations = self.check(cluster);
        if violations.is_empty() {
            1.0
        } else {
            let total_brokers = cluster.brokers.len();
            let violated_brokers = violations.len();
            1.0 - (violated_brokers as f64 / total_brokers as f64)
        }
    }

    fn optimize(&self, cluster: &ClusterModel) -> OptimizationResult {
        let violations = self.check(cluster);
        let actions = Vec::new();

        // TODO: Implement optimize() for ReplicaCapacityGoal
        // Identify brokers with replica count > max_replicas_per_broker
        // Find underloaded brokers that can accept more replicas
        // Generate MoveReplica actions to move replicas from overloaded to underloaded brokers
        // Consider rack awareness and resource capacity when selecting target brokers

        OptimizationResult {
            goal_name: self.name().to_string(),
            violations,
            proposed_actions: actions,
            score: self.score(cluster),
        }
    }
}
