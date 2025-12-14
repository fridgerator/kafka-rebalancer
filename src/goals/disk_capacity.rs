use crate::models::ClusterModel;
use super::{Goal, GoalPriority, GoalViolation, ViolationSeverity, OptimizationResult};

/// Ensures disk capacity is not exceeded on any broker
pub struct DiskCapacityGoal {
    pub threshold: f64, // e.g., 0.8 for 80%
}

impl Goal for DiskCapacityGoal {
    fn name(&self) -> &str {
        "DiskCapacityGoal"
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::High
    }

    fn check(&self, cluster: &ClusterModel) -> Vec<GoalViolation> {
        let mut violations = Vec::new();

        for broker in cluster.brokers.values() {
            let utilization = broker.utilization();

            if utilization.disk > self.threshold {
                violations.push(GoalViolation {
                    goal_name: self.name().to_string(),
                    severity: if utilization.disk > 0.95 {
                        ViolationSeverity::Critical
                    } else {
                        ViolationSeverity::High
                    },
                    description: format!(
                        "Broker {} disk utilization: {:.1}% (threshold: {:.1}%)",
                        broker.id,
                        utilization.disk * 100.0,
                        self.threshold * 100.0
                    ),
                    affected_brokers: vec![broker.id],
                });
            }
        }

        violations
    }

    fn score(&self, cluster: &ClusterModel) -> f64 {
        let mut total_score = 0.0;
        let broker_count = cluster.brokers.len() as f64;

        for broker in cluster.brokers.values() {
            let utilization = broker.utilization().disk;
            if utilization <= self.threshold {
                total_score += 1.0;
            } else {
                // Partial credit for being close to threshold
                let overage = utilization - self.threshold;
                total_score += (1.0 - overage).max(0.0);
            }
        }

        total_score / broker_count
    }

    fn optimize(&self, cluster: &ClusterModel) -> OptimizationResult {
        let violations = self.check(cluster);
        let actions = Vec::new();

        // TODO: Implement optimize() for DiskCapacityGoal
        // Identify brokers with disk utilization > threshold
        // Sort replicas on overloaded brokers by size (largest first for better impact)
        // Find target brokers with low disk utilization that have capacity
        // Generate MoveReplica actions to move large replicas off overloaded brokers
        // Consider rack awareness to avoid creating new rack violations

        OptimizationResult {
            goal_name: self.name().to_string(),
            violations,
            proposed_actions: actions,
            score: self.score(cluster),
        }
    }
}
