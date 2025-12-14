use crate::actions::Action;
use crate::models::{BrokerId, ClusterModel};

/// Priority level for goals
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GoalPriority {
    Critical = 0,  // Must be satisfied
    High = 1,
    Medium = 2,
    Low = 3,
}

/// Result of goal evaluation
#[derive(Debug)]
pub struct OptimizationResult {
    pub goal_name: String,
    pub violations: Vec<GoalViolation>,
    pub proposed_actions: Vec<Action>,
    pub score: f64, // 0.0 = complete violation, 1.0 = perfect compliance
}

#[derive(Debug, Clone)]
pub struct GoalViolation {
    pub goal_name: String,
    pub severity: ViolationSeverity,
    pub description: String,
    pub affected_brokers: Vec<BrokerId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViolationSeverity {
    Critical,
    High,
    Medium,
    Low,
}

/// Core trait for optimization goals
pub trait Goal: Send + Sync {
    /// Name of this goal
    fn name(&self) -> &str;

    /// Priority of this goal
    fn priority(&self) -> GoalPriority;

    /// Check if the cluster satisfies this goal
    fn check(&self, cluster: &ClusterModel) -> Vec<GoalViolation>;

    /// Calculate a score for how well the cluster satisfies this goal
    /// Returns 0.0 (worst) to 1.0 (perfect)
    fn score(&self, cluster: &ClusterModel) -> f64;

    /// Generate actions to optimize the cluster for this goal
    fn optimize(&self, cluster: &ClusterModel) -> OptimizationResult;

    /// Check if this is a hard goal (must be satisfied)
    fn is_hard_goal(&self) -> bool {
        matches!(self.priority(), GoalPriority::Critical)
    }
}

// Module declarations
mod rack_aware;
mod replica_capacity;
mod disk_capacity;
mod replica_distribution;
mod leader_distribution;
mod preferred_leader;

// Re-exports
pub use rack_aware::RackAwareGoal;
pub use replica_capacity::ReplicaCapacityGoal;
pub use disk_capacity::DiskCapacityGoal;
pub use replica_distribution::ReplicaDistributionGoal;
pub use leader_distribution::LeaderDistributionGoal;
pub use preferred_leader::PreferredLeaderElectionGoal;
