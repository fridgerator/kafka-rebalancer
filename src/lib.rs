// Kafka Partition Rebalancer Library
// A Rust implementation inspired by LinkedIn's Cruise Control

pub mod model;
pub mod goals;
pub mod optimizer;
pub mod actions;
pub mod constraints;

pub use model::{ClusterModel, Broker, Partition, Replica};
pub use goals::{Goal, GoalViolation, OptimizationResult};
pub use optimizer::Optimizer;
pub use actions::{Action, RebalancePlan};
pub use constraints::BalancingConstraints;

/// Main entry point for generating rebalance proposals
pub struct Rebalancer {
    optimizer: Optimizer,
}

impl Rebalancer {
    pub fn new(goals: Vec<Box<dyn Goal>>) -> Self {
        Self {
            optimizer: Optimizer::new(goals),
        }
    }

    /// Generate a rebalance plan for the given cluster state
    pub fn generate_plan(
        &self,
        cluster: &ClusterModel,
        constraints: &BalancingConstraints,
    ) -> Result<RebalancePlan, RebalancerError> {
        self.optimizer.optimize(cluster, constraints)
    }

    /// Check if the cluster violates any goals
    pub fn check_violations(&self, cluster: &ClusterModel) -> Vec<GoalViolation> {
        self.optimizer.check_violations(cluster)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RebalancerError {
    #[error("Optimization failed: {0}")]
    OptimizationFailed(String),
    
    #[error("Invalid cluster state: {0}")]
    InvalidClusterState(String),
    
    #[error("Goal violation cannot be fixed: {0}")]
    UnfixableViolation(String),
}