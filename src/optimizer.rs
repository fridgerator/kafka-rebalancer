use crate::actions::{Action, ActionOrderingStrategy, RebalancePlan};
use crate::constraints::BalancingConstraints;
use crate::goals::{Goal, GoalPriority, GoalViolation};
use crate::model::ClusterModel;
use crate::RebalancerError;
use std::collections::HashMap;

/// The optimizer coordinates multiple goals to generate an optimal rebalance plan
pub struct Optimizer {
    goals: Vec<Box<dyn Goal>>,
    ordering_strategy: ActionOrderingStrategy,
}

impl Optimizer {
    pub fn new(goals: Vec<Box<dyn Goal>>) -> Self {
        Self {
            goals,
            ordering_strategy: ActionOrderingStrategy::Sequential,
        }
    }

    pub fn with_ordering_strategy(mut self, strategy: ActionOrderingStrategy) -> Self {
        self.ordering_strategy = strategy;
        self
    }

    /// Generate an optimized rebalance plan
    pub fn optimize(
        &self,
        cluster: &ClusterModel,
        constraints: &BalancingConstraints,
    ) -> Result<RebalancePlan, RebalancerError> {
        // Sort goals by priority
        let mut sorted_goals: Vec<&Box<dyn Goal>> = self.goals.iter().collect();
        sorted_goals.sort_by_key(|g| g.priority());

        let mut current_cluster = cluster.clone();
        let mut all_actions = Vec::new();
        let mut goal_improvements = HashMap::new();

        // Optimize for each goal in priority order
        for goal in sorted_goals {
            let initial_score = goal.score(&current_cluster);
            let result = goal.optimize(&current_cluster);

            // If this is a hard goal and it's violated, we must address it
            if goal.is_hard_goal() && !result.violations.is_empty() {
                // Apply the proposed actions and check if they fix the violations
                let mut temp_cluster = current_cluster.clone();
                for action in &result.proposed_actions {
                    if let Some(updated) = self.apply_action(&temp_cluster, action) {
                        temp_cluster = updated;
                    }
                }

                // Verify the hard goal is now satisfied
                let new_violations = goal.check(&temp_cluster);
                if !new_violations.is_empty() {
                    return Err(RebalancerError::UnfixableViolation(format!(
                        "Hard goal '{}' cannot be satisfied",
                        goal.name()
                    )));
                }

                current_cluster = temp_cluster;
            }

            // Filter actions based on constraints
            let filtered_actions = self.filter_actions(result.proposed_actions, constraints);

            // Apply actions to get new cluster state
            for action in &filtered_actions {
                if let Some(updated) = self.apply_action(&current_cluster, action) {
                    current_cluster = updated;
                    all_actions.push(action.clone());
                }
            }

            let final_score = goal.score(&current_cluster);
            let improvement = final_score - initial_score;
            goal_improvements.insert(goal.name().to_string(), improvement);

            // Check if we've exceeded action limit
            if all_actions.len() >= constraints.max_actions_per_plan {
                break;
            }
        }

        // Apply ordering strategy
        let mut actions = all_actions;
        self.ordering_strategy.sort_actions(&mut actions);

        // Build the plan
        let mut plan = RebalancePlan::new(actions);
        plan.goal_improvements = goal_improvements;
        plan.metadata.goals_used = self.goals.iter().map(|g| g.name().to_string()).collect();

        // Validate plan doesn't exceed data transfer limit
        if plan.total_data_transfer_mb > constraints.max_total_data_transfer_mb {
            return Err(RebalancerError::OptimizationFailed(format!(
                "Plan exceeds maximum data transfer limit: {} MB > {} MB",
                plan.total_data_transfer_mb, constraints.max_total_data_transfer_mb
            )));
        }

        Ok(plan)
    }

    /// Check for goal violations without generating a plan
    pub fn check_violations(&self, cluster: &ClusterModel) -> Vec<GoalViolation> {
        self.goals
            .iter()
            .flat_map(|goal| goal.check(cluster))
            .collect()
    }

    /// Get a report on how well each goal is satisfied
    pub fn goal_report(&self, cluster: &ClusterModel) -> Vec<GoalReport> {
        self.goals
            .iter()
            .map(|goal| GoalReport {
                name: goal.name().to_string(),
                priority: goal.priority(),
                score: goal.score(cluster),
                violations: goal.check(cluster),
            })
            .collect()
    }

    /// Filter actions based on constraints
    fn filter_actions(
        &self,
        actions: Vec<Action>,
        constraints: &BalancingConstraints,
    ) -> Vec<Action> {
        actions
            .into_iter()
            .filter(|action| self.is_action_allowed(action, constraints))
            .collect()
    }

    /// Check if an action is allowed under the given constraints
    fn is_action_allowed(&self, action: &Action, constraints: &BalancingConstraints) -> bool {
        match action {
            Action::MoveReplica {
                to_broker, topic, ..
            } => {
                constraints.can_move_replicas()
                    && constraints.can_add_replica_to_broker(*to_broker)
                    && constraints.can_rebalance_topic(topic)
            }
            Action::ElectLeader { topic, .. } => {
                constraints.can_change_leadership() && constraints.can_rebalance_topic(topic)
            }
            Action::AddReplica { broker, topic, .. } => {
                constraints.can_move_replicas()
                    && constraints.can_add_replica_to_broker(*broker)
                    && constraints.can_rebalance_topic(topic)
            }
            Action::RemoveReplica { topic, .. } => {
                constraints.can_move_replicas() && constraints.can_rebalance_topic(topic)
            }
            Action::SwapReplicas { topic, .. } => {
                constraints.can_move_replicas() && constraints.can_rebalance_topic(topic)
            }
        }
    }

    /// Apply an action to a cluster model (simulation)
    fn apply_action(&self, cluster: &ClusterModel, action: &Action) -> Option<ClusterModel> {
        match action {
            Action::MoveReplica {
                topic,
                partition,
                from_broker,
                to_broker,
                ..
            } => cluster.simulate_replica_move(topic, *partition, *from_broker, *to_broker),
            Action::ElectLeader { .. } => {
                // Leader election doesn't change cluster structure for scoring purposes
                Some(cluster.clone())
            }
            Action::AddReplica { .. } | Action::RemoveReplica { .. } => {
                // Would need more complex simulation logic
                Some(cluster.clone())
            }
            Action::SwapReplicas { .. } => {
                // Would need more complex simulation logic
                Some(cluster.clone())
            }
        }
    }
}

/// Report on a single goal's performance
#[derive(Debug, Clone)]
pub struct GoalReport {
    pub name: String,
    pub priority: GoalPriority,
    pub score: f64,
    pub violations: Vec<GoalViolation>,
}

impl GoalReport {
    pub fn is_satisfied(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn summary(&self) -> String {
        format!(
            "{} (Priority: {:?}): Score {:.2}%, {} violations",
            self.name,
            self.priority,
            self.score * 100.0,
            self.violations.len()
        )
    }
}

/// Builder for creating an Optimizer with a fluent API
pub struct OptimizerBuilder {
    goals: Vec<Box<dyn Goal>>,
    ordering_strategy: ActionOrderingStrategy,
}

impl OptimizerBuilder {
    pub fn new() -> Self {
        Self {
            goals: Vec::new(),
            ordering_strategy: ActionOrderingStrategy::Sequential,
        }
    }

    pub fn add_goal(mut self, goal: Box<dyn Goal>) -> Self {
        self.goals.push(goal);
        self
    }

    pub fn with_ordering_strategy(mut self, strategy: ActionOrderingStrategy) -> Self {
        self.ordering_strategy = strategy;
        self
    }

    pub fn build(self) -> Optimizer {
        Optimizer {
            goals: self.goals,
            ordering_strategy: self.ordering_strategy,
        }
    }
}

impl Default for OptimizerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goals::*;
    use crate::model::*;

    #[test]
    fn test_optimizer_creation() {
        let goals: Vec<Box<dyn Goal>> = vec![
            Box::new(RackAwareGoal),
            Box::new(ReplicaDistributionGoal {
                allowed_variance: 0.1,
            }),
        ];

        let optimizer = Optimizer::new(goals);
        assert_eq!(optimizer.goals.len(), 2);
    }

    #[test]
    fn test_optimizer_builder() {
        let optimizer = OptimizerBuilder::new()
            .add_goal(Box::new(RackAwareGoal))
            .add_goal(Box::new(ReplicaDistributionGoal {
                allowed_variance: 0.1,
            }))
            .with_ordering_strategy(ActionOrderingStrategy::SmallReplicasFirst)
            .build();

        assert_eq!(optimizer.goals.len(), 2);
    }

    #[test]
    fn test_empty_cluster_optimization() {
        let cluster = ClusterModel::new();
        let optimizer = Optimizer::new(vec![]);
        let constraints = BalancingConstraints::default();

        let result = optimizer.optimize(&cluster, &constraints);
        assert!(result.is_ok());
        let plan = result.unwrap();
        assert!(plan.is_empty());
    }
}