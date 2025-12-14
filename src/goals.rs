use crate::model::*;
use crate::actions::Action;

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

// ============================================================================
// CONCRETE GOAL IMPLEMENTATIONS
// ============================================================================

/// Ensures all replicas are assigned in a rack-aware manner
pub struct RackAwareGoal;

impl Goal for RackAwareGoal {
    fn name(&self) -> &str {
        "RackAwareGoal"
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Critical
    }

    fn check(&self, cluster: &ClusterModel) -> Vec<GoalViolation> {
        let mut violations = Vec::new();

        for topic in cluster.topics.values() {
            for partition in topic.partitions.values() {
                let mut rack_counts: std::collections::HashMap<String, usize> =
                    std::collections::HashMap::new();

                for replica in &partition.replicas {
                    if let Some(broker) = cluster.get_broker(replica.broker_id) {
                        if let Some(rack) = &broker.rack {
                            *rack_counts.entry(rack.clone()).or_insert(0) += 1;
                        }
                    }
                }

                // Check if any rack has more than one replica
                for (rack, count) in rack_counts {
                    if count > 1 {
                        violations.push(GoalViolation {
                            goal_name: self.name().to_string(),
                            severity: ViolationSeverity::Critical,
                            description: format!(
                                "Partition {}/{} has {} replicas in rack {}",
                                partition.topic, partition.id, count, rack
                            ),
                            affected_brokers: partition
                                .replicas
                                .iter()
                                .map(|r| r.broker_id)
                                .collect(),
                        });
                    }
                }
            }
        }

        violations
    }

    fn score(&self, cluster: &ClusterModel) -> f64 {
        let violations = self.check(cluster);
        if violations.is_empty() {
            1.0
        } else {
            let total_partitions = cluster.all_partitions().count();
            let violated_partitions = violations.len();
            1.0 - (violated_partitions as f64 / total_partitions as f64)
        }
    }

    fn optimize(&self, cluster: &ClusterModel) -> OptimizationResult {
        let violations = self.check(cluster);
        let actions = Vec::new();

        // For each violation, try to move replicas to different racks
        for _violation in &violations {
            // Implementation would generate specific replica movements
            // This is simplified for illustration
        }

        OptimizationResult {
            goal_name: self.name().to_string(),
            violations,
            proposed_actions: actions,
            score: self.score(cluster),
        }
    }
}

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

        // Move replicas from overloaded brokers to underloaded ones
        // Implementation omitted for brevity

        OptimizationResult {
            goal_name: self.name().to_string(),
            violations,
            proposed_actions: actions,
            score: self.score(cluster),
        }
    }
}

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

        // Move replicas from high-disk brokers to low-disk brokers
        // Implementation omitted for brevity

        OptimizationResult {
            goal_name: self.name().to_string(),
            violations,
            proposed_actions: actions,
            score: self.score(cluster),
        }
    }
}

/// Attempts to distribute replicas evenly across brokers
pub struct ReplicaDistributionGoal {
    pub allowed_variance: f64, // e.g., 0.1 for 10% variance from average
}

impl Goal for ReplicaDistributionGoal {
    fn name(&self) -> &str {
        "ReplicaDistributionGoal"
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Medium
    }

    fn check(&self, cluster: &ClusterModel) -> Vec<GoalViolation> {
        let mut violations = Vec::new();
        
        let total_replicas: usize = cluster
            .brokers
            .values()
            .map(|b| cluster.replicas_on_broker(b.id).len())
            .sum();
        
        let alive_broker_count = cluster.alive_brokers().count();
        if alive_broker_count == 0 {
            return violations;
        }

        let avg_replicas = total_replicas as f64 / alive_broker_count as f64;
        let allowed_deviation = avg_replicas * self.allowed_variance;

        for broker in cluster.alive_brokers() {
            let replica_count = cluster.replicas_on_broker(broker.id).len() as f64;
            let deviation = (replica_count - avg_replicas).abs();

            if deviation > allowed_deviation {
                violations.push(GoalViolation {
                    goal_name: self.name().to_string(),
                    severity: ViolationSeverity::Medium,
                    description: format!(
                        "Broker {} has {} replicas (avg: {:.1}, allowed range: {:.1}-{:.1})",
                        broker.id,
                        replica_count,
                        avg_replicas,
                        avg_replicas - allowed_deviation,
                        avg_replicas + allowed_deviation
                    ),
                    affected_brokers: vec![broker.id],
                });
            }
        }

        violations
    }

    fn score(&self, cluster: &ClusterModel) -> f64 {
        let total_replicas: usize = cluster
            .brokers
            .values()
            .map(|b| cluster.replicas_on_broker(b.id).len())
            .sum();
        
        let alive_broker_count = cluster.alive_brokers().count();
        if alive_broker_count == 0 {
            return 1.0;
        }

        let avg_replicas = total_replicas as f64 / alive_broker_count as f64;
        
        // Calculate coefficient of variation
        let variance: f64 = cluster
            .alive_brokers()
            .map(|b| {
                let count = cluster.replicas_on_broker(b.id).len() as f64;
                (count - avg_replicas).powi(2)
            })
            .sum::<f64>()
            / alive_broker_count as f64;

        let std_dev = variance.sqrt();
        let cv = std_dev / avg_replicas;

        // Convert CV to score (lower CV = higher score)
        (1.0 - cv.min(1.0)).max(0.0)
    }

    fn optimize(&self, cluster: &ClusterModel) -> OptimizationResult {
        let violations = self.check(cluster);
        let mut actions = Vec::new();

        // Calculate current replica distribution
        let mut broker_replica_counts: std::collections::HashMap<BrokerId, usize> =
            std::collections::HashMap::new();
        
        for broker in cluster.alive_brokers() {
            let count = cluster.replicas_on_broker(broker.id).len();
            broker_replica_counts.insert(broker.id, count);
        }

        let alive_broker_count = cluster.alive_brokers().count();
        if alive_broker_count == 0 {
            return OptimizationResult {
                goal_name: self.name().to_string(),
                violations,
                proposed_actions: actions,
                score: self.score(cluster),
            };
        }

        let total_replicas: usize = broker_replica_counts.values().sum();
        let avg_replicas = total_replicas as f64 / alive_broker_count as f64;
        let allowed_deviation = avg_replicas * self.allowed_variance;

        // Identify overloaded and underloaded brokers
        let mut overloaded: Vec<(BrokerId, usize)> = broker_replica_counts
            .iter()
            .filter(|&(_, &count)| count as f64 > avg_replicas + allowed_deviation)
            .map(|(&id, &count)| (id, count))
            .collect();

        let mut underloaded: Vec<(BrokerId, usize)> = broker_replica_counts
            .iter()
            .filter(|&(_, &count)| (count as f64) < avg_replicas - allowed_deviation)
            .map(|(&id, &count)| (id, count))
            .collect();

        // Sort by deviation from average (most overloaded first, most underloaded first)
        overloaded.sort_by(|a, b| b.1.cmp(&a.1));
        underloaded.sort_by(|a, b| a.1.cmp(&b.1));

        // Generate actions to move replicas from overloaded to underloaded brokers
        for (from_broker, _) in overloaded {
            if underloaded.is_empty() {
                break;
            }

            // Collect partition/replica info for this broker
            let mut replicas_to_move = Vec::new();
            
            for topic in cluster.topics.values() {
                for partition in topic.partitions.values() {
                    for replica in &partition.replicas {
                        if replica.broker_id == from_broker {
                            replicas_to_move.push((
                                partition.topic.clone(),
                                partition.id,
                                replica.size_mb,
                            ));
                        }
                    }
                }
            }
            
            for (topic, partition_id, size_mb) in replicas_to_move {
                if underloaded.is_empty() {
                    break;
                }

                // Find the best underloaded broker for this replica
                let to_broker = self.find_best_target_broker(
                    cluster,
                    &topic,
                    partition_id,
                    from_broker,
                    &underloaded,
                );

                if let Some(target_broker) = to_broker {
                    // Create move action
                    actions.push(Action::MoveReplica {
                        topic: topic.clone(),
                        partition: partition_id,
                        from_broker,
                        to_broker: target_broker,
                        replica_size_mb: size_mb,
                    });

                    // Update tracking
                    if let Some(count) = broker_replica_counts.get_mut(&from_broker) {
                        *count -= 1;
                    }
                    if let Some(count) = broker_replica_counts.get_mut(&target_broker) {
                        *count += 1;
                    }

                    // Re-evaluate if target is still underloaded
                    let target_count = broker_replica_counts[&target_broker];
                    if target_count as f64 >= avg_replicas - allowed_deviation {
                        underloaded.retain(|(id, _)| *id != target_broker);
                    }

                    // Re-evaluate if source is still overloaded
                    let source_count = broker_replica_counts[&from_broker];
                    if source_count as f64 <= avg_replicas + allowed_deviation {
                        break; // Move to next overloaded broker
                    }
                }
            }
        }

        OptimizationResult {
            goal_name: self.name().to_string(),
            violations,
            proposed_actions: actions,
            score: self.score(cluster),
        }
    }
}

// Helper methods for ReplicaDistributionGoal
impl ReplicaDistributionGoal {
    // Helper method to find the best target broker for a replica
    fn find_best_target_broker<'a>(
        &self,
        cluster: &'a ClusterModel,
        topic: &str,
        partition_id: PartitionId,
        from_broker: BrokerId,
        underloaded: &[(BrokerId, usize)],
    ) -> Option<BrokerId> {
        // Get the partition to check existing replicas
        let partition = cluster
            .topics
            .get(topic)
            .and_then(|t| t.partitions.get(&partition_id))?;

        // Track existing brokers and racks for this partition
        let existing_brokers: std::collections::HashSet<BrokerId> =
            partition.replicas.iter().map(|r| r.broker_id).collect();

        let existing_racks: std::collections::HashSet<String> = partition
            .replicas
            .iter()
            .filter_map(|r| {
                cluster
                    .get_broker(r.broker_id)
                    .and_then(|b| b.rack.clone())
            })
            .collect();

        // Track brokers that host other partitions of the same topic
        let brokers_with_topic_partitions: std::collections::HashSet<BrokerId> =
            cluster
                .topics
                .get(topic)
                .map(|t| {
                    t.partitions
                        .values()
                        .flat_map(|p| p.replicas.iter().map(|r| r.broker_id))
                        .collect()
                })
                .unwrap_or_default();

        // Get the replica being moved for resource checks
        let replica_to_move = partition
            .replicas
            .iter()
            .find(|r| r.broker_id == from_broker)?;

        // First pass: Find brokers with no partitions of this topic, different rack, and capacity
        for &(broker_id, _) in underloaded {
            // Don't move to a broker that already has this partition
            if existing_brokers.contains(&broker_id) {
                continue;
            }

            // Prefer brokers that don't have any partitions of this topic
            if brokers_with_topic_partitions.contains(&broker_id) {
                continue;
            }

            // Check rack awareness and capacity
            if let Some(broker) = cluster.get_broker(broker_id) {
                // Prefer brokers in different racks (rack-aware placement)
                if let Some(rack) = &broker.rack {
                    if existing_racks.contains(rack) {
                        continue; // Skip brokers in same rack
                    }
                }

                // Check if broker can accommodate this replica
                if !broker.can_accommodate(&replica_to_move.resource_usage) {
                    continue;
                }

                return Some(broker_id);
            }
        }

        // Second pass: Allow brokers with topic partitions, but still require different rack
        for &(broker_id, _) in underloaded {
            if existing_brokers.contains(&broker_id) {
                continue;
            }

            // Check rack awareness and capacity
            if let Some(broker) = cluster.get_broker(broker_id) {
                // Prefer brokers in different racks (rack-aware placement)
                if let Some(rack) = &broker.rack {
                    if existing_racks.contains(rack) {
                        continue; // Skip brokers in same rack
                    }
                }

                // Check if broker can accommodate this replica
                if !broker.can_accommodate(&replica_to_move.resource_usage) {
                    continue;
                }

                return Some(broker_id);
            }
        }

        // Third pass: Relax rack constraint, but still prefer no topic partitions
        for &(broker_id, _) in underloaded {
            if existing_brokers.contains(&broker_id) {
                continue;
            }

            // Prefer brokers that don't have any partitions of this topic
            if brokers_with_topic_partitions.contains(&broker_id) {
                continue;
            }

            if let Some(broker) = cluster.get_broker(broker_id) {
                if broker.can_accommodate(&replica_to_move.resource_usage) {
                    return Some(broker_id);
                }
            }
        }

        // Final pass: Any underloaded broker that can accommodate the replica
        for &(broker_id, _) in underloaded {
            if existing_brokers.contains(&broker_id) {
                continue;
            }

            if let Some(broker) = cluster.get_broker(broker_id) {
                if broker.can_accommodate(&replica_to_move.resource_usage) {
                    return Some(broker_id);
                }
            }
        }

        None
    }
}

/// Attempts to distribute leader replicas evenly
pub struct LeaderDistributionGoal {
    pub allowed_variance: f64,
}

impl Goal for LeaderDistributionGoal {
    fn name(&self) -> &str {
        "LeaderDistributionGoal"
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Medium
    }

    fn check(&self, _cluster: &ClusterModel) -> Vec<GoalViolation> {
        // Similar to ReplicaDistributionGoal but for leaders
        Vec::new() // Simplified
    }

    fn score(&self, _cluster: &ClusterModel) -> f64 {
        1.0 // Simplified
    }

    fn optimize(&self, _cluster: &ClusterModel) -> OptimizationResult {
        OptimizationResult {
            goal_name: self.name().to_string(),
            violations: Vec::new(),
            proposed_actions: Vec::new(),
            score: 1.0,
        }
    }
}

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

        // Generate leader election actions
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