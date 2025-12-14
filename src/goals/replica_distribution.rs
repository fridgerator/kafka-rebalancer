use crate::actions::Action;
use crate::models::{BrokerId, ClusterModel, PartitionId};
use super::{Goal, GoalPriority, GoalViolation, ViolationSeverity, OptimizationResult};

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

impl ReplicaDistributionGoal {
    fn find_best_target_broker(
        &self,
        cluster: &ClusterModel,
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
