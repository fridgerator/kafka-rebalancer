use kafka_rebalancer::*;

use kafka_rebalancer::models::{Broker, ResourceCapacity, Topic, Partition, Replica, ResourceUsage};

fn main() {
    println!("Kafka Partition Rebalancer - Example Usage\n");

    // Create a sample cluster
    let cluster = create_sample_cluster();

    // Create goals
    let goals: Vec<Box<dyn Goal>> = vec![
        // Box::new(goals::RackAwareGoal),
        // Box::new(goals::DiskCapacityGoal { threshold: 0.8 }),
        Box::new(goals::ReplicaDistributionGoal {
            allowed_variance: 0.1,
        }),
        // Box::new(goals::LeaderDistributionGoal {
        //     allowed_variance: 0.1,
        // }),
    ];

    // Create rebalancer
    let rebalancer = Rebalancer::new(goals);

    // Check for violations
    println!("=== Checking for Goal Violations ===");
    let violations = rebalancer.check_violations(&cluster);
    if violations.is_empty() {
        println!("No violations found!");
    } else {
        for violation in &violations {
            println!(
                "[{:?}] {} - {}",
                violation.severity, violation.goal_name, violation.description
            );
        }
    }

    // Create constraints
    let constraints = BalancingConstraints::default();

    // Generate rebalance plan
    println!("\n=== Generating Rebalance Plan ===");
    match rebalancer.generate_plan(&cluster, &constraints) {
        Ok(plan) => {
            let summary = plan.summary();
            println!("{}\n", summary);
            
            if plan.is_empty() {
                println!("✓ No rebalancing needed - cluster is already balanced!");
                return;
            }
            
            println!("Goal Improvements:");
            for (goal, improvement) in &plan.goal_improvements {
                println!("  {}: {:+.2}%", goal, improvement * 100.0);
            }

            println!("\n=== Proposed Actions (showing first 20) ===");
            for (i, action) in plan.actions.iter().take(20).enumerate() {
                println!("{}. {}", i + 1, action.description());
            }

            if plan.actions.len() > 20 {
                println!("... and {} more actions", plan.actions.len() - 20);
            }

            // Show movements in compact format
            print_partition_movements(&plan);

            // Show batched execution plan
            println!("\n=== Batched Execution (max 5 concurrent) ===");
            let batches = plan.batch_actions(5);
            println!("Total batches: {}", batches.len());
            for (i, batch) in batches.iter().take(5).enumerate() {
                println!("  Batch {}: {} actions", i + 1, batch.len());
            }
            if batches.len() > 5 {
                println!("  ... and {} more batches", batches.len() - 5);
            }
            
            // Simulate the plan and show final state
            println!("\n=== Simulated Final State (after applying plan) ===");
            let mut final_cluster = cluster.clone();
            for action in &plan.actions {
                if let actions::Action::MoveReplica { topic, partition, from_broker, to_broker, .. } = action {
                    if let Some(updated) = final_cluster.simulate_replica_move(
                        topic,
                        *partition,
                        *from_broker,
                        *to_broker,
                    ) {
                        final_cluster = updated;
                    }
                }
            }
            
            print_replica_distribution(&final_cluster);
            print_rack_distribution(&final_cluster);
            
            // Show improvement metrics
            println!("=== Improvement Summary ===");
            println!("Replica distribution variance: {:.3} → {:.3}",
                calculate_distribution_variance(&cluster),
                calculate_distribution_variance(&final_cluster)
            );
        }
        Err(e) => {
            eprintln!("Failed to generate plan: {}", e);
        }
    }
}

fn calculate_distribution_variance(cluster: &ClusterModel) -> f64 {
    let mut counts = Vec::new();
    for broker in cluster.brokers.values() {
        counts.push(cluster.replicas_on_broker(broker.id).len() as f64);
    }
    
    let mean = counts.iter().sum::<f64>() / counts.len() as f64;
    let variance = counts.iter()
        .map(|c| (c - mean).powi(2))
        .sum::<f64>() / counts.len() as f64;
    
    variance.sqrt() / mean // Coefficient of variation
}

fn create_sample_cluster() -> ClusterModel {
    let mut cluster = ClusterModel::new();

    // Add brokers with different capacities and racks
    // Brokers 0, 1 in rack-0
    // Broker 2 in rack-1  
    // Brokers 3, 4 in rack-2
    for i in 0..5 {
        let rack = match i {
            0 | 1 => "rack-0".to_string(),
            2 => "rack-1".to_string(),
            3 | 4 => "rack-2".to_string(),
            _ => format!("rack-{}", i % 3),
        };
        let broker = Broker::new(
            i,
            Some(rack),
            ResourceCapacity {
                cpu: 16.0,
                disk_mb: 1_000_000, // 1 TB
                network_in_mbps: 1000.0,
                network_out_mbps: 1000.0,
            },
        );
        cluster.add_broker(broker);
    }

    // Create IMBALANCED distribution with rack violations
    // Strategy: 
    // 1. Put most replicas on brokers 0 and 1 (replica count imbalance)
    // 2. Put multiple replicas in same rack (rack-aware violations)
    
    for topic_idx in 0..5 {
        let topic_name = format!("topic-{}", topic_idx);
        let mut topic = Topic {
            name: topic_name.clone(),
            partitions: std::collections::HashMap::new(),
            replication_factor: 3,
        };

        for partition_idx in 0..12 {
            // Create partitions with:
            // - Replica count imbalance (favor brokers 0-2)
            // - Rack violations (put 2+ replicas in same rack for some partitions)
            let (broker1, broker2, broker3) = if partition_idx % 3 == 0 {
                // Every 3rd partition: violate rack-aware (2 replicas in rack-0)
                (0, 1, 2)  // brokers 0,1 both in rack-0!
            } else if partition_idx % 3 == 1 {
                // Heavily load broker 0
                (0, 2, 3)
            } else {
                // Heavily load broker 1 and 2
                (1, 2, 4)
            };
            
            let partition = Partition {
                topic: topic_name.clone(),
                id: partition_idx,
                replicas: vec![
                    Replica {
                        broker_id: broker1,
                        is_leader: true,
                        is_in_sync: true,
                        resource_usage: ResourceUsage {
                            cpu: 0.5,
                            disk_mb: 10_000,
                            network_in_mbps: 10.0,
                            network_out_mbps: 10.0,
                        },
                        size_mb: 10_000,
                    },
                    Replica {
                        broker_id: broker2,
                        is_leader: false,
                        is_in_sync: true,
                        resource_usage: ResourceUsage {
                            cpu: 0.2,
                            disk_mb: 10_000,
                            network_in_mbps: 10.0,
                            network_out_mbps: 5.0,
                        },
                        size_mb: 10_000,
                    },
                    Replica {
                        broker_id: broker3,
                        is_leader: false,
                        is_in_sync: true,
                        resource_usage: ResourceUsage {
                            cpu: 0.2,
                            disk_mb: 10_000,
                            network_in_mbps: 10.0,
                            network_out_mbps: 5.0,
                        },
                        size_mb: 10_000,
                    },
                ],
                leader: Some(broker1),
            };

            topic.partitions.insert(partition_idx, partition);
        }

        cluster.topics.insert(topic_name, topic);
    }

    // Print initial distribution
    println!("=== Initial Cluster State ===");
    print_replica_distribution(&cluster);
    print_rack_distribution(&cluster);
    print_cluster_tree(&cluster);

    cluster
}

fn print_replica_distribution(cluster: &ClusterModel) {
    let mut broker_counts: std::collections::HashMap<u32, usize> = 
        std::collections::HashMap::new();
    
    for broker in cluster.brokers.values() {
        let count = cluster.replicas_on_broker(broker.id).len();
        broker_counts.insert(broker.id, count);
    }
    
    let total: usize = broker_counts.values().sum();
    let avg = total as f64 / broker_counts.len() as f64;
    
    println!("\nReplica distribution:");
    for broker_id in 0..5 {
        let count = broker_counts.get(&broker_id).unwrap_or(&0);
        let diff = *count as f64 - avg;
        let broker = cluster.get_broker(broker_id).unwrap();
        let rack = broker.rack.as_ref().unwrap();
        println!(
            "  Broker {} ({}): {} replicas ({:+.1} from average of {:.1})",
            broker_id, rack, count, diff, avg
        );
    }
    println!();
}

fn print_rack_distribution(cluster: &ClusterModel) {
    println!("Rack violations:");
    let mut violation_count = 0;
    
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
            
            for (rack, count) in rack_counts {
                if count > 1 {
                    println!(
                        "  {}/{}: {} replicas in {}",
                        partition.topic, partition.id, count, rack
                    );
                    violation_count += 1;
                }
            }
        }
    }
    
    if violation_count == 0 {
        println!("  None - all partitions are rack-aware ✓");
    } else {
        println!("\nTotal: {} rack violations", violation_count);
    }
    println!();
}

fn print_cluster_tree(cluster: &ClusterModel) {
    println!("Cluster Structure:");
    for broker in cluster.brokers.values() {
        let rack = broker.rack.as_deref().unwrap_or("unknown");
        println!("Broker {} ({}):", broker.id, rack);

        // Iterate through all partitions and find ones with replicas on this broker
        for topic in cluster.topics.values() {
            for partition in topic.partitions.values() {
                for replica in &partition.replicas {
                    if replica.broker_id == broker.id {
                        println!(
                            "  - {}/{} (leader: {})",
                            partition.topic,
                            partition.id,
                            replica.is_leader
                        );
                    }
                }
            }
        }
    }
    println!();
}

fn print_partition_movements(plan: &actions::RebalancePlan) {
    println!("\n=== Partition Movements ===");

    let mut move_actions: Vec<_> = plan.actions.iter()
        .filter_map(|action| {
            if let actions::Action::MoveReplica { topic, partition, from_broker, to_broker, .. } = action {
                Some((topic, partition, from_broker, to_broker))
            } else {
                None
            }
        })
        .collect();

    if move_actions.is_empty() {
        println!("No partition movements");
        return;
    }

    // Sort by topic, then partition for cleaner output
    move_actions.sort_by(|a, b| {
        a.0.cmp(b.0).then(a.1.cmp(b.1))
    });

    for (topic, partition, from_broker, to_broker) in move_actions {
        println!("  {}/{}: {} -> {}", topic, partition, from_broker, to_broker);
    }

    println!();
}
