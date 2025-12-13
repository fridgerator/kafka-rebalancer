# Kafka Partition Rebalancer

A Rust library for intelligently rebalancing Kafka partitions across brokers, inspired by LinkedIn's Cruise Control.

## Overview

This library provides a flexible framework for optimizing Kafka cluster partition placement based on multiple configurable goals. It generates rebalance plans that can be executed to improve cluster balance, resource utilization, and fault tolerance.

## Features

- **Multi-Goal Optimization**: Prioritize and combine multiple optimization goals
- **Rack Awareness**: Ensure replicas are distributed across failure domains
- **Resource-Based Balancing**: Balance CPU, disk, and network utilization
- **Constraint Support**: Control rebalancing behavior with fine-grained constraints
- **Simulation**: Test rebalance plans before execution
- **Action Batching**: Group actions for efficient concurrent execution
- **Custom Goals**: Easily implement your own optimization goals

## Architecture

The library is organized into several key components:

### Core Types

- **ClusterModel**: Represents the current state of a Kafka cluster
- **Broker**: Individual broker with capacity and resource usage
- **Partition**: Topic partition with replicas
- **Replica**: Individual replica with location and state

### Goals

Goals define optimization objectives. The library includes several built-in goals:

- `RackAwareGoal`: Ensures rack-aware replica placement (Critical priority)
- `DiskCapacityGoal`: Prevents disk capacity violations (High priority)
- `ReplicaDistributionGoal`: Balances replica count across brokers (Medium priority)
- `LeaderDistributionGoal`: Balances leader count across brokers (Medium priority)
- `PreferredLeaderElectionGoal`: Elects preferred leaders (Low priority)

### Optimizer

The optimizer coordinates multiple goals to generate optimal rebalance plans. It:
1. Evaluates each goal in priority order
2. Generates actions to satisfy hard goals (Critical priority)
3. Improves soft goals while maintaining hard goal compliance
4. Filters actions based on constraints
5. Orders actions according to execution strategy

### Actions

Actions represent concrete operations:
- `MoveReplica`: Move a replica between brokers
- `ElectLeader`: Change partition leader
- `AddReplica`: Increase replication factor
- `RemoveReplica`: Decrease replication factor

## Usage Examples

### Basic Rebalancing

```rust
use kafka_rebalancer::*;

fn main() {
    // Create cluster model from your Kafka metadata
    let mut cluster = ClusterModel::new();
    
    // Add brokers
    cluster.add_broker(Broker::new(
        0,
        Some("rack-1".to_string()),
        ResourceCapacity {
            cpu: 16.0,
            disk_mb: 1_000_000,
            network_in_mbps: 1000.0,
            network_out_mbps: 1000.0,
        },
    ));
    
    // ... add more brokers, topics, partitions
    
    // Define goals
    let goals: Vec<Box<dyn Goal>> = vec![
        Box::new(goals::RackAwareGoal),
        Box::new(goals::DiskCapacityGoal { threshold: 0.8 }),
        Box::new(goals::ReplicaDistributionGoal { allowed_variance: 0.1 }),
    ];
    
    // Create rebalancer
    let rebalancer = Rebalancer::new(goals);
    
    // Generate plan
    let constraints = BalancingConstraints::default();
    let plan = rebalancer.generate_plan(&cluster, &constraints)?;
    
    // Execute plan (you implement the execution)
    for action in plan.actions {
        println!("{}", action.description());
        // execute_action(action);
    }
}
```

### Broker Decommissioning

```rust
let brokers_to_remove = vec![3, 4];
let constraints = BalancingConstraints::for_decommission(brokers_to_remove);

let plan = rebalancer.generate_plan(&cluster, &constraints)?;
```

### Adding New Brokers

```rust
let constraints = BalancingConstraints::for_broker_addition();
let plan = rebalancer.generate_plan(&cluster, &constraints)?;
```

### Leader-Only Rebalancing

```rust
let constraints = BalancingConstraints::leader_election_only();
let plan = rebalancer.generate_plan(&cluster, &constraints)?;
```

### Batched Execution

```rust
let plan = rebalancer.generate_plan(&cluster, &constraints)?;

// Group actions for concurrent execution
let batches = plan.batch_actions(10); // Max 10 concurrent

for batch in batches {
    // Execute batch concurrently
    for action in batch {
        // spawn_execution_task(action);
    }
    // wait_for_batch_completion();
}
```

## Custom Goals

Implement the `Goal` trait to create custom optimization goals:

```rust
use kafka_rebalancer::*;

pub struct CustomGoal {
    // Your goal parameters
}

impl Goal for CustomGoal {
    fn name(&self) -> &str {
        "CustomGoal"
    }
    
    fn priority(&self) -> GoalPriority {
        GoalPriority::Medium
    }
    
    fn check(&self, cluster: &ClusterModel) -> Vec<GoalViolation> {
        // Detect violations
        vec![]
    }
    
    fn score(&self, cluster: &ClusterModel) -> f64 {
        // Calculate 0.0-1.0 score
        1.0
    }
    
    fn optimize(&self, cluster: &ClusterModel) -> OptimizationResult {
        // Generate actions to optimize
        OptimizationResult {
            goal_name: self.name().to_string(),
            violations: vec![],
            proposed_actions: vec![],
            score: 1.0,
        }
    }
}
```

## Constraints

Control rebalancing behavior with constraints:

```rust
let mut constraints = BalancingConstraints::default();

// Limit concurrent operations
constraints.max_concurrent_partition_movements = 5;
constraints.max_concurrent_leader_elections = 50;

// Exclude brokers
constraints.excluded_brokers.insert(5);

// Limit data transfer
constraints.max_total_data_transfer_mb = 50_000; // 50 GB

// Resource thresholds
constraints.resource_thresholds = ResourceThresholds {
    cpu: 0.75,
    disk: 0.85,
    network_in: 0.80,
    network_out: 0.80,
    low_utilization: 0.20,
};
```

## Goal Priority System

Goals are executed in priority order:
1. **Critical** (Hard Goals): Must be satisfied, optimization fails if violated
2. **High**: Important goals, optimization makes best effort
3. **Medium**: Standard optimization goals
4. **Low**: Nice-to-have improvements

## Action Ordering Strategies

Choose how actions are ordered:

```rust
use kafka_rebalancer::actions::ActionOrderingStrategy;

let optimizer = Optimizer::new(goals)
    .with_ordering_strategy(ActionOrderingStrategy::SmallReplicasFirst);
```

Available strategies:
- `Sequential`: Actions in generation order
- `SmallReplicasFirst`: Minimize data transfer time
- `LargeReplicasFirst`: Clear big items first
- `UnderMinIsrFirst`: Prioritize at-risk partitions
- `MinimizeLeaderMovement`: Reduce leader churn

## Performance Considerations

- **Cluster Size**: Optimizes clusters with hundreds of brokers and thousands of partitions
- **Goal Count**: More goals increase computation time linearly
- **Action Limit**: Set `max_actions_per_plan` to cap optimization time
- **Simulation**: Each action is simulated to predict impact

## Integration with Kafka

This library provides the optimization logic only. You need to:

1. **Collect Cluster State**: Query Kafka metadata to build `ClusterModel`
2. **Execute Actions**: Use Kafka Admin API to execute generated actions
3. **Monitor Progress**: Track action execution and handle failures
4. **Throttle Execution**: Implement rate limiting per your cluster capacity

Example integration:

```rust
use kafka_rebalancer::*;
use rdkafka::admin::AdminClient;

async fn rebalance_cluster(admin: &AdminClient) -> Result<(), Box<dyn Error>> {
    // 1. Build cluster model from Kafka metadata
    let cluster = build_cluster_model(admin).await?;
    
    // 2. Generate rebalance plan
    let goals = create_goals();
    let rebalancer = Rebalancer::new(goals);
    let plan = rebalancer.generate_plan(&cluster, &BalancingConstraints::default())?;
    
    // 3. Execute actions
    for action in plan.actions {
        execute_action(admin, action).await?;
    }
    
    Ok(())
}
```

## Testing

Run tests:
```bash
cargo test
```

Run examples:
```bash
cargo run --example basic_usage
cargo run --example decommission_broker
```

## Contributing

Contributions welcome! Areas for improvement:
- Additional built-in goals (e.g., topic-level constraints)
- More sophisticated action batching algorithms
- Performance optimizations for large clusters
- Integration examples with popular Kafka libraries

## License

MIT OR Apache-2.0

## Inspiration

This library is inspired by [LinkedIn's Cruise Control](https://github.com/linkedin/cruise-control), 
reimagined in Rust with a focus on type safety, performance, and modularity.

## Differences from Cruise Control

- **Language**: Rust instead of Java
- **Scope**: Core rebalancing logic only (no metrics collection, REST API, or execution)
- **Design**: More modular with clear separation of concerns
- **Goals**: Simplified goal system, easier to extend
- **No Runtime**: Library-only, you control the execution environment

## Future Work

- Async/await support for concurrent optimization
- Prometheus metrics integration
- Cost estimation improvements
- Multi-cluster support
- Rack topology constraints
- Topic retention-based goals