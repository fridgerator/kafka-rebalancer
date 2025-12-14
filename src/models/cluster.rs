use std::collections::HashMap;
use super::{BrokerId, TopicName, PartitionId, Broker, Topic, Partition, Replica, ResourceUsage};

/// Represents the current state of a Kafka cluster
#[derive(Debug, Clone)]
pub struct ClusterModel {
    pub brokers: HashMap<BrokerId, Broker>,
    pub topics: HashMap<TopicName, Topic>,
    pub rack_mapping: HashMap<String, Vec<BrokerId>>,
}

impl ClusterModel {
    pub fn new() -> Self {
        Self {
            brokers: HashMap::new(),
            topics: HashMap::new(),
            rack_mapping: HashMap::new(),
        }
    }

    pub fn add_broker(&mut self, broker: Broker) {
        let rack = broker.rack.clone();
        let broker_id = broker.id;
        self.brokers.insert(broker_id, broker);

        // Update rack mapping
        if let Some(rack_name) = rack {
            self.rack_mapping
                .entry(rack_name)
                .or_insert_with(Vec::new)
                .push(broker_id);
        }
    }

    pub fn get_broker(&self, id: BrokerId) -> Option<&Broker> {
        self.brokers.get(&id)
    }

    pub fn get_broker_mut(&mut self, id: BrokerId) -> Option<&mut Broker> {
        self.brokers.get_mut(&id)
    }

    pub fn alive_brokers(&self) -> impl Iterator<Item = &Broker> {
        self.brokers.values().filter(|b| b.is_alive)
    }

    pub fn dead_brokers(&self) -> impl Iterator<Item = &Broker> {
        self.brokers.values().filter(|b| !b.is_alive)
    }

    /// Get all partitions across all topics
    pub fn all_partitions(&self) -> impl Iterator<Item = &Partition> + '_ {
        self.topics.values().flat_map(|t| t.partitions.values())
    }

    /// Calculate total resource usage across cluster
    pub fn total_resource_usage(&self) -> ResourceUsage {
        self.brokers
            .values()
            .map(|b| b.resource_usage.clone())
            .sum()
    }

    /// Get replicas on a specific broker
    pub fn replicas_on_broker(&self, broker_id: BrokerId) -> Vec<&Replica> {
        self.all_partitions()
            .flat_map(|p| &p.replicas)
            .filter(|r| r.broker_id == broker_id)
            .collect()
    }

    /// Simulate moving a replica
    pub fn simulate_replica_move(
        &self,
        topic: &str,
        partition_id: PartitionId,
        from_broker: BrokerId,
        to_broker: BrokerId,
    ) -> Option<ClusterModel> {
        let mut clone = self.clone();

        // Get the partition
        let topic_obj = clone.topics.get_mut(topic)?;
        let partition = topic_obj.partitions.get_mut(&partition_id)?;

        // Find and move the replica
        let replica_idx = partition
            .replicas
            .iter()
            .position(|r| r.broker_id == from_broker)?;

        partition.replicas[replica_idx].broker_id = to_broker;

        // Update broker resource usage
        let resource_usage = partition.replicas[replica_idx].resource_usage.clone();
        clone.update_broker_usage(from_broker, &resource_usage, false);
        clone.update_broker_usage(to_broker, &resource_usage, true);

        Some(clone)
    }

    fn update_broker_usage(&mut self, broker_id: BrokerId, usage: &ResourceUsage, add: bool) {
        if let Some(broker) = self.brokers.get_mut(&broker_id) {
            if add {
                broker.resource_usage = broker.resource_usage.clone() + usage.clone();
            } else {
                broker.resource_usage = broker.resource_usage.clone() - usage.clone();
            }
        }
    }
}
