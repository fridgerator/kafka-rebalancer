use std::collections::{HashMap};
use serde::{Deserialize, Serialize};

pub type BrokerId = u32;
pub type TopicName = String;
pub type PartitionId = u32;
pub type ReplicaId = u64; // Composite of topic + partition + broker

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Broker {
    pub id: BrokerId,
    pub rack: Option<String>,
    pub is_alive: bool,
    pub capacity: ResourceCapacity,
    pub resource_usage: ResourceUsage,
}

impl Broker {
    pub fn new(id: BrokerId, rack: Option<String>, capacity: ResourceCapacity) -> Self {
        Self {
            id,
            rack,
            is_alive: true,
            capacity,
            resource_usage: ResourceUsage::default(),
        }
    }

    /// Calculate utilization percentage for each resource
    pub fn utilization(&self) -> ResourceUtilization {
        ResourceUtilization {
            cpu: self.resource_usage.cpu / self.capacity.cpu,
            disk: self.resource_usage.disk_mb as f64 / self.capacity.disk_mb as f64,
            network_in: self.resource_usage.network_in_mbps / self.capacity.network_in_mbps,
            network_out: self.resource_usage.network_out_mbps / self.capacity.network_out_mbps,
        }
    }

    /// Check if adding this usage would exceed capacity
    pub fn can_accommodate(&self, additional_usage: &ResourceUsage) -> bool {
        let new_usage = self.resource_usage.clone() + additional_usage.clone();
        
        new_usage.cpu <= self.capacity.cpu
            && new_usage.disk_mb <= self.capacity.disk_mb
            && new_usage.network_in_mbps <= self.capacity.network_in_mbps
            && new_usage.network_out_mbps <= self.capacity.network_out_mbps
    }

    pub fn replica_count(&self) -> usize {
        // This would be calculated from the cluster model
        0 // Placeholder
    }

    pub fn leader_count(&self) -> usize {
        // This would be calculated from the cluster model
        0 // Placeholder
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    pub name: TopicName,
    pub partitions: HashMap<PartitionId, Partition>,
    pub replication_factor: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub topic: TopicName,
    pub id: PartitionId,
    pub replicas: Vec<Replica>,
    pub leader: Option<BrokerId>,
}

impl Partition {
    pub fn is_under_replicated(&self, expected_rf: u32) -> bool {
        self.replicas.len() < expected_rf as usize
    }

    pub fn in_sync_replicas(&self) -> impl Iterator<Item = &Replica> {
        self.replicas.iter().filter(|r| r.is_in_sync)
    }

    pub fn out_of_sync_replicas(&self) -> impl Iterator<Item = &Replica> {
        self.replicas.iter().filter(|r| !r.is_in_sync)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Replica {
    pub broker_id: BrokerId,
    pub is_leader: bool,
    pub is_in_sync: bool,
    pub resource_usage: ResourceUsage,
    pub size_mb: u64,
}

/// Resource capacity for a broker
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ResourceCapacity {
    pub cpu: f64,              // CPU cores
    pub disk_mb: u64,          // Disk space in MB
    pub network_in_mbps: f64,  // Network inbound bandwidth
    pub network_out_mbps: f64, // Network outbound bandwidth
}

/// Current resource usage
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub cpu: f64,
    pub disk_mb: u64,
    pub network_in_mbps: f64,
    pub network_out_mbps: f64,
}

impl std::ops::Add for ResourceUsage {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            cpu: self.cpu + other.cpu,
            disk_mb: self.disk_mb + other.disk_mb,
            network_in_mbps: self.network_in_mbps + other.network_in_mbps,
            network_out_mbps: self.network_out_mbps + other.network_out_mbps,
        }
    }
}

impl std::ops::Sub for ResourceUsage {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self {
            cpu: self.cpu - other.cpu,
            disk_mb: self.disk_mb.saturating_sub(other.disk_mb),
            network_in_mbps: (self.network_in_mbps - other.network_in_mbps).max(0.0),
            network_out_mbps: (self.network_out_mbps - other.network_out_mbps).max(0.0),
        }
    }
}

impl std::iter::Sum for ResourceUsage {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(ResourceUsage::default(), |acc, usage| acc + usage)
    }
}

/// Resource utilization as percentages (0.0 to 1.0)
#[derive(Debug, Clone, Copy)]
pub struct ResourceUtilization {
    pub cpu: f64,
    pub disk: f64,
    pub network_in: f64,
    pub network_out: f64,
}

impl ResourceUtilization {
    pub fn max_utilization(&self) -> f64 {
        self.cpu
            .max(self.disk)
            .max(self.network_in)
            .max(self.network_out)
    }

    pub fn is_over_threshold(&self, threshold: f64) -> bool {
        self.max_utilization() > threshold
    }
}