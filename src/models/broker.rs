use serde::{Deserialize, Serialize};
use super::{BrokerId, ResourceCapacity, ResourceUsage, ResourceUtilization};

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
