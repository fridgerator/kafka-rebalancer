use serde::{Deserialize, Serialize};

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
