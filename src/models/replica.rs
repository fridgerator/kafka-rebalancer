use serde::{Deserialize, Serialize};
use super::{BrokerId, ResourceUsage};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Replica {
    pub broker_id: BrokerId,
    pub is_leader: bool,
    pub is_in_sync: bool,
    pub resource_usage: ResourceUsage,
    pub size_mb: u64,
}
