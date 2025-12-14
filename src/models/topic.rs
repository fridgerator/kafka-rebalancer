use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use super::{TopicName, PartitionId, BrokerId, Replica};

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
