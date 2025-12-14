// Type aliases used across models
pub type BrokerId = u32;
pub type TopicName = String;
pub type PartitionId = u32;
pub type ReplicaId = u64; // Composite of topic + partition + broker

// Module declarations
mod resources;
mod broker;
mod replica;
mod topic;
mod cluster;

// Re-exports
pub use resources::{ResourceCapacity, ResourceUsage, ResourceUtilization};
pub use broker::Broker;
pub use replica::Replica;
pub use topic::{Topic, Partition};
pub use cluster::ClusterModel;
