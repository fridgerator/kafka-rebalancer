use crate::model::BrokerId;
use chrono::Timelike;
use chrono::Datelike;
use std::collections::HashSet;

/// Constraints that control how optimization is performed
#[derive(Debug, Clone)]
pub struct BalancingConstraints {
    /// Maximum number of concurrent partition movements
    pub max_concurrent_partition_movements: usize,

    /// Maximum number of concurrent leader elections
    pub max_concurrent_leader_elections: usize,

    /// Maximum network bandwidth to use for data transfer (MB/s)
    pub max_network_bandwidth_mbps: f64,

    /// Brokers to exclude from receiving new replicas
    pub excluded_brokers: HashSet<BrokerId>,

    /// Brokers being decommissioned (all replicas must be moved off)
    pub brokers_to_remove: HashSet<BrokerId>,

    /// Whether to allow leadership changes
    pub allow_leadership_changes: bool,

    /// Whether to allow replica movements
    pub allow_replica_movements: bool,

    /// Minimum time between actions on the same partition (seconds)
    pub min_action_interval_secs: u64,

    /// Maximum total actions in a single plan
    pub max_actions_per_plan: usize,

    /// Resource utilization thresholds
    pub resource_thresholds: ResourceThresholds,

    /// Topics to exclude from rebalancing
    pub excluded_topics: HashSet<String>,

    /// Maximum data to transfer in a single plan (MB)
    pub max_total_data_transfer_mb: u64,
}

impl Default for BalancingConstraints {
    fn default() -> Self {
        Self {
            max_concurrent_partition_movements: 10,
            max_concurrent_leader_elections: 100,
            max_network_bandwidth_mbps: 100.0,
            excluded_brokers: HashSet::new(),
            brokers_to_remove: HashSet::new(),
            allow_leadership_changes: true,
            allow_replica_movements: true,
            min_action_interval_secs: 30,
            max_actions_per_plan: 1000,
            resource_thresholds: ResourceThresholds::default(),
            excluded_topics: HashSet::new(),
            max_total_data_transfer_mb: 300_000, // 300 GB
        }
    }
}

impl BalancingConstraints {
    /// Create constraints for a decommission operation
    pub fn for_decommission(broker_ids: Vec<BrokerId>) -> Self {
        Self {
            brokers_to_remove: broker_ids.into_iter().collect(),
            allow_leadership_changes: true,
            allow_replica_movements: true,
            ..Default::default()
        }
    }

    /// Create constraints for adding new brokers
    pub fn for_broker_addition() -> Self {
        Self {
            allow_replica_movements: true,
            allow_leadership_changes: false, // Don't move leaders unnecessarily
            ..Default::default()
        }
    }

    /// Create constraints for preferred leader election only
    pub fn leader_election_only() -> Self {
        Self {
            allow_replica_movements: false,
            allow_leadership_changes: true,
            max_concurrent_leader_elections: 1000,
            ..Default::default()
        }
    }

    /// Create constraints for fixing broker failures
    pub fn for_broker_failure(failed_broker_ids: Vec<BrokerId>) -> Self {
        Self {
            excluded_brokers: failed_broker_ids.into_iter().collect(),
            allow_replica_movements: true,
            allow_leadership_changes: true,
            max_actions_per_plan: 10000, // May need many actions
            ..Default::default()
        }
    }

    /// Check if a broker can receive new replicas
    pub fn can_add_replica_to_broker(&self, broker_id: BrokerId) -> bool {
        !self.excluded_brokers.contains(&broker_id)
            && !self.brokers_to_remove.contains(&broker_id)
    }

    /// Check if leadership changes are allowed
    pub fn can_change_leadership(&self) -> bool {
        self.allow_leadership_changes
    }

    /// Check if replica movements are allowed
    pub fn can_move_replicas(&self) -> bool {
        self.allow_replica_movements
    }

    /// Check if a topic can be rebalanced
    pub fn can_rebalance_topic(&self, topic: &str) -> bool {
        !self.excluded_topics.contains(topic)
    }
}

/// Resource utilization thresholds for optimization
#[derive(Debug, Clone, Copy)]
pub struct ResourceThresholds {
    /// CPU utilization threshold (0.0 to 1.0)
    pub cpu: f64,

    /// Disk utilization threshold (0.0 to 1.0)
    pub disk: f64,

    /// Network inbound utilization threshold (0.0 to 1.0)
    pub network_in: f64,

    /// Network outbound utilization threshold (0.0 to 1.0)
    pub network_out: f64,

    /// Low utilization threshold for identifying underused brokers
    pub low_utilization: f64,
}

impl Default for ResourceThresholds {
    fn default() -> Self {
        Self {
            cpu: 0.8,       // 80%
            disk: 0.8,      // 80%
            network_in: 0.8, // 80%
            network_out: 0.8, // 80%
            low_utilization: 0.2, // 20%
        }
    }
}

impl ResourceThresholds {
    /// Create aggressive thresholds (tighter limits)
    pub fn aggressive() -> Self {
        Self {
            cpu: 0.7,
            disk: 0.7,
            network_in: 0.7,
            network_out: 0.7,
            low_utilization: 0.3,
        }
    }

    /// Create relaxed thresholds (looser limits)
    pub fn relaxed() -> Self {
        Self {
            cpu: 0.9,
            disk: 0.9,
            network_in: 0.9,
            network_out: 0.9,
            low_utilization: 0.1,
        }
    }
}

/// Time-based constraints for throttling rebalance operations
#[derive(Debug, Clone)]
pub struct ExecutionConstraints {
    /// Allowed time windows for execution
    pub allowed_windows: Vec<TimeWindow>,

    /// Maximum duration for a single execution batch
    pub max_batch_duration_secs: u64,

    /// Pause duration between batches
    pub inter_batch_pause_secs: u64,
}

impl Default for ExecutionConstraints {
    fn default() -> Self {
        Self {
            allowed_windows: vec![TimeWindow::always()],
            max_batch_duration_secs: 3600, // 1 hour
            inter_batch_pause_secs: 60,    // 1 minute
        }
    }
}

/// Represents a time window when operations are allowed
#[derive(Debug, Clone)]
pub struct TimeWindow {
    /// Day of week (0 = Sunday, 6 = Saturday)
    pub day_of_week: Option<u8>,

    /// Start hour (0-23)
    pub start_hour: u8,

    /// End hour (0-23)
    pub end_hour: u8,
}

impl TimeWindow {
    /// Create a time window that's always allowed
    pub fn always() -> Self {
        Self {
            day_of_week: None,
            start_hour: 0,
            end_hour: 23,
        }
    }

    /// Create a time window for weekday business hours
    pub fn business_hours() -> Self {
        Self {
            day_of_week: None, // Any weekday
            start_hour: 9,
            end_hour: 17,
        }
    }

    /// Create a time window for off-peak hours
    pub fn off_peak() -> Self {
        Self {
            day_of_week: None,
            start_hour: 22, // 10 PM
            end_hour: 6,    // 6 AM
        }
    }

    /// Check if the current time is within this window
    pub fn is_now_in_window(&self) -> bool {
        let now = chrono::Local::now();
        let current_hour = now.hour() as u8;
        let current_day = now.weekday().num_days_from_sunday() as u8;

        let hour_match = if self.start_hour <= self.end_hour {
            current_hour >= self.start_hour && current_hour <= self.end_hour
        } else {
            // Window spans midnight
            current_hour >= self.start_hour || current_hour <= self.end_hour
        };

        let day_match = self
            .day_of_week
            .map(|d| d == current_day)
            .unwrap_or(true);

        hour_match && day_match
    }
}