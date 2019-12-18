use crate::timing::DEFAULT_NUM_DROPS_PER_SECOND;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WaterClockConfig {
    /// The target _drop rate of the cluster.
    pub target_drop_duration: Duration,

    /// How many hashes to roll before emitting the next _drop entry.
    /// None enables "Low power mode", which implies:
    /// * sleep for `target_drop_duration` instead of hashing
    /// * the number of hashes per _drop will be variable
    pub hashes_per_drop: Option<u64>,
}

impl WaterClockConfig {
    pub fn new_sleep(target_drop_duration: Duration) -> Self {
        Self {
            target_drop_duration,
            hashes_per_drop: None,
        }
    }
}

impl Default for WaterClockConfig {
    fn default() -> Self {
        Self::new_sleep(Duration::from_millis(1000 / DEFAULT_NUM_DROPS_PER_SECOND))
    }
}
