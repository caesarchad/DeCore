use crate::constants::DEFAULT_NUM_DROPS_PER_SECOND;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WaterClockConfig {
    pub target_drop_duration: Duration,
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
