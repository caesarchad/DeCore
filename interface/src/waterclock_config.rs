use crate::timing::DEFAULT_NUM_TICKS_PER_SECOND;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WaterClockConfig {
    pub target_tick_duration: Duration,
    pub hashes_per_tick: Option<u64>,
}

impl WaterClockConfig {
    pub fn new_sleep(target_tick_duration: Duration) -> Self {
        Self {
            target_tick_duration,
            hashes_per_tick: None,
        }
    }
}

impl Default for WaterClockConfig {
    fn default() -> Self {
        Self::new_sleep(Duration::from_millis(1000 / DEFAULT_NUM_TICKS_PER_SECOND))
    }
}
