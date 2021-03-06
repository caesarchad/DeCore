use crate::leader_arrange::LeaderSchedule;
use crate::staking_utils;
use morgan_runtime::treasury::Treasury;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::constants::NUM_CONSECUTIVE_LEADER_SLOTS;
use proptest::{
    strategy::{Strategy, ValueTree},
    test_runner::{Config, TestRunner},
};

/// Return the leader schedule for the given epoch.
pub fn leader_schedule(epoch_height: u64, treasury: &Treasury) -> Option<LeaderSchedule> {
    staking_utils::staked_nodes_at_epoch(treasury, epoch_height).map(|stakes| {
        let mut seed = [0u8; 32];
        seed[0..8].copy_from_slice(&epoch_height.to_le_bytes());
        let mut stakes: Vec<_> = stakes.into_iter().collect();
        sort_stakes(&mut stakes);
        LeaderSchedule::new(
            &stakes,
            seed,
            treasury.get_slots_in_epoch(epoch_height),
            NUM_CONSECUTIVE_LEADER_SLOTS,
        )
    })
}

/// Return the leader for the given slot.
pub fn slot_leader_at(slot: u64, treasury: &Treasury) -> Option<BvmAddr> {
    let (epoch, slot_index) = treasury.get_epoch_and_slot_index(slot);

    leader_schedule(epoch, treasury).map(|leader_schedule| leader_schedule[slot_index])
}

// Returns the number of drops remaining from the specified drop_height to the end of the
// slot implied by the drop_height
pub fn num_drops_left_in_slot(treasury: &Treasury, drop_height: u64) -> u64 {
    treasury.drops_per_slot() - drop_height % treasury.drops_per_slot() - 1
}

pub fn drop_height_to_slot(drops_per_slot: u64, drop_height: u64) -> u64 {
    drop_height / drops_per_slot
}

/// Context for generating single values out of strategies.
///
/// Proptest is designed to be built around "value trees", which represent a spectrum from complex
/// values to simpler ones. But in some contexts, like benchmarking or generating corpuses, one just
/// wants a single value. This is a convenience struct for that.
pub struct ValueGenerator {
    runner: TestRunner,
}

impl ValueGenerator {
    /// Creates a new value generator with the default RNG.
    pub fn new() -> Self {
        Self {
            runner: TestRunner::new(Config::default()),
        }
    }

    /// Creates a new value generator with a deterministic RNG.
    ///
    /// This generator has a hardcoded seed, so its results are predictable across test runs.
    /// However, a new proptest version may change the seed.
    pub fn deterministic() -> Self {
        Self {
            runner: TestRunner::deterministic(),
        }
    }

    /// Generates a single value for this strategy.
    ///
    /// Panics if generating the new value fails. The only situation in which this can happen is if
    /// generating the value causes too many internal rejects.
    pub fn generate<S: Strategy>(&mut self, strategy: S) -> S::Value {
        strategy
            .new_tree(&mut self.runner)
            .expect("creating a new value should succeed")
            .current()
    }
}

fn sort_stakes(stakes: &mut Vec<(BvmAddr, u64)>) {
    // Sort first by stake. If stakes are the same, sort by address to ensure a
    // deterministic result.
    // Note: Use unstable sort, because we dedup right after to remove the equal elements.
    stakes.sort_unstable_by(|(l_address, l_stake), (r_address, r_stake)| {
        if r_stake == l_stake {
            r_address.cmp(&l_address)
        } else {
            r_stake.cmp(&l_stake)
        }
    });

    // Now that it's sorted, we can do an O(n) dedup.
    stakes.dedup();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::staking_utils;
    use morgan_runtime::genesis_utils::{
        create_genesis_block_with_leader, BOOTSTRAP_LEADER_DIFS,
    };

    #[test]
    fn test_leader_schedule_via_treasury() {
        let address = BvmAddr::new_rand();
        let genesis_block =
            create_genesis_block_with_leader(0, &address, BOOTSTRAP_LEADER_DIFS).genesis_block;
        let treasury = Treasury::new(&genesis_block);

        let addresss_and_stakes: Vec<_> = staking_utils::staked_nodes(&treasury).into_iter().collect();
        let seed = [0u8; 32];
        let leader_schedule = LeaderSchedule::new(
            &addresss_and_stakes,
            seed,
            genesis_block.candidate_each_round,
            NUM_CONSECUTIVE_LEADER_SLOTS,
        );

        assert_eq!(leader_schedule[0], address);
        assert_eq!(leader_schedule[1], address);
        assert_eq!(leader_schedule[2], address);
    }

    #[test]
    fn test_leader_scheduler1_basic() {
        let address = BvmAddr::new_rand();
        let genesis_block = create_genesis_block_with_leader(
            BOOTSTRAP_LEADER_DIFS,
            &address,
            BOOTSTRAP_LEADER_DIFS,
        )
        .genesis_block;
        let treasury = Treasury::new(&genesis_block);
        assert_eq!(slot_leader_at(treasury.slot(), &treasury).unwrap(), address);
    }

    #[test]
    fn test_sort_stakes_basic() {
        let address0 = BvmAddr::new_rand();
        let address1 = BvmAddr::new_rand();
        let mut stakes = vec![(address0, 1), (address1, 2)];
        sort_stakes(&mut stakes);
        assert_eq!(stakes, vec![(address1, 2), (address0, 1)]);
    }

    #[test]
    fn test_sort_stakes_with_dup() {
        let address0 = BvmAddr::new_rand();
        let address1 = BvmAddr::new_rand();
        let mut stakes = vec![(address0, 1), (address1, 2), (address0, 1)];
        sort_stakes(&mut stakes);
        assert_eq!(stakes, vec![(address1, 2), (address0, 1)]);
    }

    #[test]
    fn test_sort_stakes_with_equal_stakes() {
        let address0 = BvmAddr::default();
        let address1 = BvmAddr::new_rand();
        let mut stakes = vec![(address0, 1), (address1, 1)];
        sort_stakes(&mut stakes);
        assert_eq!(stakes, vec![(address1, 1), (address0, 1)]);
    }
}
