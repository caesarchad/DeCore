use crate::leader_arrange::LeaderSchedule;
use crate::staking_utils;
use morgan_runtime::bank::Bank;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::timing::NUM_CONSECUTIVE_LEADER_SLOTS;
use proptest::{
    strategy::{Strategy, ValueTree},
    test_runner::{Config, TestRunner},
};

/// Return the leader schedule for the given epoch.
pub fn leader_schedule(epoch_height: u64, bank: &Bank) -> Option<LeaderSchedule> {
    staking_utils::staked_nodes_at_epoch(bank, epoch_height).map(|stakes| {
        let mut seed = [0u8; 32];
        seed[0..8].copy_from_slice(&epoch_height.to_le_bytes());
        let mut stakes: Vec<_> = stakes.into_iter().collect();
        sort_stakes(&mut stakes);
        LeaderSchedule::new(
            &stakes,
            seed,
            bank.get_slots_in_epoch(epoch_height),
            NUM_CONSECUTIVE_LEADER_SLOTS,
        )
    })
}

/// Return the leader for the given slot.
pub fn slot_leader_at(slot: u64, bank: &Bank) -> Option<Pubkey> {
    let (epoch, slot_index) = bank.get_epoch_and_slot_index(slot);

    leader_schedule(epoch, bank).map(|leader_schedule| leader_schedule[slot_index])
}

// Returns the number of ticks remaining from the specified tick_height to the end of the
// slot implied by the tick_height
pub fn num_ticks_left_in_slot(bank: &Bank, tick_height: u64) -> u64 {
    bank.ticks_per_slot() - tick_height % bank.ticks_per_slot() - 1
}

pub fn tick_height_to_slot(ticks_per_slot: u64, tick_height: u64) -> u64 {
    tick_height / ticks_per_slot
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

fn sort_stakes(stakes: &mut Vec<(Pubkey, u64)>) {
    // Sort first by stake. If stakes are the same, sort by pubkey to ensure a
    // deterministic result.
    // Note: Use unstable sort, because we dedup right after to remove the equal elements.
    stakes.sort_unstable_by(|(l_pubkey, l_stake), (r_pubkey, r_stake)| {
        if r_stake == l_stake {
            r_pubkey.cmp(&l_pubkey)
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
    fn test_leader_schedule_via_bank() {
        let pubkey = Pubkey::new_rand();
        let genesis_block =
            create_genesis_block_with_leader(0, &pubkey, BOOTSTRAP_LEADER_DIFS).genesis_block;
        let bank = Bank::new(&genesis_block);

        let pubkeys_and_stakes: Vec<_> = staking_utils::staked_nodes(&bank).into_iter().collect();
        let seed = [0u8; 32];
        let leader_schedule = LeaderSchedule::new(
            &pubkeys_and_stakes,
            seed,
            genesis_block.slots_per_epoch,
            NUM_CONSECUTIVE_LEADER_SLOTS,
        );

        assert_eq!(leader_schedule[0], pubkey);
        assert_eq!(leader_schedule[1], pubkey);
        assert_eq!(leader_schedule[2], pubkey);
    }

    #[test]
    fn test_leader_scheduler1_basic() {
        let pubkey = Pubkey::new_rand();
        let genesis_block = create_genesis_block_with_leader(
            BOOTSTRAP_LEADER_DIFS,
            &pubkey,
            BOOTSTRAP_LEADER_DIFS,
        )
        .genesis_block;
        let bank = Bank::new(&genesis_block);
        assert_eq!(slot_leader_at(bank.slot(), &bank).unwrap(), pubkey);
    }

    #[test]
    fn test_sort_stakes_basic() {
        let pubkey0 = Pubkey::new_rand();
        let pubkey1 = Pubkey::new_rand();
        let mut stakes = vec![(pubkey0, 1), (pubkey1, 2)];
        sort_stakes(&mut stakes);
        assert_eq!(stakes, vec![(pubkey1, 2), (pubkey0, 1)]);
    }

    #[test]
    fn test_sort_stakes_with_dup() {
        let pubkey0 = Pubkey::new_rand();
        let pubkey1 = Pubkey::new_rand();
        let mut stakes = vec![(pubkey0, 1), (pubkey1, 2), (pubkey0, 1)];
        sort_stakes(&mut stakes);
        assert_eq!(stakes, vec![(pubkey1, 2), (pubkey0, 1)]);
    }

    #[test]
    fn test_sort_stakes_with_equal_stakes() {
        let pubkey0 = Pubkey::default();
        let pubkey1 = Pubkey::new_rand();
        let mut stakes = vec![(pubkey0, 1), (pubkey1, 1)];
        sort_stakes(&mut stakes);
        assert_eq!(stakes, vec![(pubkey1, 1), (pubkey0, 1)]);
    }
}
