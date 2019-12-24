use morgan_vote_api::vote_state::MAX_LOCKOUT_HISTORY;

pub const MINIMUM_SLOT_LENGTH: usize = MAX_LOCKOUT_HISTORY + 1;

#[derive(Default, Debug, PartialEq, Eq, Clone, Copy)]
pub struct RoundPlan {
    /// The maximum number of slots in each epoch.
    pub candidate_each_round: u64,

    /// A number of slots before slot_index 0. Used to calculate finalized staked nodes.
    pub stake_place_holder: u64,

    /// basically: log2(candidate_each_round) - log2(MINIMUM_SLOT_LEN)
    pub genesis_round: u64,

    /// basically: 2.pow(genesis_round) - MINIMUM_SLOT_LEN
    pub genesis_candidate: u64,
}

impl RoundPlan {
    pub fn new(candidate_each_round: u64, stake_place_holder: u64, warmup: bool) -> Self {
        assert!(candidate_each_round >= MINIMUM_SLOT_LENGTH as u64);
        let (genesis_round, genesis_candidate) = if warmup {
            let next_power_of_two = candidate_each_round.next_power_of_two();
            let log2_slots_per_epoch = next_power_of_two
                .trailing_zeros()
                .saturating_sub(MINIMUM_SLOT_LENGTH.trailing_zeros());

            (
                u64::from(log2_slots_per_epoch),
                next_power_of_two.saturating_sub(MINIMUM_SLOT_LENGTH as u64),
            )
        } else {
            (0, 0)
        };
        RoundPlan {
            candidate_each_round,
            stake_place_holder,
            genesis_round,
            genesis_candidate,
        }
    }

    /// get the length of the given epoch (in slots)
    pub fn get_slots_in_epoch(&self, epoch: u64) -> u64 {
        if epoch < self.genesis_round {
            2u64.pow(epoch as u32 + MINIMUM_SLOT_LENGTH.trailing_zeros() as u32)
        } else {
            self.candidate_each_round
        }
    }

    /// get the epoch for which the given slot should save off
    ///  information about stakers
    pub fn get_stakers_epoch(&self, slot: u64) -> u64 {
        if slot < self.genesis_candidate {
            // until we get to normal slots, behave as if stake_place_holder == candidate_each_round
            self.get_epoch_and_slot_index(slot).0 + 1
        } else {
            self.genesis_round
                + (slot - self.genesis_candidate + self.stake_place_holder) / self.candidate_each_round
        }
    }

    /// get epoch and offset into the epoch for the given slot
    pub fn get_epoch_and_slot_index(&self, slot: u64) -> (u64, u64) {
        if slot < self.genesis_candidate {
            let epoch = (slot + MINIMUM_SLOT_LENGTH as u64 + 1)
                .next_power_of_two()
                .trailing_zeros()
                - MINIMUM_SLOT_LENGTH.trailing_zeros()
                - 1;

            let epoch_len = 2u64.pow(epoch + MINIMUM_SLOT_LENGTH.trailing_zeros());

            (
                u64::from(epoch),
                slot - (epoch_len - MINIMUM_SLOT_LENGTH as u64),
            )
        } else {
            (
                self.genesis_round + ((slot - self.genesis_candidate) / self.candidate_each_round),
                (slot - self.genesis_candidate) % self.candidate_each_round,
            )
        }
    }

    pub fn get_first_slot_in_epoch(&self, epoch: u64) -> u64 {
        if epoch <= self.genesis_round {
            (2u64.pow(epoch as u32) - 1) * MINIMUM_SLOT_LENGTH as u64
        } else {
            (epoch - self.genesis_round) * self.candidate_each_round + self.genesis_candidate
        }
    }

    pub fn get_last_slot_in_epoch(&self, epoch: u64) -> u64 {
        self.get_first_slot_in_epoch(epoch) + self.get_slots_in_epoch(epoch) - 1
    }
}
