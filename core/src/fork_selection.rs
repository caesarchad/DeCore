// use crate::treasury_forks::TreasuryForks;
use crate::treasury_forks::TreasuryForks;
use crate::staking_utils;
use crate::bvm_types::*;
use hashbrown::{HashMap, HashSet};
use morgan_metricbot::datapoint_info;
use morgan_runtime::treasury::Treasury;
use morgan_interface::account::Account;
use morgan_interface::hash::Hash;
use morgan_interface::bvm_address::BvmAddr;
use morgan_vote_api::vote_state::{Lockout, Vote, VoteState, MAX_LOCKOUT_HISTORY};
use std::collections::VecDeque;
use std::sync::Arc;
use morgan_helper::logHelper::*;


#[derive(Default)]
pub struct EpochStakes {
    epoch: u64,
    stakes: HashMap<BvmAddr, u64>,
    self_staked: u64,
    total_staked: u64,
    delegate_address: BvmAddr,
}

#[derive(Default, Debug)]
pub struct StakeLockout {
    lockout: u64,
    stake: u64,
}

#[derive(Default)]
pub struct LockStack {
    epoch_stakes: EpochStakes,
    threshold_depth: usize,
    threshold_size: f64,
    lockouts: VoteState,
    recent_votes: VecDeque<Vote>,
}

impl EpochStakes {
    pub fn new(epoch: u64, stakes: HashMap<BvmAddr, u64>, delegate_address: &BvmAddr) -> Self {
        let total_staked = stakes.values().sum();
        let self_staked = *stakes.get(&delegate_address).unwrap_or(&0);
        Self {
            epoch,
            stakes,
            total_staked,
            self_staked,
            delegate_address: *delegate_address,
        }
    }
    pub fn new_for_tests(difs: u64) -> Self {
        Self::new(
            0,
            vec![(BvmAddr::default(), difs)].into_iter().collect(),
            &BvmAddr::default(),
        )
    }
    pub fn new_from_stakes(epoch: u64, accounts: &[(BvmAddr, (u64, Account))]) -> Self {
        let stakes = accounts.iter().map(|(k, (v, _))| (*k, *v)).collect();
        Self::new(epoch, stakes, &accounts[0].0)
    }
    pub fn new_from_treasury(treasury: &Treasury, my_address: &BvmAddr) -> Self {
        let treasury_round = treasury.get_epoch_and_slot_index(treasury.slot()).0;
        let stakes = staking_utils::vote_account_stakes_at_epoch(treasury, treasury_round)
            .expect("voting require a treasury with stakes");
        Self::new(treasury_round, stakes, my_address)
    }
}

impl LockStack {
    pub fn new_from_forks(treasury_forks: &TreasuryForks, my_address: &BvmAddr) -> Self {
        let mut frozen_treasuries: Vec<_> = treasury_forks.frozen_treasuries().values().cloned().collect();
        frozen_treasuries.sort_by_key(|b| (b.parents().len(), b.slot()));
        let epoch_stakes = {
            if let Some(treasury) = frozen_treasuries.last() {
                EpochStakes::new_from_treasury(treasury, my_address)
            } else {
                return Self::default();
            }
        };

        let mut lock_stack = Self {
            epoch_stakes,
            threshold_depth: VOTE_THRESHOLD_DEPTH,
            threshold_size: VOTE_THRESHOLD_SIZE,
            lockouts: VoteState::default(),
            recent_votes: VecDeque::default(),
        };

        let treasury = lock_stack.find_heaviest_treasury(treasury_forks).unwrap();
        lock_stack.lockouts =
            Self::initialize_lockouts_from_treasury(&treasury, lock_stack.epoch_stakes.epoch);
        lock_stack
    }
    pub fn new(epoch_stakes: EpochStakes, threshold_depth: usize, threshold_size: f64) -> Self {
        Self {
            epoch_stakes,
            threshold_depth,
            threshold_size,
            lockouts: VoteState::default(),
            recent_votes: VecDeque::default(),
        }
    }
    pub fn collect_vote_lockouts<F>(
        &self,
        treasury_slot: u64,
        vote_accounts: F,
        ancestors: &HashMap<u64, HashSet<u64>>,
    ) -> HashMap<u64, StakeLockout>
    where
        F: Iterator<Item = (BvmAddr, (u64, Account))>,
    {
        let mut stake_lockouts = HashMap::new();
        for (key, (_, account)) in vote_accounts {
            let difs: u64 = *self.epoch_stakes.stakes.get(&key).unwrap_or(&0);
            if difs == 0 {
                continue;
            }
            let vote_state = VoteState::from(&account);
            if vote_state.is_none() {
                datapoint_warn!(
                    "locktower_warn",
                    (
                        "warn",
                        format!("Unable to get vote_state from account {}", key),
                        String
                    ),
                );
                continue;
            }
            let mut vote_state = vote_state.unwrap();

            if key == self.epoch_stakes.delegate_address
                || vote_state.node_address == self.epoch_stakes.delegate_address
            {
                debug!("vote state {:?}", vote_state);
                debug!(
                    "observed slot {}",
                    vote_state.nth_recent_vote(0).map(|v| v.slot).unwrap_or(0) as i64
                );
                debug!("observed root {}", vote_state.root_slot.unwrap_or(0) as i64);
                datapoint_info!(
                    "lock_stack-observed",
                    (
                        "slot",
                        vote_state.nth_recent_vote(0).map(|v| v.slot).unwrap_or(0),
                        i64
                    ),
                    ("root", vote_state.root_slot.unwrap_or(0), i64)
                );
            }
            let start_root = vote_state.root_slot;

            vote_state.process_slot_vote_unchecked(treasury_slot);

            for vote in &vote_state.votes {
                Self::update_ancestor_lockouts(&mut stake_lockouts, &vote, ancestors);
            }
            if start_root != vote_state.root_slot {
                if let Some(root) = start_root {
                    let vote = Lockout {
                        confirmation_count: MAX_LOCKOUT_HISTORY as u32,
                        slot: root,
                    };
                    trace!("ROOT: {}", vote.slot);
                    Self::update_ancestor_lockouts(&mut stake_lockouts, &vote, ancestors);
                }
            }
            if let Some(root) = vote_state.root_slot {
                let vote = Lockout {
                    confirmation_count: MAX_LOCKOUT_HISTORY as u32,
                    slot: root,
                };
                Self::update_ancestor_lockouts(&mut stake_lockouts, &vote, ancestors);
            }

            // The last vote in the vote stack is a simulated vote on treasury_slot, which
            // we added to the vote stack earlier in this function by calling process_vote().
            // We don't want to update the ancestors stakes of this vote b/c it does not
            // represent an actual vote by the validator.

            // Note: It should not be possible for any vote state in this treasury to have
            // a vote for a slot >= treasury_slot, so we are guaranteed that the last vote in
            // this vote stack is the simulated vote, so this fetch should be sufficient
            // to find the last unsimulated vote.
            assert_eq!(
                vote_state.nth_recent_vote(0).map(|l| l.slot),
                Some(treasury_slot)
            );
            if let Some(vote) = vote_state.nth_recent_vote(1) {
                // Update all the parents of this last vote with the stake of this vote account
                Self::update_ancestor_stakes(&mut stake_lockouts, vote.slot, difs, ancestors);
            }
        }
        stake_lockouts
    }

    pub fn is_slot_confirmed(&self, slot: u64, lockouts: &HashMap<u64, StakeLockout>) -> bool {
        lockouts
            .get(&slot)
            .map(|lockout| {
                (lockout.stake as f64 / self.epoch_stakes.total_staked as f64) > self.threshold_size
            })
            .unwrap_or(false)
    }

    pub fn is_recent_epoch(&self, treasury: &Treasury) -> bool {
        let treasury_round = treasury.get_epoch_and_slot_index(treasury.slot()).0;
        treasury_round >= self.epoch_stakes.epoch
    }

    pub fn update_epoch(&mut self, treasury: &Treasury) {
        trace!(
            "updating treasury epoch slot: {} epoch: {}",
            treasury.slot(),
            self.epoch_stakes.epoch
        );
        let treasury_round = treasury.get_epoch_and_slot_index(treasury.slot()).0;
        if treasury_round != self.epoch_stakes.epoch {
            assert!(
                self.is_recent_epoch(treasury),
                "epoch_stakes cannot move backwards"
            );
            // info!(
            //     "{}",
            //     Info(format!("LockStack updated epoch treasury slot: {} epoch: {}",
            //     treasury.slot(),
            //     self.epoch_stakes.epoch).to_string())
            // );
            println!("{}",
                printLn(
                    format!("LockStack updated epoch treasury slot: {} epoch: {}",
                        treasury.slot(),
                        self.epoch_stakes.epoch
                    ).to_string(),
                    module_path!().to_string()
                )
            );
            self.epoch_stakes =
                EpochStakes::new_from_treasury(treasury, &self.epoch_stakes.delegate_address);
            datapoint_info!(
                "lock_stack-epoch",
                ("epoch", self.epoch_stakes.epoch, i64),
                ("self_staked", self.epoch_stakes.self_staked, i64),
                ("total_staked", self.epoch_stakes.total_staked, i64)
            );
        }
    }

    pub fn record_vote(&mut self, slot: u64, hash: Hash) -> Option<u64> {
        let root_slot = self.lockouts.root_slot;
        let vote = Vote { slot, hash };
        self.lockouts.process_vote_unchecked(&vote);

        // vote_state doesn't keep around the hashes, so we save them in recent_votes
        self.recent_votes.push_back(vote);
        let slots = self
            .lockouts
            .votes
            .iter()
            .skip(self.lockouts.votes.len().saturating_sub(MAX_RECENT_VOTES))
            .map(|vote| vote.slot)
            .collect::<Vec<_>>();
        self.recent_votes
            .retain(|vote| slots.iter().any(|slot| vote.slot == *slot));

        datapoint_info!(
            "lock_stack-vote",
            ("latest", slot, i64),
            ("root", self.lockouts.root_slot.unwrap_or(0), i64)
        );
        if root_slot != self.lockouts.root_slot {
            Some(self.lockouts.root_slot.unwrap())
        } else {
            None
        }
    }

    pub fn recent_votes(&self) -> Vec<Vote> {
        self.recent_votes.iter().cloned().collect::<Vec<_>>()
    }

    pub fn root(&self) -> Option<u64> {
        self.lockouts.root_slot
    }

    pub fn calculate_weight(&self, stake_lockouts: &HashMap<u64, StakeLockout>) -> u128 {
        let mut sum = 0u128;
        let root_slot = self.lockouts.root_slot.unwrap_or(0);
        for (slot, stake_lockout) in stake_lockouts {
            if self.lockouts.root_slot.is_some() && *slot <= root_slot {
                continue;
            }
            sum += u128::from(stake_lockout.lockout) * u128::from(stake_lockout.stake)
        }
        sum
    }

    pub fn has_voted(&self, slot: u64) -> bool {
        for vote in &self.lockouts.votes {
            if vote.slot == slot {
                return true;
            }
        }
        false
    }

    pub fn is_locked_out(&self, slot: u64, descendants: &HashMap<u64, HashSet<u64>>) -> bool {
        let mut lockouts = self.lockouts.clone();
        lockouts.process_slot_vote_unchecked(slot);
        for vote in &lockouts.votes {
            if vote.slot == slot {
                continue;
            }
            if !descendants[&vote.slot].contains(&slot) {
                return true;
            }
        }
        if let Some(root) = lockouts.root_slot {
            !descendants[&root].contains(&slot)
        } else {
            false
        }
    }

    pub fn check_vote_stake_threshold(
        &self,
        slot: u64,
        stake_lockouts: &HashMap<u64, StakeLockout>,
    ) -> bool {
        let mut lockouts = self.lockouts.clone();
        lockouts.process_slot_vote_unchecked(slot);
        let vote = lockouts.nth_recent_vote(self.threshold_depth);
        if let Some(vote) = vote {
            if let Some(fork_stake) = stake_lockouts.get(&vote.slot) {
                (fork_stake.stake as f64 / self.epoch_stakes.total_staked as f64)
                    > self.threshold_size
            } else {
                false
            }
        } else {
            true
        }
    }

    /// Update lockouts for all the ancestors
    fn update_ancestor_lockouts(
        stake_lockouts: &mut HashMap<u64, StakeLockout>,
        vote: &Lockout,
        ancestors: &HashMap<u64, HashSet<u64>>,
    ) {
        let mut slot_with_ancestors = vec![vote.slot];
        slot_with_ancestors.extend(ancestors.get(&vote.slot).unwrap_or(&HashSet::new()));
        for slot in slot_with_ancestors {
            let entry = &mut stake_lockouts.entry(slot).or_default();
            entry.lockout += vote.lockout();
        }
    }

    /// Update stake for all the ancestors.
    /// Note, stake is the same for all the ancestor.
    fn update_ancestor_stakes(
        stake_lockouts: &mut HashMap<u64, StakeLockout>,
        slot: u64,
        difs: u64,
        ancestors: &HashMap<u64, HashSet<u64>>,
    ) {
        let mut slot_with_ancestors = vec![slot];
        slot_with_ancestors.extend(ancestors.get(&slot).unwrap_or(&HashSet::new()));
        for slot in slot_with_ancestors {
            let entry = &mut stake_lockouts.entry(slot).or_default();
            entry.stake += difs;
        }
    }

    fn treasury_weight(&self, treasury: &Treasury, ancestors: &HashMap<u64, HashSet<u64>>) -> u128 {
        let stake_lockouts =
            self.collect_vote_lockouts(treasury.slot(), treasury.vote_accounts().into_iter(), ancestors);
        self.calculate_weight(&stake_lockouts)
    }

    fn find_heaviest_treasury(&self, treasury_forks: &TreasuryForks) -> Option<Arc<Treasury>> {
        let ancestors = treasury_forks.ancestors();
        let mut treasury_weights: Vec<_> = treasury_forks
            .frozen_treasuries()
            .values()
            .map(|b| {
                (
                    self.treasury_weight(b, &ancestors),
                    b.parents().len(),
                    b.clone(),
                )
            })
            .collect();
        treasury_weights.sort_by_key(|b| (b.0, b.1));
        treasury_weights.pop().map(|b| b.2)
    }

    fn initialize_lockouts_from_treasury(treasury: &Treasury, current_epoch: u64) -> VoteState {
        let mut lockouts = VoteState::default();
        if let Some(iter) = treasury.epoch_vote_accounts(current_epoch) {
            for (delegate_address, (_, account)) in iter {
                if *delegate_address == treasury.collector_id() {
                    let state = VoteState::deserialize(&account.data).expect("votes");
                    if lockouts.votes.len() < state.votes.len() {
                        lockouts = state;
                    }
                }
            }
        };
        lockouts
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn gen_stakes(stake_votes: &[(u64, &[u64])]) -> Vec<(BvmAddr, (u64, Account))> {
        let mut stakes = vec![];
        for (difs, votes) in stake_votes {
            let mut account = Account::default();
            account.data = vec![0; 1024];
            account.difs = *difs;
            let mut vote_state = VoteState::default();
            for slot in *votes {
                vote_state.process_slot_vote_unchecked(*slot);
            }
            vote_state
                .serialize(&mut account.data)
                .expect("serialize state");
            stakes.push((BvmAddr::new_rand(), (*difs, account)));
        }
        stakes
    }

    #[test]
    fn test_collect_vote_lockouts_no_epoch_stakes() {
        let accounts = gen_stakes(&[(1, &[0])]);
        let epoch_stakes = EpochStakes::new_for_tests(2);
        let lock_stack = LockStack::new(epoch_stakes, 0, 0.67);
        let ancestors = vec![(1, vec![0].into_iter().collect()), (0, HashSet::new())]
            .into_iter()
            .collect();
        let staked_lockouts = lock_stack.collect_vote_lockouts(1, accounts.into_iter(), &ancestors);
        assert!(staked_lockouts.is_empty());
    }

    #[test]
    fn test_collect_vote_lockouts_sums() {
        //two accounts voting for slot 0 with 1 token staked
        let accounts = gen_stakes(&[(1, &[0]), (1, &[0])]);
        let epoch_stakes = EpochStakes::new_from_stakes(0, &accounts);
        let lock_stack = LockStack::new(epoch_stakes, 0, 0.67);
        let ancestors = vec![(1, vec![0].into_iter().collect()), (0, HashSet::new())]
            .into_iter()
            .collect();
        let staked_lockouts = lock_stack.collect_vote_lockouts(1, accounts.into_iter(), &ancestors);
        assert_eq!(staked_lockouts[&0].stake, 2);
        assert_eq!(staked_lockouts[&0].lockout, 2 + 2 + 4 + 4);
    }

    #[test]
    fn test_collect_vote_lockouts_root() {
        let votes: Vec<u64> = (0..MAX_LOCKOUT_HISTORY as u64).into_iter().collect();
        //two accounts voting for slot 0 with 1 token staked
        let accounts = gen_stakes(&[(1, &votes), (1, &votes)]);
        let epoch_stakes = EpochStakes::new_from_stakes(0, &accounts);
        let mut lock_stack = LockStack::new(epoch_stakes, 0, 0.67);
        let mut ancestors = HashMap::new();
        for i in 0..(MAX_LOCKOUT_HISTORY + 1) {
            lock_stack.record_vote(i as u64, Hash::default());
            ancestors.insert(i as u64, (0..i as u64).into_iter().collect());
        }
        assert_eq!(lock_stack.lockouts.root_slot, Some(0));
        let staked_lockouts = lock_stack.collect_vote_lockouts(
            MAX_LOCKOUT_HISTORY as u64,
            accounts.into_iter(),
            &ancestors,
        );
        for i in 0..MAX_LOCKOUT_HISTORY {
            assert_eq!(staked_lockouts[&(i as u64)].stake, 2);
        }
        // should be the sum of all the weights for root
        assert!(staked_lockouts[&0].lockout > (2 * (1 << MAX_LOCKOUT_HISTORY)));
    }

    #[test]
    fn test_calculate_weight_skips_root() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 0, 0.67);
        lock_stack.lockouts.root_slot = Some(1);
        let stakes = vec![
            (
                0,
                StakeLockout {
                    stake: 1,
                    lockout: 8,
                },
            ),
            (
                1,
                StakeLockout {
                    stake: 1,
                    lockout: 8,
                },
            ),
        ]
        .into_iter()
        .collect();
        assert_eq!(lock_stack.calculate_weight(&stakes), 0u128);
    }

    #[test]
    fn test_calculate_weight() {
        let lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 0, 0.67);
        let stakes = vec![(
            0,
            StakeLockout {
                stake: 1,
                lockout: 8,
            },
        )]
        .into_iter()
        .collect();
        assert_eq!(lock_stack.calculate_weight(&stakes), 8u128);
    }

    #[test]
    fn test_check_vote_threshold_without_votes() {
        let lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 1, 0.67);
        let stakes = vec![(
            0,
            StakeLockout {
                stake: 1,
                lockout: 8,
            },
        )]
        .into_iter()
        .collect();
        assert!(lock_stack.check_vote_stake_threshold(0, &stakes));
    }

    #[test]
    fn test_is_slot_confirmed_not_enough_stake_failure() {
        let lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 1, 0.67);
        let stakes = vec![(
            0,
            StakeLockout {
                stake: 1,
                lockout: 8,
            },
        )]
        .into_iter()
        .collect();
        assert!(!lock_stack.is_slot_confirmed(0, &stakes));
    }

    #[test]
    fn test_is_slot_confirmed_unknown_slot() {
        let lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 1, 0.67);
        let stakes = HashMap::new();
        assert!(!lock_stack.is_slot_confirmed(0, &stakes));
    }

    #[test]
    fn test_is_slot_confirmed_pass() {
        let lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 1, 0.67);
        let stakes = vec![(
            0,
            StakeLockout {
                stake: 2,
                lockout: 8,
            },
        )]
        .into_iter()
        .collect();
        assert!(lock_stack.is_slot_confirmed(0, &stakes));
    }

    #[test]
    fn test_is_locked_out_empty() {
        let lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 0, 0.67);
        let descendants = HashMap::new();
        assert!(!lock_stack.is_locked_out(0, &descendants));
    }

    #[test]
    fn test_is_locked_out_root_slot_child_pass() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 0, 0.67);
        let descendants = vec![(0, vec![1].into_iter().collect())]
            .into_iter()
            .collect();
        lock_stack.lockouts.root_slot = Some(0);
        assert!(!lock_stack.is_locked_out(1, &descendants));
    }

    #[test]
    fn test_is_locked_out_root_slot_sibling_fail() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 0, 0.67);
        let descendants = vec![(0, vec![1].into_iter().collect())]
            .into_iter()
            .collect();
        lock_stack.lockouts.root_slot = Some(0);
        assert!(lock_stack.is_locked_out(2, &descendants));
    }

    #[test]
    fn test_check_already_voted() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 0, 0.67);
        lock_stack.record_vote(0, Hash::default());
        assert!(lock_stack.has_voted(0));
        assert!(!lock_stack.has_voted(1));
    }

    #[test]
    fn test_is_locked_out_double_vote() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 0, 0.67);
        let descendants = vec![(0, vec![1].into_iter().collect()), (1, HashSet::new())]
            .into_iter()
            .collect();
        lock_stack.record_vote(0, Hash::default());
        lock_stack.record_vote(1, Hash::default());
        assert!(lock_stack.is_locked_out(0, &descendants));
    }

    #[test]
    fn test_is_locked_out_child() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 0, 0.67);
        let descendants = vec![(0, vec![1].into_iter().collect())]
            .into_iter()
            .collect();
        lock_stack.record_vote(0, Hash::default());
        assert!(!lock_stack.is_locked_out(1, &descendants));
    }

    #[test]
    fn test_is_locked_out_sibling() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 0, 0.67);
        let descendants = vec![
            (0, vec![1, 2].into_iter().collect()),
            (1, HashSet::new()),
            (2, HashSet::new()),
        ]
        .into_iter()
        .collect();
        lock_stack.record_vote(0, Hash::default());
        lock_stack.record_vote(1, Hash::default());
        assert!(lock_stack.is_locked_out(2, &descendants));
    }

    #[test]
    fn test_is_locked_out_last_vote_expired() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 0, 0.67);
        let descendants = vec![(0, vec![1, 4].into_iter().collect()), (1, HashSet::new())]
            .into_iter()
            .collect();
        lock_stack.record_vote(0, Hash::default());
        lock_stack.record_vote(1, Hash::default());
        assert!(!lock_stack.is_locked_out(4, &descendants));
        lock_stack.record_vote(4, Hash::default());
        assert_eq!(lock_stack.lockouts.votes[0].slot, 0);
        assert_eq!(lock_stack.lockouts.votes[0].confirmation_count, 2);
        assert_eq!(lock_stack.lockouts.votes[1].slot, 4);
        assert_eq!(lock_stack.lockouts.votes[1].confirmation_count, 1);
    }

    #[test]
    fn test_check_vote_threshold_below_threshold() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 1, 0.67);
        let stakes = vec![(
            0,
            StakeLockout {
                stake: 1,
                lockout: 8,
            },
        )]
        .into_iter()
        .collect();
        lock_stack.record_vote(0, Hash::default());
        assert!(!lock_stack.check_vote_stake_threshold(1, &stakes));
    }
    #[test]
    fn test_check_vote_threshold_above_threshold() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 1, 0.67);
        let stakes = vec![(
            0,
            StakeLockout {
                stake: 2,
                lockout: 8,
            },
        )]
        .into_iter()
        .collect();
        lock_stack.record_vote(0, Hash::default());
        assert!(lock_stack.check_vote_stake_threshold(1, &stakes));
    }

    #[test]
    fn test_check_vote_threshold_above_threshold_after_pop() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 1, 0.67);
        let stakes = vec![(
            0,
            StakeLockout {
                stake: 2,
                lockout: 8,
            },
        )]
        .into_iter()
        .collect();
        lock_stack.record_vote(0, Hash::default());
        lock_stack.record_vote(1, Hash::default());
        lock_stack.record_vote(2, Hash::default());
        assert!(lock_stack.check_vote_stake_threshold(6, &stakes));
    }

    #[test]
    fn test_check_vote_threshold_above_threshold_no_stake() {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 1, 0.67);
        let stakes = HashMap::new();
        lock_stack.record_vote(0, Hash::default());
        assert!(!lock_stack.check_vote_stake_threshold(1, &stakes));
    }

    #[test]
    fn test_lockout_is_updated_for_entire_branch() {
        let mut stake_lockouts = HashMap::new();
        let vote = Lockout {
            slot: 2,
            confirmation_count: 1,
        };
        let set: HashSet<u64> = vec![0u64, 1u64].into_iter().collect();
        let mut ancestors = HashMap::new();
        ancestors.insert(2, set);
        let set: HashSet<u64> = vec![0u64].into_iter().collect();
        ancestors.insert(1, set);
        LockStack::update_ancestor_lockouts(&mut stake_lockouts, &vote, &ancestors);
        assert_eq!(stake_lockouts[&0].lockout, 2);
        assert_eq!(stake_lockouts[&1].lockout, 2);
        assert_eq!(stake_lockouts[&2].lockout, 2);
    }

    #[test]
    fn test_lockout_is_updated_for_slot_or_lower() {
        let mut stake_lockouts = HashMap::new();
        let set: HashSet<u64> = vec![0u64, 1u64].into_iter().collect();
        let mut ancestors = HashMap::new();
        ancestors.insert(2, set);
        let set: HashSet<u64> = vec![0u64].into_iter().collect();
        ancestors.insert(1, set);
        let vote = Lockout {
            slot: 2,
            confirmation_count: 1,
        };
        LockStack::update_ancestor_lockouts(&mut stake_lockouts, &vote, &ancestors);
        let vote = Lockout {
            slot: 1,
            confirmation_count: 2,
        };
        LockStack::update_ancestor_lockouts(&mut stake_lockouts, &vote, &ancestors);
        assert_eq!(stake_lockouts[&0].lockout, 2 + 4);
        assert_eq!(stake_lockouts[&1].lockout, 2 + 4);
        assert_eq!(stake_lockouts[&2].lockout, 2);
    }

    #[test]
    fn test_stake_is_updated_for_entire_branch() {
        let mut stake_lockouts = HashMap::new();
        let mut account = Account::default();
        account.difs = 1;
        let set: HashSet<u64> = vec![0u64, 1u64].into_iter().collect();
        let ancestors: HashMap<u64, HashSet<u64>> = [(2u64, set)].into_iter().cloned().collect();
        LockStack::update_ancestor_stakes(&mut stake_lockouts, 2, account.difs, &ancestors);
        assert_eq!(stake_lockouts[&0].stake, 1);
        assert_eq!(stake_lockouts[&1].stake, 1);
        assert_eq!(stake_lockouts[&2].stake, 1);
    }

    #[test]
    fn test_check_vote_threshold_forks() {
        // Create the ancestor relationships
        let ancestors = (0..=(VOTE_THRESHOLD_DEPTH + 1) as u64)
            .map(|slot| {
                let slot_parents: HashSet<_> = (0..slot).collect();
                (slot, slot_parents)
            })
            .collect();

        // Create votes such that
        // 1) 3/4 of the stake has voted on slot: VOTE_THRESHOLD_DEPTH - 2, lockout: 2
        // 2) 1/4 of the stake has voted on slot: VOTE_THRESHOLD_DEPTH, lockout: 2^9
        let total_stake = 4;
        let threshold_size = 0.67;
        let threshold_stake = (f64::ceil(total_stake as f64 * threshold_size)) as u64;
        let locktower_votes: Vec<u64> = (0..VOTE_THRESHOLD_DEPTH as u64).collect();
        let accounts = gen_stakes(&[
            (threshold_stake, &[(VOTE_THRESHOLD_DEPTH - 2) as u64]),
            (total_stake - threshold_stake, &locktower_votes[..]),
        ]);

        // Initialize lock_stack
        let stakes: HashMap<_, _> = accounts.iter().map(|(pk, (s, _))| (*pk, *s)).collect();
        let epoch_stakes = EpochStakes::new(0, stakes, &BvmAddr::default());
        let mut lock_stack = LockStack::new(epoch_stakes, VOTE_THRESHOLD_DEPTH, threshold_size);

        // CASE 1: Record the first VOTE_THRESHOLD lock_stack votes for fork 2. We want to
        // evaluate a vote on slot VOTE_THRESHOLD_DEPTH. The nth most recent vote should be
        // for slot 0, which is common to all account vote states, so we should pass the
        // threshold check
        let vote_to_evaluate = VOTE_THRESHOLD_DEPTH as u64;
        for vote in &locktower_votes {
            lock_stack.record_vote(*vote, Hash::default());
        }
        let stakes_lockouts = lock_stack.collect_vote_lockouts(
            vote_to_evaluate,
            accounts.clone().into_iter(),
            &ancestors,
        );
        assert!(lock_stack.check_vote_stake_threshold(vote_to_evaluate, &stakes_lockouts));

        // CASE 2: Now we want to evaluate a vote for slot VOTE_THRESHOLD_DEPTH + 1. This slot
        // will expire the vote in one of the vote accounts, so we should have insufficient
        // stake to pass the threshold
        let vote_to_evaluate = VOTE_THRESHOLD_DEPTH as u64 + 1;
        let stakes_lockouts =
            lock_stack.collect_vote_lockouts(vote_to_evaluate, accounts.into_iter(), &ancestors);
        assert!(!lock_stack.check_vote_stake_threshold(vote_to_evaluate, &stakes_lockouts));
    }

    fn vote_and_check_recent(num_votes: usize) {
        let mut lock_stack = LockStack::new(EpochStakes::new_for_tests(2), 1, 0.67);
        let start = num_votes.saturating_sub(MAX_RECENT_VOTES);
        let expected: Vec<_> = (start..num_votes)
            .map(|i| Vote::new(i as u64, Hash::default()))
            .collect();
        for i in 0..num_votes {
            lock_stack.record_vote(i as u64, Hash::default());
        }
        assert_eq!(expected, lock_stack.recent_votes())
    }

    #[test]
    fn test_recent_votes_full() {
        vote_and_check_recent(MAX_LOCKOUT_HISTORY)
    }

    #[test]
    fn test_recent_votes_empty() {
        vote_and_check_recent(0)
    }

    #[test]
    fn test_recent_votes_exact() {
        vote_and_check_recent(MAX_RECENT_VOTES)
    }
}
