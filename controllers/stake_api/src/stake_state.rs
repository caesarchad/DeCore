//! Stake state
//! * delegate stakes to vote accounts
//! * keep track of rewards
//! * own mining pools

use crate::id;
use serde_derive::{Deserialize, Serialize};
use morgan_interface::account::{Account, KeyedAccount};
use morgan_interface::account_utils::State;
use morgan_interface::opcodes::OpCodeErr;
use morgan_interface::bvm_address::BvmAddr;
use morgan_vote_api::vote_state::VoteState;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum StakeState {
    Uninitialized,
    Delegate {
        voter_address: BvmAddr,
        credits_observed: u64,
    },
    MiningPool,
}

impl Default for StakeState {
    fn default() -> Self {
        StakeState::Uninitialized
    }
}
//  TODO: trusted values of network parameters come from where?
const DROPS_PER_SECOND: f64 = 10f64;
const DROPS_PER_SLOT: f64 = 8f64;

// credits/yr or slots/yr  is        seconds/year        *   drops/second   * slots/_drop
const CREDITS_PER_YEAR: f64 = (365f64 * 24f64 * 3600f64) * DROPS_PER_SECOND / DROPS_PER_SLOT;

// TODO: 20% is a niiice rate...  TODO: make this a member of MiningPool?
const STAKE_REWARD_TARGET_RATE: f64 = 0.20;

#[cfg(test)]
const STAKE_GETS_PAID_EVERY_VOTE: u64 = 200_000_000; // if numbers above (DROPS_YEAR) move, fix this

impl StakeState {
    // utility function, used by Stakes, tests
    pub fn from(account: &Account) -> Option<StakeState> {
        account.state().ok()
    }

    // utility function, used by Stakes, tests
    pub fn voter_address_from(account: &Account) -> Option<BvmAddr> {
        Self::from(account).and_then(|state: Self| state.voter_address())
    }

    pub fn voter_address(&self) -> Option<BvmAddr> {
        match self {
            StakeState::Delegate { voter_address, .. } => Some(*voter_address),
            _ => None,
        }
    }

    pub fn calculate_rewards(
        credits_observed: u64,
        stake: u64,
        vote_state: &VoteState,
    ) -> Option<(u64, u64)> {
        if credits_observed >= vote_state.credits() {
            return None;
        }

        let total_rewards = stake as f64
            * STAKE_REWARD_TARGET_RATE
            * (vote_state.credits() - credits_observed) as f64
            / CREDITS_PER_YEAR;

        // don't bother trying to collect fractional difs
        if total_rewards < 1f64 {
            return None;
        }

        let (voter_rewards, staker_rewards, is_split) = vote_state.commission_split(total_rewards);

        if (voter_rewards < 1f64 || staker_rewards < 1f64) && is_split {
            // don't bother trying to collect fractional difs
            return None;
        }

        Some((voter_rewards as u64, staker_rewards as u64))
    }
}

pub trait StakeAccount {
    fn initialize_mining_pool(&mut self) -> Result<(), OpCodeErr>;
    fn initialize_delegate(&mut self) -> Result<(), OpCodeErr>;
    fn delegate_stake(&mut self, vote_account: &KeyedAccount) -> Result<(), OpCodeErr>;
    fn redeem_vote_credits(
        &mut self,
        stake_account: &mut KeyedAccount,
        vote_account: &mut KeyedAccount,
    ) -> Result<(), OpCodeErr>;
}

impl<'a> StakeAccount for KeyedAccount<'a> {
    fn initialize_mining_pool(&mut self) -> Result<(), OpCodeErr> {
        if let StakeState::Uninitialized = self.state()? {
            self.set_state(&StakeState::MiningPool)
        } else {
            Err(OpCodeErr::InvalidAccountData)
        }
    }
    fn initialize_delegate(&mut self) -> Result<(), OpCodeErr> {
        if let StakeState::Uninitialized = self.state()? {
            self.set_state(&StakeState::Delegate {
                voter_address: BvmAddr::default(),
                credits_observed: 0,
            })
        } else {
            Err(OpCodeErr::InvalidAccountData)
        }
    }
    fn delegate_stake(&mut self, vote_account: &KeyedAccount) -> Result<(), OpCodeErr> {
        if self.signer_key().is_none() {
            return Err(OpCodeErr::MissingRequiredSignature);
        }

        if let StakeState::Delegate { .. } = self.state()? {
            let vote_state: VoteState = vote_account.state()?;
            self.set_state(&StakeState::Delegate {
                voter_address: *vote_account.unsigned_key(),
                credits_observed: vote_state.credits(),
            })
        } else {
            Err(OpCodeErr::InvalidAccountData)
        }
    }

    fn redeem_vote_credits(
        &mut self,
        stake_account: &mut KeyedAccount,
        vote_account: &mut KeyedAccount,
    ) -> Result<(), OpCodeErr> {
        if let (
            StakeState::MiningPool,
            StakeState::Delegate {
                voter_address,
                credits_observed,
            },
        ) = (self.state()?, stake_account.state()?)
        {
            let vote_state: VoteState = vote_account.state()?;

            if voter_address != *vote_account.unsigned_key() {
                return Err(OpCodeErr::InvalidArgument);
            }

            if credits_observed > vote_state.credits() {
                return Err(OpCodeErr::InvalidAccountData);
            }

            if let Some((stakers_reward, voters_reward)) = StakeState::calculate_rewards(
                credits_observed,
                stake_account.account.difs,
                &vote_state,
            ) {
                if self.account.difs < (stakers_reward + voters_reward) {
                    return Err(OpCodeErr::DeficitOpCode);
                }
                self.account.difs -= stakers_reward + voters_reward;
                stake_account.account.difs += stakers_reward;
                vote_account.account.difs += voters_reward;

                stake_account.set_state(&StakeState::Delegate {
                    voter_address,
                    credits_observed: vote_state.credits(),
                })
            } else {
                // not worth collecting
                Err(OpCodeErr::CustomError(1))
            }
        } else {
            Err(OpCodeErr::InvalidAccountData)
        }
    }
}

// utility function, used by Treasury, tests, genesis
pub fn create_delegate_stake_account(
    voter_address: &BvmAddr,
    vote_state: &VoteState,
    difs: u64,
) -> Account {
    let mut stake_account = Account::new(difs, 0, std::mem::size_of::<StakeState>(), &id());

    stake_account
        .set_state(&StakeState::Delegate {
            voter_address: *voter_address,
            credits_observed: vote_state.credits(),
        })
        .expect("set_state");

    stake_account
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id;
    use morgan_interface::account::Account;
    use morgan_interface::bvm_address::BvmAddr;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_vote_api::vote_state;

    #[test]
    fn test_stake_delegate_stake() {
        let vote_keypair = Keypair::new();
        let mut vote_state = VoteState::default();
        for i in 0..1000 {
            vote_state.process_slot_vote_unchecked(i);
        }

        let vote_address = vote_keypair.address();
        let mut vote_account =
            vote_state::create_account(&vote_address, &BvmAddr::new_rand(), 0, 100);
        let mut vote_keyed_account = KeyedAccount::new(&vote_address, false, &mut vote_account);
        vote_keyed_account.set_state(&vote_state).unwrap();

        let stake_address = BvmAddr::default();
        let mut stake_account = Account::new(0, 0, std::mem::size_of::<StakeState>(), &id());

        let mut stake_keyed_account = KeyedAccount::new(&stake_address, false, &mut stake_account);

        {
            let stake_state: StakeState = stake_keyed_account.state().unwrap();
            assert_eq!(stake_state, StakeState::default());
        }

        stake_keyed_account.initialize_delegate().unwrap();
        assert_eq!(
            stake_keyed_account.delegate_stake(&vote_keyed_account),
            Err(OpCodeErr::MissingRequiredSignature)
        );

        let mut stake_keyed_account = KeyedAccount::new(&stake_address, true, &mut stake_account);
        assert!(stake_keyed_account
            .delegate_stake(&vote_keyed_account)
            .is_ok());

        // verify that create_delegate_stake_account() matches the
        //   resulting account from delegate_stake()
        assert_eq!(
            create_delegate_stake_account(&vote_address, &vote_state, 0),
            *stake_keyed_account.account,
        );

        let stake_state: StakeState = stake_keyed_account.state().unwrap();
        assert_eq!(
            stake_state,
            StakeState::Delegate {
                voter_address: vote_keypair.address(),
                credits_observed: vote_state.credits()
            }
        );

        let stake_state = StakeState::MiningPool;
        stake_keyed_account.set_state(&stake_state).unwrap();
        assert!(stake_keyed_account
            .delegate_stake(&vote_keyed_account)
            .is_err());
    }
    #[test]
    fn test_stake_state_calculate_rewards() {
        let mut vote_state = VoteState::default();
        let mut vote_i = 0;

        // put a credit in the vote_state
        while vote_state.credits() == 0 {
            vote_state.process_slot_vote_unchecked(vote_i);
            vote_i += 1;
        }
        // this guy can't collect now, not enough stake to get paid on 1 credit
        assert_eq!(None, StakeState::calculate_rewards(0, 100, &vote_state));
        // this guy can
        assert_eq!(
            Some((0, 1)),
            StakeState::calculate_rewards(0, STAKE_GETS_PAID_EVERY_VOTE, &vote_state)
        );
        // but, there's not enough to split
        vote_state.commission = std::u32::MAX / 2;
        assert_eq!(
            None,
            StakeState::calculate_rewards(0, STAKE_GETS_PAID_EVERY_VOTE, &vote_state)
        );

        // put more credit in the vote_state
        while vote_state.credits() < 10 {
            vote_state.process_slot_vote_unchecked(vote_i);
            vote_i += 1;
        }
        vote_state.commission = 0;
        assert_eq!(
            Some((0, 10)),
            StakeState::calculate_rewards(0, STAKE_GETS_PAID_EVERY_VOTE, &vote_state)
        );
        vote_state.commission = std::u32::MAX;
        assert_eq!(
            Some((10, 0)),
            StakeState::calculate_rewards(0, STAKE_GETS_PAID_EVERY_VOTE, &vote_state)
        );
        vote_state.commission = std::u32::MAX / 2;
        assert_eq!(
            Some((5, 5)),
            StakeState::calculate_rewards(0, STAKE_GETS_PAID_EVERY_VOTE, &vote_state)
        );
        // not even enough stake to get paid on 10 credits...
        assert_eq!(None, StakeState::calculate_rewards(0, 100, &vote_state));
    }

    #[test]
    fn test_stake_redeem_vote_credits() {
        let vote_keypair = Keypair::new();
        let mut vote_state = VoteState::default();
        for i in 0..1000 {
            vote_state.process_slot_vote_unchecked(i);
        }

        let vote_address = vote_keypair.address();
        let mut vote_account =
            vote_state::create_account(&vote_address, &BvmAddr::new_rand(), 0, 100);
        let mut vote_keyed_account = KeyedAccount::new(&vote_address, false, &mut vote_account);
        vote_keyed_account.set_state(&vote_state).unwrap();

        let address = BvmAddr::default();
        let mut stake_account = Account::new(
            STAKE_GETS_PAID_EVERY_VOTE,
            0,
            std::mem::size_of::<StakeState>(),
            &id(),
        );
        let mut stake_keyed_account = KeyedAccount::new(&address, true, &mut stake_account);
        stake_keyed_account.initialize_delegate().unwrap();

        // delegate the stake
        assert!(stake_keyed_account
            .delegate_stake(&vote_keyed_account)
            .is_ok());

        let mut mining_pool_account = Account::new(0, 0, std::mem::size_of::<StakeState>(), &id());
        let mut mining_pool_keyed_account =
            KeyedAccount::new(&address, true, &mut mining_pool_account);

        // not a mining pool yet...
        assert_eq!(
            mining_pool_keyed_account
                .redeem_vote_credits(&mut stake_keyed_account, &mut vote_keyed_account),
            Err(OpCodeErr::InvalidAccountData)
        );

        mining_pool_keyed_account
            .set_state(&StakeState::MiningPool)
            .unwrap();

        // no movement in vote account, so no redemption needed
        assert_eq!(
            mining_pool_keyed_account
                .redeem_vote_credits(&mut stake_keyed_account, &mut vote_keyed_account),
            Err(OpCodeErr::CustomError(1))
        );

        // move the vote account forward
        vote_state.process_slot_vote_unchecked(1000);
        vote_keyed_account.set_state(&vote_state).unwrap();

        // now, no difs in the pool!
        assert_eq!(
            mining_pool_keyed_account
                .redeem_vote_credits(&mut stake_keyed_account, &mut vote_keyed_account),
            Err(OpCodeErr::DeficitOpCode)
        );

        // add a dif to pool
        mining_pool_keyed_account.account.difs = 2;
        assert!(mining_pool_keyed_account
            .redeem_vote_credits(&mut stake_keyed_account, &mut vote_keyed_account)
            .is_ok()); // yay

        // difs only shifted around, none made or lost
        assert_eq!(
            2 + 100 + STAKE_GETS_PAID_EVERY_VOTE,
            mining_pool_account.difs + vote_account.difs + stake_account.difs
        );
    }

    #[test]
    fn test_stake_redeem_vote_credits_vote_errors() {
        let vote_keypair = Keypair::new();
        let mut vote_state = VoteState::default();
        for i in 0..1000 {
            vote_state.process_slot_vote_unchecked(i);
        }

        let vote_address = vote_keypair.address();
        let mut vote_account =
            vote_state::create_account(&vote_address, &BvmAddr::new_rand(), 0, 100);
        let mut vote_keyed_account = KeyedAccount::new(&vote_address, false, &mut vote_account);
        vote_keyed_account.set_state(&vote_state).unwrap();

        let address = BvmAddr::default();
        let mut stake_account = Account::new(0, 0, std::mem::size_of::<StakeState>(), &id());
        let mut stake_keyed_account = KeyedAccount::new(&address, true, &mut stake_account);
        stake_keyed_account.initialize_delegate().unwrap();

        // delegate the stake
        assert!(stake_keyed_account
            .delegate_stake(&vote_keyed_account)
            .is_ok());

        let mut mining_pool_account = Account::new(0, 0, std::mem::size_of::<StakeState>(), &id());
        let mut mining_pool_keyed_account =
            KeyedAccount::new(&address, true, &mut mining_pool_account);
        mining_pool_keyed_account
            .set_state(&StakeState::MiningPool)
            .unwrap();

        let mut vote_state = VoteState::default();
        for i in 0..100 {
            // go back in time, previous state had 1000 votes
            vote_state.process_slot_vote_unchecked(i);
        }
        vote_keyed_account.set_state(&vote_state).unwrap();
        // voter credits lower than stake_delegate credits...  TODO: is this an error?
        assert_eq!(
            mining_pool_keyed_account
                .redeem_vote_credits(&mut stake_keyed_account, &mut vote_keyed_account),
            Err(OpCodeErr::InvalidAccountData)
        );

        let vote1_keypair = Keypair::new();
        let vote1_address = vote1_keypair.address();
        let mut vote1_account =
            vote_state::create_account(&vote1_address, &BvmAddr::new_rand(), 0, 100);
        let mut vote1_keyed_account = KeyedAccount::new(&vote1_address, false, &mut vote1_account);
        vote1_keyed_account.set_state(&vote_state).unwrap();

        // wrong voter_address...
        assert_eq!(
            mining_pool_keyed_account
                .redeem_vote_credits(&mut stake_keyed_account, &mut vote1_keyed_account),
            Err(OpCodeErr::InvalidArgument)
        );
    }

}
