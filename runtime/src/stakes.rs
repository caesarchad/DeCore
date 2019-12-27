//! Stakes serve as a cache of stake and vote accounts to derive
//! node stakes
use hashbrown::HashMap;
use morgan_interface::account::Account;
use morgan_interface::bvm_address::BvmAddr;
use morgan_stake_api::stake_state::StakeState;

#[derive(Default, Clone)]
pub struct Stakes {
    /// vote accounts
    vote_accounts: HashMap<BvmAddr, (u64, Account)>,

    /// stake_accounts
    stake_accounts: HashMap<BvmAddr, Account>,
}

impl Stakes {
    // sum the stakes that point to the given voter_address
    fn calculate_stake(&self, voter_address: &BvmAddr) -> u64 {
        self.stake_accounts
            .iter()
            .filter(|(_, stake_account)| {
                Some(*voter_address) == StakeState::voter_address_from(stake_account)
            })
            .map(|(_, stake_account)| stake_account.difs)
            .sum()
    }

    pub fn is_stake(account: &Account) -> bool {
        morgan_vote_api::check_id(&account.owner) || morgan_stake_api::check_id(&account.owner)
    }

    pub fn store(&mut self, address: &BvmAddr, account: &Account) {
        if morgan_vote_api::check_id(&account.owner) {
            if account.difs == 0 {
                self.vote_accounts.remove(address);
            } else {
                // update the stake of this entry
                let stake = self
                    .vote_accounts
                    .get(address)
                    .map_or_else(|| self.calculate_stake(address), |v| v.0);

                self.vote_accounts.insert(*address, (stake, account.clone()));
            }
        } else if morgan_stake_api::check_id(&account.owner) {
            //  old_stake is stake difs and voter_address from the pre-store() version
            let old_stake = self.stake_accounts.get(address).and_then(|old_account| {
                StakeState::voter_address_from(old_account)
                    .map(|old_voter_address| (old_account.difs, old_voter_address))
            });

            let stake = StakeState::voter_address_from(account)
                .map(|voter_address| (account.difs, voter_address));

            // if adjustments need to be made...
            if stake != old_stake {
                if let Some((old_stake, old_voter_address)) = old_stake {
                    self.vote_accounts
                        .entry(old_voter_address)
                        .and_modify(|e| e.0 -= old_stake);
                }
                if let Some((stake, voter_address)) = stake {
                    self.vote_accounts
                        .entry(voter_address)
                        .and_modify(|e| e.0 += stake);
                }
            }

            if account.difs == 0 {
                self.stake_accounts.remove(address);
            } else {
                self.stake_accounts.insert(*address, account.clone());
            }
        }
    }
    pub fn vote_accounts(&self) -> &HashMap<BvmAddr, (u64, Account)> {
        &self.vote_accounts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use morgan_interface::bvm_address::BvmAddr;
    use morgan_stake_api::stake_state;
    use morgan_vote_api::vote_state::{self, VoteState};

    //  set up some dummies  for a staked node    ((     vote      )  (     stake     ))
    fn create_staked_node_accounts(stake: u64) -> ((BvmAddr, Account), (BvmAddr, Account)) {
        let vote_address = BvmAddr::new_rand();
        let vote_account = vote_state::create_account(&vote_address, &BvmAddr::new_rand(), 0, 1);
        (
            (vote_address, vote_account),
            create_stake_account(stake, &vote_address),
        )
    }

    //   add stake to a vote_address                               (   stake    )
    fn create_stake_account(stake: u64, vote_address: &BvmAddr) -> (BvmAddr, Account) {
        (
            BvmAddr::new_rand(),
            stake_state::create_delegate_stake_account(&vote_address, &VoteState::default(), stake),
        )
    }

    #[test]
    fn test_stakes_basic() {
        let mut stakes = Stakes::default();

        let ((vote_address, vote_account), (stake_address, mut stake_account)) =
            create_staked_node_accounts(10);

        stakes.store(&vote_address, &vote_account);
        stakes.store(&stake_address, &stake_account);

        {
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_address).is_some());
            assert_eq!(vote_accounts.get(&vote_address).unwrap().0, 10);
        }

        stake_account.difs = 42;
        stakes.store(&stake_address, &stake_account);
        {
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_address).is_some());
            assert_eq!(vote_accounts.get(&vote_address).unwrap().0, 42);
        }

        stake_account.difs = 0;
        stakes.store(&stake_address, &stake_account);
        {
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_address).is_some());
            assert_eq!(vote_accounts.get(&vote_address).unwrap().0, 0);
        }
    }

    #[test]
    fn test_stakes_vote_account_disappear_reappear() {
        let mut stakes = Stakes::default();

        let ((vote_address, mut vote_account), (stake_address, stake_account)) =
            create_staked_node_accounts(10);

        stakes.store(&vote_address, &vote_account);
        stakes.store(&stake_address, &stake_account);

        {
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_address).is_some());
            assert_eq!(vote_accounts.get(&vote_address).unwrap().0, 10);
        }

        vote_account.difs = 0;
        stakes.store(&vote_address, &vote_account);

        {
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_address).is_none());
        }
        vote_account.difs = 1;
        stakes.store(&vote_address, &vote_account);

        {
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_address).is_some());
            assert_eq!(vote_accounts.get(&vote_address).unwrap().0, 10);
        }
    }

    #[test]
    fn test_stakes_change_delegate() {
        let mut stakes = Stakes::default();

        let ((vote_address, vote_account), (stake_address, stake_account)) =
            create_staked_node_accounts(10);

        let ((vote_address2, vote_account2), (_stake_address2, stake_account2)) =
            create_staked_node_accounts(10);

        stakes.store(&vote_address, &vote_account);
        stakes.store(&vote_address2, &vote_account2);

        // delegates to vote_address
        stakes.store(&stake_address, &stake_account);

        {
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_address).is_some());
            assert_eq!(vote_accounts.get(&vote_address).unwrap().0, 10);
            assert!(vote_accounts.get(&vote_address2).is_some());
            assert_eq!(vote_accounts.get(&vote_address2).unwrap().0, 0);
        }

        // delegates to vote_address2
        stakes.store(&stake_address, &stake_account2);

        {
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_address).is_some());
            assert_eq!(vote_accounts.get(&vote_address).unwrap().0, 0);
            assert!(vote_accounts.get(&vote_address2).is_some());
            assert_eq!(vote_accounts.get(&vote_address2).unwrap().0, 10);
        }
    }
    #[test]
    fn test_stakes_multiple_stakers() {
        let mut stakes = Stakes::default();

        let ((vote_address, vote_account), (stake_address, stake_account)) =
            create_staked_node_accounts(10);

        let (stake_address2, stake_account2) = create_stake_account(10, &vote_address);

        stakes.store(&vote_address, &vote_account);

        // delegates to vote_address
        stakes.store(&stake_address, &stake_account);
        stakes.store(&stake_address2, &stake_account2);

        {
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_address).is_some());
            assert_eq!(vote_accounts.get(&vote_address).unwrap().0, 20);
        }
    }

    #[test]
    fn test_stakes_not_delegate() {
        let mut stakes = Stakes::default();

        let ((vote_address, vote_account), (stake_address, stake_account)) =
            create_staked_node_accounts(10);

        stakes.store(&vote_address, &vote_account);
        stakes.store(&stake_address, &stake_account);

        {
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_address).is_some());
            assert_eq!(vote_accounts.get(&vote_address).unwrap().0, 10);
        }

        // not a stake account, and whacks above entry
        stakes.store(&stake_address, &Account::new(1, 0, 0, &morgan_stake_api::id()));
        {
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_address).is_some());
            assert_eq!(vote_accounts.get(&vote_address).unwrap().0, 0);
        }
    }

}
