use crate::accounts::AccountLockType;
use crate::treasury::Treasury;
use morgan_interface::transaction::{Result, Transaction};
use std::borrow::Borrow;

// Represents the results of trying to lock a set of accounts
pub struct LockedAccountsResults<'a, 'b, I: Borrow<Transaction>> {
    locked_accounts_results: Vec<Result<()>>,
    treasury: &'a Treasury,
    transactions: &'b [I],
    lock_type: AccountLockType,
    pub(crate) needs_unlock: bool,
}

impl<'a, 'b, I: Borrow<Transaction>> LockedAccountsResults<'a, 'b, I> {
    pub fn new(
        locked_accounts_results: Vec<Result<()>>,
        treasury: &'a Treasury,
        transactions: &'b [I],
        lock_type: AccountLockType,
    ) -> Self {
        Self {
            locked_accounts_results,
            treasury,
            transactions,
            needs_unlock: true,
            lock_type,
        }
    }

    pub fn lock_type(&self) -> AccountLockType {
        self.lock_type.clone()
    }

    pub fn locked_accounts_results(&self) -> &Vec<Result<()>> {
        &self.locked_accounts_results
    }

    pub fn transactions(&self) -> &[I] {
        self.transactions
    }
}

// Unlock all locked accounts in destructor.
impl<'a, 'b, I: Borrow<Transaction>> Drop for LockedAccountsResults<'a, 'b, I> {
    fn drop(&mut self) {
        if self.needs_unlock {
            self.treasury.unlock_accounts(self)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::genesis_utils::{create_genesis_block_with_leader, GenesisBlockInfo};
    use morgan_interface::bvm_address::BvmAddr;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::sys_controller;

    #[test]
    fn test_account_locks() {
        let (treasury, txs) = setup();

        // Test getting locked accounts
        let lock_results = treasury.lock_accounts(&txs);

        // Grab locks
        assert!(lock_results
            .locked_accounts_results()
            .iter()
            .all(|x| x.is_ok()));

        // Trying to grab locks again should fail
        let lock_results2 = treasury.lock_accounts(&txs);
        assert!(lock_results2
            .locked_accounts_results()
            .iter()
            .all(|x| x.is_err()));

        // Drop the first set of locks
        drop(lock_results);

        // Now grabbing locks should work again
        let lock_results2 = treasury.lock_accounts(&txs);
        assert!(lock_results2
            .locked_accounts_results()
            .iter()
            .all(|x| x.is_ok()));
    }

    #[test]
    fn test_record_locks() {
        let (treasury, txs) = setup();

        // Test getting record locks
        let lock_results = treasury.lock_record_accounts(&txs);

        // Grabbing record locks doesn't return any results, must succeed or panic.
        assert!(lock_results.locked_accounts_results().is_empty());

        drop(lock_results);

        // Now grabbing record locks should work again
        let lock_results2 = treasury.lock_record_accounts(&txs);
        assert!(lock_results2.locked_accounts_results().is_empty());
    }

    fn setup() -> (Treasury, Vec<Transaction>) {
        let dummy_leader_pubkey = BvmAddr::new_rand();
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block_with_leader(500, &dummy_leader_pubkey, 100);
        let treasury = Treasury::new(&genesis_block);

        let pubkey = BvmAddr::new_rand();
        let keypair2 = Keypair::new();
        let pubkey2 = BvmAddr::new_rand();

        let txs = vec![
            sys_controller::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            sys_controller::transfer(&keypair2, &pubkey2, 1, genesis_block.hash()),
        ];

        (treasury, txs)
    }
}
