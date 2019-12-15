#![feature(test)]

extern crate test;

use morgan_runtime::treasury::*;
use morgan_interface::account::Account;
use morgan_interface::genesis_block::create_genesis_block;
use morgan_interface::pubkey::Pubkey;
use std::sync::Arc;
use test::Bencher;

fn deposit_many(treasury: &Bank, pubkeys: &mut Vec<Pubkey>, num: usize) {
    for t in 0..num {
        let pubkey = Pubkey::new_rand();
        let account = Account::new((t + 1) as u64, 0, 0, &Account::default().owner);
        pubkeys.push(pubkey.clone());
        assert!(treasury.get_account(&pubkey).is_none());
        treasury.deposit(&pubkey, (t + 1) as u64);
        assert_eq!(treasury.get_account(&pubkey).unwrap(), account);
    }
}

#[bench]
fn test_accounts_create(bencher: &mut Bencher) {
    let (genesis_block, _) = create_genesis_block(10_000);
    let treasury0 = Bank::new_with_paths(&genesis_block, Some("bench_a0".to_string()));
    bencher.iter(|| {
        let mut pubkeys: Vec<Pubkey> = vec![];
        deposit_many(&treasury0, &mut pubkeys, 1000);
    });
}

#[bench]
fn test_accounts_squash(bencher: &mut Bencher) {
    let (genesis_block, _) = create_genesis_block(100_000);
    let mut treasuries: Vec<Arc<Bank>> = Vec::with_capacity(10);
    treasuries.push(Arc::new(Bank::new_with_paths(
        &genesis_block,
        Some("bench_a1".to_string()),
    )));
    let mut pubkeys: Vec<Pubkey> = vec![];
    deposit_many(&treasuries[0], &mut pubkeys, 250000);
    treasuries[0].freeze();
    // Measures the performance of the squash operation merging the accounts
    // with the majority of the accounts present in the parent treasury that is
    // moved over to this treasury.
    bencher.iter(|| {
        treasuries.push(Arc::new(Bank::new_from_parent(
            &treasuries[0],
            &Pubkey::default(),
            1u64,
        )));
        for accounts in 0..10000 {
            treasuries[1].deposit(&pubkeys[accounts], (accounts + 1) as u64);
        }
        treasuries[1].squash();
    });
}
