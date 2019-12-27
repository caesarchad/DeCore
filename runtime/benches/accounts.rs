#![feature(test)]

extern crate test;

use morgan_runtime::treasury::*;
use morgan_interface::account::Account;
use morgan_interface::genesis_block::create_genesis_block;
use morgan_interface::bvm_address::BvmAddr;
use std::sync::Arc;
use test::Bencher;

fn deposit_many(treasury: &Treasury, addresss: &mut Vec<BvmAddr>, num: usize) {
    for t in 0..num {
        let address = BvmAddr::new_rand();
        let account = Account::new((t + 1) as u64, 0, 0, &Account::default().owner);
        addresss.push(address.clone());
        assert!(treasury.get_account(&address).is_none());
        treasury.deposit(&address, (t + 1) as u64);
        assert_eq!(treasury.get_account(&address).unwrap(), account);
    }
}

#[bench]
fn test_accounts_create(bencher: &mut Bencher) {
    let (genesis_block, _) = create_genesis_block(10_000);
    let treasury0 = Treasury::new_with_paths(&genesis_block, Some("bench_a0".to_string()));
    bencher.iter(|| {
        let mut addresss: Vec<BvmAddr> = vec![];
        deposit_many(&treasury0, &mut addresss, 1000);
    });
}

#[bench]
fn test_accounts_squash(bencher: &mut Bencher) {
    let (genesis_block, _) = create_genesis_block(100_000);
    let mut treasuries: Vec<Arc<Treasury>> = Vec::with_capacity(10);
    treasuries.push(Arc::new(Treasury::new_with_paths(
        &genesis_block,
        Some("bench_a1".to_string()),
    )));
    let mut addresss: Vec<BvmAddr> = vec![];
    deposit_many(&treasuries[0], &mut addresss, 250000);
    treasuries[0].freeze();
    // Measures the performance of the squash operation merging the accounts
    // with the majority of the accounts present in the parent treasury that is
    // moved over to this treasury.
    bencher.iter(|| {
        treasuries.push(Arc::new(Treasury::new_from_parent(
            &treasuries[0],
            &BvmAddr::default(),
            1u64,
        )));
        for accounts in 0..10000 {
            treasuries[1].deposit(&addresss[accounts], (accounts + 1) as u64);
        }
        treasuries[1].squash();
    });
}
