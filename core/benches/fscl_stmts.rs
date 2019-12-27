#![feature(test)]

extern crate test;

use morgan::fiscal_statement_info::{next_fiscal_statment, restore_fscl_stmt_by_data, FsclStmtSlc};
use morgan_interface::hash::{hash, Hash};
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::sys_controller;
use test::Bencher;

#[bench]
fn bench_block_to_blobs_to_block(bencher: &mut Bencher) {
    let zero = Hash::default();
    let one = hash(&zero.as_ref());
    let keypair = Keypair::new();
    let tx0 = sys_controller::transfer(&keypair, &keypair.address(), 1, one);
    let transactions = vec![tx0; 10];
    let entries = next_fiscal_statment(&zero, 1, transactions);

    bencher.iter(|| {
        let blobs = entries.to_blobs();
        assert_eq!(restore_fscl_stmt_by_data(blobs).unwrap().0, entries);
    });
}
