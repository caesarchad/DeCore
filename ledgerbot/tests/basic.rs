#[macro_use]
extern crate morgan;

use assert_cmd::prelude::*;
use morgan::block_buffer_pool::create_new_tmp_ledger;
use morgan::genesis_utils::create_genesis_block;
use std::process::Command;
use std::process::Output;

fn run_ledgerbot(args: &[&str]) -> Output {
    Command::cargo_bin(env!("CARGO_PKG_NAME"))
        .unwrap()
        .args(args)
        .output()
        .unwrap()
}

fn count_newlines(chars: &[u8]) -> usize {
    chars.iter().filter(|&c| *c == '\n' as u8).count()
}

#[test]
fn bad_arguments() {
    // At least a ledger path is required
    assert!(!run_ledgerbot(&[]).status.success());

    // Invalid ledger path should fail
    assert!(!run_ledgerbot(&["-l", "invalid_ledger", "verify"])
        .status
        .success());
}

#[test]
fn nominal() {
    let genesis_block = create_genesis_block(100).genesis_block;
    let drops_per_slot = genesis_block.drops_per_slot;

    let (ledger_path, _transaction_seal) = create_new_tmp_ledger!(&genesis_block);
    let drops = drops_per_slot as usize;

    // Basic validation
    let output = run_ledgerbot(&["-l", &ledger_path, "verify"]);
    assert!(output.status.success());

    // Print everything
    let output = run_ledgerbot(&["-l", &ledger_path, "print"]);
    assert!(output.status.success());
    assert_eq!(count_newlines(&output.stdout), drops);

    // Only print the first 5 items
    let output = run_ledgerbot(&["-l", &ledger_path, "-n", "5", "print"]);
    assert!(output.status.success());
    assert_eq!(count_newlines(&output.stdout), 5);

    // Skip entries with no hashes
    let output = run_ledgerbot(&["-l", &ledger_path, "-h", "1", "print"]);
    assert!(output.status.success());
    assert_eq!(count_newlines(&output.stdout), drops);

    // Skip entries with fewer than 2 hashes (skip everything)
    let output = run_ledgerbot(&["-l", &ledger_path, "-h", "2", "print"]);
    assert!(output.status.success());
    assert_eq!(count_newlines(&output.stdout), 0);
}
