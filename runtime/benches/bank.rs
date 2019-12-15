#![feature(test)]

extern crate test;

use log::*;
use morgan_runtime::treasury::*;
use morgan_runtime::treasury_client::BankClient;
use morgan_runtime::loader_utils::{create_invoke_instruction, load_program};
use morgan_interface::account::KeyedAccount;
use morgan_interface::client::AsyncClient;
use morgan_interface::client::SyncClient;
use morgan_interface::genesis_block::create_genesis_block;
use morgan_interface::instruction::InstructionError;
use morgan_interface::native_loader;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::transaction::Transaction;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use test::Bencher;
use morgan_helper::logHelper::*;

const BUILTIN_PROGRAM_ID: [u8; 32] = [
    098, 117, 105, 108, 116, 105, 110, 095, 112, 114, 111, 103, 114, 097, 109, 095, 105, 100, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

fn process_instruction(
    _program_id: &Pubkey,
    _keyed_accounts: &mut [KeyedAccount],
    _data: &[u8],
    _tick_height: u64,
) -> Result<(), InstructionError> {
    Ok(())
}

pub fn create_builtin_transactions(
    treasury_client: &BankClient,
    mint_keypair: &Keypair,
) -> Vec<Transaction> {
    let program_id = Pubkey::new(&BUILTIN_PROGRAM_ID);

    (0..4096)
        .into_iter()
        .map(|_| {
            // Seed the signer account
            let rando0 = Keypair::new();
            treasury_client
                .transfer(10_000, &mint_keypair, &rando0.pubkey())
                .expect(&format!("{}:{}", line!(), file!()));

            let instruction = create_invoke_instruction(rando0.pubkey(), program_id, &1u8);
            let (blockhash, _fee_calculator) = treasury_client.get_recent_blockhash().unwrap();
            Transaction::new_signed_instructions(&[&rando0], vec![instruction], blockhash)
        })
        .collect()
}

pub fn create_native_loader_transactions(
    treasury_client: &BankClient,
    mint_keypair: &Keypair,
) -> Vec<Transaction> {
    let program = "morgan_noop_program".as_bytes().to_vec();
    let program_id = load_program(&treasury_client, &mint_keypair, &native_loader::id(), program);

    (0..4096)
        .into_iter()
        .map(|_| {
            // Seed the signer account©41
            let rando0 = Keypair::new();
            treasury_client
                .transfer(10_000, &mint_keypair, &rando0.pubkey())
                .expect(&format!("{}:{}", line!(), file!()));

            let instruction = create_invoke_instruction(rando0.pubkey(), program_id, &1u8);
            let (blockhash, _fee_calculator) = treasury_client.get_recent_blockhash().unwrap();
            Transaction::new_signed_instructions(&[&rando0], vec![instruction], blockhash)
        })
        .collect()
}

fn sync_bencher(treasury: &Arc<Treasury>, _bank_client: &BankClient, transactions: &Vec<Transaction>) {
    let results = treasury.process_transactions(&transactions);
    assert!(results.iter().all(Result::is_ok));
}

fn async_bencher(treasury: &Arc<Treasury>, treasury_client: &BankClient, transactions: &Vec<Transaction>) {
    for transaction in transactions.clone() {
        treasury_client.async_send_transaction(transaction).unwrap();
    }
    for _ in 0..1_000_000_000_u64 {
        if treasury
            .get_signature_status(&transactions.last().unwrap().signatures.get(0).unwrap())
            .is_some()
        {
            break;
        }
        sleep(Duration::from_nanos(1));
    }
    if !treasury
        .get_signature_status(&transactions.last().unwrap().signatures.get(0).unwrap())
        .unwrap()
        .is_ok()
    {
        // error!(
        //     "{}",
        //     Error(format!("transaction failed: {:?}",
        //     treasury.get_signature_status(&transactions.last().unwrap().signatures.get(0).unwrap())
        //         .unwrap()).to_string())
        // );
        println!(
            "{}",
            Error(
                format!("transaction failed: {:?}",
                    treasury.get_signature_status(&transactions.last().unwrap().signatures.get(0).unwrap())
                    .unwrap()).to_string(),
                module_path!().to_string()
            )
        );
        assert!(false);
    }
}

fn do_bench_transactions(
    bencher: &mut Bencher,
    bench_work: &Fn(&Arc<Treasury>, &BankClient, &Vec<Transaction>),
    create_transactions: &Fn(&BankClient, &Keypair) -> Vec<Transaction>,
) {
    morgan_logger::setup();
    let ns_per_s = 1_000_000_000;
    let (genesis_block, mint_keypair) = create_genesis_block(100_000_000);
    let mut treasury = Treasury::new(&genesis_block);
    treasury.add_instruction_processor(Pubkey::new(&BUILTIN_PROGRAM_ID), process_instruction);
    let treasury = Arc::new(treasury);
    let treasury_client = BankClient::new_shared(&treasury);
    let transactions = create_transactions(&treasury_client, &mint_keypair);

    // Do once to fund accounts, load modules, etc...
    let results = treasury.process_transactions(&transactions);
    assert!(results.iter().all(Result::is_ok));

    bencher.iter(|| {
        // Since bencher runs this multiple times, we need to clear the signatures.
        treasury.clear_signatures();
        bench_work(&treasury, &treasury_client, &transactions);
    });

    let summary = bencher.bench(|_bencher| {}).unwrap();
    // info!("{}", Info(format!("  {:?} transactions", transactions.len()).to_string()));
    // info!("{}", Info(format!("  {:?} ns/iter median", summary.median as u64).to_string()));
    println!("{}",
        printLn(
            format!("  {:?} transactions", transactions.len()).to_string(),
            module_path!().to_string()
        )
    );
    println!("{}",
        printLn(
            format!("  {:?} ns/iter median", summary.median as u64).to_string(),
            module_path!().to_string()
        )
    );
    assert!(0f64 != summary.median);
    let tps = transactions.len() as u64 * (ns_per_s / summary.median as u64);
    // info!("{}", Info(format!("  {:?} TPS", tps).to_String()));
    println!("{}",
        printLn(
            format!("  {:?} TPS", tps).to_string(),
            module_path!().to_string()
        )
    );
}

#[bench]
fn bench_treasury_sync_process_builtin_transactions(bencher: &mut Bencher) {
    do_bench_transactions(bencher, &sync_bencher, &create_builtin_transactions);
}

#[bench]
fn bench_bank_sync_process_native_loader_transactions(bencher: &mut Bencher) {
    do_bench_transactions(bencher, &sync_bencher, &create_native_loader_transactions);
}

#[bench]
fn bench_treasury_async_process_builtin_transactions(bencher: &mut Bencher) {
    do_bench_transactions(bencher, &async_bencher, &create_builtin_transactions);
}

#[bench]
fn bench_treasury_async_process_native_loader_transactions(bencher: &mut Bencher) {
    do_bench_transactions(bencher, &async_bencher, &create_native_loader_transactions);
}
