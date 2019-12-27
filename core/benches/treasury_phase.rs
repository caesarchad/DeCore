#![feature(test)]

extern crate test;
#[macro_use]
extern crate morgan;

use log::*;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use morgan::treasury_phase::{create_test_recorder, TreasuryPhase};
use morgan::block_buffer_pool::{fetch_interim_ledger_location, BlockBufferPool};
use morgan::node_group_info::NodeGroupInfo;
use morgan::node_group_info::Node;
use morgan::genesis_utils::{create_genesis_block, GenesisBlockInfo};
use morgan::packet::to_packets_chunked;
use morgan::water_clock_recorder::WorkingTreasuryEntries;
use morgan::service::Service;
use morgan::test_tx::test_tx;
use morgan_runtime::treasury::Treasury;
use morgan_interface::hash::hash;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::signature::Signature;
use morgan_interface::sys_controller;
use morgan_interface::timing::{duration_as_ms, timestamp,};
use morgan_interface::constants::{DEFAULT_DROPS_PER_SLOT, MAX_RECENT_TRANSACTION_SEALS,};
use std::iter;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use test::Bencher;

fn check_txs(receiver: &Arc<Receiver<WorkingTreasuryEntries>>, ref_tx_count: usize) {
    let mut total = 0;
    loop {
        let entries = receiver.recv_timeout(Duration::new(1, 0));
        if let Ok((_, entries)) = entries {
            for (entry, _) in &entries {
                total += entry.transactions.len();
            }
        } else {
            break;
        }
        if total >= ref_tx_count {
            break;
        }
    }
    assert_eq!(total, ref_tx_count);
}

#[bench]
fn bench_consume_buffered(bencher: &mut Bencher) {
    let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(100_000);
    let treasury = Arc::new(Treasury::new(&genesis_block));
    let ledger_path = fetch_interim_ledger_location!();
    let my_pubkey = Pubkey::new_rand();
    {
        let block_buffer_pool = Arc::new(
            BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger"),
        );
        let (exit, waterclock_recorder, waterclock_service, _signal_receiver) =
            create_test_recorder(&treasury, &block_buffer_pool);

        let tx = test_tx();
        let len = 4096;
        let chunk_size = 1024;
        let batches = to_packets_chunked(&vec![tx; len], chunk_size);
        let mut packets = vec![];
        for batch in batches {
            let batch_len = batch.packets.len();
            packets.push((batch, vec![0usize; batch_len]));
        }
        // This tests the performance of buffering packets.
        // If the packet buffers are copied, performance will be poor.
        bencher.iter(move || {
            let _ignored =
                TreasuryPhase::consume_buffered_packets(&my_pubkey, &waterclock_recorder, &mut packets);
        });

        exit.store(true, Ordering::Relaxed);
        waterclock_service.join().unwrap();
    }
    let _unused = BlockBufferPool::remove_ledger_file(&ledger_path);
}

#[bench]
fn bench_treasury_phase_multi_accounts(bencher: &mut Bencher) {
    morgan_logger::setup();
    let num_threads = TreasuryPhase::num_threads() as usize;
    //   a multiple of packet chunk  2X duplicates to avoid races
    let txes = 192 * num_threads * 2;
    let mint_total = 1_000_000_000_000;
    let GenesisBlockInfo {
        mut genesis_block,
        mint_keypair,
        ..
    } = create_genesis_block(mint_total);

    // Set a high drops_per_slot so we don't run out of drops
    // during the benchmark
    genesis_block.drops_per_slot = 10_000;

    let (verified_sender, verified_receiver) = channel();
    let (vote_sender, vote_receiver) = channel();
    let treasury = Arc::new(Treasury::new(&genesis_block));
    let to_pubkey = Pubkey::new_rand();
    let dummy = sys_controller::transfer(&mint_keypair, &to_pubkey, 1, genesis_block.hash());
    trace!("txs: {}", txes);
    let transactions: Vec<_> = (0..txes)
        .into_par_iter()
        .map(|_| {
            let mut new = dummy.clone();
            let from: Vec<u8> = (0..64).map(|_| thread_rng().gen()).collect();
            let to: Vec<u8> = (0..64).map(|_| thread_rng().gen()).collect();
            let sig: Vec<u8> = (0..64).map(|_| thread_rng().gen()).collect();
            new.message.account_keys[0] = Pubkey::new(&from[0..32]);
            new.message.account_keys[1] = Pubkey::new(&to[0..32]);
            new.signatures = vec![Signature::new(&sig[0..64])];
            new
        })
        .collect();
    // fund all the accounts
    transactions.iter().for_each(|tx| {
        let fund = sys_controller::transfer(
            &mint_keypair,
            &tx.message.account_keys[0],
            mint_total / txes as u64,
            genesis_block.hash(),
        );
        let x = treasury.process_transaction(&fund);
        x.unwrap();
    });
    //sanity check, make sure all the transactions can execute sequentially
    transactions.iter().for_each(|tx| {
        let res = treasury.process_transaction(&tx);
        assert!(res.is_ok(), "sanity test transactions");
    });
    treasury.clear_signatures();
    //sanity check, make sure all the transactions can execute in parallel
    let res = treasury.process_transactions(&transactions);
    for r in res {
        assert!(r.is_ok(), "sanity parallel execution");
    }
    treasury.clear_signatures();
    let verified: Vec<_> = to_packets_chunked(&transactions.clone(), 192)
        .into_iter()
        .map(|x| {
            let len = x.packets.len();
            (x, iter::repeat(1).take(len).collect())
        })
        .collect();
    let ledger_path = fetch_interim_ledger_location!();
    {
        let block_buffer_pool = Arc::new(
            BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger"),
        );
        let (exit, waterclock_recorder, waterclock_service, signal_receiver) =
            create_test_recorder(&treasury, &block_buffer_pool);
        let node_group_info = NodeGroupInfo::new_with_invalid_keypair(Node::new_localhost().info);
        let node_group_info = Arc::new(RwLock::new(node_group_info));
        let _treasury_phase = TreasuryPhase::new(
            &node_group_info,
            &waterclock_recorder,
            verified_receiver,
            vote_receiver,
        );
        waterclock_recorder.lock().unwrap().set_treasury(&treasury);

        let half_len = verified.len() / 2;
        let mut start = 0;

        // This is so that the signal_receiver does not go out of scope after the closure.
        // If it is dropped before waterclock_service, then waterclock_service will error when
        // calling send() on the channel.
        let signal_receiver = Arc::new(signal_receiver);
        let signal_receiver2 = signal_receiver.clone();
        bencher.iter(move || {
            let now = Instant::now();
            for v in verified[start..start + half_len].chunks(verified.len() / num_threads) {
                trace!("sending... {}..{} {}", start, start + half_len, timestamp());
                verified_sender.send(v.to_vec()).unwrap();
            }
            check_txs(&signal_receiver2, txes / 2);
            trace!(
                "time: {} checked: {}",
                duration_as_ms(&now.elapsed()),
                txes / 2
            );
            treasury.clear_signatures();
            start += half_len;
            start %= verified.len();
        });
        drop(vote_sender);
        exit.store(true, Ordering::Relaxed);
        waterclock_service.join().unwrap();
    }
    let _unused = BlockBufferPool::remove_ledger_file(&ledger_path);
}

#[bench]
#[ignore]
fn bench_treasury_phase_multi_programs(bencher: &mut Bencher) {
    let progs = 4;
    let num_threads = TreasuryPhase::num_threads() as usize;
    //   a multiple of packet chunk  2X duplicates to avoid races
    let txes = 96 * 100 * num_threads * 2;
    let mint_total = 1_000_000_000_000;
    let GenesisBlockInfo {
        genesis_block,
        mint_keypair,
        ..
    } = create_genesis_block(mint_total);

    let (verified_sender, verified_receiver) = channel();
    let (vote_sender, vote_receiver) = channel();
    let treasury = Arc::new(Treasury::new(&genesis_block));
    let to_pubkey = Pubkey::new_rand();
    let dummy = sys_controller::transfer(&mint_keypair, &to_pubkey, 1, genesis_block.hash());
    let transactions: Vec<_> = (0..txes)
        .into_par_iter()
        .map(|_| {
            let mut new = dummy.clone();
            let from: Vec<u8> = (0..32).map(|_| thread_rng().gen()).collect();
            let sig: Vec<u8> = (0..64).map(|_| thread_rng().gen()).collect();
            let to: Vec<u8> = (0..32).map(|_| thread_rng().gen()).collect();
            new.message.account_keys[0] = Pubkey::new(&from[0..32]);
            new.message.account_keys[1] = Pubkey::new(&to[0..32]);
            let prog = new.message.instructions[0].clone();
            for i in 1..progs {
                //generate programs that spend to random keys
                let to: Vec<u8> = (0..32).map(|_| thread_rng().gen()).collect();
                let to_key = Pubkey::new(&to[0..32]);
                new.message.account_keys.push(to_key);
                assert_eq!(new.message.account_keys.len(), i + 2);
                new.message.instructions.push(prog.clone());
                assert_eq!(new.message.instructions.len(), i + 1);
                new.message.instructions[i].accounts[1] = 1 + i as u8;
                assert_eq!(new.key(i, 1), Some(&to_key));
                assert_eq!(
                    new.message.account_keys[new.message.instructions[i].accounts[1] as usize],
                    to_key
                );
            }
            assert_eq!(new.message.instructions.len(), progs);
            new.signatures = vec![Signature::new(&sig[0..64])];
            new
        })
        .collect();
    transactions.iter().for_each(|tx| {
        let fund = sys_controller::transfer(
            &mint_keypair,
            &tx.message.account_keys[0],
            mint_total / txes as u64,
            genesis_block.hash(),
        );
        treasury.process_transaction(&fund).unwrap();
    });
    //sanity check, make sure all the transactions can execute sequentially
    transactions.iter().for_each(|tx| {
        let res = treasury.process_transaction(&tx);
        assert!(res.is_ok(), "sanity test transactions");
    });
    treasury.clear_signatures();
    //sanity check, make sure all the transactions can execute in parallel
    let res = treasury.process_transactions(&transactions);
    for r in res {
        assert!(r.is_ok(), "sanity parallel execution");
    }
    treasury.clear_signatures();
    let verified: Vec<_> = pkt_bndl(&transactions.clone(), 96)
        .into_iter()
        .map(|x| {
            let len = x.packets.len();
            (x, iter::repeat(1).take(len).collect())
        })
        .collect();

    let ledger_path = fetch_interim_ledger_location!();
    {
        let block_buffer_pool = Arc::new(
            BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger"),
        );
        let (exit, waterclock_recorder, waterclock_service, signal_receiver) =
            create_test_recorder(&treasury, &block_buffer_pool);
        let node_group_info = NodeGroupInfo::new_with_invalid_keypair(Node::new_localhost().info);
        let node_group_info = Arc::new(RwLock::new(node_group_info));
        let _treasury_phase = TreasuryPhase::new(
            &node_group_info,
            &waterclock_recorder,
            verified_receiver,
            vote_receiver,
        );
        waterclock_recorder.lock().unwrap().set_treasury(&treasury);

        let mut id = genesis_block.hash();
        for _ in 0..(MAX_RECENT_TRANSACTION_SEALS * DEFAULT_DROPS_PER_SLOT as usize) {
            id = hash(&id.as_ref());
            treasury.register_drop(&id);
        }

        let half_len = verified.len() / 2;
        let mut start = 0;
        let signal_receiver = Arc::new(signal_receiver);
        let signal_receiver2 = signal_receiver.clone();
        bencher.iter(move || {
            // make sure the transactions are still valid
            treasury.register_drop(&genesis_block.hash());
            for v in verified[start..start + half_len].chunks(verified.len() / num_threads) {
                verified_sender.send(v.to_vec()).unwrap();
            }
            check_txs(&signal_receiver2, txes / 2);
            treasury.clear_signatures();
            start += half_len;
            start %= verified.len();
        });
        drop(vote_sender);
        exit.store(true, Ordering::Relaxed);
        waterclock_service.join().unwrap();
    }
    BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
}
