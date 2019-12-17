#[macro_use]
extern crate morgan;

use log::*;
use morgan::treasury_phase::create_test_recorder;
use morgan::block_buffer_pool::{create_new_tmp_ledger, BlockBufferPool};
use morgan::node_group_info::{NodeGroupInfo, Node};
use morgan::entry_info::next_entry_mut;
use morgan::entry_info::EntrySlice;
use morgan::genesis_utils::{create_genesis_block_with_leader, GenesisBlockInfo};
use morgan::gossip_service::GossipService;
use morgan::packet::index_blobs;
use morgan::rpc_subscriptions::RpcSubscriptions;
use morgan::service::Service;
use morgan::storage_stage::StorageState;
use morgan::storage_stage::STORAGE_ROTATE_TEST_COUNT;
use morgan::streamer;
use morgan::transaction_verify_centre::{Sockets, Tvu};
use morgan::verifier;
use morgan_runtime::epoch_schedule::MINIMUM_SLOT_LENGTH;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::system_transaction;
use std::fs::remove_dir_all;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use morgan_helper::logHelper::*;

fn new_gossip(
    node_group_info: Arc<RwLock<NodeGroupInfo>>,
    gossip: UdpSocket,
    exit: &Arc<AtomicBool>,
) -> GossipService {
    GossipService::new(&node_group_info, None, None, gossip, exit)
}

/// Test that message sent from leader to target1 and replayed to target2
#[test]
fn test_replay() {
    morgan_logger::setup();
    let leader = Node::new_localhost();
    let target1_keypair = Keypair::new();
    let target1 = Node::new_localhost_with_pubkey(&target1_keypair.pubkey());
    let target2 = Node::new_localhost();
    let exit = Arc::new(AtomicBool::new(false));

    // start cluster_info_l
    let cluster_info_l = NodeGroupInfo::new_with_invalid_keypair(leader.info.clone());

    let cref_l = Arc::new(RwLock::new(cluster_info_l));
    let dr_l = new_gossip(cref_l, leader.sockets.gossip, &exit);

    // start cluster_info2
    let mut cluster_info2 = NodeGroupInfo::new_with_invalid_keypair(target2.info.clone());
    cluster_info2.insert_info(leader.info.clone());
    let cref2 = Arc::new(RwLock::new(cluster_info2));
    let dr_2 = new_gossip(cref2, target2.sockets.gossip, &exit);

    // setup some blob services to send blobs into the socket
    // to simulate the genesis peer and get blobs out of the socket to
    // simulate target peer
    let (s_reader, r_reader) = channel();
    let blob_sockets: Vec<Arc<UdpSocket>> = target2.sockets.tvu.into_iter().map(Arc::new).collect();

    let t_receiver = streamer::blob_receiver(blob_sockets[0].clone(), &exit, s_reader);

    // simulate leader sending messages
    let (s_responder, r_responder) = channel();
    let t_responder = streamer::responder(
        "test_replay",
        Arc::new(leader.sockets.retransmit),
        r_responder,
    );

    let mint_balance = 10_000;
    let leader_balance = 100;
    let GenesisBlockInfo {
        mut genesis_block,
        mint_keypair,
        ..
    } = create_genesis_block_with_leader(mint_balance, &leader.info.id, leader_balance);
    genesis_block.ticks_per_slot = 160;
    genesis_block.slots_per_epoch = MINIMUM_SLOT_LENGTH as u64;
    let (block_buffer_pool_path, transaction_seal) = create_new_tmp_ledger!(&genesis_block);

    let tvu_addr = target1.info.tvu;

    let (
        treasury_forks,
        _treasury_forks_info,
        block_buffer_pool,
        ledger_signal_receiver,
        completed_slots_receiver,
        leader_schedule_cache,
        _,
    ) = verifier::new_treasuries_from_block_buffer(&block_buffer_pool_path, None);
    let working_treasury = treasury_forks.working_treasury();
    assert_eq!(
        working_treasury.get_balance(&mint_keypair.pubkey()),
        mint_balance
    );

    let leader_schedule_cache = Arc::new(leader_schedule_cache);
    // start cluster_info1
    let treasury_forks = Arc::new(RwLock::new(treasury_forks));
    let mut cluster_info1 = NodeGroupInfo::new_with_invalid_keypair(target1.info.clone());
    cluster_info1.insert_info(leader.info.clone());
    let cref1 = Arc::new(RwLock::new(cluster_info1));
    let dr_1 = new_gossip(cref1.clone(), target1.sockets.gossip, &exit);

    let voting_keypair = Keypair::new();
    let storage_keypair = Arc::new(Keypair::new());
    let block_buffer_pool = Arc::new(block_buffer_pool);
    {
        let (waterclock_service_exit, waterclock_recorder, waterclock_service, _entry_receiver) =
            create_test_recorder(&working_treasury, &block_buffer_pool);
        let tvu = Tvu::new(
            &voting_keypair.pubkey(),
            Some(&Arc::new(voting_keypair)),
            &storage_keypair,
            &treasury_forks,
            &cref1,
            {
                Sockets {
                    repair: target1.sockets.repair,
                    retransmit: target1.sockets.retransmit,
                    fetch: target1.sockets.tvu,
                }
            },
            block_buffer_pool,
            STORAGE_ROTATE_TEST_COUNT,
            &StorageState::default(),
            None,
            ledger_signal_receiver,
            &Arc::new(RpcSubscriptions::default()),
            &waterclock_recorder,
            &leader_schedule_cache,
            &exit,
            &morgan_interface::hash::Hash::default(),
            completed_slots_receiver,
        );

        let mut mint_ref_balance = mint_balance;
        let mut msgs = Vec::new();
        let mut blob_idx = 0;
        let num_transfers = 10;
        let mut transfer_amount = 501;
        let bob_keypair = Keypair::new();
        let mut cur_hash = transaction_seal;
        for i in 0..num_transfers {
            let entry0 = next_entry_mut(&mut cur_hash, i, vec![]);
            let entry_tick0 = next_entry_mut(&mut cur_hash, i + 1, vec![]);

            let tx0 = system_transaction::create_user_account(
                &mint_keypair,
                &bob_keypair.pubkey(),
                transfer_amount,
                transaction_seal,
            );
            let entry_tick1 = next_entry_mut(&mut cur_hash, i + 1, vec![]);
            let entry1 = next_entry_mut(&mut cur_hash, i + num_transfers, vec![tx0]);
            let entry_tick2 = next_entry_mut(&mut cur_hash, i + 1, vec![]);

            mint_ref_balance -= transfer_amount;
            transfer_amount -= 1; // Sneaky: change transfer_amount slightly to avoid DuplicateSignature errors

            let entries = vec![entry0, entry_tick0, entry_tick1, entry1, entry_tick2];
            let blobs = entries.to_shared_blobs();
            index_blobs(&blobs, &leader.info.id, blob_idx, 1, 0);
            blob_idx += blobs.len() as u64;
            blobs
                .iter()
                .for_each(|b| b.write().unwrap().meta.set_addr(&tvu_addr));
            msgs.extend(blobs.into_iter());
        }

        // send the blobs into the socket
        s_responder.send(msgs).expect("send");
        drop(s_responder);

        // receive retransmitted messages
        let timer = Duration::new(1, 0);
        while let Ok(_msg) = r_reader.recv_timeout(timer) {
            info!("{}", Info(format!("got msg").to_string()));
            println!("{}",
                printLn(
                    format!("got msg").to_string(),
                    module_path!().to_string()
                )
            );
        }

        let working_treasury = treasury_forks.read().unwrap().working_treasury();
        let final_mint_balance = working_treasury.get_balance(&mint_keypair.pubkey());
        assert_eq!(final_mint_balance, mint_ref_balance);

        let bob_balance = working_treasury.get_balance(&bob_keypair.pubkey());
        assert_eq!(bob_balance, mint_balance - mint_ref_balance);

        exit.store(true, Ordering::Relaxed);
        waterclock_service_exit.store(true, Ordering::Relaxed);
        waterclock_service.join().unwrap();
        tvu.join().unwrap();
        dr_l.join().unwrap();
        dr_2.join().unwrap();
        dr_1.join().unwrap();
        t_receiver.join().unwrap();
        t_responder.join().unwrap();
    }
    BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    let _ignored = remove_dir_all(&block_buffer_pool_path);
}
