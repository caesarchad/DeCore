#[macro_use]
extern crate log;

#[macro_use]
extern crate morgan;

use bincode::{deserialize, serialize};
use morgan::block_buffer_pool::{create_new_tmp_ledger, BlockBufferPool};
use morgan::node_group_info::{NodeGroupInfo, Node, FULLNODE_PORT_RANGE};
use morgan::connection_info::ContactInfo;
use morgan::gossip_service::find_node_group_host;
use morgan::local_node_group::{NodeGroupConfig, LocalNodeGroup};
use morgan::cloner::StorageMiner;
use morgan::cloner::StorageMinerRequest;
use morgan::storage_phase::STORAGE_ROTATE_TEST_COUNT;
use morgan::streamer::blob_receiver;
use morgan::verifier::ValidatorConfig;
use morgan_client::thin_client::create_client;
use morgan_interface::genesis_block::create_genesis_block;
use morgan_interface::hash::Hash;
use morgan_interface::signature::{Keypair, KeypairUtil};
use std::fs::remove_dir_all;
use std::net::SocketAddr;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use morgan_helper::logHelper::*;

fn get_slot_height(to: SocketAddr) -> u64 {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let req = StorageMinerRequest::GetSlotHeight(socket.local_addr().unwrap());
    let serialized_req = serialize(&req).unwrap();
    for _ in 0..10 {
        socket.send_to(&serialized_req, to).unwrap();
        let mut buf = [0; 1024];
        if let Ok((size, _addr)) = socket.recv_from(&mut buf) {
            return deserialize(&buf[..size]).unwrap();
        }
        sleep(Duration::from_millis(500));
    }
    panic!("Couldn't get slot height!");
}

fn check_miner_connection(storage_miner_info: &ContactInfo) {
    // Create a client which downloads from the storage-miner and see that it
    // can respond with blobs.
    let tn = Node::new_localhost();
    let node_group_info = NodeGroupInfo::new_with_invalid_keypair(tn.info.clone());
    let mut repair_index = get_slot_height(storage_miner_info.storage_addr);
    println!("{}",
        printLn(
            format!("repair index: {}", repair_index).to_string(),
            module_path!().to_string()
        )
    );
    repair_index = 0;
    let req = node_group_info
        .window_index_request_bytes(0, repair_index)
        .unwrap();

    let exit = Arc::new(AtomicBool::new(false));
    let (s_reader, r_reader) = channel();
    let repair_socket = Arc::new(tn.sockets.repair);
    let t_receiver = blob_receiver(repair_socket.clone(), &exit, s_reader);

    println!("{}",
        printLn(
            format!("Sending repair requests from: {} to: {}",
                tn.info.id, storage_miner_info.gossip).to_string(),
            module_path!().to_string()
        )
    );
    let mut received_blob = false;
    for _ in 0..5 {
        repair_socket.send_to(&req, storage_miner_info.gossip).unwrap();

        let x = r_reader.recv_timeout(Duration::new(1, 0));

        if let Ok(blobs) = x {
            for b in blobs {
                let br = b.read().unwrap();
                assert!(br.index() == repair_index);
                println!("{}",
                    printLn(
                        format!("br: {:?}", br).to_string(),
                        module_path!().to_string()
                    )
                );
                let entries = BlockBufferPool::fetch_entry_from_deserialized_blob(&br.data()).unwrap();
                for entry in &entries {
                    println!("{}",
                        printLn(
                            format!("entry: {:?}", entry).to_string(),
                            module_path!().to_string()
                        )
                    );
                    assert_ne!(entry.hash, Hash::default());
                    received_blob = true;
                }
            }
            break;
        }
    }
    exit.store(true, Ordering::Relaxed);
    t_receiver.join().unwrap();

    assert!(received_blob);
}

/// Start the cluster with the given configuration and wait till the miners are discovered
/// Then download blobs from one of them.
fn run_miner_startup_basic(num_nodes: usize, miner_amnt: usize) {
    morgan_logger::setup();
    println!("{}",
        printLn(
            format!("starting storage-miner test").to_string(),
            module_path!().to_string()
        )
    );
    let mut validator_config = ValidatorConfig::default();
    validator_config.storage_rotate_count = STORAGE_ROTATE_TEST_COUNT;
    let config = NodeGroupConfig {
        validator_config,
        miner_amnt,
        node_stakes: vec![100; num_nodes],
        node_group_difs: 10_000,
        ..NodeGroupConfig::default()
    };
    let node_group = LocalNodeGroup::new(&config);

    let (node_group_hosts, node_group_miners) = find_node_group_host(
        &node_group.entry_point_info.gossip,
        num_nodes + miner_amnt,
    )
    .unwrap();
    assert_eq!(
        node_group_hosts.len() + node_group_miners.len(),
        num_nodes + miner_amnt
    );
    let mut storage_miner_cnt = 0;
    let mut storage_miner_info = ContactInfo::default();
    for node in &node_group_miners {
        println!("{}",
            printLn(
                format!("storage: {:?} rpc: {:?}", node.storage_addr, node.rpc).to_string(),
                module_path!().to_string()
            )
        );
        if ContactInfo::is_valid_address(&node.storage_addr) {
            storage_miner_cnt += 1;
            storage_miner_info = node.clone();
        }
    }
    assert_eq!(storage_miner_cnt, miner_amnt);

    check_miner_connection(&storage_miner_info);
}

#[test]
fn test_storage_miner_startup_1_node() {
    run_miner_startup_basic(1, 1);
}

#[test]
fn test_storage_miner_startup_2_nodes() {
    run_miner_startup_basic(2, 1);
}

#[test]
fn test_storage_miner_startup_leader_hang() {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    morgan_logger::setup();
    // info!("{}", Info(format!("starting storage-miner test").to_string()));
    println!("{}",
        printLn(
            format!("starting storage-miner test").to_string(),
            module_path!().to_string()
        )
    );
    let leader_ledger_path = "path_to_leder_ledger_file";
    let (genesis_block, _mint_keypair) = create_genesis_block(10_000);
    let (miner_ledger_path, _blockhash) = create_new_tmp_ledger!(&genesis_block);

    {
        let storage_miner_keypair = Arc::new(Keypair::new());
        let storage_keypair = Arc::new(Keypair::new());

        println!("{}",
            printLn(
                format!("starting storage-miner node").to_string(),
                module_path!().to_string()
            )
        );
        let storage_miner_node = Node::new_localhost_with_pubkey(&storage_miner_keypair.pubkey());

        let fake_gossip = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);
        let leader_info = ContactInfo::new_gossip_entry_point(&fake_gossip);

        let storage_miner_res = StorageMiner::new(
            &miner_ledger_path,
            storage_miner_node,
            leader_info,
            storage_miner_keypair,
            storage_keypair,
        );

        assert!(storage_miner_res.is_err());
    }


    let _ignored = BlockBufferPool::remove_ledger_file(&leader_ledger_path);
    let _ignored = BlockBufferPool::remove_ledger_file(&miner_ledger_path);


    let _ignored = remove_dir_all(&leader_ledger_path);
    let _ignored = remove_dir_all(&miner_ledger_path);
}

#[test]
fn test_storage_miner_startup_ledger_hang() {
    morgan_logger::setup();
    // info!("{}", Info(format!("starting storage-miner test").to_string()));
    println!("{}",
        printLn(
            format!("starting storage-miner test").to_string(),
            module_path!().to_string()
        )
    );
    let mut validator_config = ValidatorConfig::default();
    validator_config.storage_rotate_count = STORAGE_ROTATE_TEST_COUNT;
    let node_group = LocalNodeGroup::new_with_equal_stakes(2, 10_000, 100);;

    // info!("{}", Info(format!("starting storage-miner node").to_string()));
    println!("{}",
        printLn(
            format!("starting storage-miner node").to_string(),
            module_path!().to_string()
        )
    );
    let bad_keys = Arc::new(Keypair::new());
    let storage_keypair = Arc::new(Keypair::new());
    let mut storage_miner_node = Node::new_localhost_with_pubkey(&bad_keys.pubkey());

    // Pass bad TVU sockets to prevent successful ledger download
    storage_miner_node.sockets.tvu = vec![std::net::UdpSocket::bind("0.0.0.0:0").unwrap()];
    let (miner_ledger_path, _blockhash) = create_new_tmp_ledger!(&node_group.genesis_block);

    let storage_miner_res = StorageMiner::new(
        &miner_ledger_path,
        storage_miner_node,
        node_group.entry_point_info.clone(),
        bad_keys,
        storage_keypair,
    );

    assert!(storage_miner_res.is_err());
}

#[test]
fn test_account_setup() {
    let num_nodes = 1;
    let miner_amnt = 1;
    let mut validator_config = ValidatorConfig::default();
    validator_config.storage_rotate_count = STORAGE_ROTATE_TEST_COUNT;
    let config = NodeGroupConfig {
        validator_config,
        miner_amnt,
        node_stakes: vec![100; num_nodes],
        node_group_difs: 10_000,
        ..NodeGroupConfig::default()
    };
    let node_group = LocalNodeGroup::new(&config);

    let _ = find_node_group_host(
        &node_group.entry_point_info.gossip,
        num_nodes + miner_amnt as usize,
    )
    .unwrap();
    // now check that the node group actually has accounts for the storage-miner.
    let client = create_client(
        node_group.entry_point_info.client_facing_addr(),
        FULLNODE_PORT_RANGE,
    );
    node_group.storage_miner_infos.iter().for_each(|(_, value)| {
        assert_eq!(
            client
                .poll_get_balance(&value.miner_storage_pubkey)
                .unwrap(),
            1
        );
    });
}
