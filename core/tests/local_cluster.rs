extern crate morgan;

use crate::morgan::block_buffer_pool::BlockBufferPool;
use hashbrown::HashSet;
use morgan::node_group::NodeGroup;
use morgan::node_group_tests;
use morgan::gossip_service::find_node_group_host;
use morgan::local_node_group::{NodeGroupConfig, LocalNodeGroup};
use morgan::verifier::ValidatorConfig;
use morgan_runtime::epoch_schedule::{RoundPlan, MINIMUM_SLOT_LENGTH};
use morgan_interface::waterclock_config::WaterClockConfig;
use morgan_interface::constants::{DEFAULT_DROPS_PER_SLOT};
use std::time::Duration;
use morgan_helper::logHelper::*;

#[test]
fn test_spend_and_verify_all_nodes_1() {
    morgan_logger::setup();
    let num_nodes = 1;
    let local = LocalNodeGroup::new_with_equal_stakes(num_nodes, 10_000, 100);
    node_group_tests::spend_and_verify_all_nodes(
        &local.entry_point_info,
        &local.funding_keypair,
        num_nodes,
    );
}

#[test]
fn test_spend_and_verify_all_nodes_2() {
    morgan_logger::setup();
    let num_nodes = 2;
    let local = LocalNodeGroup::new_with_equal_stakes(num_nodes, 10_000, 100);
    node_group_tests::spend_and_verify_all_nodes(
        &local.entry_point_info,
        &local.funding_keypair,
        num_nodes,
    );
}

#[test]
fn test_spend_and_verify_all_nodes_3() {
    morgan_logger::setup();
    let num_nodes = 3;
    let local = LocalNodeGroup::new_with_equal_stakes(num_nodes, 10_000, 100);
    node_group_tests::spend_and_verify_all_nodes(
        &local.entry_point_info,
        &local.funding_keypair,
        num_nodes,
    );
}

#[test]
#[ignore]
fn test_spend_and_verify_all_nodes_env_num_nodes() {
    morgan_logger::setup();
    let num_nodes: usize = std::env::var("NUM_NODES")
        .expect("please set environment variable NUM_NODES")
        .parse()
        .expect("could not parse NUM_NODES as a number");
    let local = LocalNodeGroup::new_with_equal_stakes(num_nodes, 10_000, 100);
    node_group_tests::spend_and_verify_all_nodes(
        &local.entry_point_info,
        &local.funding_keypair,
        num_nodes,
    );
}

#[test]
#[should_panic]
fn test_fullnode_exit_default_config_should_panic() {
    morgan_logger::setup();
    let num_nodes = 2;
    let local = LocalNodeGroup::new_with_equal_stakes(num_nodes, 10_000, 100);
    node_group_tests::fullnode_exit(&local.entry_point_info, num_nodes);
}

#[test]
fn test_fullnode_exit_2() {
    morgan_logger::setup();
    let num_nodes = 2;
    let mut validator_config = ValidatorConfig::default();
    validator_config.rpc_config.enable_fullnode_exit = true;
    let config = NodeGroupConfig {
        node_group_difs: 10_000,
        node_stakes: vec![100; 2],
        validator_config,
        ..NodeGroupConfig::default()
    };
    let local = LocalNodeGroup::new(&config);
    node_group_tests::fullnode_exit(&local.entry_point_info, num_nodes);
}

// NodeGroup needs a supermajority to remain, so the minimum size for this test is 4
#[test]
fn test_leader_failure_4() {
    morgan_logger::setup();
    let num_nodes = 4;
    let mut validator_config = ValidatorConfig::default();
    validator_config.rpc_config.enable_fullnode_exit = true;
    let config = NodeGroupConfig {
        node_group_difs: 10_000,
        node_stakes: vec![100; 4],
        validator_config: validator_config.clone(),
        ..NodeGroupConfig::default()
    };
    let local = LocalNodeGroup::new(&config);
    node_group_tests::kill_entry_and_spend_and_verify_rest(
        &local.entry_point_info,
        &local.funding_keypair,
        num_nodes,
        config.drops_per_slot * config.waterclock_config.target_drop_duration.as_millis() as u64,
    );
}
#[test]
fn test_two_unbalanced_stakes() {
    morgan_logger::setup();
    let mut validator_config = ValidatorConfig::default();
    let num_drops_per_second = 100;
    let num_drops_per_slot = 10;
    let num_slots_per_epoch = MINIMUM_SLOT_LENGTH as u64;

    validator_config.rpc_config.enable_fullnode_exit = true;
    let mut node_group = LocalNodeGroup::new(&NodeGroupConfig {
        node_stakes: vec![999_990, 3],
        node_group_difs: 1_000_000,
        validator_config: validator_config.clone(),
        drops_per_slot: num_drops_per_slot,
        candidate_each_round: num_slots_per_epoch,
        waterclock_config: WaterClockConfig::new_sleep(Duration::from_millis(1000 / num_drops_per_second)),
        ..NodeGroupConfig::default()
    });

    node_group_tests::sleep_n_epochs(
        10.0,
        &node_group.genesis_block.waterclock_config,
        num_drops_per_slot,
        num_slots_per_epoch,
    );
    node_group.close_preserve_ledgers();
    let leader_pubkey = node_group.entry_point_info.id;
    let leader_ledger = node_group.fullnode_infos[&leader_pubkey].ledger_path.clone();
    node_group_tests::verify_ledger_drops(&leader_ledger, num_drops_per_slot as usize);
}

#[test]
#[ignore]
fn test_forwarding() {
    // Set up a node_group where one node is never the leader, so all txs sent to this node
    // will be have to be forwarded in order to be confirmed
    let config = NodeGroupConfig {
        node_stakes: vec![999_990, 3],
        node_group_difs: 2_000_000,
        ..NodeGroupConfig::default()
    };
    let node_group = LocalNodeGroup::new(&config);

    let (node_group_hosts, _) = find_node_group_host(&node_group.entry_point_info.gossip, 2).unwrap();
    assert!(node_group_hosts.len() >= 2);

    let leader_pubkey = node_group.entry_point_info.id;

    let validator_info = node_group_hosts
        .iter()
        .find(|c| c.id != leader_pubkey)
        .unwrap();

    // Confirm that transactions were forwarded to and processed by the leader.
    node_group_tests::send_many_transactions(&validator_info, &node_group.funding_keypair, 20);
}

#[test]
fn test_restart_node() {
    let validator_config = ValidatorConfig::default();
    let candidate_each_round = MINIMUM_SLOT_LENGTH as u64;
    let drops_per_slot = 16;
    let mut node_group = LocalNodeGroup::new(&NodeGroupConfig {
        node_stakes: vec![3],
        node_group_difs: 100,
        validator_config: validator_config.clone(),
        drops_per_slot,
        candidate_each_round,
        ..NodeGroupConfig::default()
    });
    let nodes = node_group.get_node_pubkeys();
    node_group_tests::sleep_n_epochs(
        1.0,
        &node_group.genesis_block.waterclock_config,
        DEFAULT_DROPS_PER_SLOT,
        candidate_each_round,
    );
    node_group.restart_node(nodes[0]);
    node_group_tests::sleep_n_epochs(
        0.5,
        &node_group.genesis_block.waterclock_config,
        DEFAULT_DROPS_PER_SLOT,
        candidate_each_round,
    );
    node_group_tests::send_many_transactions(&node_group.entry_point_info, &node_group.funding_keypair, 1);
}

#[test]
fn test_listener_startup() {
    let config = NodeGroupConfig {
        node_stakes: vec![100; 1],
        node_group_difs: 1_000,
        observer_amnt: 3,
        ..NodeGroupConfig::default()
    };
    let node_group = LocalNodeGroup::new(&config);
    let (node_group_hosts, _) = find_node_group_host(&node_group.entry_point_info.gossip, 4).unwrap();
    assert_eq!(node_group_hosts.len(), 4);
}

#[test]
#[ignore]
fn test_repairman_catchup() {
    run_repairman_catchup(5);
}

fn run_repairman_catchup(num_repairmen: u64) {
    let mut validator_config = ValidatorConfig::default();
    let num_drops_per_second = 100;
    let num_drops_per_slot = 40;
    let num_slots_per_epoch = MINIMUM_SLOT_LENGTH as u64;
    let num_root_buffer_slots = 10;
    // Calculate the leader schedule num_root_buffer slots ahead. Otherwise, if stake_place_holder ==
    // num_slots_per_epoch, and num_slots_per_epoch == MINIMUM_SLOT_LENGTH, then repairmen
    // will stop sending repairs after the last slot in epoch 1 (0-indexed), because the root
    // is at most in the first epoch.
    //
    // For example:
    // Assume:
    // 1) num_slots_per_epoch = 32
    // 2) stake_place_holder = 32
    // 3) MINIMUM_SLOT_LENGTH = 32
    //
    // Then the last slot in epoch 1 is slot 63. After completing slots 0 to 63, the root on the
    // repairee is at most 31. Because, the stake_place_holder == 32, then the max confirmed epoch
    // on the repairee is epoch 1.
    // Thus the repairmen won't send any slots past epoch 1, slot 63 to this repairee until the repairee
    // updates their root, and the repairee can't update their root until they get slot 64, so no progress
    // is made. This is also not accounting for the fact that the repairee may not vote on every slot, so
    // their root could actually be much less than 31. This is why we give a num_root_buffer_slots buffer.
    let stake_place_holder = num_slots_per_epoch + num_root_buffer_slots;

    validator_config.rpc_config.enable_fullnode_exit = true;

    let difs_per_repairman = 1000;

    // Make the repairee_stake small relative to the repairmen stake so that the repairee doesn't
    // get included in the leader schedule, causing slots to get skipped while it's still trying
    // to catch up
    let repairee_stake = 3;
    let node_group_difs = 2 * difs_per_repairman * num_repairmen + repairee_stake;
    let node_stakes: Vec<_> = (0..num_repairmen).map(|_| difs_per_repairman).collect();
    let mut node_group = LocalNodeGroup::new(&NodeGroupConfig {
        node_stakes,
        node_group_difs,
        validator_config: validator_config.clone(),
        drops_per_slot: num_drops_per_slot,
        candidate_each_round: num_slots_per_epoch,
        stake_place_holder,
        waterclock_config: WaterClockConfig::new_sleep(Duration::from_millis(1000 / num_drops_per_second)),
        ..NodeGroupConfig::default()
    });

    let repairman_pubkeys: HashSet<_> = node_group.get_node_pubkeys().into_iter().collect();
    let epoch_schedule = RoundPlan::new(num_slots_per_epoch, stake_place_holder, true);
    let num_warmup_epochs = (epoch_schedule.get_stakers_epoch(0) + 1) as f64;

    // Sleep for longer than the first N warmup epochs, with a one epoch buffer for timing issues
    node_group_tests::sleep_n_epochs(
        num_warmup_epochs + 1.0,
        &node_group.genesis_block.waterclock_config,
        num_drops_per_slot,
        num_slots_per_epoch,
    );

    // Start up a new node, wait for catchup. Backwards repair won't be sufficient because the
    // leader is sending blobs past this validator's first two confirmed epochs. Thus, the repairman
    // protocol will have to kick in for this validator to repair.

    node_group.add_validator(&validator_config, repairee_stake);

    let all_pubkeys = node_group.get_node_pubkeys();
    let repairee_id = all_pubkeys
        .into_iter()
        .find(|x| !repairman_pubkeys.contains(x))
        .unwrap();

    // Wait for repairman protocol to catch this validator up
    node_group_tests::sleep_n_epochs(
        num_warmup_epochs + 1.0,
        &node_group.genesis_block.waterclock_config,
        num_drops_per_slot,
        num_slots_per_epoch,
    );

    node_group.close_preserve_ledgers();
    let validator_ledger_path = node_group.fullnode_infos[&repairee_id].ledger_path.clone();

    // Expect at least the the first two epochs to have been rooted after waiting 3 epochs.
    let num_expected_slots = num_slots_per_epoch * 2;
    let validator_ledger = BlockBufferPool::open_ledger_file(&validator_ledger_path).unwrap();
    let validator_rooted_slots: Vec<_> =
        validator_ledger.based_slot_repeater(0).unwrap().collect();

    if validator_rooted_slots.len() as u64 <= num_expected_slots {
        // error!(
        //     "{}",
        //     Error(format!("Num expected slots: {}, number of rooted slots: {}",
        //     num_expected_slots,
        //     validator_rooted_slots.len()).to_string())
        // );
        println!(
            "{}",
            Error(
                format!("Num expected slots: {}, number of rooted slots: {}",
                    num_expected_slots,
                    validator_rooted_slots.len()).to_string(),
                module_path!().to_string()
            )
        );
    }
    assert!(validator_rooted_slots.len() as u64 > num_expected_slots);
}
