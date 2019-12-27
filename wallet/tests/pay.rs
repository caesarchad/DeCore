use chrono::prelude::*;
use serde_json::Value;
use morgan_client::rpc_client::RpcClient;
use morgan_tokenbot::drone::run_local_drone;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::KeypairUtil;
use morgan_wallet::wallet::{
    process_command, request_and_confirm_airdrop, WalletCommand, WalletConfig,
};
use std::fs::remove_dir_all;
use std::sync::mpsc::channel;

#[cfg(test)]
use morgan::verifier::new_validator_for_tests;

fn check_balance(expected_balance: u64, client: &RpcClient, address: &BvmAddr) {
    let balance = client.retry_get_balance(address, 1).unwrap().unwrap();
    assert_eq!(balance, expected_balance);
}

#[test]
fn test_wallet_timestamp_tx() {
    let (server, leader_data, alice, ledger_path) = new_validator_for_tests();
    let bob_address = BvmAddr::new_rand();

    let (sender, receiver) = channel();
    run_local_drone(alice, sender, None);
    let drone_addr = receiver.recv().unwrap();

    let rpc_client = RpcClient::new_socket(leader_data.rpc);

    let mut config_payer = WalletConfig::default();
    config_payer.drone_port = drone_addr.port();
    config_payer.json_rpc_url =
        format!("http://{}:{}", leader_data.rpc.ip(), leader_data.rpc.port());

    let mut config_witness = WalletConfig::default();
    config_witness.drone_port = config_payer.drone_port;
    config_witness.json_rpc_url = config_payer.json_rpc_url.clone();

    assert_ne!(
        config_payer.keypair.address(),
        config_witness.keypair.address()
    );

    request_and_confirm_airdrop(&rpc_client, &drone_addr, &config_payer.keypair.address(), 50)
        .unwrap();
    check_balance(50, &rpc_client, &config_payer.keypair.address());

    // Make transaction (from config_payer to bob_address) requiring timestamp from config_witness
    let date_string = "\"2018-09-19T17:30:59Z\"";
    let dt: DateTime<Utc> = serde_json::from_str(&date_string).unwrap();
    config_payer.command = WalletCommand::Pay(
        10,
        bob_address,
        Some(dt),
        Some(config_witness.keypair.address()),
        None,
        None,
    );
    let sig_response = process_command(&config_payer);

    let object: Value = serde_json::from_str(&sig_response.unwrap()).unwrap();
    let process_id_str = object.get("processId").unwrap().as_str().unwrap();
    let process_id_vec = bs58::decode(process_id_str)
        .into_vec()
        .expect("base58-encoded public key");
    let process_id = BvmAddr::new(&process_id_vec);

    check_balance(40, &rpc_client, &config_payer.keypair.address()); // config_payer balance
    check_balance(10, &rpc_client, &process_id); // contract balance
    check_balance(0, &rpc_client, &bob_address); // recipient balance

    // Sign transaction by config_witness
    config_witness.command = WalletCommand::TimeElapsed(bob_address, process_id, dt);
    process_command(&config_witness).unwrap();

    check_balance(40, &rpc_client, &config_payer.keypair.address()); // config_payer balance
    check_balance(0, &rpc_client, &process_id); // contract balance
    check_balance(10, &rpc_client, &bob_address); // recipient balance

    server.close().unwrap();
    remove_dir_all(ledger_path).unwrap();
}

#[test]
fn test_wallet_witness_tx() {
    let (server, leader_data, alice, ledger_path) = new_validator_for_tests();
    let bob_address = BvmAddr::new_rand();

    let (sender, receiver) = channel();
    run_local_drone(alice, sender, None);
    let drone_addr = receiver.recv().unwrap();

    let rpc_client = RpcClient::new_socket(leader_data.rpc);

    let mut config_payer = WalletConfig::default();
    config_payer.drone_port = drone_addr.port();
    config_payer.json_rpc_url =
        format!("http://{}:{}", leader_data.rpc.ip(), leader_data.rpc.port());

    let mut config_witness = WalletConfig::default();
    config_witness.drone_port = config_payer.drone_port;
    config_witness.json_rpc_url = config_payer.json_rpc_url.clone();

    assert_ne!(
        config_payer.keypair.address(),
        config_witness.keypair.address()
    );

    request_and_confirm_airdrop(&rpc_client, &drone_addr, &config_payer.keypair.address(), 50)
        .unwrap();

    // Make transaction (from config_payer to bob_address) requiring witness signature from config_witness
    config_payer.command = WalletCommand::Pay(
        10,
        bob_address,
        None,
        None,
        Some(vec![config_witness.keypair.address()]),
        None,
    );
    let sig_response = process_command(&config_payer);

    let object: Value = serde_json::from_str(&sig_response.unwrap()).unwrap();
    let process_id_str = object.get("processId").unwrap().as_str().unwrap();
    let process_id_vec = bs58::decode(process_id_str)
        .into_vec()
        .expect("base58-encoded public key");
    let process_id = BvmAddr::new(&process_id_vec);

    check_balance(40, &rpc_client, &config_payer.keypair.address()); // config_payer balance
    check_balance(10, &rpc_client, &process_id); // contract balance
    check_balance(0, &rpc_client, &bob_address); // recipient balance

    // Sign transaction by config_witness
    config_witness.command = WalletCommand::Endorsement(bob_address, process_id);
    process_command(&config_witness).unwrap();

    check_balance(40, &rpc_client, &config_payer.keypair.address()); // config_payer balance
    check_balance(0, &rpc_client, &process_id); // contract balance
    check_balance(10, &rpc_client, &bob_address); // recipient balance

    server.close().unwrap();
    remove_dir_all(ledger_path).unwrap();
}

#[test]
fn test_wallet_cancel_tx() {
    let (server, leader_data, alice, ledger_path) = new_validator_for_tests();
    let bob_address = BvmAddr::new_rand();

    let (sender, receiver) = channel();
    run_local_drone(alice, sender, None);
    let drone_addr = receiver.recv().unwrap();

    let rpc_client = RpcClient::new_socket(leader_data.rpc);

    let mut config_payer = WalletConfig::default();
    config_payer.drone_port = drone_addr.port();
    config_payer.json_rpc_url =
        format!("http://{}:{}", leader_data.rpc.ip(), leader_data.rpc.port());

    let mut config_witness = WalletConfig::default();
    config_witness.drone_port = config_payer.drone_port;
    config_witness.json_rpc_url = config_payer.json_rpc_url.clone();

    assert_ne!(
        config_payer.keypair.address(),
        config_witness.keypair.address()
    );

    request_and_confirm_airdrop(&rpc_client, &drone_addr, &config_payer.keypair.address(), 50)
        .unwrap();

    // Make transaction (from config_payer to bob_address) requiring witness signature from config_witness
    config_payer.command = WalletCommand::Pay(
        10,
        bob_address,
        None,
        None,
        Some(vec![config_witness.keypair.address()]),
        Some(config_payer.keypair.address()),
    );
    let sig_response = process_command(&config_payer).unwrap();

    let object: Value = serde_json::from_str(&sig_response).unwrap();
    let process_id_str = object.get("processId").unwrap().as_str().unwrap();
    let process_id_vec = bs58::decode(process_id_str)
        .into_vec()
        .expect("base58-encoded public key");
    let process_id = BvmAddr::new(&process_id_vec);

    check_balance(40, &rpc_client, &config_payer.keypair.address()); // config_payer balance
    check_balance(10, &rpc_client, &process_id); // contract balance
    check_balance(0, &rpc_client, &bob_address); // recipient balance

    // Sign transaction by config_witness
    config_payer.command = WalletCommand::Cancel(process_id);
    process_command(&config_payer).unwrap();

    check_balance(50, &rpc_client, &config_payer.keypair.address()); // config_payer balance
    check_balance(0, &rpc_client, &process_id); // contract balance
    check_balance(0, &rpc_client, &bob_address); // recipient balance

    server.close().unwrap();
    remove_dir_all(ledger_path).unwrap();
}
