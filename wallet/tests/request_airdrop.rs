use morgan::verifier::new_validator_for_tests;
use morgan_client::rpc_client::RpcClient;
use morgan_tokenbot::drone::run_local_drone;
use morgan_interface::signature::KeypairUtil;
use morgan_wallet::wallet::{process_command, WalletCommand, WalletConfig};
use std::fs::remove_dir_all;
use std::sync::mpsc::channel;

#[test]
fn test_wallet_request_airdrop() {
    let (server, leader_data, alice, ledger_path) = new_validator_for_tests();
    let (sender, receiver) = channel();
    run_local_drone(alice, sender, None);
    let drone_addr = receiver.recv().unwrap();

    let mut bob_config = WalletConfig::default();
    bob_config.drone_port = drone_addr.port();
    bob_config.json_rpc_url = format!("http://{}:{}", leader_data.rpc.ip(), leader_data.rpc.port());

    bob_config.command = WalletCommand::Airdrop(50);

    let sig_response = process_command(&bob_config);
    sig_response.unwrap();

    let rpc_client = RpcClient::new_socket(leader_data.rpc);

    let balance = rpc_client
        .retry_get_balance(&bob_config.keypair.pubkey(), 1)
        .unwrap()
        .unwrap();
    assert_eq!(balance, 50);

    server.close().unwrap();
    remove_dir_all(ledger_path).unwrap();
}
