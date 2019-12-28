use serde_json::{json, Value};
use morgan::verifier::new_validator_for_tests;
use morgan_client::rpc_client::RpcClient;
use morgan_client::rpc_request::RpcRequest;
use morgan_tokenbot::drone::run_local_drone;
use morgan_interface::bvm_loader;
use morgan_wallet::wallet::{process_command, WalletCommand, WalletConfig};
use std::fs::{remove_dir_all, File};
use std::io::Read;
use std::path::PathBuf;
use std::sync::mpsc::channel;

#[test]
fn test_wallet_deploy_program() {
    morgan_logger::setup();

    let mut pathbuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    pathbuf.push("tests");
    pathbuf.push("fixtures");
    pathbuf.push("noop");
    pathbuf.set_extension("so");

    let (server, leader_data, alice, ledger_path) = new_validator_for_tests();

    let (sender, receiver) = channel();
    run_local_drone(alice, sender, None);
    let drone_addr = receiver.recv().unwrap();

    let rpc_client = RpcClient::new_socket(leader_data.rpc);

    let mut config = WalletConfig::default();
    config.drone_port = drone_addr.port();
    config.json_rpc_url = format!("http://{}:{}", leader_data.rpc.ip(), leader_data.rpc.port());
    config.command = WalletCommand::Airdrop(50);
    process_command(&config).unwrap();

    config.command = WalletCommand::Deploy(pathbuf.to_str().unwrap().to_string());

    let response = process_command(&config);
    let json: Value = serde_json::from_str(&response.unwrap()).unwrap();
    let program_id_str = json
        .as_object()
        .unwrap()
        .get("programId")
        .unwrap()
        .as_str()
        .unwrap();

    let params = json!([program_id_str]);
    let account_info = rpc_client
        .retry_make_rpc_request(&RpcRequest::GetAccountInfo, Some(params), 0)
        .unwrap();
    let account_info_obj = account_info.as_object().unwrap();
    assert_eq!(
        account_info_obj.get("difs").unwrap().as_u64().unwrap(),
        1
    );
    let owner_array = account_info.get("owner").unwrap();
    assert_eq!(owner_array, &json!(bvm_loader::id()));
    assert_eq!(
        account_info_obj
            .get("executable")
            .unwrap()
            .as_bool()
            .unwrap(),
        true
    );

    let mut file = File::open(pathbuf.to_str().unwrap().to_string()).unwrap();
    let mut elf = Vec::new();
    file.read_to_end(&mut elf).unwrap();

    assert_eq!(
        account_info_obj.get("data").unwrap().as_array().unwrap(),
        &elf
    );

    server.close().unwrap();
    remove_dir_all(ledger_path).unwrap();
}
