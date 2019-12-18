use bincode::serialize;
use log::*;
use reqwest;
use reqwest::header::CONTENT_TYPE;
use serde_json::{json, Value};
use morgan::verifier::new_validator_for_tests;
use morgan_client::rpc_client::get_rpc_request_str;
use morgan_interface::hash::Hash;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::system_transaction;
use std::fs::remove_dir_all;
use std::thread::sleep;
use std::time::Duration;
use morgan_helper::logHelper::*;

#[test]
fn test_rpc_send_tx() {
    morgan_logger::setup();

    let (server, leader_data, alice, ledger_path) = new_validator_for_tests();
    let bob_pubkey = Pubkey::new_rand();

    let client = reqwest::Client::new();
    let request = json!({
       "jsonrpc": "2.0",
       "id": 1,
       "method": "getLatestTransactionSeal",
       "params": json!([])
    });
    let rpc_addr = leader_data.rpc;
    let rpc_string = get_rpc_request_str(rpc_addr, false);
    let mut response = client
        .post(&rpc_string)
        .header(CONTENT_TYPE, "application/json")
        .body(request.to_string())
        .send()
        .unwrap();
    let json: Value = serde_json::from_str(&response.text().unwrap()).unwrap();
    let transaction_seal: Hash = json["result"][0].as_str().unwrap().parse().unwrap();

    // info!("{}", Info(format!("transaction_seal: {:?}", transaction_seal).to_string()));
    println!("{}",
        printLn(
            format!("transaction_seal: {:?}", transaction_seal).to_string(),
            module_path!().to_string()
        )
    );
    let tx = system_transaction::transfer(&alice, &bob_pubkey, 20, transaction_seal);
    let serial_tx = serialize(&tx).unwrap();

    let client = reqwest::Client::new();
    let request = json!({
       "jsonrpc": "2.0",
       "id": 1,
       "method": "sendTxn",
       "params": json!([serial_tx])
    });
    let rpc_addr = leader_data.rpc;
    let rpc_string = get_rpc_request_str(rpc_addr, false);
    let mut response = client
        .post(&rpc_string)
        .header(CONTENT_TYPE, "application/json")
        .body(request.to_string())
        .send()
        .unwrap();
    let json: Value = serde_json::from_str(&response.text().unwrap()).unwrap();
    let signature = &json["result"];

    let mut confirmed_tx = false;

    let client = reqwest::Client::new();
    let request = json!({
       "jsonrpc": "2.0",
       "id": 1,
       "method": "confirmTxn",
       "params": [signature],
    });

    for _ in 0..morgan_interface::timing::DEFAULT_DROPS_PER_SLOT {
        let mut response = client
            .post(&rpc_string)
            .header(CONTENT_TYPE, "application/json")
            .body(request.to_string())
            .send()
            .unwrap();
        let response_json_text = response.text().unwrap();
        let json: Value = serde_json::from_str(&response_json_text).unwrap();

        if true == json["result"] {
            confirmed_tx = true;
            break;
        }

        sleep(Duration::from_millis(500));
    }

    assert_eq!(confirmed_tx, true);

    server.close().unwrap();
    remove_dir_all(ledger_path).unwrap();
}
