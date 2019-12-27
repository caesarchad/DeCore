use morgan_tokenbot::drone::{request_airdrop_transaction, run_local_drone};
use morgan_interface::hash::Hash;
use morgan_interface::context::Context;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::sys_opcode;
use morgan_interface::transaction::Transaction;
use std::sync::mpsc::channel;

#[test]
fn test_local_drone() {
    let keypair = Keypair::new();
    let to = BvmAddr::new_rand();
    let difs = 50;
    let transaction_seal = Hash::new(&to.as_ref());
    let create_instruction =
        sys_opcode::create_user_account(&keypair.pubkey(), &to, difs);
    let context = Context::new(vec![create_instruction]);
    let expected_tx = Transaction::new(&[&keypair], context, transaction_seal);

    let (sender, receiver) = channel();
    run_local_drone(keypair, sender, None);
    let drone_addr = receiver.recv().unwrap();

    let result = request_airdrop_transaction(&drone_addr, &to, difs, transaction_seal);
    assert_eq!(expected_tx, result.unwrap());
}
