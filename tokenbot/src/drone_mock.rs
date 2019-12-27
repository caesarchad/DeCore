use morgan_interface::hash::Hash;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::sys_controller;
use morgan_interface::transaction::Transaction;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;

pub fn request_airdrop_transaction(
    _drone_addr: &SocketAddr,
    _id: &BvmAddr,
    difs: u64,
    _transaction_seal: Hash,
) -> Result<Transaction, Error> {
    if difs == 0 {
        Err(Error::new(ErrorKind::Other, "Airdrop failed"))?
    }
    let key = Keypair::new();
    let to = BvmAddr::new_rand();
    let transaction_seal = Hash::default();
    let tx = sys_controller::create_user_account(&key, &to, difs, transaction_seal);
    Ok(tx)
}
