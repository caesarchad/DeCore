//! Defines traits for blocking (synchronous) and non-blocking (asynchronous)
//! communication with a Morgan server as well a a trait that encompasses both.
//!
//! //! Synchronous implementations are expected to create transactions, sign them, and send
//! them with multiple retries, updating transaction_seals and resigning as-needed.
//!
//! Asynchronous implementations are expected to create transactions, sign them, and send
//! them but without waiting to see if the server accepted it.

use crate::gas_cost::GasCost;
use crate::hash::Hash;
use crate::opcodes::OpCode;
use crate::context::Context;
use crate::bvm_address::BvmAddr;
use crate::signature::{Keypair, Signature};
use crate::transaction;
use crate::transport::Result;
use std::io;

pub trait AccountHost: OnlineAccount + OfflineAccount {
    fn account_host_url(&self) -> String;
}

pub trait OnlineAccount {
    /// Create a transaction from the given message, and send it to the
    /// server, retrying as-needed.
    fn snd_online_context(&self, keypairs: &[&Keypair], context: Context) -> Result<Signature>;

    /// Create a transaction from a single instruction that only requires
    /// a single signer. Then send it to the server, retrying as-needed.
    fn snd_online_instruction(&self, keypair: &Keypair, instruction: OpCode) -> Result<Signature>;

    /// Transfer difs from `keypair` to `address`, retrying until the
    /// transfer completes or produces and error.
    fn online_transfer(&self, difs: u64, keypair: &Keypair, address: &BvmAddr) -> Result<Signature>;

    /// Get an account or None if not found.
    fn get_account_data(&self, address: &BvmAddr) -> Result<Option<Vec<u8>>>;

    /// Get account balance or 0 if not found.
    fn get_balance(&self, address: &BvmAddr) -> Result<u64>;

    /// Get signature status.
    fn get_signature_status(
        &self,
        signature: &Signature,
    ) -> Result<Option<transaction::Result<()>>>;

    /// Get recent transaction_seal
    fn get_recent_transaction_seal(&self) -> Result<(Hash, GasCost)>;

    /// Get transaction count
    fn get_transaction_count(&self) -> Result<u64>;

    /// Poll until the signature has been confirmed by at least `min_confirmed_blocks`
    fn poll_for_signature_confirmation(
        &self,
        signature: &Signature,
        min_confirmed_blocks: usize,
    ) -> Result<()>;

    /// Poll to confirm a transaction.
    fn poll_for_signature(&self, signature: &Signature) -> Result<()>;

    fn get_new_transaction_seal(&self, transaction_seal: &Hash) -> Result<(Hash, GasCost)>;
}

pub trait OfflineAccount {
    /// Send a signed transaction, but don't wait to see if the server accepted it.
    fn send_offline_transaction(
        &self,
        transaction: transaction::Transaction,
    ) -> io::Result<Signature>;

    /// Create a transaction from the given message, and send it to the
    /// server, but don't wait for to see if the server accepted it.
    fn snd_offline_context(
        &self,
        keypairs: &[&Keypair],
        context: Context,
        recent_transaction_seal: Hash,
    ) -> io::Result<Signature>;

    /// Create a transaction from a single instruction that only requires
    /// a single signer. Then send it to the server, but don't wait for a reply.
    fn send_offline_instruction(
        &self,
        keypair: &Keypair,
        instruction: OpCode,
        recent_transaction_seal: Hash,
    ) -> io::Result<Signature>;

    /// Attempt to transfer difs from `keypair` to `address`, but don't wait to confirm.
    fn offline_transfer(
        &self,
        difs: u64,
        keypair: &Keypair,
        address: &BvmAddr,
        recent_transaction_seal: Hash,
    ) -> io::Result<Signature>;
}
