//! The `slim_account_host` module is a client-side object that interfaces with
//! a server-side transaction digesting module.  AccountHost code should use this object instead of writing
//! messages to the network directly. The binary encoding of its messages are
//! unstable and may change in future releases.

use crate::rpc_client::RpcClient;
use bincode::{serialize_into, serialized_size};
use log::*;
use morgan_interface::account_host::{OfflineAccount, AccountHost, OnlineAccount};
use morgan_interface::gas_cost::GasCost;
use morgan_interface::hash::Hash;
use morgan_interface::opcodes::OpCode;
use morgan_interface::message::Context;
use morgan_interface::constants::PACKET_DATA_SIZE;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::{Keypair, KeypairUtil, Signature};
use morgan_interface::sys_opcode;
use morgan_interface::transaction::{self, Transaction};
use morgan_interface::transport::Result as TransportResult;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;
use morgan_helper::logHelper::*;

/// An object for querying and sending transactions to the network.
pub struct SlimAccountHost {
    account_host_url: SocketAddr,
    account_host_socket: UdpSocket,
    rpc_client: RpcClient,
}

impl SlimAccountHost {
    /// Create a new SlimAccountHost that will interface with the Rpc at `rpc_addr` using TCP
    /// and the TransactionDigestingModule at `account_host_url` over `account_host_socket` using UDP.
    pub fn new(
        rpc_addr: SocketAddr,
        account_host_url: SocketAddr,
        account_host_socket: UdpSocket,
    ) -> Self {
        Self::new_from_client(
            account_host_url,
            account_host_socket,
            RpcClient::new_socket(rpc_addr),
        )
    }

    pub fn new_socket_with_timeout(
        rpc_addr: SocketAddr,
        account_host_url: SocketAddr,
        account_host_socket: UdpSocket,
        timeout: Duration,
    ) -> Self {
        let rpc_client = RpcClient::new_socket_with_timeout(rpc_addr, timeout);
        Self::new_from_client(account_host_url, account_host_socket, rpc_client)
    }

    fn new_from_client(
        account_host_url: SocketAddr,
        account_host_socket: UdpSocket,
        rpc_client: RpcClient,
    ) -> Self {
        Self {
            rpc_client,
            account_host_url,
            account_host_socket,
        }
    }

    /// Retry a sending a signed Transaction to the server for processing.
    pub fn retry_transfer_until_confirmed(
        &self,
        keypair: &Keypair,
        transaction: &mut Transaction,
        tries: usize,
        min_confirmed_blocks: usize,
    ) -> io::Result<Signature> {
        self.send_and_confirm_transaction(&[keypair], transaction, tries, min_confirmed_blocks)
    }

    /// Retry sending a signed Transaction with one signing Keypair to the server for processing.
    pub fn retry_transfer(
        &self,
        keypair: &Keypair,
        transaction: &mut Transaction,
        tries: usize,
    ) -> io::Result<Signature> {
        self.send_and_confirm_transaction(&[keypair], transaction, tries, 0)
    }

    /// Retry sending a signed Transaction to the server for processing
    pub fn send_and_confirm_transaction(
        &self,
        keypairs: &[&Keypair],
        transaction: &mut Transaction,
        tries: usize,
        min_confirmed_blocks: usize,
    ) -> io::Result<Signature> {
        for x in 0..tries {
            let mut buf = vec![0; serialized_size(&transaction).unwrap() as usize];
            let mut wr = std::io::Cursor::new(&mut buf[..]);
            serialize_into(&mut wr, &transaction)
                .expect("serialize Transaction in pub fn transfer_signed");
            self.account_host_socket
                .send_to(&buf[..], &self.account_host_url)?;
            if self
                .poll_for_signature_confirmation(&transaction.signatures[0], min_confirmed_blocks)
                .is_ok()
            {
                return Ok(transaction.signatures[0]);
            }
            // info!("{}", Info(format!("{} tries failed transfer to {}", x, self.account_host_url).to_string()));
            let info:String = format!("{} tries failed transfer to {}", x, self.account_host_url).to_string();
            println!("{}",
                printLn(
                    info,
                    module_path!().to_string()
                )
            );
            let (transaction_seal, _fee_calculator) = self.rpc_client.get_recent_transaction_seal()?;
            transaction.sign(keypairs, transaction_seal);
        }
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!("retry_transfer failed in {} retries", tries),
        ))
    }

    pub fn poll_balance_with_timeout(
        &self,
        pubkey: &BvmAddr,
        polling_frequency: &Duration,
        timeout: &Duration,
    ) -> io::Result<u64> {
        self.rpc_client
            .poll_balance_with_timeout(pubkey, polling_frequency, timeout)
    }

    pub fn poll_get_balance(&self, pubkey: &BvmAddr) -> io::Result<u64> {
        self.rpc_client.poll_get_balance(pubkey)
    }

    pub fn wait_for_balance(&self, pubkey: &BvmAddr, expected_balance: Option<u64>) -> Option<u64> {
        self.rpc_client.wait_for_balance(pubkey, expected_balance)
    }

    /// Check a signature in the treasury. This method blocks
    /// until the server sends a response.
    pub fn check_signature(&self, signature: &Signature) -> bool {
        self.rpc_client.check_signature(signature)
    }

    pub fn fullnode_exit(&self) -> io::Result<bool> {
        self.rpc_client.fullnode_exit()
    }
    pub fn get_num_blocks_since_signature_confirmation(
        &mut self,
        sig: &Signature,
    ) -> io::Result<usize> {
        self.rpc_client
            .get_num_blocks_since_signature_confirmation(sig)
    }
}

impl AccountHost for SlimAccountHost {
    fn account_host_url(&self) -> String {
        self.account_host_url.to_string()
    }
}

impl OnlineAccount for SlimAccountHost {
    fn send_online_msg(&self, keypairs: &[&Keypair], message: Context) -> TransportResult<Signature> {
        let (transaction_seal, _fee_calculator) = self.get_recent_transaction_seal()?;
        let mut transaction = Transaction::new(&keypairs, message, transaction_seal);
        let signature = self.send_and_confirm_transaction(keypairs, &mut transaction, 5, 0)?;
        Ok(signature)
    }

    fn snd_online_instruction(
        &self,
        keypair: &Keypair,
        instruction: OpCode,
    ) -> TransportResult<Signature> {
        let message = Context::new(vec![instruction]);
        self.send_online_msg(&[keypair], message)
    }

    fn online_transfer(
        &self,
        difs: u64,
        keypair: &Keypair,
        pubkey: &BvmAddr,
    ) -> TransportResult<Signature> {
        let transfer_instruction =
            sys_opcode::transfer(&keypair.pubkey(), pubkey, difs);
        self.snd_online_instruction(keypair, transfer_instruction)
    }

    fn get_account_data(&self, pubkey: &BvmAddr) -> TransportResult<Option<Vec<u8>>> {
        Ok(self.rpc_client.get_account_data(pubkey).ok())
    }

    fn get_balance(&self, pubkey: &BvmAddr) -> TransportResult<u64> {
        let balance = self.rpc_client.get_balance(pubkey)?;
        Ok(balance)
    }

    fn get_signature_status(
        &self,
        signature: &Signature,
    ) -> TransportResult<Option<transaction::Result<()>>> {
        let status = self
            .rpc_client
            .get_signature_status(&signature.to_string())
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("send_transaction failed with error {:?}", err),
                )
            })?;
        Ok(status)
    }

    fn get_recent_transaction_seal(&self) -> TransportResult<(Hash, GasCost)> {
        Ok(self.rpc_client.get_recent_transaction_seal()?)
    }

    fn get_transaction_count(&self) -> TransportResult<u64> {
        let transaction_count = self.rpc_client.get_transaction_count()?;
        Ok(transaction_count)
    }

    /// Poll the server until the signature has been confirmed by at least `min_confirmed_blocks`
    fn poll_for_signature_confirmation(
        &self,
        signature: &Signature,
        min_confirmed_blocks: usize,
    ) -> TransportResult<()> {
        Ok(self
            .rpc_client
            .poll_for_signature_confirmation(signature, min_confirmed_blocks)?)
    }

    fn poll_for_signature(&self, signature: &Signature) -> TransportResult<()> {
        Ok(self.rpc_client.poll_for_signature(signature)?)
    }

    fn get_new_transaction_seal(&self, transaction_seal: &Hash) -> TransportResult<(Hash, GasCost)> {
        Ok(self.rpc_client.get_new_transaction_seal(transaction_seal)?)
    }
}

impl OfflineAccount for SlimAccountHost {
    fn send_offline_transaction(&self, transaction: Transaction) -> io::Result<Signature> {
        let mut buf = vec![0; serialized_size(&transaction).unwrap() as usize];
        let mut wr = std::io::Cursor::new(&mut buf[..]);
        serialize_into(&mut wr, &transaction)
            .expect("serialize Transaction in pub fn transfer_signed");
        assert!(buf.len() < PACKET_DATA_SIZE);
        self.account_host_socket
            .send_to(&buf[..], &self.account_host_url)?;
        Ok(transaction.signatures[0])
    }
    fn send_offline_message(
        &self,
        keypairs: &[&Keypair],
        message: Context,
        recent_transaction_seal: Hash,
    ) -> io::Result<Signature> {
        let transaction = Transaction::new(&keypairs, message, recent_transaction_seal);
        self.send_offline_transaction(transaction)
    }
    fn send_offline_instruction(
        &self,
        keypair: &Keypair,
        instruction: OpCode,
        recent_transaction_seal: Hash,
    ) -> io::Result<Signature> {
        let message = Context::new(vec![instruction]);
        self.send_offline_message(&[keypair], message, recent_transaction_seal)
    }
    fn offline_transfer(
        &self,
        difs: u64,
        keypair: &Keypair,
        pubkey: &BvmAddr,
        recent_transaction_seal: Hash,
    ) -> io::Result<Signature> {
        let transfer_instruction =
            sys_opcode::transfer(&keypair.pubkey(), pubkey, difs);
        self.send_offline_instruction(keypair, transfer_instruction, recent_transaction_seal)
    }
}

pub fn create_client((rpc, transaction_digesting_module): (SocketAddr, SocketAddr), range: (u16, u16)) -> SlimAccountHost {
    let (_, account_host_socket) = morgan_netutil::bind_in_range(range).unwrap();
    SlimAccountHost::new(rpc, transaction_digesting_module, account_host_socket)
}

pub fn create_client_with_timeout(
    (rpc, transaction_digesting_module): (SocketAddr, SocketAddr),
    range: (u16, u16),
    timeout: Duration,
) -> SlimAccountHost {
    let (_, account_host_socket) = morgan_netutil::bind_in_range(range).unwrap();
    SlimAccountHost::new_socket_with_timeout(rpc, transaction_digesting_module, account_host_socket, timeout)
}
