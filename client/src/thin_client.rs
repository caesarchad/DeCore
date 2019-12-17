//! The `thin_client` module is a client-side object that interfaces with
//! a server-side transaction digesting module.  Client code should use this object instead of writing
//! messages to the network directly. The binary encoding of its messages are
//! unstable and may change in future releases.

use crate::rpc_client::RpcClient;
use bincode::{serialize_into, serialized_size};
use log::*;
use morgan_interface::client::{AsyncClient, Client, SyncClient};
use morgan_interface::fee_calculator::FeeCalculator;
use morgan_interface::hash::Hash;
use morgan_interface::instruction::Instruction;
use morgan_interface::message::Message;
use morgan_interface::packet::PACKET_DATA_SIZE;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::signature::{Keypair, KeypairUtil, Signature};
use morgan_interface::system_instruction;
use morgan_interface::transaction::{self, Transaction};
use morgan_interface::transport::Result as TransportResult;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;
use morgan_helper::logHelper::*;

/// An object for querying and sending transactions to the network.
pub struct ThinClient {
    transactions_addr: SocketAddr,
    transactions_socket: UdpSocket,
    rpc_client: RpcClient,
}

impl ThinClient {
    /// Create a new ThinClient that will interface with the Rpc at `rpc_addr` using TCP
    /// and the TransactionDigestingModule at `transactions_addr` over `transactions_socket` using UDP.
    pub fn new(
        rpc_addr: SocketAddr,
        transactions_addr: SocketAddr,
        transactions_socket: UdpSocket,
    ) -> Self {
        Self::new_from_client(
            transactions_addr,
            transactions_socket,
            RpcClient::new_socket(rpc_addr),
        )
    }

    pub fn new_socket_with_timeout(
        rpc_addr: SocketAddr,
        transactions_addr: SocketAddr,
        transactions_socket: UdpSocket,
        timeout: Duration,
    ) -> Self {
        let rpc_client = RpcClient::new_socket_with_timeout(rpc_addr, timeout);
        Self::new_from_client(transactions_addr, transactions_socket, rpc_client)
    }

    fn new_from_client(
        transactions_addr: SocketAddr,
        transactions_socket: UdpSocket,
        rpc_client: RpcClient,
    ) -> Self {
        Self {
            rpc_client,
            transactions_addr,
            transactions_socket,
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
            self.transactions_socket
                .send_to(&buf[..], &self.transactions_addr)?;
            if self
                .poll_for_signature_confirmation(&transaction.signatures[0], min_confirmed_blocks)
                .is_ok()
            {
                return Ok(transaction.signatures[0]);
            }
            // info!("{}", Info(format!("{} tries failed transfer to {}", x, self.transactions_addr).to_string()));
            let info:String = format!("{} tries failed transfer to {}", x, self.transactions_addr).to_string();
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
        pubkey: &Pubkey,
        polling_frequency: &Duration,
        timeout: &Duration,
    ) -> io::Result<u64> {
        self.rpc_client
            .poll_balance_with_timeout(pubkey, polling_frequency, timeout)
    }

    pub fn poll_get_balance(&self, pubkey: &Pubkey) -> io::Result<u64> {
        self.rpc_client.poll_get_balance(pubkey)
    }

    pub fn wait_for_balance(&self, pubkey: &Pubkey, expected_balance: Option<u64>) -> Option<u64> {
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

impl Client for ThinClient {
    fn transactions_addr(&self) -> String {
        self.transactions_addr.to_string()
    }
}

impl SyncClient for ThinClient {
    fn send_message(&self, keypairs: &[&Keypair], message: Message) -> TransportResult<Signature> {
        let (transaction_seal, _fee_calculator) = self.get_recent_transaction_seal()?;
        let mut transaction = Transaction::new(&keypairs, message, transaction_seal);
        let signature = self.send_and_confirm_transaction(keypairs, &mut transaction, 5, 0)?;
        Ok(signature)
    }

    fn send_instruction(
        &self,
        keypair: &Keypair,
        instruction: Instruction,
    ) -> TransportResult<Signature> {
        let message = Message::new(vec![instruction]);
        self.send_message(&[keypair], message)
    }

    fn transfer(
        &self,
        difs: u64,
        keypair: &Keypair,
        pubkey: &Pubkey,
    ) -> TransportResult<Signature> {
        let transfer_instruction =
            system_instruction::transfer(&keypair.pubkey(), pubkey, difs);
        self.send_instruction(keypair, transfer_instruction)
    }

    fn get_account_data(&self, pubkey: &Pubkey) -> TransportResult<Option<Vec<u8>>> {
        Ok(self.rpc_client.get_account_data(pubkey).ok())
    }

    fn get_balance(&self, pubkey: &Pubkey) -> TransportResult<u64> {
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

    fn get_recent_transaction_seal(&self) -> TransportResult<(Hash, FeeCalculator)> {
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

    fn get_new_transaction_seal(&self, transaction_seal: &Hash) -> TransportResult<(Hash, FeeCalculator)> {
        Ok(self.rpc_client.get_new_transaction_seal(transaction_seal)?)
    }
}

impl AsyncClient for ThinClient {
    fn async_send_transaction(&self, transaction: Transaction) -> io::Result<Signature> {
        let mut buf = vec![0; serialized_size(&transaction).unwrap() as usize];
        let mut wr = std::io::Cursor::new(&mut buf[..]);
        serialize_into(&mut wr, &transaction)
            .expect("serialize Transaction in pub fn transfer_signed");
        assert!(buf.len() < PACKET_DATA_SIZE);
        self.transactions_socket
            .send_to(&buf[..], &self.transactions_addr)?;
        Ok(transaction.signatures[0])
    }
    fn async_send_message(
        &self,
        keypairs: &[&Keypair],
        message: Message,
        recent_transaction_seal: Hash,
    ) -> io::Result<Signature> {
        let transaction = Transaction::new(&keypairs, message, recent_transaction_seal);
        self.async_send_transaction(transaction)
    }
    fn async_send_instruction(
        &self,
        keypair: &Keypair,
        instruction: Instruction,
        recent_transaction_seal: Hash,
    ) -> io::Result<Signature> {
        let message = Message::new(vec![instruction]);
        self.async_send_message(&[keypair], message, recent_transaction_seal)
    }
    fn async_transfer(
        &self,
        difs: u64,
        keypair: &Keypair,
        pubkey: &Pubkey,
        recent_transaction_seal: Hash,
    ) -> io::Result<Signature> {
        let transfer_instruction =
            system_instruction::transfer(&keypair.pubkey(), pubkey, difs);
        self.async_send_instruction(keypair, transfer_instruction, recent_transaction_seal)
    }
}

pub fn create_client((rpc, transaction_digesting_module): (SocketAddr, SocketAddr), range: (u16, u16)) -> ThinClient {
    let (_, transactions_socket) = morgan_netutil::bind_in_range(range).unwrap();
    ThinClient::new(rpc, transaction_digesting_module, transactions_socket)
}

pub fn create_client_with_timeout(
    (rpc, transaction_digesting_module): (SocketAddr, SocketAddr),
    range: (u16, u16),
    timeout: Duration,
) -> ThinClient {
    let (_, transactions_socket) = morgan_netutil::bind_in_range(range).unwrap();
    ThinClient::new_socket_with_timeout(rpc, transaction_digesting_module, transactions_socket, timeout)
}
