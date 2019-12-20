use crate::treasury::Treasury;
use morgan_interface::client::{AsyncClient, Client, SyncClient};
use morgan_interface::fee_calculator::FeeCalculator;
use morgan_interface::hash::Hash;
use morgan_interface::instruction::Instruction;
use morgan_interface::message::Message;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::signature::Signature;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::system_instruction;
use morgan_interface::transaction::{self, Transaction};
use morgan_interface::transport::{Result, TransportError};
use std::io;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::{sleep, Builder};
use std::time::{Duration, Instant};

pub struct TreasuryClient {
    treasury: Arc<Treasury>,
    transaction_sender: Mutex<Sender<Transaction>>,
}

impl Client for TreasuryClient {
    fn transactions_addr(&self) -> String {
        "Local TreasuryClient".to_string()
    }
}

impl AsyncClient for TreasuryClient {
    fn async_send_transaction(&self, transaction: Transaction) -> io::Result<Signature> {
        let signature = transaction.signatures.get(0).cloned().unwrap_or_default();
        let transaction_sender = self.transaction_sender.lock().unwrap();
        transaction_sender.send(transaction).unwrap();
        Ok(signature)
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

    /// Transfer `difs` from `keypair` to `pubkey`
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

impl SyncClient for TreasuryClient {
    fn send_message(&self, keypairs: &[&Keypair], message: Message) -> Result<Signature> {
        let transaction_seal = self.treasury.last_transaction_seal();
        let transaction = Transaction::new(&keypairs, message, transaction_seal);
        self.treasury.process_transaction(&transaction)?;
        Ok(transaction.signatures.get(0).cloned().unwrap_or_default())
    }

    /// Create and process a transaction from a single instruction.
    fn send_instruction(&self, keypair: &Keypair, instruction: Instruction) -> Result<Signature> {
        let message = Message::new(vec![instruction]);
        self.send_message(&[keypair], message)
    }

    /// Transfer `difs` from `keypair` to `pubkey`
    fn transfer(&self, difs: u64, keypair: &Keypair, pubkey: &Pubkey) -> Result<Signature> {
        let transfer_instruction =
            system_instruction::transfer(&keypair.pubkey(), pubkey, difs);
        self.send_instruction(keypair, transfer_instruction)
    }

    fn get_account_data(&self, pubkey: &Pubkey) -> Result<Option<Vec<u8>>> {
        Ok(self.treasury.get_account(pubkey).map(|account| account.data))
    }

    fn get_balance(&self, pubkey: &Pubkey) -> Result<u64> {
        Ok(self.treasury.get_balance(pubkey))
    }

    fn get_signature_status(
        &self,
        signature: &Signature,
    ) -> Result<Option<transaction::Result<()>>> {
        Ok(self.treasury.get_signature_status(signature))
    }

    fn get_recent_transaction_seal(&self) -> Result<(Hash, FeeCalculator)> {
        let last_transaction_seal = self.treasury.last_transaction_seal();
        let fee_calculator = self.treasury.fee_calculator.clone();
        Ok((last_transaction_seal, fee_calculator))
    }

    fn get_transaction_count(&self) -> Result<u64> {
        Ok(self.treasury.transaction_count())
    }

    fn poll_for_signature_confirmation(
        &self,
        signature: &Signature,
        min_confirmed_blocks: usize,
    ) -> Result<()> {
        let mut now = Instant::now();
        let mut confirmed_blocks = 0;
        loop {
            let response = self.treasury.get_signature_confirmation_status(signature);
            if let Some((confirmations, res)) = response {
                if res.is_ok() {
                    if confirmed_blocks != confirmations {
                        now = Instant::now();
                        confirmed_blocks = confirmations;
                    }
                    if confirmations >= min_confirmed_blocks {
                        break;
                    }
                }
            };
            if now.elapsed().as_secs() > 15 {
                // TODO: Return a better error.
                return Err(TransportError::IoError(io::Error::new(
                    io::ErrorKind::Other,
                    "signature not found",
                )));
            }
            sleep(Duration::from_millis(250));
        }
        Ok(())
    }

    fn poll_for_signature(&self, signature: &Signature) -> Result<()> {
        let now = Instant::now();
        loop {
            let response = self.treasury.get_signature_status(signature);
            if let Some(res) = response {
                if res.is_ok() {
                    break;
                }
            }
            if now.elapsed().as_secs() > 15 {
                // TODO: Return a better error.
                return Err(TransportError::IoError(io::Error::new(
                    io::ErrorKind::Other,
                    "signature not found",
                )));
            }
            sleep(Duration::from_millis(250));
        }
        Ok(())
    }

    fn get_new_transaction_seal(&self, transaction_seal: &Hash) -> Result<(Hash, FeeCalculator)> {
        let (last_transaction_seal, fee_calculator) = self.get_recent_transaction_seal()?;
        if last_transaction_seal != *transaction_seal {
            Ok((last_transaction_seal, fee_calculator))
        } else {
            Err(TransportError::IoError(io::Error::new(
                io::ErrorKind::Other,
                "Unable to get new transaction_seal",
            )))
        }
    }
}

impl TreasuryClient {
    fn run(treasury: &Treasury, transaction_receiver: Receiver<Transaction>) {
        while let Ok(tx) = transaction_receiver.recv() {
            let mut transactions = vec![tx];
            while let Ok(tx) = transaction_receiver.try_recv() {
                transactions.push(tx);
            }
            let _ = treasury.process_transactions(&transactions);
        }
    }

    pub fn new_shared(treasury: &Arc<Treasury>) -> Self {
        let (transaction_sender, transaction_receiver) = channel();
        let transaction_sender = Mutex::new(transaction_sender);
        let thread_treasury = treasury.clone();
        let treasury = treasury.clone();
        Builder::new()
            .name("morgan-treasury-client".to_string())
            .spawn(move || Self::run(&thread_treasury, transaction_receiver))
            .unwrap();
        Self {
            treasury,
            transaction_sender,
        }
    }

    pub fn new(treasury: Treasury) -> Self {
        Self::new_shared(&Arc::new(treasury))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use morgan_interface::genesis_block::create_genesis_block;
    use morgan_interface::instruction::AccountMeta;

    #[test]
    fn test_treasury_client_new_with_keypairs() {
        let (genesis_block, john_doe_keypair) = create_genesis_block(10_000);
        let john_pubkey = john_doe_keypair.pubkey();
        let jane_doe_keypair = Keypair::new();
        let jane_pubkey = jane_doe_keypair.pubkey();
        let doe_keypairs = vec![&john_doe_keypair, &jane_doe_keypair];
        let treasury = Treasury::new(&genesis_block);
        let treasury_client = TreasuryClient::new(treasury);

        // Create 2-2 Multisig Transfer instruction.
        let bob_pubkey = Pubkey::new_rand();
        let mut transfer_instruction = system_instruction::transfer(&john_pubkey, &bob_pubkey, 42);
        transfer_instruction
            .accounts
            .push(AccountMeta::new(jane_pubkey, true));

        let message = Message::new(vec![transfer_instruction]);
        treasury_client.send_message(&doe_keypairs, message).unwrap();
        assert_eq!(treasury_client.get_balance(&bob_pubkey).unwrap(), 42);
    }
}
