//! The `system_transaction` module provides functionality for creating system transactions.

use crate::hash::Hash;
use crate::pubkey::Pubkey;
use crate::signature::{Keypair, KeypairUtil};
use crate::sys_opcode;
use crate::system_program;
use crate::transaction::Transaction;

/// Create and sign new SysOpCode::CreateAccount transaction
pub fn create_account(
    from_keypair: &Keypair,
    to: &Pubkey,
    recent_transaction_seal: Hash,
    difs: u64,
    space: u64,
    program_id: &Pubkey,
) -> Transaction {
    let from_pubkey = from_keypair.pubkey();
    let create_instruction =
        sys_opcode::create_account(&from_pubkey, to, difs, space, program_id);
    let instructions = vec![create_instruction];
    Transaction::new_s_opcodes(&[from_keypair], instructions, recent_transaction_seal)
}

/// Create and sign a transaction to create a system account
pub fn create_user_account(
    from_keypair: &Keypair,
    to: &Pubkey,
    difs: u64,
    recent_transaction_seal: Hash,
) -> Transaction {
    let program_id = system_program::id();
    create_account(from_keypair, to, recent_transaction_seal, difs, 0, &program_id)
}

/// Create and sign new sys_opcode::Assign transaction
pub fn assign(from_keypair: &Keypair, recent_transaction_seal: Hash, program_id: &Pubkey) -> Transaction {
    let from_pubkey = from_keypair.pubkey();
    let assign_instruction = sys_opcode::assign(&from_pubkey, program_id);
    let instructions = vec![assign_instruction];
    Transaction::new_s_opcodes(&[from_keypair], instructions, recent_transaction_seal)
}

/// Create and sign new sys_opcode::Transfer transaction
pub fn transfer(
    from_keypair: &Keypair,
    to: &Pubkey,
    difs: u64,
    recent_transaction_seal: Hash,
) -> Transaction {
    let from_pubkey = from_keypair.pubkey();
    let transfer_instruction = sys_opcode::transfer(&from_pubkey, to, difs);
    let instructions = vec![transfer_instruction];
    Transaction::new_s_opcodes(&[from_keypair], instructions, recent_transaction_seal)
}
