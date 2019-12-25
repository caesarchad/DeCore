use crate::hash::Hash;
use crate::pubkey::Pubkey;
use crate::signature::{Keypair, KeypairUtil};
use crate::sys_opcode;
use crate::transaction::Transaction;

const SYSTEM_PROGRAM_ID: [u8; 32] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

pub fn id() -> Pubkey {
    Pubkey::new(&SYSTEM_PROGRAM_ID)
}

pub fn check_id(program_id: &Pubkey) -> bool {
    program_id.as_ref() == SYSTEM_PROGRAM_ID
}

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
    let op_create =
        sys_opcode::create_account(&from_pubkey, to, difs, space, program_id);
    let op_vec = vec![op_create];
    Transaction::new_s_opcodes(&[from_keypair], op_vec, recent_transaction_seal)
}

/// Create and sign a transaction to create a system account
pub fn create_user_account(
    from_keypair: &Keypair,
    to: &Pubkey,
    difs: u64,
    recent_transaction_seal: Hash,
) -> Transaction {
    let program_id = id();
    create_account(from_keypair, to, recent_transaction_seal, difs, 0, &program_id)
}

/// Create and sign new sys_opcode::Assign transaction
pub fn assign(from_keypair: &Keypair, recent_transaction_seal: Hash, program_id: &Pubkey) -> Transaction {
    let from_pubkey = from_keypair.pubkey();
    let op_assign = sys_opcode::assign(&from_pubkey, program_id);
    let op_vec = vec![op_assign];
    Transaction::new_s_opcodes(&[from_keypair], op_vec, recent_transaction_seal)
}

/// Create and sign new sys_opcode::Transfer transaction
pub fn transfer(
    from_keypair: &Keypair,
    to: &Pubkey,
    difs: u64,
    recent_transaction_seal: Hash,
) -> Transaction {
    let from_pubkey = from_keypair.pubkey();
    let op_transfer = sys_opcode::transfer(&from_pubkey, to, difs);
    let op_vec = vec![op_transfer];
    Transaction::new_s_opcodes(&[from_keypair], op_vec, recent_transaction_seal)
}
