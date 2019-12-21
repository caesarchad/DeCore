use crate::opcodes::{AccountMeta, OpCode};
use crate::pubkey::Pubkey;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum MounterOpCode {
    /// Write program data into an account
    ///
    /// * key[0] - the account to write into.
    ///
    /// The transaction must be signed by key[0]
    Write { offset: u32, bytes: Vec<u8> },

    /// Finalize an account loaded with program data for execution.
    /// The exact preparation steps is loader specific but on success the loader must set the executable
    /// bit of the Account
    ///
    /// * key[0] - the account to prepare for execution
    ///
    /// The transaction must be signed by key[0]
    Finalize,
}

pub fn write(
    account_pubkey: &Pubkey,
    program_id: &Pubkey,
    offset: u32,
    bytes: Vec<u8>,
) -> OpCode {
    let account_metas = vec![AccountMeta::new(*account_pubkey, true)];
    OpCode::new(
        *program_id,
        &MounterOpCode::Write { offset, bytes },
        account_metas,
    )
}

pub fn finalize(account_pubkey: &Pubkey, program_id: &Pubkey) -> OpCode {
    let account_metas = vec![AccountMeta::new(*account_pubkey, true)];
    OpCode::new(*program_id, &MounterOpCode::Finalize, account_metas)
}
