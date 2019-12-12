use crate::instruction::{AccountMeta, Instruction};
use crate::pubkey::Pubkey;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum LoaderInstruction {
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
) -> Instruction {
    let account_metas = vec![AccountMeta::new(*account_pubkey, true)];
    Instruction::new(
        *program_id,
        &LoaderInstruction::Write { offset, bytes },
        account_metas,
    )
}

pub fn finalize(account_pubkey: &Pubkey, program_id: &Pubkey) -> Instruction {
    let account_metas = vec![AccountMeta::new(*account_pubkey, true)];
    Instruction::new(*program_id, &LoaderInstruction::Finalize, account_metas)
}
