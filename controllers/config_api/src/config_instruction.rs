use crate::id;
use crate::ConfigState;
use morgan_interface::opcodes::{AccountMeta, OpCode};
use morgan_interface::pubkey::Pubkey;
use morgan_interface::sys_opcode;

/// Create a new, empty configuration account
pub fn create_account<T: ConfigState>(
    from_account_pubkey: &Pubkey,
    config_account_pubkey: &Pubkey,
    difs: u64,
) -> OpCode {
    sys_opcode::create_account(
        from_account_pubkey,
        config_account_pubkey,
        difs,
        T::max_space(),
        &id(),
    )
}

/// Store new data in a configuration account
pub fn store<T: ConfigState>(config_account_pubkey: &Pubkey, data: &T) -> OpCode {
    let account_metas = vec![AccountMeta::new(*config_account_pubkey, true)];
    OpCode::new(id(), data, account_metas)
}
