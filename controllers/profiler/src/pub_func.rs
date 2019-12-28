use crate::pgm_id::{self,ConfigState,id,};
use morgan_interface::opcodes::{AccountMeta, OpCode};
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::sys_opcode;

/// Create a new, empty configuration account
pub fn profile_account<T: ConfigState>(
    from_addr: &BvmAddr,
    conf_addr: &BvmAddr,
    difs: u64,
) -> OpCode {
    sys_opcode::create_account(
        from_addr,
        conf_addr,
        difs,
        T::max_space(),
        &id(),
    )
}

/// Store new data in a configuration account
pub fn populate_with<T: ConfigState>(conf_addr: &BvmAddr, data: &T) -> OpCode {
    let account_metas = vec![AccountMeta::new(*conf_addr, true)];
    OpCode::new(id(), data, account_metas)
}
