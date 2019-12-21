use morgan_interface::account::KeyedAccount;
use morgan_interface::opcodes::OpCodeErr;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::morgan_entrypoint;

morgan_entrypoint!(entrypoint);
fn entrypoint(
    _program_id: &Pubkey,
    _keyed_accounts: &mut [KeyedAccount],
    _data: &[u8],
    _drop_height: u64,
) -> Result<(), OpCodeErr> {
    Err(OpCodeErr::GenericError)
}
