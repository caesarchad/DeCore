use crate::token_state::TokenState;
use log::*;
use morgan_interface::account::KeyedAccount;
use morgan_interface::opcodes::OpCodeErr;
use morgan_interface::bvm_address::BvmAddr;
use morgan_helper::logHelper::*;

pub fn handle_opcode(
    program_id: &BvmAddr,
    info: &mut [KeyedAccount],
    input: &[u8],
    _drop_height: u64,
) -> Result<(), OpCodeErr> {
    morgan_logger::setup();

    TokenState::process(program_id, info, input).map_err(|e| {
        // error!("{}", Error(format!("error: {:?}", e).to_string()));
        println!(
            "{}",
            Error(
                format!("error: {:?}", e).to_string(),
                module_path!().to_string()
            )
        );
        OpCodeErr::CustomError(e as u32)
    })
}
