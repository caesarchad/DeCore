use crate::ico_context::IcoContext;
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

    IcoContext::process(program_id, info, input).map_err(|e| {
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

#[macro_export]
macro_rules! bvm_ico_entrypoint {
    () => {
        ("bvm_ico_entrypoint".to_string(), morgan_ico_issuer::pgm_id::id())
    };
}

morgan_interface::morgan_entrypoint!(handle_opcode);
