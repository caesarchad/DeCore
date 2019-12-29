pub mod genesis_block_util;

#[macro_export]
macro_rules! morgan_storage_controller {
    () => {
        (
            "morgan_storage_controller".to_string(),
            morgan_poc_agnt::pgm_id::id(),
        )
    };
}

use morgan_poc_agnt::poc_handler::handle_opcode;
morgan_interface::morgan_entrypoint!(handle_opcode);
