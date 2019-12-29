pub mod genesis_block_util;

#[macro_export]
macro_rules! morgan_storage_controller {
    () => {
        (
            "morgan_storage_controller".to_string(),
            morgan_storage_api::pgm_id::id(),
        )
    };
}

use morgan_storage_api::poc_handler::handle_opcode;
morgan_interface::morgan_entrypoint!(handle_opcode);
