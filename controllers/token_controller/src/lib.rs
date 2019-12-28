#[macro_export]
macro_rules! morgan_token_controller {
    () => {
        ("morgan_token_controller".to_string(), morgan_token_api::pgm_id::id())
    };
}

use morgan_token_api::ico_handler::handle_opcode;

morgan_interface::morgan_entrypoint!(handle_opcode);
