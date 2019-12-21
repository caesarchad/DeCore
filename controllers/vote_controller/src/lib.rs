#[macro_export]
macro_rules! morgan_vote_controller {
    () => {
        ("morgan_vote_controller".to_string(), morgan_vote_api::id())
    };
}

use morgan_vote_api::vote_opcode::handle_opcode;
morgan_interface::morgan_entrypoint!(handle_opcode);
