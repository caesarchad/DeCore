#[macro_export]
macro_rules! morgan_stake_controller {
    () => {
        ("morgan_stake_controller".to_string(), morgan_stake_api::id())
    };
}

use morgan_stake_api::stake_opcode::handle_opcode;
morgan_interface::morgan_entrypoint!(handle_opcode);
