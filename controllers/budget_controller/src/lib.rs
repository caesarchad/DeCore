#[macro_export]
macro_rules! morgan_budget_controller {
    () => {
        ("morgan_budget_controller".to_string(), morgan_budget_api::id())
    };
}

use morgan_budget_api::sc_handler::handle_opcode;
morgan_interface::morgan_entrypoint!(handle_opcode);
