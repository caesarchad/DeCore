#[macro_export]
macro_rules! morgan_budget_controller {
    () => {
        ("morgan_budget_controller".to_string(), morgan_bvm_script::id())
    };
}

use morgan_bvm_script::sc_handler::handle_opcode;
morgan_interface::morgan_entrypoint!(handle_opcode);
