#[macro_export]
macro_rules! morgan_exchange_controller {
    () => {
        (
            "morgan_exchange_controller".to_string(),
            morgan_exchange_api::id(),
        )
    };
}
use morgan_exchange_api::exchange_processor::handle_opcode;

morgan_interface::morgan_entrypoint!(handle_opcode);
