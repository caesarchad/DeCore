#[macro_export]
macro_rules! morgan_config_controller {
    () => {
        ("morgan_config_controller".to_string(), morgan_profiler::pgm_id::id())
    };
}
use morgan_profiler::handler::handle_opcode;

morgan_interface::morgan_entrypoint!(handle_opcode);
