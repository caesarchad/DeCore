use crate::morgan_storage_controller;
use morgan_interface::genesis_block::GenesisBlock;
use morgan_interface::bvm_address::BvmAddr;
use morgan_storage_api::storage_contract;

pub trait GenesisBlockUtil {
    fn add_storage_controller(&mut self, validator_storage_pubkey: &BvmAddr);
}

impl GenesisBlockUtil for GenesisBlock {
    fn add_storage_controller(&mut self, validator_storage_pubkey: &BvmAddr) {
        self.accounts.push((
            *validator_storage_pubkey,
            storage_contract::create_validator_storage_account(1),
        ));
        self.builtin_opcode_handlers
            .push(morgan_storage_controller!());
    }
}
