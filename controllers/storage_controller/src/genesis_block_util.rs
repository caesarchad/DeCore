use crate::morgan_storage_controller;
use morgan_interface::genesis_block::GenesisBlock;
use morgan_interface::bvm_address::BvmAddr;
use morgan_poc_agnt::poc_pact;

pub trait GenesisBlockUtil {
    fn add_storage_controller(&mut self, validator_storage_address: &BvmAddr);
}

impl GenesisBlockUtil for GenesisBlock {
    fn add_storage_controller(&mut self, validator_storage_address: &BvmAddr) {
        self.accounts.push((
            *validator_storage_address,
            poc_pact::crt_vldr_strj_acct(1),
        ));
        self.builtin_opcode_handlers
            .push(morgan_storage_controller!());
    }
}
