use bitconch_interface::pubkey::BvmAddr;

pub trait Cluster {
    fn get_node_pubkeys(&self) -> Vec<BvmAddr>;
    fn restart_node(&mut self, pubkey: BvmAddr);
}

pub trait NodeGroup {
    fn get_node_pubkeys(&self) -> Vec<BvmAddr>;
    fn restart_node(&mut self, pubkey: BvmAddr);
}