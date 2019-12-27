use bitconch_interface::address::BvmAddr;

pub trait Cluster {
    fn get_node_addresss(&self) -> Vec<BvmAddr>;
    fn restart_node(&mut self, address: BvmAddr);
}

pub trait NodeGroup {
    fn get_node_addresss(&self) -> Vec<BvmAddr>;
    fn restart_node(&mut self, address: BvmAddr);
}