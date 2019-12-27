use crate::block_buffer_pool::{create_new_tmp_ledger, tmp_copy_block_buffer};
use crate::node_group::NodeGroup;
use crate::node_group_info::Node;
use crate::bvm_types::*;
use crate::connection_info::ContactInfo;
use crate::genesis_utils::{create_genesis_block_with_leader, GenesisBlockInfo};
use crate::gossip_service::find_node_group_host;
use crate::cloner::StorageMiner;
use crate::service::Service;
use crate::verifier::{Validator, ValidatorConfig};
use morgan_client::slim_account_host::create_client;
use morgan_client::slim_account_host::SlimAccountHost;
use morgan_interface::account_host::OnlineAccount;
use morgan_interface::genesis_block::GenesisBlock;
use morgan_interface::context::Context;
use morgan_interface::waterclock_config::WaterClockConfig;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::sys_controller;
use morgan_interface::constants::DEFAULT_SLOTS_PER_EPOCH;
use morgan_interface::constants::DEFAULT_DROPS_PER_SLOT;
use morgan_interface::transaction::Transaction;
use morgan_stake_api::stake_opcode;
use morgan_storage_api::storage_opcode;
use morgan_storage_controller::genesis_block_util::GenesisBlockUtil;
use morgan_vote_api::vote_opcode;
use morgan_vote_api::vote_state::VoteState;
use std::collections::HashMap;
use std::fs::remove_dir_all;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use morgan_helper::logHelper::*;

pub struct ValidatorInfo {
    pub keypair: Arc<Keypair>,
    pub voting_keypair: Arc<Keypair>,
    pub storage_keypair: Arc<Keypair>,
    pub ledger_path: String,
}

pub struct StorageMinerInfo {
    pub miner_storage_address: BvmAddr,
    pub ledger_path: String,
}

impl StorageMinerInfo {
    fn new(storage_address: BvmAddr, ledger_path: String) -> Self {
        Self {
            miner_storage_address: storage_address,
            ledger_path,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NodeGroupConfig {
    /// The fullnode config that should be applied to every node in the node group
    pub validator_config: ValidatorConfig,
    /// Number of miners in the node group
    /// Note- miners will timeout if drops_per_slot is much larger than the default 8
    pub miner_amnt: usize,
    /// Number of nodes that are unstaked and not voting (a.k.a listening)
    pub observer_amnt: u64,
    /// The stakes of each node
    pub node_stakes: Vec<u64>,
    /// The total difs available to the node group
    pub node_group_difs: u64,
    pub drops_per_slot: u64,
    pub candidate_each_round: u64,
    pub stake_place_holder: u64,
    pub builtin_opcode_handlers: Vec<(String, BvmAddr)>,
    pub waterclock_config: WaterClockConfig,
}

impl Default for NodeGroupConfig {
    fn default() -> Self {
        NodeGroupConfig {
            validator_config: ValidatorConfig::default(),
            miner_amnt: 0,
            observer_amnt: 0,
            node_stakes: vec![],
            node_group_difs: 0,
            drops_per_slot: DEFAULT_DROPS_PER_SLOT,
            candidate_each_round: DEFAULT_SLOTS_PER_EPOCH,
            stake_place_holder: DEFAULT_SLOTS_PER_EPOCH,
            builtin_opcode_handlers: vec![],
            waterclock_config: WaterClockConfig::default(),
        }
    }
}

pub struct LocalNodeGroup {
    /// Keypair with funding to participate in the network
    pub funding_keypair: Keypair,
    pub validator_config: ValidatorConfig,
    /// FsclStmt point from which the rest of the network can be discovered
    pub connection_url_inf: ContactInfo,
    pub fullnode_infos: HashMap<BvmAddr, ValidatorInfo>,
    pub listener_infos: HashMap<BvmAddr, ValidatorInfo>,
    fullnodes: HashMap<BvmAddr, Validator>,
    genesis_ledger_path: String,
    pub genesis_block: GenesisBlock,
    miners: Vec<StorageMiner>,
    pub storage_miner_infos: HashMap<BvmAddr, StorageMinerInfo>,
}

impl LocalNodeGroup {
    pub fn new_with_equal_stakes(
        num_nodes: usize,
        node_group_difs: u64,
        difs_per_node: u64,
    ) -> Self {
        let stakes: Vec<_> = (0..num_nodes).map(|_| difs_per_node).collect();
        let config = NodeGroupConfig {
            node_stakes: stakes,
            node_group_difs,
            ..NodeGroupConfig::default()
        };
        Self::new(&config)
    }

    pub fn new(config: &NodeGroupConfig) -> Self {
        let leader_keypair = Arc::new(Keypair::new());
        let leader_addr = leader_keypair.address();
        let leader_node = Node::new_localhost_with_address(&leader_keypair.address());
        let GenesisBlockInfo {
            mut genesis_block,
            mint_keypair,
            voting_keypair,
        } = create_genesis_block_with_leader(
            config.node_group_difs,
            &leader_addr,
            config.node_stakes[0],
        );
        let storage_keypair = Keypair::new();
        genesis_block.add_storage_controller(&storage_keypair.address());
        genesis_block.drops_per_slot = config.drops_per_slot;
        genesis_block.candidate_each_round = config.candidate_each_round;
        genesis_block.stake_place_holder = config.stake_place_holder;
        genesis_block.waterclock_config = config.waterclock_config.clone();
        genesis_block
            .builtin_opcode_handlers
            .extend_from_slice(&config.builtin_opcode_handlers);

        let (genesis_ledger_path, _transaction_seal) = create_new_tmp_ledger!(&genesis_block);
        let leader_ledger_path = tmp_copy_block_buffer!(&genesis_ledger_path);
        let leader_contact_info = leader_node.info.clone();
        let leader_storage_keypair = Arc::new(storage_keypair);
        let leader_voting_keypair = Arc::new(voting_keypair);
        let leader_server = Validator::new(
            leader_node,
            &leader_keypair,
            &leader_ledger_path,
            &leader_voting_keypair.address(),
            &leader_voting_keypair,
            &leader_storage_keypair,
            None,
            &config.validator_config,
        );

        let mut fullnodes = HashMap::new();
        let mut fullnode_infos = HashMap::new();
        fullnodes.insert(leader_addr, leader_server);
        fullnode_infos.insert(
            leader_addr,
            ValidatorInfo {
                keypair: leader_keypair,
                voting_keypair: leader_voting_keypair,
                storage_keypair: leader_storage_keypair,
                ledger_path: leader_ledger_path,
            },
        );

        let mut node_group = Self {
            funding_keypair: mint_keypair,
            connection_url_inf: leader_contact_info,
            fullnodes,
            miners: vec![],
            genesis_ledger_path,
            genesis_block,
            fullnode_infos,
            storage_miner_infos: HashMap::new(),
            validator_config: config.validator_config.clone(),
            listener_infos: HashMap::new(),
        };

        for stake in &config.node_stakes[1..] {
            node_group.add_validator(&config.validator_config, *stake);
        }

        let listener_config = ValidatorConfig {
            voting_disabled: true,
            ..config.validator_config.clone()
        };
        (0..config.observer_amnt).for_each(|_| node_group.add_validator(&listener_config, 0));

        find_node_group_host(
            &node_group.connection_url_inf.gossip,
            config.node_stakes.len() + config.observer_amnt as usize,
        )
        .unwrap();

        for _ in 0..config.miner_amnt {
            node_group.add_miner();
        }

        find_node_group_host(
            &node_group.connection_url_inf.gossip,
            config.node_stakes.len() + config.miner_amnt as usize,
        )
        .unwrap();

        node_group
    }

    pub fn exit(&self) {
        for node in self.fullnodes.values() {
            node.exit();
        }
    }

    pub fn close_preserve_ledgers(&mut self) {
        self.exit();
        for (_, node) in self.fullnodes.drain() {
            node.join().unwrap();
        }

        while let Some(storage_miner) = self.miners.pop() {
            storage_miner.close();
        }
    }

    pub fn add_validator(&mut self, validator_config: &ValidatorConfig, stake: u64) {
        let client = create_client(
            self.connection_url_inf.client_facing_addr(),
            FULLNODE_PORT_RANGE,
        );

        // Must have enough tokens to fund vote account and set delegate
        let validator_keypair = Arc::new(Keypair::new());
        let voting_keypair = Keypair::new();
        let storage_keypair = Arc::new(Keypair::new());
        let validator_address = validator_keypair.address();
        let validator_node = Node::new_localhost_with_address(&validator_keypair.address());
        let ledger_path = tmp_copy_block_buffer!(&self.genesis_ledger_path);

        if validator_config.voting_disabled {
            // setup as a listener
            // info!("{}", Info(format!("listener {} ", validator_address,).to_string()));
            println!("{}",
                printLn(
                    format!("listener {} ", validator_address).to_string(),
                    module_path!().to_string()
                )
            );
        } else {
            // Give the validator some difs to setup vote and storage accounts
            let validator_balance = Self::transfer_with_client(
                &client,
                &self.funding_keypair,
                &validator_address,
                stake * 2 + 2,
            );
            // info!(
            //     "{}",
            //     Info(format!("validator {} balance {}",
            //     validator_address, validator_balance).to_string())
            // );
            println!("{}",
                printLn(
                    format!("validator {} balance {}",
                        validator_address, validator_balance
                    ).to_string(),
                    module_path!().to_string()
                )
            );
            Self::setup_vote_and_stake_accounts(
                &client,
                &voting_keypair,
                &validator_keypair,
                stake,
            )
            .unwrap();

            Self::setup_storage_account(&client, &storage_keypair, &validator_keypair, false)
                .unwrap();
        }

        let voting_keypair = Arc::new(voting_keypair);
        let validator_server = Validator::new(
            validator_node,
            &validator_keypair,
            &ledger_path,
            &voting_keypair.address(),
            &voting_keypair,
            &storage_keypair,
            Some(&self.connection_url_inf),
            &validator_config,
        );

        self.fullnodes
            .insert(validator_keypair.address(), validator_server);
        if validator_config.voting_disabled {
            self.listener_infos.insert(
                validator_keypair.address(),
                ValidatorInfo {
                    keypair: validator_keypair,
                    voting_keypair,
                    storage_keypair,
                    ledger_path,
                },
            );
        } else {
            self.fullnode_infos.insert(
                validator_keypair.address(),
                ValidatorInfo {
                    keypair: validator_keypair,
                    voting_keypair,
                    storage_keypair,
                    ledger_path,
                },
            );
        }
    }

    fn add_miner(&mut self) {
        let storage_miner_keypair = Arc::new(Keypair::new());
        let storage_miner_address = storage_miner_keypair.address();
        let storage_keypair = Arc::new(Keypair::new());
        let storage_address = storage_keypair.address();
        let client = create_client(
            self.connection_url_inf.client_facing_addr(),
            FULLNODE_PORT_RANGE,
        );

        // Give the storage_miner some difs to setup its storage accounts
        Self::transfer_with_client(
            &client,
            &self.funding_keypair,
            &storage_miner_keypair.address(),
            42,
        );
        let storage_miner_node = Node::new_localhost_storage_miner(&storage_miner_address);

        Self::setup_storage_account(&client, &storage_keypair, &storage_miner_keypair, true).unwrap();

        let (miner_ledger_path, _transaction_seal) = create_new_tmp_ledger!(&self.genesis_block);
        let storage_miner = StorageMiner::new(
            &miner_ledger_path,
            storage_miner_node,
            self.connection_url_inf.clone(),
            storage_miner_keypair,
            storage_keypair,
        )
        .unwrap_or_else(|err| panic!("StorageMiner::new() failed: {:?}", err));

        self.miners.push(storage_miner);
        self.storage_miner_infos.insert(
            storage_miner_address,
            StorageMinerInfo::new(storage_address, miner_ledger_path),
        );
    }

    fn close(&mut self) {
        self.close_preserve_ledgers();
        for ledger_path in self
            .fullnode_infos
            .values()
            .map(|f| &f.ledger_path)
            .chain(self.storage_miner_infos.values().map(|info| &info.ledger_path))
        {
            remove_dir_all(&ledger_path)
                .unwrap_or_else(|_| panic!("Unable to remove {}", ledger_path));
        }
    }

    pub fn transfer(&self, source_keypair: &Keypair, dest_address: &BvmAddr, difs: u64) -> u64 {
        let client = create_client(
            self.connection_url_inf.client_facing_addr(),
            FULLNODE_PORT_RANGE,
        );
        Self::transfer_with_client(&client, source_keypair, dest_address, difs)
    }

    fn transfer_with_client(
        client: &SlimAccountHost,
        source_keypair: &Keypair,
        dest_address: &BvmAddr,
        difs: u64,
    ) -> u64 {
        trace!("getting leader transaction_seal");
        let (transaction_seal, _fee_calculator) = client.get_recent_transaction_seal().unwrap();
        let mut tx = sys_controller::create_user_account(
            &source_keypair,
            dest_address,
            difs,
            transaction_seal,
        );
        // info!(
        //     "{}",
        //     Info(format!("executing transfer of {} from {} to {}",
        //     difs,
        //     source_keypair.address(),
        //     *dest_address).to_string())
        // );
        println!("{}",
            printLn(
                format!("executing transfer of {} from {} to {}",
                    difs,
                    source_keypair.address(),
                    *dest_address
                ).to_string(),
                module_path!().to_string()
            )
        );
        client
            .retry_transfer(&source_keypair, &mut tx, 5)
            .expect("client transfer");
        client
            .wait_for_balance(dest_address, Some(difs))
            .expect("get balance")
    }

    fn setup_vote_and_stake_accounts(
        client: &SlimAccountHost,
        vote_account: &Keypair,
        from_account: &Arc<Keypair>,
        amount: u64,
    ) -> Result<()> {
        let vote_account_address = vote_account.address();
        let node_address = from_account.address();

        // Create the vote account if necessary
        if client.poll_get_balance(&vote_account_address).unwrap_or(0) == 0 {
            // 1) Create vote account

            let mut transaction = Transaction::new_s_opcodes(
                &[from_account.as_ref()],
                vote_opcode::create_account(
                    &from_account.address(),
                    &vote_account_address,
                    &node_address,
                    0,
                    amount,
                ),
                client.get_recent_transaction_seal().unwrap().0,
            );
            client
                .retry_transfer(&from_account, &mut transaction, 5)
                .expect("fund vote");
            client
                .wait_for_balance(&vote_account_address, Some(amount))
                .expect("get balance");

            let stake_account_keypair = Keypair::new();
            let stake_account_address = stake_account_keypair.address();
            let mut transaction = Transaction::new_s_opcodes(
                &[from_account.as_ref()],
                stake_opcode::create_delegate_account(
                    &from_account.address(),
                    &stake_account_address,
                    amount,
                ),
                client.get_recent_transaction_seal().unwrap().0,
            );

            client
                .retry_transfer(&from_account, &mut transaction, 5)
                .expect("fund stake");
            client
                .wait_for_balance(&stake_account_address, Some(amount))
                .expect("get balance");

            let mut transaction = Transaction::new_s_opcodes(
                &[from_account.as_ref(), &stake_account_keypair],
                vec![stake_opcode::delegate_stake(
                    &from_account.address(),
                    &stake_account_address,
                    &vote_account_address,
                )],
                client.get_recent_transaction_seal().unwrap().0,
            );
            client
                .send_and_confirm_transaction(
                    &[from_account.as_ref(), &stake_account_keypair],
                    &mut transaction,
                    5,
                    0,
                )
                .expect("delegate stake");
        }
        // info!("{}", Info(format!("Checking for vote account registration").to_string()));
        println!("{}",
            printLn(
                format!("Checking for vote account registration").to_string(),
                module_path!().to_string()
            )
        );
        let vote_account_user_data = client.get_account_data(&vote_account_address);
        if let Ok(Some(vote_account_user_data)) = vote_account_user_data {
            if let Ok(vote_state) = VoteState::deserialize(&vote_account_user_data) {
                if vote_state.node_address == node_address {
                    // info!("{}", Info(format!("vote account registered").to_string()));
                    println!("{}",
                        printLn(
                            format!("vote account registered").to_string(),
                            module_path!().to_string()
                        )
                    );
                    return Ok(());
                }
            }
        }

        Err(Error::new(
            ErrorKind::Other,
            "expected successful vote account registration",
        ))
    }

    fn setup_storage_account(
        client: &SlimAccountHost,
        storage_keypair: &Keypair,
        from_acct: &Arc<Keypair>,
        storage_miner: bool,
    ) -> Result<()> {
        let context = Context::new_with_payer(
            if storage_miner {
                storage_opcode::create_miner_storage_account(
                    &from_acct.address(),
                    &storage_keypair.address(),
                    1,
                )
            } else {
                storage_opcode::create_validator_storage_account(
                    &from_acct.address(),
                    &storage_keypair.address(),
                    1,
                )
            },
            Some(&from_acct.address()),
        );
        let signer_keys = vec![from_acct.as_ref()];
        let transaction_seal = client.get_recent_transaction_seal().unwrap().0;
        let mut transaction = Transaction::new(&signer_keys, context, transaction_seal);
        client
            .retry_transfer(&from_acct, &mut transaction, 5)
            .map(|_signature| ())
    }
}

impl NodeGroup for LocalNodeGroup {
    fn get_node_addresss(&self) -> Vec<BvmAddr> {
        self.fullnodes.keys().cloned().collect()
    }

    fn restart_node(&mut self, address: BvmAddr) {
        // Shut down the fullnode
        let node = self.fullnodes.remove(&address).unwrap();
        node.exit();
        node.join().unwrap();

        // Restart the node
        let fullnode_info = &self.fullnode_infos[&address];
        let node = Node::new_localhost_with_address(&fullnode_info.keypair.address());
        if address == self.connection_url_inf.id {
            self.connection_url_inf = node.info.clone();
        }
        let restarted_node = Validator::new(
            node,
            &fullnode_info.keypair,
            &fullnode_info.ledger_path,
            &fullnode_info.voting_keypair.address(),
            &fullnode_info.voting_keypair,
            &fullnode_info.storage_keypair,
            None,
            &self.validator_config,
        );

        self.fullnodes.insert(address, restarted_node);
    }
}

impl Drop for LocalNodeGroup {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::bvm_types::STORAGE_ROTATE_TEST_COUNT;
    use morgan_runtime::epoch_schedule::MINIMUM_SLOT_LENGTH;

    #[test]
    fn test_local_node_group_start_and_exit() {
        morgan_logger::setup();
        let num_nodes = 1;
        let node_group = LocalNodeGroup::new_with_equal_stakes(num_nodes, 100, 3);
        assert_eq!(node_group.fullnodes.len(), num_nodes);
        assert_eq!(node_group.miners.len(), 0);
    }

    #[test]
    fn test_local_node_group_start_and_exit_with_config() {
        morgan_logger::setup();
        let mut validator_config = ValidatorConfig::default();
        validator_config.rpc_config.enable_fullnode_exit = true;
        validator_config.storage_rotate_count = STORAGE_ROTATE_TEST_COUNT;
        const NUM_NODES: usize = 1;
        let miner_amnt = 1;
        let config = NodeGroupConfig {
            validator_config,
            miner_amnt,
            node_stakes: vec![3; NUM_NODES],
            node_group_difs: 100,
            drops_per_slot: 8,
            candidate_each_round: MINIMUM_SLOT_LENGTH as u64,
            ..NodeGroupConfig::default()
        };
        let node_group = LocalNodeGroup::new(&config);
        assert_eq!(node_group.fullnodes.len(), NUM_NODES);
        assert_eq!(node_group.miners.len(), miner_amnt);
    }

}
