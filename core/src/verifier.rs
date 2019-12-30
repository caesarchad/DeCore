//! The `fullnode` module hosts all the fullnode microservices.

// use crate::treasury_forks::TreasuryForks;
use crate::treasury_forks::TreasuryForks;
use crate::block_buffer_pool::{BlockBufferPool, CompletedSlotsReceiver};
use crate::block_buffer_pool_processor::{self, TreasuryForksInfo};
use crate::node_group_info::{NodeGroupInfo, Node};
use crate::connection_info::ContactInfo;
use crate::gossip_service::{find_node_group_host, GossipService};
use crate::leader_arrange_cache::LdrSchBufferPoolList;
use crate::water_clock_recorder::WaterClockRecorder;
use crate::water_clock_service::WaterClockService;
use crate::rpc::JsonRpcConfig;
use crate::rpc_pub_subervice::PubSubService;
use crate::rpc_service::JsonRpcService;
use crate::rpc_subscriptions::RpcSubscriptions;
use crate::service::Service;
use crate::storage_stage::StorageState;
use crate::transaction_process_centre::TransactionDigestingModule;
use crate::transaction_verify_centre::{Sockets, BlazeUnit};
use morgan_metricbot::inc_new_counter_info;
use morgan_runtime::treasury::Treasury;
use morgan_interface::genesis_block::GenesisBlock;
use morgan_interface::waterclock_config::WaterClockConfig;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::timing::timestamp;
use morgan_poc_agnt::pgm_id::SLOTS_PER_SEGMENT;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::Result;
use morgan_helper::logHelper::*;

#[derive(Clone, Debug)]
pub struct ValidatorConfig {
    pub sigverify_disabled: bool,
    pub voting_disabled: bool,
    pub nodesyncflow: Option<String>,
    pub storage_rotate_count: u64,
    pub account_paths: Option<String>,
    pub rpc_config: JsonRpcConfig,
}
impl Default for ValidatorConfig {
    fn default() -> Self {
        // TODO: remove this, temporary parameter to configure
        // storage amount differently for test configurations
        // so tests don't take forever to run.
        const NUM_HASHES_FOR_STORAGE_ROTATE: u64 = SLOTS_PER_SEGMENT;
        Self {
            sigverify_disabled: false,
            voting_disabled: false,
            nodesyncflow: None,
            storage_rotate_count: NUM_HASHES_FOR_STORAGE_ROTATE,
            account_paths: None,
            rpc_config: JsonRpcConfig::default(),
        }
    }
}

pub struct Validator {
    pub id: BvmAddr,
    exit: Arc<AtomicBool>,
    rpc_service: Option<JsonRpcService>,
    rpc_pubsub_service: Option<PubSubService>,
    gossip_service: GossipService,
    waterclock_recorder: Arc<Mutex<WaterClockRecorder>>,
    waterclock_service: WaterClockService,
    transaction_digesting_module: TransactionDigestingModule,
    blaze_unit: BlazeUnit,
    ip_echo_server: morgan_netutil::IpEchoServer,
}

impl Validator {
    pub fn new(
        mut node: Node,
        keypair: &Arc<Keypair>,
        ledger_path: &str,
        vote_account: &BvmAddr,
        voting_keypair: &Arc<Keypair>,
        storage_keypair: &Arc<Keypair>,
        entrypoint_info_option: Option<&ContactInfo>,
        config: &ValidatorConfig,
    ) -> Self {
        // info!("{}", Info(format!("creating treasury...").to_string()));
        println!("{}",
            printLn(
                format!("creating treasury...").to_string(),
                module_path!().to_string()
            )
        );
        let id = keypair.address();
        assert_eq!(id, node.info.id);
        let genesis_block =
            GenesisBlock::load(ledger_path).expect("Expected to successfully open genesis block");
        let treasury = Treasury::new_with_paths(&genesis_block, None);
        let genesis_transaction_seal = treasury.last_transaction_seal();

        let (
            treasury_forks,
            treasury_forks_info,
            block_buffer_pool,
            ledger_signal_receiver,
            completed_slots_receiver,
            leader_schedule_cache,
            waterclock_config,
        ) = new_treasuries_from_block_buffer(ledger_path, config.account_paths.clone());

        let leader_schedule_cache = Arc::new(leader_schedule_cache);
        let exit = Arc::new(AtomicBool::new(false));
        let treasury_info = &treasury_forks_info[0];
        let treasury = treasury_forks[treasury_info.treasury_slot].clone();

        // info!(
        //     "{}",
        //     Info(format!("starting Water Clock... {} {}",
        //     treasury.drop_height(),
        //     treasury.last_transaction_seal()).to_string())
        // );
        println!("{}",
            printLn(
                format!("starting water clock... {} {}",
                    treasury.drop_height(),
                    treasury.last_transaction_seal()).to_string(),
                module_path!().to_string()
            )
        );
        let block_buffer_pool = Arc::new(block_buffer_pool);

        let waterclock_config = Arc::new(waterclock_config);
        let (waterclock_recorder, entry_receiver) = WaterClockRecorder::new_with_clear_signal(
            treasury.drop_height(),
            treasury.last_transaction_seal(),
            treasury.slot(),
            leader_schedule_cache.next_leader_slot(&id, treasury.slot(), &treasury, Some(&block_buffer_pool)),
            treasury.drops_per_slot(),
            &id,
            &block_buffer_pool,
            block_buffer_pool.new_blobs_signals.first().cloned(),
            &leader_schedule_cache,
            &waterclock_config,
        );
        let waterclock_recorder = Arc::new(Mutex::new(waterclock_recorder));
        let waterclock_service = WaterClockService::new(waterclock_recorder.clone(), &waterclock_config, &exit);
        assert_eq!(
            block_buffer_pool.new_blobs_signals.len(),
            1,
            "New blob signal for the TVU should be the same as the clear treasury signal."
        );

        // info!("{}", Info(format!("node info: {:?}", node.info).to_string()));
        // info!("{}", Info(format!("node entrypoint_info: {:?}", entrypoint_info_option).to_string()));
        // info!(
        //     "{}",
        //     Info(format!("node local gossip address: {}",
        //     node.sockets.gossip.local_addr().unwrap()).to_string())
        // );
        println!("{}",
            printLn(
                format!("node connection info: {:?}", node.info).to_string(),
                module_path!().to_string()
            )
        );
        println!("{}",
            printLn(
                format!("node entrance address: {:?}", entrypoint_info_option).to_string(),
                module_path!().to_string()
            )
        );
        println!("{}",
            printLn(
                format!("node local gossip address: {}",
                    node.sockets.gossip.local_addr().unwrap()).to_string(),
                module_path!().to_string()
            )
        );
        let treasury_forks = Arc::new(RwLock::new(treasury_forks));

        node.info.wallclock = timestamp();
        let node_group_info = Arc::new(RwLock::new(NodeGroupInfo::new(
            node.info.clone(),
            keypair.clone(),
        )));

        let storage_state = StorageState::new();

        let rpc_service = if node.info.rpc.port() == 0 {
            None
        } else {
            Some(JsonRpcService::new(
                &node_group_info,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), node.info.rpc.port()),
                storage_state.clone(),
                config.rpc_config.clone(),
                treasury_forks.clone(),
                &exit,
            ))
        };

        let ip_echo_server =
            crate::ip_echo_server::ip_echo_server(node.sockets.gossip.local_addr().unwrap().port());

        let subscriptions = Arc::new(RpcSubscriptions::default());
        let rpc_pubsub_service = if node.info.rpc_pubsub.port() == 0 {
            None
        } else {
            Some(PubSubService::new(
                &subscriptions,
                SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                    node.info.rpc_pubsub.port(),
                ),
                &exit,
            ))
        };

        let gossip_service = GossipService::new(
            &node_group_info,
            Some(block_buffer_pool.clone()),
            Some(treasury_forks.clone()),
            node.sockets.gossip,
            &exit,
        );

        // Insert the entrypoint info, should only be None if this node
        // is the bootstrap leader

        if let Some(entrypoint_info) = entrypoint_info_option {
            node_group_info
                .write()
                .unwrap()
                .set_entrypoint(entrypoint_info.clone());
        }

        let sockets = Sockets {
            repair: node
                .sockets
                .repair
                .try_clone()
                .expect("Failed to clone repair socket"),
            retransmit: node
                .sockets
                .retransmit
                .try_clone()
                .expect("Failed to clone retransmit socket"),
            fetch: node
                .sockets
                .blaze_unit
                .iter()
                .map(|s| s.try_clone().expect("Failed to clone TVU Sockets"))
                .collect(),
        };

        let voting_keypair = if config.voting_disabled {
            None
        } else {
            Some(voting_keypair)
        };

        let blaze_unit = BlazeUnit::new(
            vote_account,
            voting_keypair,
            storage_keypair,
            &treasury_forks,
            &node_group_info,
            sockets,
            block_buffer_pool.clone(),
            config.storage_rotate_count,
            &storage_state,
            config.nodesyncflow.as_ref(),
            ledger_signal_receiver,
            &subscriptions,
            &waterclock_recorder,
            &leader_schedule_cache,
            &exit,
            &genesis_transaction_seal,
            completed_slots_receiver,
        );

        if config.sigverify_disabled {
            // warn!("signature verification disabled");
            println!(
                "{}",
                Warn(
                    format!("signature verification disabled").to_string(),
                    module_path!().to_string()
                )
            );
        }

        let transaction_digesting_module = TransactionDigestingModule::new(
            &id,
            &node_group_info,
            &waterclock_recorder,
            entry_receiver,
            node.sockets.transaction_digesting_module,
            node.sockets.transaction_digesting_module_via_blobs,
            node.sockets.broadcast,
            config.sigverify_disabled,
            &block_buffer_pool,
            &exit,
            &genesis_transaction_seal,
        );

        inc_new_counter_info!("fullnode-new", 1);
        Self {
            id,
            gossip_service,
            rpc_service,
            rpc_pubsub_service,
            transaction_digesting_module,
            blaze_unit,
            exit,
            waterclock_service,
            waterclock_recorder,
            ip_echo_server,
        }
    }

    // Used for notifying many nodes in parallel to exit
    pub fn exit(&self) {
        self.exit.store(true, Ordering::Relaxed);
    }

    pub fn close(self) -> Result<()> {
        self.exit();
        self.join()
    }
}

pub fn new_treasuries_from_block_buffer(
    block_buffer_pool_path: &str,
    account_paths: Option<String>,
) -> (
    TreasuryForks,
    Vec<TreasuryForksInfo>,
    BlockBufferPool,
    Receiver<bool>,
    CompletedSlotsReceiver,
    LdrSchBufferPoolList,
    WaterClockConfig,
) {
    let genesis_block =
        GenesisBlock::load(block_buffer_pool_path).expect("Expected to successfully open genesis block");

    let (block_buffer_pool, ledger_signal_receiver, completed_slots_receiver) =
        BlockBufferPool::open_by_message(block_buffer_pool_path)
            .expect("Expected to successfully open database ledger");

    let (treasury_forks, treasury_forks_info, leader_schedule_cache) =
        block_buffer_pool_processor::process_block_buffer_pool(&genesis_block, &block_buffer_pool, account_paths, 32)
            .expect("process_block_buffer_pool failed");

    (
        treasury_forks,
        treasury_forks_info,
        block_buffer_pool,
        ledger_signal_receiver,
        completed_slots_receiver,
        leader_schedule_cache,
        genesis_block.waterclock_config,
    )
}

impl Service for Validator {
    type JoinReturnType = ();

    fn join(self) -> Result<()> {
        self.waterclock_service.join()?;
        drop(self.waterclock_recorder);
        if let Some(rpc_service) = self.rpc_service {
            rpc_service.join()?;
        }
        if let Some(rpc_pubsub_service) = self.rpc_pubsub_service {
            rpc_pubsub_service.join()?;
        }

        self.gossip_service.join()?;
        self.transaction_digesting_module.join()?;
        self.blaze_unit.join()?;
        self.ip_echo_server.shutdown_now();

        Ok(())
    }
}

pub fn new_validator_for_tests() -> (Validator, ContactInfo, Keypair, String) {
    use crate::block_buffer_pool::create_new_tmp_ledger;
    use crate::genesis_utils::{create_genesis_block_with_leader, GenesisBlockInfo};

    let node_keypair = Arc::new(Keypair::new());
    let node = Node::new_localhost_with_address(&node_keypair.address());
    let contact_info = node.info.clone();

    let GenesisBlockInfo {
        mut genesis_block,
        mint_keypair,
        ..
    } = create_genesis_block_with_leader(10_000, &contact_info.id, 42);
    genesis_block
        .builtin_opcode_handlers
        .push(morgan_budget_controller!());

    let (ledger_path, _transaction_seal) = create_new_tmp_ledger!(&genesis_block);

    let voting_keypair = Arc::new(Keypair::new());
    let storage_keypair = Arc::new(Keypair::new());
    let node = Validator::new(
        node,
        &node_keypair,
        &ledger_path,
        &voting_keypair.address(),
        &voting_keypair,
        &storage_keypair,
        None,
        &ValidatorConfig::default(),
    );
    find_node_group_host(&contact_info.gossip, 1).expect("Node startup failed");
    (node, contact_info, mint_keypair, ledger_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_buffer_pool::create_new_tmp_ledger;
    use crate::genesis_utils::create_genesis_block_with_leader;
    use std::fs::remove_dir_all;

    #[test]
    fn validator_exit() {
        morgan_logger::setup();
        let leader_keypair = Keypair::new();
        let leader_node = Node::new_localhost_with_address(&leader_keypair.address());

        let validator_keypair = Keypair::new();
        let validator_node = Node::new_localhost_with_address(&validator_keypair.address());
        let genesis_block =
            create_genesis_block_with_leader(10_000, &leader_keypair.address(), 1000).genesis_block;
        let (validator_ledger_path, _transaction_seal) = create_new_tmp_ledger!(&genesis_block);

        let voting_keypair = Arc::new(Keypair::new());
        let storage_keypair = Arc::new(Keypair::new());
        let validator = Validator::new(
            validator_node,
            &Arc::new(validator_keypair),
            &validator_ledger_path,
            &voting_keypair.address(),
            &voting_keypair,
            &storage_keypair,
            Some(&leader_node.info),
            &ValidatorConfig::default(),
        );
        validator.close().unwrap();
        remove_dir_all(validator_ledger_path).unwrap();
    }

    #[test]
    fn validator_parallel_exit() {
        let leader_keypair = Keypair::new();
        let leader_node = Node::new_localhost_with_address(&leader_keypair.address());

        let mut ledger_paths = vec![];
        let validators: Vec<Validator> = (0..2)
            .map(|_| {
                let validator_keypair = Keypair::new();
                let validator_node = Node::new_localhost_with_address(&validator_keypair.address());
                let genesis_block =
                    create_genesis_block_with_leader(10_000, &leader_keypair.address(), 1000)
                        .genesis_block;
                let (validator_ledger_path, _transaction_seal) = create_new_tmp_ledger!(&genesis_block);
                ledger_paths.push(validator_ledger_path.clone());
                let voting_keypair = Arc::new(Keypair::new());
                let storage_keypair = Arc::new(Keypair::new());
                Validator::new(
                    validator_node,
                    &Arc::new(validator_keypair),
                    &validator_ledger_path,
                    &voting_keypair.address(),
                    &voting_keypair,
                    &storage_keypair,
                    Some(&leader_node.info),
                    &ValidatorConfig::default(),
                )
            })
            .collect();

        // Each validator can exit in parallel to speed many sequential calls to `join`
        validators.iter().for_each(|v| v.exit());
        // While join is called sequentially, the above exit call notified all the
        // validators to exit from all their threads
        validators.into_iter().for_each(|validator| {
            validator.join().unwrap();
        });

        for path in ledger_paths {
            remove_dir_all(path).unwrap();
        }
    }
}
