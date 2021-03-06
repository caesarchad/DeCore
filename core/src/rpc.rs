//! The `rpc` module implements the Morgan RPC interface.

// use crate::treasury_forks::TreasuryForks;
use crate::treasury_forks::TreasuryForks;
use crate::node_group_info::NodeGroupInfo;
use crate::connection_info::ContactInfo;
use crate::storage_stage::StorageState;
use morgan_interface::constants::PACKET_DATA_SIZE;
use bincode::{deserialize, serialize};
use jsonrpc_core::{Error, Metadata, Result};
use jsonrpc_derive::rpc;
use morgan_tokenbot::drone::{request_airdrop_transaction, request_reputation_airdrop_transaction};
use morgan_runtime::treasury::Treasury;
use morgan_interface::account::Account;
use morgan_interface::gas_cost::GasCost;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::Signature;
use morgan_interface::transaction::{self, Transaction};
use morgan_vote_api::vote_state::VoteState;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::time::{Duration, Instant};
use morgan_helper::logHelper::*;

#[derive(Debug, Clone)]
pub struct JsonRpcConfig {
    pub enable_fullnode_exit: bool, // Enable the 'fullnodeExit' command
    pub drone_addr: Option<SocketAddr>,
}

impl Default for JsonRpcConfig {
    fn default() -> Self {
        Self {
            enable_fullnode_exit: false,
            drone_addr: None,
        }
    }
}

#[derive(Clone)]
pub struct JsonRpcRequestProcessor {
    treasury_forks: Arc<RwLock<TreasuryForks>>,
    storage_state: StorageState,
    config: JsonRpcConfig,
    fullnode_exit: Arc<AtomicBool>,
}

impl JsonRpcRequestProcessor {
    fn treasury(&self) -> Arc<Treasury> {
        self.treasury_forks.read().unwrap().working_treasury()
    }

    pub fn new(
        storage_state: StorageState,
        config: JsonRpcConfig,
        treasury_forks: Arc<RwLock<TreasuryForks>>,
        fullnode_exit: &Arc<AtomicBool>,
    ) -> Self {
        JsonRpcRequestProcessor {
            treasury_forks,
            storage_state,
            config,
            fullnode_exit: fullnode_exit.clone(),
        }
    }

    pub fn get_account_info(&self, address: &BvmAddr) -> Result<Account> {
        self.treasury()
            .get_account(&address)
            .ok_or_else(Error::invalid_request)
    }

    pub fn get_balance(&self, address: &BvmAddr) -> u64 {
        self.treasury().get_balance(&address)
    }

    pub fn get_reputation(&self, address: &BvmAddr) -> u64 {
        self.treasury().get_reputation(&address)
    }

    fn get_recent_transaction_seal(&self) -> (String, GasCost) {
        (
            self.treasury().confirmed_last_transaction_seal().to_string(),
            self.treasury().fee_calculator.clone(),
        )
    }

    pub fn get_signature_status(&self, signature: Signature) -> Option<transaction::Result<()>> {
        self.get_signature_confirmation_status(signature)
            .map(|x| x.1)
    }

    pub fn get_signature_confirmations(&self, signature: Signature) -> Option<usize> {
        self.get_signature_confirmation_status(signature)
            .map(|x| x.0)
    }

    pub fn get_signature_confirmation_status(
        &self,
        signature: Signature,
    ) -> Option<(usize, transaction::Result<()>)> {
        self.treasury().get_signature_confirmation_status(&signature)
    }

    fn get_transaction_count(&self) -> Result<u64> {
        Ok(self.treasury().transaction_count() as u64)
    }

    fn get_epoch_vote_accounts(&self) -> Result<Vec<(BvmAddr, u64, VoteState)>> {
        let treasury = self.treasury();
        Ok(treasury
            .epoch_vote_accounts(treasury.get_stakers_epoch(treasury.slot()))
            .ok_or_else(Error::invalid_request)?
            .iter()
            .map(|(k, (s, a))| (*k, *s, VoteState::from(a).unwrap_or_default()))
            .collect::<Vec<_>>())
    }

    fn get_storage_transaction_seal(&self) -> Result<String> {
        Ok(self.storage_state.get_storage_transaction_seal().to_string())
    }

    fn get_storage_slot(&self) -> Result<u64> {
        Ok(self.storage_state.get_slot())
    }

    fn get_storage_addresss_for_slot(&self, slot: u64) -> Result<Vec<BvmAddr>> {
        Ok(self.storage_state.get_addresss_for_slot(slot))
    }

    pub fn fullnode_exit(&self) -> Result<bool> {
        if self.config.enable_fullnode_exit {
            // warn!("fullnode_exit request...");
            println!(
                "{}",
                Warn(
                    format!("fullnode_exit request...").to_string(),
                    module_path!().to_string()
                )
            );
            self.fullnode_exit.store(true, Ordering::Relaxed);
            Ok(true)
        } else {
            debug!("fullnode_exit ignored");
            Ok(false)
        }
    }
}

fn get_transaction_digesting_module_addr(node_group_info: &Arc<RwLock<NodeGroupInfo>>) -> Result<SocketAddr> {
    let contact_info = node_group_info.read().unwrap().my_data();
    Ok(contact_info.transaction_digesting_module)
}

fn verify_address(input: String) -> Result<BvmAddr> {
    input.parse().map_err(|_e| Error::invalid_request())
}

fn verify_signature(input: &str) -> Result<Signature> {
    input.parse().map_err(|_e| Error::invalid_request())
}

#[derive(Clone)]
pub struct Meta {
    pub request_processor: Arc<RwLock<JsonRpcRequestProcessor>>,
    pub node_group_info: Arc<RwLock<NodeGroupInfo>>,
}
impl Metadata for Meta {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcContactInfo {
    /// Base58 id
    pub id: String,
    /// Gossip port
    pub gossip: Option<SocketAddr>,
    /// TransactionDigestingModule port
    pub transaction_digesting_module: Option<SocketAddr>,
    /// JSON RPC port
    pub rpc: Option<SocketAddr>,
}

#[rpc(server)]
pub trait RpcSol {
    type Metadata;

    #[rpc(meta, name = "confirmTxn")]
    fn confirm_transaction(&self, _: Self::Metadata, _: String) -> Result<bool>;

    #[rpc(meta, name = "getAccountInfo")]
    fn get_account_info(&self, _: Self::Metadata, _: String) -> Result<Account>;

    #[rpc(meta, name = "getDif")]
    fn get_balance(&self, _: Self::Metadata, _: String) -> Result<u64>;

    #[rpc(meta, name = "getReputation")]
    fn get_reputation(&self, _: Self::Metadata, _: String) -> Result<u64>;

    #[rpc(meta, name = "getClusterNodes")]
    fn get_cluster_nodes(&self, _: Self::Metadata) -> Result<Vec<RpcContactInfo>>;

    #[rpc(meta, name = "getLatestTransactionSeal")]
    fn get_recent_transaction_seal(&self, _: Self::Metadata) -> Result<(String, GasCost)>;

    #[rpc(meta, name = "getSignatureState")]
    fn get_signature_status(
        &self,
        _: Self::Metadata,
        _: String,
    ) -> Result<Option<transaction::Result<()>>>;

    #[rpc(meta, name = "getTxnCnt")]
    fn get_transaction_count(&self, _: Self::Metadata) -> Result<u64>;

    #[rpc(meta, name = "requestDif")]
    fn request_airdrop(&self, _: Self::Metadata, _: String, _: u64) -> Result<String>;

    #[rpc(meta, name = "requestReputation")]
    fn request_reputation(&self, _: Self::Metadata, _: String, _: u64) -> Result<String>;

    #[rpc(meta, name = "sendTxn")]
    fn send_transaction(&self, _: Self::Metadata, _: Vec<u8>) -> Result<String>;

    #[rpc(meta, name = "getRoundLeader")]
    fn get_slot_leader(&self, _: Self::Metadata) -> Result<String>;

    #[rpc(meta, name = "getEpochVoteAccounts")]
    fn get_epoch_vote_accounts(&self, _: Self::Metadata) -> Result<Vec<(BvmAddr, u64, VoteState)>>;

    #[rpc(meta, name = "getStorageTransactionSeal")]
    fn get_storage_transaction_seal(&self, _: Self::Metadata) -> Result<String>;

    #[rpc(meta, name = "getStorageSlot")]
    fn get_storage_slot(&self, _: Self::Metadata) -> Result<u64>;

    #[rpc(meta, name = "getStoragePubkeysForSlot")]
    fn get_storage_addresss_for_slot(&self, _: Self::Metadata, _: u64) -> Result<Vec<BvmAddr>>;

    #[rpc(meta, name = "fullnodeQuit")]
    fn fullnode_exit(&self, _: Self::Metadata) -> Result<bool>;

    #[rpc(meta, name = "getNumBlocksSinceSignatureConfirmation")]
    fn get_num_blocks_since_signature_confirmation(
        &self,
        _: Self::Metadata,
        _: String,
    ) -> Result<Option<usize>>;

    #[rpc(meta, name = "getSignatureConfirmation")]
    fn get_signature_confirmation(
        &self,
        _: Self::Metadata,
        _: String,
    ) -> Result<Option<(usize, transaction::Result<()>)>>;
}

pub struct RpcSolImpl;
impl RpcSol for RpcSolImpl {
    type Metadata = Meta;

    fn confirm_transaction(&self, meta: Self::Metadata, id: String) -> Result<bool> {
        debug!("confirm_transaction rpc request received: {:?}", id);
        self.get_signature_status(meta, id).map(|status_option| {
            if status_option.is_none() {
                return false;
            }
            status_option.unwrap().is_ok()
        })
    }

    fn get_account_info(&self, meta: Self::Metadata, id: String) -> Result<Account> {
        debug!("get_account_info rpc request received: {:?}", id);
        let address = verify_address(id)?;
        meta.request_processor
            .read()
            .unwrap()
            .get_account_info(&address)
    }

    fn get_balance(&self, meta: Self::Metadata, id: String) -> Result<u64> {
        debug!("get_balance rpc request received: {:?}", id);
        let address = verify_address(id)?;
        Ok(meta.request_processor.read().unwrap().get_balance(&address))
    }

    fn get_reputation(&self, meta: Self::Metadata, id: String) -> Result<u64> {
        debug!("get_reputation rpc request received: {:?}", id);
        let address = verify_address(id)?;
        Ok(meta.request_processor.read().unwrap().get_reputation(&address))
    }

    fn get_cluster_nodes(&self, meta: Self::Metadata) -> Result<Vec<RpcContactInfo>> {
        let node_group_info = meta.node_group_info.read().unwrap();
        fn valid_address_or_none(addr: &SocketAddr) -> Option<SocketAddr> {
            if ContactInfo::is_valid_address(addr) {
                Some(*addr)
            } else {
                None
            }
        }
        Ok(node_group_info
            .all_peers()
            .iter()
            .filter_map(|(contact_info, _)| {
                if ContactInfo::is_valid_address(&contact_info.gossip) {
                    Some(RpcContactInfo {
                        id: contact_info.id.to_string(),
                        gossip: Some(contact_info.gossip),
                        transaction_digesting_module: valid_address_or_none(&contact_info.transaction_digesting_module),
                        rpc: valid_address_or_none(&contact_info.rpc),
                    })
                } else {
                    None // Exclude spy nodes
                }
            })
            .collect())
    }

    fn get_recent_transaction_seal(&self, meta: Self::Metadata) -> Result<(String, GasCost)> {
        debug!("get_recent_transaction_seal rpc request received");
        Ok(meta
            .request_processor
            .read()
            .unwrap()
            .get_recent_transaction_seal())
    }

    fn get_signature_status(
        &self,
        meta: Self::Metadata,
        id: String,
    ) -> Result<Option<transaction::Result<()>>> {
        self.get_signature_confirmation(meta, id)
            .map(|res| res.map(|x| x.1))
    }

    fn get_num_blocks_since_signature_confirmation(
        &self,
        meta: Self::Metadata,
        id: String,
    ) -> Result<Option<usize>> {
        self.get_signature_confirmation(meta, id)
            .map(|res| res.map(|x| x.0))
    }

    fn get_signature_confirmation(
        &self,
        meta: Self::Metadata,
        id: String,
    ) -> Result<Option<(usize, transaction::Result<()>)>> {
        debug!("get_signature_confirmation rpc request received: {:?}", id);
        let signature = verify_signature(&id)?;
        Ok(meta
            .request_processor
            .read()
            .unwrap()
            .get_signature_confirmation_status(signature))
    }

    fn get_transaction_count(&self, meta: Self::Metadata) -> Result<u64> {
        debug!("get_transaction_count rpc request received");
        meta.request_processor
            .read()
            .unwrap()
            .get_transaction_count()
    }

    fn request_airdrop(&self, meta: Self::Metadata, id: String, difs: u64) -> Result<String> {
        trace!("request_airdrop id={} difs={}", id, difs);

        let drone_addr = meta
            .request_processor
            .read()
            .unwrap()
            .config
            .drone_addr
            .ok_or_else(Error::invalid_request)?;
        let address = verify_address(id)?;

        let transaction_seal = meta
            .request_processor
            .read()
            .unwrap()
            .treasury()
            .confirmed_last_transaction_seal();
        let transaction = request_airdrop_transaction(&drone_addr, &address, difs, transaction_seal)
            .map_err(|err| {
                // info!("{}", Info(format!("request_airdrop_transaction failed: {:?}", err).to_string()));
                println!("{}",
                    printLn(
                        format!("request_airdrop_transaction failed: {:?}", err).to_string(),
                        module_path!().to_string()
                    )
                );
                Error::internal_error()
            })?;;

        let data = serialize(&transaction).map_err(|err| {
            // info!("{}", Info(format!("request_airdrop: serialize error: {:?}", err).to_string()));
            println!("{}",
                printLn(
                    format!("request_airdrop: serialize error: {:?}", err).to_string(),
                    module_path!().to_string()
                )
            );
            Error::internal_error()
        })?;

        let account_host_socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let account_host_url = get_transaction_digesting_module_addr(&meta.node_group_info)?;
        account_host_socket
            .send_to(&data, account_host_url)
            .map_err(|err| {
                // info!("{}", Info(format!("request_airdrop: send_to error: {:?}", err).to_string()));
                println!("{}",
                    printLn(
                        format!("request_airdrop: send_to error: {:?}", err).to_string(),
                        module_path!().to_string()
                    )
                );
                Error::internal_error()
            })?;

        let signature = transaction.signatures[0];
        let now = Instant::now();
        let mut signature_status;
        loop {
            signature_status = meta
                .request_processor
                .read()
                .unwrap()
                .get_signature_status(signature);

            if signature_status == Some(Ok(())) {
                // info!("{}", Info(format!("airdrop signature ok").to_string()));
                println!("{}",
                    printLn(
                        format!("airdrop signature ok").to_string(),
                        module_path!().to_string()
                    )
                );
                return Ok(signature.to_string());
            } else if now.elapsed().as_secs() > 5 {
                // info!("{}", Info(format!("airdrop signature timeout").to_string()));
                println!("{}",
                    printLn(
                        format!("airdrop signature timeout").to_string(),
                        module_path!().to_string()
                    )
                );
                return Err(Error::internal_error());
            }
            sleep(Duration::from_millis(100));
        }
    }

    fn request_reputation(&self, meta: Self::Metadata, id: String, reputations: u64) -> Result<String> {
        trace!("request_reputation id={} difs={}", id, reputations);

        let drone_addr = meta
            .request_processor
            .read()
            .unwrap()
            .config
            .drone_addr
            .ok_or_else(Error::invalid_request)?;
        let address = verify_address(id)?;

        let transaction_seal = meta
            .request_processor
            .read()
            .unwrap()
            .treasury()
            .confirmed_last_transaction_seal();
        let transaction = request_reputation_airdrop_transaction(&drone_addr, &address, reputations, transaction_seal)
            .map_err(|err| {
                // info!("{}", Info(format!("request_reputation_airdrop_transaction failed: {:?}", err).to_string()));
                println!("{}",
                    printLn(
                        format!("request_reputation_airdrop_transaction failed: {:?}", err).to_string(),
                        module_path!().to_string()
                    )
                );
                Error::internal_error()
            })?;;

        let data = serialize(&transaction).map_err(|err| {
            // info!("{}", Info(format!("request_airdrop: serialize error: {:?}", err).to_string()));
            println!("{}",
                printLn(
                    format!("request_airdrop: serialize error: {:?}", err).to_string(),
                    module_path!().to_string()
                )
            );
            Error::internal_error()
        })?;

        let account_host_socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let account_host_url = get_transaction_digesting_module_addr(&meta.node_group_info)?;
        account_host_socket
            .send_to(&data, account_host_url)
            .map_err(|err| {
                // info!("{}", Info(format!("request_airdrop: send_to error: {:?}", err).to_string()));
                println!("{}",
                    printLn(
                        format!("request_airdrop: send_to error: {:?}", err).to_string(),
                        module_path!().to_string()
                    )
                );
                Error::internal_error()
            })?;

        let signature = transaction.signatures[0];
        let now = Instant::now();
        let mut signature_status;
        loop {
            signature_status = meta
                .request_processor
                .read()
                .unwrap()
                .get_signature_status(signature);

            if signature_status == Some(Ok(())) {
                // info!("{}", Info(format!("airdrop signature ok").to_string()));
                println!("{}",
                    printLn(
                        format!("airdrop signature ok").to_string(),
                        module_path!().to_string()
                    )
                );
                return Ok(signature.to_string());
            } else if now.elapsed().as_secs() > 5 {
                // info!("{}", Info(format!("airdrop signature timeout").to_string()));
                println!("{}",
                    printLn(
                        format!("airdrop signature timeout").to_string(),
                        module_path!().to_string()
                    )
                );
                return Err(Error::internal_error());
            }
            sleep(Duration::from_millis(100));
        }
    }

    fn send_transaction(&self, meta: Self::Metadata, data: Vec<u8>) -> Result<String> {
        let tx: Transaction = deserialize(&data).map_err(|err| {
            println!("{}",
                printLn(
                    format!("send_transaction: deserialize error: {:?}", err).to_string(),
                    module_path!().to_string()
                )
            );
            Error::invalid_request()
        })?;
        if data.len() >= PACKET_DATA_SIZE {
            
            println!("{}",
                printLn(
                    format!("send_transaction: transaction too large: {} bytes (max: {} bytes)",
                        data.len(),
                        PACKET_DATA_SIZE).to_string(),
                    module_path!().to_string()
                )
            );
            return Err(Error::invalid_request());
        }
        let account_host_socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let account_host_url = get_transaction_digesting_module_addr(&meta.node_group_info)?;
        trace!("send_transaction: leader is {:?}", &account_host_url);
        account_host_socket
            .send_to(&data, account_host_url)
            .map_err(|err| {
                println!("{}",
                    printLn(
                        format!("send_transaction: send_to error: {:?}", err).to_string(),
                        module_path!().to_string()
                    )
                );
                Error::internal_error()
            })?;
        let signature = tx.signatures[0].to_string();
        trace!(
            "send_transaction: sent {} bytes, signature={}",
            data.len(),
            signature
        );
        Ok(signature)
    }

    fn get_slot_leader(&self, meta: Self::Metadata) -> Result<String> {
        let node_group_info = meta.node_group_info.read().unwrap();
        let leader_data_option = node_group_info.leader_data();
        Ok(leader_data_option
            .and_then(|leader_data| Some(leader_data.id))
            .unwrap_or_default()
            .to_string())
    }

    fn get_epoch_vote_accounts(
        &self,
        meta: Self::Metadata,
    ) -> Result<Vec<(BvmAddr, u64, VoteState)>> {
        meta.request_processor
            .read()
            .unwrap()
            .get_epoch_vote_accounts()
    }

    fn get_storage_transaction_seal(&self, meta: Self::Metadata) -> Result<String> {
        meta.request_processor
            .read()
            .unwrap()
            .get_storage_transaction_seal()
    }

    fn get_storage_slot(&self, meta: Self::Metadata) -> Result<u64> {
        meta.request_processor.read().unwrap().get_storage_slot()
    }

    fn get_storage_addresss_for_slot(&self, meta: Self::Metadata, slot: u64) -> Result<Vec<BvmAddr>> {
        meta.request_processor
            .read()
            .unwrap()
            .get_storage_addresss_for_slot(slot)
    }

    fn fullnode_exit(&self, meta: Self::Metadata) -> Result<bool> {
        meta.request_processor.read().unwrap().fullnode_exit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_info::ContactInfo;
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use jsonrpc_core::{MetaIoHandler, Response};
    use morgan_interface::hash::{hash, Hash};
    use morgan_interface::opcodes::OpCodeErr;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::sys_controller;
    use morgan_interface::transaction::TransactionError;
    use std::thread;

    fn start_rpc_handler_with_tx(
        address: &BvmAddr,
    ) -> (MetaIoHandler<Meta>, Meta, Hash, Keypair, BvmAddr) {
        let (treasury_forks, alice) = new_treasury_forks();
        let treasury = treasury_forks.read().unwrap().working_treasury();
        let exit = Arc::new(AtomicBool::new(false));

        let transaction_seal = treasury.confirmed_last_transaction_seal();
        let tx = sys_controller::transfer(&alice, address, 20, transaction_seal);
        treasury.process_transaction(&tx).expect("process transaction");

        let tx = sys_controller::transfer(&alice, &alice.address(), 20, transaction_seal);
        let _ = treasury.process_transaction(&tx);

        let request_processor = Arc::new(RwLock::new(JsonRpcRequestProcessor::new(
            StorageState::default(),
            JsonRpcConfig::default(),
            treasury_forks,
            &exit,
        )));
        let node_group_info = Arc::new(RwLock::new(NodeGroupInfo::new_with_invalid_keypair(
            ContactInfo::default(),
        )));
        let leader = ContactInfo::new_with_socketaddr(&socketaddr!("127.0.0.1:1234"));

        node_group_info.write().unwrap().insert_info(leader.clone());

        let mut io = MetaIoHandler::default();
        let rpc = RpcSolImpl;
        io.extend_with(rpc.to_delegate());
        let meta = Meta {
            request_processor,
            node_group_info,
        };
        (io, meta, transaction_seal, alice, leader.id)
    }

    #[test]
    fn test_rpc_request_processor_new() {
        let bob_address = BvmAddr::new_rand();
        let exit = Arc::new(AtomicBool::new(false));
        let (treasury_forks, alice) = new_treasury_forks();
        let treasury = treasury_forks.read().unwrap().working_treasury();
        let request_processor = JsonRpcRequestProcessor::new(
            StorageState::default(),
            JsonRpcConfig::default(),
            treasury_forks,
            &exit,
        );
        thread::spawn(move || {
            let transaction_seal = treasury.confirmed_last_transaction_seal();
            let tx = sys_controller::transfer(&alice, &bob_address, 20, transaction_seal);
            treasury.process_transaction(&tx).expect("process transaction");
        })
        .join()
        .unwrap();
        assert_eq!(request_processor.get_transaction_count().unwrap(), 1);
    }

    #[test]
    fn test_rpc_get_balance() {
        let bob_address = BvmAddr::new_rand();
        let (io, meta, _transaction_seal, _alice, _leader_address) = start_rpc_handler_with_tx(&bob_address);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getDif","params":["{}"]}}"#,
            bob_address
        );
        let res = io.handle_request_sync(&req, meta);
        let expected = format!(r#"{{"jsonrpc":"2.0","result":20,"id":1}}"#);
        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_reputation() {
        let bob_address = BvmAddr::new_rand();
        let (io, meta, _transaction_seal, _alice, _leader_address) = start_rpc_handler_with_tx(&bob_address);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getReputation","params":["{}"]}}"#,
            bob_address
        );
        let res = io.handle_request_sync(&req, meta);
        let expected = format!(r#"{{"jsonrpc":"2.0","result":0,"id":1}}"#);
        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_cluster_nodes() {
        let bob_address = BvmAddr::new_rand();
        let (io, meta, _transaction_seal, _alice, leader_addr) = start_rpc_handler_with_tx(&bob_address);

        let req = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"getClusterNodes"}}"#);
        let res = io.handle_request_sync(&req, meta);
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        let expected = format!(
            r#"{{"jsonrpc":"2.0","result":[{{"id": "{}", "gossip": "127.0.0.1:1235", "transaction_digesting_module": "127.0.0.1:1234", "rpc": "127.0.0.1:10099"}}],"id":1}}"#,
            leader_addr,
        );

        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_slot_leader() {
        let bob_address = BvmAddr::new_rand();
        let (io, meta, _transaction_seal, _alice, _leader_address) = start_rpc_handler_with_tx(&bob_address);

        let req = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"getRoundLeader"}}"#);
        let res = io.handle_request_sync(&req, meta);
        let expected =
            format!(r#"{{"jsonrpc":"2.0","result":"11111111111111111111111111111111","id":1}}"#);
        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_tx_count() {
        let bob_address = BvmAddr::new_rand();
        let (io, meta, _transaction_seal, _alice, _leader_address) = start_rpc_handler_with_tx(&bob_address);

        let req = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"getTxnCnt"}}"#);
        let res = io.handle_request_sync(&req, meta);
        let expected = format!(r#"{{"jsonrpc":"2.0","result":1,"id":1}}"#);
        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_account_info() {
        let bob_address = BvmAddr::new_rand();
        let (io, meta, _transaction_seal, _alice, _leader_address) = start_rpc_handler_with_tx(&bob_address);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{}"]}}"#,
            bob_address
        );
        let res = io.handle_request_sync(&req, meta);
        let expected = r#"{
            "jsonrpc":"2.0",
            "result":{
                "owner": [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                "difs": 20,
                "reputations": 0,
                "data": [],
                "executable": false
            },
            "id":1}
        "#;
        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_confirm_tx() {
        let bob_address = BvmAddr::new_rand();
        let (io, meta, transaction_seal, alice, _leader_address) = start_rpc_handler_with_tx(&bob_address);
        let tx = sys_controller::transfer(&alice, &bob_address, 20, transaction_seal);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"confirmTxn","params":["{}"]}}"#,
            tx.signatures[0]
        );
        let res = io.handle_request_sync(&req, meta);
        let expected = format!(r#"{{"jsonrpc":"2.0","result":true,"id":1}}"#);
        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_signature_status() {
        let bob_address = BvmAddr::new_rand();
        let (io, meta, transaction_seal, alice, _leader_address) = start_rpc_handler_with_tx(&bob_address);
        let tx = sys_controller::transfer(&alice, &bob_address, 20, transaction_seal);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSignatureState","params":["{}"]}}"#,
            tx.signatures[0]
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected_res: Option<transaction::Result<()>> = Some(Ok(()));
        let expected = json!({
            "jsonrpc": "2.0",
            "result": expected_res,
            "id": 1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Test getSignatureStatus request on unprocessed tx
        let tx = sys_controller::transfer(&alice, &bob_address, 10, transaction_seal);
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSignatureState","params":["{}"]}}"#,
            tx.signatures[0]
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected_res: Option<String> = None;
        let expected = json!({
            "jsonrpc": "2.0",
            "result": expected_res,
            "id": 1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Test getSignatureStatus request on a TransactionError
        let tx = sys_controller::transfer(&alice, &alice.address(), 20, transaction_seal);
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSignatureState","params":["{}"]}}"#,
            tx.signatures[0]
        );
        let res = io.handle_request_sync(&req, meta);
        let expected_res: Option<transaction::Result<()>> = Some(Err(
            TransactionError::OpCodeErr(0, OpCodeErr::DuplicateAccountIndex),
        ));
        let expected = json!({
            "jsonrpc": "2.0",
            "result": expected_res,
            "id": 1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_recent_transaction_seal() {
        let bob_address = BvmAddr::new_rand();
        let (io, meta, transaction_seal, _alice, _leader_address) = start_rpc_handler_with_tx(&bob_address);

        let req = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"getLatestTransactionSeal"}}"#);
        let res = io.handle_request_sync(&req, meta);
        let expected = format!(
            r#"{{"jsonrpc":"2.0","result":["{}", {{"difsPerSignature": 0}}],"id":1}}"#,
            transaction_seal
        );
        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_fail_request_airdrop() {
        let bob_address = BvmAddr::new_rand();
        let (io, meta, _transaction_seal, _alice, _leader_address) = start_rpc_handler_with_tx(&bob_address);

        // Expect internal error because no drone is available
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"requestDif","params":["{}", 50]}}"#,
            bob_address
        );
        let res = io.handle_request_sync(&req, meta);
        let expected =
            r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request"},"id":1}"#;
        let expected: Response =
            serde_json::from_str(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_send_bad_tx() {
        let exit = Arc::new(AtomicBool::new(false));

        let mut io = MetaIoHandler::default();
        let rpc = RpcSolImpl;
        io.extend_with(rpc.to_delegate());
        let meta = Meta {
            request_processor: {
                let request_processor = JsonRpcRequestProcessor::new(
                    StorageState::default(),
                    JsonRpcConfig::default(),
                    new_treasury_forks().0,
                    &exit,
                );
                Arc::new(RwLock::new(request_processor))
            },
            node_group_info: Arc::new(RwLock::new(NodeGroupInfo::new_with_invalid_keypair(
                ContactInfo::default(),
            ))),
        };

        let req =
            r#"{"jsonrpc":"2.0","id":1,"method":"sendTxn","params":[[0,0,0,0,0,0,0,0]]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let expected =
            r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request"},"id":1}"#;
        let expected: Response =
            serde_json::from_str(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_transaction_digesting_module_addr() {
        let node_group_info = Arc::new(RwLock::new(NodeGroupInfo::new_with_invalid_keypair(
            ContactInfo::new_with_socketaddr(&socketaddr!("127.0.0.1:1234")),
        )));
        assert_eq!(
            get_transaction_digesting_module_addr(&node_group_info),
            Ok(socketaddr!("127.0.0.1:1234"))
        );
    }

    #[test]
    fn test_rpc_verify_address() {
        let address = BvmAddr::new_rand();
        assert_eq!(verify_address(address.to_string()).unwrap(), address);
        let bad_address = "a1b2c3d4";
        assert_eq!(
            verify_address(bad_address.to_string()),
            Err(Error::invalid_request())
        );
    }

    #[test]
    fn test_rpc_verify_signature() {
        let tx = sys_controller::transfer(&Keypair::new(), &BvmAddr::new_rand(), 20, hash(&[0]));
        assert_eq!(
            verify_signature(&tx.signatures[0].to_string()).unwrap(),
            tx.signatures[0]
        );
        let bad_signature = "a1b2c3d4";
        assert_eq!(
            verify_signature(&bad_signature.to_string()),
            Err(Error::invalid_request())
        );
    }

    fn new_treasury_forks() -> (Arc<RwLock<TreasuryForks>>, Keypair) {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(10_000);
        let treasury = Treasury::new(&genesis_block);
        (
            Arc::new(RwLock::new(TreasuryForks::new(treasury.slot(), treasury))),
            mint_keypair,
        )
    }

    #[test]
    fn test_rpc_request_processor_config_default_trait_fullnode_exit_fails() {
        let exit = Arc::new(AtomicBool::new(false));
        let request_processor = JsonRpcRequestProcessor::new(
            StorageState::default(),
            JsonRpcConfig::default(),
            new_treasury_forks().0,
            &exit,
        );
        assert_eq!(request_processor.fullnode_exit(), Ok(false));
        assert_eq!(exit.load(Ordering::Relaxed), false);
    }

    #[test]
    fn test_rpc_request_processor_allow_fullnode_exit_config() {
        let exit = Arc::new(AtomicBool::new(false));
        let mut config = JsonRpcConfig::default();
        config.enable_fullnode_exit = true;
        let request_processor = JsonRpcRequestProcessor::new(
            StorageState::default(),
            config,
            new_treasury_forks().0,
            &exit,
        );
        assert_eq!(request_processor.fullnode_exit(), Ok(true));
        assert_eq!(exit.load(Ordering::Relaxed), true);
    }
}
