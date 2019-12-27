//! The `nodesync_srvc` implements optional streaming of entries and block metadata
//! using the `nodesyncflow` module, providing client services such as a block explorer with
//! real-time access to entries.

use crate::node_sync_flow::NodeSyncEvents;
#[cfg(test)]
use crate::node_sync_flow::FakeNodeSync as NodeSyncFlow;
#[cfg(not(test))]
use crate::node_sync_flow::SocketNodeSync as NodeSyncFlow;
use crate::block_buffer_pool::BlockBufferPool;
use crate::result::{Error, Result};
use crate::service::Service;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::constants::{
    QUALIFIER,
    ORGANIZATION,
    APPLICATION,
    KEYS_DIRECTORY,
    N3H_BINARIES_DIRECTORY,
    DNA_EXTENSION,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;
use morgan_helper::logHelper::*;
use std::path::PathBuf;

pub struct NodeSyncSrvc {
    nodesync_thread: JoinHandle<()>,
}




pub fn project_root() -> Option<directories::ProjectDirs> {
    directories::ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION)
}


pub fn config_root() -> PathBuf {
    project_root()
        .map(|dirs| dirs.config_dir().to_owned())
        .unwrap_or_else(|| PathBuf::from("/etc").join(APPLICATION))
}


pub fn data_root() -> PathBuf {
    project_root()
        .map(|dirs| dirs.data_dir().to_owned())
        .unwrap_or_else(|| PathBuf::from("/etc").join(APPLICATION))
}


pub fn keys_directory() -> PathBuf {
    config_root().join(KEYS_DIRECTORY)
}

/// Returns the path to where n3h binaries will be downloaded / run
/// Something like "~/.local/share/holochain/n3h-binaries"
pub fn n3h_binaries_directory() -> PathBuf {
    data_root().join(N3H_BINARIES_DIRECTORY)
}

impl NodeSyncSrvc {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        slot_full_receiver: Receiver<(u64, BvmAddr)>,
        block_buffer_pool: Arc<BlockBufferPool>,
        node_sync_socket: String,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        let mut nodesyncflow = NodeSyncFlow::new(node_sync_socket);
        let exit = exit.clone();
        let nodesync_thread = Builder::new()
            .name("morgan-nodesyncflow".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(e) =
                    Self::handle_fiscal_stmts(&slot_full_receiver, &block_buffer_pool, &mut nodesyncflow)
                {
                    match e {
                        Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => break,
                        Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                        _ => {
                            // info!("{}", Info(format!("Error from handle_fiscal_stmts: {:?}", e).to_string())),
                            let loginfo: String = format!("Error from handle_fiscal_stmts: {:?}", e).to_string();
                            println!("{}",
                                printLn(
                                    loginfo,
                                    module_path!().to_string()
                                )
                            );
                        }
                    }
                }
            })
            .unwrap();
        Self { nodesync_thread }
    }
    fn handle_fiscal_stmts(
        slot_full_receiver: &Receiver<(u64, BvmAddr)>,
        block_buffer_pool: &Arc<BlockBufferPool>,
        nodesyncflow: &mut NodeSyncFlow,
    ) -> Result<()> {
        let timeout = Duration::new(1, 0);
        let (slot, slot_leader) = slot_full_receiver.recv_timeout(timeout)?;

        let entries = block_buffer_pool.fetch_candidate_fscl_stmts(slot, 0, None).unwrap();
        let block_buffer_meta = block_buffer_pool.meta(slot).unwrap().unwrap();
        let _parent_slot = if slot == 0 {
            None
        } else {
            Some(block_buffer_meta.parent_slot)
        };
        let drops_per_slot = entries
            .iter()
            .filter(|entry| entry.is_drop())
            .fold(0, |acc, _| acc + 1);
        let mut drop_height = if slot > 0 {
            drops_per_slot * slot - 1
        } else {
            0
        };

        for (i, entry) in entries.iter().enumerate() {
            if entry.is_drop() {
                drop_height += 1;
            }
            nodesyncflow
                .emit_fscl_stmt_event(slot, drop_height, &slot_leader, &entry)
                .unwrap_or_else(|e| {
                    debug!("NodeSyncFlow error: {:?}, {:?}", e, nodesyncflow.output);
                });
            if i == entries.len() - 1 {
                nodesyncflow
                    .emit_block_event(slot, drop_height, &slot_leader, entry.hash)
                    .unwrap_or_else(|e| {
                        debug!("NodeSyncFlow error: {:?}, {:?}", e, nodesyncflow.output);
                    });
            }
        }
        Ok(())
    }
}

impl Service for NodeSyncSrvc {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.nodesync_thread.join()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::block_buffer_pool::create_new_tmp_ledger;
    use crate::fiscal_statement_info::{create_drops, FsclStmt};
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use bincode::{deserialize, serialize};
    use chrono::{DateTime, FixedOffset};
    use serde_json::Value;
    use morgan_interface::hash::Hash;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::sys_controller;
    use std::sync::mpsc::channel;

    #[test]
    fn test_blockstream_service_process_entries() {
        let drops_per_slot = 5;
        let leader_pubkey = BvmAddr::new_rand();

        // Set up genesis block and block_buffer_pool
        let GenesisBlockInfo {
            mut genesis_block, ..
        } = create_genesis_block(1000);
        genesis_block.drops_per_slot = drops_per_slot;

        let (ledger_path, _transaction_seal) = create_new_tmp_ledger!(&genesis_block);
        let block_buffer_pool = BlockBufferPool::open_ledger_file(&ledger_path).unwrap();

        // Set up nodesyncflow
        let mut nodesyncflow = NodeSyncFlow::new("test_stream".to_string());

        // Set up dummy channel to receive a full-slot notification
        let (slot_full_sender, slot_full_receiver) = channel();

        // Create entries - 4 drops + 1 populated entry + 1 drop
        let mut entries = create_drops(4, Hash::default());

        let keypair = Keypair::new();
        let mut transaction_seal = entries[3].hash;
        let tx = sys_controller::create_user_account(
            &keypair,
            &keypair.pubkey(),
            1,
            Hash::default(),
        );
        let entry = FsclStmt::new(&mut transaction_seal, 1, vec![tx]);
        transaction_seal = entry.hash;
        entries.push(entry);
        let final_drop = create_drops(1, transaction_seal);
        entries.extend_from_slice(&final_drop);

        let expected_entries = entries.clone();
        let expected_drop_heights = [5, 6, 7, 8, 8, 9];

        block_buffer_pool
            .update_fscl_stmts(1, 0, 0, drops_per_slot, &entries)
            .unwrap();

        slot_full_sender.send((1, leader_pubkey)).unwrap();
        NodeSyncSrvc::handle_fiscal_stmts(
            &slot_full_receiver,
            &Arc::new(block_buffer_pool),
            &mut nodesyncflow,
        )
        .unwrap();
        assert_eq!(nodesyncflow.statements().len(), 7);

        let (entry_events, block_events): (Vec<Value>, Vec<Value>) = nodesyncflow
            .statements()
            .iter()
            .map(|item| {
                let json: Value = serde_json::from_str(&item).unwrap();
                let dt_str = json["dt"].as_str().unwrap();
                // Ensure `ts` field parses as valid DateTime
                let _dt: DateTime<FixedOffset> = DateTime::parse_from_rfc3339(dt_str).unwrap();
                json
            })
            .partition(|json| {
                let item_type = json["t"].as_str().unwrap();
                item_type == "entry"
            });
        for (i, json) in entry_events.iter().enumerate() {
            let height = json["h"].as_u64().unwrap();
            assert_eq!(height, expected_drop_heights[i]);
            let entry_obj = json["entry"].clone();
            let tx = entry_obj["transactions"].as_array().unwrap();
            let entry: FsclStmt;
            if tx.len() == 0 {
                entry = serde_json::from_value(entry_obj).unwrap();
            } else {
                let entry_json = entry_obj.as_object().unwrap();
                entry = FsclStmt {
                    num_hashes: entry_json.get("num_hashes").unwrap().as_u64().unwrap(),
                    ////mark_seal: 19,
                    hash: serde_json::from_value(entry_json.get("hash").unwrap().clone()).unwrap(),
                    transactions: entry_json
                        .get("transactions")
                        .unwrap()
                        .as_array()
                        .unwrap()
                        .into_iter()
                        .enumerate()
                        .map(|(j, tx)| {
                            let tx_vec: Vec<u8> = serde_json::from_value(tx.clone()).unwrap();
                            // Check explicitly that transaction matches bincode-serialized format
                            assert_eq!(
                                tx_vec,
                                serialize(&expected_entries[i].transactions[j]).unwrap()
                            );
                            deserialize(&tx_vec).unwrap()
                        })
                        .collect(),
                };
            }
            assert_eq!(entry, expected_entries[i]);
        }
        for json in block_events {
            let slot = json["s"].as_u64().unwrap();
            assert_eq!(1, slot);
            let height = json["h"].as_u64().unwrap();
            assert_eq!(2 * drops_per_slot - 1, height);
        }
    }
}
