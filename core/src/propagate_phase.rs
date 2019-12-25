//! A phase to broadcast data from a leader node to validators
//!
use crate::block_buffer_pool::BlockBufferPool;
use crate::node_group_info::{NodeGroupInfo, NodeGroupInfoError, DATA_PLANE_FANOUT};
use crate::fiscal_statement_info::FsclStmtSlc;
use crate::expunge::CodingGenerator;
use crate::packet::index_blobs_with_genesis;
use crate::water_clock_recorder::WorkingTreasuryEntries;
use crate::result::{Error, Result};
use crate::service::Service;
use crate::staking_utils;
use rayon::prelude::*;
use morgan_metricbot::{
    datapoint, inc_new_counter_debug, inc_new_counter_error, inc_new_counter_info,
    inc_new_counter_warn,
};
use morgan_interface::hash::Hash;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::timing::duration_as_ms;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, RwLock};
use std::thread::{self, Builder, JoinHandle};
use std::time::{Duration, Instant};
use morgan_helper::logHelper::*;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BroadcastPhaseReturnType {
    ChannelDisconnected,
}

#[derive(Default)]
struct BroadcastStats {
    fscl_stmt_cnt: Vec<usize>,
    run_elapsed: Vec<u64>,
    to_blobs_elapsed: Vec<u64>,
}

struct Broadcast {
    id: Pubkey,
    coding_generator: CodingGenerator,
    stats: BroadcastStats,
}

impl Broadcast {
    fn run(
        &mut self,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        receiver: &Receiver<WorkingTreasuryEntries>,
        sock: &UdpSocket,
        block_buffer_pool: &Arc<BlockBufferPool>,
        genesis_transaction_seal: &Hash,
    ) -> Result<()> {
        let timer = Duration::new(1, 0);
        let (mut treasury, entries) = receiver.recv_timeout(timer)?;
        let mut max_drop_height = treasury.max_drop_height();

        let run_start = Instant::now();
        let mut fscl_stmt_cnt = entries.len();
        let mut ventries = Vec::new();
        let mut last_drop = entries.last().map(|v| v.1).unwrap_or(0);
        ventries.push(entries);

        assert!(last_drop <= max_drop_height);
        if last_drop != max_drop_height {
            while let Ok((same_treasury, entries)) = receiver.try_recv() {
                // If the treasury changed, that implies the previous slot was interrupted and we do not have to
                // broadcast its entries.
                if same_treasury.slot() != treasury.slot() {
                    fscl_stmt_cnt = 0;
                    ventries.clear();
                    treasury = same_treasury.clone();
                    max_drop_height = treasury.max_drop_height();
                }
                fscl_stmt_cnt += entries.len();
                last_drop = entries.last().map(|v| v.1).unwrap_or(0);
                ventries.push(entries);
                assert!(last_drop <= max_drop_height,);
                if last_drop == max_drop_height {
                    break;
                }
            }
        }

        let treasury_round = treasury.get_stakers_epoch(treasury.slot());
        let mut broadcast_table = node_group_info
            .read()
            .unwrap()
            .ordered_blaze_node_lists(staking_utils::staked_nodes_at_epoch(&treasury, treasury_round).as_ref());

        inc_new_counter_warn!("broadcast_service-num_peers", broadcast_table.len() + 1);
        // Layer 1, leader nodes are limited to the fanout size.
        broadcast_table.truncate(DATA_PLANE_FANOUT);

        inc_new_counter_info!("broadcast_service-entries_received", fscl_stmt_cnt);

        let to_blobs_start = Instant::now();

        let blobs: Vec<_> = ventries
            .into_par_iter()
            .map(|p| {
                let entries: Vec<_> = p.into_iter().map(|e| e.0).collect();
                entries.to_shared_blobs()
            })
            .flatten()
            .collect();

        let blob_index = block_buffer_pool
            .meta(treasury.slot())
            .expect("Database error")
            .map(|meta| meta.consumed)
            .unwrap_or(0);

        index_blobs_with_genesis(
            &blobs,
            &self.id,
            genesis_transaction_seal,
            blob_index,
            treasury.slot(),
            treasury.parent().map_or(0, |parent| parent.slot()),
        );

        let contains_last_drop = last_drop == max_drop_height;

        if contains_last_drop {
            blobs.last().unwrap().write().unwrap().set_is_last_in_slot();
        }

        block_buffer_pool.record_public_objs(&blobs)?;

        let coding = self.coding_generator.next(&blobs);

        let to_blobs_elapsed = duration_as_ms(&to_blobs_start.elapsed());

        let broadcast_start = Instant::now();

        // Send out data
        NodeGroupInfo::broadcast(&self.id, contains_last_drop, &broadcast_table, sock, &blobs)?;

        inc_new_counter_debug!("streamer-broadcast-sent", blobs.len());

        // send out erasures
        NodeGroupInfo::broadcast(&self.id, false, &broadcast_table, sock, &coding)?;

        self.update_broadcast_stats(
            duration_as_ms(&broadcast_start.elapsed()),
            duration_as_ms(&run_start.elapsed()),
            fscl_stmt_cnt,
            to_blobs_elapsed,
            blob_index,
        );

        Ok(())
    }

    fn update_broadcast_stats(
        &mut self,
        broadcast_elapsed: u64,
        run_elapsed: u64,
        fscl_stmt_cnt: usize,
        to_blobs_elapsed: u64,
        blob_index: u64,
    ) {
        inc_new_counter_info!("broadcast_service-time_ms", broadcast_elapsed as usize);

        self.stats.fscl_stmt_cnt.push(fscl_stmt_cnt);
        self.stats.to_blobs_elapsed.push(to_blobs_elapsed);
        self.stats.run_elapsed.push(run_elapsed);
        if self.stats.fscl_stmt_cnt.len() >= 16 {
            // info!(
            //     "{}",
            //     Info(format!("broadcast: entries: {:?} blob times ms: {:?} broadcast times ms: {:?}",
            //     self.stats.fscl_stmt_cnt, self.stats.to_blobs_elapsed, self.stats.run_elapsed).to_string())
            // );
            let loginfo: String = format!("propagation: entries num: {:?} spot times ms: {:?} propagation times ms: {:?}",
                self.stats.fscl_stmt_cnt, self.stats.to_blobs_elapsed, self.stats.run_elapsed).to_string();
            println!("{}",
                printLn(
                    loginfo,
                    module_path!().to_string()
                )
            );
            self.stats.fscl_stmt_cnt.clear();
            self.stats.to_blobs_elapsed.clear();
            self.stats.run_elapsed.clear();
        }

        datapoint!("broadcast-service", ("transmit-index", blob_index, i64));
    }
}

// Implement a destructor for the BroadcastPhase thread to signal it exited
// even on panics
struct EndSignal {
    exit_sender: Arc<AtomicBool>,
}

impl EndSignal {
    fn new(exit_sender: Arc<AtomicBool>) -> Self {
        EndSignal { exit_sender }
    }
}
// Implement a destructor for EndSignal.
impl Drop for EndSignal {
    fn drop(&mut self) {
        self.exit_sender.clone().store(true, Ordering::Relaxed);
    }
}

pub struct BroadcastPhase {
    thread_hdl: JoinHandle<BroadcastPhaseReturnType>,
}

impl BroadcastPhase {
    #[allow(clippy::too_many_arguments)]
    fn run(
        sock: &UdpSocket,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        receiver: &Receiver<WorkingTreasuryEntries>,
        block_buffer_pool: &Arc<BlockBufferPool>,
        genesis_transaction_seal: &Hash,
    ) -> BroadcastPhaseReturnType {
        let me = node_group_info.read().unwrap().my_data().clone();
        let coding_generator = CodingGenerator::default();

        let mut broadcast = Broadcast {
            id: me.id,
            coding_generator,
            stats: BroadcastStats::default(),
        };

        loop {
            if let Err(e) =
                broadcast.run(&node_group_info, receiver, sock, block_buffer_pool, genesis_transaction_seal)
            {
                match e {
                    Error::RecvTimeoutError(RecvTimeoutError::Disconnected) | Error::SendError => {
                        return BroadcastPhaseReturnType::ChannelDisconnected;
                    }
                    Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                    Error::NodeGroupInfoError(NodeGroupInfoError::NoPeers) => (), // TODO: Why are the unit-tests throwing hundreds of these?
                    _ => {
                        inc_new_counter_error!("streamer-broadcaster-error", 1, 1);
                        // error!("{}", Error(format!("broadcaster error: {:?}", e).to_string()));
                        println!(
                            "{}",
                            Error(
                                format!("broadcaster error: {:?}", e).to_string(),
                                module_path!().to_string()
                            )
                        );
                    }
                }
            }
        }
    }

    /// Service to broadcast messages from the leader to layer 1 nodes.
    /// See `node_group_info` for network layer definitions.
    /// # Arguments
    /// * `sock` - Socket to send from.
    /// * `exit` - Boolean to signal system exit.
    /// * `node_group_info` - NodeGroupInfo structure
    /// * `window` - Cache of blobs that we have broadcast
    /// * `receiver` - Receive channel for blobs to be retransmitted to all the layer 1 nodes.
    /// * `exit_sender` - Set to true when this service exits, allows rest of TransactionDigestingModule to exit cleanly.
    /// Otherwise, when a TransactionDigestingModule closes, it only closes the phasea that come after it. The phasea
    /// that come before could be blocked on a receive, and never notice that they need to
    /// exit. Now, if any phase of the TransactionDigestingModule closes, it will lead to closing the WritePhase (b/c
    /// WritePhase is the last phase in the pipeline), which will then close Broadcast service,
    /// which will then close FetchPhase in the TransactionDigestingModule, and then the rest of the TransactionDigestingModule,
    /// completing the cycle.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sock: UdpSocket,
        node_group_info: Arc<RwLock<NodeGroupInfo>>,
        receiver: Receiver<WorkingTreasuryEntries>,
        exit_sender: &Arc<AtomicBool>,
        block_buffer_pool: &Arc<BlockBufferPool>,
        genesis_transaction_seal: &Hash,
    ) -> Self {
        let block_buffer_pool = block_buffer_pool.clone();
        let exit_sender = exit_sender.clone();
        let genesis_transaction_seal = *genesis_transaction_seal;
        let thread_hdl = Builder::new()
            .name("morgan-broadcaster".to_string())
            .spawn(move || {
                let _finalizer = EndSignal::new(exit_sender);
                Self::run(
                    &sock,
                    &node_group_info,
                    &receiver,
                    &block_buffer_pool,
                    &genesis_transaction_seal,
                )
            })
            .unwrap();

        Self { thread_hdl }
    }
}

impl Service for BroadcastPhase {
    type JoinReturnType = BroadcastPhaseReturnType;

    fn join(self) -> thread::Result<BroadcastPhaseReturnType> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::block_buffer_pool::{fetch_interim_ledger_location, BlockBufferPool};
    use crate::node_group_info::{NodeGroupInfo, Node};
    use crate::fiscal_statement_info::create_drops;
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use crate::service::Service;
    use morgan_runtime::treasury::Treasury;
    use morgan_interface::hash::Hash;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, RwLock};
    use std::thread::sleep;
    use std::time::Duration;

    struct MockBroadcastPhase {
        block_buffer_pool: Arc<BlockBufferPool>,
        broadcast_service: BroadcastPhase,
        treasury: Arc<Treasury>,
    }

    fn setup_dummy_broadcast_service(
        leader_pubkey: &Pubkey,
        ledger_path: &str,
        entry_receiver: Receiver<WorkingTreasuryEntries>,
    ) -> MockBroadcastPhase {
        // Make the database ledger
        let block_buffer_pool = Arc::new(BlockBufferPool::open_ledger_file(ledger_path).unwrap());

        // Make the leader node and scheduler
        let leader_info = Node::new_localhost_with_pubkey(leader_pubkey);

        // Make a node to broadcast to
        let buddy_keypair = Keypair::new();
        let broadcast_buddy = Node::new_localhost_with_pubkey(&buddy_keypair.pubkey());

        // Fill the node_group_info with the buddy's info
        let mut node_group_info = NodeGroupInfo::new_with_invalid_keypair(leader_info.info.clone());
        node_group_info.insert_info(broadcast_buddy.info);
        let node_group_info = Arc::new(RwLock::new(node_group_info));

        let exit_sender = Arc::new(AtomicBool::new(false));

        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(10_000);
        let treasury = Arc::new(Treasury::new(&genesis_block));

        // Start up the broadcast phase
        let broadcast_service = BroadcastPhase::new(
            leader_info.sockets.broadcast,
            node_group_info,
            entry_receiver,
            &exit_sender,
            &block_buffer_pool,
            &Hash::default(),
        );

        MockBroadcastPhase {
            block_buffer_pool,
            broadcast_service,
            treasury,
        }
    }

    #[test]
    fn test_broadcast_ledger() {
        morgan_logger::setup();
        let ledger_path = fetch_interim_ledger_location("test_broadcast_ledger");

        {
            // Create the leader scheduler
            let leader_keypair = Keypair::new();

            let (entry_sender, entry_receiver) = channel();
            let broadcast_service = setup_dummy_broadcast_service(
                &leader_keypair.pubkey(),
                &ledger_path,
                entry_receiver,
            );
            let treasury = broadcast_service.treasury.clone();
            let start_drop_height = treasury.drop_height();
            let max_drop_height = treasury.max_drop_height();
            let drops_per_slot = treasury.drops_per_slot();

            let drops = create_drops(max_drop_height - start_drop_height, Hash::default());
            for (i, _drop) in drops.into_iter().enumerate() {
                entry_sender
                    .send((treasury.clone(), vec![(_drop, i as u64 + 1)]))
                    .expect("Expect successful send to broadcast service");
            }

            sleep(Duration::from_millis(2000));

            trace!(
                "[broadcast_ledger] max_drop_height: {}, start_drop_height: {}, drops_per_slot: {}",
                max_drop_height,
                start_drop_height,
                drops_per_slot,
            );

            let block_buffer_pool = broadcast_service.block_buffer_pool;
            let mut blob_index = 0;
            for i in 0..max_drop_height - start_drop_height {
                let slot = (start_drop_height + i + 1) / drops_per_slot;

                let result = block_buffer_pool.fetch_data_blob(slot, blob_index).unwrap();

                blob_index += 1;
                result.expect("expect blob presence");
            }

            drop(entry_sender);
            broadcast_service
                .broadcast_service
                .join()
                .expect("Expect successful join of broadcast service");
        }

        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }
}
