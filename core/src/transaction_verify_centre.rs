//! The `tvu` module implements the Transaction Validation Unit, a
//! multi-phase transaction validation pipeline in software.
//!
//! 1. BlobFetchPhase
//! - Incoming blobs are picked up from the TVU sockets and repair socket.
//! 2. RetransmitPhase
//! - Blobs are windowed until a contiguous chunk is available.  This phase also repairs and
//! retransmits blobs that are in the queue.
//! 3. ReplayPhase
//! - Transactions in blobs are processed and applied to the treasury.
//! - TODO We need to verify the signatures in the blobs.
//! 4. StoragePhase
//! - Generating the keys used to encrypt the ledger and sample it for storage mining.

// use crate::treasury_forks::TreasuryForks;
use crate::treasury_forks::TreasuryForks;
use crate::fetch_spot_phase::BlobFetchPhase;
use crate::block_stream_service::BlockstreamService;
use crate::block_buffer_pool::{BlockBufferPool, CompletedSlotsReceiver};
use crate::node_group_info::NodeGroupInfo;
use crate::leader_arrange_cache::LeaderScheduleCache;
use crate::water_clock_recorder::WaterClockRecorder;
use crate::repeat_phase::ReplayPhase;
use crate::retransmit_phase::RetransmitPhase;
use crate::rpc_subscriptions::RpcSubscriptions;
use crate::service::Service;
use crate::storage_stage::{StoragePhase, StorageState};
use morgan_interface::hash::Hash;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::signature::{Keypair, KeypairUtil};
use std::net::UdpSocket;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

pub struct Tvu {
    fetch_phase: BlobFetchPhase,
    retransmit_phase: RetransmitPhase,
    replay_phase: ReplayPhase,
    blockstream_service: Option<BlockstreamService>,
    storage_stage: StoragePhase,
}

pub struct Sockets {
    pub fetch: Vec<UdpSocket>,
    pub repair: UdpSocket,
    pub retransmit: UdpSocket,
}

impl Tvu {
    /// This service receives messages from a leader in the network and processes the transactions
    /// on the treasury state.
    /// # Arguments
    /// * `node_group_info` - The node_group_info state.
    /// * `sockets` - fetch, repair, and retransmit sockets
    /// * `block_buffer_pool` - the ledger itself
    #[allow(clippy::new_ret_no_self, clippy::too_many_arguments)]
    pub fn new<T>(
        vote_account: &Pubkey,
        voting_keypair: Option<&Arc<T>>,
        storage_keypair: &Arc<Keypair>,
        treasury_forks: &Arc<RwLock<TreasuryForks>>,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        sockets: Sockets,
        block_buffer_pool: Arc<BlockBufferPool>,
        storage_rotate_count: u64,
        storage_state: &StorageState,
        blockstream: Option<&String>,
        ledger_signal_receiver: Receiver<bool>,
        subscriptions: &Arc<RpcSubscriptions>,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        exit: &Arc<AtomicBool>,
        genesis_transaction_seal: &Hash,
        completed_slots_receiver: CompletedSlotsReceiver,
    ) -> Self
    where
        T: 'static + KeypairUtil + Sync + Send,
    {
        let keypair: Arc<Keypair> = node_group_info
            .read()
            .expect("Unable to read from node_group_info during Tvu creation")
            .keypair
            .clone();

        let Sockets {
            repair: repair_socket,
            fetch: fetch_sockets,
            retransmit: retransmit_socket,
        } = sockets;

        let (blob_fetch_sender, blob_fetch_receiver) = channel();

        let repair_socket = Arc::new(repair_socket);
        let mut blob_sockets: Vec<Arc<UdpSocket>> =
            fetch_sockets.into_iter().map(Arc::new).collect();
        blob_sockets.push(repair_socket.clone());
        let fetch_phase = BlobFetchPhase::new_multi_socket(blob_sockets, &blob_fetch_sender, &exit);

        //TODO
        //the packets coming out of blob_receiver need to be sent to the GPU and verified
        //then sent to the window, which does the erasure coding reconstruction
        let retransmit_phase = RetransmitPhase::new(
            treasury_forks.clone(),
            leader_schedule_cache,
            block_buffer_pool.clone(),
            &node_group_info,
            Arc::new(retransmit_socket),
            repair_socket,
            blob_fetch_receiver,
            &exit,
            genesis_transaction_seal,
            completed_slots_receiver,
            *treasury_forks.read().unwrap().working_treasury().epoch_schedule(),
        );

        let (replay_phase, slot_full_receiver, root_slot_receiver) = ReplayPhase::new(
            &keypair.pubkey(),
            vote_account,
            voting_keypair,
            block_buffer_pool.clone(),
            &treasury_forks,
            node_group_info.clone(),
            &exit,
            ledger_signal_receiver,
            subscriptions,
            waterclock_recorder,
            leader_schedule_cache,
        );

        let blockstream_service = if blockstream.is_some() {
            let blockstream_service = BlockstreamService::new(
                slot_full_receiver,
                block_buffer_pool.clone(),
                blockstream.unwrap().to_string(),
                &exit,
            );
            Some(blockstream_service)
        } else {
            None
        };

        let storage_stage = StoragePhase::new(
            storage_state,
            root_slot_receiver,
            Some(block_buffer_pool),
            &keypair,
            storage_keypair,
            &exit,
            &treasury_forks,
            storage_rotate_count,
            &node_group_info,
        );

        Tvu {
            fetch_phase,
            retransmit_phase,
            replay_phase,
            blockstream_service,
            storage_stage,
        }
    }
}

impl Service for Tvu {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.retransmit_phase.join()?;
        self.fetch_phase.join()?;
        self.storage_stage.join()?;
        if self.blockstream_service.is_some() {
            self.blockstream_service.unwrap().join()?;
        }
        self.replay_phase.join()?;
        Ok(())
    }
}

use std::{borrow::Cow, convert, ffi::OsStr, path::Path};

static LICENSE_HEADER: &str = "Copyright (c) The Libra Core Contributors\n\
                               SPDX-License-Identifier: Apache-2.0\n\
                               ";
#[allow(dead_code)]
pub(super) fn has_license_header(file: &Path, contents: &str) -> Result<(), Cow<'static, str>> {
    enum FileType {
        Rust,
        Shell,
        Proto,
    }

    let file_type = match file
        .extension()
        .map(OsStr::to_str)
        .and_then(convert::identity)
    {
        Some("rs") => FileType::Rust,
        Some("sh") => FileType::Shell,
        Some("proto") => FileType::Proto,
        _ => return Ok(()),
    };

    // Determine if the file is missing the license header
    let missing_header = match file_type {
        FileType::Rust | FileType::Proto => {
            let maybe_license = contents
                .lines()
                .skip_while(|line| line.is_empty())
                .take(2)
                .map(|s| s.trim_start_matches("// "));
            !LICENSE_HEADER.lines().eq(maybe_license)
        }
        FileType::Shell => {
            let maybe_license = contents
                .lines()
                .skip_while(|line| line.starts_with("#!"))
                .skip_while(|line| line.is_empty())
                .take(2)
                .map(|s| s.trim_start_matches("# "));
            !LICENSE_HEADER.lines().eq(maybe_license)
        }
    };

    if missing_header {
        return Err("missing a license header".into());
    }

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::treasury_phase::create_test_recorder;
    use crate::block_buffer_pool::get_tmp_ledger_path;
    use crate::node_group_info::{NodeGroupInfo, Node};
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use crate::storage_stage::STORAGE_ROTATE_TEST_COUNT;
    use morgan_runtime::treasury::Treasury;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_tvu_exit() {
        morgan_logger::setup();
        let leader = Node::new_localhost();
        let target1_keypair = Keypair::new();
        let target1 = Node::new_localhost_with_pubkey(&target1_keypair.pubkey());

        let starting_balance = 10_000;
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(starting_balance);

        let treasury_forks = TreasuryForks::new(0, Treasury::new(&genesis_block));

        //start cluster_info1
        let mut cluster_info1 = NodeGroupInfo::new_with_invalid_keypair(target1.info.clone());
        cluster_info1.insert_info(leader.info.clone());
        let cref1 = Arc::new(RwLock::new(cluster_info1));

        let block_buffer_pool_path = get_tmp_ledger_path!();
        let (block_buffer_pool, l_receiver, completed_slots_receiver) =
            BlockBufferPool::open_by_message(&block_buffer_pool_path)
                .expect("Expected to successfully open ledger");
        let block_buffer_pool = Arc::new(block_buffer_pool);
        let treasury = treasury_forks.working_treasury();
        let (exit, waterclock_recorder, waterclock_service, _entry_receiver) =
            create_test_recorder(&treasury, &block_buffer_pool);
        let voting_keypair = Keypair::new();
        let storage_keypair = Arc::new(Keypair::new());
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_treasury(&treasury));
        let tvu = Tvu::new(
            &voting_keypair.pubkey(),
            Some(&Arc::new(voting_keypair)),
            &storage_keypair,
            &Arc::new(RwLock::new(treasury_forks)),
            &cref1,
            {
                Sockets {
                    repair: target1.sockets.repair,
                    retransmit: target1.sockets.retransmit,
                    fetch: target1.sockets.tvu,
                }
            },
            block_buffer_pool,
            STORAGE_ROTATE_TEST_COUNT,
            &StorageState::default(),
            None,
            l_receiver,
            &Arc::new(RpcSubscriptions::default()),
            &waterclock_recorder,
            &leader_schedule_cache,
            &exit,
            &Hash::default(),
            completed_slots_receiver,
        );
        exit.store(true, Ordering::Relaxed);
        tvu.join().unwrap();
        waterclock_service.join().unwrap();
    }
}
