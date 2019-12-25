//! The `transaction_digesting_module` module implements the Transaction Processing Unit, a
//! multi-phase transaction processing pipeline in software.

use crate::treasury_phase::TreasuryPhase;
use crate::block_buffer_pool::BlockBufferPool;
use crate::propagate_phase::PyramidPhase;
use crate::node_group_info::NodeGroupInfo;
use crate::node_group_info_voter_listener::ClusterInfoVoteListener;
use crate::fetch_phase::FetchPhase;
use crate::water_clock_recorder::{WaterClockRecorder, WorkingTreasuryEntries};
use crate::service::Service;
use crate::signature_verify_phase ::SigVerifyPhase;
use morgan_interface::hash::Hash;
use morgan_interface::pubkey::Pubkey;
use std::net::UdpSocket;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

pub struct TransactionDigestingModule {
    fetch_phase: FetchPhase,
    sigverify_phase: SigVerifyPhase,
    treasury_phase: TreasuryPhase,
    cluster_info_vote_listener: ClusterInfoVoteListener,
    broadcast_phase: PyramidPhase,
}

impl TransactionDigestingModule {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: &Pubkey,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
        entry_receiver: Receiver<WorkingTreasuryEntries>,
        transactions_sockets: Vec<UdpSocket>,
        transaction_digesting_module_via_blobs_sockets: Vec<UdpSocket>,
        broadcast_socket: UdpSocket,
        sigverify_disabled: bool,
        block_buffer_pool: &Arc<BlockBufferPool>,
        exit: &Arc<AtomicBool>,
        genesis_transaction_seal: &Hash,
    ) -> Self {
        node_group_info.write().unwrap().set_leader(id);

        let (packet_sender, packet_receiver) = channel();
        let fetch_phase = FetchPhase::new_with_sender(
            transactions_sockets,
            transaction_digesting_module_via_blobs_sockets,
            &exit,
            &packet_sender,
            &waterclock_recorder,
        );
        let (verified_sender, verified_receiver) = channel();

        let sigverify_phase =
            SigVerifyPhase::new(packet_receiver, sigverify_disabled, verified_sender.clone());

        let (verified_vote_sender, verified_vote_receiver) = channel();
        let cluster_info_vote_listener = ClusterInfoVoteListener::new(
            &exit,
            node_group_info.clone(),
            sigverify_disabled,
            verified_vote_sender,
            &waterclock_recorder,
        );

        let treasury_phase = TreasuryPhase::new(
            &node_group_info,
            waterclock_recorder,
            verified_receiver,
            verified_vote_receiver,
        );

        let broadcast_phase = PyramidPhase::new(
            broadcast_socket,
            node_group_info.clone(),
            entry_receiver,
            &exit,
            block_buffer_pool,
            genesis_transaction_seal,
        );

        Self {
            fetch_phase,
            sigverify_phase,
            treasury_phase,
            cluster_info_vote_listener,
            broadcast_phase,
        }
    }
}

impl Service for TransactionDigestingModule {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        let mut results = vec![];
        results.push(self.fetch_phase.join());
        results.push(self.sigverify_phase.join());
        results.push(self.cluster_info_vote_listener.join());
        results.push(self.treasury_phase.join());
        let broadcast_result = self.broadcast_phase.join();
        for result in results {
            result?;
        }
        let _ = broadcast_result?;
        Ok(())
    }
}

pub fn camel_to_snake(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut first = true;
    text.chars().for_each(|c| {
        if !first && c.is_uppercase() {
            out.push('_');
            out.extend(c.to_lowercase());
        } else if first {
            first = false;
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    });
    out
}
