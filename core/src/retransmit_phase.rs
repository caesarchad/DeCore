//! The `retransmit_phase` retransmits blobs between validators

// use crate::treasury_forks::TreasuryForks;
use crate::treasury_forks::TreasuryForks;
use crate::block_buffer_pool::{BlockBufferPool, CompletedSlotsReceiver};
use crate::node_group_info::{compute_retransmit_peers, NodeGroupInfo, };
use crate::bvm_types::DATA_PLANE_FANOUT;
use crate::leader_arrange_cache::LdrSchBufferPoolList;
use crate::fix_missing_spot_service::FixPlan;
use crate::result::{Error, Result};
use crate::service::Service;
use crate::staking_utils;
use crate::bvm_types::BlobAcptr;
use crate::spot_transmit_service::{check_replay_blob, SpotTransmitService};
use morgan_metricbot::{datapoint_info, inc_new_counter_error};
use morgan_runtime::epoch_schedule::RoundPlan;
use morgan_interface::hash::Hash;
use std::net::UdpSocket;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::channel;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, RwLock};
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;

fn retransmit(
    treasury_forks: &Arc<RwLock<TreasuryForks>>,
    leader_schedule_cache: &Arc<LdrSchBufferPoolList>,
    node_group_info: &Arc<RwLock<NodeGroupInfo>>,
    r: &BlobAcptr,
    sock: &UdpSocket,
) -> Result<()> {
    let timer = Duration::new(1, 0);
    let mut blobs = r.recv_timeout(timer)?;
    while let Ok(mut nq) = r.try_recv() {
        blobs.append(&mut nq);
    }

    datapoint_info!("retransmit-phase", ("count", blobs.len(), i64));

    let r_treasury = treasury_forks.read().unwrap().working_treasury();
    let treasury_round = r_treasury.get_stakers_epoch(r_treasury.slot());
    let (neighbors, children) = compute_retransmit_peers(
        staking_utils::staked_nodes_at_epoch(&r_treasury, treasury_round).as_ref(),
        node_group_info,
        DATA_PLANE_FANOUT,
    );
    for blob in &blobs {
        let leader = leader_schedule_cache
            .slot_leader_at(blob.read().unwrap().slot(), Some(r_treasury.as_ref()));
        if blob.read().unwrap().meta.forward {
            NodeGroupInfo::retransmit_to(&node_group_info, &neighbors, blob, leader, sock, true)?;
            NodeGroupInfo::retransmit_to(&node_group_info, &children, blob, leader, sock, false)?;
        } else {
            NodeGroupInfo::retransmit_to(&node_group_info, &children, blob, leader, sock, true)?;
        }
    }
    Ok(())
}

/// Service to retransmit messages from the leader or layer 1 to relevant peer nodes.
/// See `node_group_info` for network layer definitions.
/// # Arguments
/// * `sock` - Socket to read from.  Read timeout is set to 1.
/// * `exit` - Boolean to signal system exit.
/// * `node_group_info` - This structure needs to be updated and populated by the treasury and via gossip.
/// * `recycler` - Blob recycler.
/// * `r` - Receive channel for blobs to be retransmitted to all the layer 1 nodes.
fn retransmitter(
    sock: Arc<UdpSocket>,
    treasury_forks: Arc<RwLock<TreasuryForks>>,
    leader_schedule_cache: &Arc<LdrSchBufferPoolList>,
    node_group_info: Arc<RwLock<NodeGroupInfo>>,
    r: BlobAcptr,
) -> JoinHandle<()> {
    let treasury_forks = treasury_forks.clone();
    let leader_schedule_cache = leader_schedule_cache.clone();
    Builder::new()
        .name("morgan-retransmitter".to_string())
        .spawn(move || {
            trace!("retransmitter started");
            loop {
                if let Err(e) = retransmit(
                    &treasury_forks,
                    &leader_schedule_cache,
                    &node_group_info,
                    &r,
                    &sock,
                ) {
                    match e {
                        Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => break,
                        Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                        _ => {
                            inc_new_counter_error!("data_filter-retransmit-error", 1, 1);
                        }
                    }
                }
            }
            trace!("exiting retransmitter");
        })
        .unwrap()
}

use slog::{Key, Level};

/// The KV value is being processed based on the category it is bucketed in
#[derive(Debug, PartialEq, Eq)]
pub enum KVCategory {
    /// KV value is not printed at all
    Ignore,
    /// KV value is inlined with the main message passed to slog macro
    Inline,
    /// KV value is printed as a separate line with the provided log level
    LevelLog(Level),
}

/// Structures implementing this trait are being used to categorize the KV values into one of the
/// `KVCategory`.
pub trait KVCategorizer {
    /// For a given key from KV decide which category it belongs to
    fn categorize(&self, key: Key) -> KVCategory;
    /// For a given key from KV return a name that should be printed for it
    fn name(&self, key: Key) -> &'static str;
    /// True if category of a given key is KVCategory::Ignore
    fn ignore(&self, key: Key) -> bool {
        self.categorize(key) == KVCategory::Ignore
    }
}

/// Placeholder categorizer that inlines all KV values with names equal to key
pub struct InlineCategorizer;
impl KVCategorizer for InlineCategorizer {
    fn categorize(&self, _key: Key) -> KVCategory {
        KVCategory::Inline
    }

    fn name(&self, key: Key) -> &'static str {
        key
    }
}

/// Used to properly print `error_chain` `Error`s. It displays the error and it's causes in
/// separate log lines as well as backtrace if provided.
/// The `error_chain` `Error` must implement `KV` trait. It is recommended to use `impl_kv_error`
/// macro to generate the implementation.
pub struct ErrorCategorizer;
impl KVCategorizer for ErrorCategorizer {
    fn categorize(&self, key: Key) -> KVCategory {
        match key {
            "error" => KVCategory::LevelLog(Level::Error),
            "cause" => KVCategory::LevelLog(Level::Debug),
            "backtrace" => KVCategory::LevelLog(Level::Trace),
            _ => InlineCategorizer.categorize(key),
        }
    }

    fn name(&self, key: Key) -> &'static str {
        match key {
            "error" => "Error",
            "cause" => "Caused by",
            "backtrace" => "Originated in",
            "root_cause" => "Root cause",
            _ => InlineCategorizer.name(key),
        }
    }
}

pub struct RetransmitPhase {
    thread_hdls: Vec<JoinHandle<()>>,
    spot_service: SpotTransmitService,
}

impl RetransmitPhase {
    #[allow(clippy::new_ret_no_self)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        treasury_forks: Arc<RwLock<TreasuryForks>>,
        leader_schedule_cache: &Arc<LdrSchBufferPoolList>,
        block_buffer_pool: Arc<BlockBufferPool>,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        retransmit_socket: Arc<UdpSocket>,
        fix_socket: Arc<UdpSocket>,
        fetch_phase_receiver: BlobAcptr,
        exit: &Arc<AtomicBool>,
        genesis_transaction_seal: &Hash,
        completed_slots_receiver: CompletedSlotsReceiver,
        epoch_schedule: RoundPlan,
    ) -> Self {
        let (retransmit_sender, retransmit_receiver) = channel();

        let t_retransmit = retransmitter(
            retransmit_socket,
            treasury_forks.clone(),
            leader_schedule_cache,
            node_group_info.clone(),
            retransmit_receiver,
        );

        let fix_plan = FixPlan::FixAll {
            treasury_forks,
            completed_slots_receiver,
            epoch_schedule,
        };
        let leader_schedule_cache = leader_schedule_cache.clone();
        let spot_service = SpotTransmitService::new(
            block_buffer_pool,
            node_group_info.clone(),
            fetch_phase_receiver,
            retransmit_sender,
            fix_socket,
            exit,
            fix_plan,
            genesis_transaction_seal,
            move |id, blob, working_treasury| {
                check_replay_blob(blob, working_treasury, &leader_schedule_cache, id)
            },
        );

        let thread_hdls = vec![t_retransmit];
        Self {
            thread_hdls,
            spot_service,
        }
    }
}

impl Service for RetransmitPhase {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        self.spot_service.join()?;
        Ok(())
    }
}
