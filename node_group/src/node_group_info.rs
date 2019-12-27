//! The `node_group_info` module defines a data structure that is shared by all the nodes in the network over
//! a gossip control plane.  The goal is to share small bits of off-chain information and detect and
//! repair partitions.
//!
//! This CRDT only supports a very limited set of types.  A map of BvmAddr -> Versioned Struct.
//! The last version is always picked during an update.
//!
//! The network is arranged in layers:
//!
//! * layer 0 - Leader.
//! * layer 1 - As many nodes as we can fit
//! * layer 2 - Everyone else, if layer 1 is `2^10`, layer 2 should be able to fit `2^20` number of nodes.
//!
//! Treasury needs to provide an interface for us to query the stake weight
// use crate::treasury_forks::TreasuryForks;
use crate::treasury_forks::TreasuryForks;
use crate::block_buffer_pool::BlockBufferPool;
use crate::connection_info::ContactInfo;
use crate::gossip::NodeTbleGossip;
use crate::ndtb_err::NodeTbleErr;
use crate::pull_from_gossip::NDTB_GOSSIP_PULL_CRDS_TIMEOUT_MS;
use crate::propagation_value::{ContInfTblValue, ContInfTblValueTag, EpochSlots, Vote};
use crate::packet::{to_shared_blob, Blob, ArcBlb, BLOB_SIZE};
use crate::fix_missing_spot_service::FixPlanType;
use crate::result::Result;
use crate::staking_utils;
use crate::data_filter::{BlobAcptr, BlobSndr};
use bincode::{deserialize, serialize};
use core::cmp;
use hashbrown::HashMap;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use bitconch_metricbot::{datapoint_debug, inc_new_counter_debug, inc_new_counter_error};
use bitconch_netutil::{
    bind_in_range, bind_to, find_available_port_in_range, multi_bind_in_range, PortRange,
};
use bitconch_runtime::bloom::Bloom;
use bitconch_interface::hash::Hash;
use bitconch_interface::address::BvmAddr;
use bitconch_interface::signature::{Keypair, KeypairUtil, Signable, Signature};
use bitconch_interface::timing::{duration_as_ms, timestamp};
use bitconch_interface::transaction::Transaction;
use std::cmp::min;
use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{sleep, Builder, JoinHandle};
use std::time::{Duration, Instant};
use bitconch_helper::logHelper::*;

pub const FULLNODE_PORT_RANGE: PortRange = (10_000, 12_000);

/// The Data plane fanout size, also used as the neighborhood size
pub const DATA_PLANE_FANOUT: usize = 200;
/// milliseconds we sleep for between gossip requests
pub const GOSSIP_SLEEP_MILLIS: u64 = 100;

/// the number of slots to respond with when responding to `Orphan` requests
pub const MAX_ORPHAN_REPAIR_RESPONSES: usize = 10;

#[derive(Debug, PartialEq, Eq)]
pub enum NodeGroupInfoError {
    NoPeers,
    NoLeader,
    BadContactInfo,
    BadGossipAddress,
}
#[derive(Clone)]
pub struct NodeGroupInfo {
    /// The network
    pub gossip: NodeTbleGossip,
    /// set the keypair that will be used to sign contact_info_table values generated. It is unset only in tests.
    pub(crate) keypair: Arc<Keypair>,
    // TODO: remove gossip_leader_address once all usage of `set_leader()` and `leader_data()` is
    // purged
    gossip_leader_address: BvmAddr,
    /// The network entrypoint
    entrypoint: Option<ContactInfo>,
}

#[derive(Default, Clone)]
pub struct LocalGroupInfo {
    /// The bounds of the neighborhood represented by this locality
    pub neighbor_bounds: (usize, usize),
    /// The `avalanche` layer this locality is in
    pub layer_ix: usize,
    /// The bounds of the current layer
    pub layer_bounds: (usize, usize),
    /// The bounds of the next layer
    pub next_layer_bounds: Option<(usize, usize)>,
    /// The indices of the nodes that should be contacted in next layer
    pub next_layer_peers: Vec<usize>,
}

impl fmt::Debug for LocalGroupInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LocalGroupInfo {{ neighborhood_bounds: {:?}, current_layer: {:?}, child_layer_bounds: {:?} child_layer_peers: {:?} }}",
            self.neighbor_bounds, self.layer_ix, self.next_layer_bounds, self.next_layer_peers
        )
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SlashMessage {
    /// BvmAddr of the node that sent this prune data
    //pub address: BvmAddr,
    pub sender_address: BvmAddr,
    /// Pubkeys of nodes that should be pruned
    //pub prunes: Vec<BvmAddr>,
    pub target_addresss: Vec<BvmAddr>,
    /// Signature of this Prune Context
    //pub signature: Signature,
    pub msg_signature: Signature,
    /// The BvmAddr of the intended node/destination for this message
    //pub destination: BvmAddr,
    pub receiver_address: BvmAddr,
    /// Wallclock of the node that generated this message
    pub wallclock: u64,
}

impl Signable for SlashMessage {
    fn address(&self) -> BvmAddr {
        //self.address
        self.sender_address
    }

    fn signable_data(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct SignData {
            /*
            address: BvmAddr,
            prunes: Vec<BvmAddr>,
            destination: BvmAddr,
            wallclock: u64,
            */
            sender_address: BvmAddr,
            target_addresss: Vec<BvmAddr>,
            receiver_address: BvmAddr,
            wallclock: u64,
        }
        let data = SignData {
            /*
            address: self.address,
            prunes: self.prunes.clone(),
            destination: self.destination,
            wallclock: self.wallclock,
            */
            sender_address: self.sender_address,
            target_addresss: self.target_addresss.clone(),
            receiver_address: self.receiver_address,
            wallclock: self.wallclock,
        };
        serialize(&data).expect("serialize SlashMessage")
    }

    fn get_signature(&self) -> Signature {
        //self.signature
        self.msg_signature
    }

    fn set_signature(&mut self, msg_signature: Signature) {
        self.msg_signature = msg_signature
    }
}

// TODO These messages should go through the gpu pipeline for spam filtering
#[derive(Serialize, Deserialize, Debug)]
#[allow(clippy::large_enum_variant)]
enum Protocol {
    /// Gossip protocol messages
    PullRequest(Bloom<Hash>, ContInfTblValue),
    PullResponse(BvmAddr, Vec<ContInfTblValue>),
    PushMessage(BvmAddr, Vec<ContInfTblValue>),
    PruneMessage(BvmAddr, SlashMessage),

    /// Window protocol messages
    /// TODO: move this message to a different module
    RequestWindowIndex(ContactInfo, u64, u64),
    RequestHighestWindowIndex(ContactInfo, u64, u64),
    RequestOrphan(ContactInfo, u64),
}

impl NodeGroupInfo {
    /// Without a valid keypair gossip will not function. Only useful for tests.
    pub fn new_with_invalid_keypair(contact_info: ContactInfo) -> Self {
        Self::new(contact_info, Arc::new(Keypair::new()))
    }

    pub fn new(contact_info: ContactInfo, keypair: Arc<Keypair>) -> Self {
        let mut me = Self {
            gossip: NodeTbleGossip::default(),
            keypair,
            gossip_leader_address: BvmAddr::default(),
            entrypoint: None,
        };
        let id = contact_info.id;
        me.gossip.set_self(&id);
        me.insert_self(contact_info);
        me.push_self(&HashMap::new());
        me
    }

    pub fn insert_self(&mut self, contact_info: ContactInfo) {
        if self.id() == contact_info.id {
            let mut value = ContInfTblValue::ContactInfo(contact_info.clone());
            value.sign(&self.keypair);
            let _ = self.gossip.contact_info_table.insert(value, timestamp());
        }
    }

    fn push_self(&mut self, stakes: &HashMap<BvmAddr, u64>) {
        let mut my_data = self.my_data();
        let now = timestamp();
        my_data.wallclock = now;
        let mut entry = ContInfTblValue::ContactInfo(my_data);
        entry.sign(&self.keypair);
        self.gossip.refresh_push_active_set(stakes);
        self.gossip.process_push_message(vec![entry], now);
    }

    // TODO kill insert_info, only used by tests
    pub fn insert_info(&mut self, contact_info: ContactInfo) {
        let mut value = ContInfTblValue::ContactInfo(contact_info);
        value.sign(&self.keypair);
        let _ = self.gossip.contact_info_table.insert(value, timestamp());
    }

    pub fn set_entrypoint(&mut self, entrypoint: ContactInfo) {
        self.entrypoint = Some(entrypoint)
    }

    pub fn id(&self) -> BvmAddr {
        self.gossip.id
    }

    pub fn lookup(&self, id: &BvmAddr) -> Option<&ContactInfo> {
        let entry = ContInfTblValueTag::ContactInfo(*id);
        self.gossip
            .contact_info_table
            .lookup(&entry)
            .and_then(ContInfTblValue::contact_info)
    }

    pub fn my_data(&self) -> ContactInfo {
        self.lookup(&self.id()).cloned().unwrap()
    }

    // Deprecated: don't use leader_data().
    pub fn leader_data(&self) -> Option<&ContactInfo> {
        let leader_addr = self.gossip_leader_address;
        if leader_addr == BvmAddr::default() {
            return None;
        }
        self.lookup(&leader_addr)
    }

    pub fn contact_info_trace(&self) -> String {
        let now = timestamp();
        let mut spy_nodes = 0;
        let mut miners = 0;
        let my_address = self.my_data().id;
        let nodes: Vec<_> = self
            .all_peers()
            .into_iter()
            .map(|(node, last_updated)| {
                if Self::is_spy_node(&node) {
                    spy_nodes += 1;
                } else if Self::is_storage_miner(&node) {
                    miners += 1;
                }
                fn addr_to_string(addr: &SocketAddr) -> String {
                    if ContactInfo::is_valid_address(addr) {
                        addr.to_string()
                    } else {
                        "none".to_string()
                    }
                }

                format!(
                    "- gossip: {:20} | {:5}ms | {} {}\n  \
                     transaction_digesting_module:    {:20} |         |\n  \
                     rpc:    {:20} |         |\n",
                    addr_to_string(&node.gossip),
                    now.saturating_sub(last_updated),
                    node.id,
                    if node.id == my_address { "(me)" } else { "" }.to_string(),
                    addr_to_string(&node.transaction_digesting_module),
                    addr_to_string(&node.rpc),
                )
            })
            .collect();

        format!(
            " Node contact info             | Age     | Node identifier                   \n\
             -------------------------------+---------+-----------------------------------\n\
             {}\
             Nodes: {}{}{}",
            nodes.join(""),
            nodes.len() - spy_nodes - miners,
            if miners > 0 {
                format!("\nStorage Miners: {}", miners)
            } else {
                "".to_string()
            },
            if spy_nodes > 0 {
                format!("\nSpies: {}", spy_nodes)
            } else {
                "".to_string()
            }
        )
    }

    /// Record the id of the current leader for use by `leader_transaction_digesting_module_via_blobs()`
    pub fn set_leader(&mut self, leader_addr: &BvmAddr) {
        if *leader_addr != self.gossip_leader_address {
            // warn!(
            //     "{}",
            //     Warn(format!("{}: LEADER_UPDATE TO {} from {}",
            //     self.gossip.id, leader_addr, self.gossip_leader_address).to_string())
            // );
            println!(
                "{}",
                Warn(
                    format!("{}: LEADER_UPDATE TO {} from {}",
                        self.gossip.id, leader_addr, self.gossip_leader_address).to_string(),
                    module_path!().to_string()
                )
            );
            self.gossip_leader_address = *leader_addr;
        }
    }

    pub fn push_epoch_slots(&mut self, id: BvmAddr, root: u64, slots: BTreeSet<u64>) {
        let now = timestamp();
        let mut entry = ContInfTblValue::EpochSlots(EpochSlots::new(id, root, slots, now));
        entry.sign(&self.keypair);
        self.gossip.process_push_message(vec![entry], now);
    }

    pub fn push_vote(&mut self, vote: Transaction) {
        let now = timestamp();
        let vote = Vote::new(&self.id(), vote, now);
        let mut entry = ContInfTblValue::Vote(vote);
        entry.sign(&self.keypair);
        self.gossip.process_push_message(vec![entry], now);
    }

    /// Get votes in the contact_info_table
    /// * since - The timestamp of when the vote inserted must be greater than
    /// since. This allows the treasury to query for new votes only.
    ///
    /// * return - The votes, and the max timestamp from the new set.
    pub fn get_votes(&self, since: u64) -> (Vec<Transaction>, u64) {
        let votes: Vec<_> = self
            .gossip
            .contact_info_table
            .table
            .values()
            .filter(|x| x.insert_timestamp > since)
            .filter_map(|x| {
                x.value
                    .vote()
                    .map(|v| (x.insert_timestamp, v.transaction.clone()))
            })
            .collect();
        let max_ts = votes.iter().map(|x| x.0).max().unwrap_or(since);
        let txs: Vec<Transaction> = votes.into_iter().map(|x| x.1).collect();
        (txs, max_ts)
    }

    pub fn get_epoch_state_for_node(
        &self,
        address: &BvmAddr,
        since: Option<u64>,
    ) -> Option<(&EpochSlots, u64)> {
        self.gossip
            .contact_info_table
            .table
            .get(&ContInfTblValueTag::EpochSlots(*address))
            .filter(|x| {
                since
                    .map(|since| x.insert_timestamp > since)
                    .unwrap_or(true)
            })
            .map(|x| (x.value.epoch_slots().unwrap(), x.insert_timestamp))
    }

    pub fn get_gossiped_root_for_node(&self, address: &BvmAddr, since: Option<u64>) -> Option<u64> {
        self.gossip
            .contact_info_table
            .table
            .get(&ContInfTblValueTag::EpochSlots(*address))
            .filter(|x| {
                since
                    .map(|since| x.insert_timestamp > since)
                    .unwrap_or(true)
            })
            .map(|x| x.value.epoch_slots().unwrap().root)
    }

    pub fn get_contact_info_for_node(&self, address: &BvmAddr) -> Option<&ContactInfo> {
        self.gossip
            .contact_info_table
            .table
            .get(&ContInfTblValueTag::ContactInfo(*address))
            .map(|x| x.value.contact_info().unwrap())
    }

    pub fn purge(&mut self, now: u64) {
        self.gossip.purge(now);
    }

    pub fn rpc_peers(&self) -> Vec<ContactInfo> {
        let me = self.my_data().id;
        self.gossip
            .contact_info_table
            .table
            .values()
            .filter_map(|x| x.value.contact_info())
            .filter(|x| x.id != me)
            .filter(|x| ContactInfo::is_valid_address(&x.rpc))
            .cloned()
            .collect()
    }

    // All nodes in gossip (including spy nodes) and the last time we heard about them
    pub(crate) fn all_peers(&self) -> Vec<(ContactInfo, u64)> {
        self.gossip
            .contact_info_table
            .table
            .values()
            .filter_map(|x| {
                x.value
                    .contact_info()
                    .map(|ci| (ci.clone(), x.local_timestamp))
            })
            .collect()
    }

    pub fn gossip_peers(&self) -> Vec<ContactInfo> {
        let me = self.my_data().id;
        self.gossip
            .contact_info_table
            .table
            .values()
            .filter_map(|x| x.value.contact_info())
            .filter(|x| x.id != me)
            .filter(|x| ContactInfo::is_valid_address(&x.gossip))
            .cloned()
            .collect()
    }

    /// all peers that have a valid blaze_unit port.
    pub fn fetch_blaze_node_list(&self) -> Vec<ContactInfo> {
        let me = self.my_data().id;
        self.gossip
            .contact_info_table
            .table
            .values()
            .filter_map(|x| x.value.contact_info())
            .filter(|x| ContactInfo::is_valid_address(&x.blaze_unit))
            .filter(|x| x.id != me)
            .cloned()
            .collect()
    }

    /// all peers that have a valid storage addr
    pub fn storage_peers(&self) -> Vec<ContactInfo> {
        let me = self.my_data().id;
        self.gossip
            .contact_info_table
            .table
            .values()
            .filter_map(|x| x.value.contact_info())
            .filter(|x| ContactInfo::is_valid_address(&x.storage_addr))
            .filter(|x| x.id != me)
            .cloned()
            .collect()
    }

    /// all peers that have a valid blaze_unit
    pub fn retransmit_peers(&self) -> Vec<ContactInfo> {
        let me = self.my_data().id;
        self.gossip
            .contact_info_table
            .table
            .values()
            .filter_map(|x| x.value.contact_info())
            .filter(|x| x.id != me)
            .filter(|x| ContactInfo::is_valid_address(&x.blaze_unit))
            .cloned()
            .collect()
    }

    /// all blaze_unit peers with valid gossip addrs
    fn repair_peers(&self) -> Vec<ContactInfo> {
        let me = self.my_data().id;
        NodeGroupInfo::fetch_blaze_node_list(self)
            .into_iter()
            .filter(|x| x.id != me)
            .filter(|x| ContactInfo::is_valid_address(&x.gossip))
            .collect()
    }

    fn is_spy_node(contact_info: &ContactInfo) -> bool {
        (!ContactInfo::is_valid_address(&contact_info.transaction_digesting_module)
            || !ContactInfo::is_valid_address(&contact_info.gossip)
            || !ContactInfo::is_valid_address(&contact_info.blaze_unit))
            && !ContactInfo::is_valid_address(&contact_info.storage_addr)
    }

    pub fn is_storage_miner(contact_info: &ContactInfo) -> bool {
        ContactInfo::is_valid_address(&contact_info.storage_addr)
            && !ContactInfo::is_valid_address(&contact_info.transaction_digesting_module)
    }

    fn sort_by_stake<S: std::hash::BuildHasher>(
        peers: &[ContactInfo],
        stakes: Option<&HashMap<BvmAddr, u64, S>>,
    ) -> Vec<(u64, ContactInfo)> {
        let mut peers_with_stakes: Vec<_> = peers
            .iter()
            .map(|c| {
                (
                    stakes.map_or(0, |stakes| *stakes.get(&c.id).unwrap_or(&0)),
                    c.clone(),
                )
            })
            .collect();
        peers_with_stakes.sort_unstable_by(|(l_stake, l_info), (r_stake, r_info)| {
            if r_stake == l_stake {
                r_info.id.cmp(&l_info.id)
            } else {
                r_stake.cmp(&l_stake)
            }
        });
        peers_with_stakes.dedup();
        peers_with_stakes
    }

    /// Return sorted Retransmit peers and index of `Self.id()` as if it were in that list
    fn sorted_peers_and_index<S: std::hash::BuildHasher>(
        &self,
        stakes: Option<&HashMap<BvmAddr, u64, S>>,
    ) -> (usize, Vec<ContactInfo>) {
        let mut peers = self.retransmit_peers();
        peers.push(self.lookup(&self.id()).unwrap().clone());
        let contacts_and_stakes: Vec<_> = NodeGroupInfo::sort_by_stake(&peers, stakes);
        let mut index = 0;
        let peers: Vec<_> = contacts_and_stakes
            .into_iter()
            .enumerate()
            .filter_map(|(i, (_, peer))| {
                if peer.id == self.id() {
                    index = i;
                    None
                } else {
                    Some(peer)
                }
            })
            .collect();
        (index, peers)
    }

    pub fn ordered_blaze_node_lists(&self, stakes: Option<&HashMap<BvmAddr, u64>>) -> Vec<ContactInfo> {
        let peers = self.fetch_blaze_node_list();
        let peers_with_stakes: Vec<_> = NodeGroupInfo::sort_by_stake(&peers, stakes);
        peers_with_stakes
            .iter()
            .map(|(_, peer)| (*peer).clone())
            .collect()
    }

    /// compute broadcast table
    pub fn transaction_digesting_module_peers(&self) -> Vec<ContactInfo> {
        let me = self.my_data().id;
        self.gossip
            .contact_info_table
            .table
            .values()
            .filter_map(|x| x.value.contact_info())
            .filter(|x| x.id != me)
            .filter(|x| ContactInfo::is_valid_address(&x.transaction_digesting_module))
            .cloned()
            .collect()
    }

    /// Given a node count and fanout, it calculates how many layers are needed and at what index each layer begins.
    pub fn describe_data_plane(nodes: usize, fanout: usize) -> (usize, Vec<usize>) {
        let mut layer_indices: Vec<usize> = vec![0];
        if nodes == 0 {
            (0, vec![])
        } else if nodes <= fanout {
            // single layer data plane
            (1, layer_indices)
        } else {
            //layer 1 is going to be the first num fanout nodes, so exclude those
            let mut remaining_nodes = nodes - fanout;
            layer_indices.push(fanout);
            let mut num_layers = 2;
            // fanout * num_nodes in a neighborhood, which is also fanout.
            let mut layer_capacity = fanout * fanout;
            while remaining_nodes > 0 {
                if remaining_nodes > layer_capacity {
                    // Needs more layers.
                    num_layers += 1;
                    remaining_nodes -= layer_capacity;
                    let end = *layer_indices.last().unwrap();
                    layer_indices.push(layer_capacity + end);

                    // Next layer's capacity
                    layer_capacity *= fanout;
                } else {
                    //everything will now fit in the layers we have
                    let end = *layer_indices.last().unwrap();
                    layer_indices.push(layer_capacity + end);
                    break;
                }
            }
            assert_eq!(num_layers, layer_indices.len() - 1);
            (num_layers, layer_indices)
        }
    }

    fn localize_item(
        layer_indices: &[usize],
        fanout: usize,
        select_index: usize,
        curr_index: usize,
    ) -> Option<(LocalGroupInfo)> {
        let end = layer_indices.len() - 1;
        let next = min(end, curr_index + 1);
        let layer_start = layer_indices[curr_index];
        // localized if selected index lies within the current layer's bounds
        let localized = select_index >= layer_start && select_index < layer_indices[next];
        if localized {
            let mut locality = LocalGroupInfo::default();
            let hood_ix = (select_index - layer_start) / fanout;
            match curr_index {
                _ if curr_index == 0 => {
                    locality.layer_ix = 0;
                    locality.layer_bounds = (0, fanout);
                    locality.neighbor_bounds = locality.layer_bounds;

                    if next == end {
                        locality.next_layer_bounds = None;
                        locality.next_layer_peers = vec![];
                    } else {
                        locality.next_layer_bounds =
                            Some((layer_indices[next], layer_indices[next + 1]));
                        locality.next_layer_peers = NodeGroupInfo::next_layer_peers(
                            select_index,
                            hood_ix,
                            layer_indices[next],
                            fanout,
                        );
                    }
                }
                _ if curr_index == end => {
                    locality.layer_ix = end;
                    locality.layer_bounds = (end - fanout, end);
                    locality.neighbor_bounds = locality.layer_bounds;
                    locality.next_layer_bounds = None;
                    locality.next_layer_peers = vec![];
                }
                ix => {
                    locality.layer_ix = ix;
                    locality.layer_bounds = (layer_start, layer_indices[next]);
                    locality.neighbor_bounds = (
                        ((hood_ix * fanout) + layer_start),
                        ((hood_ix + 1) * fanout + layer_start),
                    );

                    if next == end {
                        locality.next_layer_bounds = None;
                        locality.next_layer_peers = vec![];
                    } else {
                        locality.next_layer_bounds =
                            Some((layer_indices[next], layer_indices[next + 1]));
                        locality.next_layer_peers = NodeGroupInfo::next_layer_peers(
                            select_index,
                            hood_ix,
                            layer_indices[next],
                            fanout,
                        );
                    }
                }
            }
            Some(locality)
        } else {
            None
        }
    }

    /// Given a array of layer indices and an index of interest, returns (as a `LocalGroupInfo`) the layer,
    /// layer-bounds, and neighborhood-bounds in which the index resides
    fn localize(layer_indices: &[usize], fanout: usize, select_index: usize) -> LocalGroupInfo {
        (0..layer_indices.len())
            .find_map(|i| NodeGroupInfo::localize_item(layer_indices, fanout, select_index, i))
            .or_else(|| Some(LocalGroupInfo::default()))
            .unwrap()
    }

    /// Selects a range in the next layer and chooses nodes from that range as peers for the given index
    fn next_layer_peers(index: usize, hood_ix: usize, start: usize, fanout: usize) -> Vec<usize> {
        // Each neighborhood is only tasked with pushing to `fanout` neighborhoods where each neighborhood contains `fanout` nodes.
        let fanout_nodes = fanout * fanout;
        // Skip first N nodes, where N is hood_ix * (fanout_nodes)
        let start = start + (hood_ix * fanout_nodes);
        let end = start + fanout_nodes;
        (start..end)
            .step_by(fanout)
            .map(|x| x + index % fanout)
            .collect()
    }

    /// broadcast messages from the leader to layer 1 nodes
    /// # Remarks
    pub fn broadcast(
        id: &BvmAddr,
        contains_last_drop: bool,
        pyramid_node_grp_list: &[ContactInfo],
        s: &UdpSocket,
        blobs: &[ArcBlb],
    ) -> Result<()> {
        if pyramid_node_grp_list.is_empty() {
            debug!("{}:not enough peers in node_group_info table", id);
            inc_new_counter_error!("node_group_info-broadcast-not_enough_peers_error", 1);
            Err(NodeGroupInfoError::NoPeers)?;
        }

        let orders = Self::create_broadcast_orders(contains_last_drop, blobs, pyramid_node_grp_list);

        trace!("broadcast orders table {}", orders.len());

        let errs = Self::send_orders(id, s, orders);

        for e in errs {
            if let Err(e) = &e {
                trace!("{}: broadcast result {:?}", id, e);
            }
            e?;
        }

        inc_new_counter_debug!("node_group_info-broadcast-max_idx", blobs.len());

        Ok(())
    }

    /// retransmit messages to a list of nodes
    /// # Remarks
    /// We need to avoid having obj locked while doing any io, such as the `send_to`
    pub fn retransmit_to(
        obj: &Arc<RwLock<Self>>,
        peers: &[ContactInfo],
        blob: &ArcBlb,
        slot_leader_address: Option<BvmAddr>,
        s: &UdpSocket,
        forwarded: bool,
    ) -> Result<()> {
        let (me, orders): (ContactInfo, &[ContactInfo]) = {
            // copy to avoid locking during IO
            let s = obj.read().unwrap();
            (s.my_data().clone(), peers)
        };
        // hold a write lock so no one modifies the blob until we send it
        let mut wblob = blob.write().unwrap();
        let was_forwarded = !wblob.should_forward();
        wblob.set_forwarded(forwarded);
        trace!("retransmit orders {}", orders.len());
        let errs: Vec<_> = orders
            .par_iter()
            .filter(|v| v.id != slot_leader_address.unwrap_or_default())
            .map(|v| {
                debug!(
                    "{}: retransmit blob {} to {} {}",
                    me.id,
                    wblob.index(),
                    v.id,
                    v.blaze_unit,
                );
                //TODO profile this, may need multiple sockets for par_iter
                assert!(wblob.meta.size <= BLOB_SIZE);
                s.send_to(&wblob.data[..wblob.meta.size], &v.blaze_unit)
            })
            .collect();
        // reset the blob to its old state. This avoids us having to copy the blob to modify it
        wblob.set_forwarded(was_forwarded);
        for e in errs {
            if let Err(e) = &e {
                inc_new_counter_error!("node_group_info-retransmit-send_to_error", 1, 1);
                // error!("{}", Error(format!("retransmit result {:?}", e).to_string()));
                println!(
                    "{}",
                    Error(
                        format!("retransmit result {:?}", e).to_string(),
                        module_path!().to_string()
                    )
                );
            }
            e?;
        }
        Ok(())
    }

    fn send_orders(
        id: &BvmAddr,
        s: &UdpSocket,
        orders: Vec<(ArcBlb, Vec<&ContactInfo>)>,
    ) -> Vec<io::Result<usize>> {
        orders
            .into_iter()
            .flat_map(|(b, vs)| {
                let blob = b.read().unwrap();

                let ids_and_tvus = if log_enabled!(log::Level::Trace) {
                    let v_ids = vs.iter().map(|v| v.id);
                    let tvus = vs.iter().map(|v| v.blaze_unit);
                    let ids_and_tvus = v_ids.zip(tvus).collect();

                    trace!(
                        "{}: BROADCAST idx: {} sz: {} to {:?} coding: {}",
                        id,
                        blob.index(),
                        blob.meta.size,
                        ids_and_tvus,
                        blob.is_coding()
                    );

                    ids_and_tvus
                } else {
                    vec![]
                };

                assert!(blob.meta.size <= BLOB_SIZE);
                let send_errs_for_blob: Vec<_> = vs
                    .iter()
                    .map(move |v| {
                        let e = s.send_to(&blob.data[..blob.meta.size], &v.blaze_unit);
                        trace!(
                            "{}: done broadcast {} to {:?}",
                            id,
                            blob.meta.size,
                            ids_and_tvus
                        );
                        e
                    })
                    .collect();
                send_errs_for_blob
            })
            .collect()
    }

    pub fn create_broadcast_orders<'a, T>(
        contains_last_drop: bool,
        blobs: &[T],
        pyramid_node_grp_list: &'a [ContactInfo],
    ) -> Vec<(T, Vec<&'a ContactInfo>)>
    where
        T: Clone,
    {
        // enumerate all the blobs in the window, those are the indices
        // transmit them to nodes, starting from a different node.
        if blobs.is_empty() {
            return vec![];
        }
        let mut orders = Vec::with_capacity(blobs.len());

        let x = thread_rng().gen_range(0, pyramid_node_grp_list.len());
        for (i, blob) in blobs.iter().enumerate() {
            let br_idx = (x + i) % pyramid_node_grp_list.len();

            trace!("broadcast order data br_idx {}", br_idx);

            orders.push((blob.clone(), vec![&pyramid_node_grp_list[br_idx]]));
        }

        if contains_last_drop {
            // PyramidNode the last _drop to everyone on the network so it doesn't get dropped
            // (Need to maximize probability the next leader in line sees this handoff _drop
            // despite packet drops)
            // If we had a _drop at max_drop_height, then we know it must be the last
            // Blob in the broadcast, There cannot be an entry that got sent after the
            // last _drop, guaranteed by the WaterClockService).
            orders.push((
                blobs.last().unwrap().clone(),
                pyramid_node_grp_list.iter().collect(),
            ));
        }

        orders
    }

    pub fn window_index_request_bytes(&self, slot: u64, blob_index: u64) -> Result<Vec<u8>> {
        let req = Protocol::RequestWindowIndex(self.my_data().clone(), slot, blob_index);
        let out = serialize(&req)?;
        Ok(out)
    }

    fn window_highest_index_request_bytes(&self, slot: u64, blob_index: u64) -> Result<Vec<u8>> {
        let req = Protocol::RequestHighestWindowIndex(self.my_data().clone(), slot, blob_index);
        let out = serialize(&req)?;
        Ok(out)
    }

    fn orphan_bytes(&self, slot: u64) -> Result<Vec<u8>> {
        let req = Protocol::RequestOrphan(self.my_data().clone(), slot);
        let out = serialize(&req)?;
        Ok(out)
    }

    pub fn repair_request(&self, repair_request: &FixPlanType) -> Result<(SocketAddr, Vec<u8>)> {
        // find a peer that appears to be accepting replication, as indicated
        //  by a valid blaze_unit port location
        let valid: Vec<_> = self.repair_peers();
        if valid.is_empty() {
            Err(NodeGroupInfoError::NoPeers)?;
        }
        let n = thread_rng().gen::<usize>() % valid.len();
        let addr = valid[n].gossip; // send the request to the peer's gossip port
        let out = {
            match repair_request {
                FixPlanType::Blob(slot, blob_index) => {
                    datapoint_debug!(
                        "node_group_info-repair",
                        ("repair-slot", *slot, i64),
                        ("repair-ix", *blob_index, i64)
                    );
                    self.window_index_request_bytes(*slot, *blob_index)?
                }
                FixPlanType::HighestBlob(slot, blob_index) => {
                    datapoint_debug!(
                        "node_group_info-repair_highest",
                        ("repair-highest-slot", *slot, i64),
                        ("repair-highest-ix", *blob_index, i64)
                    );
                    self.window_highest_index_request_bytes(*slot, *blob_index)?
                }
                FixPlanType::SingletonFix(slot) => {
                    datapoint_debug!("node_group_info-repair_orphan", ("repair-orphan", *slot, i64));
                    self.orphan_bytes(*slot)?
                }
            }
        };

        Ok((addr, out))
    }
    // If the network entrypoint hasn't been discovered yet, add it to the contact_info_table table
    fn add_entrypoint(&mut self, pulls: &mut Vec<(BvmAddr, Bloom<Hash>, SocketAddr, ContInfTblValue)>) {
        match &self.entrypoint {
            Some(entrypoint) => {
                let self_info = self
                    .gossip
                    .contact_info_table
                    .lookup(&ContInfTblValueTag::ContactInfo(self.id()))
                    .unwrap_or_else(|| panic!("self_id invalid {}", self.id()));

                pulls.push((
                    entrypoint.id,
                    self.gossip.pull.build_crds_filter(&self.gossip.contact_info_table),
                    entrypoint.gossip,
                    self_info.clone(),
                ))
            }
            None => (),
        }
    }

    fn new_pull_requests(&mut self, stakes: &HashMap<BvmAddr, u64>) -> Vec<(SocketAddr, Protocol)> {
        let now = timestamp();
        let pulls: Vec<_> = self
            .gossip
            .new_pull_request(now, stakes)
            .ok()
            .into_iter()
            .collect();

        let mut pr: Vec<_> = pulls
            .into_iter()
            .filter_map(|(peer, filter, self_info)| {
                let peer_label = ContInfTblValueTag::ContactInfo(peer);
                self.gossip
                    .contact_info_table
                    .lookup(&peer_label)
                    .and_then(ContInfTblValue::contact_info)
                    .map(|peer_info| (peer, filter, peer_info.gossip, self_info))
            })
            .collect();
        if pr.is_empty() {
            self.add_entrypoint(&mut pr);
        }
        pr.into_iter()
            .map(|(peer, filter, gossip, self_info)| {
                self.gossip.mark_pull_request_creation_time(&peer, now);
                (gossip, Protocol::PullRequest(filter, self_info))
            })
            .collect()
    }
    fn new_push_requests(&mut self) -> Vec<(SocketAddr, Protocol)> {
        let self_id = self.gossip.id;
        let (_, peers, msgs) = self.gossip.new_push_messages(timestamp());
        peers
            .into_iter()
            .filter_map(|p| {
                let peer_label = ContInfTblValueTag::ContactInfo(p);
                self.gossip
                    .contact_info_table
                    .lookup(&peer_label)
                    .and_then(ContInfTblValue::contact_info)
                    .map(|p| p.gossip)
            })
            .map(|peer| (peer, Protocol::PushMessage(self_id, msgs.clone())))
            .collect()
    }

    fn gossip_request(&mut self, stakes: &HashMap<BvmAddr, u64>) -> Vec<(SocketAddr, Protocol)> {
        let pulls: Vec<_> = self.new_pull_requests(stakes);
        let pushes: Vec<_> = self.new_push_requests();
        vec![pulls, pushes].into_iter().flat_map(|x| x).collect()
    }

    /// At random pick a node and try to get updated changes from them
    fn run_gossip(
        obj: &Arc<RwLock<Self>>,
        stakes: &HashMap<BvmAddr, u64>,
        blob_sender: &BlobSndr,
    ) -> Result<()> {
        let reqs = obj.write().unwrap().gossip_request(&stakes);
        let blobs = reqs
            .into_iter()
            .filter_map(|(remote_gossip_addr, req)| to_shared_blob(req, remote_gossip_addr).ok())
            .collect();
        blob_sender.send(blobs)?;
        Ok(())
    }

    /// randomly pick a node and ask them for updates asynchronously
    pub fn gossip(
        obj: Arc<RwLock<Self>>,
        treasury_forks: Option<Arc<RwLock<TreasuryForks>>>,
        blob_sender: BlobSndr,
        exit: &Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let exit = exit.clone();
        Builder::new()
            .name("bitconch-gossip".to_string())
            .spawn(move || {
                let mut last_push = timestamp();
                loop {
                    let start = timestamp();
                    let stakes: HashMap<_, _> = match treasury_forks {
                        Some(ref treasury_forks) => {
                            staking_utils::staked_nodes(&treasury_forks.read().unwrap().working_treasury())
                        }
                        None => HashMap::new(),
                    };
                    let _ = Self::run_gossip(&obj, &stakes, &blob_sender);
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    obj.write().unwrap().purge(timestamp());
                    //TODO: possibly tune this parameter
                    //we saw a deadlock passing an obj.read().unwrap().timeout into sleep
                    if start - last_push > NDTB_GOSSIP_PULL_CRDS_TIMEOUT_MS / 2 {
                        obj.write().unwrap().push_self(&stakes);
                        last_push = timestamp();
                    }
                    let elapsed = timestamp() - start;
                    if GOSSIP_SLEEP_MILLIS > elapsed {
                        let time_left = GOSSIP_SLEEP_MILLIS - elapsed;
                        sleep(Duration::from_millis(time_left));
                    }
                }
            })
            .unwrap()
    }

    fn run_window_request(
        from: &ContactInfo,
        from_addr: &SocketAddr,
        block_buffer_pool: Option<&Arc<BlockBufferPool>>,
        me: &ContactInfo,
        slot: u64,
        blob_index: u64,
    ) -> Vec<ArcBlb> {
        if let Some(block_buffer_pool) = block_buffer_pool {
            // Try to find the requested index in one of the slots
            let blob = block_buffer_pool.fetch_data_blob(slot, blob_index);

            if let Ok(Some(mut blob)) = blob {
                inc_new_counter_debug!("node_group_info-window-request-ledger", 1);
                blob.meta.set_addr(from_addr);

                return vec![Arc::new(RwLock::new(blob))];
            }
        }

        inc_new_counter_debug!("node_group_info-window-request-fail", 1);
        trace!(
            "{}: failed RequestWindowIndex {} {} {}",
            me.id,
            from.id,
            slot,
            blob_index,
        );

        vec![]
    }

    fn run_highest_window_request(
        from_addr: &SocketAddr,
        block_buffer_pool: Option<&Arc<BlockBufferPool>>,
        slot: u64,
        highest_index: u64,
    ) -> Vec<ArcBlb> {
        if let Some(block_buffer_pool) = block_buffer_pool {
            // Try to find the requested index in one of the slots
            let meta = block_buffer_pool.meta(slot);

            if let Ok(Some(meta)) = meta {
                if meta.received > highest_index {
                    // meta.received must be at least 1 by this point
                    let blob = block_buffer_pool.fetch_data_blob(slot, meta.received - 1);

                    if let Ok(Some(mut blob)) = blob {
                        blob.meta.set_addr(from_addr);
                        return vec![Arc::new(RwLock::new(blob))];
                    }
                }
            }
        }

        vec![]
    }

    fn run_orphan(
        from_addr: &SocketAddr,
        block_buffer_pool: Option<&Arc<BlockBufferPool>>,
        mut slot: u64,
        max_responses: usize,
    ) -> Vec<ArcBlb> {
        let mut res = vec![];
        if let Some(block_buffer_pool) = block_buffer_pool {
            // Try to find the next "n" parent slots of the input slot
            while let Ok(Some(meta)) = block_buffer_pool.meta(slot) {
                if meta.received == 0 {
                    break;
                }
                let blob = block_buffer_pool.fetch_data_blob(slot, meta.received - 1);
                if let Ok(Some(mut blob)) = blob {
                    blob.meta.set_addr(from_addr);
                    res.push(Arc::new(RwLock::new(blob)));
                }
                if meta.is_parent_set() && res.len() <= max_responses {
                    slot = meta.parent_slot;
                } else {
                    break;
                }
            }
        }

        res
    }

    //TODO we should first coalesce all the requests
    fn handle_blob(
        obj: &Arc<RwLock<Self>>,
        block_buffer_pool: Option<&Arc<BlockBufferPool>>,
        blob: &Blob,
    ) -> Vec<ArcBlb> {
        deserialize(&blob.data[..blob.meta.size])
            .into_iter()
            .flat_map(|request| {
                NodeGroupInfo::handle_protocol(obj, &blob.meta.addr(), block_buffer_pool, request)
            })
            .collect()
    }

    fn handle_pull_request(
        me: &Arc<RwLock<Self>>,
        filter: Bloom<Hash>,
        caller: ContInfTblValue,
        from_addr: &SocketAddr,
    ) -> Vec<ArcBlb> {
        let self_id = me.read().unwrap().gossip.id;
        inc_new_counter_debug!("node_group_info-pull_request", 1);
        if caller.contact_info().is_none() {
            return vec![];
        }
        let mut from = caller.contact_info().cloned().unwrap();
        if from.id == self_id {
            // warn!(
            //     "PullRequest ignored, I'm talking to myself: me={} remoteme={}",
            //     self_id, from.id
            // );
            println!(
                "{}",
                Warn(
                    format!("PullRequest ignored, I'm talking to myself: me={} remoteme={}",
                        self_id, from.id).to_string(),
                    module_path!().to_string()
                )
            );
            inc_new_counter_debug!("node_group_info-window-request-loopback", 1);
            return vec![];
        }
        let now = timestamp();
        let data = me
            .write()
            .unwrap()
            .gossip
            .process_pull_request(caller, filter, now);
        let len = data.len();
        trace!("get updates since response {}", len);
        let rsp = Protocol::PullResponse(self_id, data);
        // The remote node may not know its public IP:PORT. Record what it looks like to us.
        // This may or may not be correct for everybody, but it's better than leaving the remote with
        // an unspecified address in our table
        if from.gossip.ip().is_unspecified() {
            inc_new_counter_debug!("node_group_info-window-request-updates-unspec-gossip", 1);
            from.gossip = *from_addr;
        }
        inc_new_counter_debug!("node_group_info-pull_request-rsp", len);
        to_shared_blob(rsp, from.gossip).ok().into_iter().collect()
    }
    fn handle_pull_response(me: &Arc<RwLock<Self>>, from: &BvmAddr, data: Vec<ContInfTblValue>) {
        let len = data.len();
        let now = Instant::now();
        let self_id = me.read().unwrap().gossip.id;
        trace!("PullResponse me: {} from: {} len={}", self_id, from, len);
        me.write()
            .unwrap()
            .gossip
            .process_pull_response(from, data, timestamp());
        inc_new_counter_debug!("node_group_info-pull_request_response", 1);
        inc_new_counter_debug!("node_group_info-pull_request_response-size", len);

        report_time_spent("ReceiveUpdates", &now.elapsed(), &format!(" len: {}", len));
    }
    fn handle_push_message(
        me: &Arc<RwLock<Self>>,
        from: &BvmAddr,
        data: Vec<ContInfTblValue>,
    ) -> Vec<ArcBlb> {
        let self_id = me.read().unwrap().gossip.id;
        inc_new_counter_debug!("node_group_info-push_message", 1, 0, 1000);

        //let prunes: Vec<_> = me
        let target_addresss: Vec<_> = me
            .write()
            .unwrap()
            .gossip
            .process_push_message(data, timestamp());

        //if !prunes.is_empty() {
          if !target_addresss.is_empty() {  
            //inc_new_counter_debug!("node_group_info-push_message-prunes", prunes.len());
            inc_new_counter_debug!("node_group_info-push_message-prunes", target_addresss.len());
            let ci = me.read().unwrap().lookup(from).cloned();
            let pushes: Vec<_> = me.write().unwrap().new_push_requests();
            inc_new_counter_debug!("node_group_info-push_message-pushes", pushes.len());
            let mut rsp: Vec<_> = ci
                .and_then(|ci| {
                    let mut slash_message = SlashMessage {
                        /*
                        address: self_id,
                        prunes,
                        signature: Signature::default(),
                        destination: *from,
                        wallclock: timestamp(),
                        */
                        sender_address: self_id,
                        target_addresss,
                        msg_signature: Signature::default(),
                        receiver_address: *from,
                        wallclock: timestamp(),
                    };
                    slash_message.sign(&me.read().unwrap().keypair);
                    let rsp = Protocol::PruneMessage(self_id, slash_message);
                    to_shared_blob(rsp, ci.gossip).ok()
                })
                .into_iter()
                .collect();
            let mut blobs: Vec<_> = pushes
                .into_iter()
                .filter_map(|(remote_gossip_addr, req)| {
                    to_shared_blob(req, remote_gossip_addr).ok()
                })
                .collect();
            rsp.append(&mut blobs);
            rsp
        } else {
            vec![]
        }
    }

    fn get_repair_sender(request: &Protocol) -> &ContactInfo {
        match request {
            Protocol::RequestWindowIndex(ref from, _, _) => from,
            Protocol::RequestHighestWindowIndex(ref from, _, _) => from,
            Protocol::RequestOrphan(ref from, _) => from,
            _ => panic!("Not a repair request"),
        }
    }

    fn handle_repair(
        me: &Arc<RwLock<Self>>,
        from_addr: &SocketAddr,
        block_buffer_pool: Option<&Arc<BlockBufferPool>>,
        request: Protocol,
    ) -> Vec<ArcBlb> {
        let now = Instant::now();

        //TODO this doesn't depend on node_group_info module, could be moved
        //but we are using the listen thread to service these request
        //TODO verify from is signed

        let self_id = me.read().unwrap().gossip.id;
        let from = Self::get_repair_sender(&request);
        if from.id == me.read().unwrap().gossip.id {
            // warn!(
            //     "{}: Ignored received repair request from ME {}",
            //     self_id, from.id,
            // );
            println!(
                "{}",
                Warn(
                    format!("{}: Ignored received repair request from ME {}",
                        self_id, from.id,).to_string(),
                    module_path!().to_string()
                )
            );
            inc_new_counter_debug!("node_group_info-handle-repair--eq", 1);
            return vec![];
        }

        me.write()
            .unwrap()
            .gossip
            .contact_info_table
            .update_record_timestamp(&from.id, timestamp());
        let my_info = me.read().unwrap().my_data().clone();

        let (res, label) = {
            match &request {
                Protocol::RequestWindowIndex(from, slot, blob_index) => {
                    inc_new_counter_debug!("node_group_info-request-window-index", 1);
                    (
                        Self::run_window_request(
                            from,
                            &from_addr,
                            block_buffer_pool,
                            &my_info,
                            *slot,
                            *blob_index,
                        ),
                        "RequestWindowIndex",
                    )
                }

                Protocol::RequestHighestWindowIndex(_, slot, highest_index) => {
                    inc_new_counter_debug!("node_group_info-request-highest-window-index", 1);
                    (
                        Self::run_highest_window_request(
                            &from_addr,
                            block_buffer_pool,
                            *slot,
                            *highest_index,
                        ),
                        "RequestHighestWindowIndex",
                    )
                }
                Protocol::RequestOrphan(_, slot) => {
                    inc_new_counter_debug!("node_group_info-request-orphan", 1);
                    (
                        Self::run_orphan(&from_addr, block_buffer_pool, *slot, MAX_ORPHAN_REPAIR_RESPONSES),
                        "RequestOrphan",
                    )
                }
                _ => panic!("Not a repair request"),
            }
        };

        trace!("{}: received repair request: {:?}", self_id, request);
        report_time_spent(label, &now.elapsed(), "");
        res
    }

    fn handle_protocol(
        me: &Arc<RwLock<Self>>,
        from_addr: &SocketAddr,
        block_buffer_pool: Option<&Arc<BlockBufferPool>>,
        request: Protocol,
    ) -> Vec<ArcBlb> {
        match request {
            // TODO verify messages faster
            Protocol::PullRequest(filter, caller) => {
                if !caller.verify() {
                    inc_new_counter_error!("node_group_info-gossip_pull_request_verify_fail", 1);
                    vec![]
                } else {
                    Self::handle_pull_request(me, filter, caller, from_addr)
                }
            }
            Protocol::PullResponse(from, mut data) => {
                data.retain(|v| {
                    let ret = v.verify();
                    if !ret {
                        inc_new_counter_error!("node_group_info-gossip_pull_response_verify_fail", 1);
                    }
                    ret
                });
                Self::handle_pull_response(me, &from, data);
                vec![]
            }
            Protocol::PushMessage(from, mut data) => {
                data.retain(|v| {
                    let ret = v.verify();
                    if !ret {
                        inc_new_counter_error!("node_group_info-gossip_push_msg_verify_fail", 1);
                    }
                    ret
                });
                Self::handle_push_message(me, &from, data)
            }
            Protocol::PruneMessage(from, data) => {
                if data.verify() {
                    inc_new_counter_debug!("node_group_info-prune_message", 1);
                    //inc_new_counter_debug!("node_group_info-prune_message-size", data.prunes.len());
                    inc_new_counter_debug!("node_group_info-prune_message-size", data.target_addresss.len());
                    match me.write().unwrap().gossip.process_prune_msg(
                        &from,
                        /*
                        &data.destination,
                        &data.prunes,
                        */
                        &data.receiver_address,
                        &data.target_addresss,
                        data.wallclock,
                        timestamp(),
                    ) {
                        Err(NodeTbleErr::PruneMessageTimeout) => {
                            inc_new_counter_debug!("node_group_info-prune_message_timeout", 1)
                        }
                        Err(NodeTbleErr::BadPruneDestination) => {
                            inc_new_counter_debug!("node_group_info-bad_prune_destination", 1)
                        }
                        Err(_) => (),
                        Ok(_) => (),
                    }
                } else {
                    inc_new_counter_debug!("node_group_info-gossip_prune_msg_verify_fail", 1);
                }
                vec![]
            }
            _ => Self::handle_repair(me, from_addr, block_buffer_pool, request),
        }
    }

    /// Process messages from the network
    fn run_listen(
        obj: &Arc<RwLock<Self>>,
        block_buffer_pool: Option<&Arc<BlockBufferPool>>,
        requests_receiver: &BlobAcptr,
        response_sender: &BlobSndr,
    ) -> Result<()> {
        //TODO cache connections
        let timeout = Duration::new(1, 0);
        let mut reqs = requests_receiver.recv_timeout(timeout)?;
        while let Ok(mut more) = requests_receiver.try_recv() {
            reqs.append(&mut more);
        }
        let mut resps = Vec::new();
        for req in reqs {
            let mut resp = Self::handle_blob(obj, block_buffer_pool, &req.read().unwrap());
            resps.append(&mut resp);
        }
        response_sender.send(resps)?;
        Ok(())
    }
    pub fn listen(
        me: Arc<RwLock<Self>>,
        block_buffer_pool: Option<Arc<BlockBufferPool>>,
        requests_receiver: BlobAcptr,
        response_sender: BlobSndr,
        exit: &Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let exit = exit.clone();
        Builder::new()
            .name("bitconch-listen".to_string())
            .spawn(move || loop {
                let e = Self::run_listen(
                    &me,
                    block_buffer_pool.as_ref(),
                    &requests_receiver,
                    &response_sender,
                );
                if exit.load(Ordering::Relaxed) {
                    return;
                }
                if e.is_err() {
                    let me = me.read().unwrap();
                    debug!(
                        "{}: run_listen timeout, table size: {}",
                        me.gossip.id,
                        me.gossip.contact_info_table.table.len()
                    );
                }
            })
            .unwrap()
    }

    /// An alternative to Spy Node that has a valid gossip address and fully participate in Gossip.
    pub fn gossip_node(id: &BvmAddr, gossip_addr: &SocketAddr) -> (ContactInfo, UdpSocket) {
        let (port, gossip_socket) = Node::get_gossip_port(gossip_addr, FULLNODE_PORT_RANGE);
        let daddr = socketaddr_any!();

        let node = ContactInfo::new(
            id,
            SocketAddr::new(gossip_addr.ip(), port),
            daddr,
            daddr,
            daddr,
            daddr,
            daddr,
            daddr,
            timestamp(),
        );
        (node, gossip_socket)
    }

    /// A Node with invalid ports to spy on gossip via pull requests
    pub fn spy_node(id: &BvmAddr) -> (ContactInfo, UdpSocket) {
        let (_, gossip_socket) = bind_in_range(FULLNODE_PORT_RANGE).unwrap();
        let daddr = socketaddr_any!();

        let node = ContactInfo::new(
            id,
            daddr,
            daddr,
            daddr,
            daddr,
            daddr,
            daddr,
            daddr,
            timestamp(),
        );
        (node, gossip_socket)
    }
}

/// Avalanche logic
/// 1 - For the current node find out if it is in layer 1
/// 1.1 - If yes, then broadcast to all layer 1 nodes
///      1 - using the layer 1 index, broadcast to all layer 2 nodes assuming you know neighborhood size
/// 1.2 - If no, then figure out what layer the node is in and who the neighbors are and only broadcast to them
///      1 - also check if there are nodes in the next layer and repeat the layer 1 to layer 2 logic

/// Returns Neighbor Nodes and Children Nodes `(neighbors, children)` for a given node based on its stake (Treasury Balance)
pub fn compute_retransmit_peers<S: std::hash::BuildHasher>(
    stakes: Option<&HashMap<BvmAddr, u64, S>>,
    node_group_info: &Arc<RwLock<NodeGroupInfo>>,
    fanout: usize,
) -> (Vec<ContactInfo>, Vec<ContactInfo>) {
    let (my_index, peers) = node_group_info.read().unwrap().sorted_peers_and_index(stakes);
    //calc num_layers and num_neighborhoods using the total number of nodes
    let (num_layers, layer_indices) = NodeGroupInfo::describe_data_plane(peers.len(), fanout);

    if num_layers <= 1 {
        /* single layer data plane */
        (peers, vec![])
    } else {
        //find my layer
        let locality = NodeGroupInfo::localize(&layer_indices, fanout, my_index);
        let upper_bound = cmp::min(locality.neighbor_bounds.1, peers.len());
        let neighbors = peers[locality.neighbor_bounds.0..upper_bound].to_vec();
        let mut children = Vec::new();
        for ix in locality.next_layer_peers {
            if let Some(peer) = peers.get(ix) {
                children.push(peer.clone());
                continue;
            }
            break;
        }
        (neighbors, children)
    }
}

#[derive(Debug)]
pub struct Sockets {
    pub gossip: UdpSocket,
    pub blaze_unit: Vec<UdpSocket>,
    pub transaction_digesting_module: Vec<UdpSocket>,
    pub transaction_digesting_module_via_blobs: Vec<UdpSocket>,
    pub broadcast: UdpSocket,
    pub repair: UdpSocket,
    pub retransmit: UdpSocket,
    pub storage: Option<UdpSocket>,
}

#[derive(Debug)]
pub struct Node {
    pub info: ContactInfo,
    pub sockets: Sockets,
}

impl Node {
    pub fn new_localhost() -> Self {
        let address = BvmAddr::new_rand();
        Self::new_localhost_with_address(&address)
    }
    pub fn new_localhost_storage_miner(address: &BvmAddr) -> Self {
        let gossip = UdpSocket::bind("127.0.0.1:0").unwrap();
        let blaze_unit = UdpSocket::bind("127.0.0.1:0").unwrap();
        let storage = UdpSocket::bind("127.0.0.1:0").unwrap();
        let empty = "0.0.0.0:0".parse().unwrap();
        let repair = UdpSocket::bind("127.0.0.1:0").unwrap();

        let broadcast = UdpSocket::bind("0.0.0.0:0").unwrap();
        let retransmit = UdpSocket::bind("0.0.0.0:0").unwrap();
        let info = ContactInfo::new(
            address,
            gossip.local_addr().unwrap(),
            blaze_unit.local_addr().unwrap(),
            empty,
            empty,
            storage.local_addr().unwrap(),
            empty,
            empty,
            timestamp(),
        );

        Node {
            info,
            sockets: Sockets {
                gossip,
                blaze_unit: vec![blaze_unit],
                transaction_digesting_module: vec![],
                transaction_digesting_module_via_blobs: vec![],
                broadcast,
                repair,
                retransmit,
                storage: Some(storage),
            },
        }
    }
    pub fn new_localhost_with_address(address: &BvmAddr) -> Self {
        let transaction_digesting_module = UdpSocket::bind("127.0.0.1:0").unwrap();
        let gossip = UdpSocket::bind("127.0.0.1:0").unwrap();
        let blaze_unit = UdpSocket::bind("127.0.0.1:0").unwrap();
        let transaction_digesting_module_via_blobs = UdpSocket::bind("127.0.0.1:0").unwrap();
        let repair = UdpSocket::bind("127.0.0.1:0").unwrap();
        let rpc_port = find_available_port_in_range((1024, 65535)).unwrap();
        let rpc_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), rpc_port);
        let rpc_pubsub_port = find_available_port_in_range((1024, 65535)).unwrap();
        let rpc_pubsub_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), rpc_pubsub_port);

        let broadcast = UdpSocket::bind("0.0.0.0:0").unwrap();
        let retransmit = UdpSocket::bind("0.0.0.0:0").unwrap();
        let storage = UdpSocket::bind("0.0.0.0:0").unwrap();
        let info = ContactInfo::new(
            address,
            gossip.local_addr().unwrap(),
            blaze_unit.local_addr().unwrap(),
            transaction_digesting_module.local_addr().unwrap(),
            transaction_digesting_module_via_blobs.local_addr().unwrap(),
            storage.local_addr().unwrap(),
            rpc_addr,
            rpc_pubsub_addr,
            timestamp(),
        );
        Node {
            info,
            sockets: Sockets {
                gossip,
                blaze_unit: vec![blaze_unit],
                transaction_digesting_module: vec![transaction_digesting_module],
                transaction_digesting_module_via_blobs: vec![transaction_digesting_module_via_blobs],
                broadcast,
                repair,
                retransmit,
                storage: None,
            },
        }
    }
    fn get_gossip_port(gossip_addr: &SocketAddr, port_range: PortRange) -> (u16, UdpSocket) {
        if gossip_addr.port() != 0 {
            (
                gossip_addr.port(),
                bind_to(gossip_addr.port(), false).unwrap_or_else(|e| {
                    panic!("gossip_addr bind_to port {}: {}", gossip_addr.port(), e)
                }),
            )
        } else {
            Self::bind(port_range)
        }
    }
    fn bind(port_range: PortRange) -> (u16, UdpSocket) {
        bind_in_range(port_range).expect("Failed to bind")
    }
    pub fn new_with_external_ip(
        address: &BvmAddr,
        gossip_addr: &SocketAddr,
        port_range: PortRange,
    ) -> Node {
        let (gossip_port, gossip) = Self::get_gossip_port(gossip_addr, port_range);

        let (blaze_node_port, blaze_node_sockets) = multi_bind_in_range(port_range, 8).expect("blaze_unit multi_bind");

        let (transaction_digesting_module_port, transaction_digesting_module_sockets) = multi_bind_in_range(port_range, 32).expect("transaction_digesting_module multi_bind");

        let (transaction_digesting_module_via_blobs_port, transaction_digesting_module_via_blobs_sockets) =
            multi_bind_in_range(port_range, 8).expect("transaction_digesting_module multi_bind");

        let (_, repair) = Self::bind(port_range);
        let (_, broadcast) = Self::bind(port_range);
        let (_, retransmit) = Self::bind(port_range);

        let info = ContactInfo::new(
            address,
            SocketAddr::new(gossip_addr.ip(), gossip_port),
            SocketAddr::new(gossip_addr.ip(), blaze_node_port),
            SocketAddr::new(gossip_addr.ip(), transaction_digesting_module_port),
            SocketAddr::new(gossip_addr.ip(), transaction_digesting_module_via_blobs_port),
            socketaddr_any!(),
            socketaddr_any!(),
            socketaddr_any!(),
            0,
        );
        trace!("new ContactInfo: {:?}", info);

        Node {
            info,
            sockets: Sockets {
                gossip,
                blaze_unit: blaze_node_sockets,
                transaction_digesting_module: transaction_digesting_module_sockets,
                transaction_digesting_module_via_blobs: transaction_digesting_module_via_blobs_sockets,
                broadcast,
                repair,
                retransmit,
                storage: None,
            },
        }
    }
    pub fn new_miner_with_external_ip(
        address: &BvmAddr,
        gossip_addr: &SocketAddr,
        port_range: PortRange,
    ) -> Node {
        let mut new = Self::new_with_external_ip(address, gossip_addr, port_range);
        let (storage_port, storage_socket) = Self::bind(port_range);

        new.info.storage_addr = SocketAddr::new(gossip_addr.ip(), storage_port);
        new.sockets.storage = Some(storage_socket);

        let empty = socketaddr_any!();
        new.info.transaction_digesting_module = empty;
        new.info.transaction_digesting_module_via_blobs = empty;
        new.sockets.transaction_digesting_module = vec![];
        new.sockets.transaction_digesting_module_via_blobs = vec![];

        new
    }
}

fn report_time_spent(label: &str, time: &Duration, extra: &str) {
    let count = duration_as_ms(time);
    if count > 5 {
        // info!("{}", Info(format!("{} took: {} ms {}", label, count, extra).to_string()));
        let loginfo: String = format!("{} took: {} ms {}", label, count, extra).to_string();
        println!("{}",
            printLn(
                loginfo,
                module_path!().to_string()
            )
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_buffer_pool::fetch_interim_ledger_location;
    use crate::block_buffer_pool::tests::compose_candidate_fscl_stmts_in_batch;
    use crate::block_buffer_pool::BlockBufferPool;
    use crate::propagation_value::ContInfTblValueTag;
    use crate::packet::BLOB_HEADER_SIZE;
    use crate::fix_missing_spot_service::FixPlanType;
    use crate::result::Error;
    use crate::test_tx::test_tx;
    use bitconch_interface::signature::{Keypair, KeypairUtil};
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_gossip_node() {
        //check that a gossip nodes always show up as spies
        let (node, _) = NodeGroupInfo::spy_node(&BvmAddr::new_rand());
        assert!(NodeGroupInfo::is_spy_node(&node));
        let (node, _) =
            NodeGroupInfo::gossip_node(&BvmAddr::new_rand(), &"1.1.1.1:1111".parse().unwrap());
        assert!(NodeGroupInfo::is_spy_node(&node));
    }

    #[test]
    fn test_cluster_spy_gossip() {
        //check that gossip doesn't try to push to invalid addresses
        let node = Node::new_localhost();
        let (spy, _) = NodeGroupInfo::spy_node(&BvmAddr::new_rand());
        let node_group_info = Arc::new(RwLock::new(NodeGroupInfo::new_with_invalid_keypair(
            node.info,
        )));
        node_group_info.write().unwrap().insert_info(spy);
        node_group_info
            .write()
            .unwrap()
            .gossip
            .refresh_push_active_set(&HashMap::new());
        let reqs = node_group_info
            .write()
            .unwrap()
            .gossip_request(&HashMap::new());
        //assert none of the addrs are invalid.
        reqs.iter().all(|(addr, _)| {
            let res = ContactInfo::is_valid_address(addr);
            assert!(res);
            res
        });
    }

    #[test]
    fn test_cluster_info_new() {
        let d = ContactInfo::new_localhost(&BvmAddr::new_rand(), timestamp());
        let node_group_info = NodeGroupInfo::new_with_invalid_keypair(d.clone());
        assert_eq!(d.id, node_group_info.my_data().id);
    }

    #[test]
    fn insert_info_test() {
        let d = ContactInfo::new_localhost(&BvmAddr::new_rand(), timestamp());
        let mut node_group_info = NodeGroupInfo::new_with_invalid_keypair(d);
        let d = ContactInfo::new_localhost(&BvmAddr::new_rand(), timestamp());
        let label = ContInfTblValueTag::ContactInfo(d.id);
        node_group_info.insert_info(d);
        assert!(node_group_info.gossip.contact_info_table.lookup(&label).is_some());
    }
    #[test]
    fn test_insert_self() {
        let d = ContactInfo::new_localhost(&BvmAddr::new_rand(), timestamp());
        let mut node_group_info = NodeGroupInfo::new_with_invalid_keypair(d.clone());
        let entry_label = ContInfTblValueTag::ContactInfo(node_group_info.id());
        assert!(node_group_info.gossip.contact_info_table.lookup(&entry_label).is_some());

        // inserting something else shouldn't work
        let d = ContactInfo::new_localhost(&BvmAddr::new_rand(), timestamp());
        node_group_info.insert_self(d.clone());
        let label = ContInfTblValueTag::ContactInfo(d.id);
        assert!(node_group_info.gossip.contact_info_table.lookup(&label).is_none());
    }
    #[test]
    fn window_index_request() {
        let me = ContactInfo::new_localhost(&BvmAddr::new_rand(), timestamp());
        let mut node_group_info = NodeGroupInfo::new_with_invalid_keypair(me);
        let rv = node_group_info.repair_request(&FixPlanType::Blob(0, 0));
        assert_matches!(rv, Err(Error::NodeGroupInfoError(NodeGroupInfoError::NoPeers)));

        let gossip_addr = socketaddr!([127, 0, 0, 1], 1234);
        let nxt = ContactInfo::new(
            &BvmAddr::new_rand(),
            gossip_addr,
            socketaddr!([127, 0, 0, 1], 1235),
            socketaddr!([127, 0, 0, 1], 1236),
            socketaddr!([127, 0, 0, 1], 1237),
            socketaddr!([127, 0, 0, 1], 1238),
            socketaddr!([127, 0, 0, 1], 1239),
            socketaddr!([127, 0, 0, 1], 1240),
            0,
        );
        node_group_info.insert_info(nxt.clone());
        let rv = node_group_info
            .repair_request(&FixPlanType::Blob(0, 0))
            .unwrap();
        assert_eq!(nxt.gossip, gossip_addr);
        assert_eq!(rv.0, nxt.gossip);

        let gossip_addr2 = socketaddr!([127, 0, 0, 2], 1234);
        let nxt = ContactInfo::new(
            &BvmAddr::new_rand(),
            gossip_addr2,
            socketaddr!([127, 0, 0, 1], 1235),
            socketaddr!([127, 0, 0, 1], 1236),
            socketaddr!([127, 0, 0, 1], 1237),
            socketaddr!([127, 0, 0, 1], 1238),
            socketaddr!([127, 0, 0, 1], 1239),
            socketaddr!([127, 0, 0, 1], 1240),
            0,
        );
        node_group_info.insert_info(nxt);
        let mut one = false;
        let mut two = false;
        while !one || !two {
            //this randomly picks an option, so eventually it should pick both
            let rv = node_group_info
                .repair_request(&FixPlanType::Blob(0, 0))
                .unwrap();
            if rv.0 == gossip_addr {
                one = true;
            }
            if rv.0 == gossip_addr2 {
                two = true;
            }
        }
        assert!(one && two);
    }

    /// test window requests respond with the right blob, and do not overrun
    #[test]
    fn run_window_request() {
        bitconch_logger::setup();
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool = Arc::new(BlockBufferPool::open_ledger_file(&ledger_path).unwrap());
            let me = ContactInfo::new(
                &BvmAddr::new_rand(),
                socketaddr!("127.0.0.1:1234"),
                socketaddr!("127.0.0.1:1235"),
                socketaddr!("127.0.0.1:1236"),
                socketaddr!("127.0.0.1:1237"),
                socketaddr!("127.0.0.1:1238"),
                socketaddr!("127.0.0.1:1239"),
                socketaddr!("127.0.0.1:1240"),
                0,
            );
            let rv = NodeGroupInfo::run_window_request(
                &me,
                &socketaddr_any!(),
                Some(&block_buffer_pool),
                &me,
                0,
                0,
            );
            assert!(rv.is_empty());
            let data_size = 1;
            let blob = ArcBlb::default();
            {
                let mut w_blob = blob.write().unwrap();
                w_blob.set_size(data_size);
                w_blob.set_index(1);
                w_blob.set_slot(2);
                w_blob.meta.size = data_size + BLOB_HEADER_SIZE;
            }

            block_buffer_pool
                .record_public_objs(vec![&blob])
                .expect("Expect successful ledger write");

            let rv = NodeGroupInfo::run_window_request(
                &me,
                &socketaddr_any!(),
                Some(&block_buffer_pool),
                &me,
                2,
                1,
            );
            assert!(!rv.is_empty());
            let v = rv[0].clone();
            assert_eq!(v.read().unwrap().index(), 1);
            assert_eq!(v.read().unwrap().slot(), 2);
            assert_eq!(v.read().unwrap().meta.size, BLOB_HEADER_SIZE + data_size);
        }

        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }

    /// test run_window_requestwindow requests respond with the right blob, and do not overrun
    #[test]
    fn run_highest_window_request() {
        bitconch_logger::setup();
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool = Arc::new(BlockBufferPool::open_ledger_file(&ledger_path).unwrap());
            let rv =
                NodeGroupInfo::run_highest_window_request(&socketaddr_any!(), Some(&block_buffer_pool), 0, 0);
            assert!(rv.is_empty());

            let data_size = 1;
            let max_index = 5;
            let blobs: Vec<_> = (0..max_index)
                .map(|i| {
                    let mut blob = Blob::default();
                    blob.set_size(data_size);
                    blob.set_index(i);
                    blob.set_slot(2);
                    blob.meta.size = data_size + BLOB_HEADER_SIZE;
                    blob
                })
                .collect();

            block_buffer_pool
                .update_blobs(&blobs)
                .expect("Expect successful ledger write");

            let rv =
                NodeGroupInfo::run_highest_window_request(&socketaddr_any!(), Some(&block_buffer_pool), 2, 1);
            assert!(!rv.is_empty());
            let v = rv[0].clone();
            assert_eq!(v.read().unwrap().index(), max_index - 1);
            assert_eq!(v.read().unwrap().slot(), 2);
            assert_eq!(v.read().unwrap().meta.size, BLOB_HEADER_SIZE + data_size);

            let rv = NodeGroupInfo::run_highest_window_request(
                &socketaddr_any!(),
                Some(&block_buffer_pool),
                2,
                max_index,
            );
            assert!(rv.is_empty());
        }

        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn run_orphan() {
        bitconch_logger::setup();
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool = Arc::new(BlockBufferPool::open_ledger_file(&ledger_path).unwrap());
            let rv = NodeGroupInfo::run_orphan(&socketaddr_any!(), Some(&block_buffer_pool), 2, 0);
            assert!(rv.is_empty());

            // Create slots 1, 2, 3 with 5 blobs apiece
            let (blobs, _) = compose_candidate_fscl_stmts_in_batch(1, 3, 5);

            block_buffer_pool
                .update_blobs(&blobs)
                .expect("Expect successful ledger write");

            // We don't have slot 4, so we don't know how to service this requeset
            let rv = NodeGroupInfo::run_orphan(&socketaddr_any!(), Some(&block_buffer_pool), 4, 5);
            assert!(rv.is_empty());

            // For slot 3, we should return the highest blobs from slots 3, 2, 1 respectively
            // for this request
            let rv: Vec<_> = NodeGroupInfo::run_orphan(&socketaddr_any!(), Some(&block_buffer_pool), 3, 5)
                .iter()
                .map(|b| b.read().unwrap().clone())
                .collect();
            let expected: Vec<_> = (1..=3)
                .rev()
                .map(|slot| block_buffer_pool.fetch_data_blob(slot, 4).unwrap().unwrap())
                .collect();
            assert_eq!(rv, expected)
        }

        BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_default_leader() {
        bitconch_logger::setup();
        let contact_info = ContactInfo::new_localhost(&BvmAddr::new_rand(), 0);
        let mut node_group_info = NodeGroupInfo::new_with_invalid_keypair(contact_info);
        let ntwk_connection_pnt =
            ContactInfo::new_gossip_connection_url(&socketaddr!("127.0.0.1:1239"));
        node_group_info.insert_info(ntwk_connection_pnt);
        assert!(node_group_info.leader_data().is_none());
    }

    fn assert_in_range(x: u16, range: (u16, u16)) {
        assert!(x >= range.0);
        assert!(x < range.1);
    }

    fn check_sockets(sockets: &Vec<UdpSocket>, ip: IpAddr, range: (u16, u16)) {
        assert!(sockets.len() > 1);
        let port = sockets[0].local_addr().unwrap().port();
        for socket in sockets.iter() {
            check_socket(socket, ip, range);
            assert_eq!(socket.local_addr().unwrap().port(), port);
        }
    }

    fn check_socket(socket: &UdpSocket, ip: IpAddr, range: (u16, u16)) {
        let local_addr = socket.local_addr().unwrap();
        assert_eq!(local_addr.ip(), ip);
        assert_in_range(local_addr.port(), range);
    }

    fn check_node_sockets(node: &Node, ip: IpAddr, range: (u16, u16)) {
        check_socket(&node.sockets.gossip, ip, range);
        check_socket(&node.sockets.repair, ip, range);

        check_sockets(&node.sockets.blaze_unit, ip, range);
        check_sockets(&node.sockets.transaction_digesting_module, ip, range);
    }

    #[test]
    fn new_with_external_ip_test_random() {
        let ip = Ipv4Addr::from(0);
        let node = Node::new_with_external_ip(
            &BvmAddr::new_rand(),
            &socketaddr!(ip, 0),
            FULLNODE_PORT_RANGE,
        );

        check_node_sockets(&node, IpAddr::V4(ip), FULLNODE_PORT_RANGE);
    }

    #[test]
    fn new_with_external_ip_test_gossip() {
        let ip = IpAddr::V4(Ipv4Addr::from(0));
        let port = {
            bind_in_range(FULLNODE_PORT_RANGE)
                .expect("Failed to bind")
                .0
        };
        let node = Node::new_with_external_ip(
            &BvmAddr::new_rand(),
            &socketaddr!(0, port),
            FULLNODE_PORT_RANGE,
        );

        check_node_sockets(&node, ip, FULLNODE_PORT_RANGE);

        assert_eq!(node.sockets.gossip.local_addr().unwrap().port(), port);
    }

    #[test]
    fn new_storage_miner_external_ip_test() {
        let ip = Ipv4Addr::from(0);
        let node = Node::new_miner_with_external_ip(
            &BvmAddr::new_rand(),
            &socketaddr!(ip, 0),
            FULLNODE_PORT_RANGE,
        );

        let ip = IpAddr::V4(ip);
        check_socket(&node.sockets.storage.unwrap(), ip, FULLNODE_PORT_RANGE);
        check_socket(&node.sockets.gossip, ip, FULLNODE_PORT_RANGE);
        check_socket(&node.sockets.repair, ip, FULLNODE_PORT_RANGE);

        check_sockets(&node.sockets.blaze_unit, ip, FULLNODE_PORT_RANGE);
    }

    //test that all node_group_info objects only generate signed messages
    //when constructed with keypairs
    #[test]
    fn test_gossip_signature_verification() {
        //create new cluster info, leader, and peer
        let keypair = Keypair::new();
        let peer_keypair = Keypair::new();
        let leader_keypair = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keypair.address(), 0);
        let leader = ContactInfo::new_localhost(&leader_keypair.address(), 0);
        let peer = ContactInfo::new_localhost(&peer_keypair.address(), 0);
        let mut node_group_info = NodeGroupInfo::new(contact_info.clone(), Arc::new(keypair));
        node_group_info.set_leader(&leader.id);
        node_group_info.insert_info(peer.clone());
        //check that all types of gossip messages are signed correctly
        let (_, _, vals) = node_group_info.gossip.new_push_messages(timestamp());
        // there should be some pushes ready
        assert!(vals.len() > 0);
        vals.par_iter().for_each(|v| assert!(v.verify()));

        let (_, _, val) = node_group_info
            .gossip
            .new_pull_request(timestamp(), &HashMap::new())
            .ok()
            .unwrap();
        assert!(val.verify());
    }

    fn num_layers(nodes: usize, fanout: usize) -> usize {
        NodeGroupInfo::describe_data_plane(nodes, fanout).0
    }

    #[test]
    fn test_describe_data_plane() {
        // no nodes
        assert_eq!(num_layers(0, 200), 0);

        // 1 node
        assert_eq!(num_layers(1, 200), 1);

        // 10 nodes with fanout of 2
        assert_eq!(num_layers(10, 2), 3);

        // fanout + 1 nodes with fanout of 2
        assert_eq!(num_layers(3, 2), 2);

        // A little more realistic
        assert_eq!(num_layers(100, 10), 2);

        // A little more realistic with odd numbers
        assert_eq!(num_layers(103, 13), 2);

        // A little more realistic with just enough for 3 layers
        assert_eq!(num_layers(111, 10), 3);

        // larger
        let (layer_cnt, layer_indices) = NodeGroupInfo::describe_data_plane(10_000, 10);
        assert_eq!(layer_cnt, 4);
        // distances between index values should increase by `fanout` for every layer.
        let mut capacity = 10 * 10;
        assert_eq!(layer_indices[1], 10);
        layer_indices[1..].windows(2).for_each(|x| {
            if x.len() == 2 {
                assert_eq!(x[1] - x[0], capacity);
                capacity *= 10;
            }
        });

        // massive
        let (layer_cnt, layer_indices) = NodeGroupInfo::describe_data_plane(500_000, 200);
        let mut capacity = 200 * 200;
        assert_eq!(layer_cnt, 3);
        // distances between index values should increase by `fanout` for every layer.
        assert_eq!(layer_indices[1], 200);
        layer_indices[1..].windows(2).for_each(|x| {
            if x.len() == 2 {
                assert_eq!(x[1] - x[0], capacity);
                capacity *= 200;
            }
        });
        let total_capacity: usize = *layer_indices.last().unwrap();
        assert!(total_capacity >= 500_000);
    }

    #[test]
    fn test_localize() {
        // go for gold
        let (_, layer_indices) = NodeGroupInfo::describe_data_plane(500_000, 200);
        let mut me = 0;
        let mut layer_ix = 0;
        let locality = NodeGroupInfo::localize(&layer_indices, 200, me);
        assert_eq!(locality.layer_ix, layer_ix);
        assert_eq!(
            locality.next_layer_bounds,
            Some((layer_indices[layer_ix + 1], layer_indices[layer_ix + 2]))
        );
        me = 201;
        layer_ix = 1;
        let locality = NodeGroupInfo::localize(&layer_indices, 200, me);
        assert_eq!(
            locality.layer_ix, layer_ix,
            "layer_indices[layer_ix] is actually {}",
            layer_indices[layer_ix]
        );
        assert_eq!(
            locality.next_layer_bounds,
            Some((layer_indices[layer_ix + 1], layer_indices[layer_ix + 2]))
        );
        me = 20_000;
        layer_ix = 1;
        let locality = NodeGroupInfo::localize(&layer_indices, 200, me);
        assert_eq!(
            locality.layer_ix, layer_ix,
            "layer_indices[layer_ix] is actually {}",
            layer_indices[layer_ix]
        );
        assert_eq!(
            locality.next_layer_bounds,
            Some((layer_indices[layer_ix + 1], layer_indices[layer_ix + 2]))
        );

        // test no child layer since last layer should have massive capacity
        let (_, layer_indices) = NodeGroupInfo::describe_data_plane(500_000, 200);
        me = 40_201;
        layer_ix = 2;
        let locality = NodeGroupInfo::localize(&layer_indices, 200, me);
        assert_eq!(
            locality.layer_ix, layer_ix,
            "layer_indices[layer_ix] is actually {}",
            layer_indices[layer_ix]
        );
        assert_eq!(locality.next_layer_bounds, None);
    }

    #[test]
    fn test_localize_child_peer_overlap() {
        let (_, layer_indices) = NodeGroupInfo::describe_data_plane(500_000, 200);
        let last_ix = layer_indices.len() - 1;
        // sample every 33 pairs to reduce test time
        for x in (0..*layer_indices.get(last_ix - 2).unwrap()).step_by(33) {
            let me_locality = NodeGroupInfo::localize(&layer_indices, 200, x);
            let buddy_locality = NodeGroupInfo::localize(&layer_indices, 200, x + 1);
            assert!(!me_locality.next_layer_peers.is_empty());
            assert!(!buddy_locality.next_layer_peers.is_empty());
            me_locality
                .next_layer_peers
                .iter()
                .zip(buddy_locality.next_layer_peers.iter())
                .for_each(|(x, y)| assert_ne!(x, y));
        }
    }

    #[test]
    fn test_network_coverage() {
        // pretend to be each node in a scaled down network and make sure the set of all the broadcast peers
        // includes every node in the network.
        let (_, layer_indices) = NodeGroupInfo::describe_data_plane(25_000, 10);
        let mut broadcast_set = HashSet::new();
        for my_index in 0..25_000 {
            let my_locality = NodeGroupInfo::localize(&layer_indices, 10, my_index);
            broadcast_set.extend(my_locality.neighbor_bounds.0..my_locality.neighbor_bounds.1);
            broadcast_set.extend(my_locality.next_layer_peers);
        }

        for i in 0..25_000 {
            assert!(broadcast_set.contains(&(i as usize)));
        }
        assert!(broadcast_set.contains(&(layer_indices.last().unwrap() - 1)));
        //sanity check for past total capacity.
        assert!(!broadcast_set.contains(&(layer_indices.last().unwrap())));
    }

    #[test]
    fn test_push_vote() {
        let keys = Keypair::new();
        let now = timestamp();
        let contact_info = ContactInfo::new_localhost(&keys.address(), 0);
        let mut node_group_info = NodeGroupInfo::new_with_invalid_keypair(contact_info);

        // make sure empty contact_info_table is handled correctly
        let (votes, max_ts) = node_group_info.get_votes(now);
        assert_eq!(votes, vec![]);
        assert_eq!(max_ts, now);

        // add a vote
        let tx = test_tx();
        node_group_info.push_vote(tx.clone());

        // -1 to make sure that the clock is strictly lower then when insert occurred
        let (votes, max_ts) = node_group_info.get_votes(now - 1);
        assert_eq!(votes, vec![tx]);
        assert!(max_ts >= now - 1);

        // make sure timestamp filter works
        let (votes, new_max_ts) = node_group_info.get_votes(max_ts);
        assert_eq!(votes, vec![]);
        assert_eq!(max_ts, new_max_ts);
    }
}
#[test]
fn test_add_entrypoint() {
    let node_keypair = Arc::new(Keypair::new());
    let mut node_group_info = NodeGroupInfo::new(
        ContactInfo::new_localhost(&node_keypair.address(), timestamp()),
        node_keypair,
    );
    let entrypoint_address = BvmAddr::new_rand();
    let entrypoint = ContactInfo::new_localhost(&entrypoint_address, timestamp());
    node_group_info.set_entrypoint(entrypoint.clone());
    let pulls = node_group_info.new_pull_requests(&HashMap::new());
    assert_eq!(1, pulls.len());
    match pulls.get(0) {
        Some((addr, msg)) => {
            assert_eq!(*addr, entrypoint.gossip);
            match msg {
                Protocol::PullRequest(_, value) => {
                    assert!(value.verify());
                    assert_eq!(value.address(), node_group_info.id())
                }
                _ => panic!("wrong protocol"),
            }
        }
        None => panic!("entrypoint should be a pull destination"),
    }

    // now add this message back to the table and make sure after the next pull, the entrypoint is unset
    let connect_url_contact_info_table_value = ContInfTblValue::ContactInfo(entrypoint.clone());
    let node_group_info = Arc::new(RwLock::new(node_group_info));
    NodeGroupInfo::handle_pull_response(
        &node_group_info,
        &entrypoint_address,
        vec![connect_url_contact_info_table_value],
    );
    let pulls = node_group_info
        .write()
        .unwrap()
        .new_pull_requests(&HashMap::new());
    assert_eq!(1, pulls.len());
    assert_eq!(node_group_info.read().unwrap().entrypoint, Some(entrypoint));
}
