//! ContactInfoTable Gossip Push overlay
//! This module is used to propagate recently created CrdsValues across the network
//! Eager push strategy is based on Plumtree
//! http://asc.di.fct.unl.pt/~jleitao/pdf/srds07-leitao.pdf
//!
//! Main differences are:
//! 1. There is no `max hop`.  Messages are signed with a local wallclock.  If they are outside of
//!    the local nodes wallclock window they are drooped silently.
//! 2. The prune set is stored in a Bloom filter.

use crate::connection_info::ContactInfo;
use crate::connection_info_table::{ContactInfoTable, VerContInfTblValue};
use crate::gossip::{get_stake, get_weight};
use crate::ndtb_err::NodeTbleErr;
use crate::propagation_value::{ContInfTblValue, ContInfTblValueTag};
use crate::bvm_types::*;
use bincode::serialized_size;
use hashbrown::HashMap;
use indexmap::map::IndexMap;
use rand;
use rand::distributions::{Distribution, WeightedIndex};
use rand::seq::SliceRandom;
use morgan_runtime::bloom::Bloom;
use morgan_interface::hash::Hash;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::timing::timestamp;
use std::cmp;



#[derive(Clone)]
pub struct NodeTbleGspPush {
    /// max bytes per message
    pub max_bytes: usize,
    /// active set of validators for push
    active_set: IndexMap<BvmAddr, Bloom<BvmAddr>>,
    /// push message queue
    push_messages: HashMap<ContInfTblValueTag, Hash>,
    pushed_once: HashMap<Hash, u64>,
    pub num_active: usize,
    pub push_fanout: usize,
    pub msg_timeout: u64,
    pub prune_timeout: u64,
}

impl Default for NodeTbleGspPush {
    fn default() -> Self {
        Self {
            max_bytes: BLOB_DATA_SIZE,
            active_set: IndexMap::new(),
            push_messages: HashMap::new(),
            pushed_once: HashMap::new(),
            num_active: NDTB_GOSSIP_NUM_ACTIVE,
            push_fanout: NDTB_GOSSIP_PUSH_FANOUT,
            msg_timeout: NDTB_GOSSIP_PUSH_MSG_TIMEOUT_MS,
            prune_timeout: NDTB_GOSSIP_PRUNE_MSG_TIMEOUT_MS,
        }
    }
}
impl NodeTbleGspPush {
    pub fn num_pending(&self) -> usize {
        self.push_messages.len()
    }
    /// process a push message to the network
    pub fn process_push_message(
        &mut self,
        contact_info_table: &mut ContactInfoTable,
        value: ContInfTblValue,
        now: u64,
    ) -> Result<Option<VerContInfTblValue>, NodeTbleErr> {
        if now > value.wallclock() + self.msg_timeout {
            return Err(NodeTbleErr::PushMessageTimeout);
        }
        if now + self.msg_timeout < value.wallclock() {
            return Err(NodeTbleErr::PushMessageTimeout);
        }
        let label = value.label();

        let new_value = contact_info_table.new_versioned(now, value);
        let value_hash = new_value.value_hash;
        if self.pushed_once.get(&value_hash).is_some() {
            return Err(NodeTbleErr::PushMessagePrune);
        }
        let old = contact_info_table.insert_versioned(new_value);
        if old.is_err() {
            return Err(NodeTbleErr::PushMessageOldVersion);
        }
        self.push_messages.insert(label, value_hash);
        self.pushed_once.insert(value_hash, now);
        Ok(old.ok().and_then(|opt| opt))
    }

    /// New push message to broadcast to peers.
    /// Returns a list of Pubkeys for the selected peers and a list of values to send to all the
    /// peers.
    /// The list of push messages is created such that all the randomly selected peers have not
    /// pruned the genesis addresses.
    pub fn new_push_messages(&mut self, contact_info_table: &ContactInfoTable, now: u64) -> (Vec<BvmAddr>, Vec<ContInfTblValue>) {
        let max = self.active_set.len();
        let mut nodes: Vec<_> = (0..max).collect();
        nodes.shuffle(&mut rand::thread_rng());
        let peers: Vec<BvmAddr> = nodes
            .into_iter()
            .filter_map(|n| self.active_set.get_index(n))
            .take(self.push_fanout)
            .map(|n| *n.0)
            .collect();
        let mut total_bytes: usize = 0;
        let mut values = vec![];
        for (label, hash) in &self.push_messages {
            let mut failed = false;
            for p in &peers {
                let filter = self.active_set.get_mut(p);
                failed |= filter.is_none() || filter.unwrap().contains(&label.address());
            }
            if failed {
                continue;
            }
            let res = contact_info_table.lookup_versioned(label);
            if res.is_none() {
                continue;
            }
            let version = res.unwrap();
            if version.value_hash != *hash {
                continue;
            }
            let value = &version.value;
            if value.wallclock() > now || value.wallclock() + self.msg_timeout < now {
                continue;
            }
            total_bytes += serialized_size(value).unwrap() as usize;
            if total_bytes > self.max_bytes {
                break;
            }
            values.push(value.clone());
        }
        for v in &values {
            self.push_messages.remove(&v.label());
        }
        (peers, values)
    }

    /// add the `from` to the peer's filter of nodes
    pub fn process_prune_msg(&mut self, peer: &BvmAddr, origins: &[BvmAddr]) {
        for origin in origins {
            if let Some(p) = self.active_set.get_mut(peer) {
                p.add(origin)
            }
        }
    }

    fn compute_need(num_active: usize, active_set_len: usize, ratio: usize) -> usize {
        let num = active_set_len / ratio;
        cmp::min(num_active, (num_active - active_set_len) + num)
    }

    /// refresh the push active set
    /// * ratio - active_set.len()/ratio is the number of actives to rotate
    pub fn refresh_push_active_set(
        &mut self,
        contact_info_table: &ContactInfoTable,
        stakes: &HashMap<BvmAddr, u64>,
        self_id: &BvmAddr,
        network_size: usize,
        ratio: usize,
    ) {
        let need = Self::compute_need(self.num_active, self.active_set.len(), ratio);
        let mut new_items = HashMap::new();

        let mut options: Vec<_> = self.push_options(contact_info_table, &self_id, stakes);
        if options.is_empty() {
            return;
        }
        while new_items.len() < need {
            let index = WeightedIndex::new(options.iter().map(|weighted| weighted.0));
            if index.is_err() {
                break;
            }
            let index = index.unwrap();
            let index = index.sample(&mut rand::thread_rng());
            let item = options[index].1;
            options.remove(index);
            if self.active_set.get(&item.id).is_some() {
                continue;
            }
            if new_items.get(&item.id).is_some() {
                continue;
            }
            let size = cmp::max(NDTB_GOSSIP_BLOOM_SIZE, network_size);
            let bloom = Bloom::random(size, 0.1, 1024 * 8 * 4);
            new_items.insert(item.id, bloom);
        }
        let mut keys: Vec<BvmAddr> = self.active_set.keys().cloned().collect();
        keys.shuffle(&mut rand::thread_rng());
        let num = keys.len() / ratio;
        for k in &keys[..num] {
            self.active_set.remove(k);
        }
        for (k, v) in new_items {
            self.active_set.insert(k, v);
        }
    }

    fn push_options<'a>(
        &self,
        contact_info_table: &'a ContactInfoTable,
        self_id: &BvmAddr,
        stakes: &HashMap<BvmAddr, u64>,
    ) -> Vec<(f32, &'a ContactInfo)> {
        contact_info_table.table
            .values()
            .filter(|v| v.value.contact_info().is_some())
            .map(|v| (v.value.contact_info().unwrap(), v))
            .filter(|(info, _)| info.id != *self_id && ContactInfo::is_valid_address(&info.gossip))
            .map(|(info, value)| {
                let max_weight = f32::from(u16::max_value()) - 1.0;
                let last_updated: u64 = value.local_timestamp;
                let since = ((timestamp() - last_updated) / 1024) as u32;
                let stake = get_stake(&info.id, stakes);
                let weight = get_weight(max_weight, since, stake);
                (weight, info)
            })
            .collect()
    }

    /// purge old pending push messages
    pub fn purge_old_pending_push_messages(&mut self, contact_info_table: &ContactInfoTable, min_time: u64) {
        let old_msgs: Vec<ContInfTblValueTag> = self
            .push_messages
            .iter()
            .filter_map(|(k, hash)| {
                if let Some(versioned) = contact_info_table.lookup_versioned(k) {
                    if versioned.value.wallclock() < min_time || versioned.value_hash != *hash {
                        Some(k)
                    } else {
                        None
                    }
                } else {
                    Some(k)
                }
            })
            .cloned()
            .collect();
        for k in old_msgs {
            self.push_messages.remove(&k);
        }
    }
    /// purge old pushed_once messages
    pub fn purge_old_pushed_once_messages(&mut self, min_time: u64) {
        let old_msgs: Vec<Hash> = self
            .pushed_once
            .iter()
            .filter_map(|(k, v)| if *v < min_time { Some(k) } else { None })
            .cloned()
            .collect();
        for k in old_msgs {
            self.pushed_once.remove(&k);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::connection_info::ContactInfo;

    #[test]
    fn test_process_push() {
        let mut contact_info_table = ContactInfoTable::default();
        let mut push = NodeTbleGspPush::default();
        let value = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&BvmAddr::new_rand(), 0));
        let label = value.label();
        // push a new message
        assert_eq!(
            push.process_push_message(&mut contact_info_table, value.clone(), 0),
            Ok(None)
        );
        assert_eq!(contact_info_table.lookup(&label), Some(&value));

        // push it again
        assert_eq!(
            push.process_push_message(&mut contact_info_table, value.clone(), 0),
            Err(NodeTbleErr::PushMessagePrune)
        );
    }
    #[test]
    fn test_process_push_old_version() {
        let mut contact_info_table = ContactInfoTable::default();
        let mut push = NodeTbleGspPush::default();
        let mut ci = ContactInfo::new_localhost(&BvmAddr::new_rand(), 0);
        ci.wallclock = 1;
        let value = ContInfTblValue::ContactInfo(ci.clone());

        // push a new message
        assert_eq!(push.process_push_message(&mut contact_info_table, value, 0), Ok(None));

        // push an old version
        ci.wallclock = 0;
        let value = ContInfTblValue::ContactInfo(ci.clone());
        assert_eq!(
            push.process_push_message(&mut contact_info_table, value, 0),
            Err(NodeTbleErr::PushMessageOldVersion)
        );
    }
    #[test]
    fn test_process_push_timeout() {
        let mut contact_info_table = ContactInfoTable::default();
        let mut push = NodeTbleGspPush::default();
        let timeout = push.msg_timeout;
        let mut ci = ContactInfo::new_localhost(&BvmAddr::new_rand(), 0);

        // push a version to far in the future
        ci.wallclock = timeout + 1;
        let value = ContInfTblValue::ContactInfo(ci.clone());
        assert_eq!(
            push.process_push_message(&mut contact_info_table, value, 0),
            Err(NodeTbleErr::PushMessageTimeout)
        );

        // push a version to far in the past
        ci.wallclock = 0;
        let value = ContInfTblValue::ContactInfo(ci.clone());
        assert_eq!(
            push.process_push_message(&mut contact_info_table, value, timeout + 1),
            Err(NodeTbleErr::PushMessageTimeout)
        );
    }
    #[test]
    fn test_process_push_update() {
        let mut contact_info_table = ContactInfoTable::default();
        let mut push = NodeTbleGspPush::default();
        let mut ci = ContactInfo::new_localhost(&BvmAddr::new_rand(), 0);
        ci.wallclock = 0;
        let value_old = ContInfTblValue::ContactInfo(ci.clone());

        // push a new message
        assert_eq!(
            push.process_push_message(&mut contact_info_table, value_old.clone(), 0),
            Ok(None)
        );

        // push an old version
        ci.wallclock = 1;
        let value = ContInfTblValue::ContactInfo(ci.clone());
        assert_eq!(
            push.process_push_message(&mut contact_info_table, value, 0)
                .unwrap()
                .unwrap()
                .value,
            value_old
        );
    }
    #[test]
    fn test_compute_need() {
        assert_eq!(NodeTbleGspPush::compute_need(30, 0, 10), 30);
        assert_eq!(NodeTbleGspPush::compute_need(30, 1, 10), 29);
        assert_eq!(NodeTbleGspPush::compute_need(30, 30, 10), 3);
        assert_eq!(NodeTbleGspPush::compute_need(30, 29, 10), 3);
    }
    #[test]
    fn test_refresh_active_set() {
        morgan_logger::setup();
        let mut contact_info_table = ContactInfoTable::default();
        let mut push = NodeTbleGspPush::default();
        let value1 = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&BvmAddr::new_rand(), 0));

        assert_eq!(contact_info_table.insert(value1.clone(), 0), Ok(None));
        push.refresh_push_active_set(&contact_info_table, &HashMap::new(), &BvmAddr::default(), 1, 1);

        assert!(push.active_set.get(&value1.label().address()).is_some());
        let value2 = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&BvmAddr::new_rand(), 0));
        assert!(push.active_set.get(&value2.label().address()).is_none());
        assert_eq!(contact_info_table.insert(value2.clone(), 0), Ok(None));
        for _ in 0..30 {
            push.refresh_push_active_set(&contact_info_table, &HashMap::new(), &BvmAddr::default(), 1, 1);
            if push.active_set.get(&value2.label().address()).is_some() {
                break;
            }
        }
        assert!(push.active_set.get(&value2.label().address()).is_some());

        for _ in 0..push.num_active {
            let value2 = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&BvmAddr::new_rand(), 0));
            assert_eq!(contact_info_table.insert(value2.clone(), 0), Ok(None));
        }
        push.refresh_push_active_set(&contact_info_table, &HashMap::new(), &BvmAddr::default(), 1, 1);
        assert_eq!(push.active_set.len(), push.num_active);
    }
    #[test]
    fn test_active_set_refresh_with_treasury() {
        let time = timestamp() - 1024; //make sure there's at least a 1 second delay
        let mut contact_info_table = ContactInfoTable::default();
        let push = NodeTbleGspPush::default();
        let mut stakes = HashMap::new();
        for i in 1..=100 {
            let peer =
                ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&BvmAddr::new_rand(), time));
            let id = peer.label().address();
            contact_info_table.insert(peer.clone(), time).unwrap();
            stakes.insert(id, i * 100);
        }
        let mut options = push.push_options(&contact_info_table, &BvmAddr::default(), &stakes);
        assert!(!options.is_empty());
        options.sort_by(|(weight_l, _), (weight_r, _)| weight_r.partial_cmp(weight_l).unwrap());
        // check that the highest stake holder is also the heaviest weighted.
        assert_eq!(
            *stakes.get(&options.get(0).unwrap().1.id).unwrap(),
            10_000_u64
        );
    }
    #[test]
    fn test_new_push_messages() {
        let mut contact_info_table = ContactInfoTable::default();
        let mut push = NodeTbleGspPush::default();
        let peer = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&BvmAddr::new_rand(), 0));
        assert_eq!(contact_info_table.insert(peer.clone(), 0), Ok(None));
        push.refresh_push_active_set(&contact_info_table, &HashMap::new(), &BvmAddr::default(), 1, 1);

        let new_msg = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&BvmAddr::new_rand(), 0));
        assert_eq!(
            push.process_push_message(&mut contact_info_table, new_msg.clone(), 0),
            Ok(None)
        );
        assert_eq!(push.active_set.len(), 1);
        assert_eq!(
            push.new_push_messages(&contact_info_table, 0),
            (vec![peer.label().address()], vec![new_msg])
        );
    }
    #[test]
    fn test_process_prune() {
        let mut contact_info_table = ContactInfoTable::default();
        let mut push = NodeTbleGspPush::default();
        let peer = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&BvmAddr::new_rand(), 0));
        assert_eq!(contact_info_table.insert(peer.clone(), 0), Ok(None));
        push.refresh_push_active_set(&contact_info_table, &HashMap::new(), &BvmAddr::default(), 1, 1);

        let new_msg = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&BvmAddr::new_rand(), 0));
        assert_eq!(
            push.process_push_message(&mut contact_info_table, new_msg.clone(), 0),
            Ok(None)
        );
        push.process_prune_msg(&peer.label().address(), &[new_msg.label().address()]);
        assert_eq!(
            push.new_push_messages(&contact_info_table, 0),
            (vec![peer.label().address()], vec![])
        );
    }
    #[test]
    fn test_purge_old_pending_push_messages() {
        let mut contact_info_table = ContactInfoTable::default();
        let mut push = NodeTbleGspPush::default();
        let peer = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&BvmAddr::new_rand(), 0));
        assert_eq!(contact_info_table.insert(peer.clone(), 0), Ok(None));
        push.refresh_push_active_set(&contact_info_table, &HashMap::new(), &BvmAddr::default(), 1, 1);

        let mut ci = ContactInfo::new_localhost(&BvmAddr::new_rand(), 0);
        ci.wallclock = 1;
        let new_msg = ContInfTblValue::ContactInfo(ci.clone());
        assert_eq!(
            push.process_push_message(&mut contact_info_table, new_msg.clone(), 1),
            Ok(None)
        );
        push.purge_old_pending_push_messages(&contact_info_table, 0);
        assert_eq!(
            push.new_push_messages(&contact_info_table, 0),
            (vec![peer.label().address()], vec![])
        );
    }

    #[test]
    fn test_purge_old_pushed_once_messages() {
        let mut contact_info_table = ContactInfoTable::default();
        let mut push = NodeTbleGspPush::default();
        let mut ci = ContactInfo::new_localhost(&BvmAddr::new_rand(), 0);
        ci.wallclock = 0;
        let value = ContInfTblValue::ContactInfo(ci.clone());
        let label = value.label();
        // push a new message
        assert_eq!(
            push.process_push_message(&mut contact_info_table, value.clone(), 0),
            Ok(None)
        );
        assert_eq!(contact_info_table.lookup(&label), Some(&value));

        // push it again
        assert_eq!(
            push.process_push_message(&mut contact_info_table, value.clone(), 0),
            Err(NodeTbleErr::PushMessagePrune)
        );

        // purge the old pushed
        push.purge_old_pushed_once_messages(1);

        // push it again
        assert_eq!(
            push.process_push_message(&mut contact_info_table, value.clone(), 0),
            Err(NodeTbleErr::PushMessageOldVersion)
        );
    }
}
