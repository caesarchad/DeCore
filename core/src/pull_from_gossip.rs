//! ContactInfoTable Gossip Pull overlay
//! This module implements the anti-entropy protocol for the network.
//!
//! The basic strategy is as follows:
//! 1. Construct a bloom filter of the local data set
//! 2. Randomly ask a node on the network for data that is not contained in the bloom filter.
//!
//! Bloom filters have a false positive rate.  Each requests uses a different bloom filter
//! with random hash functions.  So each subsequent request will have a different distribution
//! of false positives.

use crate::connection_info::ContactInfo;
use crate::connection_info_table::ContactInfoTable;
use crate::gossip::{get_stake, get_weight, NDTB_GOSSIP_BLOOM_SIZE};
use crate::gossip_error_type::NodeTbleErr;
use crate::propagation_value::{ContInfTblValue, ContInfTblValueTag};
use crate::packet::BLOB_DATA_SIZE;
use bincode::serialized_size;
use hashbrown::HashMap;
use rand;
use rand::distributions::{Distribution, WeightedIndex};
use morgan_runtime::bloom::Bloom;
use morgan_interface::hash::Hash;
use morgan_interface::pubkey::Pubkey;
use std::cmp;
use std::collections::VecDeque;

pub const NDTB_GOSSIP_PULL_CRDS_TIMEOUT_MS: u64 = 15000;

#[derive(Clone)]
pub struct NodeTbleGspPull {
    /// timestamp of last request
    pub pull_request_time: HashMap<Pubkey, u64>,
    /// hash and insert time
    purged_values: VecDeque<(Hash, u64)>,
    /// max bytes per message
    pub max_bytes: usize,
    pub ndtb_timeout: u64,
}

impl Default for NodeTbleGspPull {
    fn default() -> Self {
        Self {
            purged_values: VecDeque::new(),
            pull_request_time: HashMap::new(),
            max_bytes: BLOB_DATA_SIZE,
            ndtb_timeout: NDTB_GOSSIP_PULL_CRDS_TIMEOUT_MS,
        }
    }
}
impl NodeTbleGspPull {
    /// generate a random request
    pub fn new_pull_request(
        &self,
        contact_info_table: &ContactInfoTable,
        self_id: &Pubkey,
        now: u64,
        stakes: &HashMap<Pubkey, u64>,
    ) -> Result<(Pubkey, Bloom<Hash>, ContInfTblValue), NodeTbleErr> {
        let options = self.pull_options(contact_info_table, &self_id, now, stakes);
        if options.is_empty() {
            return Err(NodeTbleErr::NoPeers);
        }
        let filter = self.build_crds_filter(contact_info_table);
        let index = WeightedIndex::new(options.iter().map(|weighted| weighted.0)).unwrap();
        let random = index.sample(&mut rand::thread_rng());
        let self_info = contact_info_table
            .lookup(&ContInfTblValueTag::ContactInfo(*self_id))
            .unwrap_or_else(|| panic!("self_id invalid {}", self_id));
        Ok((options[random].1.id, filter, self_info.clone()))
    }

    fn pull_options<'a>(
        &self,
        contact_info_table: &'a ContactInfoTable,
        self_id: &Pubkey,
        now: u64,
        stakes: &HashMap<Pubkey, u64>,
    ) -> Vec<(f32, &'a ContactInfo)> {
        contact_info_table.table
            .values()
            .filter_map(|v| v.value.contact_info())
            .filter(|v| v.id != *self_id && ContactInfo::is_valid_address(&v.gossip))
            .map(|item| {
                let max_weight = f32::from(u16::max_value()) - 1.0;
                let req_time: u64 = *self.pull_request_time.get(&item.id).unwrap_or(&0);
                let since = ((now - req_time) / 1024) as u32;
                let stake = get_stake(&item.id, stakes);
                let weight = get_weight(max_weight, since, stake);
                (weight, item)
            })
            .collect()
    }

    /// time when a request to `from` was initiated
    /// This is used for weighted random selection during `new_pull_request`
    /// It's important to use the local nodes request creation time as the weight
    /// instead of the response received time otherwise failed nodes will increase their weight.
    pub fn mark_pull_request_creation_time(&mut self, from: &Pubkey, now: u64) {
        self.pull_request_time.insert(*from, now);
    }

    /// Store an old hash in the purged values set
    pub fn record_old_hash(&mut self, hash: Hash, timestamp: u64) {
        self.purged_values.push_back((hash, timestamp))
    }

    /// process a pull request and create a response
    pub fn process_pull_request(
        &mut self,
        contact_info_table: &mut ContactInfoTable,
        caller: ContInfTblValue,
        mut filter: Bloom<Hash>,
        now: u64,
    ) -> Vec<ContInfTblValue> {
        let rv = self.filter_crds_values(contact_info_table, &mut filter);
        let key = caller.label().pubkey();
        let old = contact_info_table.insert(caller, now);
        if let Some(val) = old.ok().and_then(|opt| opt) {
            self.purged_values
                .push_back((val.value_hash, val.local_timestamp))
        }
        contact_info_table.update_record_timestamp(&key, now);
        rv
    }
    /// process a pull response
    pub fn process_pull_response(
        &mut self,
        contact_info_table: &mut ContactInfoTable,
        from: &Pubkey,
        response: Vec<ContInfTblValue>,
        now: u64,
    ) -> usize {
        let mut failed = 0;
        for r in response {
            let owner = r.label().pubkey();
            let old = contact_info_table.insert(r, now);
            failed += old.is_err() as usize;
            old.ok().map(|opt| {
                contact_info_table.update_record_timestamp(&owner, now);
                opt.map(|val| {
                    self.purged_values
                        .push_back((val.value_hash, val.local_timestamp))
                })
            });
        }
        contact_info_table.update_record_timestamp(from, now);
        failed
    }
    /// build a filter of the current contact_info_table table
    pub fn build_crds_filter(&self, contact_info_table: &ContactInfoTable) -> Bloom<Hash> {
        let num = cmp::max(
            NDTB_GOSSIP_BLOOM_SIZE,
            contact_info_table.table.values().count() + self.purged_values.len(),
        );
        let mut bloom = Bloom::random(num, 0.1, 4 * 1024 * 8 - 1);
        for v in contact_info_table.table.values() {
            bloom.add(&v.value_hash);
        }
        for (value_hash, _insert_timestamp) in &self.purged_values {
            bloom.add(value_hash);
        }
        bloom
    }
    /// filter values that fail the bloom filter up to max_bytes
    fn filter_crds_values(&self, contact_info_table: &ContactInfoTable, filter: &mut Bloom<Hash>) -> Vec<ContInfTblValue> {
        let mut max_bytes = self.max_bytes as isize;
        let mut ret = vec![];
        for v in contact_info_table.table.values() {
            if filter.contains(&v.value_hash) {
                continue;
            }
            max_bytes -= serialized_size(&v.value).unwrap() as isize;
            if max_bytes < 0 {
                break;
            }
            ret.push(v.value.clone());
        }
        ret
    }
    /// Purge values from the contact_info_table that are older then `active_timeout`
    /// The value_hash of an active item is put into self.purged_values queue
    pub fn purge_active(&mut self, contact_info_table: &mut ContactInfoTable, self_id: &Pubkey, min_ts: u64) {
        let old = contact_info_table.find_old_labels(min_ts);
        let mut purged: VecDeque<_> = old
            .iter()
            .filter(|label| label.pubkey() != *self_id)
            .filter_map(|label| {
                let rv = contact_info_table
                    .lookup_versioned(label)
                    .map(|val| (val.value_hash, val.local_timestamp));
                contact_info_table.remove(label);
                rv
            })
            .collect();
        self.purged_values.append(&mut purged);
    }
    /// Purge values from the `self.purged_values` queue that are older then purge_timeout
    pub fn purge_purged(&mut self, min_ts: u64) {
        let cnt = self
            .purged_values
            .iter()
            .take_while(|v| v.1 < min_ts)
            .count();
        self.purged_values.drain(..cnt);
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::connection_info::ContactInfo;

    #[test]
    fn test_new_pull_with_stakes() {
        let mut contact_info_table = ContactInfoTable::default();
        let mut stakes = HashMap::new();
        let node = NodeTbleGspPull::default();
        let me = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        contact_info_table.insert(me.clone(), 0).unwrap();
        for i in 1..=30 {
            let entry = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
            let id = entry.label().pubkey();
            contact_info_table.insert(entry.clone(), 0).unwrap();
            stakes.insert(id, i * 100);
        }
        let now = 1024;
        let mut options = node.pull_options(&contact_info_table, &me.label().pubkey(), now, &stakes);
        assert!(!options.is_empty());
        options.sort_by(|(weight_l, _), (weight_r, _)| weight_r.partial_cmp(weight_l).unwrap());
        // check that the highest stake holder is also the heaviest weighted.
        assert_eq!(
            *stakes.get(&options.get(0).unwrap().1.id).unwrap(),
            3000_u64
        );
    }

    #[test]
    fn test_new_pull_request() {
        let mut contact_info_table = ContactInfoTable::default();
        let entry = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        let id = entry.label().pubkey();
        let node = NodeTbleGspPull::default();
        assert_eq!(
            node.new_pull_request(&contact_info_table, &id, 0, &HashMap::new()),
            Err(NodeTbleErr::NoPeers)
        );

        contact_info_table.insert(entry.clone(), 0).unwrap();
        assert_eq!(
            node.new_pull_request(&contact_info_table, &id, 0, &HashMap::new()),
            Err(NodeTbleErr::NoPeers)
        );

        let new = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        contact_info_table.insert(new.clone(), 0).unwrap();
        let req = node.new_pull_request(&contact_info_table, &id, 0, &HashMap::new());
        let (to, _, self_info) = req.unwrap();
        assert_eq!(to, new.label().pubkey());
        assert_eq!(self_info, entry);
    }

    #[test]
    fn test_new_mark_creation_time() {
        let mut contact_info_table = ContactInfoTable::default();
        let entry = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        let node_pubkey = entry.label().pubkey();
        let mut node = NodeTbleGspPull::default();
        contact_info_table.insert(entry.clone(), 0).unwrap();
        let old = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        contact_info_table.insert(old.clone(), 0).unwrap();
        let new = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        contact_info_table.insert(new.clone(), 0).unwrap();

        // set request creation time to max_value
        node.mark_pull_request_creation_time(&new.label().pubkey(), u64::max_value());

        // odds of getting the other request should be 1 in u64::max_value()
        for _ in 0..10 {
            let req = node.new_pull_request(&contact_info_table, &node_pubkey, u64::max_value(), &HashMap::new());
            let (to, _, self_info) = req.unwrap();
            assert_eq!(to, old.label().pubkey());
            assert_eq!(self_info, entry);
        }
    }

    #[test]
    fn test_process_pull_request() {
        let mut node_contact_table = ContactInfoTable::default();
        let entry = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        let node_pubkey = entry.label().pubkey();
        let node = NodeTbleGspPull::default();
        node_contact_table.insert(entry.clone(), 0).unwrap();
        let new = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        node_contact_table.insert(new.clone(), 0).unwrap();
        let req = node.new_pull_request(&node_contact_table, &node_pubkey, 0, &HashMap::new());

        let mut dest_crds = ContactInfoTable::default();
        let mut dest = NodeTbleGspPull::default();
        let (_, filter, caller) = req.unwrap();
        let rsp = dest.process_pull_request(&mut dest_crds, caller.clone(), filter, 1);
        assert!(rsp.is_empty());
        assert!(dest_crds.lookup(&caller.label()).is_some());
        assert_eq!(
            dest_crds
                .lookup_versioned(&caller.label())
                .unwrap()
                .insert_timestamp,
            1
        );
        assert_eq!(
            dest_crds
                .lookup_versioned(&caller.label())
                .unwrap()
                .local_timestamp,
            1
        );
    }
    #[test]
    fn test_process_pull_request_response() {
        let mut node_contact_table = ContactInfoTable::default();
        let entry = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        let node_pubkey = entry.label().pubkey();
        let mut node = NodeTbleGspPull::default();
        node_contact_table.insert(entry.clone(), 0).unwrap();

        let new = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        node_contact_table.insert(new.clone(), 0).unwrap();

        let mut dest = NodeTbleGspPull::default();
        let mut dest_crds = ContactInfoTable::default();
        let new_id = Pubkey::new_rand();
        let new = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&new_id, 1));
        dest_crds.insert(new.clone(), 0).unwrap();

        // node contains a key from the dest node, but at an older local timestamp
        let same_key = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&new_id, 0));
        assert_eq!(same_key.label(), new.label());
        assert!(same_key.wallclock() < new.wallclock());
        node_contact_table.insert(same_key.clone(), 0).unwrap();
        assert_eq!(
            node_contact_table
                .lookup_versioned(&same_key.label())
                .unwrap()
                .local_timestamp,
            0
        );
        let mut done = false;
        for _ in 0..30 {
            // there is a chance of a false positive with bloom filters
            let req = node.new_pull_request(&node_contact_table, &node_pubkey, 0, &HashMap::new());
            let (_, filter, caller) = req.unwrap();
            let rsp = dest.process_pull_request(&mut dest_crds, caller, filter, 0);
            // if there is a false positive this is empty
            // prob should be around 0.1 per iteration
            if rsp.is_empty() {
                continue;
            }

            assert_eq!(rsp.len(), 1);
            let failed = node.process_pull_response(&mut node_contact_table, &node_pubkey, rsp, 1);
            assert_eq!(failed, 0);
            assert_eq!(
                node_contact_table
                    .lookup_versioned(&new.label())
                    .unwrap()
                    .local_timestamp,
                1
            );
            // verify that the whole record was updated for dest since this is a response from dest
            assert_eq!(
                node_contact_table
                    .lookup_versioned(&same_key.label())
                    .unwrap()
                    .local_timestamp,
                1
            );
            done = true;
            break;
        }
        assert!(done);
    }
    #[test]
    fn test_gossip_purge() {
        let mut node_contact_table = ContactInfoTable::default();
        let entry = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        let node_label = entry.label();
        let node_pubkey = node_label.pubkey();
        let mut node = NodeTbleGspPull::default();
        node_contact_table.insert(entry.clone(), 0).unwrap();
        let old = ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&Pubkey::new_rand(), 0));
        node_contact_table.insert(old.clone(), 0).unwrap();
        let value_hash = node_contact_table.lookup_versioned(&old.label()).unwrap().value_hash;

        //verify self is valid
        assert_eq!(node_contact_table.lookup(&node_label).unwrap().label(), node_label);

        // purge
        node.purge_active(&mut node_contact_table, &node_pubkey, 1);

        //verify self is still valid after purge
        assert_eq!(node_contact_table.lookup(&node_label).unwrap().label(), node_label);

        assert_eq!(node_contact_table.lookup_versioned(&old.label()), None);
        assert_eq!(node.purged_values.len(), 1);
        for _ in 0..30 {
            // there is a chance of a false positive with bloom filters
            // assert that purged value is still in the set
            // chance of 30 consecutive false positives is 0.1^30
            let filter = node.build_crds_filter(&node_contact_table);
            assert!(filter.contains(&value_hash));
        }

        // purge the value
        node.purge_purged(1);
        assert_eq!(node.purged_values.len(), 0);
    }
}
