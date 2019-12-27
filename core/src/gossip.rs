//! ContactInfoTable Gossip
//! This module ties together ContactInfoTable and the push and pull gossip overlays.  The interface is
//! designed to run with a simulator or over a UDP network connection with messages up to a
//! packet::BLOB_DATA_SIZE size.

use crate::connection_info_table::ContactInfoTable;
use crate::ndtb_err::NodeTbleErr;
use crate::pull_from_gossip::NodeTbleGspPull;
use crate::push_to_gossip::NodeTbleGspPush;
use crate::propagation_value::ContInfTblValue;
use crate::bvm_types::*;
use hashbrown::HashMap;
use morgan_runtime::bloom::Bloom;
use morgan_interface::hash::Hash;
use morgan_interface::bvm_address::BvmAddr;
use std::{thread, time::Duration};



#[derive(Clone)]
pub struct NodeTbleGossip {
    pub contact_info_table: ContactInfoTable,
    pub id: BvmAddr,
    pub push: NodeTbleGspPush,
    pub pull: NodeTbleGspPull,
}

impl Default for NodeTbleGossip {
    fn default() -> Self {
        NodeTbleGossip {
            contact_info_table: ContactInfoTable::default(),
            id: BvmAddr::default(),
            push: NodeTbleGspPush::default(),
            pull: NodeTbleGspPull::default(),
        }
    }
}

impl NodeTbleGossip {
    pub fn set_self(&mut self, id: &BvmAddr) {
        self.id = *id;
    }
    /// process a push message to the network
    pub fn process_push_message(&mut self, values: Vec<ContInfTblValue>, now: u64) -> Vec<BvmAddr> {
        let labels: Vec<_> = values.iter().map(ContInfTblValue::label).collect();

        let results: Vec<_> = values
            .into_iter()
            .map(|val| self.push.process_push_message(&mut self.contact_info_table, val, now))
            .collect();

        results
            .into_iter()
            .zip(labels)
            .filter_map(|(r, d)| {
                if r == Err(NodeTbleErr::PushMessagePrune) {
                    Some(d.address())
                } else if let Ok(Some(val)) = r {
                    self.pull
                        .record_old_hash(val.value_hash, val.local_timestamp);
                    None
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn new_push_messages(&mut self, now: u64) -> (BvmAddr, Vec<BvmAddr>, Vec<ContInfTblValue>) {
        let (peers, values) = self.push.new_push_messages(&self.contact_info_table, now);
        (self.id, peers, values)
    }

    /// add the `from` to the peer's filter of nodes
    pub fn process_prune_msg(
        &mut self,
        peer: &BvmAddr,
        destination: &BvmAddr,
        origin: &[BvmAddr],
        wallclock: u64,
        now: u64,
    ) -> Result<(), NodeTbleErr> {
        let expired = now > wallclock + self.push.prune_timeout;
        if expired {
            return Err(NodeTbleErr::PruneMessageTimeout);
        }
        if self.id == *destination {
            self.push.process_prune_msg(peer, origin);
            Ok(())
        } else {
            Err(NodeTbleErr::BadPruneDestination)
        }
    }

    /// refresh the push active set
    /// * ratio - number of actives to rotate
    pub fn refresh_push_active_set(&mut self, stakes: &HashMap<BvmAddr, u64>) {
        self.push.refresh_push_active_set(
            &self.contact_info_table,
            stakes,
            &self.id,
            self.pull.pull_request_time.len(),
            NDTB_GOSSIP_NUM_ACTIVE,
        )
    }

    /// generate a random request
    pub fn new_pull_request(
        &self,
        now: u64,
        stakes: &HashMap<BvmAddr, u64>,
    ) -> Result<(BvmAddr, Bloom<Hash>, ContInfTblValue), NodeTbleErr> {
        self.pull
            .new_pull_request(&self.contact_info_table, &self.id, now, stakes)
    }

    /// time when a request to `from` was initiated
    /// This is used for weighted random selection during `new_pull_request`
    /// It's important to use the local nodes request creation time as the weight
    /// instead of the response received time otherwise failed nodes will increase their weight.
    pub fn mark_pull_request_creation_time(&mut self, from: &BvmAddr, now: u64) {
        self.pull.mark_pull_request_creation_time(from, now)
    }
    /// process a pull request and create a response
    pub fn process_pull_request(
        &mut self,
        caller: ContInfTblValue,
        filter: Bloom<Hash>,
        now: u64,
    ) -> Vec<ContInfTblValue> {
        self.pull
            .process_pull_request(&mut self.contact_info_table, caller, filter, now)
    }
    /// process a pull response
    pub fn process_pull_response(
        &mut self,
        from: &BvmAddr,
        response: Vec<ContInfTblValue>,
        now: u64,
    ) -> usize {
        self.pull
            .process_pull_response(&mut self.contact_info_table, from, response, now)
    }
    pub fn purge(&mut self, now: u64) {
        if now > self.push.msg_timeout {
            let min = now - self.push.msg_timeout;
            self.push.purge_old_pending_push_messages(&self.contact_info_table, min);
        }
        if now > 5 * self.push.msg_timeout {
            let min = now - 5 * self.push.msg_timeout;
            self.push.purge_old_pushed_once_messages(min);
        }
        if now > self.pull.ndtb_timeout {
            let min = now - self.pull.ndtb_timeout;
            self.pull.purge_active(&mut self.contact_info_table, &self.id, min);
        }
        if now > 5 * self.pull.ndtb_timeout {
            let min = now - 5 * self.pull.ndtb_timeout;
            self.pull.purge_purged(min);
        }
    }
}

/// Given an operation retries it successfully sleeping everytime it fails
/// If the operation succeeds before the iterator runs out, it returns success
pub fn retry<I, O, T, E>(iterable: I, mut operation: O) -> Result<T, E>
where
    I: IntoIterator<Item = Duration>,
    O: FnMut() -> Result<T, E>,
{
    let mut iterator = iterable.into_iter();
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(err) => {
                if let Some(delay) = iterator.next() {
                    thread::sleep(delay);
                } else {
                    return Err(err);
                }
            }
        }
    }
}

pub fn fixed_retry_strategy(delay_ms: u64, tries: usize) -> impl Iterator<Item = Duration> {
    FixedDelay::new(delay_ms).take(tries)
}

/// An iterator which uses a fixed delay
pub struct FixedDelay {
    duration: Duration,
}

impl FixedDelay {
    /// Create a new `FixedDelay` using the given duration in milliseconds.
    fn new(millis: u64) -> Self {
        FixedDelay {
            duration: Duration::from_millis(millis),
        }
    }
}

impl Iterator for FixedDelay {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        Some(self.duration)
    }
}

/// Computes a normalized(log of actual stake) stake
pub fn get_stake<S: std::hash::BuildHasher>(id: &BvmAddr, stakes: &HashMap<BvmAddr, u64, S>) -> f32 {
    // cap the max balance to u32 max (it should be plenty)
    let bal = f64::from(u32::max_value()).min(*stakes.get(id).unwrap_or(&0) as f64);
    1_f32.max((bal as f32).ln())
}

/// Computes bounded weight given some max, a time since last selected, and a stake value
/// The minimum stake is 1 and not 0 to allow 'time since last' picked to factor in.
pub fn get_weight(max_weight: f32, time_since_last_selected: u32, stake: f32) -> f32 {
    let mut weight = time_since_last_selected as f32 * stake;
    if weight.is_infinite() {
        weight = max_weight;
    }
    1.0_f32.max(weight.min(max_weight))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::connection_info::ContactInfo;
    use morgan_interface::hash::hash;
    use morgan_interface::timing::timestamp;

    #[test]
    fn test_prune_errors() {
        let mut node_table_gossip = NodeTbleGossip::default();
        node_table_gossip.id = BvmAddr::new(&[0; 32]);
        let id = node_table_gossip.id;
        let ci = ContactInfo::new_localhost(&BvmAddr::new(&[1; 32]), 0);
        let prune_address = BvmAddr::new(&[2; 32]);
        node_table_gossip
            .contact_info_table
            .insert(ContInfTblValue::ContactInfo(ci.clone()), 0)
            .unwrap();
        node_table_gossip.refresh_push_active_set(&HashMap::new());
        let now = timestamp();
        //incorrect dest
        let mut res = node_table_gossip.process_prune_msg(
            &ci.id,
            &BvmAddr::new(hash(&[1; 32]).as_ref()),
            &[prune_address],
            now,
            now,
        );
        assert_eq!(res.err(), Some(NodeTbleErr::BadPruneDestination));
        //correct dest
        res = node_table_gossip.process_prune_msg(&ci.id, &id, &[prune_address], now, now);
        res.unwrap();
        //test timeout
        let timeout = now + node_table_gossip.push.prune_timeout * 2;
        res = node_table_gossip.process_prune_msg(&ci.id, &id, &[prune_address], now, timeout);
        assert_eq!(res.err(), Some(NodeTbleErr::PruneMessageTimeout));
    }
}
