//! Crds Gossip
//! This module ties together Crds and the push and pull gossip overlays.  The interface is
//! designed to run with a simulator or over a UDP network connection with messages up to a
//! packet::BLOB_DATA_SIZE size.

use crate::connection_info_table::Crds;
use crate::gossip_error_type::CrdsGossipError;
use crate::pull_from_gossip::CrdsGossipPull;
use crate::push_to_gossip::{CrdsGossipPush, CRDS_GOSSIP_NUM_ACTIVE};
use crate::propagation_value::CrdsValue;
use hashbrown::HashMap;
use morgan_runtime::bloom::Bloom;
use morgan_interface::hash::Hash;
use morgan_interface::pubkey::Pubkey;
use std::{thread, time::Duration};

///The min size for bloom filters
pub const CRDS_GOSSIP_BLOOM_SIZE: usize = 1000;

#[derive(Clone)]
pub struct CrdsGossip {
    pub crds: Crds,
    pub id: Pubkey,
    pub push: CrdsGossipPush,
    pub pull: CrdsGossipPull,
}

impl Default for CrdsGossip {
    fn default() -> Self {
        CrdsGossip {
            crds: Crds::default(),
            id: Pubkey::default(),
            push: CrdsGossipPush::default(),
            pull: CrdsGossipPull::default(),
        }
    }
}

impl CrdsGossip {
    pub fn set_self(&mut self, id: &Pubkey) {
        self.id = *id;
    }
    /// process a push message to the network
    pub fn process_push_message(&mut self, values: Vec<CrdsValue>, now: u64) -> Vec<Pubkey> {
        let labels: Vec<_> = values.iter().map(CrdsValue::label).collect();

        let results: Vec<_> = values
            .into_iter()
            .map(|val| self.push.process_push_message(&mut self.crds, val, now))
            .collect();

        results
            .into_iter()
            .zip(labels)
            .filter_map(|(r, d)| {
                if r == Err(CrdsGossipError::PushMessagePrune) {
                    Some(d.pubkey())
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

    pub fn new_push_messages(&mut self, now: u64) -> (Pubkey, Vec<Pubkey>, Vec<CrdsValue>) {
        let (peers, values) = self.push.new_push_messages(&self.crds, now);
        (self.id, peers, values)
    }

    /// add the `from` to the peer's filter of nodes
    pub fn process_prune_msg(
        &mut self,
        peer: &Pubkey,
        destination: &Pubkey,
        origin: &[Pubkey],
        wallclock: u64,
        now: u64,
    ) -> Result<(), CrdsGossipError> {
        let expired = now > wallclock + self.push.prune_timeout;
        if expired {
            return Err(CrdsGossipError::PruneMessageTimeout);
        }
        if self.id == *destination {
            self.push.process_prune_msg(peer, origin);
            Ok(())
        } else {
            Err(CrdsGossipError::BadPruneDestination)
        }
    }

    /// refresh the push active set
    /// * ratio - number of actives to rotate
    pub fn refresh_push_active_set(&mut self, stakes: &HashMap<Pubkey, u64>) {
        self.push.refresh_push_active_set(
            &self.crds,
            stakes,
            &self.id,
            self.pull.pull_request_time.len(),
            CRDS_GOSSIP_NUM_ACTIVE,
        )
    }

    /// generate a random request
    pub fn new_pull_request(
        &self,
        now: u64,
        stakes: &HashMap<Pubkey, u64>,
    ) -> Result<(Pubkey, Bloom<Hash>, CrdsValue), CrdsGossipError> {
        self.pull
            .new_pull_request(&self.crds, &self.id, now, stakes)
    }

    /// time when a request to `from` was initiated
    /// This is used for weighted random selection during `new_pull_request`
    /// It's important to use the local nodes request creation time as the weight
    /// instead of the response received time otherwise failed nodes will increase their weight.
    pub fn mark_pull_request_creation_time(&mut self, from: &Pubkey, now: u64) {
        self.pull.mark_pull_request_creation_time(from, now)
    }
    /// process a pull request and create a response
    pub fn process_pull_request(
        &mut self,
        caller: CrdsValue,
        filter: Bloom<Hash>,
        now: u64,
    ) -> Vec<CrdsValue> {
        self.pull
            .process_pull_request(&mut self.crds, caller, filter, now)
    }
    /// process a pull response
    pub fn process_pull_response(
        &mut self,
        from: &Pubkey,
        response: Vec<CrdsValue>,
        now: u64,
    ) -> usize {
        self.pull
            .process_pull_response(&mut self.crds, from, response, now)
    }
    pub fn purge(&mut self, now: u64) {
        if now > self.push.msg_timeout {
            let min = now - self.push.msg_timeout;
            self.push.purge_old_pending_push_messages(&self.crds, min);
        }
        if now > 5 * self.push.msg_timeout {
            let min = now - 5 * self.push.msg_timeout;
            self.push.purge_old_pushed_once_messages(min);
        }
        if now > self.pull.crds_timeout {
            let min = now - self.pull.crds_timeout;
            self.pull.purge_active(&mut self.crds, &self.id, min);
        }
        if now > 5 * self.pull.crds_timeout {
            let min = now - 5 * self.pull.crds_timeout;
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
pub fn get_stake<S: std::hash::BuildHasher>(id: &Pubkey, stakes: &HashMap<Pubkey, u64, S>) -> f32 {
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
        let mut crds_gossip = CrdsGossip::default();
        crds_gossip.id = Pubkey::new(&[0; 32]);
        let id = crds_gossip.id;
        let ci = ContactInfo::new_localhost(&Pubkey::new(&[1; 32]), 0);
        let prune_pubkey = Pubkey::new(&[2; 32]);
        crds_gossip
            .crds
            .insert(CrdsValue::ContactInfo(ci.clone()), 0)
            .unwrap();
        crds_gossip.refresh_push_active_set(&HashMap::new());
        let now = timestamp();
        //incorrect dest
        let mut res = crds_gossip.process_prune_msg(
            &ci.id,
            &Pubkey::new(hash(&[1; 32]).as_ref()),
            &[prune_pubkey],
            now,
            now,
        );
        assert_eq!(res.err(), Some(CrdsGossipError::BadPruneDestination));
        //correct dest
        res = crds_gossip.process_prune_msg(&ci.id, &id, &[prune_pubkey], now, now);
        res.unwrap();
        //test timeout
        let timeout = now + crds_gossip.push.prune_timeout * 2;
        res = crds_gossip.process_prune_msg(&ci.id, &id, &[prune_pubkey], now, timeout);
        assert_eq!(res.err(), Some(CrdsGossipError::PruneMessageTimeout));
    }
}
