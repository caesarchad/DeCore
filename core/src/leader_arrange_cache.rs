use crate::block_buffer_pool::BlockBufferPool;
use crate::leader_arrange::LeaderSchedule;
use crate::leader_arrange_utils;
use morgan_runtime::treasury::Bank;
use morgan_runtime::epoch_schedule::EpochSchedule;
use morgan_interface::pubkey::Pubkey;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

type CachedSchedules = (HashMap<u64, Arc<LeaderSchedule>>, VecDeque<u64>);
const MAX_SCHEDULES: usize = 10;

#[derive(Default)]
pub struct LeaderScheduleCache {
    // Map from an epoch to a leader schedule for that epoch
    pub cached_schedules: RwLock<CachedSchedules>,
    epoch_schedule: EpochSchedule,
    max_epoch: RwLock<u64>,
}

impl LeaderScheduleCache {
    pub fn new_from_bank(treasury: &Bank) -> Self {
        Self::new(*treasury.epoch_schedule(), treasury.slot())
    }

    pub fn new(epoch_schedule: EpochSchedule, root: u64) -> Self {
        let cache = Self {
            cached_schedules: RwLock::new((HashMap::new(), VecDeque::new())),
            epoch_schedule,
            max_epoch: RwLock::new(0),
        };

        cache.set_genesis(root);
        cache
    }

    pub fn set_genesis(&self, root: u64) {
        *self.max_epoch.write().unwrap() = self.epoch_schedule.get_stakers_epoch(root);
    }

    pub fn slot_leader_at(&self, slot: u64, treasury: Option<&Bank>) -> Option<Pubkey> {
        if let Some(treasury) = treasury {
            self.slot_leader_at_else_compute(slot, treasury)
        } else {
            self.slot_leader_at_no_compute(slot)
        }
    }

    /// Return the next slot after the given current_slot that the given node will be leader
    pub fn next_leader_slot(
        &self,
        pubkey: &Pubkey,
        mut current_slot: u64,
        treasury: &Bank,
        block_buffer_pool: Option<&BlockBufferPool>,
    ) -> Option<u64> {
        let (mut epoch, mut start_index) = treasury.get_epoch_and_slot_index(current_slot + 1);
        while let Some(leader_schedule) = self.get_epoch_schedule_else_compute(epoch, treasury) {
            // clippy thinks I should do this:
            //  for (i, <item>) in leader_schedule
            //                           .iter()
            //                           .enumerate()
            //                           .take(treasury.get_slots_in_epoch(epoch))
            //                           .skip(from_slot_index + 1) {
            //
            //  but leader_schedule doesn't implement Iter...
            #[allow(clippy::needless_range_loop)]
            for i in start_index..treasury.get_slots_in_epoch(epoch) {
                current_slot += 1;
                if *pubkey == leader_schedule[i] {
                    if let Some(block_buffer_pool) = block_buffer_pool {
                        if let Some(meta) = block_buffer_pool.meta(current_slot).unwrap() {
                            // We have already sent a blob for this slot, so skip it
                            if meta.received > 0 {
                                continue;
                            }
                        }
                    }

                    return Some(current_slot);
                }
            }

            epoch += 1;
            start_index = 0;
        }
        None
    }

    fn slot_leader_at_no_compute(&self, slot: u64) -> Option<Pubkey> {
        let (epoch, slot_index) = self.epoch_schedule.get_epoch_and_slot_index(slot);
        self.cached_schedules
            .read()
            .unwrap()
            .0
            .get(&epoch)
            .map(|schedule| schedule[slot_index])
    }

    fn slot_leader_at_else_compute(&self, slot: u64, treasury: &Bank) -> Option<Pubkey> {
        let cache_result = self.slot_leader_at_no_compute(slot);
        // Forbid asking for slots in an unconfirmed epoch
        let bank_epoch = self.epoch_schedule.get_epoch_and_slot_index(slot).0;
        if bank_epoch > *self.max_epoch.read().unwrap() {
            debug!(
                "Requested leader in slot: {} of unconfirmed epoch: {}",
                slot, bank_epoch
            );
            return None;
        }
        if cache_result.is_some() {
            cache_result
        } else {
            let (epoch, slot_index) = treasury.get_epoch_and_slot_index(slot);
            if let Some(epoch_schedule) = self.compute_epoch_schedule(epoch, treasury) {
                Some(epoch_schedule[slot_index])
            } else {
                None
            }
        }
    }

    fn get_epoch_schedule_else_compute(
        &self,
        epoch: u64,
        treasury: &Bank,
    ) -> Option<Arc<LeaderSchedule>> {
        let epoch_schedule = self.cached_schedules.read().unwrap().0.get(&epoch).cloned();

        if epoch_schedule.is_some() {
            epoch_schedule
        } else if let Some(epoch_schedule) = self.compute_epoch_schedule(epoch, treasury) {
            Some(epoch_schedule)
        } else {
            None
        }
    }

    fn compute_epoch_schedule(&self, epoch: u64, treasury: &Bank) -> Option<Arc<LeaderSchedule>> {
        let leader_schedule = leader_arrange_utils::leader_schedule(epoch, treasury);
        leader_schedule.map(|leader_schedule| {
            let leader_schedule = Arc::new(leader_schedule);
            let (ref mut cached_schedules, ref mut order) = *self.cached_schedules.write().unwrap();
            // Check to see if schedule exists in case somebody already inserted in the time we were
            // waiting for the lock
            let entry = cached_schedules.entry(epoch);
            if let Entry::Vacant(v) = entry {
                v.insert(leader_schedule.clone());
                order.push_back(epoch);
                Self::retain_latest(cached_schedules, order);
            }
            leader_schedule
        })
    }

    fn retain_latest(schedules: &mut HashMap<u64, Arc<LeaderSchedule>>, order: &mut VecDeque<u64>) {
        if schedules.len() > MAX_SCHEDULES {
            let first = order.pop_front().unwrap();
            schedules.remove(&first);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_buffer_pool::tests::make_slot_entries;
    use crate::genesis_utils::create_genesis_block;
    use crate::genesis_utils::{
        create_genesis_block_with_leader, GenesisBlockInfo, BOOTSTRAP_LEADER_DIFS,
    };
    use crate::staking_utils::tests::setup_vote_and_stake_accounts;
    use morgan_runtime::treasury::Bank;
    use morgan_runtime::epoch_schedule::{EpochSchedule, MINIMUM_SLOT_LENGTH};
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::thread::Builder;

    use crate::block_buffer_pool::get_tmp_ledger_path;

    #[test]
    fn test_slot_leader_at() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
        let treasury = Bank::new(&genesis_block);
        let cache = LeaderScheduleCache::new_from_bank(&treasury);

        // Nothing in the cache, should return None
        assert!(cache.slot_leader_at(treasury.slot(), None).is_none());

        // Add something to the cache
        assert!(cache.slot_leader_at(treasury.slot(), Some(&treasury)).is_some());
        assert!(cache.slot_leader_at(treasury.slot(), None).is_some());
        assert_eq!(cache.cached_schedules.read().unwrap().0.len(), 1);
    }

    #[test]
    fn test_retain_latest() {
        let mut cached_schedules = HashMap::new();
        let mut order = VecDeque::new();
        for i in 0..=MAX_SCHEDULES {
            cached_schedules.insert(i as u64, Arc::new(LeaderSchedule::default()));
            order.push_back(i as u64);
        }
        LeaderScheduleCache::retain_latest(&mut cached_schedules, &mut order);
        assert_eq!(cached_schedules.len(), MAX_SCHEDULES);
        let mut keys: Vec<_> = cached_schedules.keys().cloned().collect();
        keys.sort();
        let expected: Vec<_> = (1..=MAX_SCHEDULES as u64).collect();
        let expected_order: VecDeque<_> = (1..=MAX_SCHEDULES as u64).collect();
        assert_eq!(expected, keys);
        assert_eq!(expected_order, order);
    }

    #[test]
    fn test_thread_race_leader_schedule_cache() {
        let num_runs = 10;
        for _ in 0..num_runs {
            run_thread_race()
        }
    }

    fn run_thread_race() {
        let slots_per_epoch = MINIMUM_SLOT_LENGTH as u64;
        let epoch_schedule = EpochSchedule::new(slots_per_epoch, slots_per_epoch / 2, true);
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
        let treasury = Arc::new(Bank::new(&genesis_block));
        let cache = Arc::new(LeaderScheduleCache::new(epoch_schedule, treasury.slot()));

        let num_threads = 10;
        let (threads, senders): (Vec<_>, Vec<_>) = (0..num_threads)
            .map(|_| {
                let cache = cache.clone();
                let treasury = treasury.clone();
                let (sender, receiver) = channel();
                (
                    Builder::new()
                        .name("test_thread_race_leader_schedule_cache".to_string())
                        .spawn(move || {
                            let _ = receiver.recv();
                            cache.slot_leader_at(treasury.slot(), Some(&treasury));
                        })
                        .unwrap(),
                    sender,
                )
            })
            .unzip();

        for sender in &senders {
            sender.send(true).unwrap();
        }

        for t in threads.into_iter() {
            t.join().unwrap();
        }

        let (ref cached_schedules, ref order) = *cache.cached_schedules.read().unwrap();
        assert_eq!(cached_schedules.len(), 1);
        assert_eq!(order.len(), 1);
    }

    #[test]
    fn test_next_leader_slot() {
        let pubkey = Pubkey::new_rand();
        let mut genesis_block = create_genesis_block_with_leader(
            BOOTSTRAP_LEADER_DIFS,
            &pubkey,
            BOOTSTRAP_LEADER_DIFS,
        )
        .genesis_block;
        genesis_block.epoch_warmup = false;

        let treasury = Bank::new(&genesis_block);
        let cache = Arc::new(LeaderScheduleCache::new_from_bank(&treasury));

        assert_eq!(
            cache.slot_leader_at(treasury.slot(), Some(&treasury)).unwrap(),
            pubkey
        );
        assert_eq!(cache.next_leader_slot(&pubkey, 0, &treasury, None), Some(1));
        assert_eq!(cache.next_leader_slot(&pubkey, 1, &treasury, None), Some(2));
        assert_eq!(
            cache.next_leader_slot(
                &pubkey,
                2 * genesis_block.slots_per_epoch - 1, // no schedule generated for epoch 2
                &treasury,
                None
            ),
            None
        );

        assert_eq!(
            cache.next_leader_slot(
                &Pubkey::new_rand(), // not in leader_schedule
                0,
                &treasury,
                None
            ),
            None
        );
    }

    #[test]
    fn test_next_leader_slot_block_buffer() {
        let pubkey = Pubkey::new_rand();
        let mut genesis_block = create_genesis_block_with_leader(
            BOOTSTRAP_LEADER_DIFS,
            &pubkey,
            BOOTSTRAP_LEADER_DIFS,
        )
        .genesis_block;
        genesis_block.epoch_warmup = false;

        let treasury = Bank::new(&genesis_block);
        let cache = Arc::new(LeaderScheduleCache::new_from_bank(&treasury));
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = Arc::new(
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger"),
            );

            assert_eq!(
                cache.slot_leader_at(treasury.slot(), Some(&treasury)).unwrap(),
                pubkey
            );
            // Check that the next leader slot after 0 is slot 1
            assert_eq!(
                cache.next_leader_slot(&pubkey, 0, &treasury, Some(&block_buffer_pool)),
                Some(1)
            );

            // Write a blob into slot 2 that chains to slot 1,
            // but slot 1 is empty so should not be skipped
            let (blobs, _) = make_slot_entries(2, 1, 1);
            block_buffer_pool.update_blobs(&blobs[..]).unwrap();
            assert_eq!(
                cache.next_leader_slot(&pubkey, 0, &treasury, Some(&block_buffer_pool)),
                Some(1)
            );

            // Write a blob into slot 1
            let (blobs, _) = make_slot_entries(1, 0, 1);

            // Check that slot 1 and 2 are skipped
            block_buffer_pool.update_blobs(&blobs[..]).unwrap();
            assert_eq!(
                cache.next_leader_slot(&pubkey, 0, &treasury, Some(&block_buffer_pool)),
                Some(3)
            );

            // Integrity checks
            assert_eq!(
                cache.next_leader_slot(
                    &pubkey,
                    2 * genesis_block.slots_per_epoch - 1, // no schedule generated for epoch 2
                    &treasury,
                    Some(&block_buffer_pool)
                ),
                None
            );

            assert_eq!(
                cache.next_leader_slot(
                    &Pubkey::new_rand(), // not in leader_schedule
                    0,
                    &treasury,
                    Some(&block_buffer_pool)
                ),
                None
            );
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_next_leader_slot_next_epoch() {
        let GenesisBlockInfo {
            mut genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(10_000);
        genesis_block.epoch_warmup = false;

        let treasury = Bank::new(&genesis_block);
        let cache = Arc::new(LeaderScheduleCache::new_from_bank(&treasury));

        // Create new vote account
        let node_pubkey = Pubkey::new_rand();
        let vote_pubkey = Pubkey::new_rand();
        setup_vote_and_stake_accounts(
            &treasury,
            &mint_keypair,
            &vote_pubkey,
            &node_pubkey,
            BOOTSTRAP_LEADER_DIFS,
        );

        // Have to wait until the epoch at after the epoch stakes generated at genesis
        // for the new votes to take effect.
        let mut target_slot = 1;
        let epoch = treasury.get_stakers_epoch(0);
        while treasury.get_stakers_epoch(target_slot) == epoch {
            target_slot += 1;
        }

        let treasury = Bank::new_from_parent(&Arc::new(treasury), &Pubkey::default(), target_slot);
        let mut expected_slot = 0;
        let epoch = treasury.get_stakers_epoch(target_slot);
        for i in 0..epoch {
            expected_slot += treasury.get_slots_in_epoch(i);
        }

        let schedule = cache.compute_epoch_schedule(epoch, &treasury).unwrap();
        let mut index = 0;
        while schedule[index] != node_pubkey {
            index += 1;
            assert_ne!(index, genesis_block.slots_per_epoch);
        }
        expected_slot += index;

        assert_eq!(
            cache.next_leader_slot(&node_pubkey, 0, &treasury, None),
            Some(expected_slot),
        );
    }

    #[test]
    fn test_schedule_for_unconfirmed_epoch() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
        let treasury = Arc::new(Bank::new(&genesis_block));
        let cache = LeaderScheduleCache::new_from_bank(&treasury);

        assert_eq!(*cache.max_epoch.read().unwrap(), 1);

        // Asking for the leader for the last slot in epoch 1 is ok b/c
        // epoch 1 is confirmed
        assert_eq!(treasury.get_epoch_and_slot_index(95).0, 1);
        assert!(cache.slot_leader_at(95, Some(&treasury)).is_some());

        // Asking for the lader for the first slot in epoch 2 is not ok
        // b/c epoch 2 is unconfirmed
        assert_eq!(treasury.get_epoch_and_slot_index(96).0, 2);
        assert!(cache.slot_leader_at(96, Some(&treasury)).is_none());

        let bank2 = Bank::new_from_parent(&treasury, &Pubkey::new_rand(), 95);
        assert!(bank2.epoch_vote_accounts(2).is_some());

        // Set root for a slot in epoch 1, so that epoch 2 is now confirmed
        cache.set_genesis(95);
        assert_eq!(*cache.max_epoch.read().unwrap(), 2);
        assert!(cache.slot_leader_at(96, Some(&bank2)).is_some());
        assert_eq!(bank2.get_epoch_and_slot_index(223).0, 2);
        assert!(cache.slot_leader_at(223, Some(&bank2)).is_some());
        assert_eq!(bank2.get_epoch_and_slot_index(224).0, 3);
        assert!(cache.slot_leader_at(224, Some(&bank2)).is_none());
    }
}
