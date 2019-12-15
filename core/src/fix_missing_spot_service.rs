//! The `repair_service` module implements the tools necessary to generate a thread which
//! regularly finds missing blobs in the ledger and sends repair requests for those blobs

// use crate::treasury_forks::TreasuryForks;
use crate::treasury_forks::TreasuryForks;
use crate::block_buffer_pool::{BlockBufferPool, CompletedSlotsReceiver, MetaInfoCol};
use crate::node_group_info::NodeGroupInfo;
use crate::node_group_info_fixer_listener::NodeGroupInfoFixListener;
use crate::result::Result;
use crate::service::Service;
use morgan_metricbot::datapoint_info;
use morgan_runtime::epoch_schedule::EpochSchedule;
use morgan_interface::pubkey::Pubkey;
use std::collections::BTreeSet;
use std::net::UdpSocket;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;
use morgan_helper::logHelper::*;

pub const MAX_REPAIR_LENGTH: usize = 16;
pub const REPAIR_MS: u64 = 100;
pub const MAX_REPAIR_TRIES: u64 = 128;
pub const NUM_FORKS_TO_REPAIR: usize = 5;
pub const MAX_ORPHANS: usize = 5;

pub enum RepairStrategy {
    RepairRange(RepairSlotRange),
    RepairAll {
        treasury_forks: Arc<RwLock<TreasuryForks>>,
        completed_slots_receiver: CompletedSlotsReceiver,
        epoch_schedule: EpochSchedule,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairType {
    Orphan(u64),
    HighestBlob(u64, u64),
    Blob(u64, u64),
}

pub struct RepairSlotRange {
    pub start: u64,
    pub end: u64,
}

impl Default for RepairSlotRange {
    fn default() -> Self {
        RepairSlotRange {
            start: 0,
            end: std::u64::MAX,
        }
    }
}

pub struct RepairService {
    t_repair: JoinHandle<()>,
    node_group_info_fix_listener: Option<NodeGroupInfoFixListener>,
}

impl RepairService {
    pub fn new(
        block_buffer_pool: Arc<BlockBufferPool>,
        exit: Arc<AtomicBool>,
        repair_socket: Arc<UdpSocket>,
        node_group_info: Arc<RwLock<NodeGroupInfo>>,
        repair_strategy: RepairStrategy,
    ) -> Self {
        let node_group_info_fix_listener = match repair_strategy {
            RepairStrategy::RepairAll {
                ref epoch_schedule, ..
            } => Some(NodeGroupInfoFixListener::new(
                &block_buffer_pool,
                &exit,
                node_group_info.clone(),
                *epoch_schedule,
            )),

            _ => None,
        };

        let t_repair = Builder::new()
            .name("morgan-repair-service".to_string())
            .spawn(move || {
                Self::run(
                    &block_buffer_pool,
                    &exit,
                    &repair_socket,
                    &node_group_info,
                    repair_strategy,
                )
            })
            .unwrap();

        RepairService {
            t_repair,
            node_group_info_fix_listener,
        }
    }

    fn run(
        block_buffer_pool: &Arc<BlockBufferPool>,
        exit: &Arc<AtomicBool>,
        repair_socket: &Arc<UdpSocket>,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        repair_strategy: RepairStrategy,
    ) {
        let mut epoch_slots: BTreeSet<u64> = BTreeSet::new();
        let id = node_group_info.read().unwrap().id();
        let mut current_root = 0;
        if let RepairStrategy::RepairAll {
            ref treasury_forks,
            ref epoch_schedule,
            ..
        } = repair_strategy
        {
            current_root = treasury_forks.read().unwrap().root();
            Self::initialize_epoch_slots(
                id,
                block_buffer_pool,
                &mut epoch_slots,
                current_root,
                epoch_schedule,
                node_group_info,
            );
        }
        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            let repairs = {
                match repair_strategy {
                    RepairStrategy::RepairRange(ref repair_slot_range) => {
                        Self::generate_repairs_in_range(
                            block_buffer_pool,
                            MAX_REPAIR_LENGTH,
                            repair_slot_range,
                        )
                    }

                    RepairStrategy::RepairAll {
                        ref treasury_forks,
                        ref completed_slots_receiver,
                        ..
                    } => {
                        let new_root = treasury_forks.read().unwrap().root();
                        Self::update_epoch_slots(
                            id,
                            new_root,
                            &mut current_root,
                            &mut epoch_slots,
                            &node_group_info,
                            completed_slots_receiver,
                        );
                        Self::generate_repairs(block_buffer_pool, new_root, MAX_REPAIR_LENGTH)
                    }
                }
            };

            if let Ok(repairs) = repairs {
                let reqs: Vec<_> = repairs
                    .into_iter()
                    .filter_map(|repair_request| {
                        node_group_info
                            .read()
                            .unwrap()
                            .repair_request(&repair_request)
                            .map(|result| (result, repair_request))
                            .ok()
                    })
                    .collect();

                for ((to, req), repair_request) in reqs {
                    if let Ok(local_addr) = repair_socket.local_addr() {
                        datapoint_info!(
                            "repair_service",
                            ("repair_request", format!("{:?}", repair_request), String),
                            ("to", to.to_string(), String),
                            ("from", local_addr.to_string(), String),
                            ("id", id.to_string(), String)
                        );
                    }
                    repair_socket.send_to(&req, to).unwrap_or_else(|e| {
                        // info!("{}", Info(format!("{} repair req send_to({}) error {:?}", id, to, e).to_string()));
                        println!("{}",
                            printLn(
                                format!("{} repair req send_to({}) error {:?}", id, to, e).to_string(),
                                module_path!().to_string()
                            )
                        );
                        0
                    });
                }
            }
            sleep(Duration::from_millis(REPAIR_MS));
        }
    }

    // Generate repairs for all slots `x` in the repair_range.start <= x <= repair_range.end
    fn generate_repairs_in_range(
        block_buffer_pool: &BlockBufferPool,
        max_repairs: usize,
        repair_range: &RepairSlotRange,
    ) -> Result<(Vec<RepairType>)> {
        // Slot height and blob indexes for blobs we want to repair
        let mut repairs: Vec<RepairType> = vec![];
        for slot in repair_range.start..=repair_range.end {
            if repairs.len() >= max_repairs {
                break;
            }

            let meta = block_buffer_pool
                .meta(slot)
                .expect("Unable to lookup slot meta")
                .unwrap_or(MetaInfoCol {
                    slot,
                    ..MetaInfoCol::default()
                });

            let new_repairs = Self::generate_repairs_for_slot(
                block_buffer_pool,
                slot,
                &meta,
                max_repairs - repairs.len(),
            );
            repairs.extend(new_repairs);
        }

        Ok(repairs)
    }

    fn generate_repairs(
        block_buffer_pool: &BlockBufferPool,
        root: u64,
        max_repairs: usize,
    ) -> Result<(Vec<RepairType>)> {
        // Slot height and blob indexes for blobs we want to repair
        let mut repairs: Vec<RepairType> = vec![];
        Self::generate_repairs_for_fork(block_buffer_pool, &mut repairs, max_repairs, root);

        // TODO: Incorporate gossip to determine priorities for repair?

        // Try to resolve orphans in block_buffer_pool
        let orphans = block_buffer_pool.fetch_singletons(Some(MAX_ORPHANS));

        Self::generate_repairs_for_orphans(&orphans[..], &mut repairs);
        Ok(repairs)
    }

    fn generate_repairs_for_slot(
        block_buffer_pool: &BlockBufferPool,
        slot: u64,
        slot_meta: &MetaInfoCol,
        max_repairs: usize,
    ) -> Vec<RepairType> {
        if slot_meta.is_full() {
            vec![]
        } else if slot_meta.consumed == slot_meta.received {
            vec![RepairType::HighestBlob(slot, slot_meta.received)]
        } else {
            let reqs = block_buffer_pool.search_absent_data_indexes(
                slot,
                slot_meta.consumed,
                slot_meta.received,
                max_repairs,
            );

            reqs.into_iter()
                .map(|i| RepairType::Blob(slot, i))
                .collect()
        }
    }

    fn generate_repairs_for_orphans(orphans: &[u64], repairs: &mut Vec<RepairType>) {
        repairs.extend(orphans.iter().map(|h| RepairType::Orphan(*h)));
    }

    /// Repairs any fork starting at the input slot
    fn generate_repairs_for_fork(
        block_buffer_pool: &BlockBufferPool,
        repairs: &mut Vec<RepairType>,
        max_repairs: usize,
        slot: u64,
    ) {
        let mut pending_slots = vec![slot];
        while repairs.len() < max_repairs && !pending_slots.is_empty() {
            let slot = pending_slots.pop().unwrap();
            if let Some(slot_meta) = block_buffer_pool.meta(slot).unwrap() {
                let new_repairs = Self::generate_repairs_for_slot(
                    block_buffer_pool,
                    slot,
                    &slot_meta,
                    max_repairs - repairs.len(),
                );
                repairs.extend(new_repairs);
                let next_slots = slot_meta.next_slots;
                pending_slots.extend(next_slots);
            } else {
                break;
            }
        }
    }

    fn get_completed_slots_past_root(
        block_buffer_pool: &BlockBufferPool,
        slots_in_gossip: &mut BTreeSet<u64>,
        root: u64,
        epoch_schedule: &EpochSchedule,
    ) {
        let last_confirmed_epoch = epoch_schedule.get_stakers_epoch(root);
        let last_epoch_slot = epoch_schedule.get_last_slot_in_epoch(last_confirmed_epoch);

        let meta_iter = block_buffer_pool
            .meta_info_col_looper(root + 1)
            .expect("Couldn't get db iterator");

        for (current_slot, meta) in meta_iter {
            if current_slot > last_epoch_slot {
                break;
            }
            if meta.is_full() {
                slots_in_gossip.insert(current_slot);
            }
        }
    }

    fn initialize_epoch_slots(
        id: Pubkey,
        block_buffer_pool: &BlockBufferPool,
        slots_in_gossip: &mut BTreeSet<u64>,
        root: u64,
        epoch_schedule: &EpochSchedule,
        node_group_info: &RwLock<NodeGroupInfo>,
    ) {
        Self::get_completed_slots_past_root(block_buffer_pool, slots_in_gossip, root, epoch_schedule);

        // Safe to set into gossip because by this time, the leader schedule cache should
        // also be updated with the latest root (done in block_buffer_processor) and thus
        // will provide a schedule to window_service for any incoming blobs up to the
        // last_confirmed_epoch.
        node_group_info
            .write()
            .unwrap()
            .push_epoch_slots(id, root, slots_in_gossip.clone());
    }

    // Update the gossiped structure used for the "Repairmen" repair protocol. See book
    // for details.
    fn update_epoch_slots(
        id: Pubkey,
        latest_known_root: u64,
        prev_root: &mut u64,
        slots_in_gossip: &mut BTreeSet<u64>,
        node_group_info: &RwLock<NodeGroupInfo>,
        completed_slots_receiver: &CompletedSlotsReceiver,
    ) {
        // If the latest known root is different, update gossip.
        let mut should_update = latest_known_root != *prev_root;
        while let Ok(completed_slots) = completed_slots_receiver.try_recv() {
            for slot in completed_slots {
                // If the newly completed slot > root, and the set did not contain this value
                // before, we should update gossip.
                if slot > latest_known_root {
                    should_update |= slots_in_gossip.insert(slot);
                }
            }
        }

        if should_update {
            // Filter out everything <= root
            if latest_known_root != *prev_root {
                *prev_root = latest_known_root;
                Self::retain_slots_greater_than_root(slots_in_gossip, latest_known_root);
            }

            node_group_info.write().unwrap().push_epoch_slots(
                id,
                latest_known_root,
                slots_in_gossip.clone(),
            );
        }
    }

    fn retain_slots_greater_than_root(slot_set: &mut BTreeSet<u64>, root: u64) {
        *slot_set = slot_set
            .range((Excluded(&root), Unbounded))
            .cloned()
            .collect();
    }
}

impl Service for RepairService {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        let mut results = vec![self.t_repair.join()];
        if let Some(node_group_info_fix_listener) = self.node_group_info_fix_listener {
            results.push(node_group_info_fix_listener.join());
        }
        for r in results {
            r?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::block_buffer_pool::tests::{
        make_chaining_slot_entries, make_many_slot_entries, make_slot_entries,
    };
    use crate::block_buffer_pool::{get_tmp_ledger_path, BlockBufferPool};
    use crate::node_group_info::Node;
    use rand::seq::SliceRandom;
    use rand::{thread_rng, Rng};
    use std::cmp::min;
    use std::sync::mpsc::channel;
    use std::thread::Builder;

    #[test]
    pub fn test_repair_orphan() {
        let block_buffer_pool_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            // Create some orphan slots
            let (mut blobs, _) = make_slot_entries(1, 0, 1);
            let (blobs2, _) = make_slot_entries(5, 2, 1);
            blobs.extend(blobs2);
            block_buffer_pool.update_blobs(&blobs).unwrap();
            assert_eq!(
                RepairService::generate_repairs(&block_buffer_pool, 0, 2).unwrap(),
                vec![
                    RepairType::HighestBlob(0, 0),
                    RepairType::Orphan(0),
                    RepairType::Orphan(2)
                ]
            );
        }

        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_empty_slot() {
        let block_buffer_pool_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            let (blobs, _) = make_slot_entries(2, 0, 1);

            // Write this blob to slot 2, should chain to slot 0, which we haven't received
            // any blobs for
            block_buffer_pool.update_blobs(&blobs).unwrap();

            // Check that repair tries to patch the empty slot
            assert_eq!(
                RepairService::generate_repairs(&block_buffer_pool, 0, 2).unwrap(),
                vec![RepairType::HighestBlob(0, 0), RepairType::Orphan(0)]
            );
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_generate_repairs() {
        let block_buffer_pool_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            let nth = 3;
            let num_entries_per_slot = 5 * nth;
            let num_slots = 2;

            // Create some blobs
            let (blobs, _) =
                make_many_slot_entries(0, num_slots as u64, num_entries_per_slot as u64);

            // write every nth blob
            let blobs_to_write: Vec<_> = blobs.iter().step_by(nth as usize).collect();

            block_buffer_pool.update_blobs(blobs_to_write).unwrap();

            let missing_indexes_per_slot: Vec<u64> = (0..num_entries_per_slot / nth - 1)
                .flat_map(|x| ((nth * x + 1) as u64..(nth * x + nth) as u64))
                .collect();

            let expected: Vec<RepairType> = (0..num_slots)
                .flat_map(|slot| {
                    missing_indexes_per_slot
                        .iter()
                        .map(move |blob_index| RepairType::Blob(slot as u64, *blob_index))
                })
                .collect();

            assert_eq!(
                RepairService::generate_repairs(&block_buffer_pool, 0, std::usize::MAX).unwrap(),
                expected
            );

            assert_eq!(
                RepairService::generate_repairs(&block_buffer_pool, 0, expected.len() - 2).unwrap()[..],
                expected[0..expected.len() - 2]
            );
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_generate_highest_repair() {
        let block_buffer_pool_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            let num_entries_per_slot = 10;

            // Create some blobs
            let (mut blobs, _) = make_slot_entries(0, 0, num_entries_per_slot as u64);

            // Remove is_last flag on last blob
            blobs.last_mut().unwrap().set_flags(0);

            block_buffer_pool.update_blobs(&blobs).unwrap();

            // We didn't get the last blob for this slot, so ask for the highest blob for that slot
            let expected: Vec<RepairType> = vec![RepairType::HighestBlob(0, num_entries_per_slot)];

            assert_eq!(
                RepairService::generate_repairs(&block_buffer_pool, 0, std::usize::MAX).unwrap(),
                expected
            );
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_range() {
        let block_buffer_pool_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            let slots: Vec<u64> = vec![1, 3, 5, 7, 8];
            let num_entries_per_slot = 10;

            let blobs = make_chaining_slot_entries(&slots, num_entries_per_slot);
            for (slot_blobs, _) in blobs.iter() {
                block_buffer_pool.update_blobs(&slot_blobs[1..]).unwrap();
            }

            // Iterate through all possible combinations of start..end (inclusive on both
            // sides of the range)
            for start in 0..slots.len() {
                for end in start..slots.len() {
                    let mut repair_slot_range = RepairSlotRange::default();
                    repair_slot_range.start = slots[start];
                    repair_slot_range.end = slots[end];
                    let expected: Vec<RepairType> = (repair_slot_range.start
                        ..=repair_slot_range.end)
                        .map(|slot_index| {
                            if slots.contains(&(slot_index as u64)) {
                                RepairType::Blob(slot_index as u64, 0)
                            } else {
                                RepairType::HighestBlob(slot_index as u64, 0)
                            }
                        })
                        .collect();

                    assert_eq!(
                        RepairService::generate_repairs_in_range(
                            &block_buffer_pool,
                            std::usize::MAX,
                            &repair_slot_range
                        )
                        .unwrap(),
                        expected
                    );
                }
            }
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_range_highest() {
        let block_buffer_pool_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();

            let num_entries_per_slot = 10;

            let num_slots = 1;
            let start = 5;

            // Create some blobs in slots 0..num_slots
            for i in start..start + num_slots {
                let parent = if i > 0 { i - 1 } else { 0 };
                let (blobs, _) = make_slot_entries(i, parent, num_entries_per_slot as u64);

                block_buffer_pool.update_blobs(&blobs).unwrap();
            }

            let end = 4;
            let expected: Vec<RepairType> = vec![
                RepairType::HighestBlob(end - 2, 0),
                RepairType::HighestBlob(end - 1, 0),
                RepairType::HighestBlob(end, 0),
            ];

            let mut repair_slot_range = RepairSlotRange::default();
            repair_slot_range.start = 2;
            repair_slot_range.end = end;

            assert_eq!(
                RepairService::generate_repairs_in_range(
                    &block_buffer_pool,
                    std::usize::MAX,
                    &repair_slot_range
                )
                .unwrap(),
                expected
            );
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_get_completed_slots_past_root() {
        let block_buffer_pool_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap();
            let num_entries_per_slot = 10;
            let root = 10;

            let fork1 = vec![5, 7, root, 15, 20, 21];
            let fork1_blobs: Vec<_> = make_chaining_slot_entries(&fork1, num_entries_per_slot)
                .into_iter()
                .flat_map(|(blobs, _)| blobs)
                .collect();
            let fork2 = vec![8, 12];
            let fork2_blobs = make_chaining_slot_entries(&fork2, num_entries_per_slot);

            // Remove the last blob from each slot to make an incomplete slot
            let fork2_incomplete_blobs: Vec<_> = fork2_blobs
                .into_iter()
                .flat_map(|(mut blobs, _)| {
                    blobs.pop();
                    blobs
                })
                .collect();
            let mut full_slots = BTreeSet::new();

            block_buffer_pool.update_blobs(&fork1_blobs).unwrap();
            block_buffer_pool.update_blobs(&fork2_incomplete_blobs).unwrap();

            // Test that only slots > root from fork1 were included
            let epoch_schedule = EpochSchedule::new(32, 32, false);

            RepairService::get_completed_slots_past_root(
                &block_buffer_pool,
                &mut full_slots,
                root,
                &epoch_schedule,
            );

            let mut expected: BTreeSet<_> = fork1.into_iter().filter(|x| *x > root).collect();
            assert_eq!(full_slots, expected);

            // Test that slots past the last confirmed epoch boundary don't get included
            let last_epoch = epoch_schedule.get_stakers_epoch(root);
            let last_slot = epoch_schedule.get_last_slot_in_epoch(last_epoch);
            let fork3 = vec![last_slot, last_slot + 1];
            let fork3_blobs: Vec<_> = make_chaining_slot_entries(&fork3, num_entries_per_slot)
                .into_iter()
                .flat_map(|(blobs, _)| blobs)
                .collect();
            block_buffer_pool.update_blobs(&fork3_blobs).unwrap();
            RepairService::get_completed_slots_past_root(
                &block_buffer_pool,
                &mut full_slots,
                root,
                &epoch_schedule,
            );
            expected.insert(last_slot);
            assert_eq!(full_slots, expected);
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_update_epoch_slots() {
        let block_buffer_pool_path = get_tmp_ledger_path!();
        {
            // Create block_buffer_pool
            let (block_buffer_pool, _, completed_slots_receiver) =
                BlockBufferPool::open_by_message(&block_buffer_pool_path).unwrap();

            let block_buffer_pool = Arc::new(block_buffer_pool);

            let mut root = 0;
            let num_slots = 100;
            let entries_per_slot = 5;
            let block_buffer_pool_ = block_buffer_pool.clone();

            // Spin up thread to write to block_buffer_pool
            let writer = Builder::new()
                .name("writer".to_string())
                .spawn(move || {
                    let slots: Vec<_> = (1..num_slots + 1).collect();
                    let mut blobs: Vec<_> = make_chaining_slot_entries(&slots, entries_per_slot)
                        .into_iter()
                        .flat_map(|(blobs, _)| blobs)
                        .collect();
                    blobs.shuffle(&mut thread_rng());
                    let mut i = 0;
                    let max_step = entries_per_slot * 4;
                    let repair_interval_ms = 10;
                    let mut rng = rand::thread_rng();
                    while i < blobs.len() as usize {
                        let step = rng.gen_range(1, max_step + 1);
                        block_buffer_pool_
                            .insert_data_blobs(&blobs[i..min(i + max_step as usize, blobs.len())])
                            .unwrap();
                        sleep(Duration::from_millis(repair_interval_ms));
                        i += step as usize;
                    }
                })
                .unwrap();

            let mut completed_slots = BTreeSet::new();
            let node_info = Node::new_localhost_with_pubkey(&Pubkey::default());
            let node_group_info = RwLock::new(NodeGroupInfo::new_with_invalid_keypair(
                node_info.info.clone(),
            ));

            while completed_slots.len() < num_slots as usize {
                RepairService::update_epoch_slots(
                    Pubkey::default(),
                    root,
                    &mut root.clone(),
                    &mut completed_slots,
                    &node_group_info,
                    &completed_slots_receiver,
                );
            }

            let mut expected: BTreeSet<_> = (1..num_slots + 1).collect();
            assert_eq!(completed_slots, expected);

            // Update with new root, should filter out the slots <= root
            root = num_slots / 2;
            let (blobs, _) = make_slot_entries(num_slots + 2, num_slots + 1, entries_per_slot);
            block_buffer_pool.insert_data_blobs(&blobs).unwrap();
            RepairService::update_epoch_slots(
                Pubkey::default(),
                root,
                &mut 0,
                &mut completed_slots,
                &node_group_info,
                &completed_slots_receiver,
            );
            expected.insert(num_slots + 2);
            RepairService::retain_slots_greater_than_root(&mut expected, root);
            assert_eq!(completed_slots, expected);
            writer.join().unwrap();
        }
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_update_epoch_slots_new_root() {
        let mut current_root = 0;

        let mut completed_slots = BTreeSet::new();
        let node_info = Node::new_localhost_with_pubkey(&Pubkey::default());
        let node_group_info = RwLock::new(NodeGroupInfo::new_with_invalid_keypair(
            node_info.info.clone(),
        ));
        let my_pubkey = Pubkey::new_rand();
        let (completed_slots_sender, completed_slots_receiver) = channel();

        // Send a new slot before the root is updated
        let newly_completed_slot = 63;
        completed_slots_sender
            .send(vec![newly_completed_slot])
            .unwrap();
        RepairService::update_epoch_slots(
            my_pubkey.clone(),
            current_root,
            &mut current_root.clone(),
            &mut completed_slots,
            &node_group_info,
            &completed_slots_receiver,
        );

        // We should see epoch state update
        let (my_epoch_slots_in_gossip, updated_ts) = {
            let r_node_group_info = node_group_info.read().unwrap();

            let (my_epoch_slots_in_gossip, updated_ts) = r_node_group_info
                .get_epoch_state_for_node(&my_pubkey, None)
                .clone()
                .unwrap();

            (my_epoch_slots_in_gossip.clone(), updated_ts)
        };

        assert_eq!(my_epoch_slots_in_gossip.root, 0);
        assert_eq!(current_root, 0);
        assert_eq!(my_epoch_slots_in_gossip.slots.len(), 1);
        assert!(my_epoch_slots_in_gossip
            .slots
            .contains(&newly_completed_slot));

        // Calling update again with no updates to either the roots or set of completed slots
        // should not update gossip
        RepairService::update_epoch_slots(
            my_pubkey.clone(),
            current_root,
            &mut current_root,
            &mut completed_slots,
            &node_group_info,
            &completed_slots_receiver,
        );

        assert!(node_group_info
            .read()
            .unwrap()
            .get_epoch_state_for_node(&my_pubkey, Some(updated_ts))
            .is_none());

        sleep(Duration::from_millis(10));
        // Updating just the root again should update gossip (simulates replay stage updating root
        // after a slot has been signaled as completed)
        RepairService::update_epoch_slots(
            my_pubkey.clone(),
            current_root + 1,
            &mut current_root,
            &mut completed_slots,
            &node_group_info,
            &completed_slots_receiver,
        );

        let r_node_group_info = node_group_info.read().unwrap();

        let (my_epoch_slots_in_gossip, _) = r_node_group_info
            .get_epoch_state_for_node(&my_pubkey, Some(updated_ts))
            .clone()
            .unwrap();

        // Check the root was updated correctly
        assert_eq!(my_epoch_slots_in_gossip.root, 1);
        assert_eq!(current_root, 1);
        assert_eq!(my_epoch_slots_in_gossip.slots.len(), 1);
        assert!(my_epoch_slots_in_gossip
            .slots
            .contains(&newly_completed_slot));
    }
}
