//! The `waterclock_recorder` module provides an object for synchronizing with Proof of History.
//! It synchronizes Water Clock, treasury's register_drop and the ledger
//!
//! WaterClockRecorder will send drops or entries to a WorkingTreasury, if the current range of drops is
//! within the specified WorkingTreasury range.
//!
//! For Drops:
//! * _drop must be > WorkingTreasury::min_drop_height && _drop must be <= WorkingTreasury::max_drop_height
//!
//! For Entries:
//! * recorded entry must be >= WorkingTreasury::min_drop_height && entry must be < WorkingTreasury::max_drop_height
//!
use crate::block_buffer_pool::BlockBufferPool;
use crate::fiscal_statement_info::FsclStmt;
use crate::leader_arrange_cache::LdrSchBufferPoolList;
use crate::leader_arrange_utils;
use crate::water_clock::WaterClock;
use crate::result::{Error, Result};
use morgan_runtime::treasury::Treasury;
use morgan_interface::hash::Hash;
use morgan_interface::waterclock_config::WaterClockConfig;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::timing;
use morgan_interface::transaction::Transaction;
use std::sync::mpsc::{channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use morgan_helper::logHelper::*;

const MAX_LAST_LEADER_GRACE_DROPS_FACTOR: u64 = 2;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum WaterClockRecorderErr {
    InvalidCallingObject,
    MaxHeightReached,
    MinHeightNotReached,
}

pub type WorkingTreasuryEntries = (Arc<Treasury>, Vec<(FsclStmt, u64)>);

#[derive(Clone)]
pub struct WorkingTreasury {
    pub treasury: Arc<Treasury>,
    pub min_drop_height: u64,
    pub max_drop_height: u64,
}

pub struct WaterClockRecorder {
    pub waterclock: Arc<Mutex<WaterClock>>,
    drop_height: u64,
    clear_treasury_signal: Option<SyncSender<bool>>,
    start_slot: u64,
    start_drop: u64,
    drop_cache: Vec<(FsclStmt, u64)>,
    working_treasury: Option<WorkingTreasury>,
    sender: Sender<WorkingTreasuryEntries>,
    start_leader_at_drop: Option<u64>,
    last_leader_drop: Option<u64>,
     max_last_leader_grace_drops: u64,
    id: BvmAddr,
    block_buffer_pool: Arc<BlockBufferPool>,
    leader_schedule_cache: Arc<LdrSchBufferPoolList>,
    waterclock_config: Arc<WaterClockConfig>,
    drops_per_slot: u64,
}

impl WaterClockRecorder {
    fn clear_treasury(&mut self) {
        if let Some(working_treasury) = self.working_treasury.take() {
            let treasury = working_treasury.treasury;
            let next_leader_slot = self.leader_schedule_cache.next_leader_slot(
                &self.id,
                treasury.slot(),
                &treasury,
                Some(&self.block_buffer_pool),
            );
            let (start_leader_at_drop, last_leader_drop) = Self::compute_leader_slot_drops(
                &next_leader_slot,
                treasury.drops_per_slot(),
                self. max_last_leader_grace_drops,
            );
            self.start_leader_at_drop = start_leader_at_drop;
            self.last_leader_drop = last_leader_drop;
        }
        if let Some(ref signal) = self.clear_treasury_signal {
            let _ = signal.try_send(true);
        }
    }

    pub fn would_be_leader(&self, within_next_n_drops: u64) -> bool {
        let close_to_leader_drop = self.start_leader_at_drop.map_or(false, |leader_drop| {
            let leader_pubkeyeal_start_drop =
                leader_drop.saturating_sub(self. max_last_leader_grace_drops);

            self.drop_height() <= self.last_leader_drop.unwrap_or(0)
                && self.drop_height()
                    >= leader_pubkeyeal_start_drop.saturating_sub(within_next_n_drops)
        });

        self.working_treasury.is_some() || close_to_leader_drop
    }

    pub fn next_slot_leader(&self) -> Option<BvmAddr> {
        let slot =
            leader_arrange_utils::drop_height_to_slot(self.drops_per_slot, self.drop_height());
        self.leader_schedule_cache.slot_leader_at(slot + 1, None)
    }

    pub fn start_slot(&self) -> u64 {
        self.start_slot
    }

    pub fn treasury(&self) -> Option<Arc<Treasury>> {
        self.working_treasury.clone().map(|w| w.treasury)
    }

    pub fn drop_height(&self) -> u64 {
        self.drop_height
    }

    // returns if leader _drop has reached, and how many grace drops were afforded
    pub fn reached_leader_drop(&self) -> (bool, u64) {
        self.start_leader_at_drop
            .map(|target_drop| {
                debug!(
                    "Current _drop {}, start _drop {} target {}, grace {}",
                    self.drop_height(),
                    self.start_drop,
                    target_drop,
                    self. max_last_leader_grace_drops
                );

                let leader_pubkeyeal_start_drop =
                    target_drop.saturating_sub(self. max_last_leader_grace_drops);
                // Is the current _drop in the same slot as the target _drop?
                // Check if either grace period has expired,
                // or target _drop is = grace period (i.e. waterclock recorder was just reset)
                if self.drop_height() <= self.last_leader_drop.unwrap_or(0)
                    && (self.drop_height() >= target_drop
                        || self. max_last_leader_grace_drops
                            >= target_drop.saturating_sub(self.start_drop))
                {
                    return (
                        true,
                        self.drop_height()
                            .saturating_sub(leader_pubkeyeal_start_drop),
                    );
                }

                (false, 0)
            })
            .unwrap_or((false, 0))
    }

    fn compute_leader_slot_drops(
        next_leader_slot: &Option<u64>,
        drops_per_slot: u64,
        grace_drops: u64,
    ) -> (Option<u64>, Option<u64>) {
        next_leader_slot
            .map(|slot| {
                (
                    Some(slot * drops_per_slot + grace_drops),
                    Some((slot + 1) * drops_per_slot - 1),
                )
            })
            .unwrap_or((None, None))
    }

    // synchronize Water Clock with a treasury
    pub fn reset(
        &mut self,
        drop_height: u64,
        transaction_seal: Hash,
        start_slot: u64,
        my_next_leader_slot: Option<u64>,
        drops_per_slot: u64,
    ) {
        self.clear_treasury();
        let mut cache = vec![];
        {
            let mut waterclock = self.waterclock.lock().unwrap();
            // info!(
            //     "{}",
            //     Info(format!("reset waterclock from: {},{} to: {},{}",
            //     waterclock.hash, self.drop_height, transaction_seal, drop_height,).to_string())
            // );
            let info:String = format!(
                "reset water clock from: {},{} to: {},{}",
                waterclock.hash,
                self.drop_height,
                transaction_seal,
                drop_height
            ).to_string();
            println!("{}", printLn(info, module_path!().to_string()));

            waterclock.reset(transaction_seal, self.waterclock_config.hashes_per_drop);
        }

        std::mem::swap(&mut cache, &mut self.drop_cache);
        self.start_slot = start_slot;
        self.start_drop = drop_height + 1;
        self.drop_height = drop_height;
        self. max_last_leader_grace_drops = drops_per_slot / MAX_LAST_LEADER_GRACE_DROPS_FACTOR;
        let (start_leader_at_drop, last_leader_drop) = Self::compute_leader_slot_drops(
            &my_next_leader_slot,
            drops_per_slot,
            self. max_last_leader_grace_drops,
        );
        self.start_leader_at_drop = start_leader_at_drop;
        self.last_leader_drop = last_leader_drop;
        self.drops_per_slot = drops_per_slot;
    }

    pub fn set_working_treasury(&mut self, working_treasury: WorkingTreasury) {
        trace!("new working treasury");
        self.working_treasury = Some(working_treasury);
    }
    pub fn set_treasury(&mut self, treasury: &Arc<Treasury>) {
        let max_drop_height = (treasury.slot() + 1) * treasury.drops_per_slot() - 1;
        let working_treasury = WorkingTreasury {
            treasury: treasury.clone(),
            min_drop_height: treasury.drop_height(),
            max_drop_height,
        };
        self.drops_per_slot = treasury.drops_per_slot();
        self.set_working_treasury(working_treasury);
    }

    // Flush cache will delay flushing the cache for a treasury until it past the WorkingTreasury::min_drop_height
    // On a record flush will flush the cache at the WorkingTreasury::min_drop_height, since a record
    // occurs after the min_drop_height was generated
    fn flush_cache(&mut self, _drop: bool) -> Result<()> {
        // check_drop_height is called before flush cache, so it cannot overrun the treasury
        // so a treasury that is so late that it's slot fully generated before it starts recording
        // will fail instead of broadcasting any drops
        let working_treasury = self
            .working_treasury
            .as_ref()
            .ok_or(Error::WaterClockRecorderErr(WaterClockRecorderErr::MaxHeightReached))?;
        if self.drop_height < working_treasury.min_drop_height {
            return Err(Error::WaterClockRecorderErr(
                WaterClockRecorderErr::MinHeightNotReached,
            ));
        }
        if _drop && self.drop_height == working_treasury.min_drop_height {
            return Err(Error::WaterClockRecorderErr(
                WaterClockRecorderErr::MinHeightNotReached,
            ));
        }

        let entry_count = self
            .drop_cache
            .iter()
            .take_while(|x| x.1 <= working_treasury.max_drop_height)
            .count();
        let send_result = if entry_count > 0 {
            debug!(
                "flush_cache: treasury_slot: {} drop_height: {} max: {} sending: {}",
                working_treasury.treasury.slot(),
                working_treasury.treasury.drop_height(),
                working_treasury.max_drop_height,
                entry_count,
            );
            let cache = &self.drop_cache[..entry_count];
            for t in cache {
                working_treasury.treasury.register_drop(&t.0.hash);
            }
            self.sender
                .send((working_treasury.treasury.clone(), cache.to_vec()))
        } else {
            Ok(())
        };
        if self.drop_height >= working_treasury.max_drop_height {
            // info!(
            //     "{}",
            //     Info(format!("waterclock_record: max_drop_height reached, setting working treasury {} to None",
            //     working_treasury.treasury.slot()).to_string())
            // );
            let info:String = format!(
                "water_clock_recorder: max timmer reached, setting current treasury {} to None",
                working_treasury.treasury.slot()
            ).to_string();
            println!("{}", printLn(info, module_path!().to_string()));

            self.start_slot = working_treasury.max_drop_height / working_treasury.treasury.drops_per_slot();
            self.start_drop = (self.start_slot + 1) * working_treasury.treasury.drops_per_slot();
            self.clear_treasury();
        }
        if send_result.is_err() {
            // info!("{}", Info(format!("WorkingTreasury::sender disconnected {:?}", send_result).to_string()));
            println!("{}",
                printLn(
                    format!("WorkingTreasury::sender disconnected {:?}", send_result).to_string(),
                    module_path!().to_string()
                )
            );
            // revert the cache, but clear the working treasury
            self.clear_treasury();
        } else {
            // commit the flush
            let _ = self.drop_cache.drain(..entry_count);
        }

        Ok(())
    }

    pub fn _drop(&mut self) {
        let now = Instant::now();
        let waterclock_entry = self.waterclock.lock().unwrap()._drop();
        inc_new_counter_warn!(
            "waterclock_recorder-drop_lock_contention",
            timing::duration_as_ms(&now.elapsed()) as usize,
            0,
            1000
        );
        let now = Instant::now();
        if let Some(waterclock_entry) = waterclock_entry {
            self.drop_height += 1;
            trace!("_drop {}", self.drop_height);

            if self.start_leader_at_drop.is_none() {
                inc_new_counter_warn!(
                    "waterclock_recorder-drop_overhead",
                    timing::duration_as_ms(&now.elapsed()) as usize,
                    0,
                    1000
                );
                return;
            }

            let entry = FsclStmt {
                num_hashes: waterclock_entry.num_hashes,
                //mark_seal: 19,
                hash: waterclock_entry.hash,
                transactions: vec![],
            };

            self.drop_cache.push((entry, self.drop_height));
            let _ = self.flush_cache(true);
        }
        inc_new_counter_warn!(
            "waterclock_recorder-drop_overhead",
            timing::duration_as_ms(&now.elapsed()) as usize,
            0,
            1000
        );
    }

    pub fn record(
        &mut self,
        treasury_slot: u64,
        mixin: Hash,
        transactions: Vec<Transaction>,
    ) -> Result<()> {
        // Entries without transactions are used to track real-time passing in the ledger and
        // cannot be generated by `record()`
        assert!(!transactions.is_empty(), "No transactions provided");
        loop {
            self.flush_cache(false)?;

            let working_treasury = self
                .working_treasury
                .as_ref()
                .ok_or(Error::WaterClockRecorderErr(WaterClockRecorderErr::MaxHeightReached))?;
            if treasury_slot != working_treasury.treasury.slot() {
                return Err(Error::WaterClockRecorderErr(WaterClockRecorderErr::MaxHeightReached));
            }

            let now = Instant::now();
            if let Some(waterclock_entry) = self.waterclock.lock().unwrap().record(mixin) {
                inc_new_counter_warn!(
                    "waterclock_recorder-record_lock_contention",
                    timing::duration_as_ms(&now.elapsed()) as usize,
                    0,
                    1000
                );
                let entry = FsclStmt {
                    num_hashes: waterclock_entry.num_hashes,
                    //mark_seal: 19,
                    hash: waterclock_entry.hash,
                    transactions,
                };
                self.sender
                    .send((working_treasury.treasury.clone(), vec![(entry, self.drop_height)]))?;
                return Ok(());
            }
            // record() might fail if the next Water Clock hash needs to be a _drop.  But that's ok, _drop()
            // and re-record()
            self._drop();
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_clear_signal(
        drop_height: u64,
        last_entry_hash: Hash,
        start_slot: u64,
        my_leader_slot_index: Option<u64>,
        drops_per_slot: u64,
        id: &BvmAddr,
        block_buffer_pool: &Arc<BlockBufferPool>,
        clear_treasury_signal: Option<SyncSender<bool>>,
        leader_schedule_cache: &Arc<LdrSchBufferPoolList>,
        waterclock_config: &Arc<WaterClockConfig>,
    ) -> (Self, Receiver<WorkingTreasuryEntries>) {
        let waterclock = Arc::new(Mutex::new(WaterClock::new(
            last_entry_hash,
            waterclock_config.hashes_per_drop,
        )));
        let (sender, receiver) = channel();
        let  max_last_leader_grace_drops = drops_per_slot / MAX_LAST_LEADER_GRACE_DROPS_FACTOR;
        let (start_leader_at_drop, last_leader_drop) = Self::compute_leader_slot_drops(
            &my_leader_slot_index,
            drops_per_slot,
             max_last_leader_grace_drops,
        );
        (
            Self {
                waterclock,
                drop_height,
                drop_cache: vec![],
                working_treasury: None,
                sender,
                clear_treasury_signal,
                start_slot,
                start_drop: drop_height + 1,
                start_leader_at_drop,
                last_leader_drop,
                 max_last_leader_grace_drops,
                id: *id,
                block_buffer_pool: block_buffer_pool.clone(),
                leader_schedule_cache: leader_schedule_cache.clone(),
                drops_per_slot,
                waterclock_config: waterclock_config.clone(),
            },
            receiver,
        )
    }

    /// A recorder to synchronize Water Clock with the following data structures
    /// * treasury - the LastId's queue is updated on `_drop` and `record` events
    /// * sender - the FsclStmt channel that outputs to the ledger
    pub fn new(
        drop_height: u64,
        last_entry_hash: Hash,
        start_slot: u64,
        my_leader_slot_index: Option<u64>,
        drops_per_slot: u64,
        id: &BvmAddr,
        block_buffer_pool: &Arc<BlockBufferPool>,
        leader_schedule_cache: &Arc<LdrSchBufferPoolList>,
        waterclock_config: &Arc<WaterClockConfig>,
    ) -> (Self, Receiver<WorkingTreasuryEntries>) {
        Self::new_with_clear_signal(
            drop_height,
            last_entry_hash,
            start_slot,
            my_leader_slot_index,
            drops_per_slot,
            id,
            block_buffer_pool,
            None,
            leader_schedule_cache,
            waterclock_config,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_buffer_pool::{fetch_interim_ledger_location, BlockBufferPool};
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use crate::test_tx::test_tx;
    use morgan_interface::hash::hash;
    use morgan_interface::constants::DEFAULT_DROPS_PER_SLOT;
    use std::sync::mpsc::sync_channel;

    #[test]
    fn test_waterclock_recorder_no_zero_drop() {
        let prev_hash = Hash::default();
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");

            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                DEFAULT_DROPS_PER_SLOT,
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_cache.len(), 1);
            assert_eq!(waterclock_recorder.drop_cache[0].1, 1);
            assert_eq!(waterclock_recorder.drop_height, 1);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_drop_height_is_last_drop() {
        let prev_hash = Hash::default();
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");

            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                DEFAULT_DROPS_PER_SLOT,
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder._drop();
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_cache.len(), 2);
            assert_eq!(waterclock_recorder.drop_cache[1].1, 2);
            assert_eq!(waterclock_recorder.drop_height, 2);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_reset_clears_cache() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                Hash::default(),
                0,
                Some(4),
                DEFAULT_DROPS_PER_SLOT,
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_cache.len(), 1);
            waterclock_recorder.reset(0, Hash::default(), 0, Some(4), DEFAULT_DROPS_PER_SLOT);
            assert_eq!(waterclock_recorder.drop_cache.len(), 0);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_clear() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let prev_hash = treasury.last_transaction_seal();
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingTreasury {
                treasury,
                min_drop_height: 2,
                max_drop_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            assert!(waterclock_recorder.working_treasury.is_some());
            waterclock_recorder.clear_treasury();
            assert!(waterclock_recorder.working_treasury.is_none());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_drop_sent_after_min() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let prev_hash = treasury.last_transaction_seal();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingTreasury {
                treasury: treasury.clone(),
                min_drop_height: 2,
                max_drop_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder._drop();
            waterclock_recorder._drop();
            //_drop height equal to min_drop_height
            //no _drop has been sent
            assert_eq!(waterclock_recorder.drop_cache.last().unwrap().1, 2);
            assert!(entry_receiver.try_recv().is_err());

            // all drops are sent after height > min
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_height, 3);
            assert_eq!(waterclock_recorder.drop_cache.len(), 0);
            let (treasury_, e) = entry_receiver.recv().expect("recv 1");
            assert_eq!(e.len(), 3);
            assert_eq!(treasury_.slot(), treasury.slot());
            assert!(waterclock_recorder.working_treasury.is_none());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_drop_sent_upto_and_including_max() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let prev_hash = treasury.last_transaction_seal();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            waterclock_recorder._drop();
            waterclock_recorder._drop();
            waterclock_recorder._drop();
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_cache.last().unwrap().1, 4);
            assert_eq!(waterclock_recorder.drop_height, 4);

            let working_treasury = WorkingTreasury {
                treasury,
                min_drop_height: 2,
                max_drop_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder._drop();

            assert_eq!(waterclock_recorder.drop_height, 5);
            assert!(waterclock_recorder.working_treasury.is_none());
            let (_, e) = entry_receiver.recv().expect("recv 1");
            assert_eq!(e.len(), 3);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_record_to_early() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let prev_hash = treasury.last_transaction_seal();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingTreasury {
                treasury: treasury.clone(),
                min_drop_height: 2,
                max_drop_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder._drop();
            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert!(waterclock_recorder
                .record(treasury.slot(), h1, vec![tx.clone()])
                .is_err());
            assert!(entry_receiver.try_recv().is_err());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_record_bad_slot() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let prev_hash = treasury.last_transaction_seal();
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingTreasury {
                treasury: treasury.clone(),
                min_drop_height: 1,
                max_drop_height: 2,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_cache.len(), 1);
            assert_eq!(waterclock_recorder.drop_height, 1);
            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert_matches!(
                waterclock_recorder.record(treasury.slot() + 1, h1, vec![tx.clone()]),
                Err(Error::WaterClockRecorderErr(WaterClockRecorderErr::MaxHeightReached))
            );
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_record_at_min_passes() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let prev_hash = treasury.last_transaction_seal();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingTreasury {
                treasury: treasury.clone(),
                min_drop_height: 1,
                max_drop_height: 2,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_cache.len(), 1);
            assert_eq!(waterclock_recorder.drop_height, 1);
            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert!(waterclock_recorder
                .record(treasury.slot(), h1, vec![tx.clone()])
                .is_ok());
            assert_eq!(waterclock_recorder.drop_cache.len(), 0);

            //_drop in the cache + entry
            let (_b, e) = entry_receiver.recv().expect("recv 1");
            assert_eq!(e.len(), 1);
            assert!(e[0].0.is_drop());
            let (_b, e) = entry_receiver.recv().expect("recv 2");
            assert!(!e[0].0.is_drop());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_record_at_max_fails() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let prev_hash = treasury.last_transaction_seal();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingTreasury {
                treasury: treasury.clone(),
                min_drop_height: 1,
                max_drop_height: 2,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder._drop();
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_height, 2);
            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert!(waterclock_recorder
                .record(treasury.slot(), h1, vec![tx.clone()])
                .is_err());

            let (_treasury, e) = entry_receiver.recv().expect("recv 1");
            assert_eq!(e.len(), 2);
            assert!(e[0].0.is_drop());
            assert!(e[1].0.is_drop());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_cache_on_disconnect() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let prev_hash = treasury.last_transaction_seal();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingTreasury {
                treasury,
                min_drop_height: 2,
                max_drop_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder._drop();
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_height, 2);
            drop(entry_receiver);
            waterclock_recorder._drop();
            assert!(waterclock_recorder.working_treasury.is_none());
            assert_eq!(waterclock_recorder.drop_cache.len(), 3);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_current() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                Hash::default(),
                0,
                Some(4),
                DEFAULT_DROPS_PER_SLOT,
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder._drop();
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_cache.len(), 2);
            let hash = waterclock_recorder.waterclock.lock().unwrap().hash;
            waterclock_recorder.reset(
                waterclock_recorder.drop_height,
                hash,
                0,
                Some(4),
                DEFAULT_DROPS_PER_SLOT,
            );
            assert_eq!(waterclock_recorder.drop_cache.len(), 0);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_with_cached() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                Hash::default(),
                0,
                Some(4),
                DEFAULT_DROPS_PER_SLOT,
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder._drop();
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_cache.len(), 2);
            waterclock_recorder.reset(
                waterclock_recorder.drop_cache[0].1,
                waterclock_recorder.drop_cache[0].0.hash,
                0,
                Some(4),
                DEFAULT_DROPS_PER_SLOT,
            );
            assert_eq!(waterclock_recorder.drop_cache.len(), 0);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_to_new_value() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                Hash::default(),
                0,
                Some(4),
                DEFAULT_DROPS_PER_SLOT,
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder._drop();
            waterclock_recorder._drop();
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_cache.len(), 3);
            assert_eq!(waterclock_recorder.drop_height, 3);
            waterclock_recorder.reset(1, hash(b"hello"), 0, Some(4), DEFAULT_DROPS_PER_SLOT);
            assert_eq!(waterclock_recorder.drop_cache.len(), 0);
            waterclock_recorder._drop();
            assert_eq!(waterclock_recorder.drop_height, 2);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_clear_treasury() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                Hash::default(),
                0,
                Some(4),
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );
            let drops_per_slot = treasury.drops_per_slot();
            let working_treasury = WorkingTreasury {
                treasury,
                min_drop_height: 2,
                max_drop_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder.reset(1, hash(b"hello"), 0, Some(4), drops_per_slot);
            assert!(waterclock_recorder.working_treasury.is_none());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    pub fn test_clear_signal() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let (sender, receiver) = sync_channel(1);
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new_with_clear_signal(
                0,
                Hash::default(),
                0,
                None,
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                Some(sender),
                &Arc::new(LdrSchBufferPoolList::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder.set_treasury(&treasury);
            waterclock_recorder.clear_treasury();
            assert!(receiver.try_recv().is_ok());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_reset_start_slot() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let drops_per_slot = 5;
            let GenesisBlockInfo {
                mut genesis_block, ..
            } = create_genesis_block(2);
            genesis_block.drops_per_slot = drops_per_slot;
            let treasury = Arc::new(Treasury::new(&genesis_block));

            let prev_hash = treasury.last_transaction_seal();
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let end_slot = 3;
            let max_drop_height = (end_slot + 1) * drops_per_slot - 1;
            let working_treasury = WorkingTreasury {
                treasury: treasury.clone(),
                min_drop_height: 1,
                max_drop_height,
            };

            waterclock_recorder.set_working_treasury(working_treasury);
            for _ in 0..max_drop_height {
                waterclock_recorder._drop();
            }

            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert!(waterclock_recorder
                .record(treasury.slot(), h1, vec![tx.clone()])
                .is_err());
            assert!(waterclock_recorder.working_treasury.is_none());
            // Make sure the starting slot is updated
            assert_eq!(waterclock_recorder.start_slot(), end_slot);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_reached_leader_drop() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let prev_hash = treasury.last_transaction_seal();
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                None,
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            // Test that with no leader slot, we don't reach the leader _drop
            assert_eq!(waterclock_recorder.reached_leader_drop().0, false);

            waterclock_recorder.reset(
                waterclock_recorder.drop_height(),
                treasury.last_transaction_seal(),
                0,
                None,
                treasury.drops_per_slot(),
            );

            // Test that with no leader slot in reset(), we don't reach the leader _drop
            assert_eq!(waterclock_recorder.reached_leader_drop().0, false);

            // Provide a leader slot 1 slot down
            waterclock_recorder.reset(
                treasury.drops_per_slot(),
                treasury.last_transaction_seal(),
                0,
                Some(2),
                treasury.drops_per_slot(),
            );

            let init_drops = waterclock_recorder.drop_height();

            // Send one slot worth of drops
            for _ in 0..treasury.drops_per_slot() {
                waterclock_recorder._drop();
            }

            // Drop should be recorded
            assert_eq!(
                waterclock_recorder.drop_height(),
                init_drops + treasury.drops_per_slot()
            );

            // Test that we don't reach the leader _drop because of grace drops
            assert_eq!(waterclock_recorder.reached_leader_drop().0, false);

            // reset waterclock now. it should discard the grace drops wait
            waterclock_recorder.reset(
                waterclock_recorder.drop_height(),
                treasury.last_transaction_seal(),
                1,
                Some(2),
                treasury.drops_per_slot(),
            );
            // without sending more drops, we should be leader now
            assert_eq!(waterclock_recorder.reached_leader_drop().0, true);
            assert_eq!(waterclock_recorder.reached_leader_drop().1, 0);

            // Now test that with grace drops we can reach leader drops
            // Set the leader slot 1 slot down
            waterclock_recorder.reset(
                waterclock_recorder.drop_height(),
                treasury.last_transaction_seal(),
                2,
                Some(3),
                treasury.drops_per_slot(),
            );

            // Send one slot worth of drops
            for _ in 0..treasury.drops_per_slot() {
                waterclock_recorder._drop();
            }

            // We are not the leader yet, as expected
            assert_eq!(waterclock_recorder.reached_leader_drop().0, false);

            // Send 1 less _drop than the grace drops
            for _ in 0..treasury.drops_per_slot() / MAX_LAST_LEADER_GRACE_DROPS_FACTOR - 1 {
                waterclock_recorder._drop();
            }
            // We are still not the leader
            assert_eq!(waterclock_recorder.reached_leader_drop().0, false);

            // Send one more _drop
            waterclock_recorder._drop();

            // We should be the leader now
            assert_eq!(waterclock_recorder.reached_leader_drop().0, true);
            assert_eq!(
                waterclock_recorder.reached_leader_drop().1,
                treasury.drops_per_slot() / MAX_LAST_LEADER_GRACE_DROPS_FACTOR
            );

            // Let's test that correct grace drops are reported
            // Set the leader slot 1 slot down
            waterclock_recorder.reset(
                waterclock_recorder.drop_height(),
                treasury.last_transaction_seal(),
                3,
                Some(4),
                treasury.drops_per_slot(),
            );

            // Send remaining drops for the slot (remember we sent extra drops in the previous part of the test)
            for _ in
                treasury.drops_per_slot() / MAX_LAST_LEADER_GRACE_DROPS_FACTOR..treasury.drops_per_slot()
            {
                waterclock_recorder._drop();
            }

            // Send one extra _drop before resetting (so that there's one grace _drop)
            waterclock_recorder._drop();

            // We are not the leader yet, as expected
            assert_eq!(waterclock_recorder.reached_leader_drop().0, false);
            waterclock_recorder.reset(
                waterclock_recorder.drop_height(),
                treasury.last_transaction_seal(),
                3,
                Some(4),
                treasury.drops_per_slot(),
            );
            // without sending more drops, we should be leader now
            assert_eq!(waterclock_recorder.reached_leader_drop().0, true);
            assert_eq!(waterclock_recorder.reached_leader_drop().1, 1);

            // Let's test that if a node overshoots the drops for its target
            // leader slot, reached_leader_drop() will return false
            // Set the leader slot 1 slot down
            waterclock_recorder.reset(
                waterclock_recorder.drop_height(),
                treasury.last_transaction_seal(),
                4,
                Some(5),
                treasury.drops_per_slot(),
            );

            // Send remaining drops for the slot (remember we sent extra drops in the previous part of the test)
            for _ in 0..4 * treasury.drops_per_slot() {
                waterclock_recorder._drop();
            }

            // We are not the leader, as expected
            assert_eq!(waterclock_recorder.reached_leader_drop().0, false);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_would_be_leader_soon() {
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Treasury::new(&genesis_block));
            let prev_hash = treasury.last_transaction_seal();
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                None,
                treasury.drops_per_slot(),
                &BvmAddr::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            // Test that with no leader slot, we don't reach the leader _drop
            assert_eq!(
                waterclock_recorder.would_be_leader(2 * treasury.drops_per_slot()),
                false
            );

            waterclock_recorder.reset(
                waterclock_recorder.drop_height(),
                treasury.last_transaction_seal(),
                0,
                None,
                treasury.drops_per_slot(),
            );

            assert_eq!(
                waterclock_recorder.would_be_leader(2 * treasury.drops_per_slot()),
                false
            );

            // We reset with leader slot after 3 slots
            waterclock_recorder.reset(
                waterclock_recorder.drop_height(),
                treasury.last_transaction_seal(),
                0,
                Some(treasury.slot() + 3),
                treasury.drops_per_slot(),
            );

            // Test that the node won't be leader in next 2 slots
            assert_eq!(
                waterclock_recorder.would_be_leader(2 * treasury.drops_per_slot()),
                false
            );

            // Test that the node will be leader in next 3 slots
            assert_eq!(
                waterclock_recorder.would_be_leader(3 * treasury.drops_per_slot()),
                true
            );

            assert_eq!(
                waterclock_recorder.would_be_leader(2 * treasury.drops_per_slot()),
                false
            );

            // If we set the working treasury, the node should be leader within next 2 slots
            waterclock_recorder.set_treasury(&treasury);
            assert_eq!(
                waterclock_recorder.would_be_leader(2 * treasury.drops_per_slot()),
                true
            );
        }
    }
}
