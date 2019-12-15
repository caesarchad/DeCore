//! The `waterclock_recorder` module provides an object for synchronizing with Proof of History.
//! It synchronizes Water Clock, treasury's register_tick and the ledger
//!
//! WaterClockRecorder will send ticks or entries to a WorkingBank, if the current range of ticks is
//! within the specified WorkingBank range.
//!
//! For Ticks:
//! * tick must be > WorkingBank::min_tick_height && tick must be <= WorkingBank::max_tick_height
//!
//! For Entries:
//! * recorded entry must be >= WorkingBank::min_tick_height && entry must be < WorkingBank::max_tick_height
//!
use crate::block_buffer_pool::BlockBufferPool;
use crate::entry_info::Entry;
use crate::leader_arrange_cache::LeaderScheduleCache;
use crate::leader_arrange_utils;
use crate::water_clock::WaterClock;
use crate::result::{Error, Result};
use morgan_runtime::treasury::Bank;
use morgan_interface::hash::Hash;
use morgan_interface::waterclock_config::WaterClockConfig;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::timing;
use morgan_interface::transaction::Transaction;
use std::sync::mpsc::{channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use morgan_helper::logHelper::*;

const MAX_LAST_LEADER_GRACE_TICKS_FACTOR: u64 = 2;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum WaterClockRecorderErr {
    InvalidCallingObject,
    MaxHeightReached,
    MinHeightNotReached,
}

pub type WorkingBankEntries = (Arc<Bank>, Vec<(Entry, u64)>);

#[derive(Clone)]
pub struct WorkingBank {
    pub treasury: Arc<Bank>,
    pub min_tick_height: u64,
    pub max_tick_height: u64,
}

pub struct WaterClockRecorder {
    pub waterclock: Arc<Mutex<WaterClock>>,
    tick_height: u64,
    clear_treasury_signal: Option<SyncSender<bool>>,
    start_slot: u64,
    start_tick: u64,
    tick_cache: Vec<(Entry, u64)>,
    working_treasury: Option<WorkingBank>,
    sender: Sender<WorkingBankEntries>,
    start_leader_at_tick: Option<u64>,
    last_leader_tick: Option<u64>,
    max_last_leader_grace_ticks: u64,
    id: Pubkey,
    block_buffer_pool: Arc<BlockBufferPool>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    waterclock_config: Arc<WaterClockConfig>,
    ticks_per_slot: u64,
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
            let (start_leader_at_tick, last_leader_tick) = Self::compute_leader_slot_ticks(
                &next_leader_slot,
                treasury.ticks_per_slot(),
                self.max_last_leader_grace_ticks,
            );
            self.start_leader_at_tick = start_leader_at_tick;
            self.last_leader_tick = last_leader_tick;
        }
        if let Some(ref signal) = self.clear_treasury_signal {
            let _ = signal.try_send(true);
        }
    }

    pub fn would_be_leader(&self, within_next_n_ticks: u64) -> bool {
        let close_to_leader_tick = self.start_leader_at_tick.map_or(false, |leader_tick| {
            let leader_pubkeyeal_start_tick =
                leader_tick.saturating_sub(self.max_last_leader_grace_ticks);

            self.tick_height() <= self.last_leader_tick.unwrap_or(0)
                && self.tick_height()
                    >= leader_pubkeyeal_start_tick.saturating_sub(within_next_n_ticks)
        });

        self.working_treasury.is_some() || close_to_leader_tick
    }

    pub fn next_slot_leader(&self) -> Option<Pubkey> {
        let slot =
            leader_arrange_utils::tick_height_to_slot(self.ticks_per_slot, self.tick_height());
        self.leader_schedule_cache.slot_leader_at(slot + 1, None)
    }

    pub fn start_slot(&self) -> u64 {
        self.start_slot
    }

    pub fn treasury(&self) -> Option<Arc<Bank>> {
        self.working_treasury.clone().map(|w| w.treasury)
    }

    pub fn tick_height(&self) -> u64 {
        self.tick_height
    }

    // returns if leader tick has reached, and how many grace ticks were afforded
    pub fn reached_leader_tick(&self) -> (bool, u64) {
        self.start_leader_at_tick
            .map(|target_tick| {
                debug!(
                    "Current tick {}, start tick {} target {}, grace {}",
                    self.tick_height(),
                    self.start_tick,
                    target_tick,
                    self.max_last_leader_grace_ticks
                );

                let leader_pubkeyeal_start_tick =
                    target_tick.saturating_sub(self.max_last_leader_grace_ticks);
                // Is the current tick in the same slot as the target tick?
                // Check if either grace period has expired,
                // or target tick is = grace period (i.e. waterclock recorder was just reset)
                if self.tick_height() <= self.last_leader_tick.unwrap_or(0)
                    && (self.tick_height() >= target_tick
                        || self.max_last_leader_grace_ticks
                            >= target_tick.saturating_sub(self.start_tick))
                {
                    return (
                        true,
                        self.tick_height()
                            .saturating_sub(leader_pubkeyeal_start_tick),
                    );
                }

                (false, 0)
            })
            .unwrap_or((false, 0))
    }

    fn compute_leader_slot_ticks(
        next_leader_slot: &Option<u64>,
        ticks_per_slot: u64,
        grace_ticks: u64,
    ) -> (Option<u64>, Option<u64>) {
        next_leader_slot
            .map(|slot| {
                (
                    Some(slot * ticks_per_slot + grace_ticks),
                    Some((slot + 1) * ticks_per_slot - 1),
                )
            })
            .unwrap_or((None, None))
    }

    // synchronize Water Clock with a treasury
    pub fn reset(
        &mut self,
        tick_height: u64,
        blockhash: Hash,
        start_slot: u64,
        my_next_leader_slot: Option<u64>,
        ticks_per_slot: u64,
    ) {
        self.clear_treasury();
        let mut cache = vec![];
        {
            let mut waterclock = self.waterclock.lock().unwrap();
            // info!(
            //     "{}",
            //     Info(format!("reset waterclock from: {},{} to: {},{}",
            //     waterclock.hash, self.tick_height, blockhash, tick_height,).to_string())
            // );
            let info:String = format!(
                "reset water clock from: {},{} to: {},{}",
                waterclock.hash,
                self.tick_height,
                blockhash,
                tick_height
            ).to_string();
            println!("{}", printLn(info, module_path!().to_string()));

            waterclock.reset(blockhash, self.waterclock_config.hashes_per_tick);
        }

        std::mem::swap(&mut cache, &mut self.tick_cache);
        self.start_slot = start_slot;
        self.start_tick = tick_height + 1;
        self.tick_height = tick_height;
        self.max_last_leader_grace_ticks = ticks_per_slot / MAX_LAST_LEADER_GRACE_TICKS_FACTOR;
        let (start_leader_at_tick, last_leader_tick) = Self::compute_leader_slot_ticks(
            &my_next_leader_slot,
            ticks_per_slot,
            self.max_last_leader_grace_ticks,
        );
        self.start_leader_at_tick = start_leader_at_tick;
        self.last_leader_tick = last_leader_tick;
        self.ticks_per_slot = ticks_per_slot;
    }

    pub fn set_working_treasury(&mut self, working_treasury: WorkingBank) {
        trace!("new working treasury");
        self.working_treasury = Some(working_treasury);
    }
    pub fn set_treasury(&mut self, treasury: &Arc<Bank>) {
        let max_tick_height = (treasury.slot() + 1) * treasury.ticks_per_slot() - 1;
        let working_treasury = WorkingBank {
            treasury: treasury.clone(),
            min_tick_height: treasury.tick_height(),
            max_tick_height,
        };
        self.ticks_per_slot = treasury.ticks_per_slot();
        self.set_working_treasury(working_treasury);
    }

    // Flush cache will delay flushing the cache for a treasury until it past the WorkingBank::min_tick_height
    // On a record flush will flush the cache at the WorkingBank::min_tick_height, since a record
    // occurs after the min_tick_height was generated
    fn flush_cache(&mut self, tick: bool) -> Result<()> {
        // check_tick_height is called before flush cache, so it cannot overrun the treasury
        // so a treasury that is so late that it's slot fully generated before it starts recording
        // will fail instead of broadcasting any ticks
        let working_treasury = self
            .working_treasury
            .as_ref()
            .ok_or(Error::WaterClockRecorderErr(WaterClockRecorderErr::MaxHeightReached))?;
        if self.tick_height < working_treasury.min_tick_height {
            return Err(Error::WaterClockRecorderErr(
                WaterClockRecorderErr::MinHeightNotReached,
            ));
        }
        if tick && self.tick_height == working_treasury.min_tick_height {
            return Err(Error::WaterClockRecorderErr(
                WaterClockRecorderErr::MinHeightNotReached,
            ));
        }

        let entry_count = self
            .tick_cache
            .iter()
            .take_while(|x| x.1 <= working_treasury.max_tick_height)
            .count();
        let send_result = if entry_count > 0 {
            debug!(
                "flush_cache: treasury_slot: {} tick_height: {} max: {} sending: {}",
                working_treasury.treasury.slot(),
                working_treasury.treasury.tick_height(),
                working_treasury.max_tick_height,
                entry_count,
            );
            let cache = &self.tick_cache[..entry_count];
            for t in cache {
                working_treasury.treasury.register_tick(&t.0.hash);
            }
            self.sender
                .send((working_treasury.treasury.clone(), cache.to_vec()))
        } else {
            Ok(())
        };
        if self.tick_height >= working_treasury.max_tick_height {
            // info!(
            //     "{}",
            //     Info(format!("waterclock_record: max_tick_height reached, setting working treasury {} to None",
            //     working_treasury.treasury.slot()).to_string())
            // );
            let info:String = format!(
                "water_clock_recorder: max timmer reached, setting current treasury {} to None",
                working_treasury.treasury.slot()
            ).to_string();
            println!("{}", printLn(info, module_path!().to_string()));

            self.start_slot = working_treasury.max_tick_height / working_treasury.treasury.ticks_per_slot();
            self.start_tick = (self.start_slot + 1) * working_treasury.treasury.ticks_per_slot();
            self.clear_treasury();
        }
        if send_result.is_err() {
            // info!("{}", Info(format!("WorkingBank::sender disconnected {:?}", send_result).to_string()));
            println!("{}",
                printLn(
                    format!("WorkingBank::sender disconnected {:?}", send_result).to_string(),
                    module_path!().to_string()
                )
            );
            // revert the cache, but clear the working treasury
            self.clear_treasury();
        } else {
            // commit the flush
            let _ = self.tick_cache.drain(..entry_count);
        }

        Ok(())
    }

    pub fn tick(&mut self) {
        let now = Instant::now();
        let waterclock_entry = self.waterclock.lock().unwrap().tick();
        inc_new_counter_warn!(
            "waterclock_recorder-tick_lock_contention",
            timing::duration_as_ms(&now.elapsed()) as usize,
            0,
            1000
        );
        let now = Instant::now();
        if let Some(waterclock_entry) = waterclock_entry {
            self.tick_height += 1;
            trace!("tick {}", self.tick_height);

            if self.start_leader_at_tick.is_none() {
                inc_new_counter_warn!(
                    "waterclock_recorder-tick_overhead",
                    timing::duration_as_ms(&now.elapsed()) as usize,
                    0,
                    1000
                );
                return;
            }

            let entry = Entry {
                num_hashes: waterclock_entry.num_hashes,
                hash: waterclock_entry.hash,
                transactions: vec![],
            };

            self.tick_cache.push((entry, self.tick_height));
            let _ = self.flush_cache(true);
        }
        inc_new_counter_warn!(
            "waterclock_recorder-tick_overhead",
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
                let entry = Entry {
                    num_hashes: waterclock_entry.num_hashes,
                    hash: waterclock_entry.hash,
                    transactions,
                };
                self.sender
                    .send((working_treasury.treasury.clone(), vec![(entry, self.tick_height)]))?;
                return Ok(());
            }
            // record() might fail if the next Water Clock hash needs to be a tick.  But that's ok, tick()
            // and re-record()
            self.tick();
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_clear_signal(
        tick_height: u64,
        last_entry_hash: Hash,
        start_slot: u64,
        my_leader_slot_index: Option<u64>,
        ticks_per_slot: u64,
        id: &Pubkey,
        block_buffer_pool: &Arc<BlockBufferPool>,
        clear_treasury_signal: Option<SyncSender<bool>>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        waterclock_config: &Arc<WaterClockConfig>,
    ) -> (Self, Receiver<WorkingBankEntries>) {
        let waterclock = Arc::new(Mutex::new(WaterClock::new(
            last_entry_hash,
            waterclock_config.hashes_per_tick,
        )));
        let (sender, receiver) = channel();
        let max_last_leader_grace_ticks = ticks_per_slot / MAX_LAST_LEADER_GRACE_TICKS_FACTOR;
        let (start_leader_at_tick, last_leader_tick) = Self::compute_leader_slot_ticks(
            &my_leader_slot_index,
            ticks_per_slot,
            max_last_leader_grace_ticks,
        );
        (
            Self {
                waterclock,
                tick_height,
                tick_cache: vec![],
                working_treasury: None,
                sender,
                clear_treasury_signal,
                start_slot,
                start_tick: tick_height + 1,
                start_leader_at_tick,
                last_leader_tick,
                max_last_leader_grace_ticks,
                id: *id,
                block_buffer_pool: block_buffer_pool.clone(),
                leader_schedule_cache: leader_schedule_cache.clone(),
                ticks_per_slot,
                waterclock_config: waterclock_config.clone(),
            },
            receiver,
        )
    }

    /// A recorder to synchronize Water Clock with the following data structures
    /// * treasury - the LastId's queue is updated on `tick` and `record` events
    /// * sender - the Entry channel that outputs to the ledger
    pub fn new(
        tick_height: u64,
        last_entry_hash: Hash,
        start_slot: u64,
        my_leader_slot_index: Option<u64>,
        ticks_per_slot: u64,
        id: &Pubkey,
        block_buffer_pool: &Arc<BlockBufferPool>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        waterclock_config: &Arc<WaterClockConfig>,
    ) -> (Self, Receiver<WorkingBankEntries>) {
        Self::new_with_clear_signal(
            tick_height,
            last_entry_hash,
            start_slot,
            my_leader_slot_index,
            ticks_per_slot,
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
    use crate::block_buffer_pool::{get_tmp_ledger_path, BlockBufferPool};
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use crate::test_tx::test_tx;
    use morgan_interface::hash::hash;
    use morgan_interface::timing::DEFAULT_TICKS_PER_SLOT;
    use std::sync::mpsc::sync_channel;

    #[test]
    fn test_waterclock_recorder_no_zero_tick() {
        let prev_hash = Hash::default();
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");

            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_cache.len(), 1);
            assert_eq!(waterclock_recorder.tick_cache[0].1, 1);
            assert_eq!(waterclock_recorder.tick_height, 1);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_tick_height_is_last_tick() {
        let prev_hash = Hash::default();
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");

            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder.tick();
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_cache.len(), 2);
            assert_eq!(waterclock_recorder.tick_cache[1].1, 2);
            assert_eq!(waterclock_recorder.tick_height, 2);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_reset_clears_cache() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                Hash::default(),
                0,
                Some(4),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_cache.len(), 1);
            waterclock_recorder.reset(0, Hash::default(), 0, Some(4), DEFAULT_TICKS_PER_SLOT);
            assert_eq!(waterclock_recorder.tick_cache.len(), 0);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_clear() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let prev_hash = treasury.last_blockhash();
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingBank {
                treasury,
                min_tick_height: 2,
                max_tick_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            assert!(waterclock_recorder.working_treasury.is_some());
            waterclock_recorder.clear_treasury();
            assert!(waterclock_recorder.working_treasury.is_none());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_tick_sent_after_min() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let prev_hash = treasury.last_blockhash();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingBank {
                treasury: treasury.clone(),
                min_tick_height: 2,
                max_tick_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder.tick();
            waterclock_recorder.tick();
            //tick height equal to min_tick_height
            //no tick has been sent
            assert_eq!(waterclock_recorder.tick_cache.last().unwrap().1, 2);
            assert!(entry_receiver.try_recv().is_err());

            // all ticks are sent after height > min
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_height, 3);
            assert_eq!(waterclock_recorder.tick_cache.len(), 0);
            let (bank_, e) = entry_receiver.recv().expect("recv 1");
            assert_eq!(e.len(), 3);
            assert_eq!(bank_.slot(), treasury.slot());
            assert!(waterclock_recorder.working_treasury.is_none());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_tick_sent_upto_and_including_max() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let prev_hash = treasury.last_blockhash();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            waterclock_recorder.tick();
            waterclock_recorder.tick();
            waterclock_recorder.tick();
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_cache.last().unwrap().1, 4);
            assert_eq!(waterclock_recorder.tick_height, 4);

            let working_treasury = WorkingBank {
                treasury,
                min_tick_height: 2,
                max_tick_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder.tick();

            assert_eq!(waterclock_recorder.tick_height, 5);
            assert!(waterclock_recorder.working_treasury.is_none());
            let (_, e) = entry_receiver.recv().expect("recv 1");
            assert_eq!(e.len(), 3);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_record_to_early() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let prev_hash = treasury.last_blockhash();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingBank {
                treasury: treasury.clone(),
                min_tick_height: 2,
                max_tick_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder.tick();
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
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let prev_hash = treasury.last_blockhash();
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingBank {
                treasury: treasury.clone(),
                min_tick_height: 1,
                max_tick_height: 2,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_cache.len(), 1);
            assert_eq!(waterclock_recorder.tick_height, 1);
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
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let prev_hash = treasury.last_blockhash();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingBank {
                treasury: treasury.clone(),
                min_tick_height: 1,
                max_tick_height: 2,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_cache.len(), 1);
            assert_eq!(waterclock_recorder.tick_height, 1);
            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert!(waterclock_recorder
                .record(treasury.slot(), h1, vec![tx.clone()])
                .is_ok());
            assert_eq!(waterclock_recorder.tick_cache.len(), 0);

            //tick in the cache + entry
            let (_b, e) = entry_receiver.recv().expect("recv 1");
            assert_eq!(e.len(), 1);
            assert!(e[0].0.is_tick());
            let (_b, e) = entry_receiver.recv().expect("recv 2");
            assert!(!e[0].0.is_tick());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_recorder_record_at_max_fails() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let prev_hash = treasury.last_blockhash();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingBank {
                treasury: treasury.clone(),
                min_tick_height: 1,
                max_tick_height: 2,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder.tick();
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_height, 2);
            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert!(waterclock_recorder
                .record(treasury.slot(), h1, vec![tx.clone()])
                .is_err());

            let (_bank, e) = entry_receiver.recv().expect("recv 1");
            assert_eq!(e.len(), 2);
            assert!(e[0].0.is_tick());
            assert!(e[1].0.is_tick());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_waterclock_cache_on_disconnect() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let prev_hash = treasury.last_blockhash();
            let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let working_treasury = WorkingBank {
                treasury,
                min_tick_height: 2,
                max_tick_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder.tick();
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_height, 2);
            drop(entry_receiver);
            waterclock_recorder.tick();
            assert!(waterclock_recorder.working_treasury.is_none());
            assert_eq!(waterclock_recorder.tick_cache.len(), 3);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_current() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                Hash::default(),
                0,
                Some(4),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder.tick();
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_cache.len(), 2);
            let hash = waterclock_recorder.waterclock.lock().unwrap().hash;
            waterclock_recorder.reset(
                waterclock_recorder.tick_height,
                hash,
                0,
                Some(4),
                DEFAULT_TICKS_PER_SLOT,
            );
            assert_eq!(waterclock_recorder.tick_cache.len(), 0);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_with_cached() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                Hash::default(),
                0,
                Some(4),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder.tick();
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_cache.len(), 2);
            waterclock_recorder.reset(
                waterclock_recorder.tick_cache[0].1,
                waterclock_recorder.tick_cache[0].0.hash,
                0,
                Some(4),
                DEFAULT_TICKS_PER_SLOT,
            );
            assert_eq!(waterclock_recorder.tick_cache.len(), 0);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_to_new_value() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                Hash::default(),
                0,
                Some(4),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(WaterClockConfig::default()),
            );
            waterclock_recorder.tick();
            waterclock_recorder.tick();
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_cache.len(), 3);
            assert_eq!(waterclock_recorder.tick_height, 3);
            waterclock_recorder.reset(1, hash(b"hello"), 0, Some(4), DEFAULT_TICKS_PER_SLOT);
            assert_eq!(waterclock_recorder.tick_cache.len(), 0);
            waterclock_recorder.tick();
            assert_eq!(waterclock_recorder.tick_height, 2);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_clear_treasury() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                Hash::default(),
                0,
                Some(4),
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );
            let ticks_per_slot = treasury.ticks_per_slot();
            let working_treasury = WorkingBank {
                treasury,
                min_tick_height: 2,
                max_tick_height: 3,
            };
            waterclock_recorder.set_working_treasury(working_treasury);
            waterclock_recorder.reset(1, hash(b"hello"), 0, Some(4), ticks_per_slot);
            assert!(waterclock_recorder.working_treasury.is_none());
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    pub fn test_clear_signal() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let (sender, receiver) = sync_channel(1);
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new_with_clear_signal(
                0,
                Hash::default(),
                0,
                None,
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                Some(sender),
                &Arc::new(LeaderScheduleCache::default()),
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
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let ticks_per_slot = 5;
            let GenesisBlockInfo {
                mut genesis_block, ..
            } = create_genesis_block(2);
            genesis_block.ticks_per_slot = ticks_per_slot;
            let treasury = Arc::new(Bank::new(&genesis_block));

            let prev_hash = treasury.last_blockhash();
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                Some(4),
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            let end_slot = 3;
            let max_tick_height = (end_slot + 1) * ticks_per_slot - 1;
            let working_treasury = WorkingBank {
                treasury: treasury.clone(),
                min_tick_height: 1,
                max_tick_height,
            };

            waterclock_recorder.set_working_treasury(working_treasury);
            for _ in 0..max_tick_height {
                waterclock_recorder.tick();
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
    fn test_reached_leader_tick() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let prev_hash = treasury.last_blockhash();
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                None,
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            // Test that with no leader slot, we don't reach the leader tick
            assert_eq!(waterclock_recorder.reached_leader_tick().0, false);

            waterclock_recorder.reset(
                waterclock_recorder.tick_height(),
                treasury.last_blockhash(),
                0,
                None,
                treasury.ticks_per_slot(),
            );

            // Test that with no leader slot in reset(), we don't reach the leader tick
            assert_eq!(waterclock_recorder.reached_leader_tick().0, false);

            // Provide a leader slot 1 slot down
            waterclock_recorder.reset(
                treasury.ticks_per_slot(),
                treasury.last_blockhash(),
                0,
                Some(2),
                treasury.ticks_per_slot(),
            );

            let init_ticks = waterclock_recorder.tick_height();

            // Send one slot worth of ticks
            for _ in 0..treasury.ticks_per_slot() {
                waterclock_recorder.tick();
            }

            // Tick should be recorded
            assert_eq!(
                waterclock_recorder.tick_height(),
                init_ticks + treasury.ticks_per_slot()
            );

            // Test that we don't reach the leader tick because of grace ticks
            assert_eq!(waterclock_recorder.reached_leader_tick().0, false);

            // reset waterclock now. it should discard the grace ticks wait
            waterclock_recorder.reset(
                waterclock_recorder.tick_height(),
                treasury.last_blockhash(),
                1,
                Some(2),
                treasury.ticks_per_slot(),
            );
            // without sending more ticks, we should be leader now
            assert_eq!(waterclock_recorder.reached_leader_tick().0, true);
            assert_eq!(waterclock_recorder.reached_leader_tick().1, 0);

            // Now test that with grace ticks we can reach leader ticks
            // Set the leader slot 1 slot down
            waterclock_recorder.reset(
                waterclock_recorder.tick_height(),
                treasury.last_blockhash(),
                2,
                Some(3),
                treasury.ticks_per_slot(),
            );

            // Send one slot worth of ticks
            for _ in 0..treasury.ticks_per_slot() {
                waterclock_recorder.tick();
            }

            // We are not the leader yet, as expected
            assert_eq!(waterclock_recorder.reached_leader_tick().0, false);

            // Send 1 less tick than the grace ticks
            for _ in 0..treasury.ticks_per_slot() / MAX_LAST_LEADER_GRACE_TICKS_FACTOR - 1 {
                waterclock_recorder.tick();
            }
            // We are still not the leader
            assert_eq!(waterclock_recorder.reached_leader_tick().0, false);

            // Send one more tick
            waterclock_recorder.tick();

            // We should be the leader now
            assert_eq!(waterclock_recorder.reached_leader_tick().0, true);
            assert_eq!(
                waterclock_recorder.reached_leader_tick().1,
                treasury.ticks_per_slot() / MAX_LAST_LEADER_GRACE_TICKS_FACTOR
            );

            // Let's test that correct grace ticks are reported
            // Set the leader slot 1 slot down
            waterclock_recorder.reset(
                waterclock_recorder.tick_height(),
                treasury.last_blockhash(),
                3,
                Some(4),
                treasury.ticks_per_slot(),
            );

            // Send remaining ticks for the slot (remember we sent extra ticks in the previous part of the test)
            for _ in
                treasury.ticks_per_slot() / MAX_LAST_LEADER_GRACE_TICKS_FACTOR..treasury.ticks_per_slot()
            {
                waterclock_recorder.tick();
            }

            // Send one extra tick before resetting (so that there's one grace tick)
            waterclock_recorder.tick();

            // We are not the leader yet, as expected
            assert_eq!(waterclock_recorder.reached_leader_tick().0, false);
            waterclock_recorder.reset(
                waterclock_recorder.tick_height(),
                treasury.last_blockhash(),
                3,
                Some(4),
                treasury.ticks_per_slot(),
            );
            // without sending more ticks, we should be leader now
            assert_eq!(waterclock_recorder.reached_leader_tick().0, true);
            assert_eq!(waterclock_recorder.reached_leader_tick().1, 1);

            // Let's test that if a node overshoots the ticks for its target
            // leader slot, reached_leader_tick() will return false
            // Set the leader slot 1 slot down
            waterclock_recorder.reset(
                waterclock_recorder.tick_height(),
                treasury.last_blockhash(),
                4,
                Some(5),
                treasury.ticks_per_slot(),
            );

            // Send remaining ticks for the slot (remember we sent extra ticks in the previous part of the test)
            for _ in 0..4 * treasury.ticks_per_slot() {
                waterclock_recorder.tick();
            }

            // We are not the leader, as expected
            assert_eq!(waterclock_recorder.reached_leader_tick().0, false);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_would_be_leader_soon() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
            let treasury = Arc::new(Bank::new(&genesis_block));
            let prev_hash = treasury.last_blockhash();
            let (mut waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                0,
                prev_hash,
                0,
                None,
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_treasury(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );

            // Test that with no leader slot, we don't reach the leader tick
            assert_eq!(
                waterclock_recorder.would_be_leader(2 * treasury.ticks_per_slot()),
                false
            );

            waterclock_recorder.reset(
                waterclock_recorder.tick_height(),
                treasury.last_blockhash(),
                0,
                None,
                treasury.ticks_per_slot(),
            );

            assert_eq!(
                waterclock_recorder.would_be_leader(2 * treasury.ticks_per_slot()),
                false
            );

            // We reset with leader slot after 3 slots
            waterclock_recorder.reset(
                waterclock_recorder.tick_height(),
                treasury.last_blockhash(),
                0,
                Some(treasury.slot() + 3),
                treasury.ticks_per_slot(),
            );

            // Test that the node won't be leader in next 2 slots
            assert_eq!(
                waterclock_recorder.would_be_leader(2 * treasury.ticks_per_slot()),
                false
            );

            // Test that the node will be leader in next 3 slots
            assert_eq!(
                waterclock_recorder.would_be_leader(3 * treasury.ticks_per_slot()),
                true
            );

            assert_eq!(
                waterclock_recorder.would_be_leader(2 * treasury.ticks_per_slot()),
                false
            );

            // If we set the working treasury, the node should be leader within next 2 slots
            waterclock_recorder.set_treasury(&treasury);
            assert_eq!(
                waterclock_recorder.would_be_leader(2 * treasury.ticks_per_slot()),
                true
            );
        }
    }
}
