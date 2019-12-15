//! The `replay_stage` replays transactions broadcast by the leader.

// use crate::treasury_forks::BankForks;
use crate::treasury_forks::BankForks;
use crate::block_buffer_pool::BlockBufferPool;
use crate::block_buffer_pool_processor;
use crate::node_group_info::NodeGroupInfo;
use crate::entry_info::{Entry, EntrySlice};
use crate::leader_arrange_cache::LeaderScheduleCache;
use crate::leader_arrange_utils;
use crate::fork_selection::{Locktower, StakeLockout};
use crate::packet::BlobError;
use crate::water_clock_recorder::WaterClockRecorder;
use crate::result::{Error, Result};
use crate::rpc_subscriptions::RpcSubscriptions;
use crate::service::Service;
use hashbrown::HashMap;
use morgan_metricbot::{datapoint_warn, inc_new_counter_error, inc_new_counter_info};
use morgan_runtime::treasury::Bank;
use morgan_interface::hash::Hash;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::signature::KeypairUtil;
use morgan_interface::timing::{self, duration_as_ms};
use morgan_interface::transaction::Transaction;
use morgan_vote_api::vote_instruction;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;
use std::time::Instant;
use morgan_helper::logHelper::*;
use chrono::prelude::*;

pub const MAX_ENTRY_RECV_PER_ITER: usize = 512;

// Implement a destructor for the ReplayStage thread to signal it exited
// even on panics
struct Finalizer {
    exit_sender: Arc<AtomicBool>,
}

impl Finalizer {
    fn new(exit_sender: Arc<AtomicBool>) -> Self {
        Finalizer { exit_sender }
    }
}
// Implement a destructor for Finalizer.
impl Drop for Finalizer {
    fn drop(&mut self) {
        self.exit_sender.clone().store(true, Ordering::Relaxed);
    }
}

pub struct ReplayStage {
    t_replay: JoinHandle<Result<()>>,
}

#[derive(Default)]
struct ForkProgress {
    last_entry: Hash,
    num_blobs: usize,
    started_ms: u64,
}
impl ForkProgress {
    pub fn new(last_entry: Hash) -> Self {
        Self {
            last_entry,
            num_blobs: 0,
            started_ms: timing::timestamp(),
        }
    }
}

impl ReplayStage {
    #[allow(clippy::new_ret_no_self, clippy::too_many_arguments)]
    pub fn new<T>(
        my_pubkey: &Pubkey,
        vote_account: &Pubkey,
        voting_keypair: Option<&Arc<T>>,
        block_buffer_pool: Arc<BlockBufferPool>,
        treasury_forks: &Arc<RwLock<BankForks>>,
        node_group_info: Arc<RwLock<NodeGroupInfo>>,
        exit: &Arc<AtomicBool>,
        ledger_signal_receiver: Receiver<bool>,
        subscriptions: &Arc<RpcSubscriptions>,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
    ) -> (Self, Receiver<(u64, Pubkey)>, Receiver<Vec<u64>>)
    where
        T: 'static + KeypairUtil + Send + Sync,
    {
        let (root_slot_sender, root_slot_receiver) = channel();
        let (slot_full_sender, slot_full_receiver) = channel();
        trace!("replay stage");
        let exit_ = exit.clone();
        let subscriptions = subscriptions.clone();
        let treasury_forks = treasury_forks.clone();
        let waterclock_recorder = waterclock_recorder.clone();
        let my_pubkey = *my_pubkey;
        let mut ticks_per_slot = 0;
        let mut locktower = Locktower::new_from_forks(&treasury_forks.read().unwrap(), &my_pubkey);
        // Start the replay stage loop
        let leader_schedule_cache = leader_schedule_cache.clone();
        let vote_account = *vote_account;
        let voting_keypair = voting_keypair.cloned();
        let t_replay = Builder::new()
            .name("morgan-replay-stage".to_string())
            .spawn(move || {
                let _exit = Finalizer::new(exit_.clone());
                let mut progress = HashMap::new();
                loop {
                    let now = Instant::now();
                    // Stop getting entries if we get exit signal
                    if exit_.load(Ordering::Relaxed) {
                        break;
                    }

                    Self::generate_new_treasury_forks(
                        &block_buffer_pool,
                        &mut treasury_forks.write().unwrap(),
                        &leader_schedule_cache,
                    );

                    let mut is_tpu_treasury_active = waterclock_recorder.lock().unwrap().treasury().is_some();

                    Self::replay_active_treasuries(
                        &block_buffer_pool,
                        &treasury_forks,
                        &my_pubkey,
                        &mut ticks_per_slot,
                        &mut progress,
                        &slot_full_sender,
                    )?;

                    if ticks_per_slot == 0 {
                        let frozen_treasuries = treasury_forks.read().unwrap().frozen_treasuries();
                        let treasury = frozen_treasuries.values().next().unwrap();
                        ticks_per_slot = treasury.ticks_per_slot();
                    }

                    let votable =
                        Self::generate_votable_treasuries(&treasury_forks, &locktower, &mut progress);

                    if let Some((_, treasury)) = votable.last() {
                        subscriptions.notify_subscribers(treasury.slot(), &treasury_forks);

                        Self::handle_votable_treasury(
                            &treasury,
                            &treasury_forks,
                            &mut locktower,
                            &mut progress,
                            &vote_account,
                            &voting_keypair,
                            &node_group_info,
                            &block_buffer_pool,
                            &leader_schedule_cache,
                            &root_slot_sender,
                        )?;

                        Self::reset_waterclock_recorder(
                            &my_pubkey,
                            &block_buffer_pool,
                            &treasury,
                            &waterclock_recorder,
                            ticks_per_slot,
                            &leader_schedule_cache,
                        );

                        is_tpu_treasury_active = false;
                    }

                    let (reached_leader_tick, grace_ticks) = if !is_tpu_treasury_active {
                        let waterclock = waterclock_recorder.lock().unwrap();
                        waterclock.reached_leader_tick()
                    } else {
                        (false, 0)
                    };

                    if !is_tpu_treasury_active {
                        assert!(ticks_per_slot > 0);
                        let waterclock_tick_height = waterclock_recorder.lock().unwrap().tick_height();
                        let waterclock_slot = leader_arrange_utils::tick_height_to_slot(
                            ticks_per_slot,
                            waterclock_tick_height + 1,
                        );
                        Self::start_leader(
                            &my_pubkey,
                            &treasury_forks,
                            &waterclock_recorder,
                            &node_group_info,
                            waterclock_slot,
                            reached_leader_tick,
                            grace_ticks,
                            &leader_schedule_cache,
                        );
                    }

                    inc_new_counter_info!(
                        "replicate_stage-duration",
                        duration_as_ms(&now.elapsed()) as usize
                    );
                    let timer = Duration::from_millis(100);
                    let result = ledger_signal_receiver.recv_timeout(timer);
                    match result {
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(_) => break,
                        Ok(_) => trace!("block_buffer_pool signal"),
                    };
                }
                Ok(())
            })
            .unwrap();
        (Self { t_replay }, slot_full_receiver, root_slot_receiver)
    }
    pub fn start_leader(
        my_pubkey: &Pubkey,
        treasury_forks: &Arc<RwLock<BankForks>>,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        waterclock_slot: u64,
        reached_leader_tick: bool,
        grace_ticks: u64,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
    ) {
        trace!("{} checking waterclock slot {}", my_pubkey, waterclock_slot);
        if treasury_forks.read().unwrap().get(waterclock_slot).is_none() {
            let parent_slot = waterclock_recorder.lock().unwrap().start_slot();
            let parent = {
                let r_bf = treasury_forks.read().unwrap();
                r_bf.get(parent_slot)
                    .expect("start slot doesn't exist in treasury forks")
                    .clone()
            };
            assert!(parent.is_frozen());

            leader_schedule_cache.slot_leader_at(waterclock_slot, Some(&parent))
                .map(|next_leader| {
                    debug!(
                        "me: {} leader {} at waterclock slot {}",
                        my_pubkey, next_leader, waterclock_slot
                    );
                    node_group_info.write().unwrap().set_leader(&next_leader);
                    if next_leader == *my_pubkey && reached_leader_tick {
                        debug!("{} starting tpu for slot {}", my_pubkey, waterclock_slot);
                        datapoint_warn!(
                            "replay_stage-new_leader",
                            ("count", waterclock_slot, i64),
                            ("grace", grace_ticks, i64));
                        let tpu_treasury = Bank::new_from_parent(&parent, my_pubkey, waterclock_slot);
                        treasury_forks.write().unwrap().insert(tpu_treasury);
                        if let Some(tpu_treasury) = treasury_forks.read().unwrap().get(waterclock_slot).cloned() {
                            assert_eq!(
                                treasury_forks.read().unwrap().working_treasury().slot(),
                                tpu_treasury.slot()
                            );
                            debug!(
                                "waterclock_recorder new working treasury: me: {} next_slot: {} next_leader: {}",
                                my_pubkey,
                                tpu_treasury.slot(),
                                next_leader
                            );
                            waterclock_recorder.lock().unwrap().set_treasury(&tpu_treasury);
                        }
                    }
                })
                .or_else(|| {
                    // warn!("{} No next leader found", my_pubkey);
                    println!(
                        "{}",
                        Warn(
                            format!("{} No next leader found", my_pubkey).to_string(),
                            module_path!().to_string()
                        )
                    );
                    None
                });
        }
    }
    fn replay_block_buffer_into_treasury(
        treasury: &Bank,
        block_buffer_pool: &BlockBufferPool,
        progress: &mut HashMap<u64, ForkProgress>,
    ) -> Result<()> {
        let (entries, num) = Self::load_block_buffer_entries(treasury, block_buffer_pool, progress)?;
        let len = entries.len();
        let result = Self::replay_entries_into_treasury(treasury, entries, progress, num);
        if result.is_ok() {
            trace!("verified entries {}", len);
            inc_new_counter_info!("replicate-stage_process_entries", len);
        } else {
            // info!("{}", Info(format!("debug to verify entries {}", len).to_string()));
            println!("{}",
                printLn(
                    format!("debug to verify entries {}", len).to_string(),
                    module_path!().to_string()
                )
            );
            //TODO: mark this fork as failed
            inc_new_counter_error!("replicate-stage_failed_process_entries", len);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_votable_treasury<T>(
        treasury: &Arc<Bank>,
        treasury_forks: &Arc<RwLock<BankForks>>,
        locktower: &mut Locktower,
        progress: &mut HashMap<u64, ForkProgress>,
        vote_account: &Pubkey,
        voting_keypair: &Option<Arc<T>>,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        block_buffer_pool: &Arc<BlockBufferPool>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        root_slot_sender: &Sender<Vec<u64>>,
    ) -> Result<()>
    where
        T: 'static + KeypairUtil + Send + Sync,
    {
        if let Some(new_root) = locktower.record_vote(treasury.slot(), treasury.hash()) {
            // get the root treasury before squash
            let root_treasury = treasury_forks
                .read()
                .unwrap()
                .get(new_root)
                .expect("Root treasury doesn't exist")
                .clone();
            let mut rooted_slots = root_treasury
                .parents()
                .into_iter()
                .map(|treasury| treasury.slot())
                .collect::<Vec<_>>();
            rooted_slots.push(root_treasury.slot());
            let old_root = treasury_forks.read().unwrap().root();
            block_buffer_pool
                .set_genesis(new_root, old_root)
                .expect("Ledger set root failed");
            // Set root first in leader schedule_cache before treasury_forks because treasury_forks.root
            // is consumed by repair_service to update gossip, so we don't want to get blobs for
            // repair on gossip before we update leader schedule, otherwise they may get dropped.
            leader_schedule_cache.set_genesis(new_root);
            treasury_forks.write().unwrap().set_genesis(new_root);
            Self::handle_new_root(&treasury_forks, progress);
            root_slot_sender.send(rooted_slots)?;
        }
        locktower.update_epoch(&treasury);
        if let Some(ref voting_keypair) = voting_keypair {
            let node_keypair = node_group_info.read().unwrap().keypair.clone();

            // Send our last few votes along with the new one
            let vote_ix = vote_instruction::vote(
                &node_keypair.pubkey(),
                &vote_account,
                &voting_keypair.pubkey(),
                locktower.recent_votes(),
            );

            let mut vote_tx = Transaction::new_unsigned_instructions(vec![vote_ix]);
            let blockhash = treasury.last_blockhash();
            vote_tx.partial_sign(&[node_keypair.as_ref()], blockhash);
            vote_tx.partial_sign(&[voting_keypair.as_ref()], blockhash);
            node_group_info.write().unwrap().push_vote(vote_tx);
        }
        Ok(())
    }

    fn reset_waterclock_recorder(
        my_pubkey: &Pubkey,
        block_buffer_pool: &BlockBufferPool,
        treasury: &Arc<Bank>,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
        ticks_per_slot: u64,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
    ) {
        let next_leader_slot =
            leader_schedule_cache.next_leader_slot(&my_pubkey, treasury.slot(), &treasury, Some(block_buffer_pool));
        waterclock_recorder.lock().unwrap().reset(
            treasury.tick_height(),
            treasury.last_blockhash(),
            treasury.slot(),
            next_leader_slot,
            ticks_per_slot,
        );
        debug!(
            "{:?} voted and reset waterclock at {}. next leader slot {:?}",
            my_pubkey,
            treasury.tick_height(),
            next_leader_slot
        );
    }

    fn replay_active_treasuries(
        block_buffer_pool: &Arc<BlockBufferPool>,
        treasury_forks: &Arc<RwLock<BankForks>>,
        my_pubkey: &Pubkey,
        ticks_per_slot: &mut u64,
        progress: &mut HashMap<u64, ForkProgress>,
        slot_full_sender: &Sender<(u64, Pubkey)>,
    ) -> Result<()> {
        let active_treasuries = treasury_forks.read().unwrap().active_treasuries();
        trace!("active treasuries {:?}", active_treasuries);

        for treasury_slot in &active_treasuries {
            let treasury = treasury_forks.read().unwrap().get(*treasury_slot).unwrap().clone();
            *ticks_per_slot = treasury.ticks_per_slot();
            if treasury.collector_id() != *my_pubkey {
                Self::replay_block_buffer_into_treasury(&treasury, &block_buffer_pool, progress)?;
            }
            let max_tick_height = (*treasury_slot + 1) * treasury.ticks_per_slot() - 1;
            if treasury.tick_height() == max_tick_height {
                Self::process_completed_bank(my_pubkey, treasury, slot_full_sender);
            }
        }
        Ok(())
    }

    fn generate_votable_treasuries(
        treasury_forks: &Arc<RwLock<BankForks>>,
        locktower: &Locktower,
        progress: &mut HashMap<u64, ForkProgress>,
    ) -> Vec<(u128, Arc<Bank>)> {
        let locktower_start = Instant::now();
        // Locktower voting
        let descendants = treasury_forks.read().unwrap().descendants();
        let ancestors = treasury_forks.read().unwrap().ancestors();
        let frozen_treasuries = treasury_forks.read().unwrap().frozen_treasuries();

        trace!("frozen_treasuries {}", frozen_treasuries.len());
        let mut votable: Vec<(u128, Arc<Bank>)> = frozen_treasuries
            .values()
            .filter(|b| {
                let is_votable = b.is_votable();
                trace!("treasury is votable: {} {}", b.slot(), is_votable);
                is_votable
            })
            .filter(|b| {
                let is_recent_epoch = locktower.is_recent_epoch(b);
                trace!("treasury is is_recent_epoch: {} {}", b.slot(), is_recent_epoch);
                is_recent_epoch
            })
            .filter(|b| {
                let has_voted = locktower.has_voted(b.slot());
                trace!("treasury is has_voted: {} {}", b.slot(), has_voted);
                !has_voted
            })
            .filter(|b| {
                let is_locked_out = locktower.is_locked_out(b.slot(), &descendants);
                trace!("treasury is is_locked_out: {} {}", b.slot(), is_locked_out);
                !is_locked_out
            })
            .map(|treasury| {
                (
                    treasury,
                    locktower.collect_vote_lockouts(
                        treasury.slot(),
                        treasury.vote_accounts().into_iter(),
                        &ancestors,
                    ),
                )
            })
            .filter(|(b, stake_lockouts)| {
                let vote_threshold =
                    locktower.check_vote_stake_threshold(b.slot(), &stake_lockouts);
                Self::confirm_forks(locktower, stake_lockouts, progress, treasury_forks);
                debug!("treasury vote_threshold: {} {}", b.slot(), vote_threshold);
                vote_threshold
            })
            .map(|(b, stake_lockouts)| (locktower.calculate_weight(&stake_lockouts), b.clone()))
            .collect();

        votable.sort_by_key(|b| b.0);
        let ms = timing::duration_as_ms(&locktower_start.elapsed());

        trace!("votable_banks {}", votable.len());
        if !votable.is_empty() {
            let weights: Vec<u128> = votable.iter().map(|x| x.0).collect();
            // info!(
            //     "{}",
            //     Info(format!("@{:?} locktower duration: {:?} len: {} weights: {:?}",
            //     timing::timestamp(),
            //     ms,
            //     votable.len(),
            //     weights).to_string())
            // );
            // let local: DateTime<Local> = Local::now();
            // println!(
            //     "< {} {} {} {} >",
            //     Info(format!("{}", local).to_string()),
            //     Info(format!("INFO").to_string()),
            //     Info(format!("@{:?} locktower duration: {:?} len: {} weights: {:?}",
            //         timing::timestamp(),
            //         ms,
            //         votable.len(),
            //         weights).to_string()
            //     ),
            //     Info(format!("morgan::replay_stage").to_string())
            // );
            let info: String = format!("@{:?} fork selection duration: {:?} num: {} weights: {:?}",
                timing::timestamp(),
                ms,
                votable.len(),
                weights).to_string();
            println!("{}", printLn(info, module_path!().to_string()));
        }
        inc_new_counter_info!("replay_stage-locktower_duration", ms as usize);

        votable
    }

    fn confirm_forks(
        locktower: &Locktower,
        stake_lockouts: &HashMap<u64, StakeLockout>,
        progress: &mut HashMap<u64, ForkProgress>,
        treasury_forks: &Arc<RwLock<BankForks>>,
    ) {
        progress.retain(|slot, prog| {
            let duration = timing::timestamp() - prog.started_ms;
            if locktower.is_slot_confirmed(*slot, stake_lockouts)
                && treasury_forks
                    .read()
                    .unwrap()
                    .get(*slot)
                    .map(|s| s.is_frozen())
                    .unwrap_or(true)
            {
                // info!("{}", Info(format!("validator fork confirmed {} {}", *slot, duration).to_string()));
                println!("{}",
                    printLn(
                        format!("validator fork confirmed {} {}", *slot, duration).to_string(),
                        module_path!().to_string()
                    )
                );
                datapoint_warn!("validator-confirmation", ("duration_ms", duration, i64));
                false
            } else {
                debug!(
                    "validator fork not confirmed {} {} {:?}",
                    *slot,
                    duration,
                    stake_lockouts.get(slot)
                );
                true
            }
        });
    }

    fn load_block_buffer_entries(
        treasury: &Bank,
        block_buffer_pool: &BlockBufferPool,
        progress: &mut HashMap<u64, ForkProgress>,
    ) -> Result<(Vec<Entry>, usize)> {
        let treasury_slot = treasury.slot();
        let treasury_progress = &mut progress
            .entry(treasury_slot)
            .or_insert(ForkProgress::new(treasury.last_blockhash()));
        block_buffer_pool.fetch_slot_entries_by_blob_len(treasury_slot, treasury_progress.num_blobs as u64, None)
    }

    fn replay_entries_into_treasury(
        treasury: &Bank,
        entries: Vec<Entry>,
        progress: &mut HashMap<u64, ForkProgress>,
        num: usize,
    ) -> Result<()> {
        let treasury_progress = &mut progress
            .entry(treasury.slot())
            .or_insert(ForkProgress::new(treasury.last_blockhash()));
        let result = Self::verify_and_process_entries(&treasury, &entries, &treasury_progress.last_entry);
        treasury_progress.num_blobs += num;
        if let Some(last_entry) = entries.last() {
            treasury_progress.last_entry = last_entry.hash;
        }
        result
    }

    pub fn verify_and_process_entries(
        treasury: &Bank,
        entries: &[Entry],
        last_entry: &Hash,
    ) -> Result<()> {
        if !entries.verify(last_entry) {
            trace!(
                "entry verification failed {} {} {} {}",
                entries.len(),
                treasury.tick_height(),
                last_entry,
                treasury.last_blockhash()
            );
            return Err(Error::BlobError(BlobError::VerificationFailed));
        }
        block_buffer_pool_processor::process_entries(treasury, entries)?;

        Ok(())
    }

    fn handle_new_root(
        treasury_forks: &Arc<RwLock<BankForks>>,
        progress: &mut HashMap<u64, ForkProgress>,
    ) {
        let r_treasury_forks = treasury_forks.read().unwrap();
        progress.retain(|k, _| r_treasury_forks.get(*k).is_some());
    }

    fn process_completed_bank(
        my_pubkey: &Pubkey,
        treasury: Arc<Bank>,
        slot_full_sender: &Sender<(u64, Pubkey)>,
    ) {
        treasury.freeze();
        // info!("{}", Info(format!("treasury frozen {}", treasury.slot()).to_string()));
        let info:String = format!(
            "treasury locked {}",
            treasury.slot()
        ).to_string();
        println!("{}", printLn(info, module_path!().to_string()));

        if let Err(e) = slot_full_sender.send((treasury.slot(), treasury.collector_id())) {
            trace!("{} slot_full alert failed: {:?}", my_pubkey, e);
        }
    }

    fn generate_new_treasury_forks(
        block_buffer_pool: &BlockBufferPool,
        forks: &mut BankForks,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
    ) {
        // Find the next slot that chains to the old slot
        let frozen_treasuries = forks.frozen_treasuries();
        let frozen_bank_slots: Vec<u64> = frozen_treasuries.keys().cloned().collect();
        trace!("frozen_treasuries {:?}", frozen_bank_slots);
        let next_slots = block_buffer_pool
            .fetch_slot_from(&frozen_bank_slots)
            .expect("Db error");
        // Filter out what we've already seen
        trace!("generate new forks {:?}", next_slots);
        for (parent_id, children) in next_slots {
            let parent_bank = frozen_treasuries
                .get(&parent_id)
                .expect("missing parent in treasury forks")
                .clone();
            for child_id in children {
                if forks.get(child_id).is_some() {
                    trace!("child already active or frozen {}", child_id);
                    continue;
                }
                let leader = leader_schedule_cache
                    .slot_leader_at(child_id, Some(&parent_bank))
                    .unwrap();
                // info!("{}", Info(format!("new fork:{} parent:{}", child_id, parent_id).to_string()));
                println!("{}",
                    printLn(
                        format!("new fork:{} parent:{}", child_id, parent_id).to_string(),
                        module_path!().to_string()
                    )
                );
                forks.insert(Bank::new_from_parent(&parent_bank, &leader, child_id));
            }
        }
    }
}

impl Service for ReplayStage {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.t_replay.join().map(|_| ())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::block_buffer_pool::get_tmp_ledger_path;
    use crate::genesis_utils::create_genesis_block;
    use crate::packet::Blob;
    use crate::repeat_stage::ReplayStage;
    use morgan_interface::hash::Hash;
    use std::fs::remove_dir_all;
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_child_slots_of_same_parent() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = Arc::new(
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger"),
            );

            let genesis_block = create_genesis_block(10_000).genesis_block;
            let treasury0 = Bank::new(&genesis_block);
            let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_treasury(&treasury0));
            let mut treasury_forks = BankForks::new(0, treasury0);
            treasury_forks.working_treasury().freeze();

            // Insert blob for slot 1, generate new forks, check result
            let mut blob_slot_1 = Blob::default();
            blob_slot_1.set_slot(1);
            blob_slot_1.set_parent(0);
            block_buffer_pool.insert_data_blobs(&vec![blob_slot_1]).unwrap();
            assert!(treasury_forks.get(1).is_none());
            ReplayStage::generate_new_treasury_forks(
                &block_buffer_pool,
                &mut treasury_forks,
                &leader_schedule_cache,
            );
            assert!(treasury_forks.get(1).is_some());

            // Insert blob for slot 3, generate new forks, check result
            let mut blob_slot_2 = Blob::default();
            blob_slot_2.set_slot(2);
            blob_slot_2.set_parent(0);
            block_buffer_pool.insert_data_blobs(&vec![blob_slot_2]).unwrap();
            assert!(treasury_forks.get(2).is_none());
            ReplayStage::generate_new_treasury_forks(
                &block_buffer_pool,
                &mut treasury_forks,
                &leader_schedule_cache,
            );
            assert!(treasury_forks.get(1).is_some());
            assert!(treasury_forks.get(2).is_some());
        }

        let _ignored = remove_dir_all(&ledger_path);
    }

    #[test]
    fn test_handle_new_root() {
        let genesis_block = create_genesis_block(10_000).genesis_block;
        let treasury0 = Bank::new(&genesis_block);
        let treasury_forks = Arc::new(RwLock::new(BankForks::new(0, treasury0)));
        let mut progress = HashMap::new();
        progress.insert(5, ForkProgress::new(Hash::default()));
        ReplayStage::handle_new_root(&treasury_forks, &mut progress);
        assert!(progress.is_empty());
    }
}
