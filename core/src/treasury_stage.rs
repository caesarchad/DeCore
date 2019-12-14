//! The `treasury_phase` processes Transaction messages. It is intended to be used
//! to contruct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.
use crate::block_buffer_pool::BlockBufferPool;
use crate::node_group_info::NodeGroupInfo;
use crate::entry_info;
use crate::entry_info::{hash_transactions, Entry};
use crate::leader_arrange_cache::LeaderScheduleCache;
use crate::packet;
use crate::packet::{Packet, Packets};
use crate::water_clock_recorder::{WaterClockRecorder, WaterClockRecorderErr, WorkingBankEntries};
use crate::water_clock_service::WaterClockService;
use crate::result::{Error, Result};
use crate::service::Service;
use crate::signature_verify_stage::VerifiedPackets;
use bincode::deserialize;
use itertools::Itertools;
use morgan_metricbot::{inc_new_counter_debug, inc_new_counter_info, inc_new_counter_warn};
use morgan_runtime::accounts_db::ErrorCounters;
use morgan_runtime::treasury::Bank;
use morgan_runtime::locked_accounts_results::LockedAccountsResults;
use morgan_interface::waterclock_config::WaterClockConfig;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::timing::{
    self, duration_as_us, DEFAULT_TICKS_PER_SLOT, MAX_RECENT_BLOCKHASHES,
    MAX_TRANSACTION_FORWARDING_DELAY,
};
use morgan_interface::transaction::{self, Transaction, TransactionError};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;
use std::time::Instant;
use sys_info;
use morgan_helper::logHelper::*;

type PacketsAndOffsets = (Packets, Vec<usize>);
pub type UnprocessedPackets = Vec<PacketsAndOffsets>;

// number of threads is 1 until mt treasury is ready
pub const NUM_THREADS: u32 = 10;

/// Stores the stage's thread handle and output receiver.
pub struct BankingStage {
    bank_thread_hdls: Vec<JoinHandle<()>>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BufferedPacketsDecision {
    Consume,
    Forward,
    Hold,
}

impl BankingStage {
    /// Create the stage using `treasury`. Exit when `verified_receiver` is dropped.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
        verified_receiver: Receiver<VerifiedPackets>,
        verified_vote_receiver: Receiver<VerifiedPackets>,
    ) -> Self {
        Self::new_num_threads(
            node_group_info,
            waterclock_recorder,
            verified_receiver,
            verified_vote_receiver,
            2, // 1 for voting, 1 for banking.
               // More than 2 threads is slower in testnet testing.
        )
    }

    fn new_num_threads(
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
        verified_receiver: Receiver<VerifiedPackets>,
        verified_vote_receiver: Receiver<VerifiedPackets>,
        num_threads: u32,
    ) -> Self {
        let verified_receiver = Arc::new(Mutex::new(verified_receiver));
        let verified_vote_receiver = Arc::new(Mutex::new(verified_vote_receiver));

        // Single thread to generate entries from many banks.
        // This thread talks to waterclock_service and broadcasts the entries once they have been recorded.
        // Once an entry has been recorded, its blockhash is registered with the treasury.
        let exit = Arc::new(AtomicBool::new(false));

        // Many banks that process transactions in parallel.
        let bank_thread_hdls: Vec<JoinHandle<()>> = (0..num_threads)
            .map(|i| {
                let (verified_receiver, enable_forwarding) = if i < num_threads - 1 {
                    (verified_receiver.clone(), true)
                } else {
                    // Disable forwarding of vote transactions, as votes are gossiped
                    (verified_vote_receiver.clone(), false)
                };

                let waterclock_recorder = waterclock_recorder.clone();
                let node_group_info = node_group_info.clone();
                let exit = exit.clone();
                let mut recv_start = Instant::now();
                Builder::new()
                    .name("morgan-banking-stage-tx".to_string())
                    .spawn(move || {
                        Self::process_loop(
                            &verified_receiver,
                            &waterclock_recorder,
                            &node_group_info,
                            &mut recv_start,
                            enable_forwarding,
                            i,
                        );
                        exit.store(true, Ordering::Relaxed);
                    })
                    .unwrap()
            })
            .collect();
        Self { bank_thread_hdls }
    }

    fn filter_valid_packets_for_forwarding(all_packets: &[PacketsAndOffsets]) -> Vec<&Packet> {
        all_packets
            .iter()
            .flat_map(|(p, valid_indexes)| valid_indexes.iter().map(move |x| &p.packets[*x]))
            .collect()
    }

    fn forward_buffered_packets(
        socket: &std::net::UdpSocket,
        tpu_via_blobs: &std::net::SocketAddr,
        unprocessed_packets: &[PacketsAndOffsets],
    ) -> std::io::Result<()> {
        let packets = Self::filter_valid_packets_for_forwarding(unprocessed_packets);
        inc_new_counter_info!("treasury_phase-forwarded_packets", packets.len());
        let blobs = packet::packets_to_blobs(&packets);

        for blob in blobs {
            socket.send_to(&blob.data[..blob.meta.size], tpu_via_blobs)?;
        }

        Ok(())
    }

    pub fn consume_buffered_packets(
        my_pubkey: &Pubkey,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
        buffered_packets: &mut Vec<PacketsAndOffsets>,
    ) -> Result<UnprocessedPackets> {
        let mut unprocessed_packets = vec![];
        let mut rebuffered_packets = 0;
        let mut new_tx_count = 0;
        let buffered_len = buffered_packets.len();
        let mut buffered_packets_iter = buffered_packets.drain(..);

        let proc_start = Instant::now();
        while let Some((msgs, unprocessed_indexes)) = buffered_packets_iter.next() {
            let treasury = waterclock_recorder.lock().unwrap().treasury();
            if treasury.is_none() {
                rebuffered_packets += unprocessed_indexes.len();
                Self::push_unprocessed(&mut unprocessed_packets, msgs, unprocessed_indexes);
                continue;
            }
            let treasury = treasury.unwrap();

            let (processed, verified_txs_len, new_unprocessed_indexes) =
                Self::process_received_packets(
                    &treasury,
                    &waterclock_recorder,
                    &msgs,
                    unprocessed_indexes.to_owned(),
                )?;

            new_tx_count += processed;

            // Collect any unprocessed transactions in this batch for forwarding
            rebuffered_packets += new_unprocessed_indexes.len();
            Self::push_unprocessed(&mut unprocessed_packets, msgs, new_unprocessed_indexes);

            if processed < verified_txs_len {
                let next_leader = waterclock_recorder.lock().unwrap().next_slot_leader();
                // Walk thru rest of the transactions and filter out the invalid (e.g. too old) ones
                while let Some((msgs, unprocessed_indexes)) = buffered_packets_iter.next() {
                    let unprocessed_indexes = Self::filter_unprocessed_packets(
                        &treasury,
                        &msgs,
                        &unprocessed_indexes,
                        my_pubkey,
                        next_leader,
                    );
                    Self::push_unprocessed(&mut unprocessed_packets, msgs, unprocessed_indexes);
                }
            }
        }

        let total_time_s = timing::duration_as_s(&proc_start.elapsed());
        let total_time_ms = timing::duration_as_ms(&proc_start.elapsed());

        debug!(
            "@{:?} done processing buffered batches: {} time: {:?}ms tx count: {} tx/s: {}",
            timing::timestamp(),
            buffered_len,
            total_time_ms,
            new_tx_count,
            (new_tx_count as f32) / (total_time_s)
        );

        inc_new_counter_info!("treasury_phase-rebuffered_packets", rebuffered_packets);
        inc_new_counter_info!("treasury_phase-consumed_buffered_packets", new_tx_count);
        inc_new_counter_debug!("treasury_phase-process_transactions", new_tx_count);

        Ok(unprocessed_packets)
    }

    fn consume_or_forward_packets(
        leader_pubkey: Option<Pubkey>,
        bank_is_available: bool,
        would_be_leader: bool,
        my_pubkey: &Pubkey,
    ) -> BufferedPacketsDecision {
        leader_pubkey.map_or(
            // If leader is not known, return the buffered packets as is
            BufferedPacketsDecision::Hold,
            // else process the packets
            |x| {
                if bank_is_available {
                    // If the treasury is available, this node is the leader
                    BufferedPacketsDecision::Consume
                } else if would_be_leader {
                    // If the node will be the leader soon, hold the packets for now
                    BufferedPacketsDecision::Hold
                } else if x != *my_pubkey {
                    // If the current node is not the leader, forward the buffered packets
                    BufferedPacketsDecision::Forward
                } else {
                    // We don't know the leader. Hold the packets for now
                    BufferedPacketsDecision::Hold
                }
            },
        )
    }

    fn process_buffered_packets(
        socket: &std::net::UdpSocket,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        buffered_packets: &mut Vec<PacketsAndOffsets>,
        enable_forwarding: bool,
    ) -> Result<()> {
        let r_node_group_info = node_group_info.read().unwrap();

        let (decision, next_leader) = {
            let waterclock = waterclock_recorder.lock().unwrap();
            let next_leader = waterclock.next_slot_leader();
            (
                Self::consume_or_forward_packets(
                    next_leader,
                    waterclock.treasury().is_some(),
                    waterclock.would_be_leader(DEFAULT_TICKS_PER_SLOT * 2),
                    &r_node_group_info.id(),
                ),
                next_leader,
            )
        };

        match decision {
            BufferedPacketsDecision::Consume => {
                let mut unprocessed = Self::consume_buffered_packets(
                    &r_node_group_info.id(),
                    waterclock_recorder,
                    buffered_packets,
                )?;
                buffered_packets.append(&mut unprocessed);
                Ok(())
            }
            BufferedPacketsDecision::Forward => {
                if enable_forwarding {
                    next_leader.map_or(Ok(()), |leader_pubkey| {
                        r_node_group_info
                            .lookup(&leader_pubkey)
                            .map_or(Ok(()), |leader| {
                                let _ = Self::forward_buffered_packets(
                                    &socket,
                                    &leader.tpu_via_blobs,
                                    &buffered_packets,
                                );
                                buffered_packets.clear();
                                Ok(())
                            })
                    })
                } else {
                    buffered_packets.clear();
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }

    pub fn process_loop(
        verified_receiver: &Arc<Mutex<Receiver<VerifiedPackets>>>,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        recv_start: &mut Instant,
        enable_forwarding: bool,
        id: u32,
    ) {
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let mut buffered_packets = vec![];
        loop {
            if !buffered_packets.is_empty() {
                Self::process_buffered_packets(
                    &socket,
                    waterclock_recorder,
                    node_group_info,
                    &mut buffered_packets,
                    enable_forwarding,
                )
                .unwrap_or_else(|_| buffered_packets.clear());
            }

            let recv_timeout = if !buffered_packets.is_empty() {
                // If packets are buffered, let's wait for less time on recv from the channel.
                // This helps detect the next leader faster, and processing the buffered
                // packets quickly
                Duration::from_millis(10)
            } else {
                // Default wait time
                Duration::from_millis(100)
            };

            match Self::process_packets(
                &verified_receiver,
                &waterclock_recorder,
                recv_start,
                recv_timeout,
                node_group_info,
                id,
            ) {
                Err(Error::RecvTimeoutError(RecvTimeoutError::Timeout)) => (),
                Ok(mut unprocessed_packets) => {
                    if unprocessed_packets.is_empty() {
                        continue;
                    }
                    let num = unprocessed_packets
                        .iter()
                        .map(|(_, unprocessed)| unprocessed.len())
                        .sum();
                    inc_new_counter_info!("treasury_phase-buffered_packets", num);
                    buffered_packets.append(&mut unprocessed_packets);
                }
                Err(err) => {
                    debug!("morgan-banking-stage-tx: exit due to {:?}", err);
                    break;
                }
            }
        }
    }

    pub fn num_threads() -> u32 {
        sys_info::cpu_num().unwrap_or(NUM_THREADS)
    }

    /// Convert the transactions from a blob of binary data to a vector of transactions
    fn deserialize_transactions(p: &Packets) -> Vec<Option<Transaction>> {
        p.packets
            .iter()
            .map(|x| deserialize(&x.data[0..x.meta.size]).ok())
            .collect()
    }

    fn record_transactions<'a, 'b>(
        treasury: &'a Bank,
        txs: &'b [Transaction],
        results: &[transaction::Result<()>],
        waterclock: &Arc<Mutex<WaterClockRecorder>>,
        recordable_txs: &'b mut Vec<&'b Transaction>,
    ) -> Result<LockedAccountsResults<'a, 'b, &'b Transaction>> {
        let processed_transactions: Vec<_> = results
            .iter()
            .zip(txs.iter())
            .filter_map(|(r, x)| {
                if Bank::can_commit(r) {
                    recordable_txs.push(x);
                    Some(x.clone())
                } else {
                    None
                }
            })
            .collect();
        let record_locks = treasury.lock_record_accounts(recordable_txs);
        debug!("processed: {} ", processed_transactions.len());
        // unlock all the accounts with errors which are filtered by the above `filter_map`
        if !processed_transactions.is_empty() {
            inc_new_counter_warn!(
                "treasury_phase-record_transactions",
                processed_transactions.len()
            );
            let hash = hash_transactions(&processed_transactions);
            // record and unlock will unlock all the successful transactions
            waterclock.lock()
                .unwrap()
                .record(treasury.slot(), hash, processed_transactions)?;
        }
        Ok(record_locks)
    }

    fn process_and_record_transactions_locked(
        treasury: &Bank,
        txs: &[Transaction],
        waterclock: &Arc<Mutex<WaterClockRecorder>>,
        lock_results: &LockedAccountsResults<Transaction>,
    ) -> Result<()> {
        let now = Instant::now();
        // Use a shorter maximum age when adding transactions into the pipeline.  This will reduce
        // the likelihood of any single thread getting starved and processing old ids.
        // TODO: Banking stage threads should be prioritized to complete faster then this queue
        // expires.
        let (loaded_accounts, results) =
            treasury.load_and_execute_transactions(txs, lock_results, MAX_RECENT_BLOCKHASHES / 2);
        let load_execute_time = now.elapsed();

        let freeze_lock = treasury.freeze_lock();

        let mut recordable_txs = vec![];
        let (record_time, record_locks) = {
            let now = Instant::now();
            let record_locks =
                Self::record_transactions(treasury, txs, &results, waterclock, &mut recordable_txs)?;
            (now.elapsed(), record_locks)
        };

        let commit_time = {
            let now = Instant::now();
            treasury.commit_transactions(txs, &loaded_accounts, &results);
            now.elapsed()
        };

        drop(record_locks);
        drop(freeze_lock);

        debug!(
            "treasury: {} load_execute: {}us record: {}us commit: {}us txs_len: {}",
            treasury.slot(),
            duration_as_us(&load_execute_time),
            duration_as_us(&record_time),
            duration_as_us(&commit_time),
            txs.len(),
        );

        Ok(())
    }

    pub fn process_and_record_transactions(
        treasury: &Bank,
        txs: &[Transaction],
        waterclock: &Arc<Mutex<WaterClockRecorder>>,
        chunk_offset: usize,
    ) -> (Result<()>, Vec<usize>) {
        let now = Instant::now();
        // Once accounts are locked, other threads cannot encode transactions that will modify the
        // same account state
        let lock_results = treasury.lock_accounts(txs);
        let lock_time = now.elapsed();

        let unprocessed_txs: Vec<_> = lock_results
            .locked_accounts_results()
            .iter()
            .zip(chunk_offset..)
            .filter_map(|(res, index)| match res {
                Err(TransactionError::AccountInUse) => Some(index),
                Ok(_) => None,
                Err(_) => None,
            })
            .collect();

        let results = Self::process_and_record_transactions_locked(treasury, txs, waterclock, &lock_results);

        let now = Instant::now();
        // Once the accounts are new transactions can enter the pipeline to process them
        drop(lock_results);
        let unlock_time = now.elapsed();

        debug!(
            "treasury: {} lock: {}us unlock: {}us txs_len: {}",
            treasury.slot(),
            duration_as_us(&lock_time),
            duration_as_us(&unlock_time),
            txs.len(),
        );

        (results, unprocessed_txs)
    }

    /// Sends transactions to the treasury.
    ///
    /// Returns the number of transactions successfully processed by the treasury, which may be less
    /// than the total number if max Water Clock height was reached and the treasury halted
    fn process_transactions(
        treasury: &Bank,
        transactions: &[Transaction],
        waterclock: &Arc<Mutex<WaterClockRecorder>>,
    ) -> Result<(usize, Vec<usize>)> {
        let mut chunk_start = 0;
        let mut unprocessed_txs = vec![];
        while chunk_start != transactions.len() {
            let chunk_end = chunk_start
                + entry_info::num_will_fit(
                    &transactions[chunk_start..],
                    packet::BLOB_DATA_SIZE as u64,
                    &Entry::serialized_to_blob_size,
                );

            let (result, unprocessed_txs_in_chunk) = Self::process_and_record_transactions(
                treasury,
                &transactions[chunk_start..chunk_end],
                waterclock,
                chunk_start,
            );
            trace!("process_transactions: {:?}", result);
            unprocessed_txs.extend_from_slice(&unprocessed_txs_in_chunk);
            if let Err(Error::WaterClockRecorderErr(WaterClockRecorderErr::MaxHeightReached)) = result {
                // info!(
                //     "{}",
                //     Info(format!("process transactions: max height reached slot: {} height: {}",
                //     treasury.slot(),
                //     treasury.tick_height()).to_string())
                // );
                let loginfo: String = format!("process transactions: max height reached slot: {} height: {}",
                    treasury.slot(),
                    treasury.tick_height()).to_string();
                println!("{}",
                    printLn(
                        loginfo,
                        module_path!().to_string()
                    )
                );
                let range: Vec<usize> = (chunk_start..chunk_end).collect();
                unprocessed_txs.extend_from_slice(&range);
                unprocessed_txs.sort_unstable();
                unprocessed_txs.dedup();
                break;
            }
            result?;
            chunk_start = chunk_end;
        }
        Ok((chunk_start, unprocessed_txs))
    }

    // This function returns a vector of transactions that are not None. It also returns a vector
    // with position of the transaction in the input list
    fn filter_transaction_indexes(
        transactions: Vec<Option<Transaction>>,
        indexes: &[usize],
    ) -> (Vec<Transaction>, Vec<usize>) {
        transactions
            .into_iter()
            .zip(indexes)
            .filter_map(|(tx, index)| match tx {
                None => None,
                Some(tx) => Some((tx, index)),
            })
            .unzip()
    }

    // This function creates a filter of transaction results with Ok() for every pending
    // transaction. The non-pending transactions are marked with TransactionError
    fn prepare_filter_for_pending_transactions(
        transactions: &[Transaction],
        pending_tx_indexes: &[usize],
    ) -> Vec<transaction::Result<()>> {
        let mut mask = vec![Err(TransactionError::BlockhashNotFound); transactions.len()];
        pending_tx_indexes.iter().for_each(|x| mask[*x] = Ok(()));
        mask
    }

    // This function returns a vector containing index of all valid transactions. A valid
    // transaction has result Ok() as the value
    fn filter_valid_transaction_indexes(
        valid_txs: &[transaction::Result<()>],
        transaction_indexes: &[usize],
    ) -> Vec<usize> {
        let valid_transactions = valid_txs
            .iter()
            .enumerate()
            .filter_map(|(index, x)| if x.is_ok() { Some(index) } else { None })
            .collect_vec();

        valid_transactions
            .iter()
            .map(|x| transaction_indexes[*x])
            .collect()
    }

    // This function deserializes packets into transactions and returns non-None transactions
    fn transactions_from_packets(
        msgs: &Packets,
        transaction_indexes: &[usize],
    ) -> (Vec<Transaction>, Vec<usize>) {
        let packets = Packets::new(
            transaction_indexes
                .iter()
                .map(|x| msgs.packets[*x].to_owned())
                .collect_vec(),
        );

        let transactions = Self::deserialize_transactions(&packets);

        Self::filter_transaction_indexes(transactions, &transaction_indexes)
    }

    // This function  filters pending transactions that are still valid
    fn filter_pending_transactions(
        treasury: &Arc<Bank>,
        transactions: &[Transaction],
        transaction_indexes: &[usize],
        pending_indexes: &[usize],
    ) -> Vec<usize> {
        let filter = Self::prepare_filter_for_pending_transactions(transactions, pending_indexes);

        let mut error_counters = ErrorCounters::default();
        let result = treasury.check_transactions(
            transactions,
            &filter,
            (MAX_RECENT_BLOCKHASHES - MAX_TRANSACTION_FORWARDING_DELAY) / 2,
            &mut error_counters,
        );

        Self::filter_valid_transaction_indexes(&result, transaction_indexes)
    }

    fn process_received_packets(
        treasury: &Arc<Bank>,
        waterclock: &Arc<Mutex<WaterClockRecorder>>,
        msgs: &Packets,
        transaction_indexes: Vec<usize>,
    ) -> Result<(usize, usize, Vec<usize>)> {
        let (transactions, transaction_indexes) =
            Self::transactions_from_packets(msgs, &transaction_indexes);
        debug!(
            "treasury: {} filtered transactions {}",
            treasury.slot(),
            transactions.len()
        );

        let tx_len = transactions.len();

        let (processed, unprocessed_tx_indexes) =
            Self::process_transactions(treasury, &transactions, waterclock)?;

        let unprocessed_tx_count = unprocessed_tx_indexes.len();

        let filtered_unprocessed_tx_indexes = Self::filter_pending_transactions(
            treasury,
            &transactions,
            &transaction_indexes,
            &unprocessed_tx_indexes,
        );
        inc_new_counter_info!(
            "treasury_phase-dropped_tx_before_forwarding",
            unprocessed_tx_count.saturating_sub(filtered_unprocessed_tx_indexes.len())
        );

        Ok((processed, tx_len, filtered_unprocessed_tx_indexes))
    }

    fn filter_unprocessed_packets(
        treasury: &Arc<Bank>,
        msgs: &Packets,
        transaction_indexes: &[usize],
        my_pubkey: &Pubkey,
        next_leader: Option<Pubkey>,
    ) -> Vec<usize> {
        // Check if we are the next leader. If so, let's not filter the packets
        // as we'll filter it again while processing the packets.
        // Filtering helps if we were going to forward the packets to some other node
        if let Some(leader) = next_leader {
            if leader == *my_pubkey {
                return transaction_indexes.to_vec();
            }
        }

        let (transactions, transaction_indexes) =
            Self::transactions_from_packets(msgs, &transaction_indexes);

        let tx_count = transaction_indexes.len();

        let unprocessed_tx_indexes = (0..transactions.len()).collect_vec();
        let filtered_unprocessed_tx_indexes = Self::filter_pending_transactions(
            treasury,
            &transactions,
            &transaction_indexes,
            &unprocessed_tx_indexes,
        );

        inc_new_counter_info!(
            "treasury_phase-dropped_tx_before_forwarding",
            tx_count.saturating_sub(filtered_unprocessed_tx_indexes.len())
        );

        filtered_unprocessed_tx_indexes
    }

    fn generate_packet_indexes(vers: Vec<u8>) -> Vec<usize> {
        vers.iter()
            .enumerate()
            .filter_map(|(index, ver)| if *ver != 0 { Some(index) } else { None })
            .collect()
    }

    /// Process the incoming packets
    pub fn process_packets(
        verified_receiver: &Arc<Mutex<Receiver<VerifiedPackets>>>,
        waterclock: &Arc<Mutex<WaterClockRecorder>>,
        recv_start: &mut Instant,
        recv_timeout: Duration,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        id: u32,
    ) -> Result<UnprocessedPackets> {
        let mms = verified_receiver
            .lock()
            .unwrap()
            .recv_timeout(recv_timeout)?;

        let mms_len = mms.len();
        let count: usize = mms.iter().map(|x| x.1.len()).sum();
        debug!(
            "@{:?} process start stalled for: {:?}ms txs: {} id: {}",
            timing::timestamp(),
            timing::duration_as_ms(&recv_start.elapsed()),
            count,
            id,
        );
        inc_new_counter_debug!("treasury_phase-transactions_received", count);
        let proc_start = Instant::now();
        let mut new_tx_count = 0;

        let mut mms_iter = mms.into_iter();
        let mut unprocessed_packets = vec![];
        while let Some((msgs, vers)) = mms_iter.next() {
            let packet_indexes = Self::generate_packet_indexes(vers);
            let treasury = waterclock.lock().unwrap().treasury();
            if treasury.is_none() {
                Self::push_unprocessed(&mut unprocessed_packets, msgs, packet_indexes);
                continue;
            }
            let treasury = treasury.unwrap();

            let (processed, verified_txs_len, unprocessed_indexes) =
                Self::process_received_packets(&treasury, &waterclock, &msgs, packet_indexes)?;

            new_tx_count += processed;

            // Collect any unprocessed transactions in this batch for forwarding
            Self::push_unprocessed(&mut unprocessed_packets, msgs, unprocessed_indexes);

            if processed < verified_txs_len {
                let next_leader = waterclock.lock().unwrap().next_slot_leader();
                let my_pubkey = node_group_info.read().unwrap().id();
                // Walk thru rest of the transactions and filter out the invalid (e.g. too old) ones
                while let Some((msgs, vers)) = mms_iter.next() {
                    let packet_indexes = Self::generate_packet_indexes(vers);
                    let unprocessed_indexes = Self::filter_unprocessed_packets(
                        &treasury,
                        &msgs,
                        &packet_indexes,
                        &my_pubkey,
                        next_leader,
                    );
                    Self::push_unprocessed(&mut unprocessed_packets, msgs, unprocessed_indexes);
                }
            }
        }

        inc_new_counter_debug!(
            "treasury_phase-time_ms",
            timing::duration_as_ms(&proc_start.elapsed()) as usize
        );
        let total_time_s = timing::duration_as_s(&proc_start.elapsed());
        let total_time_ms = timing::duration_as_ms(&proc_start.elapsed());
        debug!(
            "@{:?} done processing transaction batches: {} time: {:?}ms tx count: {} tx/s: {} total count: {} id: {}",
            timing::timestamp(),
            mms_len,
            total_time_ms,
            new_tx_count,
            (new_tx_count as f32) / (total_time_s),
            count,
            id,
        );
        inc_new_counter_debug!("treasury_phase-process_packets", count);
        inc_new_counter_debug!("treasury_phase-process_transactions", new_tx_count);

        *recv_start = Instant::now();

        Ok(unprocessed_packets)
    }

    fn push_unprocessed(
        unprocessed_packets: &mut UnprocessedPackets,
        packets: Packets,
        packet_indexes: Vec<usize>,
    ) {
        if !packet_indexes.is_empty() {
            unprocessed_packets.push((packets, packet_indexes));
        }
    }
}

impl Service for BankingStage {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        for bank_thread_hdl in self.bank_thread_hdls {
            bank_thread_hdl.join()?;
        }
        Ok(())
    }
}

pub fn create_test_recorder(
    treasury: &Arc<Bank>,
    block_buffer_pool: &Arc<BlockBufferPool>,
) -> (
    Arc<AtomicBool>,
    Arc<Mutex<WaterClockRecorder>>,
    WaterClockService,
    Receiver<WorkingBankEntries>,
) {
    let exit = Arc::new(AtomicBool::new(false));
    let waterclock_config = Arc::new(WaterClockConfig::default());
    let (mut waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
        treasury.tick_height(),
        treasury.last_blockhash(),
        treasury.slot(),
        Some(4),
        treasury.ticks_per_slot(),
        &Pubkey::default(),
        block_buffer_pool,
        &Arc::new(LeaderScheduleCache::new_from_bank(&treasury)),
        &waterclock_config,
    );
    waterclock_recorder.set_treasury(&treasury);

    let waterclock_recorder = Arc::new(Mutex::new(waterclock_recorder));
    let waterclock_service = WaterClockService::new(waterclock_recorder.clone(), &waterclock_config, &exit);

    (exit, waterclock_recorder, waterclock_service, entry_receiver)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_buffer_pool::get_tmp_ledger_path;
    use crate::node_group_info::Node;
    use crate::entry_info::EntrySlice;
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use crate::packet::to_packets;
    use crate::water_clock_recorder::WorkingBank;
    use crate::{get_tmp_ledger_path, tmp_ledger_name};
    use itertools::Itertools;
    use morgan_interface::instruction::InstructionError;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::system_transaction;
    use morgan_interface::transaction::TransactionError;
    use std::sync::mpsc::channel;
    use std::thread::sleep;

    #[test]
    fn test_banking_stage_shutdown1() {
        let genesis_block = create_genesis_block(2).genesis_block;
        let treasury = Arc::new(Bank::new(&genesis_block));
        let (verified_sender, verified_receiver) = channel();
        let (vote_sender, vote_receiver) = channel();
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = Arc::new(
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger"),
            );
            let (exit, waterclock_recorder, waterclock_service, _entry_receiever) =
                create_test_recorder(&treasury, &block_buffer_pool);
            let node_group_info = NodeGroupInfo::new_with_invalid_keypair(Node::new_localhost().info);
            let node_group_info = Arc::new(RwLock::new(node_group_info));
            let treasury_phase = BankingStage::new(
                &node_group_info,
                &waterclock_recorder,
                verified_receiver,
                vote_receiver,
            );
            drop(verified_sender);
            drop(vote_sender);
            exit.store(true, Ordering::Relaxed);
            treasury_phase.join().unwrap();
            waterclock_service.join().unwrap();
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_banking_stage_tick() {
        morgan_logger::setup();
        let GenesisBlockInfo {
            mut genesis_block, ..
        } = create_genesis_block(2);
        genesis_block.ticks_per_slot = 4;
        let treasury = Arc::new(Bank::new(&genesis_block));
        let start_hash = treasury.last_blockhash();
        let (verified_sender, verified_receiver) = channel();
        let (vote_sender, vote_receiver) = channel();
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = Arc::new(
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger"),
            );
            let (exit, waterclock_recorder, waterclock_service, entry_receiver) =
                create_test_recorder(&treasury, &block_buffer_pool);
            let node_group_info = NodeGroupInfo::new_with_invalid_keypair(Node::new_localhost().info);
            let node_group_info = Arc::new(RwLock::new(node_group_info));
            let treasury_phase = BankingStage::new(
                &node_group_info,
                &waterclock_recorder,
                verified_receiver,
                vote_receiver,
            );
            trace!("sending treasury");
            sleep(Duration::from_millis(600));
            drop(verified_sender);
            drop(vote_sender);
            exit.store(true, Ordering::Relaxed);
            waterclock_service.join().unwrap();
            drop(waterclock_recorder);

            trace!("getting entries");
            let entries: Vec<_> = entry_receiver
                .iter()
                .flat_map(|x| x.1.into_iter().map(|e| e.0))
                .collect();
            trace!("done");
            assert_eq!(entries.len(), genesis_block.ticks_per_slot as usize - 1);
            assert!(entries.verify(&start_hash));
            assert_eq!(entries[entries.len() - 1].hash, treasury.last_blockhash());
            treasury_phase.join().unwrap();
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_banking_stage_entries_only() {
        morgan_logger::setup();
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(10);
        let treasury = Arc::new(Bank::new(&genesis_block));
        let start_hash = treasury.last_blockhash();
        let (verified_sender, verified_receiver) = channel();
        let (vote_sender, vote_receiver) = channel();
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool = Arc::new(
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger"),
            );
            let (exit, waterclock_recorder, waterclock_service, entry_receiver) =
                create_test_recorder(&treasury, &block_buffer_pool);
            let node_group_info = NodeGroupInfo::new_with_invalid_keypair(Node::new_localhost().info);
            let node_group_info = Arc::new(RwLock::new(node_group_info));
            let treasury_phase = BankingStage::new(
                &node_group_info,
                &waterclock_recorder,
                verified_receiver,
                vote_receiver,
            );

            // fund another account so we can send 2 good transactions in a single batch.
            let keypair = Keypair::new();
            let fund_tx = system_transaction::create_user_account(
                &mint_keypair,
                &keypair.pubkey(),
                2,
                start_hash,
            );
            treasury.process_transaction(&fund_tx).unwrap();

            // good tx
            let to = Pubkey::new_rand();
            let tx = system_transaction::create_user_account(&mint_keypair, &to, 1, start_hash);

            // good tx, but no verify
            let to2 = Pubkey::new_rand();
            let tx_no_ver = system_transaction::create_user_account(&keypair, &to2, 2, start_hash);

            // bad tx, AccountNotFound
            let keypair = Keypair::new();
            let to3 = Pubkey::new_rand();
            let tx_anf = system_transaction::create_user_account(&keypair, &to3, 1, start_hash);

            // send 'em over
            let packets = to_packets(&[tx_no_ver, tx_anf, tx]);

            // glad they all fit
            assert_eq!(packets.len(), 1);

            let packets = packets
                .into_iter()
                .map(|packets| (packets, vec![0u8, 1u8, 1u8]))
                .collect();

            verified_sender // no_ver, anf, tx
                .send(packets)
                .unwrap();

            drop(verified_sender);
            drop(vote_sender);
            exit.store(true, Ordering::Relaxed);
            waterclock_service.join().unwrap();
            drop(waterclock_recorder);

            let mut blockhash = start_hash;
            let treasury = Bank::new(&genesis_block);
            treasury.process_transaction(&fund_tx).unwrap();
            //receive entries + ticks
            for _ in 0..10 {
                let ventries: Vec<Vec<Entry>> = entry_receiver
                    .iter()
                    .map(|x| x.1.into_iter().map(|e| e.0).collect())
                    .collect();

                for entries in &ventries {
                    for entry in entries {
                        treasury.process_transactions(&entry.transactions)
                            .iter()
                            .for_each(|x| assert_eq!(*x, Ok(())));
                    }
                    assert!(entries.verify(&blockhash));
                    blockhash = entries.last().unwrap().hash;
                }

                if treasury.get_balance(&to) == 1 {
                    break;
                }

                sleep(Duration::from_millis(200));
            }

            assert_eq!(treasury.get_balance(&to), 1);
            assert_eq!(treasury.get_balance(&to2), 0);

            drop(entry_receiver);
            treasury_phase.join().unwrap();
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_banking_stage_entryfication() {
        morgan_logger::setup();
        // In this attack we'll demonstrate that a verifier can interpret the ledger
        // differently if either the server doesn't signal the ledger to add an
        // Entry OR if the verifier tries to parallelize across multiple Entries.
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(2);
        let (verified_sender, verified_receiver) = channel();

        // Process a batch that includes a transaction that receives two difs.
        let alice = Keypair::new();
        let tx = system_transaction::create_user_account(
            &mint_keypair,
            &alice.pubkey(),
            2,
            genesis_block.hash(),
        );

        let packets = to_packets(&[tx]);
        let packets = packets
            .into_iter()
            .map(|packets| (packets, vec![1u8]))
            .collect();
        verified_sender.send(packets).unwrap();

        // Process a second batch that spends one of those difs.
        let tx = system_transaction::create_user_account(
            &alice,
            &mint_keypair.pubkey(),
            1,
            genesis_block.hash(),
        );
        let packets = to_packets(&[tx]);
        let packets = packets
            .into_iter()
            .map(|packets| (packets, vec![1u8]))
            .collect();
        verified_sender.send(packets).unwrap();

        let (vote_sender, vote_receiver) = channel();
        let ledger_path = get_tmp_ledger_path!();
        {
            let entry_receiver = {
                // start a treasury_phase to eat verified receiver
                let treasury = Arc::new(Bank::new(&genesis_block));
                let block_buffer_pool = Arc::new(
                    BlockBufferPool::open_ledger_file(&ledger_path)
                        .expect("Expected to be able to open database ledger"),
                );
                let (exit, waterclock_recorder, waterclock_service, entry_receiver) =
                    create_test_recorder(&treasury, &block_buffer_pool);
                let node_group_info =
                    NodeGroupInfo::new_with_invalid_keypair(Node::new_localhost().info);
                let node_group_info = Arc::new(RwLock::new(node_group_info));
                let _treasury_phase = BankingStage::new_num_threads(
                    &node_group_info,
                    &waterclock_recorder,
                    verified_receiver,
                    vote_receiver,
                    2,
                );

                // wait for treasury_phase to eat the packets
                while treasury.get_balance(&alice.pubkey()) != 1 {
                    sleep(Duration::from_millis(100));
                }
                exit.store(true, Ordering::Relaxed);
                waterclock_service.join().unwrap();
                entry_receiver
            };
            drop(verified_sender);
            drop(vote_sender);

            // consume the entire entry_receiver, feed it into a new treasury
            // check that the balance is what we expect.
            let entries: Vec<_> = entry_receiver
                .iter()
                .flat_map(|x| x.1.into_iter().map(|e| e.0))
                .collect();

            let treasury = Bank::new(&genesis_block);
            for entry in &entries {
                treasury.process_transactions(&entry.transactions)
                    .iter()
                    .for_each(|x| assert_eq!(*x, Ok(())));
            }

            // Assert the user holds one dif, not two. If the stage only outputs one
            // entry, then the second transaction will be rejected, because it drives
            // the account balance below zero before the credit is added.
            assert_eq!(treasury.get_balance(&alice.pubkey()), 1);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_bank_record_transactions() {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(10_000);
        let treasury = Arc::new(Bank::new(&genesis_block));
        let working_treasury = WorkingBank {
            treasury: treasury.clone(),
            min_tick_height: treasury.tick_height(),
            max_tick_height: std::u64::MAX,
        };
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let (waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                treasury.tick_height(),
                treasury.last_blockhash(),
                treasury.slot(),
                None,
                treasury.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_bank(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );
            let waterclock_recorder = Arc::new(Mutex::new(waterclock_recorder));

            waterclock_recorder.lock().unwrap().set_working_bank(working_treasury);
            let pubkey = Pubkey::new_rand();
            let keypair2 = Keypair::new();
            let pubkey2 = Pubkey::new_rand();

            let transactions = vec![
                system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
                system_transaction::transfer(&keypair2, &pubkey2, 1, genesis_block.hash()),
            ];

            let mut results = vec![Ok(()), Ok(())];
            BankingStage::record_transactions(
                &treasury,
                &transactions,
                &results,
                &waterclock_recorder,
                &mut vec![],
            )
            .unwrap();
            let (_, entries) = entry_receiver.recv().unwrap();
            assert_eq!(entries[0].0.transactions.len(), transactions.len());

            // InstructionErrors should still be recorded
            results[0] = Err(TransactionError::InstructionError(
                1,
                InstructionError::new_result_with_negative_difs(),
            ));
            BankingStage::record_transactions(
                &treasury,
                &transactions,
                &results,
                &waterclock_recorder,
                &mut vec![],
            )
            .unwrap();
            let (_, entries) = entry_receiver.recv().unwrap();
            assert_eq!(entries[0].0.transactions.len(), transactions.len());

            // Other TransactionErrors should not be recorded
            results[0] = Err(TransactionError::AccountNotFound);
            BankingStage::record_transactions(
                &treasury,
                &transactions,
                &results,
                &waterclock_recorder,
                &mut vec![],
            )
            .unwrap();
            let (_, entries) = entry_receiver.recv().unwrap();
            assert_eq!(entries[0].0.transactions.len(), transactions.len() - 1);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_bank_filter_transaction_indexes() {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(10_000);
        let pubkey = Pubkey::new_rand();

        let transactions = vec![
            None,
            Some(system_transaction::transfer(
                &mint_keypair,
                &pubkey,
                1,
                genesis_block.hash(),
            )),
            Some(system_transaction::transfer(
                &mint_keypair,
                &pubkey,
                1,
                genesis_block.hash(),
            )),
            Some(system_transaction::transfer(
                &mint_keypair,
                &pubkey,
                1,
                genesis_block.hash(),
            )),
            None,
            None,
            Some(system_transaction::transfer(
                &mint_keypair,
                &pubkey,
                1,
                genesis_block.hash(),
            )),
            None,
            Some(system_transaction::transfer(
                &mint_keypair,
                &pubkey,
                1,
                genesis_block.hash(),
            )),
            None,
            Some(system_transaction::transfer(
                &mint_keypair,
                &pubkey,
                1,
                genesis_block.hash(),
            )),
            None,
            None,
        ];

        let filtered_transactions = vec![
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
        ];

        assert_eq!(
            BankingStage::filter_transaction_indexes(
                transactions.clone(),
                &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            ),
            (filtered_transactions.clone(), vec![1, 2, 3, 6, 8, 10])
        );

        assert_eq!(
            BankingStage::filter_transaction_indexes(
                transactions,
                &vec![1, 2, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15],
            ),
            (filtered_transactions, vec![2, 4, 5, 9, 11, 13])
        );
    }

    #[test]
    fn test_bank_prepare_filter_for_pending_transaction() {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(10_000);
        let pubkey = Pubkey::new_rand();

        let transactions = vec![
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
        ];

        assert_eq!(
            BankingStage::prepare_filter_for_pending_transactions(&transactions, &vec![2, 4, 5],),
            vec![
                Err(TransactionError::BlockhashNotFound),
                Err(TransactionError::BlockhashNotFound),
                Ok(()),
                Err(TransactionError::BlockhashNotFound),
                Ok(()),
                Ok(())
            ]
        );

        assert_eq!(
            BankingStage::prepare_filter_for_pending_transactions(&transactions, &vec![0, 2, 3],),
            vec![
                Ok(()),
                Err(TransactionError::BlockhashNotFound),
                Ok(()),
                Ok(()),
                Err(TransactionError::BlockhashNotFound),
                Err(TransactionError::BlockhashNotFound),
            ]
        );
    }

    #[test]
    fn test_bank_filter_valid_transaction_indexes() {
        assert_eq!(
            BankingStage::filter_valid_transaction_indexes(
                &vec![
                    Err(TransactionError::BlockhashNotFound),
                    Err(TransactionError::BlockhashNotFound),
                    Ok(()),
                    Err(TransactionError::BlockhashNotFound),
                    Ok(()),
                    Ok(())
                ],
                &vec![2, 4, 5, 9, 11, 13]
            ),
            vec![5, 11, 13]
        );

        assert_eq!(
            BankingStage::filter_valid_transaction_indexes(
                &vec![
                    Ok(()),
                    Err(TransactionError::BlockhashNotFound),
                    Err(TransactionError::BlockhashNotFound),
                    Ok(()),
                    Ok(()),
                    Ok(())
                ],
                &vec![1, 6, 7, 9, 31, 43]
            ),
            vec![1, 9, 31, 43]
        );
    }

    #[test]
    fn test_should_process_or_forward_packets() {
        let my_pubkey = Pubkey::new_rand();
        let my_pubkey1 = Pubkey::new_rand();

        assert_eq!(
            BankingStage::consume_or_forward_packets(None, true, false, &my_pubkey),
            BufferedPacketsDecision::Hold
        );
        assert_eq!(
            BankingStage::consume_or_forward_packets(None, false, false, &my_pubkey),
            BufferedPacketsDecision::Hold
        );
        assert_eq!(
            BankingStage::consume_or_forward_packets(None, false, false, &my_pubkey1),
            BufferedPacketsDecision::Hold
        );

        assert_eq!(
            BankingStage::consume_or_forward_packets(
                Some(my_pubkey1.clone()),
                false,
                false,
                &my_pubkey
            ),
            BufferedPacketsDecision::Forward
        );
        assert_eq!(
            BankingStage::consume_or_forward_packets(
                Some(my_pubkey1.clone()),
                false,
                true,
                &my_pubkey
            ),
            BufferedPacketsDecision::Hold
        );
        assert_eq!(
            BankingStage::consume_or_forward_packets(
                Some(my_pubkey1.clone()),
                true,
                false,
                &my_pubkey
            ),
            BufferedPacketsDecision::Consume
        );
        assert_eq!(
            BankingStage::consume_or_forward_packets(
                Some(my_pubkey1.clone()),
                false,
                false,
                &my_pubkey1
            ),
            BufferedPacketsDecision::Hold
        );
        assert_eq!(
            BankingStage::consume_or_forward_packets(
                Some(my_pubkey1.clone()),
                true,
                false,
                &my_pubkey1
            ),
            BufferedPacketsDecision::Consume
        );
    }

    #[test]
    fn test_bank_process_and_record_transactions() {
        morgan_logger::setup();
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(10_000);
        let treasury = Arc::new(Bank::new(&genesis_block));
        let pubkey = Pubkey::new_rand();

        let transactions = vec![system_transaction::transfer(
            &mint_keypair,
            &pubkey,
            1,
            genesis_block.hash(),
        )];

        let working_treasury = WorkingBank {
            treasury: treasury.clone(),
            min_tick_height: treasury.tick_height(),
            max_tick_height: treasury.tick_height() + 1,
        };
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let (waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                treasury.tick_height(),
                treasury.last_blockhash(),
                treasury.slot(),
                Some(4),
                treasury.ticks_per_slot(),
                &pubkey,
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_bank(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );
            let waterclock_recorder = Arc::new(Mutex::new(waterclock_recorder));

            waterclock_recorder.lock().unwrap().set_working_bank(working_treasury);

            BankingStage::process_and_record_transactions(&treasury, &transactions, &waterclock_recorder, 0)
                .0
                .unwrap();
            waterclock_recorder.lock().unwrap().tick();

            let mut done = false;
            // read entries until I find mine, might be ticks...
            while let Ok((_, entries)) = entry_receiver.recv() {
                for (entry, _) in entries {
                    if !entry.is_tick() {
                        trace!("got entry");
                        assert_eq!(entry.transactions.len(), transactions.len());
                        assert_eq!(treasury.get_balance(&pubkey), 1);
                        done = true;
                    }
                }
                if done {
                    break;
                }
            }
            trace!("done ticking");

            assert_eq!(done, true);

            let transactions = vec![system_transaction::transfer(
                &mint_keypair,
                &pubkey,
                2,
                genesis_block.hash(),
            )];

            assert_matches!(
                BankingStage::process_and_record_transactions(
                    &treasury,
                    &transactions,
                    &waterclock_recorder,
                    0
                )
                .0,
                Err(Error::WaterClockRecorderErr(WaterClockRecorderErr::MaxHeightReached))
            );

            assert_eq!(treasury.get_balance(&pubkey), 1);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_bank_process_and_record_transactions_account_in_use() {
        morgan_logger::setup();
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(10_000);
        let treasury = Arc::new(Bank::new(&genesis_block));
        let pubkey = Pubkey::new_rand();
        let pubkey1 = Pubkey::new_rand();

        let transactions = vec![
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_block.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey1, 1, genesis_block.hash()),
        ];

        let working_treasury = WorkingBank {
            treasury: treasury.clone(),
            min_tick_height: treasury.tick_height(),
            max_tick_height: treasury.tick_height() + 1,
        };
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let (waterclock_recorder, _entry_receiver) = WaterClockRecorder::new(
                treasury.tick_height(),
                treasury.last_blockhash(),
                treasury.slot(),
                Some(4),
                treasury.ticks_per_slot(),
                &pubkey,
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_bank(&treasury)),
                &Arc::new(WaterClockConfig::default()),
            );
            let waterclock_recorder = Arc::new(Mutex::new(waterclock_recorder));

            waterclock_recorder.lock().unwrap().set_working_bank(working_treasury);

            let (result, unprocessed) = BankingStage::process_and_record_transactions(
                &treasury,
                &transactions,
                &waterclock_recorder,
                0,
            );

            assert!(result.is_ok());
            assert_eq!(unprocessed.len(), 1);
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }

    #[test]
    fn test_filter_valid_packets() {
        morgan_logger::setup();

        let all_packets = (0..16)
            .map(|packets_id| {
                let packets = Packets::new(
                    (0..32)
                        .map(|packet_id| {
                            let mut p = Packet::default();
                            p.meta.port = packets_id << 8 | packet_id;
                            p
                        })
                        .collect_vec(),
                );
                let valid_indexes = (0..32)
                    .filter_map(|x| if x % 2 != 0 { Some(x as usize) } else { None })
                    .collect_vec();
                (packets, valid_indexes)
            })
            .collect_vec();

        let result = BankingStage::filter_valid_packets_for_forwarding(&all_packets);

        assert_eq!(result.len(), 256);

        let _ = result
            .into_iter()
            .enumerate()
            .map(|(index, p)| {
                let packets_id = index / 16;
                let packet_id = (index % 16) * 2 + 1;
                assert_eq!(p.meta.port, (packets_id << 8 | packet_id) as u16);
            })
            .collect_vec();
    }
}
