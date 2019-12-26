//! `spot_service` handles the data plane incoming blobs, storing them in
//!   block_buffer_pool and retransmitting where required
//!
use crate::block_buffer_pool::BlockBufferPool;
use crate::node_group_info::NodeGroupInfo;
use crate::leader_arrange_cache::LdrSchBufferPoolList;
use crate::packet::{Blob, SharedBlob, BLOB_HEADER_SIZE};
use crate::fix_missing_spot_service::{FixService, FixPlan};
use crate::result::{Error, Result};
use crate::service::Service;
use crate::bvm_types::{BlobAcptr, BlobSndr};
use morgan_metricbot::{inc_new_counter_debug, inc_new_counter_error};
use morgan_runtime::treasury::Treasury;
use morgan_interface::hash::Hash;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::timing::duration_as_ms;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, RwLock};
use std::thread::{self, Builder, JoinHandle};
use std::time::{Duration, Instant};
use morgan_helper::logHelper::*;

fn replay_blobs(blobs: &[SharedBlob], retransmit: &BlobSndr, id: &Pubkey) -> Result<()> {
    let mut retransmit_queue: Vec<SharedBlob> = Vec::new();
    for blob in blobs {
        if blob.read().unwrap().id() != *id {
            let mut w_blob = blob.write().unwrap();
            w_blob.meta.forward = w_blob.should_forward();
            w_blob.set_forwarded(false);
            retransmit_queue.push(blob.clone());
        }
    }

    if !retransmit_queue.is_empty() {
        inc_new_counter_debug!(
            "data_filter-accept_epoch-retransmit",
            retransmit_queue.len(),
            0,
            1000
        );
        retransmit.send(retransmit_queue)?;
    }
    Ok(())
}

fn handle_data_blob(blobs: &[SharedBlob], block_buffer_pool: &Arc<BlockBufferPool>) -> Result<()> {
    let blobs: Vec<_> = blobs.iter().map(move |blob| blob.read().unwrap()).collect();

    block_buffer_pool.insert_data_blobs(blobs.iter().filter_map(|blob| {
        if !blob.is_coding() {
            Some(&(**blob))
        } else {
            None
        }
    }))?;

    for blob in blobs {
        
        if blob.is_coding() {
            block_buffer_pool.insert_coding_blob_bytes(
                blob.slot(),
                blob.index(),
                &blob.data[..BLOB_HEADER_SIZE + blob.size()],
            )?;
        }
    }
    Ok(())
}

pub fn check_replay_blob(
    blob: &Blob,
    treasury: Option<Arc<Treasury>>,
    leader_schedule_cache: &Arc<LdrSchBufferPoolList>,
    my_pubkey: &Pubkey,
) -> bool {
    let slot_leader_pubkey = match treasury {
        None => leader_schedule_cache.slot_leader_at(blob.slot(), None),
        Some(treasury) => leader_schedule_cache.slot_leader_at(blob.slot(), Some(&treasury)),
    };

    if blob.id() == *my_pubkey {
        inc_new_counter_debug!("data_filter-accept_epoch-circular_transmission", 1);
        false
    } else if slot_leader_pubkey == None {
        inc_new_counter_debug!("data_filter-accept_epoch-unknown_leader", 1);
        false
    } else if slot_leader_pubkey != Some(blob.id()) {
        inc_new_counter_debug!("data_filter-accept_epoch-wrong_leader", 1);
        false
    } else {
        true
    }
}

fn accept_epoch<F>(
    block_buffer_pool: &Arc<BlockBufferPool>,
    my_pubkey: &Pubkey,
    r: &BlobAcptr,
    retransmit: &BlobSndr,
    genesis_transaction_seal: &Hash,
    blob_filter: F,
) -> Result<()>
where
    F: Fn(&Blob) -> bool,
{
    let timer = Duration::from_millis(200);
    let mut blobs = r.recv_timeout(timer)?;

    while let Ok(mut blob) = r.try_recv() {
        blobs.append(&mut blob)
    }
    let now = Instant::now();
    inc_new_counter_debug!("data_filter-accept_epoch-recv", blobs.len(), 0, 1000);

    blobs.retain(|blob| {
        blob_filter(&blob.read().unwrap())
            && blob.read().unwrap().genesis_transaction_seal() == *genesis_transaction_seal
    });

    replay_blobs(&blobs, retransmit, my_pubkey)?;

    trace!("{} num blobs received: {}", my_pubkey, blobs.len());

    handle_data_blob(&blobs, block_buffer_pool)?;

    trace!(
        "Elapsed processing time in accept_epoch(): {}",
        duration_as_ms(&now.elapsed())
    );

    Ok(())
}

struct EndSignal {
    exit_sender: Arc<AtomicBool>,
}

impl EndSignal {
    fn new(exit_sender: Arc<AtomicBool>) -> Self {
        EndSignal { exit_sender }
    }
}

impl Drop for EndSignal {
    fn drop(&mut self) {
        self.exit_sender.clone().store(true, Ordering::Relaxed);
    }
}

pub struct SpotTransmitService {
    t_window: JoinHandle<()>,
    repair_service: FixService,
}

impl SpotTransmitService {
    #[allow(clippy::too_many_arguments)]
    pub fn new<F>(
        block_buffer_pool: Arc<BlockBufferPool>,
        node_group_info: Arc<RwLock<NodeGroupInfo>>,
        r: BlobAcptr,
        retransmit: BlobSndr,
        fix_socket: Arc<UdpSocket>,
        exit: &Arc<AtomicBool>,
        fix_plan: FixPlan,
        genesis_transaction_seal: &Hash,
        blob_filter: F,
    ) -> SpotTransmitService
    where
        F: 'static
            + Fn(&Pubkey, &Blob, Option<Arc<Treasury>>) -> bool
            + std::marker::Send
            + std::marker::Sync,
    {
        let treasury_forks = match fix_plan {
            FixPlan::FixSlotList(_) => None,

            FixPlan::FixAll { ref treasury_forks, .. } => Some(treasury_forks.clone()),
        };

        let repair_service = FixService::new(
            block_buffer_pool.clone(),
            exit.clone(),
            fix_socket,
            node_group_info.clone(),
            fix_plan,
        );
        let exit = exit.clone();
        let hash = *genesis_transaction_seal;
        let blob_filter = Arc::new(blob_filter);
        let treasury_forks = treasury_forks.clone();
        let t_window = Builder::new()
            .name("morgan-window".to_string())
            .spawn(move || {
                let _exit = EndSignal::new(exit.clone());
                let id = node_group_info.read().unwrap().id();
                trace!("{}: RECV_WINDOW started", id);
                loop {
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }

                    if let Err(e) = accept_epoch(&block_buffer_pool, &id, &r, &retransmit, &hash, |blob| {
                        blob_filter(
                            &id,
                            blob,
                            treasury_forks
                                .as_ref()
                                .map(|treasury_forks| treasury_forks.read().unwrap().working_treasury()),
                        )
                    }) {
                        match e {
                            Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => break,
                            Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                            _ => {
                                inc_new_counter_error!("data_filter-window-error", 1, 1);
                                
                                println!(
                                    "{}",
                                    Error(
                                        format!("window error: {:?}", e).to_string(),
                                        module_path!().to_string()
                                    )
                                );
                            }
                        }
                    }
                }
            })
            .unwrap();

        SpotTransmitService {
            t_window,
            repair_service,
        }
    }
}

impl Service for SpotTransmitService {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.t_window.join()?;
        self.repair_service.join()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::treasury_forks::TreasuryForks;
    use crate::block_buffer_pool::{fetch_interim_ledger_location, BlockBufferPool};
    use crate::node_group_info::{NodeGroupInfo, Node};
    use crate::fiscal_statement_info::{make_consecutive_blobs, compose_s_fiscal_stmt_nohash, FsclStmtSlc};
    use crate::genesis_utils::create_genesis_block_with_leader;
    use crate::packet::{index_blobs, Blob};
    use crate::service::Service;
    use crate::data_filter::{acptr_srvc, handle_forward_srvc};
    use morgan_runtime::epoch_schedule::MINIMUM_SLOT_LENGTH;
    use morgan_interface::hash::Hash;
    use std::fs::remove_dir_all;
    use std::net::UdpSocket;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    #[test]
    fn test_process_blob() {
        let block_buffer_pool_path = fetch_interim_ledger_location!();
        let block_buffer_pool = Arc::new(BlockBufferPool::open_ledger_file(&block_buffer_pool_path).unwrap());
        let fscl_stmt_cnt = 10;
        let original_entries = compose_s_fiscal_stmt_nohash(fscl_stmt_cnt);
        let shared_blobs = original_entries.clone().to_shared_blobs();

        index_blobs(&shared_blobs, &Pubkey::new_rand(), 0, 0, 0);

        for blob in shared_blobs.into_iter().rev() {
            handle_data_blob(&[blob], &block_buffer_pool).expect("Expect successful processing of blob");
        }

        assert_eq!(
            block_buffer_pool.fetch_candidate_fscl_stmts(0, 0, None).unwrap(),
            original_entries
        );

        drop(block_buffer_pool);
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_should_retransmit_and_persist() {
        let me_id = Pubkey::new_rand();
        let leader_pubkey = Pubkey::new_rand();
        let treasury = Arc::new(Treasury::new(
            &create_genesis_block_with_leader(100, &leader_pubkey, 10).genesis_block,
        ));
        let cache = Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury));

        let mut blob = Blob::default();
        blob.set_id(&leader_pubkey);

        // without a Treasury and blobs not from me, blob gets thrown out
        assert_eq!(
            check_replay_blob(&blob, None, &cache, &me_id),
            false
        );

        // with a Treasury for slot 0, blob continues
        assert_eq!(
            check_replay_blob(&blob, Some(treasury.clone()), &cache, &me_id),
            true
        );

        // set the blob to have come from the wrong leader
        blob.set_id(&Pubkey::new_rand());
        assert_eq!(
            check_replay_blob(&blob, Some(treasury.clone()), &cache, &me_id),
            false
        );

        // with a Treasury and no idea who leader is, blob gets thrown out
        blob.set_slot(MINIMUM_SLOT_LENGTH as u64 * 3);
        assert_eq!(
            check_replay_blob(&blob, Some(treasury), &cache, &me_id),
            false
        );

        // if the blob came back from me, it doesn't continue, whether or not I have a treasury
        blob.set_id(&me_id);
        assert_eq!(
            check_replay_blob(&blob, None, &cache, &me_id),
            false
        );
    }

    #[test]
    pub fn window_send_test() {
        morgan_logger::setup();
        // setup a leader whose id is used to generates blobs and a validator
        // node whose window service will retransmit leader blobs.
        let leader_node = Node::new_localhost();
        let validator_node = Node::new_localhost();
        let exit = Arc::new(AtomicBool::new(false));
        let cluster_info_me = NodeGroupInfo::new_with_invalid_keypair(validator_node.info.clone());
        let me_id = leader_node.info.id;
        let subs = Arc::new(RwLock::new(cluster_info_me));

        let (s_reader, r_reader) = channel();
        let t_receiver = acptr_srvc(Arc::new(leader_node.sockets.gossip), &exit, s_reader);
        let (s_retransmit, r_retransmit) = channel();
        let block_buffer_pool_path = fetch_interim_ledger_location!();
        let (block_buffer_pool, _, completed_slots_receiver) = BlockBufferPool::open_by_message(&block_buffer_pool_path)
            .expect("Expected to be able to open database ledger");
        let block_buffer_pool = Arc::new(block_buffer_pool);

        let treasury = Treasury::new(&create_genesis_block_with_leader(100, &me_id, 10).genesis_block);
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new(0, treasury)));
        let fix_plan = FixPlan::FixAll {
            treasury_forks: treasury_forks.clone(),
            completed_slots_receiver,
            epoch_schedule: treasury_forks
                .read()
                .unwrap()
                .working_treasury()
                .epoch_schedule()
                .clone(),
        };
        let t_window = SpotTransmitService::new(
            block_buffer_pool,
            subs,
            r_reader,
            s_retransmit,
            Arc::new(leader_node.sockets.repair),
            &exit,
            fix_plan,
            &Hash::default(),
            |_, _, _| true,
        );
        let t_responder = {
            let (s_responder, r_responder) = channel();
            let blob_sockets: Vec<Arc<UdpSocket>> =
                leader_node.sockets.blaze_unit.into_iter().map(Arc::new).collect();

            let t_responder = handle_forward_srvc("window_send_test", blob_sockets[0].clone(), r_responder);
            let num_blobs_to_make = 10;
            let gossip_address = &leader_node.info.gossip;
            let msgs = make_consecutive_blobs(
                &me_id,
                num_blobs_to_make,
                0,
                Hash::default(),
                &gossip_address,
            )
            .into_iter()
            .rev()
            .collect();;
            s_responder.send(msgs).expect("send");
            t_responder
        };

        let max_attempts = 10;
        let mut num_attempts = 0;
        let mut q = Vec::new();
        loop {
            assert!(num_attempts != max_attempts);
            while let Ok(mut nq) = r_retransmit.recv_timeout(Duration::from_millis(500)) {
                q.append(&mut nq);
            }
            if q.len() == 10 {
                break;
            }
            num_attempts += 1;
        }

        exit.store(true, Ordering::Relaxed);
        t_receiver.join().expect("join");
        t_responder.join().expect("join");
        t_window.join().expect("join");
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
        let _ignored = remove_dir_all(&block_buffer_pool_path);
    }

    #[test]
    pub fn window_send_leader_test2() {
        morgan_logger::setup();
        // setup a leader whose id is used to generates blobs and a validator
        // node whose window service will retransmit leader blobs.
        let leader_node = Node::new_localhost();
        let validator_node = Node::new_localhost();
        let exit = Arc::new(AtomicBool::new(false));
        let cluster_info_me = NodeGroupInfo::new_with_invalid_keypair(validator_node.info.clone());
        let me_id = leader_node.info.id;
        let subs = Arc::new(RwLock::new(cluster_info_me));

        let (s_reader, r_reader) = channel();
        let t_receiver = acptr_srvc(Arc::new(leader_node.sockets.gossip), &exit, s_reader);
        let (s_retransmit, r_retransmit) = channel();
        let block_buffer_pool_path = fetch_interim_ledger_location!();
        let (block_buffer_pool, _, completed_slots_receiver) = BlockBufferPool::open_by_message(&block_buffer_pool_path)
            .expect("Expected to be able to open database ledger");

        let block_buffer_pool = Arc::new(block_buffer_pool);
        let treasury = Treasury::new(&create_genesis_block_with_leader(100, &me_id, 10).genesis_block);
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new(0, treasury)));
        let epoch_schedule = *treasury_forks.read().unwrap().working_treasury().epoch_schedule();
        let fix_plan = FixPlan::FixAll {
            treasury_forks,
            completed_slots_receiver,
            epoch_schedule,
        };
        let t_window = SpotTransmitService::new(
            block_buffer_pool,
            subs.clone(),
            r_reader,
            s_retransmit,
            Arc::new(leader_node.sockets.repair),
            &exit,
            fix_plan,
            &Hash::default(),
            |_, _, _| true,
        );
        let t_responder = {
            let (s_responder, r_responder) = channel();
            let blob_sockets: Vec<Arc<UdpSocket>> =
                leader_node.sockets.blaze_unit.into_iter().map(Arc::new).collect();
            let t_responder = handle_forward_srvc("window_send_test", blob_sockets[0].clone(), r_responder);
            let mut msgs = Vec::new();
            let blobs =
                make_consecutive_blobs(&me_id, 14u64, 0, Hash::default(), &leader_node.info.gossip);

            for v in 0..10 {
                let i = 9 - v;
                msgs.push(blobs[i].clone());
            }
            s_responder.send(msgs).expect("send");

            let mut msgs1 = Vec::new();
            for v in 1..5 {
                let i = 9 + v;
                msgs1.push(blobs[i].clone());
            }
            s_responder.send(msgs1).expect("send");
            t_responder
        };
        let mut q = Vec::new();
        while let Ok(mut nq) = r_retransmit.recv_timeout(Duration::from_millis(500)) {
            q.append(&mut nq);
        }
        assert!(q.len() > 10);
        exit.store(true, Ordering::Relaxed);
        t_receiver.join().expect("join");
        t_responder.join().expect("join");
        t_window.join().expect("join");
        BlockBufferPool::remove_ledger_file(&block_buffer_pool_path).expect("Expected successful database destruction");
        let _ignored = remove_dir_all(&block_buffer_pool_path);
    }
}
