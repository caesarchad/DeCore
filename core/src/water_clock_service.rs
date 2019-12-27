//! The water_clock_service implements a system-wide clock to measure the passage of time
use crate::water_clock_recorder::WaterClockRecorder;
use crate::service::Service;
use crate::bvm_types::*;
use core_affinity;
use morgan_interface::waterclock_config::WaterClockConfig;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep, Builder, JoinHandle};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::io::Result;

pub struct WaterClockService {
    drop_producer: JoinHandle<()>,
}




impl WaterClockService {
    pub fn new(
        waterclock_recorder: Arc<Mutex<WaterClockRecorder>>,
        waterclock_config: &Arc<WaterClockConfig>,
        waterclock_exit: &Arc<AtomicBool>,
    ) -> Self {
        let waterclock_exit_ = waterclock_exit.clone();
        let waterclock_config = waterclock_config.clone();
        let drop_producer = Builder::new()
            .name("morgan-waterclock-service-drop_producer".to_string())
            .spawn(move || {
                if waterclock_config.hashes_per_drop.is_none() {
                    Self::sleepy_drop_producer(waterclock_recorder, &waterclock_config, &waterclock_exit_);
                } else {
                    if let Some(cores) = core_affinity::get_core_ids() {
                        core_affinity::set_for_current(cores[0]);
                    }
                    Self::drop_producer(waterclock_recorder, &waterclock_exit_);
                }
                waterclock_exit_.store(true, Ordering::Relaxed);
            })
            .unwrap();

        Self { drop_producer }
    }

    fn sleepy_drop_producer(
        waterclock_recorder: Arc<Mutex<WaterClockRecorder>>,
        waterclock_config: &WaterClockConfig,
        waterclock_exit: &AtomicBool,
    ) {
        while !waterclock_exit.load(Ordering::Relaxed) {
            sleep(waterclock_config.target_drop_duration);
            waterclock_recorder.lock().unwrap()._drop();
        }
    }

    fn drop_producer(waterclock_recorder: Arc<Mutex<WaterClockRecorder>>, waterclock_exit: &AtomicBool) {
        let waterclock = waterclock_recorder.lock().unwrap().waterclock.clone();
        loop {
            if waterclock.lock().unwrap().hash(NUM_HASHES_PER_BATCH) {
                waterclock_recorder.lock().unwrap()._drop();
                if waterclock_exit.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }

}

impl Service for WaterClockService {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.drop_producer.join()
    }
}

pub fn write_u16frame<'stream, 'buf, 'c, TSocket>(
    stream: &'stream mut TSocket,
    buf: &'buf [u8],
) -> Result<()>
where
    'stream: 'c,
    'buf: 'c,
    TSocket: AsyncWrite + Unpin,
{
    

    Ok(())
}

pub fn write_u16frame_len<TSocket>(stream: &mut TSocket, len: u16) -> Result<()>
where
    TSocket: AsyncWrite + Unpin,
{
    let len = u16::to_be_bytes(len);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_buffer_pool::{fetch_interim_ledger_location, BlockBufferPool};
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use crate::leader_arrange_cache::LdrSchBufferPoolList;
    use crate::water_clock_recorder::WorkingTreasury;
    use crate::result::Result;
    use crate::test_tx::test_tx;
    use morgan_runtime::treasury::Treasury;
    use morgan_interface::hash::hash;
    use morgan_interface::pubkey::Pubkey;
    use std::time::Duration;

    #[test]
    fn test_waterclock_service() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
        let treasury = Arc::new(Treasury::new(&genesis_block));
        let prev_hash = treasury.last_transaction_seal();
        let ledger_path = fetch_interim_ledger_location!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let waterclock_config = Arc::new(WaterClockConfig {
                hashes_per_drop: Some(2),
                target_drop_duration: Duration::from_millis(42),
            });
            let (waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                treasury.drop_height(),
                prev_hash,
                treasury.slot(),
                Some(4),
                treasury.drops_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LdrSchBufferPoolList::new_from_treasury(&treasury)),
                &waterclock_config,
            );
            let waterclock_recorder = Arc::new(Mutex::new(waterclock_recorder));
            let exit = Arc::new(AtomicBool::new(false));
            let working_treasury = WorkingTreasury {
                treasury: treasury.clone(),
                min_drop_height: treasury.drop_height(),
                max_drop_height: std::u64::MAX,
            };

            let entry_producer: JoinHandle<Result<()>> = {
                let waterclock_recorder = waterclock_recorder.clone();
                let exit = exit.clone();

                Builder::new()
                    .name("morgan-waterclock-service-entry_producer".to_string())
                    .spawn(move || {
                        loop {
                            // send some data
                            let h1 = hash(b"hello world!");
                            let tx = test_tx();
                            let _ = waterclock_recorder
                                .lock()
                                .unwrap()
                                .record(treasury.slot(), h1, vec![tx]);

                            if exit.load(Ordering::Relaxed) {
                                break Ok(());
                            }
                        }
                    })
                    .unwrap()
            };

            let waterclock_service = WaterClockService::new(waterclock_recorder.clone(), &waterclock_config, &exit);
            waterclock_recorder.lock().unwrap().set_working_treasury(working_treasury);

            // get some events
            let mut hashes = 0;
            let mut need_drop = true;
            let mut need_entry = true;
            let mut need_partial = true;

            while need_drop || need_entry || need_partial {
                for entry in entry_receiver.recv().unwrap().1 {
                    let entry = &entry.0;
                    if entry.is_drop() {
                        assert!(
                            entry.num_hashes <= waterclock_config.hashes_per_drop.unwrap(),
                            format!(
                                "{} <= {}",
                                entry.num_hashes,
                                waterclock_config.hashes_per_drop.unwrap()
                            )
                        );

                        if entry.num_hashes == waterclock_config.hashes_per_drop.unwrap() {
                            need_drop = false;
                        } else {
                            need_partial = false;
                        }

                        hashes += entry.num_hashes;

                        assert_eq!(hashes, waterclock_config.hashes_per_drop.unwrap());

                        hashes = 0;
                    } else {
                        assert!(entry.num_hashes >= 1);
                        need_entry = false;
                        hashes += entry.num_hashes;
                    }
                }
            }
            exit.store(true, Ordering::Relaxed);
            let _ = waterclock_service.join().unwrap();
            let _ = entry_producer.join().unwrap();
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }
}
