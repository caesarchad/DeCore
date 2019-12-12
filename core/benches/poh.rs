// This bench attempts to justify the value of `morgan::waterclock_service::NUM_HASHES_PER_BATCH`

#![feature(test)]
extern crate test;

use morgan::water_clock::WaterClock;
use morgan::water_clock_service::NUM_HASHES_PER_BATCH;
use morgan_interface::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use test::Bencher;

const NUM_HASHES: u64 = 30_000; // Should require ~10ms on a 2017 MacBook Pro

#[bench]
// No locking.  Fastest.
fn bench_waterclock_hash(bencher: &mut Bencher) {
    let mut waterclock = WaterClock::new(Hash::default(), None);
    bencher.iter(|| {
        waterclock.hash(NUM_HASHES);
    })
}

#[bench]
// Lock on each iteration.  Slowest.
fn bench_arc_mutex_waterclock_hash(bencher: &mut Bencher) {
    let waterclock = Arc::new(Mutex::new(WaterClock::new(Hash::default(), None)));
    bencher.iter(|| {
        for _ in 0..NUM_HASHES {
            waterclock.lock().unwrap().hash(1);
        }
    })
}

#[bench]
// Acquire lock every NUM_HASHES_PER_BATCH iterations.
// Speed should be close to bench_waterclock_hash() if NUM_HASHES_PER_BATCH is set well.
fn bench_arc_mutex_waterclock_batched_hash(bencher: &mut Bencher) {
    let waterclock = Arc::new(Mutex::new(WaterClock::new(Hash::default(), Some(NUM_HASHES))));
    //let exit = Arc::new(AtomicBool::new(false));
    let exit = Arc::new(AtomicBool::new(true));

    bencher.iter(|| {
        // NOTE: This block attempts to look as close as possible to `WaterClockService::tick_producer()`
        loop {
            if waterclock.lock().unwrap().hash(NUM_HASHES_PER_BATCH) {
                waterclock.lock().unwrap().tick().unwrap();
                if exit.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    })
}

#[bench]
// Worst case transaction record delay due to batch hashing at NUM_HASHES_PER_BATCH
fn bench_waterclock_lock_time_per_batch(bencher: &mut Bencher) {
    let mut waterclock = WaterClock::new(Hash::default(), None);
    bencher.iter(|| {
        waterclock.hash(NUM_HASHES_PER_BATCH);
    })
}
