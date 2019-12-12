use log::*;
use morgan_interface::client::Client;
use morgan_interface::timing::duration_as_s;
use morgan_helper::logHelper::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime};
use ansi_term::Color::{Green};
use chrono::prelude::*;

#[derive(Default)]
pub struct SampleStats {
    
    pub tps: f32,
    
    pub elapsed: Duration,
    
    pub txs: u64,
}

pub fn sample_txs<T>(
    exit_signal: &Arc<AtomicBool>,
    sample_stats: &Arc<RwLock<Vec<(String, SampleStats)>>>,
    sample_period: u64,
    client: &Arc<T>,
) where
    T: Client,
{
    let mut max_tps = 0.0;
    let mut total_elapsed;
    let mut total_txs;
    let mut now = Instant::now();
    let start_time = now;
    let initial_txs = client.get_transaction_count().expect("transaction count");
    let mut last_txs = initial_txs;

    loop {
        total_elapsed = start_time.elapsed();
        let elapsed = now.elapsed();
        now = Instant::now();
        let mut txs = client.get_transaction_count().expect("transaction count");

        if txs < last_txs {
            let info:String = format!("Expected txs({}) >= last_txs({})", txs, last_txs).to_string();
            println!("{}",
                printLn(
                    info,
                    module_path!().to_string()
                )
            );

            txs = last_txs;
        }
        total_txs = txs - initial_txs;
        let sample_txs = txs - last_txs;
        last_txs = txs;

        let tps = sample_txs as f32 / duration_as_s(&elapsed);
        if tps > max_tps {
            max_tps = tps;
        }
        
        let info:String = format!(
            "Sampler {:9.2} TPS, Transactions: {:6}, Total transactions: {} over {} s", 
            tps,
            sample_txs,
            total_txs,
            total_elapsed.as_secs()
        ).to_string();
        println!("{}",
            printLn(
                info,
                module_path!().to_string()
            )
        );

        if exit_signal.load(Ordering::Relaxed) {
            let stats = SampleStats {
                tps: max_tps,
                elapsed: total_elapsed,
                txs: total_txs,
            };
            sample_stats
                .write()
                .unwrap()
                .push((client.transactions_addr(), stats));
            return;
        }
        sleep(Duration::from_secs(sample_period));
    }
}
