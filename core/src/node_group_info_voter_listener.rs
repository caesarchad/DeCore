use crate::node_group_info::{NodeGroupInfo, GOSSIP_SLEEP_MILLIS};
use crate::water_clock_recorder::WaterClockRecorder;
use crate::result::Result;
use crate::service::Service;
use crate::signature_verify_phase ::VerifiedPackets;
use crate::{packet, signature_verify};
use morgan_metricbot::inc_new_counter_debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, sleep, Builder, JoinHandle};
use std::time::Duration;
use morgan_helper::logHelper::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub struct ClusterInfoVoteListener {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl ClusterInfoVoteListener {
    pub fn new(
        exit: &Arc<AtomicBool>,
        node_group_info: Arc<RwLock<NodeGroupInfo>>,
        sigverify_disabled: bool,
        sender: Sender<VerifiedPackets>,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
    ) -> Self {
        let exit = exit.clone();
        let waterclock_recorder = waterclock_recorder.clone();
        let thread = Builder::new()
            .name("morgan-cluster_info_vote_listener".to_string())
            .spawn(move || {
                let _ = Self::recv_loop(
                    exit,
                    &node_group_info,
                    sigverify_disabled,
                    &sender,
                    waterclock_recorder,
                );
            })
            .unwrap();
        Self {
            thread_hdls: vec![thread],
        }
    }
    fn recv_loop(
        exit: Arc<AtomicBool>,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        sigverify_disabled: bool,
        sender: &Sender<VerifiedPackets>,
        waterclock_recorder: Arc<Mutex<WaterClockRecorder>>,
    ) -> Result<()> {
        let mut last_ts = 0;
        loop {
            if exit.load(Ordering::Relaxed) {
                return Ok(());
            }
            let (votes, new_ts) = node_group_info.read().unwrap().get_votes(last_ts);
            if waterclock_recorder.lock().unwrap().treasury().is_some() {
                last_ts = new_ts;
                inc_new_counter_debug!("node_group_info_vote_listener-recv_count", votes.len());
                let msgs = packet::to_packets(&votes);
                if !msgs.is_empty() {
                    let r = if sigverify_disabled {
                        signature_verify::ed25519_verify_disabled(&msgs)
                    } else {
                        signature_verify::ed25519_verify_cpu(&msgs)
                    };
                    sender.send(msgs.into_iter().zip(r).collect())?;
                }
            }
            sleep(Duration::from_millis(GOSSIP_SLEEP_MILLIS));
        }
    }
}

impl Service for ClusterInfoVoteListener {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct SafetyRulesConfig {
    pub backend: SafetyRulesBackend,
}

impl Default for SafetyRulesConfig {
    fn default() -> Self {
        Self {
            backend: SafetyRulesBackend::InMemoryStorage,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum SafetyRulesBackend {
    InMemoryStorage,
    OnDiskStorage {
        // In testing scenarios this implies that the default state is okay if
        // a state is not specified.
        default: bool,
        // Required path for on disk storage
        path: PathBuf,
    },
}

#[cfg(test)]
mod tests {
    use crate::fork_selection::MAX_RECENT_VOTES;
    use crate::packet;
    use morgan_interface::hash::Hash;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::transaction::Transaction;
    use morgan_vote_api::vote_opcode;
    use morgan_vote_api::vote_state::Vote;
    use morgan_helper::logHelper::*;

    #[test]
    fn test_max_vote_tx_fits() {
        morgan_logger::setup();
        let node_keypair = Keypair::new();
        let vote_keypair = Keypair::new();
        let votes = (0..MAX_RECENT_VOTES)
            .map(|i| Vote::new(i as u64, Hash::default()))
            .collect::<Vec<_>>();
        let vote_ix = vote_opcode::vote(
            &node_keypair.pubkey(),
            &vote_keypair.pubkey(),
            &vote_keypair.pubkey(),
            votes,
        );

        let mut vote_tx = Transaction::new_u_opcodes(vec![vote_ix]);
        vote_tx.partial_sign(&[&node_keypair], Hash::default());
        vote_tx.partial_sign(&[&vote_keypair], Hash::default());

        use bincode::serialized_size;
        // info!("{}", Info(format!("max vote size {}", serialized_size(&vote_tx).unwrap()).to_string()));
        let loginfo: String = format!("max vote size {}", serialized_size(&vote_tx).unwrap()).to_string();
        println!("{}",
            printLn(
                loginfo,
                module_path!().to_string()
            )
        );
        let msgs = packet::to_packets(&[vote_tx]); // panics if won't fit

        assert_eq!(msgs.len(), 1);
    }
}
