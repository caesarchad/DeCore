//! The `fetch_phase` batches input from a UDP socket and sends it to a channel.

use crate::water_clock_recorder::WaterClockRecorder;
use crate::result::{Error, Result};
use crate::service::Service;
use crate::streamer::{self, PacketReceiver, PacketSender};
use morgan_metricbot::{inc_new_counter_debug, inc_new_counter_info};
use morgan_interface::timing::DEFAULT_DROPS_PER_SLOT;
use std::net::UdpSocket;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{channel, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::{self, Builder, JoinHandle};
use morgan_helper::logHelper::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct DebugInterfaceConfig {
    pub admission_control_node_debug_port: u16,
    pub storage_node_debug_port: u16,
    // This has similar use to the core-node-debug-server itself
    pub metrics_server_port: u16,
    pub public_metrics_server_port: u16,
    pub address: String,
}

impl Default for DebugInterfaceConfig {
    fn default() -> DebugInterfaceConfig {
        DebugInterfaceConfig {
            admission_control_node_debug_port: 6191,
            storage_node_debug_port: 6194,
            metrics_server_port: 9101,
            public_metrics_server_port: 9102,
            address: "localhost".to_string(),
        }
    }
}

pub struct FetchPhase {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl FetchPhase {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        sockets: Vec<UdpSocket>,
        transaction_digesting_module_via_blobs_sockets: Vec<UdpSocket>,
        exit: &Arc<AtomicBool>,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
    ) -> (Self, PacketReceiver) {
        let (sender, receiver) = channel();
        (
            Self::new_with_sender(sockets, transaction_digesting_module_via_blobs_sockets, exit, &sender, &waterclock_recorder),
            receiver,
        )
    }
    pub fn new_with_sender(
        sockets: Vec<UdpSocket>,
        transaction_digesting_module_via_blobs_sockets: Vec<UdpSocket>,
        exit: &Arc<AtomicBool>,
        sender: &PacketSender,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
    ) -> Self {
        let tx_sockets = sockets.into_iter().map(Arc::new).collect();
        let transaction_digesting_module_via_blobs_sockets = transaction_digesting_module_via_blobs_sockets.into_iter().map(Arc::new).collect();
        Self::new_multi_socket(
            tx_sockets,
            transaction_digesting_module_via_blobs_sockets,
            exit,
            &sender,
            &waterclock_recorder,
        )
    }

    fn handle_forwarded_packets(
        recvr: &PacketReceiver,
        sendr: &PacketSender,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
    ) -> Result<()> {
        let msgs = recvr.recv()?;
        let mut len = msgs.packets.len();
        let mut batch = vec![msgs];
        while let Ok(more) = recvr.try_recv() {
            len += more.packets.len();
            batch.push(more);
        }

        if waterclock_recorder
            .lock()
            .unwrap()
            .would_be_leader(DEFAULT_DROPS_PER_SLOT * 2)
        {
            inc_new_counter_debug!("fetch_phase-honor_forwards", len);
            for packets in batch {
                if sendr.send(packets).is_err() {
                    return Err(Error::SendError);
                }
            }
        } else {
            inc_new_counter_info!("fetch_phase-discard_forwards", len);
        }

        Ok(())
    }

    fn new_multi_socket(
        sockets: Vec<Arc<UdpSocket>>,
        transaction_digesting_module_via_blobs_sockets: Vec<Arc<UdpSocket>>,
        exit: &Arc<AtomicBool>,
        sender: &PacketSender,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
    ) -> Self {
        let transaction_digesting_module_threads = sockets
            .into_iter()
            .map(|socket| streamer::receiver(socket, &exit, sender.clone()));

        let (forward_sender, forward_receiver) = channel();
        let transaction_digesting_module_via_blobs_threads = transaction_digesting_module_via_blobs_sockets
            .into_iter()
            .map(|socket| streamer::blob_packet_receiver(socket, &exit, forward_sender.clone()));

        let sender = sender.clone();
        let waterclock_recorder = waterclock_recorder.clone();

        let fwd_thread_hdl = Builder::new()
            .name("morgan-fetch-phase-fwd-rcvr".to_string())
            .spawn(move || loop {
                if let Err(e) =
                    Self::handle_forwarded_packets(&forward_receiver, &sender, &waterclock_recorder)
                {
                    match e {
                        Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => break,
                        Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                        Error::RecvError(_) => break,
                        Error::SendError => break,
                        _ => {
                            // error!("{}", Error(format!("{:?}", e).to_string())),
                            println!(
                                "{}",
                                Error(
                                    format!("{:?}", e).to_string(),
                                    module_path!().to_string()
                                )
                            );
                        }
                    }
                }
            })
            .unwrap();

        let mut thread_hdls: Vec<_> = transaction_digesting_module_threads.chain(transaction_digesting_module_via_blobs_threads).collect();
        thread_hdls.push(fwd_thread_hdl);
        Self { thread_hdls }
    }
}

impl Service for FetchPhase {
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
pub struct AdmissionControlConfig {
    pub address: String,
    pub admission_control_service_port: u16,
    pub need_to_check_mempool_before_validation: bool,
    pub max_concurrent_inbound_syncs: usize,
    pub upstream_proxy_timeout: Duration,
}

impl Default for AdmissionControlConfig {
    fn default() -> AdmissionControlConfig {
        AdmissionControlConfig {
            address: "0.0.0.0".to_string(),
            admission_control_service_port: 8001,
            need_to_check_mempool_before_validation: false,
            max_concurrent_inbound_syncs: 100,
            upstream_proxy_timeout: Duration::from_secs(1),
        }
    }
}
