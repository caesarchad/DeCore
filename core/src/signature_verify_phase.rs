//! The `sigverify_phase` implements the signature verification phase of the transaction digesting module. It
//! receives a list of lists of packets and outputs the same list, but tags each
//! top-level list with a list of booleans, telling the next phase whether the
//! signature in that packet is valid. It assumes each packet contains one
//! transaction. All processing is done on the CPU by default and on a GPU
//! if the `cuda` feature is enabled with `--features=cuda`.

use crate::packet::Packets;
use crate::result::{Error, Result};
use crate::service::Service;
use crate::signature_verify;
use crate::data_filter;
use crate::bvm_types::PcktAcptr;
use morgan_metricbot::{datapoint_info, inc_new_counter_info};
use morgan_interface::timing;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, Builder, JoinHandle};
use std::time::Instant;
use morgan_helper::logHelper::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_sync::semaphore::{AcquireError, Permit as TokioPermit, Semaphore as TokioSemaphore};

#[cfg(feature = "cuda")]
const RECV_BATCH_MAX: usize = 60_000;

#[cfg(not(feature = "cuda"))]
const RECV_BATCH_MAX: usize = 1000;

pub type VerifiedPackets = Vec<(Packets, Vec<u8>)>;

pub struct SigVerifyPhase {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl SigVerifyPhase {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        packet_receiver: Receiver<Packets>,
        sigverify_disabled: bool,
        verified_sender: Sender<VerifiedPackets>,
    ) -> Self {
        signature_verify::init();
        let thread_hdls =
            Self::verifier_services(packet_receiver, verified_sender, sigverify_disabled);
        Self { thread_hdls }
    }

    fn verify_batch(batch: Vec<Packets>, sigverify_disabled: bool) -> VerifiedPackets {
        let r = if sigverify_disabled {
            signature_verify::ed25519_verify_disabled(&batch)
        } else {
            signature_verify::ed25519_verify(&batch)
        };
        batch.into_iter().zip(r).collect()
    }

    fn verifier(
        recvr: &Arc<Mutex<PcktAcptr>>,
        sendr: &Sender<VerifiedPackets>,
        sigverify_disabled: bool,
        id: usize,
    ) -> Result<()> {
        let (batch, len, recv_time) = data_filter::batch_forward(
            &recvr.lock().expect("'recvr' lock in fn verifier"),
            RECV_BATCH_MAX,
        )?;
        inc_new_counter_info!("sigverify_phase-packets_received", len);

        let now = Instant::now();
        let batch_len = batch.len();
        debug!(
            "@{:?} verifier: verifying: {} id: {}",
            timing::timestamp(),
            batch.len(),
            id
        );

        let verified_batch = Self::verify_batch(batch, sigverify_disabled);
        inc_new_counter_info!("sigverify_phase-verified_packets_send", len);

        if sendr.send(verified_batch).is_err() {
            return Err(Error::SendError);
        }

        let total_time_ms = timing::duration_as_ms(&now.elapsed());
        let total_time_s = timing::duration_as_s(&now.elapsed());
        inc_new_counter_info!(
            "sigverify_phase-time_ms",
            (total_time_ms + recv_time) as usize
        );
        debug!(
            "@{:?} verifier: done. batches: {} total verify time: {:?} id: {} verified: {} v/s {}",
            timing::timestamp(),
            batch_len,
            total_time_ms,
            id,
            len,
            (len as f32 / total_time_s)
        );

        datapoint_info!(
            "sigverify_phase-total_verify_time",
            ("batch_len", batch_len, i64),
            ("len", len, i64),
            ("total_time_ms", total_time_ms, i64)
        );

        Ok(())
    }

    fn verifier_service(
        packet_receiver: Arc<Mutex<PcktAcptr>>,
        verified_sender: Sender<VerifiedPackets>,
        sigverify_disabled: bool,
        id: usize,
    ) -> JoinHandle<()> {
        Builder::new()
            .name(format!("morgan-verifier-{}", id))
            .spawn(move || loop {
                if let Err(e) =
                    Self::verifier(&packet_receiver, &verified_sender, sigverify_disabled, id)
                {
                    match e {
                        Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => break,
                        Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                        Error::SendError => {
                            break;
                        }
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
            .unwrap()
    }

    fn verifier_services(
        packet_receiver: PcktAcptr,
        verified_sender: Sender<VerifiedPackets>,
        sigverify_disabled: bool,
    ) -> Vec<JoinHandle<()>> {
        let receiver = Arc::new(Mutex::new(packet_receiver));
        (0..4)
            .map(|id| {
                Self::verifier_service(
                    receiver.clone(),
                    verified_sender.clone(),
                    sigverify_disabled,
                    id,
                )
            })
            .collect()
    }
}

impl Service for SigVerifyPhase {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}

/// The wrapped tokio_sync [`Semaphore`](TokioSemaphore) and total permit capacity.
#[derive(Debug)]
struct Inner {
    semaphore: TokioSemaphore,
    capacity: usize,
}

/// A futures-aware semaphore.
#[derive(Clone, Debug)]
pub struct Semaphore {
    inner: Arc<Inner>,
}

/// A permit acquired from a semaphore, allowing access to a shared resource.
/// Dropping a `Permit` will release it back to the semaphore.
#[derive(Debug)]
pub struct Permit {
    inner: Arc<Inner>,
    permit: TokioPermit,
}

impl Semaphore {
    /// Create a new semaphore with `capacity` number of available permits.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                semaphore: TokioSemaphore::new(capacity),
                capacity,
            }),
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn available_permits(&self) -> usize {
        self.inner.semaphore.available_permits()
    }

    pub fn is_idle(&self) -> bool {
        self.available_permits() == self.capacity()
    }

    pub fn is_full(&self) -> bool {
        self.available_permits() == 0
    }

    /// Try to acquire an available permit from the semaphore. If no permits are
    /// available, return `None`.
    pub fn try_acquire(&self) -> Option<Permit> {
        let mut permit = TokioPermit::new();
        match permit.try_acquire(&self.inner.semaphore) {
            Ok(()) => Some(Permit {
                inner: Arc::clone(&self.inner),
                permit,
            }),
            Err(err) => {
                // The TokioSemaphore is not dropped yet and our wrapper never
                // calls .close(), so the TokioSemaphore can never be closed
                // unless all references, including this &self, have been
                // dropped.
                assert!(!err.is_closed());
                assert!(err.is_no_permits());
                None
            }
        }
    }
}

impl Permit {
    pub fn release(&mut self) {
        self.permit.release(&self.inner.semaphore);
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct PermitFuture {
    pub permit: Option<Permit>,
}

impl PermitFuture {
    pub fn new(permit: Permit) -> Self {
        Self {
            permit: Some(permit),
        }
    }
}