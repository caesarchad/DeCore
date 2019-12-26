
use crate::packet::{
    deserialize_packets_in_blob, Blob, Meta, Packets, SharedBlobs, 
};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};

pub type PcktAcptr = Receiver<Packets>;
pub type PcktSndr = Sender<Packets>;
pub type BlobSndr = Sender<SharedBlobs>;
pub type BlobAcptr = Receiver<SharedBlobs>;