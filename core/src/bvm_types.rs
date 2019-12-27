
use crate::packet::{
    deserialize_packets_in_blob, Blob, Meta, Packets, SharedBlobs, 
};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use morgan_netutil::PortRange;
pub use morgan_interface::constants::PACKET_DATA_SIZE;
use std::time::Duration;
use std::mem::size_of;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::hash::Hash;
pub type PcktAcptr = Receiver<Packets>;
pub type PcktSndr = Sender<Packets>;
pub type BlobSndr = Sender<SharedBlobs>;
pub type BlobAcptr = Receiver<SharedBlobs>;
macro_rules! align {
    ($x:expr, $align:expr) => {
        $x + ($align - 1) & !($align - 1)
    };
}
macro_rules! range {
    ($prev:expr, $type:ident) => {
        $prev..$prev + size_of::<$type>()
    };
}

pub const PARENT_RANGE: std::ops::Range<usize> = range!(0, u64);
pub const SLOT_RANGE: std::ops::Range<usize> = range!(PARENT_RANGE.end, u64);
pub const INDEX_RANGE: std::ops::Range<usize> = range!(SLOT_RANGE.end, u64);
pub const ID_RANGE: std::ops::Range<usize> = range!(INDEX_RANGE.end, Pubkey);
pub const FORWARDED_RANGE: std::ops::Range<usize> = range!(ID_RANGE.end, bool);
pub const GENESIS_RANGE: std::ops::Range<usize> = range!(FORWARDED_RANGE.end, Hash);
pub const FLAGS_RANGE: std::ops::Range<usize> = range!(GENESIS_RANGE.end, u32);
pub const SIZE_RANGE: std::ops::Range<usize> = range!(FLAGS_RANGE.end, u64);
pub const CHACHA_BLOCK_SIZE: usize = 64;
pub const CHACHA_KEY_SIZE: usize = 32;
pub const NUM_DATA: usize = 8;
pub const NUM_CODING: usize = 8;
pub const ERASURE_SET_SIZE: usize = NUM_DATA + NUM_CODING;
pub const MAX_REPAIR_LENGTH: usize = 16;
pub const REPAIR_MS: u64 = 100;
pub const MAX_REPAIR_TRIES: u64 = 128;
pub const NUM_FORKS_TO_REPAIR: usize = 5;
pub const MAX_ORPHANS: usize = 5;
pub const VOTE_THRESHOLD_DEPTH: usize = 8;
pub const VOTE_THRESHOLD_SIZE: f64 = 2f64 / 3f64;
pub const MAX_RECENT_VOTES: usize = 16;
pub const NDTB_GOSSIP_BLOOM_SIZE: usize = 1000;
pub const FIXER_SLEEP_TIME: usize = 150;
pub const FIX_REDUNDANCY: usize = 1;
pub const BUFFER_SLOTS: usize = 50;
pub const DELAY_SLOTS: usize = 2;
pub const UPDATE_SLOTS: usize = 2;
pub const FULLNODE_PORT_RANGE: PortRange = (10_000, 12_000);
/// The Data plane fanout size, also used as the neighborhood size
pub const DATA_PLANE_FANOUT: usize = 200;
/// milliseconds we sleep for between gossip requests
pub const GOSSIP_SLEEP_MILLIS: u64 = 100;
/// the number of slots to respond with when responding to `Orphan` requests
pub const MAX_ORPHAN_REPAIR_RESPONSES: usize = 10;
pub const INTERFACE_CONNECT_ATTEMPTS_MAX: usize = 30;
pub const INTERFACE_CONNECT_INTERVAL: Duration = Duration::from_secs(1);
pub const NUM_PACKETS: usize = 1024 * 8;
pub const BLOB_SIZE: usize = (64 * 1024 - 128); // wikipedia says there should be 20b for ipv4 headers
pub const BLOB_DATA_SIZE: usize = BLOB_SIZE - (BLOB_HEADER_SIZE * 2);
pub const BLOB_DATA_ALIGN: usize = 16; // safe for erasure input pointers, gf.c needs 16byte-aligned buffers
pub const NUM_BLOBS: usize = (NUM_PACKETS * PACKET_DATA_SIZE) / BLOB_SIZE;
pub const BLOB_HEADER_SIZE: usize = align!(SIZE_RANGE.end, BLOB_DATA_ALIGN); // make sure data() is safe for erasure

pub const BLOB_FLAG_IS_LAST_IN_SLOT: u32 = 0x2;

pub const BLOB_FLAG_IS_CODING: u32 = 0x1;
pub const NDTB_GOSSIP_NUM_ACTIVE: usize = 30;
pub const NDTB_GOSSIP_PUSH_FANOUT: usize = 6;
pub const NDTB_GOSSIP_PUSH_MSG_TIMEOUT_MS: u64 = 5000;
pub const NDTB_GOSSIP_PRUNE_MSG_TIMEOUT_MS: u64 = 500;
pub const NDTB_GOSSIP_PULL_CRDS_TIMEOUT_MS: u64 = 15000;
pub const NUM_RCVMMSGS: usize = 16;
pub const MAX_ENTRY_RECV_PER_ITER: usize = 512;
pub const STORAGE_ROTATE_TEST_COUNT: u64 = 2;
pub const NUM_STORAGE_SAMPLES: usize = 4;
pub const NUM_THREADS: u32 = 10;
pub const NUM_HASHES_PER_BATCH: u64 = 1;