pub const PACKET_DATA_SIZE: usize = 1280 - 40 - 8;
/// Default port number for JSON RPC API
pub const DEFAULT_RPC_PORT: u16 = 10099;

/// Default port number for JSON RPC pubsub
pub const DEFAULT_RPC_PUBSUB_PORT: u16 = 10100;

// The default _drop rate that the cluster attempts to achieve.  Note that the actual _drop
// rate at any given time should be expected to drift
pub const DEFAULT_NUM_DROPS_PER_SECOND: u64 = 10;

// At 10 drops/s, 8 drops per slot implies that leader rotation and voting will happen
// every 800 ms. A fast voting cadence ensures faster finality and convergence
pub const DEFAULT_DROPS_PER_SLOT: u64 = 8;

// 1 Epoch = 800 * 4096 ms ~= 55 minutes
pub const DEFAULT_SLOTS_PER_EPOCH: u64 = 4096;

pub const NUM_CONSECUTIVE_LEADER_SLOTS: u64 = 8;

/// The time window of recent block hash values that the treasury will track the signatures
/// of over. Once the treasury discards a block hash, it will reject any transactions that use
/// that `recent_transaction_seal` in a transaction. Lowering this value reduces memory consumption,
/// but requires clients to update its `recent_transaction_seal` more frequently. Raising the value
/// lengthens the time a client must wait to be certain a missing transaction will
/// not be processed by the network.
pub const MAX_HASH_AGE_IN_SECONDS: usize = 120;

// This must be <= MAX_HASH_AGE_IN_SECONDS, otherwise there's risk for DuplicateSignature errors
pub const MAX_RECENT_TRANSACTION_SEALS: usize = MAX_HASH_AGE_IN_SECONDS;

/// This is maximum time consumed in forwarding a transaction from one node to next, before
/// it can be processed in the target node
#[cfg(feature = "cuda")]
pub const MAX_TRANSACTION_FORWARDING_DELAY: usize = 4;

/// More delay is expected if CUDA is not enabled (as signature verification takes longer)
#[cfg(not(feature = "cuda"))]
pub const MAX_TRANSACTION_FORWARDING_DELAY: usize = 12;
pub const QUALIFIER: &str = "org";
pub const ORGANIZATION: &str = "bitconch-core";
pub const APPLICATION: &str = "bitconch-core";
pub const KEYS_DIRECTORY: &str = "keys";
pub const N3H_BINARIES_DIRECTORY: &str = "n3h-binaries";
pub const DNA_EXTENSION: &str = "dna.json";