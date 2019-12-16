//! The `morgan` library implements the Morgan high-performance blockchain architecture.
//! It includes a full Rust implementation of the architecture (see
//! [Validator](server/struct.Validator.html)) as well as hooks to GPU implementations of its most
//! paralellizable components (i.e. [SigVerify](sigverify/index.html)).  It also includes
//! command-line tools to spin up fullnodes and a Rust library
//!

// pub mod treasury_forks;
pub mod treasury_forks;
pub mod treasury_phase;
pub mod fetch_spot_phase;
pub mod propagate_phase;
#[cfg(feature = "chacha")]
pub mod chacha;
#[cfg(all(feature = "chacha", feature = "cuda"))]
pub mod chacha_cuda;
pub mod node_group_info_voter_listener;
#[macro_use]
pub mod connection_info;
pub mod connection_info_table;
pub mod gossip;
pub mod gossip_error_type;
pub mod pull_from_gossip;
pub mod push_to_gossip;
pub mod propagation_value;
#[macro_use]
pub mod block_buffer_pool;
pub mod block_stream;
pub mod block_stream_service;
pub mod block_buffer_pool_processor;
pub mod node_group;
pub mod node_group_info;
pub mod node_group_info_fixer_listener;
pub mod node_group_tests;
pub mod entry_info;
pub mod expunge;
pub mod fetch_phase;
pub mod create_keys;
pub mod genesis_utils;
pub mod gossip_service;
pub mod leader_arrange;
pub mod leader_arrange_cache;
pub mod leader_arrange_utils;
pub mod local_node_group;
pub mod local_vote_signer_service;
pub mod fork_selection;
pub mod packet;
pub mod water_clock;
pub mod water_clock_recorder;
pub mod water_clock_service;
pub mod recvmmsg;
pub mod fix_missing_spot_service;
pub mod repeat_phase;
pub mod cloner;
pub mod result;
pub mod retransmit_phase;
pub mod rpc;
pub mod rpc_pub_sub;
pub mod rpc_pub_subervice;
pub mod rpc_service;
pub mod rpc_subscriptions;
pub mod service;
pub mod signature_verify;
pub mod signature_verify_phase;
pub mod staking_utils;
pub mod storage_stage;
pub mod streamer;
pub mod test_tx;
pub mod transaction_process_centre;
pub mod transaction_verify_centre;
pub mod verifier;
pub mod spot_transmit_service;

#[macro_use]
extern crate morgan_budget_controller;

#[cfg(test)]
#[cfg(any(feature = "chacha", feature = "cuda"))]
#[macro_use]
extern crate hex_literal;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate morgan_metricbot;

#[cfg(test)]
#[macro_use]
extern crate matches;
