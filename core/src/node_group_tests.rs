use crate::block_buffer_pool::BlockBufferPool;
/// NodeGroup independant integration tests
///
/// All tests must start from an entry point and a funding keypair and
/// discover the rest of the network.
use crate::node_group_info::FULLNODE_PORT_RANGE;
use crate::connection_info::ContactInfo;
use crate::entry_info::{Entry, EntrySlice};
use crate::gossip_service::find_node_group_host;
use crate::fork_selection::VOTE_THRESHOLD_DEPTH;
use morgan_client::slim_account_host::create_client;
use morgan_runtime::epoch_schedule::MINIMUM_SLOT_LENGTH;
use morgan_interface::client::SyncClient;
use morgan_interface::hash::Hash;
use morgan_interface::waterclock_config::WaterClockConfig;
use morgan_interface::signature::{Keypair, KeypairUtil, Signature};
use morgan_interface::system_transaction;
use morgan_interface::timing::{
    duration_as_ms, DEFAULT_NUM_DROPS_PER_SECOND, DEFAULT_DROPS_PER_SLOT,
    NUM_CONSECUTIVE_LEADER_SLOTS,
};
use morgan_interface::transport::TransportError;
use std::thread::sleep;
use std::time::Duration;
use morgan_helper::logHelper::*;
use futures::{
    future::Future,
    io::{AsyncRead, AsyncWrite},
    stream::Stream,
};
use std::{fmt::Debug, io};
use std::{borrow::Cow, convert, ffi::OsStr, path::Path, str};

const DEFAULT_SLOT_MILLIS: u64 = (DEFAULT_DROPS_PER_SLOT * 1000) / DEFAULT_NUM_DROPS_PER_SECOND;

/// Spend and verify from every node in the network
pub fn spend_and_verify_all_nodes(
    entry_point_info: &ContactInfo,
    funding_keypair: &Keypair,
    nodes: usize,
) {
    let (node_group_hosts, _) = find_node_group_host(&entry_point_info.gossip, nodes).unwrap();
    assert!(node_group_hosts.len() >= nodes);
    for ingress_node in &node_group_hosts {
        let random_keypair = Keypair::new();
        let client = create_client(ingress_node.client_facing_addr(), FULLNODE_PORT_RANGE);
        let bal = client
            .poll_get_balance(&funding_keypair.pubkey())
            .expect("balance in genesis");
        assert!(bal > 0);
        let (transaction_seal, _fee_calculator) = client.get_recent_transaction_seal().unwrap();
        let mut transaction =
            system_transaction::transfer(&funding_keypair, &random_keypair.pubkey(), 1, transaction_seal);
        let confs = VOTE_THRESHOLD_DEPTH + 1;
        let sig = client
            .retry_transfer_until_confirmed(&funding_keypair, &mut transaction, 5, confs)
            .unwrap();
        for validator in &node_group_hosts {
            let client = create_client(validator.client_facing_addr(), FULLNODE_PORT_RANGE);
            client.poll_for_signature_confirmation(&sig, confs).unwrap();
        }
    }
}

pub fn send_many_transactions(node: &ContactInfo, funding_keypair: &Keypair, num_txs: u64) {
    let client = create_client(node.client_facing_addr(), FULLNODE_PORT_RANGE);
    for _ in 0..num_txs {
        let random_keypair = Keypair::new();
        let bal = client
            .poll_get_balance(&funding_keypair.pubkey())
            .expect("balance in genesis");
        assert!(bal > 0);
        let (transaction_seal, _fee_calculator) = client.get_recent_transaction_seal().unwrap();
        let mut transaction =
            system_transaction::transfer(&funding_keypair, &random_keypair.pubkey(), 1, transaction_seal);
        client
            .retry_transfer(&funding_keypair, &mut transaction, 5)
            .unwrap();
    }
}

pub fn fullnode_exit(entry_point_info: &ContactInfo, nodes: usize) {
    let (node_group_hosts, _) = find_node_group_host(&entry_point_info.gossip, nodes).unwrap();
    assert!(node_group_hosts.len() >= nodes);
    for node in &node_group_hosts {
        let client = create_client(node.client_facing_addr(), FULLNODE_PORT_RANGE);
        assert!(client.fullnode_exit().unwrap());
    }
    sleep(Duration::from_millis(DEFAULT_SLOT_MILLIS));
    for node in &node_group_hosts {
        let client = create_client(node.client_facing_addr(), FULLNODE_PORT_RANGE);
        assert!(client.fullnode_exit().is_err());
    }
}

pub fn verify_ledger_drops(ledger_path: &str, drops_per_slot: usize) {
    let ledger = BlockBufferPool::open_ledger_file(ledger_path).unwrap();
    let zeroth_slot = ledger.fetch_slot_entries(0, 0, None).unwrap();
    let last_id = zeroth_slot.last().unwrap().hash;
    let next_slots = ledger.fetch_slot_from(&[0]).unwrap().remove(&0).unwrap();
    let mut pending_slots: Vec<_> = next_slots
        .into_iter()
        .map(|slot| (slot, 0, last_id))
        .collect();
    while !pending_slots.is_empty() {
        let (slot, parent_slot, last_id) = pending_slots.pop().unwrap();
        let next_slots = ledger
            .fetch_slot_from(&[slot])
            .unwrap()
            .remove(&slot)
            .unwrap();

        // If you're not the last slot, you should have a full set of drops
        let should_verify_drops = if !next_slots.is_empty() {
            Some((slot - parent_slot) as usize * drops_per_slot)
        } else {
            None
        };

        let last_id = verify_slot_drops(&ledger, slot, &last_id, should_verify_drops);
        pending_slots.extend(
            next_slots
                .into_iter()
                .map(|child_slot| (child_slot, slot, last_id)),
        );
    }
}

/// A StreamMultiplexer is responsible for multiplexing multiple [`AsyncRead`]/[`AsyncWrite`]
/// streams over a single underlying [`AsyncRead`]/[`AsyncWrite`] stream.
///
/// New substreams are opened either by [listening](StreamMultiplexer::listen_for_inbound) for
/// inbound substreams opened by the remote side or by [opening](StreamMultiplexer::open_outbound)
/// and outbound substream locally.
pub trait StreamMultiplexer: Debug + Send + Sync {
    /// The type of substreams opened by this Multiplexer.
    ///
    /// Must implement both AsyncRead and AsyncWrite.
    type Substream: AsyncRead + AsyncWrite + Send + Debug + Unpin;

    /// A stream of new [`Substreams`](StreamMultiplexer::Substream) opened by the remote side.
    type Listener: Stream<Item = io::Result<Self::Substream>> + Send + Unpin;

    /// A pending [`Substream`](StreamMultiplexer::Substream) to be opened on the underlying
    /// connection, obtained from [requesting a new substream](StreamMultiplexer::open_outbound).
    type Outbound: Future<Output = io::Result<Self::Substream>> + Send;

    /// A pending request to shut down the underlying connection, obtained from
    /// [closing](StreamMultiplexer::close).
    type Close: Future<Output = io::Result<()>> + Send;

    /// Returns a stream of new Substreams opened by the remote side.
    fn listen_for_inbound(&self) -> Self::Listener;

    /// Requests that a new Substream be opened.
    fn open_outbound(&self) -> Self::Outbound;

    /// Close and shutdown this [`StreamMultiplexer`].
    ///
    /// After the returned future has resolved this multiplexer will be shutdown.  All subsequent
    /// reads or writes to any still existing handles to substreams opened through this multiplexer
    /// must return EOF (in the case of a read), or an error.
    fn close(&self) -> Self::Close;
}

pub fn sleep_n_epochs(
    num_epochs: f64,
    config: &WaterClockConfig,
    drops_per_slot: u64,
    slots_per_epoch: u64,
) {
    let num_drops_per_second = (1000 / duration_as_ms(&config.target_drop_duration)) as f64;
    let num_drops_to_sleep = num_epochs * drops_per_slot as f64 * slots_per_epoch as f64;
    let secs = ((num_drops_to_sleep + num_drops_per_second - 1.0) / num_drops_per_second) as u64;
    // warn!("sleep_n_epochs: {} seconds", secs);
    println!(
        "{}",
        Warn(
            format!("sleep_n_epochs: {} seconds", secs).to_string(),
            module_path!().to_string()
        )
    );
    sleep(Duration::from_secs(secs));
}

pub fn kill_entry_and_spend_and_verify_rest(
    entry_point_info: &ContactInfo,
    funding_keypair: &Keypair,
    nodes: usize,
    slot_millis: u64,
) {
    morgan_logger::setup();
    let (node_group_hosts, _) = find_node_group_host(&entry_point_info.gossip, nodes).unwrap();
    assert!(node_group_hosts.len() >= nodes);
    let client = create_client(entry_point_info.client_facing_addr(), FULLNODE_PORT_RANGE);
    let first_two_epoch_slots = MINIMUM_SLOT_LENGTH * 3;

    for ingress_node in &node_group_hosts {
        client
            .poll_get_balance(&ingress_node.id)
            .unwrap_or_else(|err| panic!("Node {} has no balance: {}", ingress_node.id, err));
    }

    // info!("{}", Info(format!("sleeping for 2 leader fortnights").to_string()));
    let loginfo: String = format!("sleeping for 2 leader fortnights").to_string();
    println!("{}",
        printLn(
            loginfo,
            module_path!().to_string()
        )
    );

    sleep(Duration::from_millis(
        slot_millis * first_two_epoch_slots as u64,
    ));
    // info!("{}", Info(format!("done sleeping for first 2 warmup epochs").to_string()));
    println!("{}",
        printLn(
            format!("done sleeping for first 2 warmup epochs").to_string(),
            module_path!().to_string()
        )
    );
    // info!("{}", Info(format!("killing entry point: {}", entry_point_info.id).to_string()));
    println!("{}",
        printLn(
            format!("killing entry point: {}", entry_point_info.id).to_string(),
            module_path!().to_string()
        )
    );
    assert!(client.fullnode_exit().unwrap());
    // info!("{}", Info(format!("sleeping for some time").to_string()));
    println!("{}",
        printLn(
            format!("sleeping for some time").to_string(),
            module_path!().to_string()
        )
    );
    sleep(Duration::from_millis(
        slot_millis * NUM_CONSECUTIVE_LEADER_SLOTS,
    ));
    // info!("{}", Info(format!("done sleeping for 2 fortnights").to_string()));
    println!("{}",
        printLn(
            format!("done sleeping for 2 fortnights").to_string(),
            module_path!().to_string()
        )
    );
    for ingress_node in &node_group_hosts {
        if ingress_node.id == entry_point_info.id {
            continue;
        }

        let client = create_client(ingress_node.client_facing_addr(), FULLNODE_PORT_RANGE);
        let balance = client
            .poll_get_balance(&funding_keypair.pubkey())
            .expect("balance in genesis");
        assert_ne!(balance, 0);

        let mut result = Ok(());
        let mut retries = 0;
        loop {
            retries += 1;
            if retries > 5 {
                result.unwrap();
            }

            let random_keypair = Keypair::new();
            let (transaction_seal, _fee_calculator) = client.get_recent_transaction_seal().unwrap();
            let mut transaction = system_transaction::transfer(
                &funding_keypair,
                &random_keypair.pubkey(),
                1,
                transaction_seal,
            );

            let confs = VOTE_THRESHOLD_DEPTH + 1;
            let sig = {
                let sig = client.retry_transfer_until_confirmed(
                    &funding_keypair,
                    &mut transaction,
                    5,
                    confs,
                );
                match sig {
                    Err(e) => {
                        result = Err(TransportError::IoError(e));
                        continue;
                    }

                    Ok(sig) => sig,
                }
            };

            match poll_all_nodes_for_signature(&entry_point_info, &node_group_hosts, &sig, confs) {
                Err(e) => {
                    result = Err(e);
                }
                Ok(()) => {
                    break;
                }
            }
        }
    }
}

fn poll_all_nodes_for_signature(
    entry_point_info: &ContactInfo,
    node_group_hosts: &[ContactInfo],
    sig: &Signature,
    confs: usize,
) -> Result<(), TransportError> {
    for validator in node_group_hosts {
        if validator.id == entry_point_info.id {
            continue;
        }
        let client = create_client(validator.client_facing_addr(), FULLNODE_PORT_RANGE);
        client.poll_for_signature_confirmation(&sig, confs)?;
    }

    Ok(())
}


fn get_and_verify_slot_entries(block_buffer_pool: &BlockBufferPool, slot: u64, last_entry: &Hash) -> Vec<Entry> {
    let entries = block_buffer_pool.fetch_slot_entries(slot, 0, None).unwrap();
    assert!(entries.verify(last_entry));
    entries
}

fn verify_slot_drops(
    block_buffer_pool: &BlockBufferPool,
    slot: u64,
    last_entry: &Hash,
    expected_num_drops: Option<usize>,
) -> Hash {
    let entries = get_and_verify_slot_entries(block_buffer_pool, slot, last_entry);
    let num_drops: usize = entries.iter().map(|entry| entry.is_drop() as usize).sum();
    if let Some(expected_num_drops) = expected_num_drops {
        assert_eq!(num_drops, expected_num_drops);
    }
    entries.last().unwrap().hash
}
