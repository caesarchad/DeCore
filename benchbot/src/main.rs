mod bench;
mod cli;

use crate::bench::{do_bench_tps, generate_and_fund_keypairs, Config, NUM_DIFS_PER_ACCOUNT};
use morgan::gossip_service::{find_node_group_host, get_clients};
use std::process::exit;

fn main() {
    morgan_logger::setup();
    morgan_metricbot::set_panic_hook("bench-tps");

    let matches = cli::build_args().get_matches();
    let cli_config = cli::extract_args(&matches);

    let cli::Config {
        connection_url_addr,
        drone_addr,
        id,
        threads,
        num_nodes,
        duration,
        tx_count,
        thread_batch_sleep_ms,
        sustained,
    } = cli_config;

    println!("Connecting to the cluster");
    let (nodes, _storage_miners) =
        find_node_group_host(&connection_url_addr, num_nodes).unwrap_or_else(|err| {
            eprintln!("Failed to discover {} nodes: {:?}", num_nodes, err);
            exit(1);
        });
    if nodes.len() < num_nodes {
        eprintln!(
            "Error: Insufficient nodes discovered.  Expecting {} or more",
            num_nodes
        );
        exit(1);
    }

    let clients = get_clients(&nodes);

    let (keypairs, keypair_balance) = generate_and_fund_keypairs(
        &clients[0],
        Some(drone_addr),
        &id,
        tx_count,
        NUM_DIFS_PER_ACCOUNT,
    );

    let config = Config {
        id,
        threads,
        thread_batch_sleep_ms,
        duration,
        tx_count,
        sustained,
    };

    do_bench_tps(clients, config, keypairs, keypair_balance);
}
