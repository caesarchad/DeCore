use clap::{crate_description, crate_name, crate_version, App, Arg};
use morgan::node_group_info::{Node, FULLNODE_PORT_RANGE};
use morgan::connection_info::ContactInfo;
use morgan::local_vote_signer_service::LocalVoteSignerService;
use morgan::service::Service;
use morgan::socketaddr;
use morgan::verifier::{Validator, ValidatorConfig};
use morgan_netutil::parse_port_range;
use morgan_interface::signature::{read_keypair, Keypair, KeypairUtil};
use std::fs::File;
use std::net::SocketAddr;
use std::process::exit;
use std::sync::Arc;
use morgan_helper::logHelper::*;

fn port_range_validator(port_range: String) -> Result<(), String> {
    if parse_port_range(&port_range).is_some() {
        Ok(())
    } else {
        Err("Invalid port range".to_string())
    }
}

fn main() {
    morgan_logger::setup();
    morgan_metricbot::set_panic_hook("validator");

    let default_dynamic_port_range =
        &format!("{}-{}", FULLNODE_PORT_RANGE.0, FULLNODE_PORT_RANGE.1);

    let matches = App::new(crate_name!()).about(crate_description!())
        .version(crate_version!())
        .arg(
            Arg::with_name("blockstream")
                .long("blockstream")
                .takes_value(true)
                .value_name("UNIX DOMAIN SOCKET")
                .help("Open blockstream at this unix domain socket location")
        )
        .arg(
            Arg::with_name("identity")
                .short("i")
                .long("identity")
                .value_name("PATH")
                .takes_value(true)
                .help("File containing an identity (keypair)"),
        )
        .arg(
            Arg::with_name("vote_account")
                .long("vote-account")
                .value_name("PUBKEY_BASE58_STR")
                .takes_value(true)
                .help("Public key of the vote account, where to send votes"),
        )
        .arg(
            Arg::with_name("voting_keypair")
                .long("voting-keypair")
                .value_name("PATH")
                .takes_value(true)
                .help("File containing the authorized voting keypair"),
        )
        .arg(
            Arg::with_name("storage_keypair")
                .long("storage-keypair")
                .value_name("PATH")
                .takes_value(true)
                .required(true)
                .help("File containing the storage account keypair"),
        )
        .arg(
            Arg::with_name("init_complete_file")
                .long("init-complete-file")
                .value_name("FILE")
                .takes_value(true)
                .help("Create this file, if it doesn't already exist, once node initialization is complete"),
        )
        .arg(
            Arg::with_name("ledger")
                .short("l")
                .long("ledger")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                .help("Use DIR as persistent ledger location"),
        )
        .arg(
            Arg::with_name("entrypoint")
                .short("n")
                .long("entrypoint")
                .value_name("HOST:PORT")
                .takes_value(true)
                .help("Rendezvous with the cluster at this connection url"),
        )
        .arg(
            Arg::with_name("no_voting")
                .long("no-voting")
                .takes_value(false)
                .help("Launch node without voting"),
        )
        .arg(
            Arg::with_name("no_sigverify")
                .short("v")
                .long("no-sigverify")
                .takes_value(false)
                .help("Run without signature verification"),
        )
        .arg(
            Arg::with_name("rpc_port")
                .long("rpc-port")
                .value_name("PORT")
                .takes_value(true)
                .help("RPC port to use for this node"),
        )
        .arg(
            Arg::with_name("enable_rpc_exit")
                .long("enable-rpc-exit")
                .takes_value(false)
                .help("Enable the JSON RPC 'fullnodeExit' API.  Only enable in a debug environment"),
        )
        .arg(
            Arg::with_name("rpc_drone_address")
                .long("rpc-drone-address")
                .value_name("HOST:PORT")
                .takes_value(true)
                .help("Enable the JSON RPC 'requestAirdrop' API with this drone address."),
        )
        .arg(
            Arg::with_name("signer")
                .short("s")
                .long("signer")
                .value_name("HOST:PORT")
                .takes_value(true)
                .help("Rendezvous with the vote signer at this RPC end point"),
        )
        .arg(
            Arg::with_name("accounts")
                .short("a")
                .long("accounts")
                .value_name("PATHS")
                .takes_value(true)
                .help("Comma separated persistent accounts location"),
        )
        .arg(
            clap::Arg::with_name("gossip_port")
                .long("gossip-port")
                .value_name("HOST:PORT")
                .takes_value(true)
                .help("Gossip port number for the node"),
        )
        .arg(
            clap::Arg::with_name("dynamic_port_range")
                .long("dynamic-port-range")
                .value_name("MIN_PORT-MAX_PORT")
                .takes_value(true)
                .default_value(default_dynamic_port_range)
                .validator(port_range_validator)
                .help("Range to use for dynamically assigned ports"),
        )
        .get_matches();

    let mut validator_config = ValidatorConfig::default();
    let keypair = if let Some(identity) = matches.value_of("identity") {
        read_keypair(identity).unwrap_or_else(|err| {
            eprintln!("{}: Unable to open keypair file: {}", err, identity);
            exit(1);
        })
    } else {
        Keypair::new()
    };
    let voting_keypair = if let Some(identity) = matches.value_of("voting_keypair") {
        read_keypair(identity).unwrap_or_else(|err| {
            eprintln!("{}: Unable to open keypair file: {}", err, identity);
            exit(1);
        })
    } else {
        Keypair::new()
    };
    let storage_keypair = if let Some(storage_keypair) = matches.value_of("storage_keypair") {
        read_keypair(storage_keypair).unwrap_or_else(|err| {
            eprintln!("{}: Unable to open keypair file: {}", err, storage_keypair);
            exit(1);
        })
    } else {
        Keypair::new()
    };

    let staking_account = matches
        .value_of("staking_account")
        .map_or(voting_keypair.pubkey(), |pubkey| {
            pubkey.parse().expect("failed to parse staking_account")
        });

    let ledger_path = matches.value_of("ledger").unwrap();

    validator_config.sigverify_disabled = matches.is_present("no_sigverify");

    validator_config.voting_disabled = matches.is_present("no_voting");

    if matches.is_present("enable_rpc_exit") {
        validator_config.rpc_config.enable_fullnode_exit = true;
    }
    validator_config.rpc_config.drone_addr = matches.value_of("rpc_drone_address").map(|address| {
        morgan_netutil::parse_host_port(address).expect("failed to parse drone address")
    });

    let dynamic_port_range = parse_port_range(matches.value_of("dynamic_port_range").unwrap())
        .expect("invalid dynamic_port_range");

    let mut gossip_addr = morgan_netutil::parse_port_or_addr(
        matches.value_of("gossip_port"),
        socketaddr!(
            [127, 0, 0, 1],
            morgan_netutil::find_available_port_in_range(dynamic_port_range)
                .expect("unable to find an available gossip port")
        ),
    );

    if let Some(paths) = matches.value_of("accounts") {
        validator_config.account_paths = Some(paths.to_string());
    } else {
        validator_config.account_paths = None;
    }
    let node_group_entrypoint = matches.value_of("entrypoint").map(|entrypoint| {
        let connection_url_addr = morgan_netutil::parse_host_port(entrypoint)
            .expect("failed to parse entrypoint address");
        gossip_addr.set_ip(morgan_netutil::get_public_ip_addr(&connection_url_addr).unwrap());

        ContactInfo::new_gossip_connection_url(&connection_url_addr)
    });
    let (_signer_service, _signer_addr) = if let Some(signer_addr) = matches.value_of("signer") {
        (
            None,
            signer_addr.to_string().parse().expect("Signer IP Address"),
        )
    } else {
        // Run a local vote signer if a vote signer service address was not provided
        let (signer_service, signer_addr) = LocalVoteSignerService::new(dynamic_port_range);
        (Some(signer_service), signer_addr)
    };
    let init_complete_file = matches.value_of("init_complete_file");
    validator_config.blockstream = matches.value_of("blockstream").map(ToString::to_string);

    let keypair = Arc::new(keypair);
    let mut node = Node::new_with_external_ip(&keypair.pubkey(), &gossip_addr, dynamic_port_range);
    if let Some(port) = matches.value_of("rpc_port") {
        let port_number = port.to_string().parse().expect("integer");
        if port_number == 0 {
            eprintln!("Invalid RPC port requested: {:?}", port);
            exit(1);
        }
        node.info.rpc = SocketAddr::new(gossip_addr.ip(), port_number);
        node.info.rpc_pubsub = SocketAddr::new(gossip_addr.ip(), port_number + 1);
    };

    let validator = Validator::new(
        node,
        &keypair,
        ledger_path,
        &staking_account,
        &Arc::new(voting_keypair),
        &Arc::new(storage_keypair),
        node_group_entrypoint.as_ref(),
        &validator_config,
    );

    if let Some(filename) = init_complete_file {
        File::create(filename).unwrap_or_else(|_| panic!("Unable to create: {}", filename));
    }
    // info!("{}", Info(format!("Validator initialized").to_string()));
    println!("{}",
        printLn(
            format!("Verifier initialized").to_string(),
            module_path!().to_string()
        )
    );
    validator.join().expect("Verifier exit");
    // info!("{}", Info(format!("Validator exiting..").to_string()));
    println!("{}",
        printLn(
            format!("Verifier exiting..").to_string(),
            module_path!().to_string()
        )
    );
}
