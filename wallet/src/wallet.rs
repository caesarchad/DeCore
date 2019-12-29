use chrono::prelude::*;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use log::*;
use num_traits::FromPrimitive;
use serde_json;
use serde_json::json;
use morgan_bvm_script;
use morgan_bvm_script::sc_opcode;
use morgan_bvm_script::script_state::BudgetError;
use morgan_client::account_host_err::ClientError;
use morgan_client::rpc_client::RpcClient;
#[cfg(not(test))]
use morgan_tokenbot::drone::request_airdrop_transaction;
use morgan_tokenbot::drone::DRONE_PORT;
#[cfg(test)]
use morgan_tokenbot::drone_mock::request_airdrop_transaction;
use morgan_interface::account_utils::State;
use morgan_interface::bvm_loader;
use morgan_interface::hash::Hash;
use morgan_interface::opcodes::OpCodeErr;
use morgan_interface::opcodes_utils::DecodeError;
use morgan_interface::mounter_opcode;
use morgan_interface::context::Context;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::{read_keypair, Keypair, KeypairUtil, Signature};
use morgan_interface::sys_opcode::SystemError;
use morgan_interface::sys_controller;
use morgan_interface::transaction::{Transaction, TransactionError};
use morgan_stake_api::stake_opcode;
use morgan_storage_api::poc_opcode;
use morgan_vote_api::vote_opcode;
use std::fs::File;
use std::io::Read;
use std::net::{IpAddr, SocketAddr};
use std::thread::sleep;
use std::time::Duration;
use std::{error, fmt};
use morgan_helper::logHelper::*;

const USERDATA_CHUNK_SIZE: usize = 229; // Keep program chunks under PACKET_DATA_SIZE

#[derive(Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum WalletCommand {
    Address,
    Airdrop(u64),
    Balance(BvmAddr),
    Cancel(BvmAddr),
    Confirm(Signature),
    AuthorizeVoter(BvmAddr, Keypair, BvmAddr),
    CreateVoteAccount(BvmAddr, BvmAddr, u32, u64),
    ShowVoteAccount(BvmAddr),
    CreateStakeAccount(BvmAddr, u64),
    CreateMiningPoolAccount(BvmAddr, u64),
    DelegateStake(Keypair, BvmAddr),
    RedeemVoteCredits(BvmAddr, BvmAddr, BvmAddr),
    ShowStakeAccount(BvmAddr),
    CreateStorageMiningPoolAccount(BvmAddr, u64),
    CreateMinerStorageAccount(BvmAddr),
    CreateValidatorStorageAccount(BvmAddr),
    ClaimStorageReward(BvmAddr, BvmAddr, u64),
    ShowStorageAccount(BvmAddr),
    Deploy(String),
    GetTransactionCount,
    // Pay(difs, to, timestamp, timestamp_address, witness(es), cancelable)
    Pay(
        u64,
        BvmAddr,
        Option<DateTime<Utc>>,
        Option<BvmAddr>,
        Option<Vec<BvmAddr>>,
        Option<BvmAddr>,
    ),
    // TimeElapsed(to, process_id, timestamp)
    TimeElapsed(BvmAddr, BvmAddr, DateTime<Utc>),
    // Endorsement(to, process_id)
    Endorsement(BvmAddr, BvmAddr),
}

#[derive(Debug, Clone)]
pub enum WalletError {
    CommandNotRecognized(String),
    BadParameter(String),
    DynamicProgramError(String),
    RpcRequestError(String),
}

impl fmt::Display for WalletError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid")
    }
}

impl error::Error for WalletError {
    fn description(&self) -> &str {
        "invalid"
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

pub struct WalletConfig {
    pub command: WalletCommand,
    pub drone_host: Option<IpAddr>,
    pub drone_port: u16,
    pub json_rpc_url: String,
    pub keypair: Keypair,
    pub rpc_client: Option<RpcClient>,
}

impl Default for WalletConfig {
    fn default() -> WalletConfig {
        WalletConfig {
            command: WalletCommand::Balance(BvmAddr::default()),
            drone_host: None,
            drone_port: DRONE_PORT,
            json_rpc_url: "http://testnet.morgan.com:10099".to_string(),
            keypair: Keypair::new(),
            rpc_client: None,
        }
    }
}

impl WalletConfig {
    pub fn drone_addr(&self) -> SocketAddr {
        SocketAddr::new(
            self.drone_host.unwrap_or_else(|| {
                let drone_host = url::Url::parse(&self.json_rpc_url)
                    .unwrap()
                    .host()
                    .unwrap()
                    .to_string();
                morgan_netutil::parse_host(&drone_host).unwrap_or_else(|err| {
                    panic!("Unable to resolve {}: {}", drone_host, err);
                })
            }),
            self.drone_port,
        )
    }
}

// Return the address for an argument with `name` or None if not present.
fn get_address(matches: &ArgMatches<'_>, name: &str) -> Option<BvmAddr> {
    matches.value_of(name).map(|x| x.parse::<BvmAddr>().unwrap())
}

// Return the addresss for arguments with `name` or None if none present.
fn address_list(matches: &ArgMatches<'_>, name: &str) -> Option<Vec<BvmAddr>> {
    matches
        .values_of(name)
        .map(|xs| xs.map(|x| x.parse::<BvmAddr>().unwrap()).collect())
}

// Return the keypair for an argument with filename `name` or None if not present.
fn keypair_of(matches: &ArgMatches<'_>, name: &str) -> Option<Keypair> {
    matches.value_of(name).map(|x| read_keypair(x).unwrap())
}

pub fn parse_command(
    address: &BvmAddr,
    matches: &ArgMatches<'_>,
) -> Result<WalletCommand, Box<dyn error::Error>> {
    let response = match matches.subcommand() {
        ("address", Some(_address_matches)) => Ok(WalletCommand::Address),
        ("airdrop", Some(airdrop_matches)) => {
            let difs = airdrop_matches.value_of("difs").unwrap().parse()?;
            Ok(WalletCommand::Airdrop(difs))
        }
        ("balance", Some(balance_matches)) => {
            let address = get_address(&balance_matches, "address").unwrap_or(*address);
            Ok(WalletCommand::Balance(address))
        }
        ("cancel", Some(cancel_matches)) => {
            let process_id = get_address(cancel_matches, "process_id").unwrap();
            Ok(WalletCommand::Cancel(process_id))
        }
        ("confirm", Some(confirm_matches)) => {
            match confirm_matches.value_of("signature").unwrap().parse() {
                Ok(signature) => Ok(WalletCommand::Confirm(signature)),
                _ => {
                    eprintln!("{}", confirm_matches.usage());
                    Err(WalletError::BadParameter("Invalid signature".to_string()))
                }
            }
        }
        ("create-vote-account", Some(matches)) => {
            let voting_account_address = get_address(matches, "voting_account_address").unwrap();
            let node_address = get_address(matches, "node_address").unwrap();
            let commission = if let Some(commission) = matches.value_of("commission") {
                commission.parse()?
            } else {
                0
            };
            let difs = matches.value_of("difs").unwrap().parse()?;
            Ok(WalletCommand::CreateVoteAccount(
                voting_account_address,
                node_address,
                commission,
                difs,
            ))
        }
        ("authorize-voter", Some(matches)) => {
            let voting_account_address = get_address(matches, "voting_account_address").unwrap();
            let authorized_voter_keypair =
                keypair_of(matches, "authorized_voter_keypair_file").unwrap();
            let new_authorized_voter_address =
                get_address(matches, "new_authorized_voter_address").unwrap();

            Ok(WalletCommand::AuthorizeVoter(
                voting_account_address,
                authorized_voter_keypair,
                new_authorized_voter_address,
            ))
        }
        ("show-vote-account", Some(matches)) => {
            let voting_account_address = get_address(matches, "voting_account_address").unwrap();
            Ok(WalletCommand::ShowVoteAccount(voting_account_address))
        }
        ("create-stake-account", Some(matches)) => {
            let staking_account_address = get_address(matches, "staking_account_address").unwrap();
            let difs = matches.value_of("difs").unwrap().parse()?;
            Ok(WalletCommand::CreateStakeAccount(
                staking_account_address,
                difs,
            ))
        }
        ("create-mining-pool-account", Some(matches)) => {
            let mining_pool_account_address =
                get_address(matches, "mining_pool_account_address").unwrap();
            let difs = matches.value_of("difs").unwrap().parse()?;
            Ok(WalletCommand::CreateMiningPoolAccount(
                mining_pool_account_address,
                difs,
            ))
        }
        ("delegate-stake", Some(matches)) => {
            let staking_account_keypair =
                keypair_of(matches, "staking_account_keypair_file").unwrap();
            let voting_account_address = get_address(matches, "voting_account_address").unwrap();
            Ok(WalletCommand::DelegateStake(
                staking_account_keypair,
                voting_account_address,
            ))
        }
        ("redeem-vote-credits", Some(matches)) => {
            let mining_pool_account_address =
                get_address(matches, "mining_pool_account_address").unwrap();
            let staking_account_address = get_address(matches, "staking_account_address").unwrap();
            let voting_account_address = get_address(matches, "voting_account_address").unwrap();
            Ok(WalletCommand::RedeemVoteCredits(
                mining_pool_account_address,
                staking_account_address,
                voting_account_address,
            ))
        }
        ("show-stake-account", Some(matches)) => {
            let staking_account_address = get_address(matches, "staking_account_address").unwrap();
            Ok(WalletCommand::ShowStakeAccount(staking_account_address))
        }
        ("create-storage-mining-pool-account", Some(matches)) => {
            let storage_mining_pool_account_address =
                get_address(matches, "storage_mining_pool_account_address").unwrap();
            let difs = matches.value_of("difs").unwrap().parse()?;
            Ok(WalletCommand::CreateStorageMiningPoolAccount(
                storage_mining_pool_account_address,
                difs,
            ))
        }
        ("create-storage-miner-storage-account", Some(matches)) => {
            let storage_account_address = get_address(matches, "storage_account_address").unwrap();
            Ok(WalletCommand::CreateMinerStorageAccount(
                storage_account_address,
            ))
        }
        ("create-validator-storage-account", Some(matches)) => {
            let storage_account_address = get_address(matches, "storage_account_address").unwrap();
            Ok(WalletCommand::CreateValidatorStorageAccount(
                storage_account_address,
            ))
        }
        ("claim-storage-reward", Some(matches)) => {
            let storage_mining_pool_account_address =
                get_address(matches, "storage_mining_pool_account_address").unwrap();
            let storage_account_address = get_address(matches, "storage_account_address").unwrap();
            let slot = matches.value_of("slot").unwrap().parse()?;
            Ok(WalletCommand::ClaimStorageReward(
                storage_mining_pool_account_address,
                storage_account_address,
                slot,
            ))
        }
        ("show-storage-account", Some(matches)) => {
            let storage_account_address = get_address(matches, "storage_account_address").unwrap();
            Ok(WalletCommand::ShowStorageAccount(storage_account_address))
        }
        ("deploy", Some(deploy_matches)) => Ok(WalletCommand::Deploy(
            deploy_matches
                .value_of("program_location")
                .unwrap()
                .to_string(),
        )),
        ("get-transaction-count", Some(_matches)) => Ok(WalletCommand::GetTransactionCount),
        ("pay", Some(pay_matches)) => {
            let difs = pay_matches.value_of("difs").unwrap().parse()?;
            let to = get_address(&pay_matches, "to").unwrap_or(*address);
            let timestamp = if pay_matches.is_present("timestamp") {
                // Parse input for serde_json
                let date_string = if !pay_matches.value_of("timestamp").unwrap().contains('Z') {
                    format!("\"{}Z\"", pay_matches.value_of("timestamp").unwrap())
                } else {
                    format!("\"{}\"", pay_matches.value_of("timestamp").unwrap())
                };
                Some(serde_json::from_str(&date_string)?)
            } else {
                None
            };
            let timestamp_address = get_address(&pay_matches, "timestamp_address");
            let witness_vec = address_list(&pay_matches, "witness");
            let cancelable = if pay_matches.is_present("cancelable") {
                Some(*address)
            } else {
                None
            };

            Ok(WalletCommand::Pay(
                difs,
                to,
                timestamp,
                timestamp_address,
                witness_vec,
                cancelable,
            ))
        }
        ("send-signature", Some(sig_matches)) => {
            let to = get_address(&sig_matches, "to").unwrap();
            let process_id = get_address(&sig_matches, "process_id").unwrap();
            Ok(WalletCommand::Endorsement(to, process_id))
        }
        ("send-timestamp", Some(timestamp_matches)) => {
            let to = get_address(&timestamp_matches, "to").unwrap();
            let process_id = get_address(&timestamp_matches, "process_id").unwrap();
            let dt = if timestamp_matches.is_present("datetime") {
                // Parse input for serde_json
                let date_string = if !timestamp_matches
                    .value_of("datetime")
                    .unwrap()
                    .contains('Z')
                {
                    format!("\"{}Z\"", timestamp_matches.value_of("datetime").unwrap())
                } else {
                    format!("\"{}\"", timestamp_matches.value_of("datetime").unwrap())
                };
                serde_json::from_str(&date_string)?
            } else {
                Utc::now()
            };
            Ok(WalletCommand::TimeElapsed(to, process_id, dt))
        }
        ("", None) => {
            eprintln!("{}", matches.usage());
            Err(WalletError::CommandNotRecognized(
                "no subcommand given".to_string(),
            ))
        }
        _ => unreachable!(),
    }?;
    Ok(response)
}

type ProcessResult = Result<String, Box<dyn error::Error>>;

fn process_airdrop(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    drone_addr: SocketAddr,
    difs: u64,
) -> ProcessResult {
    println!(
        "Requesting airdrop of {:?} difs from {}",
        difs, drone_addr
    );
    let previous_balance = match rpc_client.retry_get_balance(&config.keypair.address(), 5)? {
        Some(difs) => difs,
        None => Err(WalletError::RpcRequestError(
            "Received result of an unexpected type".to_string(),
        ))?,
    };

    request_and_confirm_airdrop(&rpc_client, &drone_addr, &config.keypair.address(), difs)?;

    let current_balance = rpc_client
        .retry_get_balance(&config.keypair.address(), 5)?
        .unwrap_or(previous_balance);

    if current_balance < previous_balance {
        Err(format!(
            "Airdrop failed: current_balance({}) < previous_balance({})",
            current_balance, previous_balance
        ))?;
    }
    if current_balance - previous_balance < difs {
        Err(format!(
            "Airdrop failed: Account balance increased by {} instead of {}",
            current_balance - previous_balance,
            difs
        ))?;
    }
    Ok(format!("Your balance is: {:?}", current_balance))
}

fn process_balance(address: &BvmAddr, rpc_client: &RpcClient) -> ProcessResult {
    let balance = rpc_client.retry_get_balance(address, 5)?;
    match balance {
        Some(difs) => {
            let ess = if difs == 1 { "" } else { "s" };
            Ok(format!("{:?} dif{}", difs, ess))
        }
        None => Err(WalletError::RpcRequestError(
            "Received result of an unexpected type".to_string(),
        ))?,
    }
}

fn process_confirm(rpc_client: &RpcClient, signature: &Signature) -> ProcessResult {
    match rpc_client.get_signature_status(&signature.to_string()) {
        Ok(status) => {
            if let Some(result) = status {
                match result {
                    Ok(_) => Ok("Confirmed".to_string()),
                    Err(err) => Ok(format!("Transaction failed with error {:?}", err)),
                }
            } else {
                Ok("Not found".to_string())
            }
        }
        Err(err) => Err(WalletError::RpcRequestError(format!(
            "Unable to confirm: {:?}",
            err
        )))?,
    }
}

fn process_create_vote_account(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    voting_account_address: &BvmAddr,
    node_address: &BvmAddr,
    commission: u32,
    difs: u64,
) -> ProcessResult {
    let ixs = vote_opcode::create_account(
        &config.keypair.address(),
        voting_account_address,
        node_address,
        commission,
        difs,
    );
    let (recent_transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let mut tx = Transaction::new_s_opcodes(&[&config.keypair], ixs, recent_transaction_seal);
    let signature_str = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair])?;
    Ok(signature_str.to_string())
}

fn process_authorize_voter(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    voting_account_address: &BvmAddr,
    authorized_voter_keypair: &Keypair,
    new_authorized_voter_address: &BvmAddr,
) -> ProcessResult {
    let (recent_transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let ixs = vec![vote_opcode::authorize_voter(
        &config.keypair.address(),           // from
        voting_account_address,              // vote account to update
        &authorized_voter_keypair.address(), // current authorized voter (often the vote account itself)
        new_authorized_voter_address,        // new vote signer
    )];

    let mut tx = Transaction::new_s_opcodes(
        &[&config.keypair, &authorized_voter_keypair],
        ixs,
        recent_transaction_seal,
    );
    let signature_str = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair])?;
    Ok(signature_str.to_string())
}

fn process_show_vote_account(
    rpc_client: &RpcClient,
    _config: &WalletConfig,
    voting_account_address: &BvmAddr,
) -> ProcessResult {
    use morgan_vote_api::vote_state::VoteState;
    let vote_account_difs = rpc_client.retry_get_balance(voting_account_address, 5)?;
    let vote_account_data = rpc_client.get_account_data(voting_account_address)?;
    let vote_state = VoteState::deserialize(&vote_account_data).map_err(|_| {
        WalletError::RpcRequestError(
            "Account data could not be deserialized to vote state".to_string(),
        )
    })?;

    println!("account difs: {}", vote_account_difs.unwrap());
    println!("node id: {}", vote_state.node_address);
    println!(
        "authorized voter address: {}",
        vote_state.authorized_voter_address
    );
    println!("credits: {}", vote_state.credits());
    println!(
        "commission: {}%",
        f64::from(vote_state.commission) / f64::from(std::u32::MAX)
    );
    println!(
        "root slot: {}",
        match vote_state.root_slot {
            Some(slot) => slot.to_string(),
            None => "~".to_string(),
        }
    );
    if !vote_state.votes.is_empty() {
        println!("votes:");
        for vote in vote_state.votes {
            println!(
                "- slot={}, confirmation count={}",
                vote.slot, vote.confirmation_count
            );
        }
    }
    Ok("".to_string())
}

fn process_create_stake_account(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    staking_account_address: &BvmAddr,
    difs: u64,
) -> ProcessResult {
    let (recent_transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let ixs = stake_opcode::create_delegate_account(
        &config.keypair.address(),
        staking_account_address,
        difs,
    );
    let mut tx = Transaction::new_s_opcodes(&[&config.keypair], ixs, recent_transaction_seal);
    let signature_str = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair])?;
    Ok(signature_str.to_string())
}

fn process_create_mining_pool_account(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    mining_pool_account_address: &BvmAddr,
    difs: u64,
) -> ProcessResult {
    let (recent_transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let ixs = stake_opcode::create_mining_pool_account(
        &config.keypair.address(),
        mining_pool_account_address,
        difs,
    );
    let mut tx = Transaction::new_s_opcodes(&[&config.keypair], ixs, recent_transaction_seal);
    let signature_str = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair])?;
    Ok(signature_str.to_string())
}

fn process_delegate_stake(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    staking_account_keypair: &Keypair,
    voting_account_address: &BvmAddr,
) -> ProcessResult {
    let (recent_transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let ixs = vec![stake_opcode::delegate_stake(
        &config.keypair.address(),
        &staking_account_keypair.address(),
        voting_account_address,
    )];
    let mut tx = Transaction::new_s_opcodes(
        &[&config.keypair, &staking_account_keypair],
        ixs,
        recent_transaction_seal,
    );
    let signature_str = rpc_client
        .send_and_confirm_transaction(&mut tx, &[&config.keypair, &staking_account_keypair])?;
    Ok(signature_str.to_string())
}

fn process_redeem_vote_credits(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    mining_pool_account_address: &BvmAddr,
    staking_account_address: &BvmAddr,
    voting_account_address: &BvmAddr,
) -> ProcessResult {
    let (recent_transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let ixs = vec![stake_opcode::redeem_vote_credits(
        &config.keypair.address(),
        mining_pool_account_address,
        staking_account_address,
        voting_account_address,
    )];
    let mut tx = Transaction::new_s_opcodes(&[&config.keypair], ixs, recent_transaction_seal);
    let signature_str = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair])?;
    Ok(signature_str.to_string())
}

fn process_show_stake_account(
    rpc_client: &RpcClient,
    _config: &WalletConfig,
    staking_account_address: &BvmAddr,
) -> ProcessResult {
    use morgan_stake_api::stake_state::StakeState;
    let stake_account = rpc_client.get_account(staking_account_address)?;
    match stake_account.state() {
        Ok(StakeState::Delegate {
            voter_address,
            credits_observed,
        }) => {
            println!("account difs: {}", stake_account.difs);
            println!("voter address: {}", voter_address);
            println!("credits observed: {}", credits_observed);
            Ok("".to_string())
        }
        Ok(StakeState::PocPool) => {
            println!("account difs: {}", stake_account.difs);
            Ok("".to_string())
        }
        _ => Err(WalletError::RpcRequestError(
            "Account data could not be deserialized to stake state".to_string(),
        ))?,
    }
}

fn process_create_storage_mining_pool_account(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    storage_account_address: &BvmAddr,
    difs: u64,
) -> ProcessResult {
    let (recent_transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let ixs = poc_opcode::create_mining_pool_account(
        &config.keypair.address(),
        storage_account_address,
        difs,
    );
    let mut tx = Transaction::new_s_opcodes(&[&config.keypair], ixs, recent_transaction_seal);
    let signature_str = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair])?;
    Ok(signature_str.to_string())
}

fn process_create_miner_storage_account(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    storage_account_address: &BvmAddr,
) -> ProcessResult {
    let (recent_transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let ixs = poc_opcode::create_miner_storage_account(
        &config.keypair.address(),
        storage_account_address,
        1,
    );
    let mut tx = Transaction::new_s_opcodes(&[&config.keypair], ixs, recent_transaction_seal);
    let signature_str = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair])?;
    Ok(signature_str.to_string())
}

fn process_create_validator_storage_account(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    storage_account_address: &BvmAddr,
) -> ProcessResult {
    let (recent_transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let ixs = poc_opcode::crt_vldr_strj_acct(
        &config.keypair.address(),
        storage_account_address,
        1,
    );
    let mut tx = Transaction::new_s_opcodes(&[&config.keypair], ixs, recent_transaction_seal);
    let signature_str = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair])?;
    Ok(signature_str.to_string())
}

fn process_claim_storage_reward(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    storage_mining_pool_account_address: &BvmAddr,
    storage_account_address: &BvmAddr,
    slot: u64,
) -> ProcessResult {
    let (recent_transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;

    let instruction = poc_opcode::claim_reward(
        storage_account_address,
        storage_mining_pool_account_address,
        slot,
    );
    let signers = [&config.keypair];
    let context = Context::new_with_payer(vec![instruction], Some(&signers[0].address()));

    let mut transaction = Transaction::new(&signers, context, recent_transaction_seal);
    let signature_str = rpc_client.send_and_confirm_transaction(&mut transaction, &signers)?;
    Ok(signature_str.to_string())
}

fn process_show_storage_account(
    rpc_client: &RpcClient,
    _config: &WalletConfig,
    storage_account_address: &BvmAddr,
) -> ProcessResult {
    use morgan_storage_api::poc_pact::PocType;
    let account = rpc_client.get_account(storage_account_address)?;
    let storage_contract: PocType = account.state().map_err(|err| {
        WalletError::RpcRequestError(
            format!("Unable to deserialize storage account: {:?}", err).to_string(),
        )
    })?;
    println!("{:?}", storage_contract);
    println!("account difs: {}", account.difs);
    Ok("".to_string())
}

fn process_deploy(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    program_location: &str,
) -> ProcessResult {
    let balance = rpc_client.retry_get_balance(&config.keypair.address(), 5)?;
    if let Some(difs) = balance {
        if difs < 1 {
            Err(WalletError::DynamicProgramError(
                "Insufficient funds".to_string(),
            ))?
        }
    }

    let (transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let program_id = Keypair::new();
    let mut file = File::open(program_location).map_err(|err| {
        WalletError::DynamicProgramError(
            format!("Unable to open program file: {}", err).to_string(),
        )
    })?;
    let mut program_data = Vec::new();
    file.read_to_end(&mut program_data).map_err(|err| {
        WalletError::DynamicProgramError(
            format!("Unable to read program file: {}", err).to_string(),
        )
    })?;

    let mut tx = sys_controller::create_account(
        &config.keypair,
        &program_id.address(),
        transaction_seal,
        1,
        program_data.len() as u64,
        &bvm_loader::id(),
    );
    trace!("Creating program account");
    let result = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair]);
    log_instruction_custom_error::<SystemError>(result).map_err(|_| {
        WalletError::DynamicProgramError("Program allocate space failed".to_string())
    })?;

    trace!("Writing program data");
    let signers = [&config.keypair, &program_id];
    let write_transactions: Vec<_> = program_data
        .chunks(USERDATA_CHUNK_SIZE)
        .zip(0..)
        .map(|(chunk, i)| {
            let instruction = morgan_interface::mounter_opcode::write(
                &program_id.address(),
                &bvm_loader::id(),
                (i * USERDATA_CHUNK_SIZE) as u32,
                chunk.to_vec(),
            );
            let context = Context::new_with_payer(vec![instruction], Some(&signers[0].address()));
            Transaction::new(&signers, context, transaction_seal)
        })
        .collect();
    rpc_client.send_and_confirm_transactions(write_transactions, &signers)?;

    trace!("Finalizing program account");
    let instruction = morgan_interface::mounter_opcode::finalize(&program_id.address(), &bvm_loader::id());
    let context = Context::new_with_payer(vec![instruction], Some(&signers[0].address()));
    let mut tx = Transaction::new(&signers, context, transaction_seal);
    rpc_client
        .send_and_confirm_transaction(&mut tx, &signers)
        .map_err(|_| {
            WalletError::DynamicProgramError("Program finalize transaction failed".to_string())
        })?;

    Ok(json!({
        "programId": format!("{}", program_id.address()),
    })
    .to_string())
}

fn process_pay(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    difs: u64,
    to: &BvmAddr,
    timestamp: Option<DateTime<Utc>>,
    timestamp_address: Option<BvmAddr>,
    witnesses: &Option<Vec<BvmAddr>>,
    cancelable: Option<BvmAddr>,
) -> ProcessResult {
    let (transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;

    if timestamp == None && *witnesses == None {
        let mut tx = sys_controller::transfer(&config.keypair, to, difs, transaction_seal);
        let result = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair]);
        let signature_str = log_instruction_custom_error::<SystemError>(result)?;
        Ok(signature_str.to_string())
    } else if *witnesses == None {
        let dt = timestamp.unwrap();
        let dt_address = match timestamp_address {
            Some(address) => address,
            None => config.keypair.address(),
        };

        let contract_state = Keypair::new();

        // Initializing contract
        let ixs = sc_opcode::on_date(
            &config.keypair.address(),
            to,
            &contract_state.address(),
            dt,
            &dt_address,
            cancelable,
            difs,
        );
        let mut tx = Transaction::new_s_opcodes(&[&config.keypair], ixs, transaction_seal);
        let result = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair]);
        let signature_str = log_instruction_custom_error::<BudgetError>(result)?;

        Ok(json!({
            "signature": signature_str,
            "processId": format!("{}", contract_state.address()),
        })
        .to_string())
    } else if timestamp == None {
        let (transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;

        let witness = if let Some(ref witness_vec) = *witnesses {
            witness_vec[0]
        } else {
            Err(WalletError::BadParameter(
                "Could not parse required signature address(s)".to_string(),
            ))?
        };

        let contract_state = Keypair::new();

        // Initializing contract
        let ixs = sc_opcode::when_signed(
            &config.keypair.address(),
            to,
            &contract_state.address(),
            &witness,
            cancelable,
            difs,
        );
        let mut tx = Transaction::new_s_opcodes(&[&config.keypair], ixs, transaction_seal);
        let result = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair]);
        let signature_str = log_instruction_custom_error::<BudgetError>(result)?;

        Ok(json!({
            "signature": signature_str,
            "processId": format!("{}", contract_state.address()),
        })
        .to_string())
    } else {
        Ok("Combo transactions not yet handled".to_string())
    }
}

fn process_cancel(rpc_client: &RpcClient, config: &WalletConfig, address: &BvmAddr) -> ProcessResult {
    let (transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let ix = sc_opcode::apply_signature(
        &config.keypair.address(),
        address,
        &config.keypair.address(),
    );
    let mut tx = Transaction::new_s_opcodes(&[&config.keypair], vec![ix], transaction_seal);
    let result = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair]);
    let signature_str = log_instruction_custom_error::<BudgetError>(result)?;
    Ok(signature_str.to_string())
}

fn process_get_transaction_count(rpc_client: &RpcClient) -> ProcessResult {
    let transaction_count = rpc_client.get_transaction_count()?;
    Ok(transaction_count.to_string())
}

fn process_time_elapsed(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    drone_addr: SocketAddr,
    to: &BvmAddr,
    address: &BvmAddr,
    dt: DateTime<Utc>,
) -> ProcessResult {
    let balance = rpc_client.retry_get_balance(&config.keypair.address(), 5)?;

    if let Some(0) = balance {
        request_and_confirm_airdrop(&rpc_client, &drone_addr, &config.keypair.address(), 1)?;
    }

    let (transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;

    let ix = sc_opcode::apply_timestamp(&config.keypair.address(), address, to, dt);
    let mut tx = Transaction::new_s_opcodes(&[&config.keypair], vec![ix], transaction_seal);
    let result = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair]);
    let signature_str = log_instruction_custom_error::<BudgetError>(result)?;

    Ok(signature_str.to_string())
}

fn process_witness(
    rpc_client: &RpcClient,
    config: &WalletConfig,
    drone_addr: SocketAddr,
    to: &BvmAddr,
    address: &BvmAddr,
) -> ProcessResult {
    let balance = rpc_client.retry_get_balance(&config.keypair.address(), 5)?;

    if let Some(0) = balance {
        request_and_confirm_airdrop(&rpc_client, &drone_addr, &config.keypair.address(), 1)?;
    }

    let (transaction_seal, _fee_calculator) = rpc_client.get_recent_transaction_seal()?;
    let ix = sc_opcode::apply_signature(&config.keypair.address(), address, to);
    let mut tx = Transaction::new_s_opcodes(&[&config.keypair], vec![ix], transaction_seal);
    let result = rpc_client.send_and_confirm_transaction(&mut tx, &[&config.keypair]);
    let signature_str = log_instruction_custom_error::<BudgetError>(result)?;

    Ok(signature_str.to_string())
}

pub fn process_command(config: &WalletConfig) -> ProcessResult {
    if let WalletCommand::Address = config.command {
        // Get address of this client
        return Ok(format!("{}", config.keypair.address()));
    }

    let drone_addr = config.drone_addr();

    let mut _rpc_client;
    let rpc_client = if config.rpc_client.is_none() {
        _rpc_client = RpcClient::new(config.json_rpc_url.to_string());
        &_rpc_client
    } else {
        // Primarily for testing
        config.rpc_client.as_ref().unwrap()
    };

    match &config.command {
        // Get address of this client
        WalletCommand::Address => unreachable!(),

        // Request an airdrop from Morgan Drone;
        WalletCommand::Airdrop(difs) => {
            process_airdrop(&rpc_client, config, drone_addr, *difs)
        }

        // Check client balance
        WalletCommand::Balance(address) => process_balance(&address, &rpc_client),

        // Cancel a contract by contract BvmAddr
        WalletCommand::Cancel(address) => process_cancel(&rpc_client, config, &address),

        // Confirm the last client transaction by signature
        WalletCommand::Confirm(signature) => process_confirm(&rpc_client, signature),

        // Create vote account
        WalletCommand::CreateVoteAccount(
            voting_account_address,
            node_address,
            commission,
            difs,
        ) => process_create_vote_account(
            &rpc_client,
            config,
            &voting_account_address,
            &node_address,
            *commission,
            *difs,
        ),
        // Configure staking account already created
        WalletCommand::AuthorizeVoter(
            voting_account_address,
            authorized_voter_keypair,
            new_authorized_voter_address,
        ) => process_authorize_voter(
            &rpc_client,
            config,
            &voting_account_address,
            &authorized_voter_keypair,
            &new_authorized_voter_address,
        ),
        // Show a vote account
        WalletCommand::ShowVoteAccount(voting_account_address) => {
            process_show_vote_account(&rpc_client, config, &voting_account_address)
        }

        // Create stake account
        WalletCommand::CreateStakeAccount(staking_account_address, difs) => {
            process_create_stake_account(&rpc_client, config, &staking_account_address, *difs)
        }

        WalletCommand::CreateMiningPoolAccount(mining_pool_account_address, difs) => {
            process_create_mining_pool_account(
                &rpc_client,
                config,
                &mining_pool_account_address,
                *difs,
            )
        }

        WalletCommand::DelegateStake(staking_account_keypair, voting_account_address) => {
            process_delegate_stake(
                &rpc_client,
                config,
                &staking_account_keypair,
                &voting_account_address,
            )
        }

        WalletCommand::RedeemVoteCredits(
            mining_pool_account_address,
            staking_account_address,
            voting_account_address,
        ) => process_redeem_vote_credits(
            &rpc_client,
            config,
            &mining_pool_account_address,
            &staking_account_address,
            &voting_account_address,
        ),

        WalletCommand::ShowStakeAccount(staking_account_address) => {
            process_show_stake_account(&rpc_client, config, &staking_account_address)
        }

        WalletCommand::CreateStorageMiningPoolAccount(storage_account_address, difs) => {
            process_create_storage_mining_pool_account(
                &rpc_client,
                config,
                &storage_account_address,
                *difs,
            )
        }

        WalletCommand::CreateMinerStorageAccount(storage_account_address) => {
            process_create_miner_storage_account(&rpc_client, config, &storage_account_address)
        }

        WalletCommand::CreateValidatorStorageAccount(storage_account_address) => {
            process_create_validator_storage_account(&rpc_client, config, &storage_account_address)
        }

        WalletCommand::ClaimStorageReward(
            storage_mining_pool_account_address,
            storage_account_address,
            slot,
        ) => process_claim_storage_reward(
            &rpc_client,
            config,
            &storage_mining_pool_account_address,
            &storage_account_address,
            *slot,
        ),

        WalletCommand::ShowStorageAccount(storage_account_address) => {
            process_show_storage_account(&rpc_client, config, &storage_account_address)
        }

        // Deploy a custom program to the chain
        WalletCommand::Deploy(ref program_location) => {
            process_deploy(&rpc_client, config, program_location)
        }

        WalletCommand::GetTransactionCount => process_get_transaction_count(&rpc_client),

        // If client has positive balance, pay difs to another address
        WalletCommand::Pay(
            difs,
            to,
            timestamp,
            timestamp_address,
            ref witnesses,
            cancelable,
        ) => process_pay(
            &rpc_client,
            config,
            *difs,
            &to,
            *timestamp,
            *timestamp_address,
            witnesses,
            *cancelable,
        ),

        // Apply time elapsed to contract
        WalletCommand::TimeElapsed(to, address, dt) => {
            process_time_elapsed(&rpc_client, config, drone_addr, &to, &address, *dt)
        }

        // Apply witness signature to contract
        WalletCommand::Endorsement(to, address) => {
            process_witness(&rpc_client, config, drone_addr, &to, &address)
        }
    }
}

// Quick and dirty Keypair that assumes the client will do retries but not update the
// transaction_seal. If the client updates the transaction_seal, the signature will be invalid.
// TODO: Parse `msg` and use that data to make a new airdrop request.
struct DroneKeypair {
    transaction: Transaction,
}

impl DroneKeypair {
    fn new_keypair(
        airdrop: &SocketAddr,
        to_addr: &BvmAddr,
        difs: u64,
        tx_seal: Hash,
    ) -> Result<Self, Box<dyn error::Error>> {
        let transaction = request_airdrop_transaction(airdrop, to_addr, difs, tx_seal)?;
        Ok(Self { transaction })
    }

    fn airdrop_transaction(&self) -> Transaction {
        self.transaction.clone()
    }
}

impl KeypairUtil for DroneKeypair {
    fn new() -> Self {
        unimplemented!();
    }

    /// Return the public key of the keypair used to sign votes
    fn address(&self) -> BvmAddr {
        self.transaction.self_context().account_keys[0]
    }

    fn sign_context(&self, _msg: &[u8]) -> Signature {
        self.transaction.signatures[0]
    }
}

pub fn request_and_confirm_airdrop(
    rpc_client: &RpcClient,
    airdrop: &SocketAddr,
    to_addr: &BvmAddr,
    difs: u64,
) -> Result<(), Box<dyn error::Error>> {
    let (tx_seal, _gas_cost) = rpc_client.get_recent_transaction_seal()?;
    let keypair = {
        let mut retries = 5;
        loop {
            let result = DroneKeypair::new_keypair(airdrop, to_addr, difs, tx_seal);
            if result.is_ok() || retries == 0 {
                break result;
            }
            retries -= 1;
            sleep(Duration::from_secs(1));
        }
    }?;
    let mut tx = keypair.airdrop_transaction();
    let result = rpc_client.send_and_confirm_transaction(&mut tx, &[&keypair]);
    log_instruction_custom_error::<SystemError>(result)?;
    Ok(())
}

fn log_instruction_custom_error<E>(result: Result<String, ClientError>) -> ProcessResult
where
    E: 'static + std::error::Error + DecodeError<E> + FromPrimitive,
{
    if result.is_err() {
        let err = result.unwrap_err();
        if let ClientError::TransactionError(TransactionError::OpCodeErr(
            _,
            OpCodeErr::CustomError(code),
        )) = err
        {
            if let Some(specific_error) = E::decode_custom_error_to_enum(code) {
                // error!(
                //     "{}",
                //     Error(format!("{:?}: {}::{:?}",
                //     err,
                //     specific_error.type_of(),
                //     specific_error).to_string())
                // );
                println!(
                    "{}",
                    Error(
                        format!("{:?}: {}::{:?}",
                            err,
                            specific_error.type_of(),
                            specific_error).to_string(),
                        module_path!().to_string()
                    )
                );
                Err(specific_error)?
            }
        }
        // error!("{}", Error(format!("{:?}", err).to_string()));
        println!(
            "{}",
            Error(
                format!("{:?}", err).to_string(),
                module_path!().to_string()
            )
        );
        Err(err)?
    } else {
        Ok(result.unwrap())
    }
}

// Return an error if a address cannot be parsed.
fn is_address(string: String) -> Result<(), String> {
    match string.parse::<BvmAddr>() {
        Ok(_) => Ok(()),
        Err(err) => Err(format!("{:?}", err)),
    }
}

pub fn app<'ab, 'v>(name: &str, about: &'ab str, version: &'v str) -> App<'ab, 'v> {
    App::new(name)
        .about(about)
        .version(version)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(SubCommand::with_name("address").about("Get your public key"))
        .subcommand(
            SubCommand::with_name("airdrop")
                .about("Request a batch of difs")
                .arg(
                    Arg::with_name("difs")
                        .index(1)
                        .value_name("NUM")
                        .takes_value(true)
                        .required(true)
                        .help("Air drop amount of tokens"),
                ),
        )
        .subcommand(
            SubCommand::with_name("balance")
                .about("Return the balance of the specific account")
                .arg(
                    Arg::with_name("address")
                        .index(1)
                        .value_name("ADDRESS")
                        .takes_value(true)
                        .validator(is_address)
                        .help("The account's address"),
                ),
        )
        .subcommand(
            SubCommand::with_name("cancel")
                .about("Cancel a transfer")
                .arg(
                    Arg::with_name("process_id")
                        .index(1)
                        .value_name("PROCESS_ID")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("The process id of the transfer to cancel"),
                ),
        )
        .subcommand(
            SubCommand::with_name("confirm")
                .about("Confirm transaction by signature")
                .arg(
                    Arg::with_name("signature")
                        .index(1)
                        .value_name("SIGNATURE")
                        .takes_value(true)
                        .required(true)
                        .help("The transaction signature to confirm"),
                ),
        )
        .subcommand(
            SubCommand::with_name("authorize-voter")
                .about("Authorize a new vote signing keypair for the given vote account")
                .arg(
                    Arg::with_name("voting_account_address")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Vote account in which to set the authorized voter"),
                )
                .arg(
                    Arg::with_name("authorized_voter_keypair_file")
                        .index(2)
                        .value_name("KEYPAIR_FILE")
                        .takes_value(true)
                        .required(true)
                        .help("Keypair file for the currently authorized vote signer"),
                )
                .arg(
                    Arg::with_name("new_authorized_voter_address")
                        .index(3)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("New vote signer to authorize"),
                ),
        )
        .subcommand(
            SubCommand::with_name("create-vote-account")
                .about("Create vote account for a node")
                .arg(
                    Arg::with_name("voting_account_address")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Vote account address to fund"),
                )
                .arg(
                    Arg::with_name("node_address")
                        .index(2)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Node that will vote in this account"),
                )
                .arg(
                    Arg::with_name("difs")
                        .index(3)
                        .value_name("NUM")
                        .takes_value(true)
                        .required(true)
                        .help("The number of difs to send to the vote account"),
                )
                .arg(
                    Arg::with_name("commission")
                        .long("commission")
                        .value_name("NUM")
                        .takes_value(true)
                        .help("The commission taken on reward redemption, default: 0"),
                ),
        )
        .subcommand(
            SubCommand::with_name("show-vote-account")
                .about("Show the contents of a vote account")
                .arg(
                    Arg::with_name("voting_account_address")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Vote account address"),
                )
        )
        .subcommand(
            SubCommand::with_name("create-mining-pool-account")
                .about("Create staking mining pool account")
                .arg(
                    Arg::with_name("mining_pool_account_address")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Staking mining pool account address to fund"),
                )
                .arg(
                    Arg::with_name("difs")
                        .index(2)
                        .value_name("NUM")
                        .takes_value(true)
                        .required(true)
                        .help("The number of difs to assign to the mining pool account"),
                ),
        )
       .subcommand(
            SubCommand::with_name("create-stake-account")
                .about("Create staking account")
                .arg(
                    Arg::with_name("staking_account_address")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Staking account address to fund"),
                )
                .arg(
                    Arg::with_name("difs")
                        .index(2)
                        .value_name("NUM")
                        .takes_value(true)
                        .required(true)
                        .help("The number of difs to send to staking account"),
                ),
        )
        .subcommand(
            SubCommand::with_name("delegate-stake")
                .about("Delegate the stake to some vote account")
                .arg(
                    Arg::with_name("staking_account_keypair_file")
                        .index(1)
                        .value_name("KEYPAIR_FILE")
                        .takes_value(true)
                        .required(true)
                        .help("Keypair file for the staking account, for signing the delegate transaction."),
                )
                .arg(
                    Arg::with_name("voting_account_address")
                        .index(2)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("The voting account to which to delegate the stake."),
                ),
        )
        .subcommand(
            SubCommand::with_name("redeem-vote-credits")
                .about("Redeem credits in the staking account")
                .arg(
                    Arg::with_name("mining_pool_account_address")
                        .index(1)
                        .value_name("MINING POOL PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Mining pool account to redeem credits from"),
                )
                .arg(
                    Arg::with_name("staking_account_address")
                        .index(2)
                        .value_name("STAKING ACCOUNT PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Staking account address to redeem credits for"),
                )
                .arg(
                    Arg::with_name("voting_account_address")
                        .index(3)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("The voting account to which the stake was previously delegated."),
                ),
        )
        .subcommand(
            SubCommand::with_name("show-stake-account")
                .about("Show the contents of a stake account")
                .arg(
                    Arg::with_name("staking_account_address")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Stake account address"),
                )
        )
        .subcommand(
            SubCommand::with_name("create-storage-mining-pool-account")
                .about("Create mining pool account")
                .arg(
                    Arg::with_name("storage_account_address")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Storage mining pool account address to fund"),
                )
                .arg(
                    Arg::with_name("difs")
                        .index(2)
                        .value_name("NUM")
                        .takes_value(true)
                        .required(true)
                        .help("The number of difs to assign to the storage mining pool account"),
                ),
        )
        .subcommand(
            SubCommand::with_name("create-storage-miner-storage-account")
                .about("Create a storage-miner storage account")
                .arg(
                    Arg::with_name("storage_account_address")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                )
        )
        .subcommand(
            SubCommand::with_name("create-validator-storage-account")
                .about("Create a validator storage account")
                .arg(
                    Arg::with_name("storage_account_address")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                )
        )
        .subcommand(
            SubCommand::with_name("claim-storage-reward")
                .about("Redeem storage reward credits")
                .arg(
                    Arg::with_name("storage_mining_pool_account_address")
                        .index(1)
                        .value_name("MINING POOL PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Mining pool account to redeem credits from"),
                )
                .arg(
                    Arg::with_name("storage_account_address")
                        .index(2)
                        .value_name("STORAGE ACCOUNT PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Storage account address to redeem credits for"),
                )
                .arg(
                    Arg::with_name("slot")
                        .index(3)
                        .value_name("SLOT")
                        .takes_value(true)
                        .required(true)
                        .help("The slot to claim rewards for"),
                ),)

        .subcommand(
            SubCommand::with_name("show-storage-account")
                .about("Show the contents of a storage account")
                .arg(
                    Arg::with_name("storage_account_address")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("Storage account address"),
                )
        )
        .subcommand(
            SubCommand::with_name("deploy")
                .about("Deploy a program")
                .arg(
                    Arg::with_name("program_location")
                        .index(1)
                        .value_name("PATH")
                        .takes_value(true)
                        .required(true)
                        .help("/path/to/program.o"),
                ), // TODO: Add "loader" argument; current default is bvm_loader
        )
        .subcommand(
            SubCommand::with_name("get-transaction-count")
                .about("Get current transaction count"),
        )
        .subcommand(
            SubCommand::with_name("pay")
                .about("Send a payment")
                .arg(
                    Arg::with_name("to")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("The address of recipient"),
                )
                .arg(
                    Arg::with_name("difs")
                        .index(2)
                        .value_name("NUM")
                        .takes_value(true)
                        .required(true)
                        .help("The number of difs to send"),
                )
                .arg(
                    Arg::with_name("timestamp")
                        .long("after")
                        .value_name("DATETIME")
                        .takes_value(true)
                        .help("A timestamp after which transaction will execute"),
                )
                .arg(
                    Arg::with_name("timestamp_address")
                        .long("require-timestamp-from")
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .requires("timestamp")
                        .validator(is_address)
                        .help("Require timestamp from this third party"),
                )
                .arg(
                    Arg::with_name("witness")
                        .long("require-signature-from")
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .validator(is_address)
                        .help("Any third party signatures required to unlock the difs"),
                )
                .arg(
                    Arg::with_name("cancelable")
                        .long("cancelable")
                        .takes_value(false),
                ),
        )
        .subcommand(
            SubCommand::with_name("send-signature")
                .about("Send a signature to authorize a transfer")
                .arg(
                    Arg::with_name("to")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("The address of recipient"),
                )
                .arg(
                    Arg::with_name("process_id")
                        .index(2)
                        .value_name("PROCESS_ID")
                        .takes_value(true)
                        .required(true)
                        .help("The process id of the transfer to authorize"),
                ),
        )
        .subcommand(
            SubCommand::with_name("send-timestamp")
                .about("Send a timestamp to unlock a transfer")
                .arg(
                    Arg::with_name("to")
                        .index(1)
                        .value_name("PUBKEY")
                        .takes_value(true)
                        .required(true)
                        .validator(is_address)
                        .help("The address of recipient"),
                )
                .arg(
                    Arg::with_name("process_id")
                        .index(2)
                        .value_name("PROCESS_ID")
                        .takes_value(true)
                        .required(true)
                        .help("The process id of the transfer to unlock"),
                )
                .arg(
                    Arg::with_name("datetime")
                        .long("date")
                        .value_name("DATETIME")
                        .takes_value(true)
                        .help("Optional arbitrary timestamp to apply"),
                ),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use morgan_client::rpc_client_request::SIGNATURE;
    use morgan_interface::signature::gen_keypair_file;
    use morgan_interface::transaction::TransactionError;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::path::PathBuf;

    #[test]
    fn test_wallet_config_drone_addr() {
        let mut config = WalletConfig::default();
        config.json_rpc_url = "http://127.0.0.1:10099".to_string();
        let rpc_host = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        assert_eq!(
            config.drone_addr(),
            SocketAddr::new(rpc_host, config.drone_port)
        );

        config.drone_port = 1234;
        assert_eq!(config.drone_addr(), SocketAddr::new(rpc_host, 1234));

        config.drone_host = Some(rpc_host);
        assert_eq!(
            config.drone_addr(),
            SocketAddr::new(config.drone_host.unwrap(), 1234)
        );
    }

    #[test]
    fn test_wallet_parse_command() {
        let test_commands = app("test", "desc", "version");

        let address = BvmAddr::new_rand();
        let address_string = format!("{}", address);
        let witness0 = BvmAddr::new_rand();
        let witness0_string = format!("{}", witness0);
        let witness1 = BvmAddr::new_rand();
        let witness1_string = format!("{}", witness1);
        let dt = Utc.ymd(2018, 9, 19).and_hms(17, 30, 59);

        // Test Airdrop Subcommand
        let test_airdrop = test_commands
            .clone()
            .get_matches_from(vec!["test", "airdrop", "50"]);
        assert_eq!(
            parse_command(&address, &test_airdrop).unwrap(),
            WalletCommand::Airdrop(50)
        );
        let test_bad_airdrop = test_commands
            .clone()
            .get_matches_from(vec!["test", "airdrop", "notint"]);
        assert!(parse_command(&address, &test_bad_airdrop).is_err());

        // Test Cancel Subcommand
        let test_cancel =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "cancel", &address_string]);
        assert_eq!(
            parse_command(&address, &test_cancel).unwrap(),
            WalletCommand::Cancel(address)
        );

        // Test Confirm Subcommand
        let signature = Signature::new(&vec![1; 64]);
        let signature_string = format!("{:?}", signature);
        let test_confirm =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "confirm", &signature_string]);
        assert_eq!(
            parse_command(&address, &test_confirm).unwrap(),
            WalletCommand::Confirm(signature)
        );
        let test_bad_signature = test_commands
            .clone()
            .get_matches_from(vec!["test", "confirm", "deadbeef"]);
        assert!(parse_command(&address, &test_bad_signature).is_err());

        // Test AuthorizeVoter Subcommand
        let keypair_file = make_tmp_path("keypair_file");
        gen_keypair_file(&keypair_file).unwrap();
        let keypair = read_keypair(&keypair_file).unwrap();

        let test_authorize_voter = test_commands.clone().get_matches_from(vec![
            "test",
            "authorize-voter",
            &address_string,
            &keypair_file,
            &address_string,
        ]);
        assert_eq!(
            parse_command(&address, &test_authorize_voter).unwrap(),
            WalletCommand::AuthorizeVoter(address, keypair, address)
        );

        // Test CreateVoteAccount SubCommand
        let node_address = BvmAddr::new_rand();
        let node_address_string = format!("{}", node_address);
        let test_create_vote_account = test_commands.clone().get_matches_from(vec![
            "test",
            "create-vote-account",
            &address_string,
            &node_address_string,
            "50",
            "--commission",
            "10",
        ]);
        assert_eq!(
            parse_command(&address, &test_create_vote_account).unwrap(),
            WalletCommand::CreateVoteAccount(address, node_address, 10, 50)
        );
        let test_create_vote_account2 = test_commands.clone().get_matches_from(vec![
            "test",
            "create-vote-account",
            &address_string,
            &node_address_string,
            "50",
        ]);
        assert_eq!(
            parse_command(&address, &test_create_vote_account2).unwrap(),
            WalletCommand::CreateVoteAccount(address, node_address, 0, 50)
        );

        // Test Create Stake Account
        let test_create_stake_account = test_commands.clone().get_matches_from(vec![
            "test",
            "create-stake-account",
            &address_string,
            "50",
        ]);
        assert_eq!(
            parse_command(&address, &test_create_stake_account).unwrap(),
            WalletCommand::CreateStakeAccount(address, 50)
        );

        fn make_tmp_path(name: &str) -> String {
            let out_dir = std::env::var("OUT_DIR").unwrap_or_else(|_| "target".to_string());
            let keypair = Keypair::new();

            let path = format!("{}/tmp/{}-{}", out_dir, name, keypair.address());

            // whack any possible collision
            let _ignored = std::fs::remove_dir_all(&path);
            // whack any possible collision
            let _ignored = std::fs::remove_file(&path);

            path
        }

        let keypair_file = make_tmp_path("keypair_file");
        gen_keypair_file(&keypair_file).unwrap();
        let keypair = read_keypair(&keypair_file).unwrap();
        // Test Delegate Stake Subcommand
        let test_delegate_stake = test_commands.clone().get_matches_from(vec![
            "test",
            "delegate-stake",
            &keypair_file,
            &address_string,
        ]);
        assert_eq!(
            parse_command(&address, &test_delegate_stake).unwrap(),
            WalletCommand::DelegateStake(keypair, address)
        );

        // Test Deploy Subcommand
        let test_deploy =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "deploy", "/Users/test/program.o"]);
        assert_eq!(
            parse_command(&address, &test_deploy).unwrap(),
            WalletCommand::Deploy("/Users/test/program.o".to_string())
        );

        // Test Simple Pay Subcommand
        let test_pay =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "pay", &address_string, "50"]);
        assert_eq!(
            parse_command(&address, &test_pay).unwrap(),
            WalletCommand::Pay(50, address, None, None, None, None)
        );

        // Test Pay Subcommand w/ Endorsement
        let test_pay_multiple_witnesses = test_commands.clone().get_matches_from(vec![
            "test",
            "pay",
            &address_string,
            "50",
            "--require-signature-from",
            &witness0_string,
            "--require-signature-from",
            &witness1_string,
        ]);
        assert_eq!(
            parse_command(&address, &test_pay_multiple_witnesses).unwrap(),
            WalletCommand::Pay(50, address, None, None, Some(vec![witness0, witness1]), None)
        );
        let test_pay_single_witness = test_commands.clone().get_matches_from(vec![
            "test",
            "pay",
            &address_string,
            "50",
            "--require-signature-from",
            &witness0_string,
        ]);
        assert_eq!(
            parse_command(&address, &test_pay_single_witness).unwrap(),
            WalletCommand::Pay(50, address, None, None, Some(vec![witness0]), None)
        );

        // Test Pay Subcommand w/ Timestamp
        let test_pay_timestamp = test_commands.clone().get_matches_from(vec![
            "test",
            "pay",
            &address_string,
            "50",
            "--after",
            "2018-09-19T17:30:59",
            "--require-timestamp-from",
            &witness0_string,
        ]);
        assert_eq!(
            parse_command(&address, &test_pay_timestamp).unwrap(),
            WalletCommand::Pay(50, address, Some(dt), Some(witness0), None, None)
        );

        // Test Send-Signature Subcommand
        let test_send_signature = test_commands.clone().get_matches_from(vec![
            "test",
            "send-signature",
            &address_string,
            &address_string,
        ]);
        assert_eq!(
            parse_command(&address, &test_send_signature).unwrap(),
            WalletCommand::Endorsement(address, address)
        );
        let test_pay_multiple_witnesses = test_commands.clone().get_matches_from(vec![
            "test",
            "pay",
            &address_string,
            "50",
            "--after",
            "2018-09-19T17:30:59",
            "--require-signature-from",
            &witness0_string,
            "--require-timestamp-from",
            &witness0_string,
            "--require-signature-from",
            &witness1_string,
        ]);
        assert_eq!(
            parse_command(&address, &test_pay_multiple_witnesses).unwrap(),
            WalletCommand::Pay(
                50,
                address,
                Some(dt),
                Some(witness0),
                Some(vec![witness0, witness1]),
                None
            )
        );

        // Test Send-Timestamp Subcommand
        let test_send_timestamp = test_commands.clone().get_matches_from(vec![
            "test",
            "send-timestamp",
            &address_string,
            &address_string,
            "--date",
            "2018-09-19T17:30:59",
        ]);
        assert_eq!(
            parse_command(&address, &test_send_timestamp).unwrap(),
            WalletCommand::TimeElapsed(address, address, dt)
        );
        let test_bad_timestamp = test_commands.clone().get_matches_from(vec![
            "test",
            "send-timestamp",
            &address_string,
            &address_string,
            "--date",
            "20180919T17:30:59",
        ]);
        assert!(parse_command(&address, &test_bad_timestamp).is_err());
    }

    #[test]
    fn test_wallet_process_command() {
        // Success cases
        let mut config = WalletConfig::default();
        config.rpc_client = Some(RpcClient::new_mock("succeeds".to_string()));

        let keypair = Keypair::new();
        let address = keypair.address().to_string();
        config.keypair = keypair;
        config.command = WalletCommand::Address;
        assert_eq!(process_command(&config).unwrap(), address);

        config.command = WalletCommand::Balance(config.keypair.address());
        assert_eq!(process_command(&config).unwrap(), "50 difs");

        let process_id = BvmAddr::new_rand();
        config.command = WalletCommand::Cancel(process_id);
        assert_eq!(process_command(&config).unwrap(), SIGNATURE);

        let good_signature = Signature::new(&bs58::decode(SIGNATURE).into_vec().unwrap());
        config.command = WalletCommand::Confirm(good_signature);
        assert_eq!(process_command(&config).unwrap(), "Confirmed");

        let bob_address = BvmAddr::new_rand();
        let node_address = BvmAddr::new_rand();
        config.command = WalletCommand::CreateVoteAccount(bob_address, node_address, 0, 10);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        let bob_keypair = Keypair::new();
        config.command = WalletCommand::AuthorizeVoter(bob_address, bob_keypair, bob_address);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        config.command = WalletCommand::CreateStakeAccount(bob_address, 10);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        let bob_keypair = Keypair::new();
        let node_address = BvmAddr::new_rand();
        config.command = WalletCommand::DelegateStake(bob_keypair.into(), node_address);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        config.command = WalletCommand::GetTransactionCount;
        assert_eq!(process_command(&config).unwrap(), "1234");

        config.command = WalletCommand::Pay(10, bob_address, None, None, None, None);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        let date_string = "\"2018-09-19T17:30:59Z\"";
        let dt: DateTime<Utc> = serde_json::from_str(&date_string).unwrap();
        config.command = WalletCommand::Pay(
            10,
            bob_address,
            Some(dt),
            Some(config.keypair.address()),
            None,
            None,
        );
        let result = process_command(&config);
        let json: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(
            json.as_object()
                .unwrap()
                .get("signature")
                .unwrap()
                .as_str()
                .unwrap(),
            SIGNATURE.to_string()
        );

        let witness = BvmAddr::new_rand();
        config.command = WalletCommand::Pay(
            10,
            bob_address,
            None,
            None,
            Some(vec![witness]),
            Some(config.keypair.address()),
        );
        let result = process_command(&config);
        let json: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(
            json.as_object()
                .unwrap()
                .get("signature")
                .unwrap()
                .as_str()
                .unwrap(),
            SIGNATURE.to_string()
        );

        let process_id = BvmAddr::new_rand();
        config.command = WalletCommand::TimeElapsed(bob_address, process_id, dt);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        let witness = BvmAddr::new_rand();
        config.command = WalletCommand::Endorsement(bob_address, witness);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        // Need airdrop cases
        config.command = WalletCommand::Airdrop(50);
        assert!(process_command(&config).is_err());

        config.rpc_client = Some(RpcClient::new_mock("airdrop".to_string()));
        config.command = WalletCommand::TimeElapsed(bob_address, process_id, dt);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        let witness = BvmAddr::new_rand();
        config.command = WalletCommand::Endorsement(bob_address, witness);
        let signature = process_command(&config);
        assert_eq!(signature.unwrap(), SIGNATURE.to_string());

        // sig_not_found case
        config.rpc_client = Some(RpcClient::new_mock("sig_not_found".to_string()));
        let missing_signature = Signature::new(&bs58::decode("5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW").into_vec().unwrap());
        config.command = WalletCommand::Confirm(missing_signature);
        assert_eq!(process_command(&config).unwrap(), "Not found");

        // Tx error case
        config.rpc_client = Some(RpcClient::new_mock("account_in_use".to_string()));
        let any_signature = Signature::new(&bs58::decode(SIGNATURE).into_vec().unwrap());
        config.command = WalletCommand::Confirm(any_signature);
        assert_eq!(
            process_command(&config).unwrap(),
            format!(
                "Transaction failed with error {:?}",
                TransactionError::AccountInUse
            )
        );

        // Failure cases
        config.rpc_client = Some(RpcClient::new_mock("fails".to_string()));

        config.command = WalletCommand::Airdrop(50);
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::Balance(config.keypair.address());
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::CreateVoteAccount(bob_address, node_address, 0, 10);
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::AuthorizeVoter(bob_address, Keypair::new(), bob_address);
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::GetTransactionCount;
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::Pay(10, bob_address, None, None, None, None);
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::Pay(
            10,
            bob_address,
            Some(dt),
            Some(config.keypair.address()),
            None,
            None,
        );
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::Pay(
            10,
            bob_address,
            None,
            None,
            Some(vec![witness]),
            Some(config.keypair.address()),
        );
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::TimeElapsed(bob_address, process_id, dt);
        assert!(process_command(&config).is_err());
    }

    #[test]
    fn test_wallet_deploy() {
        morgan_logger::setup();
        let mut pathbuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pathbuf.push("tests");
        pathbuf.push("fixtures");
        pathbuf.push("noop");
        pathbuf.set_extension("so");

        // Success case
        let mut config = WalletConfig::default();
        config.rpc_client = Some(RpcClient::new_mock("succeeds".to_string()));

        config.command = WalletCommand::Deploy(pathbuf.to_str().unwrap().to_string());
        let result = process_command(&config);
        let json: Value = serde_json::from_str(&result.unwrap()).unwrap();
        let program_id = json
            .as_object()
            .unwrap()
            .get("programId")
            .unwrap()
            .as_str()
            .unwrap();

        assert!(program_id.parse::<BvmAddr>().is_ok());

        // Failure cases
        config.rpc_client = Some(RpcClient::new_mock("airdrop".to_string()));
        assert!(process_command(&config).is_err());

        config.command = WalletCommand::Deploy("bad/file/location.so".to_string());
        assert!(process_command(&config).is_err());
    }
}
