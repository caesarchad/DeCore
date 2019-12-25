//! A command-line executable for generating the chain's genesis block.
#[macro_use]
extern crate morgan_vote_controller;
#[macro_use]
extern crate morgan_stake_controller;
#[macro_use]
extern crate morgan_budget_controller;
#[macro_use]
extern crate morgan_token_controller;
#[macro_use]
extern crate morgan_config_controller;

use clap::{crate_description, crate_name, crate_version, value_t_or_exit, App, Arg};
use morgan::block_buffer_pool::make_new_ledger_file;
use morgan_interface::account::Account;
use morgan_interface::gas_cost::GasCost;
use morgan_interface::genesis_block::GenesisBlock;
use morgan_interface::hash::{hash, Hash};
use morgan_interface::waterclock_config::WaterClockConfig;
use morgan_interface::signature::{read_keypair, KeypairUtil};
use morgan_interface::sys_controller;
use morgan_interface::timing;
use morgan_interface::constants;
use morgan_stake_api::stake_state;
use morgan_storage_controller::genesis_block_util::GenesisBlockUtil;
use morgan_vote_api::vote_state;
use std::error;
use std::time::{Duration, Instant};

pub const BOOTSTRAP_LEADER_DIFS: u64 = 42;

fn main() -> Result<(), Box<dyn error::Error>> {
    let default_bootstrap_leader_difs = &BOOTSTRAP_LEADER_DIFS.to_string();
    let default_difs_per_signature =
        &GasCost::default().difs_per_signature.to_string();
    let default_target_drop_duration =
        &timing::duration_as_ms(&WaterClockConfig::default().target_drop_duration).to_string();
    let default_drops_per_slot = &constants::DEFAULT_DROPS_PER_SLOT.to_string();
    let default_slots_per_epoch = &constants::DEFAULT_SLOTS_PER_EPOCH.to_string();

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(crate_version!())
        .arg(
            Arg::with_name("bootstrap_leader_keypair_file")
                .short("b")
                .long("bootstrap-leader-keypair")
                .value_name("BOOTSTRAP LEADER KEYPAIR")
                .takes_value(true)
                .required(true)
                .help("Path to file containing the bootstrap leader's keypair"),
        )
        .arg(
            Arg::with_name("ledger_path")
                .short("l")
                .long("ledger")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                .help("Use directory as persistent ledger location"),
        )
        .arg(
            Arg::with_name("difs")
                .short("t")
                .long("difs")
                .value_name("DIFS")
                .takes_value(true)
                .required(true)
                .help("Number of difs to create in the mint"),
        )
        .arg(
            Arg::with_name("mint_keypair_file")
                .short("m")
                .long("mint")
                .value_name("MINT")
                .takes_value(true)
                .required(true)
                .help("Path to file containing keys of the mint"),
        )
        .arg(
            Arg::with_name("bootstrap_vote_keypair_file")
                .short("s")
                .long("bootstrap-vote-keypair")
                .value_name("BOOTSTRAP VOTE KEYPAIR")
                .takes_value(true)
                .required(true)
                .help("Path to file containing the bootstrap leader's voting keypair"),
        )
        .arg(
            Arg::with_name("bootstrap_stake_keypair_file")
                .short("k")
                .long("bootstrap-stake-keypair")
                .value_name("BOOTSTRAP STAKE KEYPAIR")
                .takes_value(true)
                .required(true)
                .help("Path to file containing the bootstrap leader's staking keypair"),
        )
        .arg(
            Arg::with_name("bootstrap_storage_keypair_file")
                .long("bootstrap-storage-keypair")
                .value_name("BOOTSTRAP STORAGE KEYPAIR")
                .takes_value(true)
                .required(true)
                .help("Path to file containing the bootstrap leader's storage keypair"),
        )
        .arg(
            Arg::with_name("bootstrap_leader_difs")
                .long("bootstrap-leader-difs")
                .value_name("DIFS")
                .takes_value(true)
                .default_value(default_bootstrap_leader_difs)
                .required(true)
                .help("Number of difs to assign to the bootstrap leader"),
        )
        .arg(
            Arg::with_name("difs_per_signature")
                .long("difs-per-signature")
                .value_name("DIFS")
                .takes_value(true)
                .default_value(default_difs_per_signature)
                .help("Number of difs the cluster will charge for signature verification"),
        )
        .arg(
            Arg::with_name("target_drop_duration")
                .long("target-_drop-duration")
                .value_name("MILLIS")
                .takes_value(true)
                .default_value(default_target_drop_duration)
                .help("The target _drop rate of the cluster in milliseconds"),
        )
        .arg(
            Arg::with_name("hashes_per_drop")
                .long("hashes-per-_drop")
                .value_name("NUM_HASHES|\"auto\"|\"sleep\"")
                .takes_value(true)
                .default_value("auto")
                .help(
                    "How many Water Clock hashes to roll before emitting the next _drop. \
                     If \"auto\", determine based on --target-_drop-duration \
                     and the hash rate of this computer. If \"sleep\", for development \
                     sleep for --target-_drop-duration instead of hashing",
                ),
        )
        .arg(
            Arg::with_name("drops_per_slot")
                .long("drops-per-slot")
                .value_name("DROPS")
                .takes_value(true)
                .default_value(default_drops_per_slot)
                .help("The number of drops in a slot"),
        )
        .arg(
            Arg::with_name("candidate_each_round")
                .long("slots-per-epoch")
                .value_name("SLOTS")
                .takes_value(true)
                .default_value(default_slots_per_epoch)
                .help("The number of slots in an epoch"),
        )
        .get_matches();

    let bootstrap_leader_keypair_file = matches.value_of("bootstrap_leader_keypair_file").unwrap();
    let bootstrap_vote_keypair_file = matches.value_of("bootstrap_vote_keypair_file").unwrap();
    let bootstrap_stake_keypair_file = matches.value_of("bootstrap_stake_keypair_file").unwrap();
    let bootstrap_storage_keypair_file =
        matches.value_of("bootstrap_storage_keypair_file").unwrap();
    let mint_keypair_file = matches.value_of("mint_keypair_file").unwrap();
    let ledger_path = matches.value_of("ledger_path").unwrap();
    let difs = value_t_or_exit!(matches, "difs", u64);
    let bootstrap_leader_stake_difs =
        value_t_or_exit!(matches, "bootstrap_leader_difs", u64);

    let bootstrap_leader_keypair = read_keypair(bootstrap_leader_keypair_file)?;
    let bootstrap_vote_keypair = read_keypair(bootstrap_vote_keypair_file)?;
    let bootstrap_stake_keypair = read_keypair(bootstrap_stake_keypair_file)?;
    let bootstrap_storage_keypair = read_keypair(bootstrap_storage_keypair_file)?;
    let mint_keypair = read_keypair(mint_keypair_file)?;

    // TODO: de-duplicate the stake once passive staking
    //  is fully implemented
    //  https://github.com/morgan-labs/morgan/issues/4213
    let (vote_account, vote_state) = vote_state::create_bootstrap_leader_account(
        &bootstrap_vote_keypair.pubkey(),
        &bootstrap_leader_keypair.pubkey(),
        0,
        bootstrap_leader_stake_difs,
    );

    let mut genesis_block = GenesisBlock::new(
        &bootstrap_leader_keypair.pubkey(),
        &[
            // the mint
            (
                mint_keypair.pubkey(),
                Account::new(difs, 0, 0, &sys_controller::id()),
            ),
            // node needs an account to issue votes from
            (
                bootstrap_leader_keypair.pubkey(),
                Account::new(1, 0, 0, &sys_controller::id()),
            ),
            // where votes go to
            (bootstrap_vote_keypair.pubkey(), vote_account),
            // passive bootstrap leader stake, duplicates above temporarily
            (
                bootstrap_stake_keypair.pubkey(),
                stake_state::create_delegate_stake_account(
                    &bootstrap_vote_keypair.pubkey(),
                    &vote_state,
                    bootstrap_leader_stake_difs,
                ),
            ),
        ],
        &[
            morgan_vote_controller!(),
            morgan_stake_controller!(),
            morgan_budget_controller!(),
            morgan_token_controller!(),
            morgan_config_controller!(),
        ],
    );
    genesis_block.add_storage_controller(&bootstrap_storage_keypair.pubkey());

    genesis_block.fee_calculator.difs_per_signature =
        value_t_or_exit!(matches, "difs_per_signature", u64);
    genesis_block.drops_per_slot = value_t_or_exit!(matches, "drops_per_slot", u64);
    genesis_block.candidate_each_round = value_t_or_exit!(matches, "candidate_each_round", u64);
    genesis_block.waterclock_config.target_drop_duration =
        Duration::from_millis(value_t_or_exit!(matches, "target_drop_duration", u64));

    match matches.value_of("hashes_per_drop").unwrap() {
        "auto" => {
            let mut v = Hash::default();
            println!("Running 1 million hashes...");
            let start = Instant::now();
            for _ in 0..1_000_000 {
                v = hash(&v.as_ref());
            }
            let end = Instant::now();
            let elapsed = end.duration_since(start).as_millis();

            let hashes_per_drop = (genesis_block.waterclock_config.target_drop_duration.as_millis()
                * 1_000_000
                / elapsed) as u64;
            println!("Hashes per _drop: {}", hashes_per_drop);
            genesis_block.waterclock_config.hashes_per_drop = Some(hashes_per_drop);
        }
        "sleep" => {
            genesis_block.waterclock_config.hashes_per_drop = None;
        }
        _ => {
            genesis_block.waterclock_config.hashes_per_drop =
                Some(value_t_or_exit!(matches, "hashes_per_drop", u64));
        }
    }

    make_new_ledger_file(ledger_path, &genesis_block)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use hashbrown::HashSet;

    #[test]
    fn test_program_ids() {
        let ids = [
            (
                "11111111111111111111111111111111",
                morgan_interface::sys_controller::id(),
            ),
            (
                "NativeLoader1111111111111111111111111111111",
                morgan_interface::bultin_mounter::id(),
            ),
            (
                "BPFLoader1111111111111111111111111111111111",
                morgan_interface::bvm_controller::id(),
            ),
            (
                "Budget1111111111111111111111111111111111111",
                morgan_bvm_script::id(),
            ),
            (
                "Stake11111111111111111111111111111111111111",
                morgan_stake_api::id(),
            ),
            (
                "Storage111111111111111111111111111111111111",
                morgan_storage_api::id(),
            ),
            (
                "Token11111111111111111111111111111111111111",
                morgan_token_api::id(),
            ),
            (
                "Vote111111111111111111111111111111111111111",
                morgan_vote_api::id(),
            ),
            (
                "Stake11111111111111111111111111111111111111",
                morgan_stake_api::id(),
            ),
            (
                "Config1111111111111111111111111111111111111",
                morgan_config_api::id(),
            ),
        ];
        assert!(ids.iter().all(|(name, id)| *name == id.to_string()));
    }

    #[test]
    fn test_program_id_uniqueness() {
        let mut unique = HashSet::new();
        let ids = vec![
            morgan_interface::sys_controller::id(),
            morgan_interface::bultin_mounter::id(),
            morgan_interface::bvm_controller::id(),
            morgan_bvm_script::id(),
            morgan_storage_api::id(),
            morgan_token_api::id(),
            morgan_vote_api::id(),
            morgan_stake_api::id(),
            morgan_config_api::id(),
        ];
        assert!(ids.into_iter().all(move |id| unique.insert(id)));
    }
}
