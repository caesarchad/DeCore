//! The `genesis_block` module is a library for generating the chain's genesis block.

use crate::account::Account;
use crate::gas_cost::GasCost;
use crate::hash::{hash, Hash};
use crate::waterclock_config::WaterClockConfig;
use crate::pubkey::Pubkey;
use crate::signature::{Keypair, KeypairUtil};
use crate::sys_controller;
use crate::constants::{DEFAULT_SLOTS_PER_EPOCH, DEFAULT_DROPS_PER_SLOT};
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Serialize, Deserialize, Debug)]
pub struct GenesisBlock {
    pub accounts: Vec<(Pubkey, Account)>,
    pub bootstrap_leader_pubkey: Pubkey,
    pub epoch_warmup: bool,
    pub fee_calculator: GasCost,
    pub builtin_opcode_handlers: Vec<(String, Pubkey)>,
    pub candidate_each_round: u64,
    pub stake_place_holder: u64,
    pub drops_per_slot: u64,
    pub waterclock_config: WaterClockConfig,
}

// useful for basic tests
pub fn create_genesis_block(difs: u64) -> (GenesisBlock, Keypair) {
    let mint_keypair = Keypair::new();
    (
        GenesisBlock::new(
            &Pubkey::default(),
            &[(
                mint_keypair.pubkey(),
                Account::new(difs, 0, 0, &sys_controller::id()),
            )],
            &[],
        ),
        mint_keypair,
    )
}

impl GenesisBlock {
    pub fn new(
        bootstrap_leader_pubkey: &Pubkey,
        accounts: &[(Pubkey, Account)],
        builtin_opcode_handlers: &[(String, Pubkey)],
    ) -> Self {
        Self {
            accounts: accounts.to_vec(),
            bootstrap_leader_pubkey: *bootstrap_leader_pubkey, // TODO: leader_schedule to derive from actual stakes, instead ;)
            epoch_warmup: true,
            fee_calculator: GasCost::default(),
            builtin_opcode_handlers: builtin_opcode_handlers.to_vec(),
            candidate_each_round: DEFAULT_SLOTS_PER_EPOCH,
            stake_place_holder: DEFAULT_SLOTS_PER_EPOCH,
            drops_per_slot: DEFAULT_DROPS_PER_SLOT,
            waterclock_config: WaterClockConfig::default(),
        }
    }

    pub fn hash(&self) -> Hash {
        let serialized = serde_json::to_string(self).unwrap();
        hash(&serialized.into_bytes())
    }

    pub fn load(ledger_path: &str) -> Result<Self, std::io::Error> {
        let file = File::open(&Path::new(ledger_path).join("genesis.json"))?;
        let genesis_block = serde_json::from_reader(file)?;
        Ok(genesis_block)
    }

    pub fn write(&self, ledger_path: &str) -> Result<(), std::io::Error> {
        let serialized = serde_json::to_string(self)?;

        let dir = Path::new(ledger_path);
        std::fs::create_dir_all(&dir)?;

        let mut file = File::create(&dir.join("genesis.json"))?;
        file.write_all(&serialized.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signature::{Keypair, KeypairUtil};

    fn make_tmp_path(name: &str) -> String {
        let out_dir = std::env::var("OUT_DIR").unwrap_or_else(|_| "target".to_string());
        let keypair = Keypair::new();

        let path = format!("{}/tmp/{}-{}", out_dir, name, keypair.pubkey());

        // whack any possible collision
        let _ignored = std::fs::remove_dir_all(&path);
        // whack any possible collision
        let _ignored = std::fs::remove_file(&path);

        path
    }

    #[test]
    fn test_genesis_block() {
        let mint_keypair = Keypair::new();
        let block = GenesisBlock::new(
            &Pubkey::default(),
            &[
                (
                    mint_keypair.pubkey(),
                    Account::new(10_000, 0, 0, &Pubkey::default()),
                ),
                (Pubkey::new_rand(), Account::new(1, 0, 0, &Pubkey::default())),
            ],
            &[("hi".to_string(), Pubkey::new_rand())],
        );
        assert_eq!(block.accounts.len(), 2);
        assert!(block.accounts.iter().any(
            |(pubkey, account)| *pubkey == mint_keypair.pubkey() && account.difs == 10_000
        ));

        let path = &make_tmp_path("genesis_block");
        block.write(&path).expect("write");
        let loaded_block = GenesisBlock::load(&path).expect("load");
        assert_eq!(block.hash(), loaded_block.hash());
        let _ignored = std::fs::remove_file(&path);
    }

}
