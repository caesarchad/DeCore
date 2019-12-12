//! The `bank_forks` module implments BankForks a DAG of checkpointed Banks

use hashbrown::{HashMap, HashSet};
use morgan_metricbot::inc_new_counter_info;
use morgan_runtime::bank::Bank;
use morgan_interface::timing;
use std::ops::Index;
use std::sync::Arc;
use std::time::Instant;
use std::{borrow::Cow, convert, ffi::OsStr, path::Path, str};

pub struct BankForks {
    banks: HashMap<u64, Arc<Bank>>,
    working_bank: Arc<Bank>,
    root: u64,
}

impl Index<u64> for BankForks {
    type Output = Arc<Bank>;
    fn index(&self, bank_slot: u64) -> &Arc<Bank> {
        &self.banks[&bank_slot]
    }
}

impl BankForks {
    pub fn new(bank_slot: u64, bank: Bank) -> Self {
        let mut banks = HashMap::new();
        let working_bank = Arc::new(bank);
        banks.insert(bank_slot, working_bank.clone());
        Self {
            banks,
            working_bank,
            root: 0,
        }
    }

    /// Create a map of bank slot id to the set of ancestors for the bank slot.
    pub fn ancestors(&self) -> HashMap<u64, HashSet<u64>> {
        let mut ancestors = HashMap::new();
        for bank in self.banks.values() {
            let mut set: HashSet<u64> = bank.ancestors.keys().cloned().collect();
            set.remove(&bank.slot());
            ancestors.insert(bank.slot(), set);
        }
        ancestors
    }

    /// Create a map of bank slot id to the set of all of its descendants
    pub fn descendants(&self) -> HashMap<u64, HashSet<u64>> {
        let mut descendants = HashMap::new();
        for bank in self.banks.values() {
            let _ = descendants.entry(bank.slot()).or_insert(HashSet::new());
            let mut set: HashSet<u64> = bank.ancestors.keys().cloned().collect();
            set.remove(&bank.slot());
            for parent in set {
                descendants
                    .entry(parent)
                    .or_insert(HashSet::new())
                    .insert(bank.slot());
            }
        }
        descendants
    }

    pub fn frozen_banks(&self) -> HashMap<u64, Arc<Bank>> {
        self.banks
            .iter()
            .filter(|(_, b)| b.is_frozen())
            .map(|(k, b)| (*k, b.clone()))
            .collect()
    }

    pub fn active_banks(&self) -> Vec<u64> {
        self.banks
            .iter()
            .filter(|(_, v)| !v.is_frozen())
            .map(|(k, _v)| *k)
            .collect()
    }

    pub fn get(&self, bank_slot: u64) -> Option<&Arc<Bank>> {
        self.banks.get(&bank_slot)
    }

    pub fn new_from_banks(initial_banks: &[Arc<Bank>], root: u64) -> Self {
        let mut banks = HashMap::new();
        let working_bank = initial_banks[0].clone();
        for bank in initial_banks {
            banks.insert(bank.slot(), bank.clone());
        }
        Self {
            root,
            banks,
            working_bank,
        }
    }

    pub fn insert(&mut self, bank: Bank) {
        let bank = Arc::new(bank);
        let prev = self.banks.insert(bank.slot(), bank.clone());
        assert!(prev.is_none());

        self.working_bank = bank.clone();
    }

    // TODO: really want to kill this...
    pub fn working_bank(&self) -> Arc<Bank> {
        self.working_bank.clone()
    }

    pub fn config_base(&mut self, root: u64) {
        self.root = root;
        let set_root_start = Instant::now();
        let root_bank = self
            .banks
            .get(&root)
            .expect("root bank didn't exist in bank_forks");
        let root_tx_count = root_bank
            .parents()
            .last()
            .map(|bank| bank.transaction_count())
            .unwrap_or(0);
        root_bank.squash();
        let new_tx_count = root_bank.transaction_count();
        self.prune_non_root(root);

        inc_new_counter_info!(
            "bank-forks_set_root_ms",
            timing::duration_as_ms(&set_root_start.elapsed()) as usize
        );
        inc_new_counter_info!(
            "bank-forks_set_root_tx_count",
            (new_tx_count - root_tx_count) as usize
        );
    }

    pub fn root(&self) -> u64 {
        self.root
    }

    fn prune_non_root(&mut self, root: u64) {
        let descendants = self.descendants();
        self.banks
            .retain(|slot, _| descendants[&root].contains(slot))
    }

    pub fn has_newline_at_eof(file: &Path, contents: &str) -> Result<(), Cow<'static, str>> {
        if Self::skip_whitespace_checks(file) {
            return Ok(());
        }

        if !contents.ends_with('\n') {
            Err("missing a newline at EOF".into())
        } else {
            Ok(())
        }
    }

    pub fn has_trailing_whitespace(file: &Path,contents: &str,) -> Result<(), Cow<'static, str>> {
        if Self::skip_whitespace_checks(file) {
            return Ok(());
        }

        for (ln, line) in contents
            .lines()
            .enumerate()
            .map(|(ln, line)| (ln + 1, line))
        {
            if line.trim_end() != line {
                return Err(Cow::Owned(format!("trailing whitespace on line {}", ln)));
            }
        }

        if contents
            .lines()
            .rev()
            .take_while(|line| line.is_empty())
            .count()
            > 0
        {
            return Err("trailing whitespace at EOF".into());
        }

        Ok(())
    }

    pub fn skip_whitespace_checks(file: &Path) -> bool {
        match file
            .extension()
            .map(OsStr::to_str)
            .and_then(convert::identity)
        {
            Some("exp") => true,
            _ => false,
        }
    }
}




 



#[cfg(test)]
mod tests {
    use super::*;
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use morgan_interface::hash::Hash;
    use morgan_interface::pubkey::Pubkey;

    #[test]
    fn test_bank_forks() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(10_000);
        let bank = Bank::new(&genesis_block);
        let mut bank_forks = BankForks::new(0, bank);
        let child_bank = Bank::new_from_parent(&bank_forks[0u64], &Pubkey::default(), 1);
        child_bank.register_tick(&Hash::default());
        bank_forks.insert(child_bank);
        assert_eq!(bank_forks[1u64].tick_height(), 1);
        assert_eq!(bank_forks.working_bank().tick_height(), 1);
    }

    #[test]
    fn test_bank_forks_descendants() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(10_000);
        let bank = Bank::new(&genesis_block);
        let mut bank_forks = BankForks::new(0, bank);
        let bank0 = bank_forks[0].clone();
        let bank = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
        bank_forks.insert(bank);
        let bank = Bank::new_from_parent(&bank0, &Pubkey::default(), 2);
        bank_forks.insert(bank);
        let descendants = bank_forks.descendants();
        let children: Vec<u64> = descendants[&0].iter().cloned().collect();
        assert_eq!(children, vec![1, 2]);
        assert!(descendants[&1].is_empty());
        assert!(descendants[&2].is_empty());
    }

    #[test]
    fn test_bank_forks_ancestors() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(10_000);
        let bank = Bank::new(&genesis_block);
        let mut bank_forks = BankForks::new(0, bank);
        let bank0 = bank_forks[0].clone();
        let bank = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
        bank_forks.insert(bank);
        let bank = Bank::new_from_parent(&bank0, &Pubkey::default(), 2);
        bank_forks.insert(bank);
        let ancestors = bank_forks.ancestors();
        assert!(ancestors[&0].is_empty());
        let parents: Vec<u64> = ancestors[&1].iter().cloned().collect();
        assert_eq!(parents, vec![0]);
        let parents: Vec<u64> = ancestors[&2].iter().cloned().collect();
        assert_eq!(parents, vec![0]);
    }

    #[test]
    fn test_bank_forks_frozen_banks() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(10_000);
        let bank = Bank::new(&genesis_block);
        let mut bank_forks = BankForks::new(0, bank);
        let child_bank = Bank::new_from_parent(&bank_forks[0u64], &Pubkey::default(), 1);
        bank_forks.insert(child_bank);
        assert!(bank_forks.frozen_banks().get(&0).is_some());
        assert!(bank_forks.frozen_banks().get(&1).is_none());
    }

    #[test]
    fn test_bank_forks_active_banks() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(10_000);
        let bank = Bank::new(&genesis_block);
        let mut bank_forks = BankForks::new(0, bank);
        let child_bank = Bank::new_from_parent(&bank_forks[0u64], &Pubkey::default(), 1);
        bank_forks.insert(child_bank);
        assert_eq!(bank_forks.active_banks(), vec![1]);
    }

}
