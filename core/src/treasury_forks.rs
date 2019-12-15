//! The `treasury_forks` module implments BankForks a DAG of checkpointed Banks

use hashbrown::{HashMap, HashSet};
use morgan_metricbot::inc_new_counter_info;
use morgan_runtime::treasury::Bank;
use morgan_interface::timing;
use std::ops::Index;
use std::sync::Arc;
use std::time::Instant;
use std::{borrow::Cow, convert, ffi::OsStr, path::Path, str};

pub struct BankForks {
    treasuries: HashMap<u64, Arc<Bank>>,
    working_treasury: Arc<Bank>,
    root: u64,
}

impl Index<u64> for BankForks {
    type Output = Arc<Bank>;
    fn index(&self, treasury_slot: u64) -> &Arc<Bank> {
        &self.treasuries[&treasury_slot]
    }
}

impl BankForks {
    pub fn new(treasury_slot: u64, treasury: Bank) -> Self {
        let mut treasuries = HashMap::new();
        let working_treasury = Arc::new(treasury);
        treasuries.insert(treasury_slot, working_treasury.clone());
        Self {
            treasuries,
            working_treasury,
            root: 0,
        }
    }

    /// Create a map of treasury slot id to the set of ancestors for the treasury slot.
    pub fn ancestors(&self) -> HashMap<u64, HashSet<u64>> {
        let mut ancestors = HashMap::new();
        for treasury in self.treasuries.values() {
            let mut set: HashSet<u64> = treasury.ancestors.keys().cloned().collect();
            set.remove(&treasury.slot());
            ancestors.insert(treasury.slot(), set);
        }
        ancestors
    }

    /// Create a map of treasury slot id to the set of all of its descendants
    pub fn descendants(&self) -> HashMap<u64, HashSet<u64>> {
        let mut descendants = HashMap::new();
        for treasury in self.treasuries.values() {
            let _ = descendants.entry(treasury.slot()).or_insert(HashSet::new());
            let mut set: HashSet<u64> = treasury.ancestors.keys().cloned().collect();
            set.remove(&treasury.slot());
            for parent in set {
                descendants
                    .entry(parent)
                    .or_insert(HashSet::new())
                    .insert(treasury.slot());
            }
        }
        descendants
    }

    pub fn frozen_treasuries(&self) -> HashMap<u64, Arc<Bank>> {
        self.treasuries
            .iter()
            .filter(|(_, b)| b.is_frozen())
            .map(|(k, b)| (*k, b.clone()))
            .collect()
    }

    pub fn active_banks(&self) -> Vec<u64> {
        self.treasuries
            .iter()
            .filter(|(_, v)| !v.is_frozen())
            .map(|(k, _v)| *k)
            .collect()
    }

    pub fn get(&self, treasury_slot: u64) -> Option<&Arc<Bank>> {
        self.treasuries.get(&treasury_slot)
    }

    pub fn new_from_banks(initial_banks: &[Arc<Bank>], root: u64) -> Self {
        let mut treasuries = HashMap::new();
        let working_treasury = initial_banks[0].clone();
        for treasury in initial_banks {
            treasuries.insert(treasury.slot(), treasury.clone());
        }
        Self {
            root,
            treasuries,
            working_treasury,
        }
    }

    pub fn insert(&mut self, treasury: Bank) {
        let treasury = Arc::new(treasury);
        let prev = self.treasuries.insert(treasury.slot(), treasury.clone());
        assert!(prev.is_none());

        self.working_treasury = treasury.clone();
    }

    // TODO: really want to kill this...
    pub fn working_treasury(&self) -> Arc<Bank> {
        self.working_treasury.clone()
    }

    pub fn set_genesis(&mut self, root: u64) {
        self.root = root;
        let set_root_start = Instant::now();
        let root_bank = self
            .treasuries
            .get(&root)
            .expect("root treasury didn't exist in treasury_forks");
        let root_tx_count = root_bank
            .parents()
            .last()
            .map(|treasury| treasury.transaction_count())
            .unwrap_or(0);
        root_bank.squash();
        let new_tx_count = root_bank.transaction_count();
        self.prune_non_root(root);

        inc_new_counter_info!(
            "treasury-forks_set_root_ms",
            timing::duration_as_ms(&set_root_start.elapsed()) as usize
        );
        inc_new_counter_info!(
            "treasury-forks_set_root_tx_count",
            (new_tx_count - root_tx_count) as usize
        );
    }

    pub fn root(&self) -> u64 {
        self.root
    }

    fn prune_non_root(&mut self, root: u64) {
        let descendants = self.descendants();
        self.treasuries
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
        let treasury = Bank::new(&genesis_block);
        let mut treasury_forks = BankForks::new(0, treasury);
        let child_treasury = Bank::new_from_parent(&treasury_forks[0u64], &Pubkey::default(), 1);
        child_treasury.register_tick(&Hash::default());
        treasury_forks.insert(child_treasury);
        assert_eq!(treasury_forks[1u64].tick_height(), 1);
        assert_eq!(treasury_forks.working_treasury().tick_height(), 1);
    }

    #[test]
    fn test_bank_forks_descendants() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(10_000);
        let treasury = Bank::new(&genesis_block);
        let mut treasury_forks = BankForks::new(0, treasury);
        let treasury0 = treasury_forks[0].clone();
        let treasury = Bank::new_from_parent(&treasury0, &Pubkey::default(), 1);
        treasury_forks.insert(treasury);
        let treasury = Bank::new_from_parent(&treasury0, &Pubkey::default(), 2);
        treasury_forks.insert(treasury);
        let descendants = treasury_forks.descendants();
        let children: Vec<u64> = descendants[&0].iter().cloned().collect();
        assert_eq!(children, vec![1, 2]);
        assert!(descendants[&1].is_empty());
        assert!(descendants[&2].is_empty());
    }

    #[test]
    fn test_bank_forks_ancestors() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(10_000);
        let treasury = Bank::new(&genesis_block);
        let mut treasury_forks = BankForks::new(0, treasury);
        let treasury0 = treasury_forks[0].clone();
        let treasury = Bank::new_from_parent(&treasury0, &Pubkey::default(), 1);
        treasury_forks.insert(treasury);
        let treasury = Bank::new_from_parent(&treasury0, &Pubkey::default(), 2);
        treasury_forks.insert(treasury);
        let ancestors = treasury_forks.ancestors();
        assert!(ancestors[&0].is_empty());
        let parents: Vec<u64> = ancestors[&1].iter().cloned().collect();
        assert_eq!(parents, vec![0]);
        let parents: Vec<u64> = ancestors[&2].iter().cloned().collect();
        assert_eq!(parents, vec![0]);
    }

    #[test]
    fn test_bank_forks_frozen_banks() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(10_000);
        let treasury = Bank::new(&genesis_block);
        let mut treasury_forks = BankForks::new(0, treasury);
        let child_treasury = Bank::new_from_parent(&treasury_forks[0u64], &Pubkey::default(), 1);
        treasury_forks.insert(child_treasury);
        assert!(treasury_forks.frozen_treasuries().get(&0).is_some());
        assert!(treasury_forks.frozen_treasuries().get(&1).is_none());
    }

    #[test]
    fn test_bank_forks_active_banks() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(10_000);
        let treasury = Bank::new(&genesis_block);
        let mut treasury_forks = BankForks::new(0, treasury);
        let child_treasury = Bank::new_from_parent(&treasury_forks[0u64], &Pubkey::default(), 1);
        treasury_forks.insert(child_treasury);
        assert_eq!(treasury_forks.active_banks(), vec![1]);
    }

}
