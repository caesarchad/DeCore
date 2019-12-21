use hashbrown::HashMap;
use morgan_runtime::treasury::Treasury;
use morgan_interface::account::Account;
use morgan_interface::pubkey::Pubkey;
use morgan_vote_api::vote_state::VoteState;
use std::borrow::Borrow;
use lazy_static::lazy_static;
use serde_json::{self, value as json};
use std::{collections::VecDeque, sync::Mutex, time::SystemTime};

/// Looks through vote accounts, and finds the latest slot that has achieved
/// supermajority lockout
pub fn get_supermajority_slot(treasury: &Treasury, epoch_height: u64) -> Option<u64> {
    // Find the amount of stake needed for supermajority
    let stakes_and_lockouts = epoch_stakes_and_lockouts(treasury, epoch_height);
    let total_stake: u64 = stakes_and_lockouts.iter().map(|s| s.0).sum();
    let supermajority_stake = total_stake * 2 / 3;

    // Filter out the states that don't have a max lockout
    find_supermajority_slot(supermajority_stake, stakes_and_lockouts.iter())
}

pub fn vote_account_stakes(treasury: &Treasury) -> HashMap<Pubkey, u64> {
    treasury.vote_accounts()
        .into_iter()
        .map(|(id, (stake, _))| (id, stake))
        .collect()
}

/// Collect the staked nodes, as named by staked vote accounts from the given treasury
pub fn staked_nodes(treasury: &Treasury) -> HashMap<Pubkey, u64> {
    to_staked_nodes(to_vote_states(treasury.vote_accounts().into_iter()))
}

/// At the specified epoch, collect the node account balance and vote states for nodes that
/// have non-zero balance in their corresponding staking accounts
pub fn vote_account_stakes_at_epoch(
    treasury: &Treasury,
    epoch_height: u64,
) -> Option<HashMap<Pubkey, u64>> {
    treasury.epoch_vote_accounts(epoch_height).map(|accounts| {
        accounts
            .iter()
            .map(|(id, (stake, _))| (*id, *stake))
            .collect()
    })
}

/// At the specified epoch, collect the delegate account balance and vote states for delegates
/// that have non-zero balance in any of their managed staking accounts
pub fn staked_nodes_at_epoch(treasury: &Treasury, epoch_height: u64) -> Option<HashMap<Pubkey, u64>> {
    treasury.epoch_vote_accounts(epoch_height)
        .map(|vote_accounts| to_staked_nodes(to_vote_states(vote_accounts.into_iter())))
}

pub struct JsonLogEntry {
    pub name: &'static str,
    pub timestamp: u128,
    pub json: json::Value,
}

const MAX_EVENTS_IN_QUEUE: usize = 1_000;

/// Writes event to event stream
/// Example:
///   event!("committed", block="b");
// TODO: ideally we want to unify it with existing logger
#[macro_export]
macro_rules! event {
    ($name:expr, $($json:tt)*) => {
        $crate::json_log::send_json_log($crate::json_log::JsonLogEntry::new(
            $name,
            serde_json::json!({$($json)+}),
        ));
    };
}

lazy_static! {
    // This queue maintains last MAX_EVENTS_IN_QUEUE events
    // This is very efficiently implemented with circular buffer with fixed capacity
    static ref JSON_LOG_ENTRY_QUEUE: Mutex<VecDeque<JsonLogEntry>> = Mutex::new(
        VecDeque::with_capacity(MAX_EVENTS_IN_QUEUE)
    );
}

impl JsonLogEntry {
    pub fn new(name: &'static str, json: json::Value) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("now > UNIX_EPOCH")
            .as_millis();
        JsonLogEntry {
            name,
            timestamp,
            json,
        }
    }
}

/// Sends event to event stream.
///
/// Note that this method acquires global lock for brief moment.
/// This means that very hot threads can not use this method concurrently, otherwise they
/// will contend for same lock.
// TODO: if we use events more often we should rewrite it to be non-blocking
pub fn send_json_log(entry: JsonLogEntry) {
    let mut queue = JSON_LOG_ENTRY_QUEUE.lock().unwrap();
    if queue.len() >= MAX_EVENTS_IN_QUEUE {
        queue.pop_front();
    }
    queue.push_back(entry);
}

/// Get up to MAX_EVENTS_IN_QUEUE last events and clears the queue
pub fn pop_last_entries() -> Vec<JsonLogEntry> {
    let mut queue = JSON_LOG_ENTRY_QUEUE.lock().unwrap();
    queue.drain(..).collect()
}

// input (vote_pubkey, (stake, vote_account)) => (stake, vote_state)
fn to_vote_states(
    node_staked_accounts: impl Iterator<Item = (impl Borrow<Pubkey>, impl Borrow<(u64, Account)>)>,
) -> impl Iterator<Item = (u64, VoteState)> {
    node_staked_accounts.filter_map(|(_, stake_account)| {
        VoteState::deserialize(&stake_account.borrow().1.data)
            .ok()
            .map(|vote_state| (stake_account.borrow().0, vote_state))
    })
}

// (stake, vote_state) => (node, stake)
fn to_staked_nodes(
    node_staked_accounts: impl Iterator<Item = (u64, VoteState)>,
) -> HashMap<Pubkey, u64> {
    let mut map: HashMap<Pubkey, u64> = HashMap::new();
    node_staked_accounts.for_each(|(stake, state)| {
        map.entry(state.node_pubkey)
            .and_modify(|s| *s += stake)
            .or_insert(stake);
    });
    map
}

fn epoch_stakes_and_lockouts(treasury: &Treasury, epoch_height: u64) -> Vec<(u64, Option<u64>)> {
    let node_staked_accounts = treasury
        .epoch_vote_accounts(epoch_height)
        .expect("Treasury state for epoch is missing")
        .into_iter();

    to_vote_states(node_staked_accounts)
        .map(|(stake, states)| (stake, states.root_slot))
        .collect()
}

fn find_supermajority_slot<'a, I>(supermajority_stake: u64, stakes_and_lockouts: I) -> Option<u64>
where
    I: Iterator<Item = &'a (u64, Option<u64>)>,
{
    // Filter out the states that don't have a max lockout
    let mut stakes_and_lockouts: Vec<_> = stakes_and_lockouts
        .filter_map(|(stake, slot)| slot.map(|s| (stake, s)))
        .collect();

    // Sort by the root slot, in descending order
    stakes_and_lockouts.sort_unstable_by(|s1, s2| s1.1.cmp(&s2.1).reverse());

    // Find if any slot has achieved sufficient votes for supermajority lockout
    let mut total = 0;
    for (stake, slot) in stakes_and_lockouts {
        total += stake;
        if total > supermajority_stake {
            return Some(slot);
        }
    }

    None
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::genesis_utils::{
        create_genesis_block, create_genesis_block_with_leader, GenesisBlockInfo,
        BOOTSTRAP_LEADER_DIFS,
    };
    use hashbrown::HashSet;
    use morgan_interface::opcodes::OpCode;
    use morgan_interface::pubkey::Pubkey;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::transaction::Transaction;
    use morgan_stake_api::stake_opcode;
    use morgan_vote_api::vote_opcode;
    use std::iter::FromIterator;
    use std::sync::Arc;

    fn new_from_parent(parent: &Arc<Treasury>, slot: u64) -> Treasury {
        Treasury::new_from_parent(parent, &Pubkey::default(), slot)
    }

    #[test]
    fn test_vote_account_stakes_at_epoch() {
        let GenesisBlockInfo {
            genesis_block,
            voting_keypair,
            ..
        } = create_genesis_block_with_leader(1, &Pubkey::new_rand(), BOOTSTRAP_LEADER_DIFS);

        let treasury = Treasury::new(&genesis_block);

        // Epoch doesn't exist
        let mut expected = HashMap::new();
        assert_eq!(vote_account_stakes_at_epoch(&treasury, 10), None);

        // First epoch has the bootstrap leader
        expected.insert(voting_keypair.pubkey(), BOOTSTRAP_LEADER_DIFS);
        let expected = Some(expected);
        assert_eq!(vote_account_stakes_at_epoch(&treasury, 0), expected);

        // Second epoch carries same information
        let treasury = new_from_parent(&Arc::new(treasury), 1);
        assert_eq!(vote_account_stakes_at_epoch(&treasury, 0), expected);
        assert_eq!(vote_account_stakes_at_epoch(&treasury, 1), expected);
    }

    pub(crate) fn setup_vote_and_stake_accounts(
        treasury: &Treasury,
        from_account: &Keypair,
        vote_pubkey: &Pubkey,
        node_pubkey: &Pubkey,
        amount: u64,
    ) {
        fn process_instructions<T: KeypairUtil>(
            treasury: &Treasury,
            keypairs: &[&T],
            ixs: Vec<OpCode>,
        ) {
            treasury.process_transaction(&Transaction::new_s_opcodes(
                keypairs,
                ixs,
                treasury.last_transaction_seal(),
            ))
            .unwrap();
        }

        process_instructions(
            treasury,
            &[from_account],
            vote_opcode::create_account(
                &from_account.pubkey(),
                vote_pubkey,
                node_pubkey,
                0,
                amount,
            ),
        );

        let stake_account_keypair = Keypair::new();
        let stake_account_pubkey = stake_account_keypair.pubkey();

        process_instructions(
            treasury,
            &[from_account],
            stake_opcode::create_delegate_account(
                &from_account.pubkey(),
                &stake_account_pubkey,
                amount,
            ),
        );

        process_instructions(
            treasury,
            &[from_account, &stake_account_keypair],
            vec![stake_opcode::delegate_stake(
                &from_account.pubkey(),
                &stake_account_pubkey,
                vote_pubkey,
            )],
        );
    }

    #[test]
    fn test_epoch_stakes_and_lockouts() {
        let stake = 42;
        let validator = Keypair::new();

        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(10_000);

        let treasury = Treasury::new(&genesis_block);
        let vote_pubkey = Pubkey::new_rand();

        // Give the validator some stake but don't setup a staking account
        // Validator has no difs staked, so they get filtered out. Only the bootstrap leader
        // created by the genesis block will get included
        treasury.transfer(1, &mint_keypair, &validator.pubkey())
            .unwrap();

        // Make a mint vote account. Because the mint has nonzero stake, this
        // should show up in the active set
        setup_vote_and_stake_accounts(
            &treasury,
            &mint_keypair,
            &vote_pubkey,
            &mint_keypair.pubkey(),
            stake,
        );

        // soonest slot that could be a new epoch is 1
        let mut slot = 1;
        let mut epoch = treasury.get_stakers_epoch(0);
        // find the first slot in the next stakers_epoch
        while treasury.get_stakers_epoch(slot) == epoch {
            slot += 1;
        }

        epoch = treasury.get_stakers_epoch(slot);

        let treasury = new_from_parent(&Arc::new(treasury), slot);

        let result: Vec<_> = epoch_stakes_and_lockouts(&treasury, 0);
        assert_eq!(result, vec![(BOOTSTRAP_LEADER_DIFS, None)]);

        let result: HashSet<_> = HashSet::from_iter(epoch_stakes_and_lockouts(&treasury, epoch));
        let expected: HashSet<_> =
            HashSet::from_iter(vec![(BOOTSTRAP_LEADER_DIFS, None), (stake, None)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_find_supermajority_slot() {
        let supermajority = 10;

        let stakes_and_slots = vec![];
        assert_eq!(
            find_supermajority_slot(supermajority, stakes_and_slots.iter()),
            None
        );

        let stakes_and_slots = vec![(5, None), (5, None)];
        assert_eq!(
            find_supermajority_slot(supermajority, stakes_and_slots.iter()),
            None
        );

        let stakes_and_slots = vec![(5, None), (5, None), (9, Some(2))];
        assert_eq!(
            find_supermajority_slot(supermajority, stakes_and_slots.iter()),
            None
        );

        let stakes_and_slots = vec![(5, None), (5, None), (9, Some(2)), (1, Some(3))];
        assert_eq!(
            find_supermajority_slot(supermajority, stakes_and_slots.iter()),
            None
        );

        let stakes_and_slots = vec![(5, None), (5, None), (9, Some(2)), (2, Some(3))];
        assert_eq!(
            find_supermajority_slot(supermajority, stakes_and_slots.iter()),
            Some(2)
        );

        let stakes_and_slots = vec![(9, Some(2)), (2, Some(3)), (9, None)];
        assert_eq!(
            find_supermajority_slot(supermajority, stakes_and_slots.iter()),
            Some(2)
        );

        let stakes_and_slots = vec![(9, Some(2)), (2, Some(3)), (9, Some(3))];
        assert_eq!(
            find_supermajority_slot(supermajority, stakes_and_slots.iter()),
            Some(3)
        );
    }

    #[test]
    fn test_to_staked_nodes() {
        let mut stakes = Vec::new();
        let node1 = Pubkey::new_rand();
        let node2 = Pubkey::new_rand();

        // Node 1 has stake of 3
        for i in 0..3 {
            stakes.push((i, VoteState::new(&Pubkey::new_rand(), &node1, 0)));
        }

        // Node 1 has stake of 5
        stakes.push((5, VoteState::new(&Pubkey::new_rand(), &node2, 0)));

        let result = to_staked_nodes(stakes.into_iter());
        assert_eq!(result.len(), 2);
        assert_eq!(result[&node1], 3);
        assert_eq!(result[&node2], 5);
    }
}
