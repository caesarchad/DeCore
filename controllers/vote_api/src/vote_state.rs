//! Vote state, vote program
//! Receive and processes votes from validators
use crate::id;
use bincode::{deserialize, serialize_into, serialized_size, ErrorKind};
use log::*;
use serde_derive::{Deserialize, Serialize};
use morgan_interface::account::{Account, KeyedAccount};
use morgan_interface::account_utils::State;
use morgan_interface::hash::Hash;
use morgan_interface::instruction::InstructionError;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::syscall::slot_hashes;
use std::collections::VecDeque;
use morgan_helper::logHelper::*;

// Maximum number of votes to keep around
pub const MAX_LOCKOUT_HISTORY: usize = 31;
pub const INITIAL_LOCKOUT: usize = 2;

#[derive(Serialize, Default, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Vote {
    /// A vote for height slot
    pub slot: u64,
    // signature of the bank's state at given slot
    pub hash: Hash,
}

impl Vote {
    pub fn new(slot: u64, hash: Hash) -> Self {
        Self { slot, hash }
    }
}

#[derive(Serialize, Default, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Lockout {
    pub slot: u64,
    pub confirmation_count: u32,
}

impl Lockout {
    pub fn new(vote: &Vote) -> Self {
        Self {
            slot: vote.slot,
            confirmation_count: 1,
        }
    }

    // The number of slots for which this vote is locked
    pub fn lockout(&self) -> u64 {
        (INITIAL_LOCKOUT as u64).pow(self.confirmation_count)
    }

    // The slot height at which this vote expires (cannot vote for any slot
    // less than this)
    pub fn expiration_slot(&self) -> u64 {
        self.slot + self.lockout()
    }
    pub fn is_expired(&self, slot: u64) -> bool {
        self.expiration_slot() < slot
    }
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct VoteState {
    pub votes: VecDeque<Lockout>,
    pub node_pubkey: Pubkey,
    pub authorized_voter_pubkey: Pubkey,
    /// fraction of std::u32::MAX that represents what part of a rewards
    ///  payout should be given to this VoteAccount
    pub commission: u32,
    pub root_slot: Option<u64>,
    credits: u64,
}

impl VoteState {
    pub fn new(vote_pubkey: &Pubkey, node_pubkey: &Pubkey, commission: u32) -> Self {
        let votes = VecDeque::new();
        let credits = 0;
        let root_slot = None;
        Self {
            votes,
            node_pubkey: *node_pubkey,
            authorized_voter_pubkey: *vote_pubkey,
            credits,
            commission,
            root_slot,
        }
    }

    pub fn size_of() -> usize {
        // Upper limit on the size of the Vote State. Equal to
        // size_of(VoteState) when votes.len() is MAX_LOCKOUT_HISTORY
        let mut vote_state = Self::default();
        vote_state.votes = VecDeque::from(vec![Lockout::default(); MAX_LOCKOUT_HISTORY]);
        vote_state.root_slot = Some(std::u64::MAX);
        serialized_size(&vote_state).unwrap() as usize
    }

    // utility function, used by Stakes, tests
    pub fn from(account: &Account) -> Option<VoteState> {
        account.state().ok()
    }

    pub fn deserialize(input: &[u8]) -> Result<Self, InstructionError> {
        deserialize(input).map_err(|_| InstructionError::InvalidAccountData)
    }

    pub fn serialize(&self, output: &mut [u8]) -> Result<(), InstructionError> {
        serialize_into(output, self).map_err(|err| match *err {
            ErrorKind::SizeLimit => InstructionError::AccountDataTooSmall,
            _ => InstructionError::GenericError,
        })
    }

    /// returns commission split as (voter_portion, staker_portion, was_split) tuple
    ///
    ///  if commission calculation is 100% one way or other,
    ///   indicate with false for was_split
    pub fn commission_split(&self, on: f64) -> (f64, f64, bool) {
        match self.commission {
            0 => (0.0, on, false),
            std::u32::MAX => (on, 0.0, false),
            split => {
                let mine = on * f64::from(split) / f64::from(std::u32::MAX);
                (mine, on - mine, true)
            }
        }
    }

    pub fn process_votes(&mut self, votes: &[Vote], slot_hashes: &[(u64, Hash)]) {
        votes.iter().for_each(|v| self.process_vote(v, slot_hashes));
    }

    pub fn process_vote(&mut self, vote: &Vote, slot_hashes: &[(u64, Hash)]) {
        // Ignore votes for slots earlier than we already have votes for
        if self
            .votes
            .back()
            .map_or(false, |old_vote| old_vote.slot >= vote.slot)
        {
            return;
        }

        // drop votes for which there is no matching slot and hash
        if !slot_hashes
            .iter()
            .any(|(slot, hash)| vote.slot == *slot && vote.hash == *hash)
        {
            if log_enabled!(log::Level::Warn) {
                for (slot, hash) in slot_hashes {
                    if vote.slot == *slot {
                        // warn!(
                        //     "dropped vote {:?} matched slot {}, but not hash {:?}",
                        //     vote, *slot, *hash
                        // );
                        println!(
                            "{}",
                            Warn(
                                format!("dropped vote {:?} matched slot {}, but not hash {:?}",
                                    vote, *slot, *hash).to_string(),
                                module_path!().to_string()
                            )
                        );
                    }
                    if vote.hash == *hash {
                        // warn!(
                        //     "dropped vote {:?} matched hash {:?}, but not slot {}",
                        //     vote, *hash, *slot,
                        // );
                        println!(
                            "{}",
                            Warn(
                                format!("dropped vote {:?} matched hash {:?}, but not slot {}",
                                    vote, *hash, *slot).to_string(),
                                module_path!().to_string()
                            )
                        );
                    }
                }
            }
            return;
        }

        let vote = Lockout::new(&vote);

        // TODO: Integrity checks
        // Verify the vote's bank hash matches what is expected

        self.pop_expired_votes(vote.slot);
        // Once the stack is full, pop the oldest vote and distribute rewards
        if self.votes.len() == MAX_LOCKOUT_HISTORY {
            let vote = self.votes.pop_front().unwrap();
            self.root_slot = Some(vote.slot);
            self.credits += 1;
        }
        self.votes.push_back(vote);
        self.double_lockouts();
    }

    pub fn process_vote_unchecked(&mut self, vote: &Vote) {
        self.process_vote(vote, &[(vote.slot, vote.hash)]);
    }
    pub fn process_slot_vote_unchecked(&mut self, slot: u64) {
        self.process_vote_unchecked(&Vote::new(slot, Hash::default()));
    }

    pub fn nth_recent_vote(&self, position: usize) -> Option<&Lockout> {
        if position < self.votes.len() {
            let pos = self.votes.len() - 1 - position;
            self.votes.get(pos)
        } else {
            None
        }
    }

    /// Number of "credits" owed to this account from the mining pool. Submit this
    /// VoteState to the Rewards program to trade credits for difs.
    pub fn credits(&self) -> u64 {
        self.credits
    }

    fn pop_expired_votes(&mut self, slot: u64) {
        loop {
            if self.votes.back().map_or(false, |v| v.is_expired(slot)) {
                self.votes.pop_back();
            } else {
                break;
            }
        }
    }

    fn double_lockouts(&mut self) {
        let stack_depth = self.votes.len();
        for (i, v) in self.votes.iter_mut().enumerate() {
            // Don't increase the lockout for this vote until we get more confirmations
            // than the max number of confirmations this vote has seen
            if stack_depth > i + v.confirmation_count as usize {
                v.confirmation_count += 1;
            }
        }
    }
}

/// Authorize the given pubkey to sign votes. This may be called multiple times,
/// but will implicitly withdraw authorization from the previously authorized
/// voter. The default voter is the owner of the vote account's pubkey.
pub fn authorize_voter(
    vote_account: &mut KeyedAccount,
    other_signers: &[KeyedAccount],
    authorized_voter_pubkey: &Pubkey,
) -> Result<(), InstructionError> {
    let mut vote_state: VoteState = vote_account.state()?;

    // current authorized signer must say "yay"
    let authorized = Some(&vote_state.authorized_voter_pubkey);
    if vote_account.signer_key() != authorized
        && other_signers
            .iter()
            .all(|account| account.signer_key() != authorized)
    {
        return Err(InstructionError::MissingRequiredSignature);
    }

    vote_state.authorized_voter_pubkey = *authorized_voter_pubkey;
    vote_account.set_state(&vote_state)
}

/// Initialize the vote_state for a vote account
/// Assumes that the account is being init as part of a account creation or balance transfer and
/// that the transaction must be signed by the staker's keys
pub fn initialize_account(
    vote_account: &mut KeyedAccount,
    node_pubkey: &Pubkey,
    commission: u32,
) -> Result<(), InstructionError> {
    let vote_state: VoteState = vote_account.state()?;

    if vote_state.authorized_voter_pubkey != Pubkey::default() {
        return Err(InstructionError::AccountAlreadyInitialized);
    }
    vote_account.set_state(&VoteState::new(
        vote_account.unsigned_key(),
        node_pubkey,
        commission,
    ))
}

pub fn process_votes(
    vote_account: &mut KeyedAccount,
    slot_hashes_account: &mut KeyedAccount,
    other_signers: &[KeyedAccount],
    votes: &[Vote],
) -> Result<(), InstructionError> {
    let mut vote_state: VoteState = vote_account.state()?;

    if vote_state.authorized_voter_pubkey == Pubkey::default() {
        return Err(InstructionError::UninitializedAccount);
    }

    if !slot_hashes::check_id(slot_hashes_account.unsigned_key()) {
        return Err(InstructionError::InvalidArgument);
    }

    let slot_hashes: Vec<(u64, Hash)> = slot_hashes_account.state()?;

    let authorized = Some(&vote_state.authorized_voter_pubkey);
    // find a signer that matches the authorized_voter_pubkey
    if vote_account.signer_key() != authorized
        && other_signers
            .iter()
            .all(|account| account.signer_key() != authorized)
    {
        return Err(InstructionError::MissingRequiredSignature);
    }

    vote_state.process_votes(&votes, &slot_hashes);
    vote_account.set_state(&vote_state)
}

// utility function, used by Bank, tests
pub fn create_account(
    vote_pubkey: &Pubkey,
    node_pubkey: &Pubkey,
    commission: u32,
    difs: u64,
) -> Account {
    let mut vote_account = Account::new(difs, 0, VoteState::size_of(), &id());

    initialize_account(
        &mut KeyedAccount::new(vote_pubkey, false, &mut vote_account),
        node_pubkey,
        commission,
    )
    .unwrap();
    vote_account
}

// utility function, used by morgan-genesis, tests
pub fn create_bootstrap_leader_account(
    vote_pubkey: &Pubkey,
    node_pubkey: &Pubkey,
    commission: u32,
    difs: u64,
) -> (Account, VoteState) {
    // Construct a vote account for the bootstrap_leader such that the leader_scheduler
    // will be forced to select it as the leader for height 0
    let mut vote_account = create_account(&vote_pubkey, &node_pubkey, commission, difs);

    let mut vote_state: VoteState = vote_account.state().unwrap();
    // TODO: get a hash for slot 0?
    vote_state.process_slot_vote_unchecked(0);

    vote_account.set_state(&vote_state).unwrap();
    (vote_account, vote_state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vote_state;
    use bincode::serialized_size;
    use morgan_interface::account::Account;
    use morgan_interface::account_utils::State;
    use morgan_interface::hash::hash;
    use morgan_interface::syscall;
    use morgan_interface::syscall::slot_hashes;

    const MAX_RECENT_VOTES: usize = 16;

    #[test]
    fn test_initialize_vote_account() {
        let vote_account_pubkey = Pubkey::new_rand();
        let mut vote_account = Account::new(100, 0, VoteState::size_of(), &id());

        let node_pubkey = Pubkey::new_rand();

        //init should pass
        let mut vote_account = KeyedAccount::new(&vote_account_pubkey, false, &mut vote_account);
        let res = initialize_account(&mut vote_account, &node_pubkey, 0);
        assert_eq!(res, Ok(()));

        // reinit should fail
        let res = initialize_account(&mut vote_account, &node_pubkey, 0);
        assert_eq!(res, Err(InstructionError::AccountAlreadyInitialized));
    }

    fn create_test_account() -> (Pubkey, Account) {
        let vote_pubkey = Pubkey::new_rand();
        (
            vote_pubkey,
            vote_state::create_account(&vote_pubkey, &Pubkey::new_rand(), 0, 100),
        )
    }

    fn create_test_slot_hashes_account(slot_hashes: &[(u64, Hash)]) -> (Pubkey, Account) {
        let mut slot_hashes_account = Account::new(
            0,
            0,
            serialized_size(&slot_hashes).unwrap() as usize,
            &syscall::id(),
        );
        slot_hashes_account
            .set_state(&slot_hashes.to_vec())
            .unwrap();
        (slot_hashes::id(), slot_hashes_account)
    }

    fn simulate_process_vote(
        vote_pubkey: &Pubkey,
        vote_account: &mut Account,
        vote: &Vote,
        slot_hashes: &[(u64, Hash)],
    ) -> Result<VoteState, InstructionError> {
        let (slot_hashes_id, mut slot_hashes_account) =
            create_test_slot_hashes_account(slot_hashes);

        process_votes(
            &mut KeyedAccount::new(vote_pubkey, true, vote_account),
            &mut KeyedAccount::new(&slot_hashes_id, false, &mut slot_hashes_account),
            &[],
            &[vote.clone()],
        )?;
        vote_account.state()
    }

    /// exercises all the keyed accounts stuff
    fn simulate_process_vote_unchecked(
        vote_pubkey: &Pubkey,
        vote_account: &mut Account,
        vote: &Vote,
    ) -> Result<VoteState, InstructionError> {
        simulate_process_vote(vote_pubkey, vote_account, vote, &[(vote.slot, vote.hash)])
    }

    #[test]
    fn test_vote_create_bootstrap_leader_account() {
        let vote_pubkey = Pubkey::new_rand();
        let (_vote_account, vote_state) =
            vote_state::create_bootstrap_leader_account(&vote_pubkey, &Pubkey::new_rand(), 0, 100);

        assert_eq!(vote_state.votes.len(), 1);
        assert_eq!(vote_state.votes[0], Lockout::new(&Vote::default()));
    }

    #[test]
    fn test_vote_serialize() {
        let mut buffer: Vec<u8> = vec![0; VoteState::size_of()];
        let mut vote_state = VoteState::default();
        vote_state
            .votes
            .resize(MAX_LOCKOUT_HISTORY, Lockout::default());
        assert!(vote_state.serialize(&mut buffer[0..4]).is_err());
        vote_state.serialize(&mut buffer).unwrap();
        assert_eq!(VoteState::deserialize(&buffer).unwrap(), vote_state);
    }

    #[test]
    fn test_voter_registration() {
        let (vote_pubkey, vote_account) = create_test_account();

        let vote_state: VoteState = vote_account.state().unwrap();
        assert_eq!(vote_state.authorized_voter_pubkey, vote_pubkey);
        assert!(vote_state.votes.is_empty());
    }

    #[test]
    fn test_vote() {
        let (vote_pubkey, mut vote_account) = create_test_account();

        let vote = Vote::new(1, Hash::default());
        let vote_state =
            simulate_process_vote_unchecked(&vote_pubkey, &mut vote_account, &vote).unwrap();
        assert_eq!(vote_state.votes, vec![Lockout::new(&vote)]);
        assert_eq!(vote_state.credits(), 0);
    }

    #[test]
    fn test_vote_slot_hashes() {
        let (vote_pubkey, mut vote_account) = create_test_account();

        let hash = hash(&[0u8]);
        let vote = Vote::new(0, hash);

        // wrong hash
        let vote_state = simulate_process_vote(
            &vote_pubkey,
            &mut vote_account,
            &vote,
            &[(0, Hash::default())],
        )
        .unwrap();
        assert_eq!(vote_state.votes.len(), 0);

        // wrong slot
        let vote_state =
            simulate_process_vote(&vote_pubkey, &mut vote_account, &vote, &[(1, hash)]).unwrap();
        assert_eq!(vote_state.votes.len(), 0);

        // empty slot_hashes
        let vote_state =
            simulate_process_vote(&vote_pubkey, &mut vote_account, &vote, &[]).unwrap();
        assert_eq!(vote_state.votes.len(), 0);

        // this one would work, but the wrong account is passed for slot_hashes_id
        let (_slot_hashes_id, mut slot_hashes_account) =
            create_test_slot_hashes_account(&[(vote.slot, vote.hash)]);
        assert_eq!(
            process_votes(
                &mut KeyedAccount::new(&vote_pubkey, true, &mut vote_account),
                &mut KeyedAccount::new(&Pubkey::default(), false, &mut slot_hashes_account),
                &[],
                &[vote.clone()],
            ),
            Err(InstructionError::InvalidArgument)
        );
    }

    #[test]
    fn test_vote_signature() {
        let (vote_pubkey, mut vote_account) = create_test_account();

        let vote = vec![Vote::new(1, Hash::default())];

        let (slot_hashes_id, mut slot_hashes_account) =
            create_test_slot_hashes_account(&[(1, Hash::default())]);

        // unsigned
        let res = process_votes(
            &mut KeyedAccount::new(&vote_pubkey, false, &mut vote_account),
            &mut KeyedAccount::new(&slot_hashes_id, false, &mut slot_hashes_account),
            &[],
            &vote,
        );
        assert_eq!(res, Err(InstructionError::MissingRequiredSignature));

        // unsigned
        let res = process_votes(
            &mut KeyedAccount::new(&vote_pubkey, true, &mut vote_account),
            &mut KeyedAccount::new(&slot_hashes_id, false, &mut slot_hashes_account),
            &[],
            &vote,
        );
        assert_eq!(res, Ok(()));

        // another voter
        let authorized_voter_pubkey = Pubkey::new_rand();
        let res = authorize_voter(
            &mut KeyedAccount::new(&vote_pubkey, false, &mut vote_account),
            &[],
            &authorized_voter_pubkey,
        );
        assert_eq!(res, Err(InstructionError::MissingRequiredSignature));

        let res = authorize_voter(
            &mut KeyedAccount::new(&vote_pubkey, true, &mut vote_account),
            &[],
            &authorized_voter_pubkey,
        );
        assert_eq!(res, Ok(()));
        // verify authorized_voter_pubkey can authorize authorized_voter_pubkey ;)
        let res = authorize_voter(
            &mut KeyedAccount::new(&vote_pubkey, false, &mut vote_account),
            &[KeyedAccount::new(
                &authorized_voter_pubkey,
                true,
                &mut Account::default(),
            )],
            &authorized_voter_pubkey,
        );
        assert_eq!(res, Ok(()));

        // not signed by authorized voter
        let vote = vec![Vote::new(2, Hash::default())];
        let (slot_hashes_id, mut slot_hashes_account) =
            create_test_slot_hashes_account(&[(2, Hash::default())]);
        let res = process_votes(
            &mut KeyedAccount::new(&vote_pubkey, true, &mut vote_account),
            &mut KeyedAccount::new(&slot_hashes_id, false, &mut slot_hashes_account),
            &[],
            &vote,
        );
        assert_eq!(res, Err(InstructionError::MissingRequiredSignature));

        // signed by authorized voter
        let vote = vec![Vote::new(2, Hash::default())];
        let res = process_votes(
            &mut KeyedAccount::new(&vote_pubkey, false, &mut vote_account),
            &mut KeyedAccount::new(&slot_hashes_id, false, &mut slot_hashes_account),
            &[KeyedAccount::new(
                &authorized_voter_pubkey,
                true,
                &mut Account::default(),
            )],
            &vote,
        );
        assert_eq!(res, Ok(()));
    }

    #[test]
    fn test_vote_without_initialization() {
        let vote_pubkey = Pubkey::new_rand();
        let mut vote_account = Account::new(100, 0, VoteState::size_of(), &id());

        let res = simulate_process_vote_unchecked(
            &vote_pubkey,
            &mut vote_account,
            &Vote::new(1, Hash::default()),
        );
        assert_eq!(res, Err(InstructionError::UninitializedAccount));
    }

    #[test]
    fn test_vote_lockout() {
        let (_vote_pubkey, vote_account) = create_test_account();

        let mut vote_state: VoteState = vote_account.state().unwrap();

        for i in 0..(MAX_LOCKOUT_HISTORY + 1) {
            vote_state.process_slot_vote_unchecked((INITIAL_LOCKOUT as usize * i) as u64);
        }

        // The last vote should have been popped b/c it reached a depth of MAX_LOCKOUT_HISTORY
        assert_eq!(vote_state.votes.len(), MAX_LOCKOUT_HISTORY);
        assert_eq!(vote_state.root_slot, Some(0));
        check_lockouts(&vote_state);

        // One more vote that confirms the entire stack,
        // the root_slot should change to the
        // second vote
        let top_vote = vote_state.votes.front().unwrap().slot;
        vote_state.process_slot_vote_unchecked(vote_state.votes.back().unwrap().expiration_slot());
        assert_eq!(Some(top_vote), vote_state.root_slot);

        // Expire everything except the first vote
        vote_state.process_slot_vote_unchecked(vote_state.votes.front().unwrap().expiration_slot());
        // First vote and new vote are both stored for a total of 2 votes
        assert_eq!(vote_state.votes.len(), 2);
    }

    #[test]
    fn test_vote_double_lockout_after_expiration() {
        let voter_pubkey = Pubkey::new_rand();
        let mut vote_state = VoteState::new(&voter_pubkey, &Pubkey::new_rand(), 0);

        for i in 0..3 {
            vote_state.process_slot_vote_unchecked(i as u64);
        }

        check_lockouts(&vote_state);

        // Expire the third vote (which was a vote for slot 2). The height of the
        // vote stack is unchanged, so none of the previous votes should have
        // doubled in lockout
        vote_state.process_slot_vote_unchecked((2 + INITIAL_LOCKOUT + 1) as u64);
        check_lockouts(&vote_state);

        // Vote again, this time the vote stack depth increases, so the lockouts should
        // double for everybody
        vote_state.process_slot_vote_unchecked((2 + INITIAL_LOCKOUT + 2) as u64);
        check_lockouts(&vote_state);

        // Vote again, this time the vote stack depth increases, so the lockouts should
        // double for everybody
        vote_state.process_slot_vote_unchecked((2 + INITIAL_LOCKOUT + 3) as u64);
        check_lockouts(&vote_state);
    }

    #[test]
    fn test_expire_multiple_votes() {
        let voter_pubkey = Pubkey::new_rand();
        let mut vote_state = VoteState::new(&voter_pubkey, &Pubkey::new_rand(), 0);

        for i in 0..3 {
            vote_state.process_slot_vote_unchecked(i as u64);
        }

        assert_eq!(vote_state.votes[0].confirmation_count, 3);

        // Expire the second and third votes
        let expire_slot = vote_state.votes[1].slot + vote_state.votes[1].lockout() + 1;
        vote_state.process_slot_vote_unchecked(expire_slot);
        assert_eq!(vote_state.votes.len(), 2);

        // Check that the old votes expired
        assert_eq!(vote_state.votes[0].slot, 0);
        assert_eq!(vote_state.votes[1].slot, expire_slot);

        // Process one more vote
        vote_state.process_slot_vote_unchecked(expire_slot + 1);

        // Confirmation count for the older first vote should remain unchanged
        assert_eq!(vote_state.votes[0].confirmation_count, 3);

        // The later votes should still have increasing confirmation counts
        assert_eq!(vote_state.votes[1].confirmation_count, 2);
        assert_eq!(vote_state.votes[2].confirmation_count, 1);
    }

    #[test]
    fn test_vote_credits() {
        let voter_pubkey = Pubkey::new_rand();
        let mut vote_state = VoteState::new(&voter_pubkey, &Pubkey::new_rand(), 0);

        for i in 0..MAX_LOCKOUT_HISTORY {
            vote_state.process_slot_vote_unchecked(i as u64);
        }

        assert_eq!(vote_state.credits, 0);

        vote_state.process_slot_vote_unchecked(MAX_LOCKOUT_HISTORY as u64 + 1);
        assert_eq!(vote_state.credits, 1);
        vote_state.process_slot_vote_unchecked(MAX_LOCKOUT_HISTORY as u64 + 2);
        assert_eq!(vote_state.credits(), 2);
        vote_state.process_slot_vote_unchecked(MAX_LOCKOUT_HISTORY as u64 + 3);
        assert_eq!(vote_state.credits(), 3);
    }

    #[test]
    fn test_duplicate_vote() {
        let voter_pubkey = Pubkey::new_rand();
        let mut vote_state = VoteState::new(&voter_pubkey, &Pubkey::new_rand(), 0);
        vote_state.process_slot_vote_unchecked(0);
        vote_state.process_slot_vote_unchecked(1);
        vote_state.process_slot_vote_unchecked(0);
        assert_eq!(vote_state.nth_recent_vote(0).unwrap().slot, 1);
        assert_eq!(vote_state.nth_recent_vote(1).unwrap().slot, 0);
        assert!(vote_state.nth_recent_vote(2).is_none());
    }

    #[test]
    fn test_nth_recent_vote() {
        let voter_pubkey = Pubkey::new_rand();
        let mut vote_state = VoteState::new(&voter_pubkey, &Pubkey::new_rand(), 0);
        for i in 0..MAX_LOCKOUT_HISTORY {
            vote_state.process_slot_vote_unchecked(i as u64);
        }
        for i in 0..(MAX_LOCKOUT_HISTORY - 1) {
            assert_eq!(
                vote_state.nth_recent_vote(i).unwrap().slot as usize,
                MAX_LOCKOUT_HISTORY - i - 1,
            );
        }
        assert!(vote_state.nth_recent_vote(MAX_LOCKOUT_HISTORY).is_none());
    }

    fn check_lockouts(vote_state: &VoteState) {
        for (i, vote) in vote_state.votes.iter().enumerate() {
            let num_lockouts = vote_state.votes.len() - i;
            assert_eq!(
                vote.lockout(),
                INITIAL_LOCKOUT.pow(num_lockouts as u32) as u64
            );
        }
    }

    fn recent_votes(vote_state: &VoteState) -> Vec<Vote> {
        let start = vote_state.votes.len().saturating_sub(MAX_RECENT_VOTES);
        (start..vote_state.votes.len())
            .map(|i| Vote::new(vote_state.votes.get(i).unwrap().slot, Hash::default()))
            .collect()
    }

    /// check that two accounts with different data can be brought to the same state with one vote submission
    #[test]
    fn test_process_missed_votes() {
        let account_a = Pubkey::new_rand();
        let mut vote_state_a = VoteState::new(&account_a, &Pubkey::new_rand(), 0);
        let account_b = Pubkey::new_rand();
        let mut vote_state_b = VoteState::new(&account_b, &Pubkey::new_rand(), 0);

        // process some votes on account a
        (0..5)
            .into_iter()
            .for_each(|i| vote_state_a.process_slot_vote_unchecked(i as u64));
        assert_ne!(recent_votes(&vote_state_a), recent_votes(&vote_state_b));

        // as long as b has missed less than "NUM_RECENT" votes both accounts should be in sync
        let votes: Vec<_> = (0..MAX_RECENT_VOTES)
            .into_iter()
            .map(|i| Vote::new(i as u64, Hash::default()))
            .collect();
        let slot_hashes: Vec<_> = votes.iter().map(|vote| (vote.slot, vote.hash)).collect();

        vote_state_a.process_votes(&votes, &slot_hashes);
        vote_state_b.process_votes(&votes, &slot_hashes);
        assert_eq!(recent_votes(&vote_state_a), recent_votes(&vote_state_b));
    }

    #[test]
    fn test_vote_state_commission_split() {
        let vote_state = VoteState::new(&Pubkey::default(), &Pubkey::default(), 0);

        assert_eq!(vote_state.commission_split(1.0), (0.0, 1.0, false));

        let vote_state = VoteState::new(&Pubkey::default(), &Pubkey::default(), std::u32::MAX);
        assert_eq!(vote_state.commission_split(1.0), (1.0, 0.0, false));

        let vote_state = VoteState::new(&Pubkey::default(), &Pubkey::default(), std::u32::MAX / 2);
        let (voter_portion, staker_portion, was_split) = vote_state.commission_split(10.0);

        assert_eq!(
            (voter_portion.round(), staker_portion.round(), was_split),
            (5.0, 5.0, true)
        );
    }

}
