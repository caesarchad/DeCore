//! Vote program
//! Receive and processes votes from validators

use crate::id;
use crate::vote_state::{self, Vote, VoteState};
use bincode::deserialize;
use log::*;
use serde_derive::{Deserialize, Serialize};
use morgan_metricbot::datapoint_warn;
use morgan_interface::account::KeyedAccount;
use morgan_interface::opcodes::{AccountMeta, OpCode, OpCodeErr};
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::syscall::slot_hashes;
use morgan_interface::sys_opcode;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum VoteOpCode {
    /// Initialize the VoteState for this `vote account`
    /// takes a node_pubkey and commission
    InitializeAccount(BvmAddr, u32),

    /// Authorize a voter to send signed votes.
    AuthorizeVoter(BvmAddr),

    /// A Vote instruction with recent votes
    Vote(Vec<Vote>),
}

fn initialize_account(
    from_pubkey: &BvmAddr,
    vote_pubkey: &BvmAddr,
    node_pubkey: &BvmAddr,
    commission: u32,
) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*from_pubkey, true),
        AccountMeta::new(*vote_pubkey, false),
    ];
    OpCode::new(
        id(),
        &VoteOpCode::InitializeAccount(*node_pubkey, commission),
        account_metas,
    )
}

pub fn create_account(
    from_pubkey: &BvmAddr,
    vote_pubkey: &BvmAddr,
    node_pubkey: &BvmAddr,
    commission: u32,
    difs: u64,
) -> Vec<OpCode> {
    let space = VoteState::size_of() as u64;
    let create_ix =
        sys_opcode::create_account(from_pubkey, vote_pubkey, difs, space, &id());
    let init_ix = initialize_account(from_pubkey, vote_pubkey, node_pubkey, commission);
    vec![create_ix, init_ix]
}

fn metas_for_authorized_signer(
    from_pubkey: &BvmAddr,
    vote_pubkey: &BvmAddr,
    authorized_voter_pubkey: &BvmAddr, // currently authorized
) -> Vec<AccountMeta> {
    let mut account_metas = vec![AccountMeta::new(*from_pubkey, true)]; // sender

    let is_own_signer = authorized_voter_pubkey == vote_pubkey;

    account_metas.push(AccountMeta::new(*vote_pubkey, is_own_signer)); // vote account

    if !is_own_signer {
        account_metas.push(AccountMeta::new(*authorized_voter_pubkey, true)) // signer
    }
    account_metas
}

pub fn authorize_voter(
    from_pubkey: &BvmAddr,
    vote_pubkey: &BvmAddr,
    authorized_voter_pubkey: &BvmAddr, // currently authorized
    new_authorized_voter_pubkey: &BvmAddr,
) -> OpCode {
    let account_metas =
        metas_for_authorized_signer(from_pubkey, vote_pubkey, authorized_voter_pubkey);

    OpCode::new(
        id(),
        &VoteOpCode::AuthorizeVoter(*new_authorized_voter_pubkey),
        account_metas,
    )
}

pub fn vote(
    from_pubkey: &BvmAddr,
    vote_pubkey: &BvmAddr,
    authorized_voter_pubkey: &BvmAddr,
    recent_votes: Vec<Vote>,
) -> OpCode {
    let mut account_metas =
        metas_for_authorized_signer(from_pubkey, vote_pubkey, authorized_voter_pubkey);

    // request slot_hashes syscall account after vote_pubkey
    account_metas.insert(2, AccountMeta::new(slot_hashes::id(), false));

    OpCode::new(id(), &VoteOpCode::Vote(recent_votes), account_metas)
}

pub fn handle_opcode(
    _program_id: &BvmAddr,
    keyed_accounts: &mut [KeyedAccount],
    data: &[u8],
    _drop_height: u64,
) -> Result<(), OpCodeErr> {
    morgan_logger::setup();

    trace!("handle_opcode: {:?}", data);
    trace!("keyed_accounts: {:?}", keyed_accounts);

    if keyed_accounts.len() < 2 {
        Err(OpCodeErr::BadOpCodeContext)?;
    }

    // 0th index is the guy who paid for the transaction
    let (me, rest) = &mut keyed_accounts.split_at_mut(2);
    let me = &mut me[1];

    // TODO: data-driven unpack and dispatch of KeyedAccounts
    match deserialize(data).map_err(|_| OpCodeErr::BadOpCodeContext)? {
        VoteOpCode::InitializeAccount(node_pubkey, commission) => {
            vote_state::initialize_account(me, &node_pubkey, commission)
        }
        VoteOpCode::AuthorizeVoter(voter_pubkey) => {
            vote_state::authorize_voter(me, rest, &voter_pubkey)
        }
        VoteOpCode::Vote(votes) => {
            datapoint_warn!("vote-native", ("count", 1, i64));
            let (slot_hashes, other_signers) = rest.split_at_mut(1);
            let slot_hashes = &mut slot_hashes[0];
            vote_state::process_votes(me, slot_hashes, other_signers, &votes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use morgan_interface::account::Account;

    // these are for 100% coverage in this file
    #[test]
    fn test_vote_process_instruction_decode_bail() {
        assert_eq!(
            super::handle_opcode(&BvmAddr::default(), &mut [], &[], 0,),
            Err(OpCodeErr::BadOpCodeContext),
        );
    }

    fn handle_opcode(instruction: &OpCode) -> Result<(), OpCodeErr> {
        let mut accounts = vec![];
        for _ in 0..instruction.accounts.len() {
            accounts.push(Account::default());
        }
        {
            let mut keyed_accounts: Vec<_> = instruction
                .accounts
                .iter()
                .zip(accounts.iter_mut())
                .map(|(meta, account)| KeyedAccount::new(&meta.pubkey, meta.is_signer, account))
                .collect();
            super::handle_opcode(
                &BvmAddr::default(),
                &mut keyed_accounts,
                &instruction.data,
                0,
            )
        }
    }

    #[test]
    fn test_vote_process_instruction() {
        let instructions = create_account(
            &BvmAddr::default(),
            &BvmAddr::default(),
            &BvmAddr::default(),
            0,
            100,
        );
        assert_eq!(
            handle_opcode(&instructions[1]),
            Err(OpCodeErr::InvalidAccountData),
        );
        assert_eq!(
            handle_opcode(&vote(
                &BvmAddr::default(),
                &BvmAddr::default(),
                &BvmAddr::default(),
                vec![Vote::default()]
            )),
            Err(OpCodeErr::InvalidAccountData),
        );
        assert_eq!(
            handle_opcode(&authorize_voter(
                &BvmAddr::default(),
                &BvmAddr::default(),
                &BvmAddr::default(),
                &BvmAddr::default(),
            )),
            Err(OpCodeErr::InvalidAccountData),
        );
    }

}
