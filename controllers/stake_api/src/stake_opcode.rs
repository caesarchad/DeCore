use crate::id;
use crate::stake_state::{StakeAccount, StakeState};
use bincode::deserialize;
use log::*;
use serde_derive::{Deserialize, Serialize};
use morgan_interface::account::KeyedAccount;
use morgan_interface::opcodes::{AccountMeta, OpCode, OpCodeErr};
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::sys_opcode;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum StakeOpCode {
    
    InitializeDelegate,
    
    InitializeMiningPool,
    
    DelegateStake,

    RedeemVoteCredits,
}

pub fn create_delegate_account(
    from_address: &BvmAddr,
    staker_address: &BvmAddr,
    difs: u64,
) -> Vec<OpCode> {
    vec![
        sys_opcode::create_account(
            from_address,
            staker_address,
            difs,
            std::mem::size_of::<StakeState>() as u64,
            &id(),
        ),
        OpCode::new(
            id(),
            &StakeOpCode::InitializeDelegate,
            vec![
                AccountMeta::new(*from_address, true),
                AccountMeta::new(*staker_address, false),
            ],
        ),
    ]
}

pub fn create_mining_pool_account(
    from_address: &BvmAddr,
    staker_address: &BvmAddr,
    difs: u64,
) -> Vec<OpCode> {
    vec![
        sys_opcode::create_account(
            from_address,
            staker_address,
            difs,
            std::mem::size_of::<StakeState>() as u64,
            &id(),
        ),
        OpCode::new(
            id(),
            &StakeOpCode::InitializeMiningPool,
            vec![
                AccountMeta::new(*from_address, true),
                AccountMeta::new(*staker_address, false),
            ],
        ),
    ]
}

pub fn redeem_vote_credits(
    from_address: &BvmAddr,
    mining_pool_address: &BvmAddr,
    stake_address: &BvmAddr,
    vote_address: &BvmAddr,
) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*from_address, true),
        AccountMeta::new(*mining_pool_address, false),
        AccountMeta::new(*stake_address, false),
        AccountMeta::new(*vote_address, false),
    ];
    OpCode::new(id(), &StakeOpCode::RedeemVoteCredits, account_metas)
}

pub fn delegate_stake(
    from_address: &BvmAddr,
    stake_address: &BvmAddr,
    vote_address: &BvmAddr,
) -> OpCode {
    let account_metas = vec![
        AccountMeta::new(*from_address, true),
        AccountMeta::new(*stake_address, true),
        AccountMeta::new(*vote_address, false),
    ];
    OpCode::new(id(), &StakeOpCode::DelegateStake, account_metas)
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

    // 0th index is the account who paid for the transaction
    // TODO: Remove the 0th index from the instruction. The stake program doesn't care who paid.
    let (me, rest) = &mut keyed_accounts.split_at_mut(2);
    let me = &mut me[1];

    // TODO: data-driven unpack and dispatch of KeyedAccounts
    match deserialize(data).map_err(|_| OpCodeErr::BadOpCodeContext)? {
        StakeOpCode::InitializeMiningPool => {
            if !rest.is_empty() {
                Err(OpCodeErr::BadOpCodeContext)?;
            }
            me.initialize_mining_pool()
        }
        StakeOpCode::InitializeDelegate => {
            if !rest.is_empty() {
                Err(OpCodeErr::BadOpCodeContext)?;
            }
            me.initialize_delegate()
        }
        StakeOpCode::DelegateStake => {
            if rest.len() != 1 {
                Err(OpCodeErr::BadOpCodeContext)?;
            }
            let vote = &rest[0];
            me.delegate_stake(vote)
        }
        StakeOpCode::RedeemVoteCredits => {
            if rest.len() != 2 {
                Err(OpCodeErr::BadOpCodeContext)?;
            }
            let (stake, vote) = rest.split_at_mut(1);
            let stake = &mut stake[0];
            let vote = &mut vote[0];

            me.redeem_vote_credits(stake, vote)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::serialize;
    use morgan_interface::account::Account;

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
                .map(|(meta, account)| KeyedAccount::new(&meta.address, meta.is_signer, account))
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
    fn test_stake_process_instruction() {
        assert_eq!(
            handle_opcode(&redeem_vote_credits(
                &BvmAddr::default(),
                &BvmAddr::default(),
                &BvmAddr::default(),
                &BvmAddr::default()
            )),
            Err(OpCodeErr::InvalidAccountData),
        );
        assert_eq!(
            handle_opcode(&delegate_stake(
                &BvmAddr::default(),
                &BvmAddr::default(),
                &BvmAddr::default()
            )),
            Err(OpCodeErr::InvalidAccountData),
        );
    }

    #[test]
    fn test_stake_process_instruction_decode_bail() {
        // these will not call stake_state, have bogus contents

        // gets the first check
        assert_eq!(
            super::handle_opcode(
                &BvmAddr::default(),
                &mut [KeyedAccount::new(
                    &BvmAddr::default(),
                    false,
                    &mut Account::default(),
                )],
                &serialize(&StakeOpCode::DelegateStake).unwrap(),
                0,
            ),
            Err(OpCodeErr::BadOpCodeContext),
        );

        // gets the sub-check for number of args
        assert_eq!(
            super::handle_opcode(
                &BvmAddr::default(),
                &mut [
                    KeyedAccount::new(&BvmAddr::default(), true, &mut Account::default()),
                    KeyedAccount::new(&BvmAddr::default(), false, &mut Account::default()),
                ],
                &serialize(&StakeOpCode::DelegateStake).unwrap(),
                0,
            ),
            Err(OpCodeErr::BadOpCodeContext),
        );

        assert_eq!(
            super::handle_opcode(
                &BvmAddr::default(),
                &mut [
                    KeyedAccount::new(&BvmAddr::default(), false, &mut Account::default()),
                    KeyedAccount::new(&BvmAddr::default(), false, &mut Account::default()),
                    KeyedAccount::new(&BvmAddr::default(), false, &mut Account::default()),
                ],
                &serialize(&StakeOpCode::RedeemVoteCredits).unwrap(),
                0,
            ),
            Err(OpCodeErr::BadOpCodeContext),
        );

        // gets the check in delegate_stake
        assert_eq!(
            super::handle_opcode(
                &BvmAddr::default(),
                &mut [
                    KeyedAccount::new(&BvmAddr::default(), true, &mut Account::default()), // from
                    KeyedAccount::new(&BvmAddr::default(), true, &mut Account::default()),
                    KeyedAccount::new(&BvmAddr::default(), false, &mut Account::default()),
                ],
                &serialize(&StakeOpCode::DelegateStake).unwrap(),
                0,
            ),
            Err(OpCodeErr::InvalidAccountData),
        );

        // gets the check in redeem_vote_credits
        assert_eq!(
            super::handle_opcode(
                &BvmAddr::default(),
                &mut [
                    KeyedAccount::new(&BvmAddr::default(), true, &mut Account::default()), // from
                    KeyedAccount::new(&BvmAddr::default(), false, &mut Account::default()),
                    KeyedAccount::new(&BvmAddr::default(), false, &mut Account::default()),
                    KeyedAccount::new(&BvmAddr::default(), false, &mut Account::default()),
                ],
                &serialize(&StakeOpCode::RedeemVoteCredits).unwrap(),
                0,
            ),
            Err(OpCodeErr::InvalidAccountData),
        );
    }

}
