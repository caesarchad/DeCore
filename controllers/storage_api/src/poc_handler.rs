//! storage program
//!  Receive mining proofs from miners, validate the answers
//!  and give reward for good proofs.
use crate::poc_pact::PocAcct;
use crate::poc_opcode::PocOpCode;
use morgan_interface::account::KeyedAccount;
use morgan_interface::opcodes::OpCodeErr;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::constants::DEFAULT_DROPS_PER_SLOT;
use morgan_helper::logHelper::*;

pub fn handle_opcode(
    _program_id: &BvmAddr,
    keyed_accounts: &mut [KeyedAccount],
    data: &[u8],
    drop_height: u64,
) -> Result<(), OpCodeErr> {
    morgan_logger::setup();

    let (me, rest) = keyed_accounts.split_at_mut(1);
    let me_unsigned = me[0].signer_key().is_none();
    let mut storage_account = PocAcct::new(&mut me[0].account);

    match bincode::deserialize(data).map_err(|_| OpCodeErr::BadOpCodeContext)? {
        PocOpCode::SetPocPool => {
            if !rest.is_empty() {
                Err(OpCodeErr::InvalidArgument)?;
            }
            storage_account.set_poc_pool()
        }
        PocOpCode::SetPocStorage => {
            if !rest.is_empty() {
                Err(OpCodeErr::InvalidArgument)?;
            }
            storage_account.set_poc_miner_strj()
        }
        PocOpCode::SetLocalStorage => {
            if !rest.is_empty() {
                Err(OpCodeErr::InvalidArgument)?;
            }
            storage_account.set_local_strj()
        }
        PocOpCode::SetPocSig {
            sha_state,
            slot,
            signature,
        } => {
            if me_unsigned || !rest.is_empty() {
                // This instruction must be signed by `me`
                Err(OpCodeErr::InvalidArgument)?;
            }
            storage_account.issue_poc_sig(
                sha_state,
                slot,
                signature,
                drop_height / DEFAULT_DROPS_PER_SLOT,
            )
        }
        PocOpCode::BrdcstPocLastTxSeal { hash, slot } => {
            if me_unsigned || !rest.is_empty() {
                // This instruction must be signed by `me`
                Err(OpCodeErr::InvalidArgument)?;
            }
            storage_account.brdcst_strj_rcnt_tx_seal(
                hash,
                slot,
                drop_height / DEFAULT_DROPS_PER_SLOT,
            )
        }
        PocOpCode::ClaimStorageReward { slot } => {
            if rest.len() != 1 {
                Err(OpCodeErr::InvalidArgument)?;
            }
            storage_account.collect_poc_incentive(
                &mut rest[0],
                slot,
                drop_height / DEFAULT_DROPS_PER_SLOT,
            )
        }
        PocOpCode::CheckPocSig { segment, poc_sigs } => {
            if me_unsigned || rest.is_empty() {
                // This instruction must be signed by `me` and `rest` cannot be empty
                Err(OpCodeErr::InvalidArgument)?;
            }
            let mut rest: Vec<_> = rest
                .iter_mut()
                .map(|keyed_account| PocAcct::new(&mut keyed_account.account))
                .collect();
            storage_account.verify_poc_sig(segment, poc_sigs, &mut rest)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::poc_pact::{
        VeriPocSig, PocSig, PocSeal, PocType, POC_ACCT_ROM,
        TOTAL_STORAGE_MINER_REWARDS, TOTAL_VALIDATOR_REWARDS,
    };
    use crate::poc_opcode;
    use crate::pgm_id::{
        SLOTS_PER_SEGMENT,
        get_segment_from_slot, 
        id,
    };
    use assert_matches::assert_matches;
    use bincode::deserialize;
    use log::*;
    use morgan_runtime::treasury::Treasury;
    use morgan_runtime::treasury_client::TreasuryClient;
    use morgan_interface::account::{create_keyed_accounts, Account};
    use morgan_interface::account_host::OnlineAccount;
    use morgan_interface::genesis_block::create_genesis_block;
    use morgan_interface::hash::{hash, Hash};
    use morgan_interface::opcodes::OpCode;
    use morgan_interface::context::Context;
    use morgan_interface::bvm_address::BvmAddr;
    use morgan_interface::signature::{Keypair, KeypairUtil, Signature};
    use std::collections::HashMap;
    use std::sync::Arc;

    const DROPS_IN_SEGMENT: u64 = SLOTS_PER_SEGMENT * DEFAULT_DROPS_PER_SLOT;

    fn test_instruction(
        ix: &OpCode,
        program_accounts: &mut [Account],
        drop_height: u64,
    ) -> Result<(), OpCodeErr> {
        let mut keyed_accounts: Vec<_> = ix
            .accounts
            .iter()
            .zip(program_accounts.iter_mut())
            .map(|(account_meta, account)| {
                KeyedAccount::new(&account_meta.address, account_meta.is_signer, account)
            })
            .collect();

        let ret = handle_opcode(&id(), &mut keyed_accounts, &ix.data, drop_height);
        // info!("{}", Info(format!("ret: {:?}", ret).to_string()));
        let info:String = format!("ret: {:?}", ret).to_string();
        println!("{}",
            printLn(
                info,
                module_path!().to_string()
            )
        );
        ret
    }

    #[test]
    fn test_proof_bounds() {
        let address = BvmAddr::new_rand();
        let mut account = Account {
            data: vec![0; POC_ACCT_ROM as usize],
            ..Account::default()
        };
        {
            let mut storage_account = PocAcct::new(&mut account);
            storage_account.set_poc_miner_strj().unwrap();
        }

        let ix = poc_opcode::poc_signature(
            &address,
            Hash::default(),
            SLOTS_PER_SEGMENT,
            Signature::default(),
        );
        // the proof is for slot 16, which is in segment 0, need to move the _drop height into segment 2
        let drops_till_next_segment = DROPS_IN_SEGMENT * 2;

        assert_eq!(
            test_instruction(&ix, &mut [account], drops_till_next_segment),
            Ok(())
        );
    }

    #[test]
    fn test_storage_tx() {
        let address = BvmAddr::new_rand();
        let mut accounts = [(address, Account::default())];
        let mut keyed_accounts = create_keyed_accounts(&mut accounts);
        assert!(handle_opcode(&id(), &mut keyed_accounts, &[], 42).is_err());
    }

    #[test]
    fn test_serialize_overflow() {
        let address = BvmAddr::new_rand();
        let mut keyed_accounts = Vec::new();
        let mut user_account = Account::default();
        keyed_accounts.push(KeyedAccount::new(&address, true, &mut user_account));

        let ix = poc_opcode::brdcst_last_tx_seal(
            &address,
            Hash::default(),
            SLOTS_PER_SEGMENT,
        );

        assert_eq!(
            handle_opcode(&id(), &mut keyed_accounts, &ix.data, 42),
            Err(OpCodeErr::InvalidAccountData)
        );
    }

    #[test]
    fn test_invalid_accounts_len() {
        let address = BvmAddr::new_rand();
        let mut accounts = [Account::default()];

        let ix =
            poc_opcode::poc_signature(&address, Hash::default(), 0, Signature::default());
        // move _drop height into segment 1
        let drops_till_next_segment = DROPS_IN_SEGMENT + 1;

        assert!(test_instruction(&ix, &mut accounts, drops_till_next_segment).is_err());

        let mut accounts = [Account::default(), Account::default(), Account::default()];

        assert!(test_instruction(&ix, &mut accounts, drops_till_next_segment).is_err());
    }

    #[test]
    fn test_submit_mining_invalid_slot() {
        morgan_logger::setup();
        let address = BvmAddr::new_rand();
        let mut accounts = [Account::default(), Account::default()];
        accounts[0].data.resize(POC_ACCT_ROM as usize, 0);
        accounts[1].data.resize(POC_ACCT_ROM as usize, 0);

        let ix =
            poc_opcode::poc_signature(&address, Hash::default(), 0, Signature::default());

        // submitting a proof for a slot in the past, so this should fail
        assert!(test_instruction(&ix, &mut accounts, 0).is_err());
    }

    #[test]
    fn test_submit_mining_ok() {
        morgan_logger::setup();
        let address = BvmAddr::new_rand();
        let mut accounts = [Account::default(), Account::default()];
        accounts[0].data.resize(POC_ACCT_ROM as usize, 0);
        {
            let mut storage_account = PocAcct::new(&mut accounts[0]);
            storage_account.set_poc_miner_strj().unwrap();
        }

        let ix =
            poc_opcode::poc_signature(&address, Hash::default(), 0, Signature::default());
        // move _drop height into segment 1
        let drops_till_next_segment = DROPS_IN_SEGMENT + 1;

        assert_matches!(
            test_instruction(&ix, &mut accounts, drops_till_next_segment),
            Ok(_)
        );
    }

    #[test]
    fn test_validate_mining() {
        morgan_logger::setup();
        let (genesis_block, mint_keypair) = create_genesis_block(1000);
        let mint_address = mint_keypair.address();

        let miner_1_storage_keypair = Keypair::new();
        let miner_1_storage_id = miner_1_storage_keypair.address();

        let miner_2_storage_keypair = Keypair::new();
        let miner_2_storage_id = miner_2_storage_keypair.address();

        let validator_storage_keypair = Keypair::new();
        let validator_storage_id = validator_storage_keypair.address();

        let mining_pool_keypair = Keypair::new();
        let mining_pool_address = mining_pool_keypair.address();

        let mut treasury = Treasury::new(&genesis_block);
        treasury.add_opcode_handler(id(), handle_opcode);
        let treasury = Arc::new(treasury);
        let slot = 0;
        let treasury_client = TreasuryClient::new_shared(&treasury);

        init_storage_accounts(
            &treasury_client,
            &mint_keypair,
            &[&validator_storage_id],
            &[&miner_1_storage_id, &miner_2_storage_id],
            10,
        );
        let context = Context::new(poc_opcode::create_mining_pool_account(
            &mint_address,
            &mining_pool_address,
            100,
        ));
        treasury_client.snd_online_context(&[&mint_keypair], context).unwrap();

        // _drop the treasury up until it's moved into storage segment 2 because the next advertise is for segment 1
        let next_storage_segment_drop_height = DROPS_IN_SEGMENT * 2;
        for _ in 0..next_storage_segment_drop_height {
            treasury.register_drop(&treasury.last_transaction_seal());
        }

        // advertise for storage segment 1
        let context = Context::new_with_payer(
            vec![poc_opcode::brdcst_last_tx_seal(
                &validator_storage_id,
                Hash::default(),
                SLOTS_PER_SEGMENT,
            )],
            Some(&mint_address),
        );
        assert_matches!(
            treasury_client.snd_online_context(&[&mint_keypair, &validator_storage_keypair], context),
            Ok(_)
        );

        // submit proofs 5 proofs for each storage-miner for segment 0
        let mut verified_poc_sigs: HashMap<_, Vec<_>> = HashMap::new();
        for slot in 0..5 {
            verified_poc_sigs
                .entry(miner_1_storage_id)
                .or_default()
                .push(submit_proof(
                    &mint_keypair,
                    &miner_1_storage_keypair,
                    slot,
                    &treasury_client,
                ));
            verified_poc_sigs
                .entry(miner_2_storage_id)
                .or_default()
                .push(submit_proof(
                    &mint_keypair,
                    &miner_2_storage_keypair,
                    slot,
                    &treasury_client,
                ));
        }
        let context = Context::new_with_payer(
            vec![poc_opcode::brdcst_last_tx_seal(
                &validator_storage_id,
                Hash::default(),
                SLOTS_PER_SEGMENT * 2,
            )],
            Some(&mint_address),
        );

        let next_storage_segment_drop_height = DROPS_IN_SEGMENT;
        for _ in 0..next_storage_segment_drop_height {
            treasury.register_drop(&treasury.last_transaction_seal());
        }

        assert_matches!(
            treasury_client.snd_online_context(&[&mint_keypair, &validator_storage_keypair], context),
            Ok(_)
        );

        let context = Context::new_with_payer(
            vec![poc_opcode::verify_poc_sig(
                &validator_storage_id,
                get_segment_from_slot(slot) as u64,
                verified_poc_sigs,
            )],
            Some(&mint_address),
        );

        assert_matches!(
            treasury_client.snd_online_context(&[&mint_keypair, &validator_storage_keypair], context),
            Ok(_)
        );

        let context = Context::new_with_payer(
            vec![poc_opcode::brdcst_last_tx_seal(
                &validator_storage_id,
                Hash::default(),
                SLOTS_PER_SEGMENT * 3,
            )],
            Some(&mint_address),
        );

        let next_storage_segment_drop_height = DROPS_IN_SEGMENT;
        for _ in 0..next_storage_segment_drop_height {
            treasury.register_drop(&treasury.last_transaction_seal());
        }

        assert_matches!(
            treasury_client.snd_online_context(&[&mint_keypair, &validator_storage_keypair], context),
            Ok(_)
        );

        assert_eq!(treasury_client.get_balance(&validator_storage_id).unwrap(), 10);

        let context = Context::new_with_payer(
            vec![poc_opcode::claim_reward(
                &validator_storage_id,
                &mining_pool_address,
                slot,
            )],
            Some(&mint_address),
        );
        assert_matches!(treasury_client.snd_online_context(&[&mint_keypair], context), Ok(_));
        assert_eq!(
            treasury_client.get_balance(&validator_storage_id).unwrap(),
            10 + (TOTAL_VALIDATOR_REWARDS * 10)
        );

        // _drop the treasury into the next storage epoch so that rewards can be claimed
        for _ in 0..=DROPS_IN_SEGMENT {
            treasury.register_drop(&treasury.last_transaction_seal());
        }

        assert_eq!(
            treasury_client.get_balance(&miner_1_storage_id).unwrap(),
            10
        );

        let context = Context::new_with_payer(
            vec![poc_opcode::claim_reward(
                &miner_1_storage_id,
                &mining_pool_address,
                slot,
            )],
            Some(&mint_address),
        );
        assert_matches!(treasury_client.snd_online_context(&[&mint_keypair], context), Ok(_));

        let context = Context::new_with_payer(
            vec![poc_opcode::claim_reward(
                &miner_2_storage_id,
                &mining_pool_address,
                slot,
            )],
            Some(&mint_address),
        );
        assert_matches!(treasury_client.snd_online_context(&[&mint_keypair], context), Ok(_));

        // TODO enable when rewards are working
        assert_eq!(
            treasury_client.get_balance(&miner_1_storage_id).unwrap(),
            10 + (TOTAL_STORAGE_MINER_REWARDS * 5)
        );
    }

    fn init_storage_accounts(
        client: &TreasuryClient,
        mint: &Keypair,
        validator_accounts_to_create: &[&BvmAddr],
        storage_miner_accounts_to_create: &[&BvmAddr],
        difs: u64,
    ) {
        let mut ixs: Vec<_> = validator_accounts_to_create
            .into_iter()
            .flat_map(|account| {
                poc_opcode::crt_vldr_strj_acct(
                    &mint.address(),
                    account,
                    difs,
                )
            })
            .collect();
        storage_miner_accounts_to_create
            .into_iter()
            .for_each(|account| {
                ixs.append(&mut poc_opcode::create_miner_storage_account(
                    &mint.address(),
                    account,
                    difs,
                ))
            });
        let context = Context::new(ixs);
        client.snd_online_context(&[mint], context).unwrap();
    }

    fn get_storage_slot<C: OnlineAccount>(client: &C, account: &BvmAddr) -> u64 {
        match client.get_account_data(&account).unwrap() {
            Some(storage_system_account_data) => {
                let contract = deserialize(&storage_system_account_data);
                if let Ok(contract) = contract {
                    match contract {
                        PocType::LocalStrj { slot, .. } => {
                            return slot;
                        }
                        _ => {
                            // info!("{}", Info(format!("error in reading slot".to_string()))),
                            let info:String = format!("error in reading slot").to_string();
                            println!("{}",
                                printLn(
                                    info,
                                    module_path!().to_string()
                                )
                            );
                        }
                    }
                }
            }
            None => {
                // info!("{}", Info(format!("error in reading slot").to_string()));
                let info:String = format!("error in reading slot").to_string();
                println!("{}",
                    printLn(
                        info,
                        module_path!().to_string()
                    )
                );
            }
        }
        0
    }

    fn submit_proof(
        mint_keypair: &Keypair,
        storage_keypair: &Keypair,
        slot: u64,
        treasury_client: &TreasuryClient,
    ) -> VeriPocSig {
        let sha_state = Hash::new(BvmAddr::new_rand().as_ref());
        let context = Context::new_with_payer(
            vec![poc_opcode::poc_signature(
                &storage_keypair.address(),
                sha_state,
                slot,
                Signature::default(),
            )],
            Some(&mint_keypair.address()),
        );

        assert_matches!(
            treasury_client.snd_online_context(&[&mint_keypair, &storage_keypair], context),
            Ok(_)
        );
        VeriPocSig {
            poc_sig: PocSig {
                signature: Signature::default(),
                sha_state,
            },
            status: PocSeal::Good,
        }
    }

    fn get_storage_transaction_seal<C: OnlineAccount>(client: &C, account: &BvmAddr) -> Hash {
        if let Some(storage_system_account_data) = client.get_account_data(&account).unwrap() {
            let contract = deserialize(&storage_system_account_data);
            if let Ok(contract) = contract {
                match contract {
                    PocType::LocalStrj { hash, .. } => {
                        return hash;
                    }
                    _ => (),
                }
            }
        }
        Hash::default()
    }

    #[test]
    fn test_treasury_storage() {
        let (genesis_block, mint_keypair) = create_genesis_block(1000);
        let mint_address = mint_keypair.address();
        let miner_keypair = Keypair::new();
        let miner_address = miner_keypair.address();
        let validator_keypair = Keypair::new();
        let validator_address = validator_keypair.address();

        let mut treasury = Treasury::new(&genesis_block);
        treasury.add_opcode_handler(id(), handle_opcode);
        // _drop the treasury up until it's moved into storage segment 2
        let next_storage_segment_drop_height = DROPS_IN_SEGMENT * 2;
        for _ in 0..next_storage_segment_drop_height {
            treasury.register_drop(&treasury.last_transaction_seal());
        }
        let treasury_client = TreasuryClient::new(treasury);

        let x = 42;
        let x2 = x * 2;
        let storage_transaction_seal = hash(&[x2]);

        treasury_client
            .online_transfer(10, &mint_keypair, &miner_address)
            .unwrap();

        let context = Context::new(poc_opcode::create_miner_storage_account(
            &mint_address,
            &miner_address,
            1,
        ));
        treasury_client.snd_online_context(&[&mint_keypair], context).unwrap();

        let context = Context::new(poc_opcode::crt_vldr_strj_acct(
            &mint_address,
            &validator_address,
            1,
        ));
        treasury_client.snd_online_context(&[&mint_keypair], context).unwrap();

        let context = Context::new_with_payer(
            vec![poc_opcode::brdcst_last_tx_seal(
                &validator_address,
                storage_transaction_seal,
                SLOTS_PER_SEGMENT,
            )],
            Some(&mint_address),
        );

        assert_matches!(
            treasury_client.snd_online_context(&[&mint_keypair, &validator_keypair], context),
            Ok(_)
        );

        let slot = 0;
        let context = Context::new_with_payer(
            vec![poc_opcode::poc_signature(
                &miner_address,
                Hash::default(),
                slot,
                Signature::default(),
            )],
            Some(&mint_address),
        );
        assert_matches!(
            treasury_client.snd_online_context(&[&mint_keypair, &miner_keypair], context),
            Ok(_)
        );

        assert_eq!(
            get_storage_slot(&treasury_client, &validator_address),
            SLOTS_PER_SEGMENT
        );
        assert_eq!(
            get_storage_transaction_seal(&treasury_client, &validator_address),
            storage_transaction_seal
        );
    }
}
