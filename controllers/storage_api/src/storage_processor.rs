//! storage program
//!  Receive mining proofs from miners, validate the answers
//!  and give reward for good proofs.
use crate::storage_contract::StorageAccount;
use crate::storage_instruction::StorageInstruction;
use morgan_interface::account::KeyedAccount;
use morgan_interface::instruction::InstructionError;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::timing::DEFAULT_DROPS_PER_SLOT;
use morgan_helper::logHelper::*;

pub fn process_instruction(
    _program_id: &Pubkey,
    keyed_accounts: &mut [KeyedAccount],
    data: &[u8],
    drop_height: u64,
) -> Result<(), InstructionError> {
    morgan_logger::setup();

    let (me, rest) = keyed_accounts.split_at_mut(1);
    let me_unsigned = me[0].signer_key().is_none();
    let mut storage_account = StorageAccount::new(&mut me[0].account);

    match bincode::deserialize(data).map_err(|_| InstructionError::InvalidInstructionData)? {
        StorageInstruction::InitializeMiningPool => {
            if !rest.is_empty() {
                Err(InstructionError::InvalidArgument)?;
            }
            storage_account.initialize_mining_pool()
        }
        StorageInstruction::InitializeMinerStorage => {
            if !rest.is_empty() {
                Err(InstructionError::InvalidArgument)?;
            }
            storage_account.initialize_storage_miner_storage()
        }
        StorageInstruction::InitializeValidatorStorage => {
            if !rest.is_empty() {
                Err(InstructionError::InvalidArgument)?;
            }
            storage_account.initialize_validator_storage()
        }
        StorageInstruction::SubmitMiningProof {
            sha_state,
            slot,
            signature,
        } => {
            if me_unsigned || !rest.is_empty() {
                // This instruction must be signed by `me`
                Err(InstructionError::InvalidArgument)?;
            }
            storage_account.submit_mining_proof(
                sha_state,
                slot,
                signature,
                drop_height / DEFAULT_DROPS_PER_SLOT,
            )
        }
        StorageInstruction::AdvertiseStorageRecentTransactionSeal { hash, slot } => {
            if me_unsigned || !rest.is_empty() {
                // This instruction must be signed by `me`
                Err(InstructionError::InvalidArgument)?;
            }
            storage_account.advertise_storage_recent_transaction_seal(
                hash,
                slot,
                drop_height / DEFAULT_DROPS_PER_SLOT,
            )
        }
        StorageInstruction::ClaimStorageReward { slot } => {
            if rest.len() != 1 {
                Err(InstructionError::InvalidArgument)?;
            }
            storage_account.claim_storage_reward(
                &mut rest[0],
                slot,
                drop_height / DEFAULT_DROPS_PER_SLOT,
            )
        }
        StorageInstruction::ProofValidation { segment, proofs } => {
            if me_unsigned || rest.is_empty() {
                // This instruction must be signed by `me` and `rest` cannot be empty
                Err(InstructionError::InvalidArgument)?;
            }
            let mut rest: Vec<_> = rest
                .iter_mut()
                .map(|keyed_account| StorageAccount::new(&mut keyed_account.account))
                .collect();
            storage_account.proof_validation(segment, proofs, &mut rest)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_contract::{
        CheckedProof, Proof, ProofStatus, StorageContract, STORAGE_ACCOUNT_SPACE,
        TOTAL_STORAGE_MINER_REWARDS, TOTAL_VALIDATOR_REWARDS,
    };
    use crate::storage_instruction;
    use crate::SLOTS_PER_SEGMENT;
    use crate::{get_segment_from_slot, id};
    use assert_matches::assert_matches;
    use bincode::deserialize;
    use log::*;
    use morgan_runtime::treasury::Treasury;
    use morgan_runtime::treasury_client::TreasuryClient;
    use morgan_interface::account::{create_keyed_accounts, Account};
    use morgan_interface::client::SyncClient;
    use morgan_interface::genesis_block::create_genesis_block;
    use morgan_interface::hash::{hash, Hash};
    use morgan_interface::instruction::Instruction;
    use morgan_interface::message::Message;
    use morgan_interface::pubkey::Pubkey;
    use morgan_interface::signature::{Keypair, KeypairUtil, Signature};
    use std::collections::HashMap;
    use std::sync::Arc;

    const DROPS_IN_SEGMENT: u64 = SLOTS_PER_SEGMENT * DEFAULT_DROPS_PER_SLOT;

    fn test_instruction(
        ix: &Instruction,
        program_accounts: &mut [Account],
        drop_height: u64,
    ) -> Result<(), InstructionError> {
        let mut keyed_accounts: Vec<_> = ix
            .accounts
            .iter()
            .zip(program_accounts.iter_mut())
            .map(|(account_meta, account)| {
                KeyedAccount::new(&account_meta.pubkey, account_meta.is_signer, account)
            })
            .collect();

        let ret = process_instruction(&id(), &mut keyed_accounts, &ix.data, drop_height);
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
        let pubkey = Pubkey::new_rand();
        let mut account = Account {
            data: vec![0; STORAGE_ACCOUNT_SPACE as usize],
            ..Account::default()
        };
        {
            let mut storage_account = StorageAccount::new(&mut account);
            storage_account.initialize_storage_miner_storage().unwrap();
        }

        let ix = storage_instruction::mining_proof(
            &pubkey,
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
        let pubkey = Pubkey::new_rand();
        let mut accounts = [(pubkey, Account::default())];
        let mut keyed_accounts = create_keyed_accounts(&mut accounts);
        assert!(process_instruction(&id(), &mut keyed_accounts, &[], 42).is_err());
    }

    #[test]
    fn test_serialize_overflow() {
        let pubkey = Pubkey::new_rand();
        let mut keyed_accounts = Vec::new();
        let mut user_account = Account::default();
        keyed_accounts.push(KeyedAccount::new(&pubkey, true, &mut user_account));

        let ix = storage_instruction::advertise_recent_transaction_seal(
            &pubkey,
            Hash::default(),
            SLOTS_PER_SEGMENT,
        );

        assert_eq!(
            process_instruction(&id(), &mut keyed_accounts, &ix.data, 42),
            Err(InstructionError::InvalidAccountData)
        );
    }

    #[test]
    fn test_invalid_accounts_len() {
        let pubkey = Pubkey::new_rand();
        let mut accounts = [Account::default()];

        let ix =
            storage_instruction::mining_proof(&pubkey, Hash::default(), 0, Signature::default());
        // move _drop height into segment 1
        let drops_till_next_segment = DROPS_IN_SEGMENT + 1;

        assert!(test_instruction(&ix, &mut accounts, drops_till_next_segment).is_err());

        let mut accounts = [Account::default(), Account::default(), Account::default()];

        assert!(test_instruction(&ix, &mut accounts, drops_till_next_segment).is_err());
    }

    #[test]
    fn test_submit_mining_invalid_slot() {
        morgan_logger::setup();
        let pubkey = Pubkey::new_rand();
        let mut accounts = [Account::default(), Account::default()];
        accounts[0].data.resize(STORAGE_ACCOUNT_SPACE as usize, 0);
        accounts[1].data.resize(STORAGE_ACCOUNT_SPACE as usize, 0);

        let ix =
            storage_instruction::mining_proof(&pubkey, Hash::default(), 0, Signature::default());

        // submitting a proof for a slot in the past, so this should fail
        assert!(test_instruction(&ix, &mut accounts, 0).is_err());
    }

    #[test]
    fn test_submit_mining_ok() {
        morgan_logger::setup();
        let pubkey = Pubkey::new_rand();
        let mut accounts = [Account::default(), Account::default()];
        accounts[0].data.resize(STORAGE_ACCOUNT_SPACE as usize, 0);
        {
            let mut storage_account = StorageAccount::new(&mut accounts[0]);
            storage_account.initialize_storage_miner_storage().unwrap();
        }

        let ix =
            storage_instruction::mining_proof(&pubkey, Hash::default(), 0, Signature::default());
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
        let mint_pubkey = mint_keypair.pubkey();

        let miner_1_storage_keypair = Keypair::new();
        let miner_1_storage_id = miner_1_storage_keypair.pubkey();

        let miner_2_storage_keypair = Keypair::new();
        let miner_2_storage_id = miner_2_storage_keypair.pubkey();

        let validator_storage_keypair = Keypair::new();
        let validator_storage_id = validator_storage_keypair.pubkey();

        let mining_pool_keypair = Keypair::new();
        let mining_pool_pubkey = mining_pool_keypair.pubkey();

        let mut treasury = Treasury::new(&genesis_block);
        treasury.add_instruction_processor(id(), process_instruction);
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
        let message = Message::new(storage_instruction::create_mining_pool_account(
            &mint_pubkey,
            &mining_pool_pubkey,
            100,
        ));
        treasury_client.send_message(&[&mint_keypair], message).unwrap();

        // _drop the treasury up until it's moved into storage segment 2 because the next advertise is for segment 1
        let next_storage_segment_drop_height = DROPS_IN_SEGMENT * 2;
        for _ in 0..next_storage_segment_drop_height {
            treasury.register_drop(&treasury.last_transaction_seal());
        }

        // advertise for storage segment 1
        let message = Message::new_with_payer(
            vec![storage_instruction::advertise_recent_transaction_seal(
                &validator_storage_id,
                Hash::default(),
                SLOTS_PER_SEGMENT,
            )],
            Some(&mint_pubkey),
        );
        assert_matches!(
            treasury_client.send_message(&[&mint_keypair, &validator_storage_keypair], message),
            Ok(_)
        );

        // submit proofs 5 proofs for each storage-miner for segment 0
        let mut checked_proofs: HashMap<_, Vec<_>> = HashMap::new();
        for slot in 0..5 {
            checked_proofs
                .entry(miner_1_storage_id)
                .or_default()
                .push(submit_proof(
                    &mint_keypair,
                    &miner_1_storage_keypair,
                    slot,
                    &treasury_client,
                ));
            checked_proofs
                .entry(miner_2_storage_id)
                .or_default()
                .push(submit_proof(
                    &mint_keypair,
                    &miner_2_storage_keypair,
                    slot,
                    &treasury_client,
                ));
        }
        let message = Message::new_with_payer(
            vec![storage_instruction::advertise_recent_transaction_seal(
                &validator_storage_id,
                Hash::default(),
                SLOTS_PER_SEGMENT * 2,
            )],
            Some(&mint_pubkey),
        );

        let next_storage_segment_drop_height = DROPS_IN_SEGMENT;
        for _ in 0..next_storage_segment_drop_height {
            treasury.register_drop(&treasury.last_transaction_seal());
        }

        assert_matches!(
            treasury_client.send_message(&[&mint_keypair, &validator_storage_keypair], message),
            Ok(_)
        );

        let message = Message::new_with_payer(
            vec![storage_instruction::proof_validation(
                &validator_storage_id,
                get_segment_from_slot(slot) as u64,
                checked_proofs,
            )],
            Some(&mint_pubkey),
        );

        assert_matches!(
            treasury_client.send_message(&[&mint_keypair, &validator_storage_keypair], message),
            Ok(_)
        );

        let message = Message::new_with_payer(
            vec![storage_instruction::advertise_recent_transaction_seal(
                &validator_storage_id,
                Hash::default(),
                SLOTS_PER_SEGMENT * 3,
            )],
            Some(&mint_pubkey),
        );

        let next_storage_segment_drop_height = DROPS_IN_SEGMENT;
        for _ in 0..next_storage_segment_drop_height {
            treasury.register_drop(&treasury.last_transaction_seal());
        }

        assert_matches!(
            treasury_client.send_message(&[&mint_keypair, &validator_storage_keypair], message),
            Ok(_)
        );

        assert_eq!(treasury_client.get_balance(&validator_storage_id).unwrap(), 10);

        let message = Message::new_with_payer(
            vec![storage_instruction::claim_reward(
                &validator_storage_id,
                &mining_pool_pubkey,
                slot,
            )],
            Some(&mint_pubkey),
        );
        assert_matches!(treasury_client.send_message(&[&mint_keypair], message), Ok(_));
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

        let message = Message::new_with_payer(
            vec![storage_instruction::claim_reward(
                &miner_1_storage_id,
                &mining_pool_pubkey,
                slot,
            )],
            Some(&mint_pubkey),
        );
        assert_matches!(treasury_client.send_message(&[&mint_keypair], message), Ok(_));

        let message = Message::new_with_payer(
            vec![storage_instruction::claim_reward(
                &miner_2_storage_id,
                &mining_pool_pubkey,
                slot,
            )],
            Some(&mint_pubkey),
        );
        assert_matches!(treasury_client.send_message(&[&mint_keypair], message), Ok(_));

        // TODO enable when rewards are working
        assert_eq!(
            treasury_client.get_balance(&miner_1_storage_id).unwrap(),
            10 + (TOTAL_STORAGE_MINER_REWARDS * 5)
        );
    }

    fn init_storage_accounts(
        client: &TreasuryClient,
        mint: &Keypair,
        validator_accounts_to_create: &[&Pubkey],
        storage_miner_accounts_to_create: &[&Pubkey],
        difs: u64,
    ) {
        let mut ixs: Vec<_> = validator_accounts_to_create
            .into_iter()
            .flat_map(|account| {
                storage_instruction::create_validator_storage_account(
                    &mint.pubkey(),
                    account,
                    difs,
                )
            })
            .collect();
        storage_miner_accounts_to_create
            .into_iter()
            .for_each(|account| {
                ixs.append(&mut storage_instruction::create_miner_storage_account(
                    &mint.pubkey(),
                    account,
                    difs,
                ))
            });
        let message = Message::new(ixs);
        client.send_message(&[mint], message).unwrap();
    }

    fn get_storage_slot<C: SyncClient>(client: &C, account: &Pubkey) -> u64 {
        match client.get_account_data(&account).unwrap() {
            Some(storage_system_account_data) => {
                let contract = deserialize(&storage_system_account_data);
                if let Ok(contract) = contract {
                    match contract {
                        StorageContract::ValidatorStorage { slot, .. } => {
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
    ) -> CheckedProof {
        let sha_state = Hash::new(Pubkey::new_rand().as_ref());
        let message = Message::new_with_payer(
            vec![storage_instruction::mining_proof(
                &storage_keypair.pubkey(),
                sha_state,
                slot,
                Signature::default(),
            )],
            Some(&mint_keypair.pubkey()),
        );

        assert_matches!(
            treasury_client.send_message(&[&mint_keypair, &storage_keypair], message),
            Ok(_)
        );
        CheckedProof {
            proof: Proof {
                signature: Signature::default(),
                sha_state,
            },
            status: ProofStatus::Valid,
        }
    }

    fn get_storage_transaction_seal<C: SyncClient>(client: &C, account: &Pubkey) -> Hash {
        if let Some(storage_system_account_data) = client.get_account_data(&account).unwrap() {
            let contract = deserialize(&storage_system_account_data);
            if let Ok(contract) = contract {
                match contract {
                    StorageContract::ValidatorStorage { hash, .. } => {
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
        let mint_pubkey = mint_keypair.pubkey();
        let miner_keypair = Keypair::new();
        let miner_pubkey = miner_keypair.pubkey();
        let validator_keypair = Keypair::new();
        let validator_pubkey = validator_keypair.pubkey();

        let mut treasury = Treasury::new(&genesis_block);
        treasury.add_instruction_processor(id(), process_instruction);
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
            .transfer(10, &mint_keypair, &miner_pubkey)
            .unwrap();

        let message = Message::new(storage_instruction::create_miner_storage_account(
            &mint_pubkey,
            &miner_pubkey,
            1,
        ));
        treasury_client.send_message(&[&mint_keypair], message).unwrap();

        let message = Message::new(storage_instruction::create_validator_storage_account(
            &mint_pubkey,
            &validator_pubkey,
            1,
        ));
        treasury_client.send_message(&[&mint_keypair], message).unwrap();

        let message = Message::new_with_payer(
            vec![storage_instruction::advertise_recent_transaction_seal(
                &validator_pubkey,
                storage_transaction_seal,
                SLOTS_PER_SEGMENT,
            )],
            Some(&mint_pubkey),
        );

        assert_matches!(
            treasury_client.send_message(&[&mint_keypair, &validator_keypair], message),
            Ok(_)
        );

        let slot = 0;
        let message = Message::new_with_payer(
            vec![storage_instruction::mining_proof(
                &miner_pubkey,
                Hash::default(),
                slot,
                Signature::default(),
            )],
            Some(&mint_pubkey),
        );
        assert_matches!(
            treasury_client.send_message(&[&mint_keypair, &miner_keypair], message),
            Ok(_)
        );

        assert_eq!(
            get_storage_slot(&treasury_client, &validator_pubkey),
            SLOTS_PER_SEGMENT
        );
        assert_eq!(
            get_storage_transaction_seal(&treasury_client, &validator_pubkey),
            storage_transaction_seal
        );
    }
}
