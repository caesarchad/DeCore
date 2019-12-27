use crate::id;
use crate::storage_contract::{CheckedProof, STORAGE_ACCOUNT_SPACE};
use serde_derive::{Deserialize, Serialize};
use morgan_interface::hash::Hash;
use morgan_interface::opcodes::{AccountMeta, OpCode};
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::Signature;
use morgan_interface::sys_opcode;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum StorageOpCode {
    /// Initialize the account as a mining pool, validator or storage-miner
    ///
    /// Expects 1 Account:
    ///    0 - Account to be initialized
    InitializeMiningPool,
    InitializeValidatorStorage,
    InitializeMinerStorage,

    SubmitMiningProof {
        sha_state: Hash,
        slot: u64,
        signature: Signature,
    },
    AdvertiseStorageRecentTransactionSeal {
        hash: Hash,
        slot: u64,
    },
    /// Redeem storage reward credits
    ///
    /// Expects 1 Account:
    ///    0 - Storage account with credits to redeem
    ///    1 - MiningPool account to redeem credits from
    ClaimStorageReward {
        slot: u64,
    },
    ProofValidation {
        segment: u64,
        proofs: Vec<(BvmAddr, Vec<CheckedProof>)>,
    },
}

pub fn create_validator_storage_account(
    from_address: &BvmAddr,
    storage_address: &BvmAddr,
    difs: u64,
) -> Vec<OpCode> {
    vec![
        sys_opcode::create_account(
            from_address,
            storage_address,
            difs,
            STORAGE_ACCOUNT_SPACE,
            &id(),
        ),
        OpCode::new(
            id(),
            &StorageOpCode::InitializeValidatorStorage,
            vec![AccountMeta::new(*storage_address, false)],
        ),
    ]
}

pub fn create_miner_storage_account(
    from_address: &BvmAddr,
    storage_address: &BvmAddr,
    difs: u64,
) -> Vec<OpCode> {
    vec![
        sys_opcode::create_account(
            from_address,
            storage_address,
            difs,
            STORAGE_ACCOUNT_SPACE,
            &id(),
        ),
        OpCode::new(
            id(),
            &StorageOpCode::InitializeMinerStorage,
            vec![AccountMeta::new(*storage_address, false)],
        ),
    ]
}

pub fn create_mining_pool_account(
    from_address: &BvmAddr,
    storage_address: &BvmAddr,
    difs: u64,
) -> Vec<OpCode> {
    vec![
        sys_opcode::create_account(
            from_address,
            storage_address,
            difs,
            STORAGE_ACCOUNT_SPACE,
            &id(),
        ),
        OpCode::new(
            id(),
            &StorageOpCode::InitializeMiningPool,
            vec![AccountMeta::new(*storage_address, false)],
        ),
    ]
}

pub fn mining_proof(
    storage_address: &BvmAddr,
    sha_state: Hash,
    slot: u64,
    signature: Signature,
) -> OpCode {
    let storage_opcode = StorageOpCode::SubmitMiningProof {
        sha_state,
        slot,
        signature,
    };
    let account_metas = vec![AccountMeta::new(*storage_address, true)];
    OpCode::new(id(), &storage_opcode, account_metas)
}

pub fn advertise_recent_transaction_seal(
    storage_address: &BvmAddr,
    storage_hash: Hash,
    slot: u64,
) -> OpCode {
    let storage_opcode = StorageOpCode::AdvertiseStorageRecentTransactionSeal {
        hash: storage_hash,
        slot,
    };
    let account_metas = vec![AccountMeta::new(*storage_address, true)];
    OpCode::new(id(), &storage_opcode, account_metas)
}

pub fn proof_validation<S: std::hash::BuildHasher>(
    storage_address: &BvmAddr,
    segment: u64,
    checked_proofs: HashMap<BvmAddr, Vec<CheckedProof>, S>,
) -> OpCode {
    let mut account_metas = vec![AccountMeta::new(*storage_address, true)];
    let mut proofs = vec![];
    checked_proofs.into_iter().for_each(|(id, p)| {
        proofs.push((id, p));
        account_metas.push(AccountMeta::new(id, false))
    });
    let storage_opcode = StorageOpCode::ProofValidation { segment, proofs };
    OpCode::new(id(), &storage_opcode, account_metas)
}

pub fn claim_reward(
    storage_address: &BvmAddr,
    mining_pool_address: &BvmAddr,
    slot: u64,
) -> OpCode {
    let storage_opcode = StorageOpCode::ClaimStorageReward { slot };
    let account_metas = vec![
        AccountMeta::new(*storage_address, false),
        AccountMeta::new(*mining_pool_address, false),
    ];
    OpCode::new(id(), &storage_opcode, account_metas)
}
