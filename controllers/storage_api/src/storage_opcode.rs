use crate::pgm_id::id;
use crate::storage_contract::{VeriPocSig, POC_ACCT_ROM};
use serde_derive::{Deserialize, Serialize};
use morgan_interface::hash::Hash;
use morgan_interface::opcodes::{AccountMeta, OpCode};
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::Signature;
use morgan_interface::sys_opcode;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PocOpCode {
    /// Initialize the account as a mining pool, validator or storage-miner
    ///
    /// Expects 1 Account:
    ///    0 - Account to be initialized
    SetPocPool,
    SetLocalStorage,
    SetPocStorage,

    SetPocSig {
        sha_state: Hash,
        slot: u64,
        signature: Signature,
    },
    BrdcstPocLastTxSeal {
        hash: Hash,
        slot: u64,
    },
    /// Redeem storage reward credits
    ///
    /// Expects 1 Account:
    ///    0 - Storage account with credits to redeem
    ///    1 - PocPool account to redeem credits from
    ClaimStorageReward {
        slot: u64,
    },
    CheckPocSig {
        segment: u64,
        poc_sigs: Vec<(BvmAddr, Vec<VeriPocSig>)>,
    },
}

pub fn crt_vldr_strj_acct(
    from_address: &BvmAddr,
    storage_address: &BvmAddr,
    difs: u64,
) -> Vec<OpCode> {
    vec![
        sys_opcode::create_account(
            from_address,
            storage_address,
            difs,
            POC_ACCT_ROM,
            &id(),
        ),
        OpCode::new(
            id(),
            &PocOpCode::SetLocalStorage,
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
            POC_ACCT_ROM,
            &id(),
        ),
        OpCode::new(
            id(),
            &PocOpCode::SetPocStorage,
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
            POC_ACCT_ROM,
            &id(),
        ),
        OpCode::new(
            id(),
            &PocOpCode::SetPocPool,
            vec![AccountMeta::new(*storage_address, false)],
        ),
    ]
}

pub fn poc_signature(
    storage_address: &BvmAddr,
    sha_state: Hash,
    slot: u64,
    signature: Signature,
) -> OpCode {
    let storage_opcode = PocOpCode::SetPocSig {
        sha_state,
        slot,
        signature,
    };
    let account_metas = vec![AccountMeta::new(*storage_address, true)];
    OpCode::new(id(), &storage_opcode, account_metas)
}

pub fn brdcst_last_tx_seal(
    storage_address: &BvmAddr,
    storage_hash: Hash,
    slot: u64,
) -> OpCode {
    let storage_opcode = PocOpCode::BrdcstPocLastTxSeal {
        hash: storage_hash,
        slot,
    };
    let account_metas = vec![AccountMeta::new(*storage_address, true)];
    OpCode::new(id(), &storage_opcode, account_metas)
}

pub fn verify_poc_sig<S: std::hash::BuildHasher>(
    storage_address: &BvmAddr,
    segment: u64,
    veri_poc_sigs: HashMap<BvmAddr, Vec<VeriPocSig>, S>,
) -> OpCode {
    let mut account_metas = vec![AccountMeta::new(*storage_address, true)];
    let mut proofs = vec![];
    veri_poc_sigs.into_iter().for_each(|(id, p)| {
        proofs.push((id, p));
        account_metas.push(AccountMeta::new(id, false))
    });
    let storage_opcode = PocOpCode::CheckPocSig { segment, poc_sigs:proofs };
    OpCode::new(id(), &storage_opcode, account_metas)
}

pub fn claim_reward(
    storage_address: &BvmAddr,
    mining_pool_address: &BvmAddr,
    slot: u64,
) -> OpCode {
    let storage_opcode = PocOpCode::ClaimStorageReward { slot };
    let account_metas = vec![
        AccountMeta::new(*storage_address, false),
        AccountMeta::new(*mining_pool_address, false),
    ];
    OpCode::new(id(), &storage_opcode, account_metas)
}
