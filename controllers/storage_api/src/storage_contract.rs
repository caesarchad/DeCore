use crate::get_segment_from_slot;
use log::*;
use serde_derive::{Deserialize, Serialize};
use morgan_interface::account::Account;
use morgan_interface::account::KeyedAccount;
use morgan_interface::account_utils::State;
use morgan_interface::hash::Hash;
use morgan_interface::opcodes::OpCodeErr;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::Signature;
use std::collections::HashMap;
use morgan_helper::logHelper::*;

pub const TOTAL_VALIDATOR_REWARDS: u64 = 1;
pub const TOTAL_STORAGE_MINER_REWARDS: u64 = 1;
// Todo Tune this for actual use cases when storage miners are feature complete
pub const STORAGE_ACCOUNT_SPACE: u64 = 1024 * 8;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum ProofStatus {
    Skipped,
    Valid,
    NotValid,
}

impl Default for ProofStatus {
    fn default() -> Self {
        ProofStatus::Skipped
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Proof {
    pub signature: Signature,
    pub sha_state: Hash,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct CheckedProof {
    pub proof: Proof,
    pub status: ProofStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StorageContract {
    Uninitialized, // Must be first (aka, 0)

    ValidatorStorage {
        // Most recently advertised slot
        slot: u64,
        // Most recently advertised transaction_seal
        hash: Hash,
        lockout_validations: HashMap<usize, HashMap<Hash, ProofStatus>>,
        reward_validations: HashMap<usize, HashMap<Hash, ProofStatus>>,
    },
    MinerStorage {
        /// Map of Proofs per segment, in a HashMap based on the sha_state
        proofs: HashMap<usize, HashMap<Hash, Proof>>,
        /// Map of Rewards per segment, in a HashMap based on the sha_state
        /// Multiple validators can validate the same set of proofs so it needs a Vec
        reward_validations: HashMap<usize, HashMap<Hash, Vec<ProofStatus>>>,
    },

    MiningPool,
}

// utility function, used by Treasury, tests, genesis
pub fn create_validator_storage_account(difs: u64) -> Account {
    let mut storage_account = Account::new(difs, 0, STORAGE_ACCOUNT_SPACE as usize, &crate::id());

    storage_account
        .set_state(&StorageContract::ValidatorStorage {
            slot: 0,
            hash: Hash::default(),
            lockout_validations: HashMap::new(),
            reward_validations: HashMap::new(),
        })
        .expect("set_state");

    storage_account
}

pub struct StorageAccount<'a> {
    account: &'a mut Account,
}

impl<'a> StorageAccount<'a> {
    pub fn new(account: &'a mut Account) -> Self {
        Self { account }
    }

    pub fn initialize_mining_pool(&mut self) -> Result<(), OpCodeErr> {
        let storage_contract = &mut self.account.state()?;
        if let StorageContract::Uninitialized = storage_contract {
            *storage_contract = StorageContract::MiningPool;
            self.account.set_state(storage_contract)
        } else {
            Err(OpCodeErr::AccountAlreadyInitialized)?
        }
    }

    pub fn initialize_storage_miner_storage(&mut self) -> Result<(), OpCodeErr> {
        let storage_contract = &mut self.account.state()?;
        if let StorageContract::Uninitialized = storage_contract {
            *storage_contract = StorageContract::MinerStorage {
                proofs: HashMap::new(),
                reward_validations: HashMap::new(),
            };
            self.account.set_state(storage_contract)
        } else {
            Err(OpCodeErr::AccountAlreadyInitialized)?
        }
    }

    pub fn initialize_validator_storage(&mut self) -> Result<(), OpCodeErr> {
        let storage_contract = &mut self.account.state()?;
        if let StorageContract::Uninitialized = storage_contract {
            *storage_contract = StorageContract::ValidatorStorage {
                slot: 0,
                hash: Hash::default(),
                lockout_validations: HashMap::new(),
                reward_validations: HashMap::new(),
            };
            self.account.set_state(storage_contract)
        } else {
            Err(OpCodeErr::AccountAlreadyInitialized)?
        }
    }

    pub fn submit_mining_proof(
        &mut self,
        sha_state: Hash,
        slot: u64,
        signature: Signature,
        current_slot: u64,
    ) -> Result<(), OpCodeErr> {
        let mut storage_contract = &mut self.account.state()?;
        if let StorageContract::MinerStorage { proofs, .. } = &mut storage_contract {
            let segment_index = get_segment_from_slot(slot);
            let current_segment = get_segment_from_slot(current_slot);

            if segment_index >= current_segment {
                // attempt to submit proof for unconfirmed segment
                return Err(OpCodeErr::InvalidArgument);
            }

            debug!(
                "Mining proof submitted with contract {:?} slot: {}",
                sha_state, slot
            );

            let segment_proofs = proofs.entry(segment_index).or_default();
            if segment_proofs.contains_key(&sha_state) {
                // do not accept duplicate proofs
                return Err(OpCodeErr::InvalidArgument);
            }
            segment_proofs.insert(
                sha_state,
                Proof {
                    sha_state,
                    signature,
                },
            );

            self.account.set_state(storage_contract)
        } else {
            Err(OpCodeErr::InvalidArgument)?
        }
    }

    pub fn advertise_storage_recent_transaction_seal(
        &mut self,
        hash: Hash,
        slot: u64,
        current_slot: u64,
    ) -> Result<(), OpCodeErr> {
        let mut storage_contract = &mut self.account.state()?;
        if let StorageContract::ValidatorStorage {
            slot: state_slot,
            hash: state_hash,
            reward_validations,
            lockout_validations,
        } = &mut storage_contract
        {
            let current_segment = get_segment_from_slot(current_slot);
            let original_segment = get_segment_from_slot(*state_slot);
            let segment = get_segment_from_slot(slot);
            debug!(
                "advertise new segment: {} orig: {}",
                segment, current_segment
            );
            if segment < original_segment || segment >= current_segment {
                return Err(OpCodeErr::InvalidArgument);
            }

            *state_slot = slot;
            *state_hash = hash;

            // move storage epoch updated, move the lockout_validations to reward_validations
            reward_validations.extend(lockout_validations.drain());
            self.account.set_state(storage_contract)
        } else {
            Err(OpCodeErr::InvalidArgument)?
        }
    }

    pub fn proof_validation(
        &mut self,
        segment: u64,
        proofs: Vec<(BvmAddr, Vec<CheckedProof>)>,
        storage_miner_accounts: &mut [StorageAccount],
    ) -> Result<(), OpCodeErr> {
        let mut storage_contract = &mut self.account.state()?;
        if let StorageContract::ValidatorStorage {
            slot: state_slot,
            lockout_validations,
            ..
        } = &mut storage_contract
        {
            let segment_index = segment as usize;
            let state_segment = get_segment_from_slot(*state_slot);

            if segment_index > state_segment {
                return Err(OpCodeErr::InvalidArgument);
            }

            let accounts_and_proofs = storage_miner_accounts
                .iter_mut()
                .filter_map(|account| {
                    account
                        .account
                        .state()
                        .ok()
                        .map(move |contract| match contract {
                            StorageContract::MinerStorage { proofs, .. } => {
                                if let Some(proofs) = proofs.get(&segment_index).cloned() {
                                    Some((account, proofs))
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        })
                })
                .flatten()
                .collect::<Vec<_>>();

            if accounts_and_proofs.len() != proofs.len() {
                // don't have all the accounts to validate the proofs against
                return Err(OpCodeErr::InvalidArgument);
            }

            let valid_proofs: Vec<_> = proofs
                .into_iter()
                .zip(accounts_and_proofs.into_iter())
                .flat_map(|((_id, checked_proofs), (account, proofs))| {
                    checked_proofs.into_iter().filter_map(move |checked_proof| {
                        proofs.get(&checked_proof.proof.sha_state).map(|proof| {
                            process_validation(account, segment_index, &proof, &checked_proof)
                                .map(|_| checked_proof)
                        })
                    })
                })
                .flatten()
                .collect();

            // allow validators to store successful validations
            valid_proofs.into_iter().for_each(|proof| {
                lockout_validations
                    .entry(segment_index)
                    .or_default()
                    .insert(proof.proof.sha_state, proof.status);
            });

            self.account.set_state(storage_contract)
        } else {
            Err(OpCodeErr::InvalidArgument)?
        }
    }

    pub fn claim_storage_reward(
        &mut self,
        mining_pool: &mut KeyedAccount,
        slot: u64,
        current_slot: u64,
    ) -> Result<(), OpCodeErr> {
        let mut storage_contract = &mut self.account.state()?;

        if let StorageContract::ValidatorStorage {
            reward_validations,
            slot: state_slot,
            ..
        } = &mut storage_contract
        {
            let state_segment = get_segment_from_slot(*state_slot);
            let claim_segment = get_segment_from_slot(slot);
            if state_segment <= claim_segment || !reward_validations.contains_key(&claim_segment) {
                debug!(
                    "current {:?}, claim {:?}, have rewards for {:?} segments",
                    state_segment,
                    claim_segment,
                    reward_validations.len()
                );
                return Err(OpCodeErr::InvalidArgument);
            }
            let num_validations = count_valid_proofs(
                &reward_validations
                    .remove(&claim_segment)
                    .map(|mut proofs| proofs.drain().map(|(_, proof)| proof).collect::<Vec<_>>())
                    .unwrap_or_default(),
            );
            let reward = TOTAL_VALIDATOR_REWARDS * num_validations;
            mining_pool.account.difs -= reward;
            self.account.difs += reward;
            self.account.set_state(storage_contract)
        } else if let StorageContract::MinerStorage {
            proofs,
            reward_validations,
        } = &mut storage_contract
        {
            // if current _drop height is a full segment away, allow reward collection
            let claim_index = get_segment_from_slot(current_slot);
            let claim_segment = get_segment_from_slot(slot);
            // Todo this might might always be true
            if claim_index <= claim_segment
                || !reward_validations.contains_key(&claim_segment)
                || !proofs.contains_key(&claim_segment)
            {
                // info!(
                //     "{}",
                //     Info(format!("current {:?}, claim {:?}, have rewards for {:?} segments",
                //     claim_index,
                //     claim_segment,
                //     reward_validations.len()).to_string())
                // );
                let info:String = format!("current {:?}, claim {:?}, have rewards for {:?} segments",
                    claim_index,
                    claim_segment,
                    reward_validations.len()).to_string();
                println!("{}",
                    printLn(
                        info,
                        module_path!().to_string()
                    )
                );
                return Err(OpCodeErr::InvalidArgument);
            }
            // remove proofs for which rewards have already been collected
            let segment_proofs = proofs.get_mut(&claim_segment).unwrap();
            let checked_proofs = reward_validations
                .remove(&claim_segment)
                .map(|mut proofs| {
                    proofs
                        .drain()
                        .map(|(sha_state, proof)| {
                            proof
                                .into_iter()
                                .map(|proof| {
                                    segment_proofs.remove(&sha_state);
                                    proof
                                })
                                .collect::<Vec<_>>()
                        })
                        .flatten()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let total_proofs = checked_proofs.len() as u64;
            let num_validations = count_valid_proofs(&checked_proofs);
            let reward =
                num_validations * TOTAL_STORAGE_MINER_REWARDS * (num_validations / total_proofs);
            mining_pool.account.difs -= reward;
            self.account.difs += reward;
            self.account.set_state(storage_contract)
        } else {
            Err(OpCodeErr::InvalidArgument)?
        }
    }
}

/// Store the result of a proof validation into the storage-miner account
fn store_validation_result(
    storage_account: &mut StorageAccount,
    segment: usize,
    checked_proof: CheckedProof,
) -> Result<(), OpCodeErr> {
    let mut storage_contract = storage_account.account.state()?;
    match &mut storage_contract {
        StorageContract::MinerStorage {
            proofs,
            reward_validations,
            ..
        } => {
            if !proofs.contains_key(&segment) {
                return Err(OpCodeErr::InvalidAccountData);
            }

            if proofs
                .get(&segment)
                .unwrap()
                .contains_key(&checked_proof.proof.sha_state)
            {
                reward_validations
                    .entry(segment)
                    .or_default()
                    .entry(checked_proof.proof.sha_state)
                    .or_default()
                    .push(checked_proof.status);
            } else {
                return Err(OpCodeErr::InvalidAccountData);
            }
        }
        _ => return Err(OpCodeErr::InvalidAccountData),
    }
    storage_account.account.set_state(&storage_contract)
}

fn count_valid_proofs(proofs: &[ProofStatus]) -> u64 {
    let mut num = 0;
    for proof in proofs {
        if let ProofStatus::Valid = proof {
            num += 1;
        }
    }
    num
}

fn process_validation(
    account: &mut StorageAccount,
    segment_index: usize,
    proof: &Proof,
    checked_proof: &CheckedProof,
) -> Result<(), OpCodeErr> {
    store_validation_result(account, segment_index, checked_proof.clone())?;
    if proof.signature != checked_proof.proof.signature
        || checked_proof.status != ProofStatus::Valid
    {
        return Err(OpCodeErr::GenericError);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id;

    #[test]
    fn test_account_data() {
        morgan_logger::setup();
        let mut account = Account::default();
        account.data.resize(STORAGE_ACCOUNT_SPACE as usize, 0);
        let storage_account = StorageAccount::new(&mut account);
        // pretend it's a validator op code
        let mut contract = storage_account.account.state().unwrap();
        if let StorageContract::ValidatorStorage { .. } = contract {
            assert!(true)
        }
        if let StorageContract::MinerStorage { .. } = &mut contract {
            panic!("Contract should not decode into two types");
        }

        contract = StorageContract::ValidatorStorage {
            slot: 0,
            hash: Hash::default(),
            lockout_validations: HashMap::new(),
            reward_validations: HashMap::new(),
        };
        storage_account.account.set_state(&contract).unwrap();
        if let StorageContract::MinerStorage { .. } = contract {
            panic!("Wrong contract type");
        }
        contract = StorageContract::MinerStorage {
            proofs: HashMap::new(),
            reward_validations: HashMap::new(),
        };
        storage_account.account.set_state(&contract).unwrap();
        if let StorageContract::ValidatorStorage { .. } = contract {
            panic!("Wrong contract type");
        }
    }

    #[test]
    fn test_process_validation() {
        let mut account = StorageAccount {
            account: &mut Account {
                difs: 0,
                reputations: 0,
                data: vec![],
                owner: id(),
                executable: false,
            },
        };
        let segment_index = 0_usize;
        let proof = Proof {
            signature: Signature::default(),
            sha_state: Hash::default(),
        };
        let mut checked_proof = CheckedProof {
            proof: proof.clone(),
            status: ProofStatus::Valid,
        };

        // account has no space
        process_validation(&mut account, segment_index, &proof, &checked_proof).unwrap_err();

        account
            .account
            .data
            .resize(STORAGE_ACCOUNT_SPACE as usize, 0);
        let storage_contract = &mut account.account.state().unwrap();
        if let StorageContract::Uninitialized = storage_contract {
            let mut proof_map = HashMap::new();
            proof_map.insert(proof.sha_state, proof.clone());
            let mut proofs = HashMap::new();
            proofs.insert(0, proof_map);
            *storage_contract = StorageContract::MinerStorage {
                proofs,
                reward_validations: HashMap::new(),
            };
        };
        account.account.set_state(storage_contract).unwrap();

        // proof is valid
        process_validation(&mut account, segment_index, &proof, &checked_proof).unwrap();

        checked_proof.status = ProofStatus::NotValid;

        // proof failed verification
        process_validation(&mut account, segment_index, &proof, &checked_proof).unwrap_err();
    }
}
