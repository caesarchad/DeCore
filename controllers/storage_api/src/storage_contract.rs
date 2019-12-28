use crate::pgm_id::get_segment_from_slot;
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
pub const POC_ACCT_ROM: u64 = 1024 * 8;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum PocSeal {
    LeapSeal,
    Good,
    Bad,
}

impl Default for PocSeal {
    fn default() -> Self {
        PocSeal::LeapSeal
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PocSig {
    pub signature: Signature,
    pub sha_state: Hash,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct VeriPocSig {
    pub poc_sig: PocSig,
    pub status: PocSeal,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PocType {
    Uninitialized, // Must be first (aka, 0)

    LocalStrj {
        // Most recently advertised slot
        slot: u64,
        // Most recently advertised transaction_seal
        hash: Hash,
        lck_check: HashMap<usize, HashMap<Hash, PocSeal>>,
        coinbase_check: HashMap<usize, HashMap<Hash, PocSeal>>,
    },
    PocMiner {
        /// Map of Proofs per segment, in a HashMap based on the sha_state
        poc_sigs: HashMap<usize, HashMap<Hash, PocSig>>,
        /// Map of Rewards per segment, in a HashMap based on the sha_state
        /// Multiple validators can validate the same set of proofs so it needs a Vec
        coinbase_check: HashMap<usize, HashMap<Hash, Vec<PocSeal>>>,
    },

    PocPool,
}

// utility function, used by Treasury, tests, genesis
pub fn crt_vldr_strj_acct(difs: u64) -> Account {
    let mut strj_acct = Account::new(difs, 0, POC_ACCT_ROM as usize, &crate::pgm_id::id());

    strj_acct
        .set_state(&PocType::LocalStrj {
            slot: 0,
            hash: Hash::default(),
            lck_check: HashMap::new(),
            coinbase_check: HashMap::new(),
        })
        .expect("set_state");

        strj_acct
}

pub struct PocAcct<'a> {
    account: &'a mut Account,
}

impl<'a> PocAcct<'a> {
    pub fn new(account: &'a mut Account) -> Self {
        Self { account }
    }

    pub fn set_poc_pool(&mut self) -> Result<(), OpCodeErr> {
        let poc_pact = &mut self.account.state()?;
        if let PocType::Uninitialized = poc_pact {
            *poc_pact = PocType::PocPool;
            self.account.set_state(poc_pact)
        } else {
            Err(OpCodeErr::AccountAlreadyInitialized)?
        }
    }

    pub fn set_poc_miner_strj(&mut self) -> Result<(), OpCodeErr> {
        let poc_pact = &mut self.account.state()?;
        if let PocType::Uninitialized = poc_pact {
            *poc_pact = PocType::PocMiner {
                poc_sigs: HashMap::new(),
                coinbase_check: HashMap::new(),
            };
            self.account.set_state(poc_pact)
        } else {
            Err(OpCodeErr::AccountAlreadyInitialized)?
        }
    }

    pub fn set_local_strj(&mut self) -> Result<(), OpCodeErr> {
        let poc_pact = &mut self.account.state()?;
        if let PocType::Uninitialized = poc_pact {
            *poc_pact = PocType::LocalStrj {
                slot: 0,
                hash: Hash::default(),
                lck_check: HashMap::new(),
                coinbase_check: HashMap::new(),
            };
            self.account.set_state(poc_pact)
        } else {
            Err(OpCodeErr::AccountAlreadyInitialized)?
        }
    }

    pub fn issue_poc_sig(
        &mut self,
        sha_state: Hash,
        slot: u64,
        signature: Signature,
        current_slot: u64,
    ) -> Result<(), OpCodeErr> {
        let mut poc_pact = &mut self.account.state()?;
        if let PocType::PocMiner { poc_sigs, .. } = &mut poc_pact {
            let sgmt_indx = get_segment_from_slot(slot);
            let crnt_sgmt = get_segment_from_slot(current_slot);

            if sgmt_indx >= crnt_sgmt {
                // attempt to submit proof for unconfirmed segment
                return Err(OpCodeErr::InvalidArgument);
            }

            debug!(
                "Mining proof submitted with contract {:?} slot: {}",
                sha_state, slot
            );

            let sgmt_poc_sigs = poc_sigs.entry(sgmt_indx).or_default();
            if sgmt_poc_sigs.contains_key(&sha_state) {
                // do not accept duplicate proofs
                return Err(OpCodeErr::InvalidArgument);
            }
            sgmt_poc_sigs.insert(
                sha_state,
                PocSig {
                    sha_state,
                    signature,
                },
            );

            self.account.set_state(poc_pact)
        } else {
            Err(OpCodeErr::InvalidArgument)?
        }
    }

    pub fn brdcst_strj_rcnt_tx_seal(
        &mut self,
        hash: Hash,
        slot: u64,
        current_slot: u64,
    ) -> Result<(), OpCodeErr> {
        let mut poc_pact = &mut self.account.state()?;
        if let PocType::LocalStrj {
            slot: state_slot,
            hash: state_hash,
            coinbase_check,
            lck_check,
        } = &mut poc_pact
        {
            let crnt_sgmt = get_segment_from_slot(current_slot);
            let orgi_sgmt = get_segment_from_slot(*state_slot);
            let segment = get_segment_from_slot(slot);
            debug!(
                "advertise new segment: {} orig: {}",
                segment, crnt_sgmt
            );
            if segment < orgi_sgmt || segment >= crnt_sgmt {
                return Err(OpCodeErr::InvalidArgument);
            }

            *state_slot = slot;
            *state_hash = hash;

            // move storage epoch updated, move the lck_check to coinbase_check
            coinbase_check.extend(lck_check.drain());
            self.account.set_state(poc_pact)
        } else {
            Err(OpCodeErr::InvalidArgument)?
        }
    }

    pub fn verify_poc_sig(
        &mut self,
        segment: u64,
        proofs: Vec<(BvmAddr, Vec<VeriPocSig>)>,
        storage_miner_accounts: &mut [PocAcct],
    ) -> Result<(), OpCodeErr> {
        let mut poc_pact = &mut self.account.state()?;
        if let PocType::LocalStrj {
            slot: state_slot,
            lck_check,
            ..
        } = &mut poc_pact
        {
            let sgmt_indx = segment as usize;
            let s_sgmt = get_segment_from_slot(*state_slot);

            if sgmt_indx > s_sgmt {
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
                            PocType::PocMiner { poc_sigs, .. } => {
                                if let Some(poc_sigs) = poc_sigs.get(&sgmt_indx).cloned() {
                                    Some((account, poc_sigs))
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
                .flat_map(|((_id, verf_poc_sigs), (account, poc_sigs))| {
                    verf_poc_sigs.into_iter().filter_map(move |verified_poc_sig| {
                        poc_sigs.get(&verified_poc_sig.poc_sig.sha_state).map(|proof| {
                            save_verf_poc_sigs(account, sgmt_indx, &proof, &verified_poc_sig)
                                .map(|_| verified_poc_sig)
                        })
                    })
                })
                .flatten()
                .collect();

            // allow validators to store successful validations
            valid_proofs.into_iter().for_each(|proof| {
                lck_check
                    .entry(sgmt_indx)
                    .or_default()
                    .insert(proof.poc_sig.sha_state, proof.status);
            });

            self.account.set_state(poc_pact)
        } else {
            Err(OpCodeErr::InvalidArgument)?
        }
    }

    pub fn collect_poc_incentive(
        &mut self,
        mining_pool: &mut KeyedAccount,
        slot: u64,
        current_slot: u64,
    ) -> Result<(), OpCodeErr> {
        let mut poc_pact = &mut self.account.state()?;

        if let PocType::LocalStrj {
            coinbase_check,
            slot: state_slot,
            ..
        } = &mut poc_pact
        {
            let s_sgmt = get_segment_from_slot(*state_slot);
            let c_segment = get_segment_from_slot(slot);
            if s_sgmt <= c_segment || !coinbase_check.contains_key(&c_segment) {
                debug!(
                    "current {:?}, claim {:?}, have rewards for {:?} segments",
                    s_sgmt,
                    c_segment,
                    coinbase_check.len()
                );
                return Err(OpCodeErr::InvalidArgument);
            }
            let num_validations = poc_sigs_num(
                &coinbase_check
                    .remove(&c_segment)
                    .map(|mut proofs| proofs.drain().map(|(_, proof)| proof).collect::<Vec<_>>())
                    .unwrap_or_default(),
            );
            let reward = TOTAL_VALIDATOR_REWARDS * num_validations;
            mining_pool.account.difs -= reward;
            self.account.difs += reward;
            self.account.set_state(poc_pact)
        } else if let PocType::PocMiner {
            poc_sigs,
            coinbase_check,
        } = &mut poc_pact
        {
            // if current _drop height is a full segment away, allow reward collection
            let c_indx = get_segment_from_slot(current_slot);
            let c_segment = get_segment_from_slot(slot);
            // Todo this might might always be true
            if c_indx <= c_segment
                || !coinbase_check.contains_key(&c_segment)
                || !poc_sigs.contains_key(&c_segment)
            {
                // info!(
                //     "{}",
                //     Info(format!("current {:?}, claim {:?}, have rewards for {:?} segments",
                //     c_indx,
                //     c_segment,
                //     coinbase_check.len()).to_string())
                // );
                let info:String = format!("current {:?}, claim {:?}, have rewards for {:?} segments",
                    c_indx,
                    c_segment,
                    coinbase_check.len()).to_string();
                println!("{}",
                    printLn(
                        info,
                        module_path!().to_string()
                    )
                );
                return Err(OpCodeErr::InvalidArgument);
            }
            // remove proofs for which rewards have already been collected
            let sgmt_poc_sigs = poc_sigs.get_mut(&c_segment).unwrap();
            let verf_poc_sigs = coinbase_check
                .remove(&c_segment)
                .map(|mut poc_sigs| {
                    poc_sigs
                        .drain()
                        .map(|(sha_state, proof)| {
                            proof
                                .into_iter()
                                .map(|proof| {
                                    sgmt_poc_sigs.remove(&sha_state);
                                    proof
                                })
                                .collect::<Vec<_>>()
                        })
                        .flatten()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let total_proofs = verf_poc_sigs.len() as u64;
            let num_validations = poc_sigs_num(&verf_poc_sigs);
            let reward =
                num_validations * TOTAL_STORAGE_MINER_REWARDS * (num_validations / total_proofs);
            mining_pool.account.difs -= reward;
            self.account.difs += reward;
            self.account.set_state(poc_pact)
        } else {
            Err(OpCodeErr::InvalidArgument)?
        }
    }
}

/// Store the result of a proof validation into the storage-miner account
fn save_verf_result(
    poc_acct: &mut PocAcct,
    segment: usize,
    verified_poc_sig: VeriPocSig,
) -> Result<(), OpCodeErr> {
    let mut poc_pact = poc_acct.account.state()?;
    match &mut poc_pact {
        PocType::PocMiner {
            poc_sigs,
            coinbase_check,
            ..
        } => {
            if !poc_sigs.contains_key(&segment) {
                return Err(OpCodeErr::InvalidAccountData);
            }

            if poc_sigs
                .get(&segment)
                .unwrap()
                .contains_key(&verified_poc_sig.poc_sig.sha_state)
            {
                coinbase_check
                    .entry(segment)
                    .or_default()
                    .entry(verified_poc_sig.poc_sig.sha_state)
                    .or_default()
                    .push(verified_poc_sig.status);
            } else {
                return Err(OpCodeErr::InvalidAccountData);
            }
        }
        _ => return Err(OpCodeErr::InvalidAccountData),
    }
    poc_acct.account.set_state(&poc_pact)
}

fn poc_sigs_num(poc_sigs: &[PocSeal]) -> u64 {
    let mut num = 0;
    for poc_sig in poc_sigs {
        if let PocSeal::Good = poc_sig {
            num += 1;
        }
    }
    num
}

fn save_verf_poc_sigs(
    account: &mut PocAcct,
    sgmt_indx: usize,
    poc_sig: &PocSig,
    verified_poc_sig: &VeriPocSig,
) -> Result<(), OpCodeErr> {
    save_verf_result(account, sgmt_indx, verified_poc_sig.clone())?;
    if poc_sig.signature != verified_poc_sig.poc_sig.signature
        || verified_poc_sig.status != PocSeal::Good
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
        account.data.resize(POC_ACCT_ROM as usize, 0);
        let poc_acct = PocAcct::new(&mut account);
        // pretend it's a validator op code
        let mut contract = poc_acct.account.state().unwrap();
        if let PocType::LocalStrj { .. } = contract {
            assert!(true)
        }
        if let PocType::PocMiner { .. } = &mut contract {
            panic!("Contract should not decode into two types");
        }

        contract = PocType::LocalStrj {
            slot: 0,
            hash: Hash::default(),
            lck_check: HashMap::new(),
            coinbase_check: HashMap::new(),
        };
        poc_acct.account.set_state(&contract).unwrap();
        if let PocType::PocMiner { .. } = contract {
            panic!("Wrong contract type");
        }
        contract = PocType::PocMiner {
            poc_sigs: HashMap::new(),
            coinbase_check: HashMap::new(),
        };
        poc_acct.account.set_state(&contract).unwrap();
        if let PocType::LocalStrj { .. } = contract {
            panic!("Wrong contract type");
        }
    }

    #[test]
    fn test_process_validation() {
        let mut account = PocAcct {
            account: &mut Account {
                difs: 0,
                reputations: 0,
                data: vec![],
                owner: id(),
                executable: false,
            },
        };
        let sgmt_indx = 0_usize;
        let poc_sig = PocSig {
            signature: Signature::default(),
            sha_state: Hash::default(),
        };
        let mut verified_poc_sig = VeriPocSig {
            poc_sig: poc_sig.clone(),
            status: PocSeal::Good,
        };

        // account has no space
        save_verf_poc_sigs(&mut account, sgmt_indx, &poc_sig, &verified_poc_sig).unwrap_err();

        account
            .account
            .data
            .resize(POC_ACCT_ROM as usize, 0);
        let poc_pact = &mut account.account.state().unwrap();
        if let PocType::Uninitialized = poc_pact {
            
            let mut poc_sig_map = HashMap::new();
            poc_sig_map.insert(poc_sig.sha_state, poc_sig.clone());


            let mut poc_sig_vec = HashMap::new();
            poc_sig_vec.insert(0, poc_sig_map);
            *poc_pact = PocType::PocMiner {
                poc_sigs:poc_sig_vec,
                coinbase_check: HashMap::new(),
            };
        };
        account.account.set_state(poc_pact).unwrap();

        // proof is valid
        save_verf_poc_sigs(&mut account, sgmt_indx, &poc_sig, &verified_poc_sig).unwrap();

        verified_poc_sig.status = PocSeal::Bad;

        // proof failed verification
        save_verf_poc_sigs(&mut account, sgmt_indx, &poc_sig, &verified_poc_sig).unwrap_err();
    }
}
