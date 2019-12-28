// A phase that handles generating the keys used to encrypt the ledger and sample it
// for storage mining. miners submit storage proofs, validator then bundles them
// to submit its proof for mining to be rewarded.

// use crate::treasury_forks::TreasuryForks;
use crate::treasury_forks::TreasuryForks;
use crate::block_buffer_pool::BlockBufferPool;
#[cfg(all(feature = "chacha", feature = "cuda"))]
use crate::chacha_cuda::chacha_cbc_encrypt_file_many_keys;
use crate::node_group_info::NodeGroupInfo;
use crate::result::{Error, Result};
use crate::service::Service;
use crate::bvm_types::*;
use bincode::deserialize;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use morgan_interface::hash::Hash;
use morgan_interface::opcodes::OpCode;
use morgan_interface::context::Context;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::{Keypair, KeypairUtil, Signature};
use morgan_interface::transaction::Transaction;
use morgan_storage_api::storage_contract::{VeriPocSig, PocSig, PocSeal};
use morgan_storage_api::storage_opcode::{self,verify_poc_sig, PocOpCode};
use morgan_storage_api::pgm_id::get_segment_from_slot;
use std::collections::HashMap;
use std::io;
use std::mem::size_of;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{self, sleep, Builder, JoinHandle};
use std::time::Duration;
use morgan_helper::logHelper::*;

// Block of hash answers to validate against
// Vec of [ledger blocks] x [keys]
type StorageResults = Vec<Hash>;
type StorageKeys = Vec<u8>;
type StorageMinerMap = Vec<HashMap<BvmAddr, Vec<PocSig>>>;

#[derive(Default)]
pub struct StorageStateInner {
    storage_results: StorageResults,
    storage_keys: StorageKeys,
    storage_miner_map: StorageMinerMap,
    storage_transaction_seal: Hash,
    slot: u64,
}

#[derive(Clone, Default)]
pub struct StorageState {
    state: Arc<RwLock<StorageStateInner>>,
}

pub struct StoragePhase {
    t_storage_mining_verifier: JoinHandle<()>,
    t_storage_create_accounts: JoinHandle<()>,
}


// TODO: some way to dynamically size NUM_IDENTITIES
const NUM_IDENTITIES: usize = 1024;

const KEY_SIZE: usize = 64;

type OpCodeSender = Sender<OpCode>;

fn get_identity_index_from_signature(key: &Signature) -> usize {
    let rkey = key.as_ref();
    let mut res: usize = (rkey[0] as usize)
        | ((rkey[1] as usize) << 8)
        | ((rkey[2] as usize) << 16)
        | ((rkey[3] as usize) << 24);
    res &= NUM_IDENTITIES - 1;
    res
}

impl StorageState {
    pub fn new() -> Self {
        let storage_keys = vec![0u8; KEY_SIZE * NUM_IDENTITIES];
        let storage_results = vec![Hash::default(); NUM_IDENTITIES];
        let storage_miner_map = vec![];

        let state = StorageStateInner {
            storage_keys,
            storage_results,
            storage_miner_map,
            slot: 0,
            storage_transaction_seal: Hash::default(),
        };

        StorageState {
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub fn get_mining_key(&self, key: &Signature) -> Vec<u8> {
        let idx = get_identity_index_from_signature(key);
        self.state.read().unwrap().storage_keys[idx..idx + KEY_SIZE].to_vec()
    }

    pub fn get_mining_result(&self, key: &Signature) -> Hash {
        let idx = get_identity_index_from_signature(key);
        self.state.read().unwrap().storage_results[idx]
    }

    pub fn get_storage_transaction_seal(&self) -> Hash {
        self.state.read().unwrap().storage_transaction_seal
    }

    pub fn get_slot(&self) -> u64 {
        self.state.read().unwrap().slot
    }

    pub fn get_addresss_for_slot(&self, slot: u64) -> Vec<BvmAddr> {
        const MAX_PUBKEYS_TO_RETURN: usize = 5;
        let index = get_segment_from_slot(slot) as usize;
        let storage_miner_map = &self.state.read().unwrap().storage_miner_map;
        if index < storage_miner_map.len() {
            storage_miner_map[index]
                .keys()
                .cloned()
                .take(MAX_PUBKEYS_TO_RETURN)
                .collect::<Vec<_>>()
        } else {
            vec![]
        }
    }
}

impl StoragePhase {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_state: &StorageState,
        slot_rcvr: Receiver<Vec<u64>>,
        block_buffer_pool: Option<Arc<BlockBufferPool>>,
        keypair: &Arc<Keypair>,
        storage_keypair: &Arc<Keypair>,
        exit: &Arc<AtomicBool>,
        treasury_forks: &Arc<RwLock<TreasuryForks>>,
        storage_rotate_count: u64,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
    ) -> Self {
        let (instruction_sndr, opcode_rcvr) = channel();

        let t_storage_mining_verifier = {
            let storage_state_inner = storage_state.state.clone();
            let exit = exit.clone();
            let storage_keypair = storage_keypair.clone();
            Builder::new()
                .name("morgan-storage-mining-verify-phase".to_string())
                .spawn(move || {
                    let mut current_key = 0;
                    let mut slot_count = 0;
                    let mut last_root = 0;
                    loop {
                        if let Some(ref some_block_buffer) = block_buffer_pool {
                            if let Err(e) = Self::handle_fiscal_stmts(
                                &storage_keypair,
                                &storage_state_inner,
                                &slot_rcvr,
                                &some_block_buffer,
                                &mut slot_count,
                                &mut last_root,
                                &mut current_key,
                                storage_rotate_count,
                                &instruction_sndr,
                            ) {
                                match e {
                                    Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => {
                                        break
                                    }
                                    Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                                    _ => {
                                        // info!("{}", Info(format!("Error from handle_fiscal_stmts: {:?}", e).to_string())),
                                        println!("{}",
                                            printLn(
                                                format!("Error from handle_fiscal_stmts: {:?}", e).to_string(),
                                                module_path!().to_string()
                                            )
                                        );
                                    }
                                }
                            }
                        }
                        if exit.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                })
                .unwrap()
        };

        let t_storage_create_accounts = {
            let node_group_info = node_group_info.clone();
            let exit = exit.clone();
            let keypair = keypair.clone();
            let storage_keypair = storage_keypair.clone();
            let treasury_forks = treasury_forks.clone();
            Builder::new()
                .name("morgan-storage-create-accounts".to_string())
                .spawn(move || {
                    let account_host_socket = UdpSocket::bind("0.0.0.0:0").unwrap();

                    {
                        let working_treasury = treasury_forks.read().unwrap().working_treasury();
                        let storage_account = working_treasury.get_account(&storage_keypair.address());
                        if storage_account.is_none() {
                            // warn!("Storage account not found: {}", storage_keypair.address());
                            println!(
                                "{}",
                                Warn(
                                    format!("Storage account not found: {}", storage_keypair.address()).to_string(),
                                    module_path!().to_string()
                                )
                            );
                        }
                    }

                    loop {
                        match opcode_rcvr.recv_timeout(Duration::from_secs(1)) {
                            Ok(instruction) => {
                                Self::send_transaction(
                                    &treasury_forks,
                                    &node_group_info,
                                    instruction,
                                    &keypair,
                                    &storage_keypair,
                                    &account_host_socket,
                                )
                                .unwrap_or_else(|err| {
                                    // info!("{}", Info(format!("failed to send storage transaction: {:?}", err).to_string()))
                                    println!("{}",
                                        printLn(
                                            format!("failed to send storage transaction: {:?}", err).to_string(),
                                            module_path!().to_string()
                                        )
                                    );
                                });
                            }
                            Err(e) => match e {
                                RecvTimeoutError::Disconnected => break,
                                RecvTimeoutError::Timeout => (),
                            },
                        };

                        if exit.load(Ordering::Relaxed) {
                            break;
                        }
                        sleep(Duration::from_millis(100));
                    }
                })
                .unwrap()
        };

        StoragePhase {
            t_storage_mining_verifier,
            t_storage_create_accounts,
        }
    }

    fn send_transaction(
        treasury_forks: &Arc<RwLock<TreasuryForks>>,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        instruction: OpCode,
        keypair: &Arc<Keypair>,
        storage_keypair: &Arc<Keypair>,
        account_host_socket: &UdpSocket,
    ) -> io::Result<()> {
        let working_treasury = treasury_forks.read().unwrap().working_treasury();
        let transaction_seal = working_treasury.confirmed_last_transaction_seal();
        let keypair_balance = working_treasury.get_balance(&keypair.address());

        if keypair_balance == 0 {
            // warn!("keypair account balance empty: {}", keypair.address(),);
            println!(
                "{}",
                Warn(
                    format!("keypair account balance empty: {}", keypair.address()).to_string(),
                    module_path!().to_string()
                )
            );
        } else {
            debug!(
                "keypair account balance: {}: {}",
                keypair.address(),
                keypair_balance
            );
        }
        if working_treasury
            .get_account(&storage_keypair.address())
            .is_none()
        {
            // warn!(
            //     "storage account does not exist: {}",
            //     storage_keypair.address()
            // );
            println!(
                "{}",
                Warn(
                    format!("storage account does not exist: {}",
                        storage_keypair.address()).to_string(),
                    module_path!().to_string()
                )
            );
        }

        let signer_keys = vec![keypair.as_ref(), storage_keypair.as_ref()];
        let context = Context::new_with_payer(vec![instruction], Some(&signer_keys[0].address()));
        let transaction = Transaction::new(&signer_keys, context, transaction_seal);

        account_host_socket.send_to(
            &bincode::serialize(&transaction).unwrap(),
            node_group_info.read().unwrap().my_data().transaction_digesting_module,
        )?;
        Ok(())
    }

    fn process_entry_crossing(
        storage_keypair: &Arc<Keypair>,
        state: &Arc<RwLock<StorageStateInner>>,
        _block_buffer_pool: &Arc<BlockBufferPool>,
        entry_id: Hash,
        slot: u64,
        instruction_sndr: &OpCodeSender,
    ) -> Result<()> {
        let mut seed = [0u8; 32];
        let signature = storage_keypair.sign(&entry_id.as_ref());

        let ix = storage_opcode::brdcst_last_tx_seal(
            &storage_keypair.address(),
            entry_id,
            slot,
        );
        instruction_sndr.send(ix)?;

        seed.copy_from_slice(&signature.to_bytes()[..32]);

        let mut rng = ChaChaRng::from_seed(seed);

        state.write().unwrap().slot = slot;

        // Regenerate the answers
        let num_segments = get_segment_from_slot(slot) as usize;
        if num_segments == 0 {
            // info!("{}", Info(format!("Ledger has 0 segments!").to_string()));
            println!("{}",
                printLn(
                    format!("Ledger has 0 segments!").to_string(),
                    module_path!().to_string()
                )
            );
            return Ok(());
        }
        // TODO: what if the validator does not have this segment
        let segment = signature.to_bytes()[0] as usize % num_segments;

        debug!(
            "storage verifying: segment: {} identities: {}",
            segment, NUM_IDENTITIES,
        );

        let mut samples = vec![];
        for _ in 0..NUM_STORAGE_SAMPLES {
            samples.push(rng.gen_range(0, 10));
        }
        debug!("generated samples: {:?}", samples);
        // TODO: cuda required to generate the reference values
        // but if it is missing, then we need to take care not to
        // process storage mining results.
        #[cfg(all(feature = "chacha", feature = "cuda"))]
        {
            // Lock the keys, since this is the IV memory,
            // it will be updated in-place by the encryption.
            // Should be overwritten by the proof signatures which replace the
            // key values by the time it runs again.

            let mut statew = state.write().unwrap();

            match chacha_cbc_encrypt_file_many_keys(
                _block_buffer_pool,
                segment as u64,
                &mut statew.storage_keys,
                &samples,
            ) {
                Ok(hashes) => {
                    debug!("Success! encrypted ledger segment: {}", segment);
                    statew.storage_results.copy_from_slice(&hashes);
                }
                Err(e) => {
                    // info!("{}", Info(format!("error encrypting file: {:?}", e).to_string()));
                    println!("{}",
                        printLn(
                            format!("error encrypting file: {:?}", e).to_string(),
                            module_path!().to_string()
                        )
                    );
                    Err(e)?;
                }
            }
        }
        Ok(())
    }

    fn process_storage_transaction(
        data: &[u8],
        slot: u64,
        storage_state: &Arc<RwLock<StorageStateInner>>,
        current_key_idx: &mut usize,
        storage_account_key: BvmAddr,
    ) {
        match deserialize(data) {
            Ok(PocOpCode::SetPocSig {
                slot: proof_slot,
                signature,
                sha_state,
            }) => {
                if proof_slot < slot {
                    {
                        debug!(
                            "generating storage_keys from storage txs current_key_idx: {}",
                            *current_key_idx
                        );
                        let storage_keys = &mut storage_state.write().unwrap().storage_keys;
                        storage_keys[*current_key_idx..*current_key_idx + size_of::<Signature>()]
                            .copy_from_slice(signature.as_ref());
                        *current_key_idx += size_of::<Signature>();
                        *current_key_idx %= storage_keys.len();
                    }

                    let mut statew = storage_state.write().unwrap();
                    let max_segment_index = get_segment_from_slot(slot) as usize;
                    if statew.storage_miner_map.len() < max_segment_index {
                        statew
                            .storage_miner_map
                            .resize(max_segment_index, HashMap::new());
                    }
                    let proof_segment_index = get_segment_from_slot(proof_slot) as usize;
                    if proof_segment_index < statew.storage_miner_map.len() {
                        // Copy the submitted proof
                        statew.storage_miner_map[proof_segment_index]
                            .entry(storage_account_key)
                            .or_default()
                            .push(PocSig {
                                signature,
                                sha_state,
                            });
                    }
                }
                debug!("storage proof: slot: {}", slot);
            }
            Ok(_) => {}
            Err(e) => {
                // info!("{}", Info(format!("error: {:?}", e).to_string()));
                println!("{}",
                    printLn(
                        format!("error: {:?}", e).to_string(),
                        module_path!().to_string()
                    )
                );
            }
        }
    }

    fn handle_fiscal_stmts(
        storage_keypair: &Arc<Keypair>,
        storage_state: &Arc<RwLock<StorageStateInner>>,
        slot_rcvr: &Receiver<Vec<u64>>,
        block_buffer_pool: &Arc<BlockBufferPool>,
        slot_count: &mut u64,
        last_root: &mut u64,
        current_key_idx: &mut usize,
        storage_rotate_count: u64,
        instruction_sndr: &OpCodeSender,
    ) -> Result<()> {
        let timeout = Duration::new(1, 0);
        let slots: Vec<u64> = slot_rcvr.recv_timeout(timeout)?;
        // check if any rooted slots were missed leading up to this one and bump slot count and process proofs for each missed root
        for slot in slots.into_iter().rev() {
            if slot > *last_root {
                *slot_count += 1;
                *last_root = slot;

                if let Ok(entries) = block_buffer_pool.fetch_candidate_fscl_stmts(slot, 0, None) {
                    for entry in &entries {
                        // Go through the transactions, find proofs, and use them to update
                        // the storage_keys with their signatures
                        for tx in &entry.transactions {
                            for instruction in tx.context.instructions.iter() {
                                let program_id =
                                    tx.context.account_keys[instruction.program_ids_index as usize];
                                if morgan_storage_api::pgm_id::check_id(&program_id) {
                                    let storage_account_key =
                                        tx.context.account_keys[instruction.accounts[0] as usize];
                                    Self::process_storage_transaction(
                                        &instruction.data,
                                        slot,
                                        storage_state,
                                        current_key_idx,
                                        storage_account_key,
                                    );
                                }
                            }
                        }
                    }
                    if *slot_count % storage_rotate_count == 0 {
                        // assume the last entry in the slot is the transaction_seal for that slot
                        let entry_hash = entries.last().unwrap().hash;
                        debug!(
                            "crosses sending at root slot: {}! with last entry's hash {}",
                            slot_count, entry_hash
                        );
                        Self::process_entry_crossing(
                            &storage_keypair,
                            &storage_state,
                            &block_buffer_pool,
                            entries.last().unwrap().hash,
                            slot,
                            instruction_sndr,
                        )?;
                        // bundle up mining submissions from miners
                        // and submit them in a tx to the leader to get rewarded.
                        let mut w_state = storage_state.write().unwrap();
                        let instructions: Vec<_> = w_state
                            .storage_miner_map
                            .iter_mut()
                            .enumerate()
                            .flat_map(|(segment, proof_map)| {
                                let checked_proofs = proof_map
                                    .iter_mut()
                                    .map(|(id, proofs)| {
                                        (
                                            *id,
                                            proofs
                                                .drain(..)
                                                .map(|poc_sig| VeriPocSig {
                                                    poc_sig,
                                                    status: PocSeal::Good,
                                                })
                                                .collect::<Vec<_>>(),
                                        )
                                    })
                                    .collect::<HashMap<_, _>>();
                                if !checked_proofs.is_empty() {
                                    let ix = verify_poc_sig(
                                        &storage_keypair.address(),
                                        segment as u64,
                                        checked_proofs,
                                    );
                                    Some(ix)
                                } else {
                                    None
                                }
                            })
                            .collect();
                        // TODO Avoid AccountInUse errors in this loop
                        let res: std::result::Result<_, _> = instructions
                            .into_iter()
                            .map(|ix| instruction_sndr.send(ix))
                            .collect();
                        res?
                    }
                }
            }
        }
        Ok(())
    }
}

impl Service for StoragePhase {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.t_storage_create_accounts.join().unwrap();
        self.t_storage_mining_verifier.join()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_buffer_pool::{create_new_tmp_ledger, BlockBufferPool};
    use crate::node_group_info::NodeGroupInfo;
    use crate::connection_info::ContactInfo;
    use crate::fiscal_statement_info::{compose_s_fiscal_stmt_nohash, FsclStmt};
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use crate::service::Service;
    use rayon::prelude::*;
    use morgan_runtime::treasury::Treasury;
    use morgan_interface::hash::{Hash, Hasher};
    use morgan_interface::bvm_address::BvmAddr;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_storage_api::pgm_id::SLOTS_PER_SEGMENT;
    use std::cmp::{max, min};
    use std::fs::remove_dir_all;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::{Arc, RwLock};
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_storage_phase_none_ledger() {
        let keypair = Arc::new(Keypair::new());
        let storage_keypair = Arc::new(Keypair::new());
        let exit = Arc::new(AtomicBool::new(false));

        let node_group_info = test_node_group_info(&keypair.address());
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(1000);
        let treasury = Arc::new(Treasury::new(&genesis_block));
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new_from_treasuries(&[treasury], 0)));
        let (_slot_sender, slot_rcvr) = channel();
        let storage_state = StorageState::new();
        let storage_stage = StoragePhase::new(
            &storage_state,
            slot_rcvr,
            None,
            &keypair,
            &storage_keypair,
            &exit.clone(),
            &treasury_forks,
            STORAGE_ROTATE_TEST_COUNT,
            &node_group_info,
        );
        exit.store(true, Ordering::Relaxed);
        storage_stage.join().unwrap();
    }

    fn test_node_group_info(id: &BvmAddr) -> Arc<RwLock<NodeGroupInfo>> {
        let contact_info = ContactInfo::new_localhost(id, 0);
        let node_group_info = NodeGroupInfo::new_with_invalid_keypair(contact_info);
        Arc::new(RwLock::new(node_group_info))
    }

    #[test]
    fn test_storage_phase_process_entries() {
        morgan_logger::setup();
        let keypair = Arc::new(Keypair::new());
        let storage_keypair = Arc::new(Keypair::new());
        let exit = Arc::new(AtomicBool::new(false));

        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(1000);
        let drops_per_slot = genesis_block.drops_per_slot;
        let (ledger_path, _transaction_seal) = create_new_tmp_ledger!(&genesis_block);

        let entries = compose_s_fiscal_stmt_nohash(64);
        let block_buffer_pool = Arc::new(BlockBufferPool::open_ledger_file(&ledger_path).unwrap());
        let slot = 1;
        let treasury = Arc::new(Treasury::new(&genesis_block));
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new_from_treasuries(&[treasury], 0)));
        block_buffer_pool
            .update_fscl_stmts(slot, 0, 0, drops_per_slot, &entries)
            .unwrap();

        let node_group_info = test_node_group_info(&keypair.address());

        let (slot_sender, slot_rcvr) = channel();
        let storage_state = StorageState::new();
        let storage_stage = StoragePhase::new(
            &storage_state,
            slot_rcvr,
            Some(block_buffer_pool.clone()),
            &keypair,
            &storage_keypair,
            &exit.clone(),
            &treasury_forks,
            STORAGE_ROTATE_TEST_COUNT,
            &node_group_info,
        );
        slot_sender.send(vec![slot]).unwrap();

        let keypair = Keypair::new();
        let hash = Hash::default();
        let signature = keypair.sign_context(&hash.as_ref());
        let mut result = storage_state.get_mining_result(&signature);
        assert_eq!(result, Hash::default());

        let rooted_slots = (slot..slot + SLOTS_PER_SEGMENT + 1)
            .map(|i| {
                block_buffer_pool
                    .update_fscl_stmts(i, 0, 0, drops_per_slot, &entries)
                    .unwrap();
                i
            })
            .collect::<Vec<_>>();
        slot_sender.send(rooted_slots).unwrap();
        for _ in 0..5 {
            result = storage_state.get_mining_result(&signature);
            if result != Hash::default() {
                // info!("{}", Info(format!("found result = {:?} sleeping..", result).to_string()));
                println!("{}",
                    printLn(
                        format!("found result = {:?} sleeping..", result).to_string(),
                        module_path!().to_string()
                    )
                );
                break;
            }
            // info!("{}", Info(format!("result = {:?} sleeping..", result).to_string()));
            println!("{}",
                printLn(
                    format!("result = {:?} sleeping..", result).to_string(),
                    module_path!().to_string()
                )
            );
            sleep(Duration::new(1, 0));
        }

        // info!("{}", Info(format!("joining..?").to_string()));
        println!("{}",
            printLn(
                format!("joining..?").to_string(),
                module_path!().to_string()
            )
        );
        exit.store(true, Ordering::Relaxed);
        storage_stage.join().unwrap();

        #[cfg(not(all(feature = "cuda", feature = "chacha")))]
        assert_eq!(result, Hash::default());

        #[cfg(all(feature = "cuda", feature = "chacha"))]
        assert_ne!(result, Hash::default());

        remove_dir_all(ledger_path).unwrap();
    }

    #[test]
    fn test_storage_phase_process_proof_entries() {
        morgan_logger::setup();
        let keypair = Arc::new(Keypair::new());
        let storage_keypair = Arc::new(Keypair::new());
        let exit = Arc::new(AtomicBool::new(false));

        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(1000);
        let drops_per_slot = genesis_block.drops_per_slot;;
        let (ledger_path, _transaction_seal) = create_new_tmp_ledger!(&genesis_block);

        let entries = compose_s_fiscal_stmt_nohash(128);
        let block_buffer_pool = Arc::new(BlockBufferPool::open_ledger_file(&ledger_path).unwrap());
        block_buffer_pool
            .update_fscl_stmts(1, 0, 0, drops_per_slot, &entries)
            .unwrap();
        let treasury = Arc::new(Treasury::new(&genesis_block));
        let treasury_forks = Arc::new(RwLock::new(TreasuryForks::new_from_treasuries(&[treasury], 0)));
        let node_group_info = test_node_group_info(&keypair.address());

        let (slot_sender, slot_rcvr) = channel();
        let storage_state = StorageState::new();
        let storage_stage = StoragePhase::new(
            &storage_state,
            slot_rcvr,
            Some(block_buffer_pool.clone()),
            &keypair,
            &storage_keypair,
            &exit.clone(),
            &treasury_forks,
            STORAGE_ROTATE_TEST_COUNT,
            &node_group_info,
        );
        slot_sender.send(vec![1]).unwrap();

        let mut reference_keys;
        {
            let keys = &storage_state.state.read().unwrap().storage_keys;
            reference_keys = vec![0; keys.len()];
            reference_keys.copy_from_slice(keys);
        }

        let keypair = Keypair::new();
        let mining_proof_ix = storage_opcode::poc_signature(
            &keypair.address(),
            Hash::default(),
            0,
            keypair.sign_context(b"test"),
        );
        let mining_proof_tx = Transaction::new_u_opcodes(vec![mining_proof_ix]);
        let mining_txs = vec![mining_proof_tx];

        let proof_entries = vec![FsclStmt::new(&Hash::default(), 1, mining_txs)];
        block_buffer_pool
            .update_fscl_stmts(2, 0, 0, drops_per_slot, &proof_entries)
            .unwrap();
        slot_sender.send(vec![2]).unwrap();

        for _ in 0..5 {
            {
                let keys = &storage_state.state.read().unwrap().storage_keys;
                if keys[..] != *reference_keys.as_slice() {
                    break;
                }
            }

            sleep(Duration::new(1, 0));
        }

        debug!("joining..?");
        exit.store(true, Ordering::Relaxed);
        storage_stage.join().unwrap();

        {
            let keys = &storage_state.state.read().unwrap().storage_keys;
            assert_ne!(keys[..], *reference_keys);
        }

        remove_dir_all(ledger_path).unwrap();
    }

    #[test]
    fn test_signature_distribution() {
        // See that signatures have an even-ish distribution..
        let mut hist = Arc::new(vec![]);
        for _ in 0..NUM_IDENTITIES {
            Arc::get_mut(&mut hist).unwrap().push(AtomicUsize::new(0));
        }
        let hasher = Hasher::default();
        {
            let hist = hist.clone();
            (0..(32 * NUM_IDENTITIES))
                .into_par_iter()
                .for_each(move |_| {
                    let keypair = Keypair::new();
                    let hash = hasher.clone().result();
                    let signature = keypair.sign_context(&hash.as_ref());
                    let ix = get_identity_index_from_signature(&signature);
                    hist[ix].fetch_add(1, Ordering::Relaxed);
                });
        }

        let mut hist_max = 0;
        let mut hist_min = NUM_IDENTITIES;
        for x in hist.iter() {
            let val = x.load(Ordering::Relaxed);
            hist_max = max(val, hist_max);
            hist_min = min(val, hist_min);
        }
        // info!("{}", Info(format!("min: {} max: {}", hist_min, hist_max).to_string()));
        println!("{}",
            printLn(
                format!("min: {} max: {}", hist_min, hist_max).to_string(),
                module_path!().to_string()
            )
        );
        assert_ne!(hist_min, 0);
    }
}
