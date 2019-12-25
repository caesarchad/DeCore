//! The `entry` module is a fundamental building block of Proof of History. It contains a
//! unique ID that is the hash of the FsclStmt before it, plus the hash of the
//! transactions within it. Entries cannot be reordered, and its field `num_hashes`
//! represents an approximate amount of time since the last FsclStmt was created.
use crate::packet::{Blob, SharedBlob, BLOB_DATA_SIZE};
use crate::water_clock::WaterClock;
use crate::result::Result;
use bincode::{deserialize, serialized_size};
use chrono::prelude::Utc;
use rayon::prelude::*;
use morgan_budget_api::sc_opcode;
use morgan_interface::hash::{Hash, Hasher};
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::transaction::Transaction;
use std::borrow::Borrow;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};
use morgan_helper::logHelper::*;

pub type FsclStmtSndr = Sender<Vec<FsclStmt>>;
pub type FsclStmtRcvr = Receiver<Vec<FsclStmt>>;



#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct FsclStmt {
    
    pub num_hashes: u64,

    //pub //mark_seal:u64,
    
    pub hash: Hash,

    pub transactions: Vec<Transaction>,
}

impl FsclStmt {
    /// Creates the next FsclStmt `num_hashes` after `start_hash`.
    pub fn new(prev_hash: &Hash, num_hashes: u64, transactions: Vec<Transaction>) -> Self {
        assert!(Self::serialized_to_blob_size(&transactions) <= BLOB_DATA_SIZE as u64);

        if num_hashes == 0 && transactions.is_empty() {
            FsclStmt {
                num_hashes: 0,
                ////mark_seal: 19,
                hash: *prev_hash,
                transactions,
            }
        } else if num_hashes == 0 {
            // If you passed in transactions, but passed in num_hashes == 0, then
            // next_hash will generate the next hash and set num_hashes == 1
            let hash = next_hash(prev_hash, 1, &transactions);
            FsclStmt {
                num_hashes: 1,
                //mark_seal: 19,
                hash,
                transactions,
            }
        } else {
            // Otherwise, the next FsclStmt `num_hashes` after `start_hash`.
            // If you wanted a _drop for instance, then pass in num_hashes = 1
            // and transactions = empty
            let hash = next_hash(prev_hash, num_hashes, &transactions);
            FsclStmt {
                num_hashes,
                //mark_seal: 19,
                hash,
                transactions,
            }
        }
    }

    pub fn to_shared_blob(&self) -> SharedBlob {
        let blob = self.to_blob();
        Arc::new(RwLock::new(blob))
    }

    pub fn to_blob(&self) -> Blob {
        Blob::from_serializable(&vec![&self])
    }

    
    pub fn serialized_to_blob_size(transactions: &[Transaction]) -> u64 {
        let txs_size: u64 = transactions
            .iter()
            .map(|tx| serialized_size(tx).unwrap())
            .sum();

        serialized_size(&vec![FsclStmt {
            num_hashes: 0,
            //mark_seal: 19,
            hash: Hash::default(),
            transactions: vec![],
        }])
        .unwrap()
            + txs_size
    }

    pub fn new_mut(
        start_hash: &mut Hash,
        num_hashes: &mut u64,
        transactions: Vec<Transaction>,
    ) -> Self {
        assert!(Self::serialized_to_blob_size(&transactions) <= BLOB_DATA_SIZE as u64);

        let entry = Self::new(start_hash, *num_hashes, transactions);
        *start_hash = entry.hash;
        *num_hashes = 0;

        entry
    }

    #[cfg(test)]
    pub fn new_drop(num_hashes: u64, hash: &Hash) -> Self {
        FsclStmt {
            num_hashes,
            //mark_seal: 19,
            hash: *hash,
            transactions: vec![],
        }
    }

    pub fn verify(&self, start_hash: &Hash) -> bool {
        let ref_hash = next_hash(start_hash, self.num_hashes, &self.transactions);
        if self.hash != ref_hash {
            println!(
                "{}",
                Warn(
                    format!("next_hash is invalid expected: {:?} actual: {:?}",
                        self.hash, ref_hash).to_string(),
                    module_path!().to_string()
                )
            );
            return false;
        }
        true
    }

    pub fn is_drop(&self) -> bool {
        self.transactions.is_empty()
    }
}

pub fn seal_block(transactions: &[Transaction]) -> Hash {
    let mut hasher = Hasher::default();
    transactions.iter().for_each(|tx| {
        if !tx.signatures.is_empty() {
            hasher.hash(&tx.signatures[0].as_ref());
        }
    });
    hasher.result()
}

fn next_hash(start_hash: &Hash, num_hashes: u64, transactions: &[Transaction]) -> Hash {
    if num_hashes == 0 && transactions.is_empty() {
        return *start_hash;
    }

    let mut waterclock = WaterClock::new(*start_hash, None);
    waterclock.hash(num_hashes.saturating_sub(1));
    if transactions.is_empty() {
        waterclock._drop().unwrap().hash
    } else {
        waterclock.record(seal_block(transactions)).unwrap().hash
    }
}

pub fn restore_fscl_stmt_by_data<I>(blobs: I) -> Result<(Vec<FsclStmt>, u64)>
where
    I: IntoIterator,
    I::Item: Borrow<Blob>,
{
    let mut entries: Vec<FsclStmt> = vec![];
    let mut num_drops = 0;

    for blob in blobs.into_iter() {
        let new_entries: Vec<FsclStmt> = {
            let msg_size = blob.borrow().size();
            deserialize(&blob.borrow().data()[..msg_size])?
        };

        let num_new_drops: u64 = new_entries.iter().map(|entry| entry.is_drop() as u64).sum();
        num_drops += num_new_drops;
        entries.extend(new_entries)
    }
    Ok((entries, num_drops))
}


pub trait  FsclStmtSlc  {
    fn verify(&self, start_hash: &Hash) -> bool;
    fn to_shared_blobs(&self) -> Vec<SharedBlob>;
    fn to_blobs(&self) -> Vec<Blob>;
    fn one_stmt_blbs(&self) -> Vec<Blob>;
    fn one_stmt_shard_blbs(&self) -> Vec<SharedBlob>;
}

impl FsclStmtSlc for [FsclStmt] {
    fn verify(&self, start_hash: &Hash) -> bool {
        let genesis = [FsclStmt {
            num_hashes: 0,
            //mark_seal: 19,
            hash: *start_hash,
            transactions: vec![],
        }];
        let entry_pairs = genesis.par_iter().chain(self).zip(self);
        entry_pairs.all(|(x0, x1)| {
            let r = x1.verify(&x0.hash);
            if !r {
                println!(
                    "{}",
                    Warn(
                        format!("entry invalid!: x0: {:?}, x1: {:?} num txs: {}",
                            x0.hash,
                            x1.hash,
                            x1.transactions.len()).to_string(),
                        module_path!().to_string()
                    )
                );
            }
            r
        })
    }

    fn to_blobs(&self) -> Vec<Blob> {
        slash_fscl_chnks(
            &self,
            BLOB_DATA_SIZE as u64,
            &|s| bincode::serialized_size(&s).unwrap(),
            &mut |entries: &[FsclStmt]| Blob::from_serializable(entries),
        )
    }

    fn to_shared_blobs(&self) -> Vec<SharedBlob> {
        self.to_blobs()
            .into_iter()
            .map(|b| Arc::new(RwLock::new(b)))
            .collect()
    }

    fn one_stmt_shard_blbs(&self) -> Vec<SharedBlob> {
        self.one_stmt_blbs()
            .into_iter()
            .map(|b| Arc::new(RwLock::new(b)))
            .collect()
    }

    fn one_stmt_blbs(&self) -> Vec<Blob> {
        self.iter().map(FsclStmt::to_blob).collect()
    }
}

pub fn next_entry_mut(start: &mut Hash, num_hashes: u64, transactions: Vec<Transaction>) -> FsclStmt {
    let entry = FsclStmt::new(&start, num_hashes, transactions);
    *start = entry.hash;
    entry
}

pub fn is_bound<T, F>(serializables: &[T], max_size: u64, serialized_size: &F) -> usize
where
    F: Fn(&[T]) -> u64,
{
    if serializables.is_empty() {
        return 0;
    }
    let mut num = serializables.len();
    let mut upper = serializables.len();
    let mut lower = 1; 
    loop {
        let next;
        if serialized_size(&serializables[..num]) <= max_size {
            next = (upper + num) / 2;
            lower = num;
        } else {
            if num == 1 {
                num = 0;
                break;
            }
            next = (lower + num) / 2;
            upper = num;
        }
        if next == num {
            break;
        }
        num = next;
    }
    num
}

pub fn slash_fscl_chnks<T, R, F1, F2>(
    serializables: &[T],
    max_size: u64,
    serialized_size: &F1,
    converter: &mut F2,
) -> Vec<R>
where
    F1: Fn(&[T]) -> u64,
    F2: FnMut(&[T]) -> R,
{
    let mut result = vec![];
    let mut chunk_start = 0;
    while chunk_start < serializables.len() {
        let chunk_end =
            chunk_start + is_bound(&serializables[chunk_start..], max_size, serialized_size);
        result.push(converter(&serializables[chunk_start..chunk_end]));
        chunk_start = chunk_end;
    }

    result
}

fn nxt_fscl_stmt_mutable(
    start_hash: &mut Hash,
    num_hashes: &mut u64,
    transactions: Vec<Transaction>,
) -> Vec<FsclStmt> {
    slash_fscl_chnks(
        &transactions[..],
        BLOB_DATA_SIZE as u64,
        &FsclStmt::serialized_to_blob_size,
        &mut |txs: &[Transaction]| FsclStmt::new_mut(start_hash, num_hashes, txs.to_vec()),
    )
}

pub fn next_fiscal_statment(
    start_hash: &Hash,
    num_hashes: u64,
    transactions: Vec<Transaction>,
) -> Vec<FsclStmt> {
    let mut hash = *start_hash;
    let mut num_hashes = num_hashes;
    nxt_fscl_stmt_mutable(&mut hash, &mut num_hashes, transactions)
}

pub fn create_drops(num_drops: u64, mut hash: Hash) -> Vec<FsclStmt> {
    let mut drops = Vec::with_capacity(num_drops as usize);
    for _ in 0..num_drops {
        let new_drop = next_entry_mut(&mut hash, 1, vec![]);
        drops.push(new_drop);
    }

    drops
}

pub fn compose_s_fiscal_stmt(start: &Hash, num: usize) -> Vec<FsclStmt> {
    let keypair = Keypair::new();
    let pubkey = keypair.pubkey();

    let mut hash = *start;
    let mut num_hashes = 0;
    (0..num)
        .map(|_| {
            let ix = sc_opcode::apply_timestamp(&pubkey, &pubkey, &pubkey, Utc::now());
            let tx = Transaction::new_s_opcodes(&[&keypair], vec![ix], *start);
            FsclStmt::new_mut(&mut hash, &mut num_hashes, vec![tx])
        })
        .collect()
}

pub fn compose_s_fiscal_stmt_nohash(num: usize) -> Vec<FsclStmt> {
    let zero = Hash::default();
    let one = morgan_interface::hash::hash(&zero.as_ref());
    compose_s_fiscal_stmt(&one, num)
}

pub fn compose_b_fiscal_stmt(fscl_stmt_cnt: usize) -> Vec<FsclStmt> {
    let zero = Hash::default();
    let one = morgan_interface::hash::hash(&zero.as_ref());
    let keypair = Keypair::new();
    let pubkey = keypair.pubkey();

    let ix = sc_opcode::apply_timestamp(&pubkey, &pubkey, &pubkey, Utc::now());
    let tx = Transaction::new_s_opcodes(&[&keypair], vec![ix], one);

    let serialized_size = serialized_size(&tx).unwrap();
    let num_txs = BLOB_DATA_SIZE / serialized_size as usize;
    let txs = vec![tx; num_txs];
    let entry = next_fiscal_statment(&one, 1, txs)[0].clone();
    vec![entry; fscl_stmt_cnt]
}

#[cfg(test)]
pub fn make_consecutive_blobs(
    id: &morgan_interface::pubkey::Pubkey,
    num_blobs_to_make: u64,
    start_height: u64,
    start_hash: Hash,
    addr: &std::net::SocketAddr,
) -> Vec<SharedBlob> {
    let entries = create_drops(num_blobs_to_make, start_hash);

    let blobs = entries.one_stmt_shard_blbs();
    let mut index = start_height;
    for blob in &blobs {
        let mut blob = blob.write().unwrap();
        blob.set_index(index);
        blob.set_id(id);
        blob.meta.set_addr(addr);
        index += 1;
    }
    blobs
}

#[cfg(test)]
/// Creates the next Drop or Transaction FsclStmt `num_hashes` after `start_hash`.
pub fn next_entry(prev_hash: &Hash, num_hashes: u64, transactions: Vec<Transaction>) -> FsclStmt {
    assert!(num_hashes > 0 || transactions.is_empty());
    FsclStmt {
        num_hashes,
        //mark_seal: 19,
        hash: next_hash(prev_hash, num_hashes, &transactions),
        transactions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fiscal_statement_info::FsclStmt;
    use crate::packet::{to_blobs, BLOB_DATA_SIZE};
    use morgan_interface::constants::PACKET_DATA_SIZE;
    use morgan_interface::hash::hash;
    use morgan_interface::opcodes::OpCode;
    use morgan_interface::pubkey::Pubkey;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::system_transaction;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn create_sample_payment(keypair: &Keypair, hash: Hash) -> Transaction {
        let pubkey = keypair.pubkey();
        let ixs = sc_opcode::payment(&pubkey, &pubkey, 1);
        Transaction::new_s_opcodes(&[keypair], ixs, hash)
    }

    fn create_sample_timestamp(keypair: &Keypair, hash: Hash) -> Transaction {
        let pubkey = keypair.pubkey();
        let ix = sc_opcode::apply_timestamp(&pubkey, &pubkey, &pubkey, Utc::now());
        Transaction::new_s_opcodes(&[keypair], vec![ix], hash)
    }

    fn create_sample_apply_signature(keypair: &Keypair, hash: Hash) -> Transaction {
        let pubkey = keypair.pubkey();
        let ix = sc_opcode::apply_signature(&pubkey, &pubkey, &pubkey);
        Transaction::new_s_opcodes(&[keypair], vec![ix], hash)
    }

    #[test]
    fn test_entry_verify() {
        let zero = Hash::default();
        let one = hash(&zero.as_ref());
        assert!(FsclStmt::new_drop(0, &zero).verify(&zero)); // base case, never used
        assert!(!FsclStmt::new_drop(0, &zero).verify(&one)); // base case, bad
        assert!(next_entry(&zero, 1, vec![]).verify(&zero)); // inductive step
        assert!(!next_entry(&zero, 1, vec![]).verify(&one)); // inductive step, bad
    }

    #[test]
    fn test_transaction_reorder_attack() {
        let zero = Hash::default();

        // First, verify entries
        let keypair = Keypair::new();
        let tx0 = system_transaction::create_user_account(&keypair, &keypair.pubkey(), 0, zero);
        let tx1 = system_transaction::create_user_account(&keypair, &keypair.pubkey(), 1, zero);
        let mut e0 = FsclStmt::new(&zero, 0, vec![tx0.clone(), tx1.clone()]);
        assert!(e0.verify(&zero));

        // Next, swap two transactions and ensure verification fails.
        e0.transactions[0] = tx1; // <-- attack
        e0.transactions[1] = tx0;
        assert!(!e0.verify(&zero));
    }

    #[test]
    fn test_witness_reorder_attack() {
        let zero = Hash::default();

        // First, verify entries
        let keypair = Keypair::new();
        let tx0 = create_sample_timestamp(&keypair, zero);
        let tx1 = create_sample_apply_signature(&keypair, zero);
        let mut e0 = FsclStmt::new(&zero, 0, vec![tx0.clone(), tx1.clone()]);
        assert!(e0.verify(&zero));

        // Next, swap two witness transactions and ensure verification fails.
        e0.transactions[0] = tx1; // <-- attack
        e0.transactions[1] = tx0;
        assert!(!e0.verify(&zero));
    }

    #[test]
    fn test_next_entry() {
        let zero = Hash::default();
        let _drop = next_entry(&zero, 1, vec![]);
        assert_eq!(_drop.num_hashes, 1);
        assert_ne!(_drop.hash, zero);

        let _drop = next_entry(&zero, 0, vec![]);
        assert_eq!(_drop.num_hashes, 0);
        assert_eq!(_drop.hash, zero);

        let keypair = Keypair::new();
        let tx0 = create_sample_timestamp(&keypair, zero);
        let entry0 = next_entry(&zero, 1, vec![tx0.clone()]);
        assert_eq!(entry0.num_hashes, 1);
        assert_eq!(entry0.hash, next_hash(&zero, 1, &vec![tx0]));
    }

    #[test]
    #[should_panic]
    fn test_next_entry_panic() {
        let zero = Hash::default();
        let keypair = Keypair::new();
        let tx = system_transaction::create_user_account(&keypair, &keypair.pubkey(), 0, zero);
        next_entry(&zero, 0, vec![tx]);
    }

    #[test]
    fn test_serialized_to_blob_size() {
        let zero = Hash::default();
        let keypair = Keypair::new();
        let tx = system_transaction::create_user_account(&keypair, &keypair.pubkey(), 0, zero);
        let entry = next_entry(&zero, 1, vec![tx.clone()]);
        assert_eq!(
            FsclStmt::serialized_to_blob_size(&[tx]),
            serialized_size(&vec![entry]).unwrap() // blobs are Vec<FsclStmt>
        );
    }

    #[test]
    fn test_verify_slice() {
        morgan_logger::setup();
        let zero = Hash::default();
        let one = hash(&zero.as_ref());
        assert!(vec![][..].verify(&zero)); // base case
        assert!(vec![FsclStmt::new_drop(0, &zero)][..].verify(&zero)); // singleton case 1
        assert!(!vec![FsclStmt::new_drop(0, &zero)][..].verify(&one)); // singleton case 2, bad
        assert!(vec![next_entry(&zero, 0, vec![]); 2][..].verify(&zero)); // inductive step

        let mut bad_drops = vec![next_entry(&zero, 0, vec![]); 2];
        bad_drops[1].hash = one;
        assert!(!bad_drops.verify(&zero)); // inductive step, bad
    }

    fn blob_sized_entries(fscl_stmt_cnt: usize) -> Vec<FsclStmt> {
        // rough guess
        let mut magic_len = BLOB_DATA_SIZE
            - serialized_size(&vec![FsclStmt {
                num_hashes: 0,
                //mark_seal: 19,
                hash: Hash::default(),
                transactions: vec![],
            }])
            .unwrap() as usize;

        loop {
            let entries = vec![FsclStmt {
                num_hashes: 0,
                //mark_seal: 19,
                hash: Hash::default(),
                transactions: vec![Transaction::new_u_opcodes(vec![
                    OpCode::new(Pubkey::default(), &vec![0u8; magic_len as usize], vec![]),
                ])],
            }];
            let size = serialized_size(&entries).unwrap() as usize;
            if size < BLOB_DATA_SIZE {
                magic_len += BLOB_DATA_SIZE - size;
            } else if size > BLOB_DATA_SIZE {
                magic_len -= size - BLOB_DATA_SIZE;
            } else {
                break;
            }
        }
        vec![
            FsclStmt {
                num_hashes: 0,
                //mark_seal: 19,
                hash: Hash::default(),
                transactions: vec![Transaction::new_u_opcodes(vec![
                    OpCode::new(Pubkey::default(), &vec![0u8; magic_len], vec![]),
                ])],
            };
            fscl_stmt_cnt
        ]
    }

    #[test]
    fn test_entries_to_blobs() {
        morgan_logger::setup();
        let entries = blob_sized_entries(10);

        let blobs = entries.to_blobs();
        for blob in &blobs {
            assert_eq!(blob.size(), BLOB_DATA_SIZE);
        }

        assert_eq!(restore_fscl_stmt_by_data(blobs).unwrap().0, entries);
    }

    #[test]
    fn test_multiple_entries_to_blobs() {
        morgan_logger::setup();
        let num_blobs = 10;
        let serialized_size =
            bincode::serialized_size(&compose_s_fiscal_stmt(&Hash::default(), 1))
                .unwrap();

        let fscl_stmt_cnt = (num_blobs * BLOB_DATA_SIZE as u64) / serialized_size;
        let entries = compose_s_fiscal_stmt(&Hash::default(), fscl_stmt_cnt as usize);

        let blob_q = entries.to_blobs();

        assert_eq!(blob_q.len() as u64, num_blobs);
        assert_eq!(restore_fscl_stmt_by_data(blob_q).unwrap().0, entries);
    }

    #[test]
    fn test_bad_blobs_attack() {
        morgan_logger::setup();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8000);
        let blobs_q = to_blobs(vec![(0, addr)]).unwrap(); // <-- attack!
        assert!(restore_fscl_stmt_by_data(blobs_q).is_err());
    }

    #[test]
    fn test_next_entries() {
        morgan_logger::setup();
        let hash = Hash::default();
        let next_hash = morgan_interface::hash::hash(&hash.as_ref());
        let keypair = Keypair::new();
        let tx_small = create_sample_timestamp(&keypair, next_hash);
        let tx_large = create_sample_payment(&keypair, next_hash);

        let tx_small_size = serialized_size(&tx_small).unwrap() as usize;
        let tx_large_size = serialized_size(&tx_large).unwrap() as usize;
        let entry_size = serialized_size(&FsclStmt {
            num_hashes: 0,
            //mark_seal: 19,
            hash: Hash::default(),
            transactions: vec![],
        })
        .unwrap() as usize;
        assert!(tx_small_size < tx_large_size);
        assert!(tx_large_size < PACKET_DATA_SIZE);

        let threshold = (BLOB_DATA_SIZE - entry_size) / tx_small_size;

        // verify no split
        let transactions = vec![tx_small.clone(); threshold];
        let entries0 = next_fiscal_statment(&hash, 0, transactions.clone());
        assert_eq!(entries0.len(), 1);
        assert!(entries0.verify(&hash));

        // verify the split with uniform transactions
        let transactions = vec![tx_small.clone(); threshold * 2];
        let entries0 = next_fiscal_statment(&hash, 0, transactions.clone());
        assert_eq!(entries0.len(), 2);
        assert!(entries0.verify(&hash));

        // verify the split with small transactions followed by large
        // transactions
        let mut transactions = vec![tx_small.clone(); BLOB_DATA_SIZE / tx_small_size];
        let large_transactions = vec![tx_large.clone(); BLOB_DATA_SIZE / tx_large_size];

        transactions.extend(large_transactions);

        let entries0 = next_fiscal_statment(&hash, 0, transactions.clone());
        assert!(entries0.len() >= 2);
        assert!(entries0.verify(&hash));
    }

    #[test]
    fn test_num_will_fit_empty() {
        let serializables: Vec<u32> = vec![];
        let result = is_bound(&serializables[..], 8, &|_| 4);
        assert_eq!(result, 0);
    }

    #[test]
    fn test_num_will_fit() {
        let serializables_vec: Vec<u8> = (0..10).map(|_| 1).collect();
        let serializables = &serializables_vec[..];
        let sum = |i: &[u8]| (0..i.len()).into_iter().sum::<usize>() as u64;
        // sum[0] is = 0, but sum[0..1] > 0, so result contains 1 item
        let result = is_bound(serializables, 0, &sum);
        assert_eq!(result, 1);

        // sum[0..3] is <= 8, but sum[0..4] > 8, so result contains 3 items
        let result = is_bound(serializables, 8, &sum);
        assert_eq!(result, 4);

        // sum[0..1] is = 1, but sum[0..2] > 0, so result contains 2 items
        let result = is_bound(serializables, 1, &sum);
        assert_eq!(result, 2);

        // sum[0..9] = 45, so contains all items
        let result = is_bound(serializables, 45, &sum);
        assert_eq!(result, 10);

        // sum[0..8] <= 44, but sum[0..9] = 45, so contains all but last item
        let result = is_bound(serializables, 44, &sum);
        assert_eq!(result, 9);

        // sum[0..9] <= 46, but contains all items
        let result = is_bound(serializables, 46, &sum);
        assert_eq!(result, 10);

        // too small to fit a single u64
        let result = is_bound(&[0u64], (std::mem::size_of::<u64>() - 1) as u64, &|i| {
            (std::mem::size_of::<u64>() * i.len()) as u64
        });
        assert_eq!(result, 0);
    }
}
