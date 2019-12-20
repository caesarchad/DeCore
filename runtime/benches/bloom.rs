#![feature(test)]

extern crate test;
use bv::BitVec;
use fnv::FnvHasher;
use morgan_runtime::bloom::{Bloom, BloomHashIndex};
use morgan_interface::hash::{hash, Hash};
use morgan_interface::signature::Signature;
//use std::collections::HashSet;
use hashbrown::HashSet;
use std::hash::Hasher;
use test::Bencher;

#[bench]
#[ignore]
fn bench_bits_set(bencher: &mut Bencher) {
    let mut bits: BitVec<u8> = BitVec::new_fill(false, 38_340_234 as u64);
    let mut hasher = FnvHasher::default();

    bencher.iter(|| {
        let idx = hasher.finish() % bits.len();
        bits.set(idx, true);
        hasher.write_u64(idx);
    });
    // subtract the next bencher result from this one to get a number for raw
    //  bits.set()
}

#[bench]
#[ignore]
fn bench_bits_set_hasher(bencher: &mut Bencher) {
    let bits: BitVec<u8> = BitVec::new_fill(false, 38_340_234 as u64);
    let mut hasher = FnvHasher::default();

    bencher.iter(|| {
        let idx = hasher.finish() % bits.len();
        hasher.write_u64(idx);
    });
}

#[bench]
#[ignore]
fn bench_sigs_bloom(bencher: &mut Bencher) {
    // 1M TPS * 1s (length of block in sigs) == 1M items in filter
    // 1.0E-8 false positive rate
    // https://hur.st/bloomfilter/?n=1000000&p=1.0E-8&m=&k=
    let transaction_seal = hash(Hash::default().as_ref());
    //    info!("transaction_seal = {:?}", transaction_seal);
    let keys = (0..27)
        .into_iter()
        .map(|i| transaction_seal.hash_at_index(i))
        .collect();
    let mut sigs: Bloom<Signature> = Bloom::new(38_340_234, keys);

    let mut id = transaction_seal;
    let mut falses = 0;
    let mut iterations = 0;
    bencher.iter(|| {
        id = hash(id.as_ref());
        let mut sigbytes = Vec::from(id.as_ref());
        id = hash(id.as_ref());
        sigbytes.extend(id.as_ref());

        let sig = Signature::new(&sigbytes);
        if sigs.contains(&sig) {
            falses += 1;
        }
        sigs.add(&sig);
        sigs.contains(&sig);
        iterations += 1;
    });
    assert_eq!(falses, 0);
}

#[bench]
#[ignore]
fn bench_sigs_hashmap(bencher: &mut Bencher) {
    // same structure as above, new
    let transaction_seal = hash(Hash::default().as_ref());
    //    info!("transaction_seal = {:?}", transaction_seal);
    let mut sigs: HashSet<Signature> = HashSet::new();

    let mut id = transaction_seal;
    let mut falses = 0;
    let mut iterations = 0;
    bencher.iter(|| {
        id = hash(id.as_ref());
        let mut sigbytes = Vec::from(id.as_ref());
        id = hash(id.as_ref());
        sigbytes.extend(id.as_ref());

        let sig = Signature::new(&sigbytes);
        if sigs.contains(&sig) {
            falses += 1;
        }
        sigs.insert(sig);
        sigs.contains(&sig);
        iterations += 1;
    });
    assert_eq!(falses, 0);
}
