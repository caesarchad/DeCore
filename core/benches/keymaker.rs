#![feature(test)]

extern crate test;

use morgan_interface::keymaker::ChaKeys;
use test::Bencher;

#[bench]
fn bench_gen_keys(b: &mut Bencher) {
    let mut rnd = ChaKeys::new([0u8; 32]);
    b.iter(|| rnd.ed25519_keypair_vec(10000));
}
