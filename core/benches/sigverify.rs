#![feature(test)]

extern crate test;

use morgan::packet::pkt_bndl;
use morgan::signature_verify;
use morgan::test_tx::test_tx;
use test::Bencher;

#[bench]
fn bench_sigverify(bencher: &mut Bencher) {
    let tx = test_tx();

    // generate packet vector
    let batches = pkt_bndl(&vec![tx; 128]);

    // verify packets
    bencher.iter(|| {
        let _ans = signature_verify::ed25519_verify(&batches);
    })
}
