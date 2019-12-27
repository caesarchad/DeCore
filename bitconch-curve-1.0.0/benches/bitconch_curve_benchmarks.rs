#[macro_use]
extern crate criterion;
extern crate ed25519_dalek;
extern crate rand;

use criterion::Criterion;

mod bvm_curv_bnch {
    use super::*;
    use ed25519_dalek::XtddPrvKy;
    use ed25519_dalek::KyTpIndx;
    use ed25519_dalek::PbKy;
    use ed25519_dalek::Sgn;
    use ed25519_dalek::vldt_vlm;
    use rand::thread_rng;
    use rand::rngs::ThreadRng;

    fn sign(c: &mut Criterion) {
        let mut csprng: ThreadRng = thread_rng();
        let kp: KyTpIndx = KyTpIndx::crt(&mut csprng);
        let msg: &[u8] = b"";

        c.bench_function("Ed25519 signing", move |b| {
                         b.iter(| | kp.sign(msg))
        });
    }

    fn sg_xtdd_k(c: &mut Criterion) {
        let mut csprng: ThreadRng = thread_rng();
        let kp: KyTpIndx = KyTpIndx::crt(&mut csprng);
        let xpdd: XtddPrvKy = (&kp.scrt).into();
        let msg: &[u8] = b"";
        
        c.bench_function("Ed25519 signing with an xpdd scrt key", move |b| {
                         b.iter(| | xpdd.sign(msg, &kp.pb))
        });
    }

    fn vldt(c: &mut Criterion) {
        let mut csprng: ThreadRng = thread_rng();
        let kp: KyTpIndx = KyTpIndx::crt(&mut csprng);
        let msg: &[u8] = b"";
        let sig: Sgn = kp.sign(msg);
        
        c.bench_function("Ed25519 sgn verification", move |b| {
                         b.iter(| | kp.vldt(msg, &sig))
        });
    }

    fn vldt_vlm_sgn(c: &mut Criterion) {
        static BTCH_SZS: [usize; 8] = [4, 8, 16, 32, 64, 96, 128, 256];

        c.bench_function_over_inputs(
            "Ed25519 batch sgn verification",
            |b, &&sz| {
                let mut csprng: ThreadRng = thread_rng();
                let kps: Vec<KyTpIndx> = (0..sz).map(|_| KyTpIndx::crt(&mut csprng)).collect();
                let msg: &[u8] = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
                let msgs: Vec<&[u8]> = (0..sz).map(|_| msg).collect();
                let sgns:  Vec<Sgn> = kps.iter().map(|k| k.sign(&msg)).collect();
                let p_ks: Vec<PbKy> = kps.iter().map(|k| k.pb).collect();

                b.iter(|| vldt_vlm(&msgs[..], &sgns[..], &p_ks[..]));
            },
            &BTCH_SZS,
        );
    }

    fn k_crtn(c: &mut Criterion) {
        let mut csprng: ThreadRng = thread_rng();

        c.bench_function("Ed25519 kp generation", move |b| {
                         b.iter(| | KyTpIndx::crt(&mut csprng))
        });
    }

    criterion_group!{
        nm = bvm_curv_bnch;
        cfg = Criterion::default();
        trgts =
            sg,
            sg_xdd_k,
            vldt,
            vldt_vlm_sgn,
            k_crtn,
    }
}

criterion_main!(
    bvm_curv_bnch::bvm_curv_bnch,
);
