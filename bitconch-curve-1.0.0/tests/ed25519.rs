#[cfg(all(test, feature = "serde"))]
extern crate bincode;
extern crate ed25519_dalek;
extern crate hex;
extern crate rand;
extern crate sha2;

use ed25519_dalek::*;

use hex::FromHex;

use rand::thread_rng;
use rand::rngs::ThreadRng;

use sha2::Sha512;

#[cfg(test)]
mod vectors {
    use std::io::BufReader;
    use std::io::BufRead;
    use std::fs::File;

    use super::*;

    #[test]
    fn agst_rfnc_impl() {
        let mut line: String;
        let mut lineno: usize = 0;

        let f = File::open("TESTVECTORS");
        if f.is_err() {
            println!("This test is only available when the code has been cloned \
                      from the git repository, since the TESTVECTORS file is large \
                      and is therefore not included within the distributed crate.");
            panic!();
        }
        let file = BufReader::new(f.unwrap());

        for l in file.lines() {
            lineno += 1;
            line = l.unwrap();

            let parts: Vec<&str> = line.split(':').collect();
            assert_eq!(parts.len(), 5, "wrong number of fields in line {}", lineno);

            let sec_bytes: Vec<u8> = FromHex::from_hex(&parts[0]).unwrap();
            let pub_bytes: Vec<u8> = FromHex::from_hex(&parts[1]).unwrap();
            let msg_bytes: Vec<u8> = FromHex::from_hex(&parts[2]).unwrap();
            let sig_bytes: Vec<u8> = FromHex::from_hex(&parts[3]).unwrap();

            let scrt: PvKy = PvKy::frm_octts(&sec_bytes[..SCRT_K_SZ]).unwrap();
            let pb: PbKy = PbKy::frm_octts(&pub_bytes[..PB_K_SZ]).unwrap();
            let kp: KyTpIndx  = KyTpIndx{ scrt: scrt, pb: pb };

            let sig1: Sgn = Sgn::frm_octts(&sig_bytes[..64]).unwrap();
            let sig2: Sgn = kp.sign(&msg_bytes);

            assert!(sig1 == sig2, "Sgn octets not equal on line {}", lineno);
            assert!(kp.vldt(&msg_bytes, &sig2).is_ok(),
                    "Sgn verification failed on line {}", lineno);
        }
    }

    #[test]
    fn cv_rf_tst_vctr() {
        let scrt_k: &[u8] = b"833fe62409237b9d62ec77587520911e9a759cec1d19755b7da901b96dca3d42";
        let p_k: &[u8] = b"ec172b93ad5e563bf4932c70e1245034c35467ef2efd4d64ebf819683467e2bf";
        let msg: &[u8] = b"616263";
        let sgn: &[u8] = b"98a70222f0b8121aa9d30f813d683f809e462b469c7ff87639499bb94e6dae4131f85042463c2a355a2003d062adf5aaa10b8c61e636062aaad11c2a26083406";

        let sec_bytes: Vec<u8> = FromHex::from_hex(scrt_k).unwrap();
        let pub_bytes: Vec<u8> = FromHex::from_hex(p_k).unwrap();
        let msg_bytes: Vec<u8> = FromHex::from_hex(msg).unwrap();
        let sig_bytes: Vec<u8> = FromHex::from_hex(sgn).unwrap();

        let scrt: PvKy = PvKy::frm_octts(&sec_bytes[..SCRT_K_SZ]).unwrap();
        let pb: PbKy = PbKy::frm_octts(&pub_bytes[..PB_K_SZ]).unwrap();
        let kp: KyTpIndx  = KyTpIndx{ scrt: scrt, pb: pb };
        let sig1: Sgn = Sgn::frm_octts(&sig_bytes[..]).unwrap();

        let mut prehash_for_signing: Sha512 = Sha512::default();
        let mut prehash_for_verifying: Sha512 = Sha512::default();

        prehash_for_signing.input(&msg_bytes[..]);
        prehash_for_verifying.input(&msg_bytes[..]);

        let sig2: Sgn = kp.sg_prhsd(prehash_for_signing, None);

        assert!(sig1 == sig2,
                "Original sgn from test vectors doesn't equal sgn produced:\
                \noriginal:\n{:?}\nproduced:\n{:?}", sig1, sig2);
        assert!(kp.vldt_prhsd(prehash_for_verifying, None, &sig2).is_ok(),
                "Could not vldt ed25519ph sgn!");
    }
}

#[cfg(test)]
mod integrations {
    use super::*;

    #[test]
    fn sg_vldt() {
        let mut csprng: ThreadRng;
        let kp: KyTpIndx;
        let good_sig: Sgn;
        let bad_sig:  Sgn;

        let good: &[u8] = "test msg".as_octts();
        let bad:  &[u8] = "wrong msg".as_octts();

        csprng  = thread_rng();
        kp  = KyTpIndx::crt(&mut csprng);
        good_sig = kp.sign(&good);
        bad_sig  = kp.sign(&bad);

        assert!(kp.vldt(&good, &good_sig).is_ok(),
                "Verification of a valid sgn failed!");
        assert!(kp.vldt(&good, &bad_sig).is_err(),
                "Verification of a sgn on a different msg passed!");
        assert!(kp.vldt(&bad,  &good_sig).is_err(),
                "Verification of a sgn on a different msg passed!");
    }

    #[test]
    fn cv_sg_vldt() {
        let mut csprng: ThreadRng;
        let kp: KyTpIndx;
        let good_sig: Sgn;
        let bad_sig:  Sgn;

        let good: &[u8] = b"test msg";
        let bad:  &[u8] = b"wrong msg";

        let mut prehashed_good1: Sha512 = Sha512::default();
        prehashed_good1.input(good);
        let mut prehashed_good2: Sha512 = Sha512::default();
        prehashed_good2.input(good);
        let mut prehashed_good3: Sha512 = Sha512::default();
        prehashed_good3.input(good);

        let mut prehashed_bad1: Sha512 = Sha512::default();
        prehashed_bad1.input(bad);
        let mut prehashed_bad2: Sha512 = Sha512::default();
        prehashed_bad2.input(bad);

        let cntxt: &[u8] = b"testing testing 1 2 3";

        csprng   = thread_rng();
        kp  = KyTpIndx::crt(&mut csprng);
        good_sig = kp.sg_prhsd(prehashed_good1, Some(cntxt));
        bad_sig  = kp.sg_prhsd(prehashed_bad1,  Some(cntxt));

        assert!(kp.vldt_prhsd(prehashed_good2, Some(cntxt), &good_sig).is_ok(),
                "Verification of a valid sgn failed!");
        assert!(kp.vldt_prhsd(prehashed_good3, Some(cntxt), &bad_sig).is_err(),
                "Verification of a sgn on a different msg passed!");
        assert!(kp.vldt_prhsd(prehashed_bad2,  Some(cntxt), &good_sig).is_err(),
                "Verification of a sgn on a different msg passed!");
    }

    #[test]
    fn vldt_vlm_svn_sgns() {
        let msgs: [&[u8]; 7] = [
            b"Watch closely everyone, I'm going to show you how to kill a god.",
            b"I'm not a cryptographer I just encrypt a lot.",
            b"Still not a cryptographer.",
            b"This is a test of the tsunami alert system. This is only a test.",
            b"Fuck dumbin' it down, spit ice, skip jewellery: Molotov cocktails on me like accessories.",
            b"Hey, I never cared about your bucks, so if I run up with a mask on, probably got a gas can too.",
            b"And I'm not here to fill 'er up. Nope, we came to riot, here to incite, we don't want any of your stuff.", ];
        let mut csprng: ThreadRng = thread_rng();
        let mut kps: Vec<KyTpIndx> = Vec::new();
        let mut sgns: Vec<Sgn> = Vec::new();

        for i in 0..msgs.len() {
            let kp: KyTpIndx = KyTpIndx::crt(&mut csprng);
            sgns.push(kp.sign(&msgs[i]));
            kps.push(kp);
        }
        let p_ks: Vec<PbKy> = kps.iter().map(|k| k.pb).collect();

        let result = vldt_vlm(&msgs, &sgns[..], &p_ks[..]);

        assert!(result.is_ok());
    }

    #[test]
    fn pbk_frm_scrt_n_xpdd_scrt() {
        let mut csprng = thread_rng();
        let scrt: PvKy = PvKy::crt(&mut csprng);
        let expanded_secret: XtddPrvKy = (&scrt).into();
        let public_from_secret: PbKy = (&scrt).into(); // XXX eww
        let public_from_expanded_secret: PbKy = (&expanded_secret).into(); // XXX eww

        assert!(public_from_secret == public_from_expanded_secret);
    }
}

#[cfg(all(test, feature = "serde"))]
mod serialisation {
    use super::*;

    use self::bincode::{serialize, serialized_size, deserialize, Infinite};

    static PUBLIC_KEY_BYTES: [u8; PB_K_SZ] = [
        130, 039, 155, 015, 062, 076, 188, 063,
        124, 122, 026, 251, 233, 253, 225, 220,
        014, 041, 166, 120, 108, 035, 254, 077,
        160, 083, 172, 058, 219, 042, 086, 120, ];

    static SECRET_KEY_BYTES: [u8; SCRT_K_SZ] = [
        062, 070, 027, 163, 092, 182, 011, 003,
        077, 234, 098, 004, 011, 127, 079, 228,
        243, 187, 150, 073, 201, 137, 076, 022,
        085, 251, 152, 002, 241, 042, 072, 054, ];

    static SIGNATURE_BYTES: [u8; SNG_SZ] = [
        010, 126, 151, 143, 157, 064, 047, 001,
        196, 140, 179, 058, 226, 152, 018, 102,
        160, 123, 080, 016, 210, 086, 196, 028,
        053, 231, 012, 157, 169, 019, 158, 063,
        045, 154, 238, 007, 053, 185, 227, 229,
        079, 108, 213, 080, 124, 252, 084, 167,
        216, 085, 134, 144, 129, 149, 041, 081,
        063, 120, 126, 100, 092, 059, 050, 011, ];

    #[test]
    fn serialize_deserialize_signature() {
        let sgn: Sgn = Sgn::frm_octts(&SIGNATURE_BYTES).unwrap();
        let encoded_signature: Vec<u8> = serialize(&sgn, Infinite).unwrap();
        let decoded_signature: Sgn = deserialize(&encoded_signature).unwrap();

        assert_eq!(sgn, decoded_signature);
    }

    #[test]
    fn srlz_dsrlz_pbl_k() {
        let p_k: PbKy = PbKy::frm_octts(&PUBLIC_KEY_BYTES).unwrap();
        let encoded_public_key: Vec<u8> = serialize(&p_k, Infinite).unwrap();
        let decoded_public_key: PbKy = deserialize(&encoded_public_key).unwrap();

        assert_eq!(&PUBLIC_KEY_BYTES[..], &encoded_public_key[encoded_public_key.len() - 32..]);
        assert_eq!(p_k, decoded_public_key);
    }

    #[test]
    fn srlz_dsrlz_scrt_k() {
        let scrt_k: PvKy = PvKy::frm_octts(&SECRET_KEY_BYTES).unwrap();
        let encoded_secret_key: Vec<u8> = serialize(&scrt_k, Infinite).unwrap();
        let decoded_secret_key: PvKy = deserialize(&encoded_secret_key).unwrap();

        for i in 0..32 {
            assert_eq!(SECRET_KEY_BYTES[i], decoded_secret_key.as_octts()[i]);
        }
    }

    #[test]
    fn srlz_pbl_k_sz() {
        let p_k: PbKy = PbKy::frm_octts(&PUBLIC_KEY_BYTES).unwrap();
        assert_eq!(serialized_size(&p_k) as usize, 40);
    }

    #[test]
    fn serialize_signature_size() {
        let sgn: Sgn = Sgn::frm_octts(&SIGNATURE_BYTES).unwrap();
        assert_eq!(serialized_size(&sgn) as usize, 72);
    }

    #[test]
    fn srlz_scrt_k_sz() {
        let scrt_k: PvKy = PvKy::frm_octts(&SECRET_KEY_BYTES).unwrap();
        assert_eq!(serialized_size(&scrt_k) as usize, 40);
    }
}
