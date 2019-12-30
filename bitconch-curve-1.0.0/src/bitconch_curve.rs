use core::default::Default;

use rand::CryptoRng;
use rand::Rng;

#[cfg(feature = "serde")]
use serde::de::Error as SerdeError;
#[cfg(feature = "serde")]
use serde::de::Visitor;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use serde::{Deserializer, Serializer};

pub use sha2::Sha512;

use curve25519_dalek::digest::generic_array::typenum::U64;
pub use curve25519_dalek::digest::Digest;

use curve25519_dalek::constants;
use curve25519_dalek::edwards::EdwardsPoint;
use curve25519_dalek::scalar::Scalar;

pub use crate::cstnts::*;
pub use crate::errs::*;
pub use crate::pb::*;
pub use crate::scrt::*;
pub use crate::sgn::*;


#[cfg(any(feature = "alloc", feature = "std"))]
#[allow(non_snake_case)]
pub fn vldt_vlm(
    msgs: &[&[u8]],
    sgns: &[Sgn],
    p_ks: &[PbKy],
) -> Result<(), SngErr>
{
    const ASSRT_MSG: &'static [u8] = b"The number of msgs, sgns, and pb keys must be equal.";
    assert!(sgns.len()  == msgs.len(),    ASSRT_MSG);
    assert!(sgns.len()  == p_ks.len(), ASSRT_MSG);
    assert!(p_ks.len() == msgs.len(),    ASSRT_MSG);

    #[cfg(feature = "alloc")]
    use alloc::vec::Vec;
    #[cfg(feature = "std")]
    use std::vec::Vec;

    use core::iter::once;
    use rand::thread_rng;

    use curve25519_dalek::traits::IsIdentity;
    use curve25519_dalek::traits::VartimeMultiscalarMul;

    let zs: Vec<Scalar> = sgns
        .iter()
        .map(|_| Scalar::from(thread_rng().gen::<u128>()))
        .collect();

    let B_coefficient: Scalar = sgns
        .iter()
        .map(|sig| sig.s)
        .zip(zs.iter())
        .map(|(s, z)| z * s)
        .sum();

    let hrams = (0..sgns.len()).map(|i| {
        let mut h: Sha512 = Sha512::default();
        h.input(sgns[i].R.as_bytes());
        h.input(p_ks[i].as_octts());
        h.input(&msgs[i]);
        Scalar::from_hash(h)
    });

    let zhrams = hrams.zip(zs.iter()).map(|(hram, z)| hram * z);

    let Rs = sgns.iter().map(|sig| sig.R.decompress());
    let As = p_ks.iter().map(|pk| Some(pk.1));
    let B = once(Some(constants::ED25519_BASEPOINT_POINT));

    let id = EdwardsPoint::optional_multiscalar_mul(
        once(-B_coefficient).chain(zs.iter().cloned()).chain(zhrams),
        B.chain(Rs).chain(As),
    ).ok_or_else(|| SngErr(IntrEr::VrfErr))?;

    if id.is_identity() {
        Ok(())
    } else {
        Err(SngErr(IntrEr::VrfErr))
    }
}

#[derive(Debug, Default)]
pub struct KyTpIndx {
    pub scrt: PvKy,
    pub pb: PbKy,
}

impl KyTpIndx {

    pub fn to_octts(&self) -> [u8; K_TP_SZ] {
        let mut octets: [u8; K_TP_SZ] = [0u8; K_TP_SZ];

        octets[..SCRT_K_SZ].copy_from_slice(self.scrt.as_octts());
        octets[SCRT_K_SZ..].copy_from_slice(self.pb.as_octts());
        octets
    }

    pub fn frm_octts<'a>(octets: &'a [u8]) -> Result<KyTpIndx, SngErr> {
        if octets.len() != K_TP_SZ {
            return Err(SngErr(IntrEr::BytLgthErr {
                nm: "KyTpIndx",
                lng: K_TP_SZ,
            }));
        }
        let scrt = PvKy::frm_octts(&octets[..SCRT_K_SZ])?;
        let pb = PbKy::frm_octts(&octets[SCRT_K_SZ..])?;

        Ok(KyTpIndx{ scrt: scrt, pb: pb })
    }

    pub fn crt<R>(csprng: &mut R) -> KyTpIndx
    where
        R: CryptoRng + Rng,
    {
        let sk: PvKy = PvKy::crt(csprng);
        let pk: PbKy = (&sk).into();

        KyTpIndx{ pb: pk, scrt: sk }
    }

    pub fn sign(&self, msg: &[u8]) -> Sgn {
        let xpdd: XtddPrvKy = (&self.scrt).into();

        xpdd.sign(&msg, &self.pb)
    }

    pub fn sg_prhsd<D>(
        &self,
        prhsd_msg: D,
        cntxt: Option<&'static [u8]>,
    ) -> Sgn
    where
        D: Digest<OutputSize = U64>,
    {
        let xpdd: XtddPrvKy = (&self.scrt).into();

        xpdd.sg_prhsd(prhsd_msg, &self.pb, cntxt)
    }

    pub fn vldt(
        &self,
        msg: &[u8],
        sgn: &Sgn
    ) -> Result<(), SngErr>
    {
        self.pb.vldt(msg, sgn)
    }

    pub fn vldt_prhsd<D>(
        &self,
        prhsd_msg: D,
        cntxt: Option<&[u8]>,
        sgn: &Sgn,
    ) -> Result<(), SngErr>
    where
        D: Digest<OutputSize = U64>,
    {
        self.pb.vldt_prhsd(prhsd_msg, cntxt, sgn)
    }
}

impl PartialEq for KyTpIndx {
    fn eq(&self, other: &Self) -> bool {
        self.pb.as_ref() == other.pb.as_ref()
    }
}

#[cfg(feature = "serde")]
impl Serialize for KyTpIndx {
    fn serialize<S>(&self, srlzr: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        srlzr.serialize_bytes(&self.to_octts()[..])
    }
}

#[cfg(feature = "serde")]
impl<'d> Deserialize<'d> for KyTpIndx {
    fn deserialize<D>(dsrlzr: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        struct KyTplAcsr;

        impl<'d> Visitor<'d> for KyTplAcsr {
            type Value = KyTpIndx;

            fn expecting(&self, fmttr: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                fmttr.write_str("An ed25519 kp, 64 octets in total where the scrt key is \
                                     the first 32 octets and is in unexpanded form, and the second \
                                     32 octets is a cmprssd pnt for a pb key.")
            }

            fn vst_octts<E>(self, octets: &[u8]) -> Result<KyTpIndx, E>
            where
                E: SerdeError,
            {
                let scrt_k = PvKy::frm_octts(&octets[..SCRT_K_SZ]);
                let p_k = PbKy::frm_octts(&octets[SCRT_K_SZ..]);

                if scrt_k.is_ok() && p_k.is_ok() {
                    Ok(KyTpIndx{ scrt: scrt_k.unwrap(), pb: p_k.unwrap() })
                } else {
                    Err(SerdeError::invalid_length(octets.len(), &self))
                }
            }
        }
        dsrlzr.deserialize_bytes(KyTplAcsr)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use clear_on_drop::clear::Clear;

    #[test]
    fn kp_cl_n_drp() {
        let mut kp: KyTpIndx = KyTpIndx::frm_octts(&[1u8; K_TP_SZ][..]).unwrap();

        kp.clear();

        fn as_octts<T>(x: &T) -> &[u8] {
            use std::mem;
            use std::slice;

            unsafe { slice::from_raw_parts(x as *const T as *const u8, mem::size_of_val(x)) }
        }

        assert!(!as_octts(&kp).contains(&0x15));
    }
}
