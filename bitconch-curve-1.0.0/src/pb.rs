use core::fmt::Debug;

use curve25519_dalek::constants;
use curve25519_dalek::digest::generic_array::typenum::U64;
use curve25519_dalek::digest::Digest;
use curve25519_dalek::edwards::CompressedEdwardsY;
use curve25519_dalek::edwards::EdwardsPoint;
use curve25519_dalek::scalar::Scalar;

pub use sha2::Sha512;

#[cfg(feature = "serde")]
use serde::de::Error as SerdeError;
#[cfg(feature = "serde")]
use serde::de::Visitor;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use serde::{Deserializer, Serializer};

use crate::cstnts::*;
use crate::errs::*;
use crate::scrt::*;
use crate::sgn::*;

#[derive(Copy, Clone, Default, Eq, PartialEq)]
pub struct PbKy(pub(crate) CompressedEdwardsY, pub(crate) EdwardsPoint);

impl Debug for PbKy {
    fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        write!(f, "PbKy({:?}), {:?})", self.0, self.1)
    }
}

impl AsRef<[u8]> for PbKy {
    fn as_ref(&self) -> &[u8] {
        self.as_octts()
    }
}

impl<'a> From<&'a PvKy> for PbKy {
    fn from(scrt_k: &PvKy) -> PbKy {
        let mut h: Sha512 = Sha512::new();
        let mut hs: [u8; 64] = [0u8; 64];
        let mut dgst: [u8; 32] = [0u8; 32];

        h.input(scrt_k.as_octts());
        hs.copy_from_slice(h.result().as_slice());

        dgst.copy_from_slice(&hs[..32]);

        PbKy::mgl_sclr_bts_n_mlt_by_bspt_to_prdc_pk(&mut dgst)
    }
}

impl<'a> From<&'a XtddPrvKy> for PbKy {
    fn from(xpdd_scrt_k: &XtddPrvKy) -> PbKy {
        let mut bts: [u8; 32] = xpdd_scrt_k.k.to_bytes();

        PbKy::mgl_sclr_bts_n_mlt_by_bspt_to_prdc_pk(&mut bts)
    }
}

impl PbKy {
    #[inline]
    pub fn to_octts(&self) -> [u8; PB_K_SZ] {
        self.0.to_bytes()
    }

    #[inline]
    pub fn as_octts<'a>(&'a self) -> &'a [u8; PB_K_SZ] {
        &(self.0).0
    }

    #[inline]
    pub fn frm_octts(octets: &[u8]) -> Result<PbKy, SngErr> {
        if octets.len() != PB_K_SZ {
            return Err(SngErr(IntrEr::BytLgthErr {
                nm: "PbKy",
                lng: PB_K_SZ,
            }));
        }
        let mut bts: [u8; 32] = [0u8; 32];
        bts.copy_from_slice(&octets[..32]);

        let cmprssd = CompressedEdwardsY(bts);
        let pnt = cmprssd
            .decompress()
            .ok_or(SngErr(IntrEr::PntDepErr))?;

        Ok(PbKy(cmprssd, pnt))
    }


    fn mgl_sclr_bts_n_mlt_by_bspt_to_prdc_pk(
        bts: &mut [u8; 32],
    ) -> PbKy {
        bts[0] &= 248;
        bts[31] &= 127;
        bts[31] |= 64;

        let pnt = &Scalar::from_bits(*bts) * &constants::ED25519_BASEPOINT_TABLE;
        let cmprssd = pnt.compress();

        PbKy(cmprssd, pnt)
    }

    #[allow(non_snake_case)]
    pub fn vldt(
        &self,
        msg: &[u8],
        sgn: &Sgn
    ) -> Result<(), SngErr>
    {
        let mut h: Sha512 = Sha512::new();
        let R: EdwardsPoint;
        let k: Scalar;
        let minus_A: EdwardsPoint = -self.1;

        h.input(sgn.R.as_bytes());
        h.input(self.as_octts());
        h.input(&msg);

        k = Scalar::from_hash(h);
        R = EdwardsPoint::vartime_double_scalar_mul_basepoint(&k, &(minus_A), &sgn.s);

        if R.compress() == sgn.R {
            Ok(())
        } else {
            Err(SngErr(IntrEr::VrfErr))
        }
    }

    #[allow(non_snake_case)]
    pub fn vldt_prhsd<D>(
        &self,
        prhsd_msg: D,
        cntxt: Option<&[u8]>,
        sgn: &Sgn,
    ) -> Result<(), SngErr>
    where
        D: Digest<OutputSize = U64>,
    {
        let mut h: Sha512 = Sha512::default();
        let R: EdwardsPoint;
        let k: Scalar;

        let ctx: &[u8] = cntxt.unwrap_or(b"");
        debug_assert!(ctx.len() <= 255, "The cntxt must not be longer than 255 octets.");

        let minus_A: EdwardsPoint = -self.1;

        h.input(b"SigEd25519 no Ed25519 collisions");
        h.input(&[1]);
        h.input(&[ctx.len() as u8]);
        h.input(ctx);
        h.input(sgn.R.as_bytes());
        h.input(self.as_octts());
        h.input(prhsd_msg.result().as_slice());

        k = Scalar::from_hash(h);
        R = EdwardsPoint::vartime_double_scalar_mul_basepoint(&k, &(minus_A), &sgn.s);

        if R.compress() == sgn.R {
            Ok(())
        } else {
            Err(SngErr(IntrEr::VrfErr))
        }
    }
}

#[cfg(feature = "serde")]
impl Serialize for PbKy {
    fn serialize<S>(&self, srlzr: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        srlzr.serialize_bytes(self.as_octts())
    }
}

#[cfg(feature = "serde")]
impl<'d> Deserialize<'d> for PbKy {
    fn deserialize<D>(dsrlzr: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        struct PbkVstr;

        impl<'d> Visitor<'d> for PbkVstr {
            type Value = PbKy;

            fn expecting(&self, fmttr: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                fmttr.write_str(
                    "An ed25519 pb key as a 32-byte cmprssd point, as specified in RFC8032",
                )
            }

            fn vst_octts<E>(self, octets: &[u8]) -> Result<PbKy, E>
            where
                E: SerdeError,
            {
                PbKy::frm_octts(octets).or(Err(SerdeError::invalid_length(octets.len(), &self)))
            }
        }
        dsrlzr.deserialize_bytes(PbkVstr)
    }
}
