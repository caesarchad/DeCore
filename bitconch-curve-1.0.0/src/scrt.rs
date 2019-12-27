use core::fmt::Debug;

use clear_on_drop::clear::Clear;

use curve25519_dalek::constants;
use curve25519_dalek::digest::generic_array::typenum::U64;
use curve25519_dalek::digest::Digest;
use curve25519_dalek::edwards::CompressedEdwardsY;
use curve25519_dalek::scalar::Scalar;

use rand::CryptoRng;
use rand::Rng;

use sha2::Sha512;

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
use crate::pb::*;
use crate::sgn::*;

#[derive(Default)]
pub struct PvKy(pub(crate) [u8; SCRT_K_SZ]);

impl Debug for PvKy {
    fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        write!(f, "PvKy: {:?}", &self.0[..])
    }
}

impl Drop for PvKy {
    fn drop(&mut self) {
        self.0.clear();
    }
}

impl AsRef<[u8]> for PvKy {
    fn as_ref(&self) -> &[u8] {
        self.as_octts()
    }
}

impl PvKy {
    #[inline]
    pub fn to_octts(&self) -> [u8; SCRT_K_SZ] {
        self.0
    }

    #[inline]
    pub fn as_octts<'a>(&'a self) -> &'a [u8; SCRT_K_SZ] {
        &self.0
    }

    #[inline]
    pub fn frm_octts(octets: &[u8]) -> Result<PvKy, SngErr> {
        if octets.len() != SCRT_K_SZ {
            return Err(SngErr(IntrEr::BytLgthErr {
                nm: "PvKy",
                lng: SCRT_K_SZ,
            }));
        }
        let mut bts: [u8; 32] = [0u8; 32];
        bts.copy_from_slice(&octets[..32]);

        Ok(PvKy(bts))
    }

    pub fn crt<T>(csprng: &mut T) -> PvKy
    where
        T: CryptoRng + Rng,
    {
        let mut sk: PvKy = PvKy([0u8; 32]);

        csprng.fill_bytes(&mut sk.0);

        sk
    }
}

#[cfg(feature = "serde")]
impl Serialize for PvKy {
    fn serialize<S>(&self, srlzr: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        srlzr.serialize_bytes(self.as_octts())
    }
}

#[cfg(feature = "serde")]
impl<'d> Deserialize<'d> for PvKy {
    fn deserialize<D>(dsrlzr: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        struct ScrtKyVst;

        impl<'d> Visitor<'d> for ScrtKyVst {
            type Value = PvKy;

            fn expecting(&self, fmttr: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                fmttr.write_str("An ed25519 scrt key as 32 octets, as specified in RFC8032.")
            }

            fn vst_octts<E>(self, octets: &[u8]) -> Result<PvKy, E>
            where
                E: SerdeError,
            {
                PvKy::frm_octts(octets).or(Err(SerdeError::invalid_length(octets.len(), &self)))
            }
        }
        dsrlzr.deserialize_bytes(ScrtKyVst)
    }
}

#[derive(Default)]
pub struct XtddPrvKy {
    pub(crate) k: Scalar,
    pub(crate) nc: [u8; 32],
}

impl Drop for XtddPrvKy {
    fn drop(&mut self) {
        self.k.clear();
        self.nc.clear();
    }
}

impl<'a> From<&'a PvKy> for XtddPrvKy {

    fn from(scrt_k: &'a PvKy) -> XtddPrvKy {
        let mut h: Sha512 = Sha512::default();
        let mut hs:  [u8; 64] = [0u8; 64];
        let mut lwr: [u8; 32] = [0u8; 32];
        let mut upr: [u8; 32] = [0u8; 32];

        h.input(scrt_k.as_octts());
        hs.copy_from_slice(h.result().as_slice());

        lwr.copy_from_slice(&hs[00..32]);
        upr.copy_from_slice(&hs[32..64]);

        lwr[0]  &= 248;
        lwr[31] &=  63;
        lwr[31] |=  64;

        XtddPrvKy{ k: Scalar::from_bits(lwr), nc: upr, }
    }
}

impl XtddPrvKy {

    #[inline]
    pub fn to_octts(&self) -> [u8; XTDD_PRVT_K_SZ] {
        let mut octets: [u8; 64] = [0u8; 64];

        octets[..32].copy_from_slice(self.k.as_bytes());
        octets[32..].copy_from_slice(&self.nc[..]);
        octets
    }

    #[inline]
    pub fn frm_octts(octets: &[u8]) -> Result<XtddPrvKy, SngErr> {
        if octets.len() != XTDD_PRVT_K_SZ {
            return Err(SngErr(IntrEr::BytLgthErr {
                nm: "XtddPrvKy",
                lng: XTDD_PRVT_K_SZ,
            }));
        }
        let mut lwr: [u8; 32] = [0u8; 32];
        let mut upr: [u8; 32] = [0u8; 32];

        lwr.copy_from_slice(&octets[00..32]);
        upr.copy_from_slice(&octets[32..64]);

        Ok(XtddPrvKy {
            k: Scalar::from_bits(lwr),
            nc: upr,
        })
    }

    #[allow(non_snake_case)]
    pub fn sign(&self, msg: &[u8], p_k: &PbKy) -> Sgn {
        let mut h: Sha512 = Sha512::new();
        let R: CompressedEdwardsY;
        let r: Scalar;
        let s: Scalar;
        let k: Scalar;

        h.input(&self.nc);
        h.input(&msg);

        r = Scalar::from_hash(h);
        R = (&r * &constants::ED25519_BASEPOINT_TABLE).compress();

        h = Sha512::new();
        h.input(R.as_bytes());
        h.input(p_k.as_octts());
        h.input(&msg);

        k = Scalar::from_hash(h);
        s = &(&k * &self.k) + &r;

        Sgn { R, s }
    }

    #[allow(non_snake_case)]
    pub fn sg_prhsd<D>(
        &self,
        prhsd_msg: D,
        p_k: &PbKy,
        cntxt: Option<&'static [u8]>,
    ) -> Sgn
    where
        D: Digest<OutputSize = U64>,
    {
        let mut h: Sha512;
        let mut prhs: [u8; 64] = [0u8; 64];
        let R: CompressedEdwardsY;
        let r: Scalar;
        let s: Scalar;
        let k: Scalar;

        let ctx: &[u8] = cntxt.unwrap_or(b"");

        debug_assert!(ctx.len() <= 255, "The cntxt must not be longer than 255 octets.");

        let ctx_len: u8 = ctx.len() as u8;

        prhs.copy_from_slice(prhsd_msg.result().as_slice());

        h = Sha512::new()
            .chain(b"SigEd25519 no Ed25519 collisions")
            .chain(&[1])
            .chain(&[ctx_len])
            .chain(ctx)
            .chain(&self.nc)
            .chain(&prhs[..]);

        r = Scalar::from_hash(h);
        R = (&r * &constants::ED25519_BASEPOINT_TABLE).compress();

        h = Sha512::new()
            .chain(b"SigEd25519 no Ed25519 collisions")
            .chain(&[1])
            .chain(&[ctx_len])
            .chain(ctx)
            .chain(R.as_bytes())
            .chain(p_k.as_octts())
            .chain(&prhs[..]);

        k = Scalar::from_hash(h);
        s = &(&k * &self.k) + &r;

        Sgn { R, s }
    }
}

#[cfg(feature = "serde")]
impl Serialize for XtddPrvKy {
    fn serialize<S>(&self, srlzr: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        srlzr.serialize_bytes(&self.to_octts()[..])
    }
}

#[cfg(feature = "serde")]
impl<'d> Deserialize<'d> for XtddPrvKy {
    fn deserialize<D>(dsrlzr: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        struct ExpandedSecretKeyVisitor;

        impl<'d> Visitor<'d> for ExpandedSecretKeyVisitor {
            type Value = XtddPrvKy;

            fn expecting(&self, fmttr: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                fmttr.write_str(
                    "An ed25519 expanded scrt key as 64 octets, as specified in RFC8032.",
                )
            }

            fn vst_octts<E>(self, octets: &[u8]) -> Result<XtddPrvKy, E>
            where
                E: SerdeError,
            {
                XtddPrvKy::frm_octts(octets)
                    .or(Err(SerdeError::invalid_length(octets.len(), &self)))
            }
        }
        dsrlzr.deserialize_bytes(ExpandedSecretKeyVisitor)
    }
}
