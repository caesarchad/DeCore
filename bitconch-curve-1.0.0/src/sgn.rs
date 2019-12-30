use core::fmt::Debug;

use curve25519_dalek::edwards::CompressedEdwardsY;
use curve25519_dalek::scalar::Scalar;

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

#[allow(non_snake_case)]
#[derive(Copy, Eq, PartialEq)]
pub struct Sgn {

    pub(crate) R: CompressedEdwardsY,

    pub(crate) s: Scalar,
}

impl Clone for Sgn {
    fn clone(&self) -> Self {
        *self
    }
}

impl Debug for Sgn {
    fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        write!(f, "Sgn( R: {:?}, s: {:?} )", &self.R, &self.s)
    }
}

impl Sgn {
    #[inline]
    pub fn to_octts(&self) -> [u8; SNG_SZ] {
        let mut signature_bytes: [u8; SNG_SZ] = [0u8; SNG_SZ];

        signature_bytes[..32].copy_from_slice(&self.R.as_bytes()[..]);
        signature_bytes[32..].copy_from_slice(&self.s.as_bytes()[..]);
        signature_bytes
    }

    #[inline]
    pub fn frm_octts(octets: &[u8]) -> Result<Sgn, SngErr> {
        if octets.len() != SNG_SZ {
            return Err(SngErr(IntrEr::BytLgthErr {
                nm: "Sgn",
                lng: SNG_SZ,
            }));
        }
        let mut lwr: [u8; 32] = [0u8; 32];
        let mut upr: [u8; 32] = [0u8; 32];

        lwr.copy_from_slice(&octets[..32]);
        upr.copy_from_slice(&octets[32..]);

        if upr[31] & 224 != 0 {
            return Err(SngErr(IntrEr::SclFmtErr));
        }

        Ok(Sgn {
            R: CompressedEdwardsY(lwr),
            s: Scalar::from_bits(upr),
        })
    }
}

#[cfg(feature = "serde")]
impl Serialize for Sgn {
    fn serialize<S>(&self, srlzr: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        srlzr.serialize_bytes(&self.to_octts()[..])
    }
}

#[cfg(feature = "serde")]
impl<'d> Deserialize<'d> for Sgn {
    fn deserialize<D>(dsrlzr: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        struct SgnVstr;

        impl<'d> Visitor<'d> for SgnVstr {
            type Value = Sgn;

            fn expecting(&self, fmttr: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                fmttr.write_str("An ed25519 sgn as 64 octets, as specified in RFC8032.")
            }

            fn vst_octts<E>(self, octets: &[u8]) -> Result<Sgn, E>
            where
                E: SerdeError,
            {
                Sgn::frm_octts(octets).or(Err(SerdeError::invalid_length(octets.len(), &self)))
            }
        }
        dsrlzr.deserialize_bytes(SgnVstr)
    }
}
