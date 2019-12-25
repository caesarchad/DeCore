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

use crate::constants::*;
use crate::errors::*;
use crate::private::*;
use crate::signature::*;

#[derive(Copy, Clone, Default, Eq, PartialEq)]
pub struct PublicKey(pub(crate) CompressedEdwardsY, pub(crate) EdwardsPoint);

impl Debug for PublicKey {
    fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        write!(f, "PublicKey({:?}), {:?})", self.0, self.1)
    }
}

impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        self.as_octets()
    }
}

impl<'a> From<&'a PrivateKey> for PublicKey {
    fn from(secret_key: &PrivateKey) -> PublicKey {
        let mut h: Sha512 = Sha512::new();
        let mut hash: [u8; 64] = [0u8; 64];
        let mut digest: [u8; 32] = [0u8; 32];

        h.input(secret_key.as_octets());
        hash.copy_from_slice(h.result().as_slice());

        digest.copy_from_slice(&hash[..32]);

        PublicKey::bash_value_bits_and_mul_via_origin_to_generate_pubkey(&mut digest)
    }
}

impl<'a> From<&'a ExpandedSecretKey> for PublicKey {
    fn from(expanded_secret_key: &ExpandedSecretKey) -> PublicKey {
        let mut bits: [u8; 32] = expanded_secret_key.key.to_bytes();

        PublicKey::bash_value_bits_and_mul_via_origin_to_generate_pubkey(&mut bits)
    }
}

impl PublicKey {
    #[inline]
    pub fn to_octets(&self) -> [u8; PUBLIC_KEY_LENGTH] {
        self.0.to_bytes()
    }

    #[inline]
    pub fn as_octets<'a>(&'a self) -> &'a [u8; PUBLIC_KEY_LENGTH] {
        &(self.0).0
    }

    #[inline]
    pub fn from_octets(bytes: &[u8]) -> Result<PublicKey, SignatureError> {
        if bytes.len() != PUBLIC_KEY_LENGTH {
            return Err(SignatureError(InternalError::BytesLengthError {
                name: "PublicKey",
                length: PUBLIC_KEY_LENGTH,
            }));
        }
        let mut bits: [u8; 32] = [0u8; 32];
        bits.copy_from_slice(&bytes[..32]);

        let compressed = CompressedEdwardsY(bits);
        let point = compressed
            .decompress()
            .ok_or(SignatureError(InternalError::PointDecompressionError))?;

        Ok(PublicKey(compressed, point))
    }


    fn bash_value_bits_and_mul_via_origin_to_generate_pubkey(
        bits: &mut [u8; 32],
    ) -> PublicKey {
        bits[0] &= 248;
        bits[31] &= 127;
        bits[31] |= 64;

        let point = &Scalar::from_bits(*bits) * &constants::ED25519_BASEPOINT_TABLE;
        let compressed = point.compress();

        PublicKey(compressed, point)
    }

    #[allow(non_snake_case)]
    pub fn validate(
        &self,
        message: &[u8],
        signature: &Signature
    ) -> Result<(), SignatureError>
    {
        let mut h: Sha512 = Sha512::new();
        let R: EdwardsPoint;
        let k: Scalar;
        let minus_A: EdwardsPoint = -self.1;

        h.input(signature.R.as_bytes());
        h.input(self.as_octets());
        h.input(&message);

        k = Scalar::from_hash(h);
        R = EdwardsPoint::vartime_double_scalar_mul_basepoint(&k, &(minus_A), &signature.s);

        if R.compress() == signature.R {
            Ok(())
        } else {
            Err(SignatureError(InternalError::VerifyError))
        }
    }

    #[allow(non_snake_case)]
    pub fn validate_already_hashed<D>(
        &self,
        prehashed_message: D,
        context: Option<&[u8]>,
        signature: &Signature,
    ) -> Result<(), SignatureError>
    where
        D: Digest<OutputSize = U64>,
    {
        let mut h: Sha512 = Sha512::default();
        let R: EdwardsPoint;
        let k: Scalar;

        let ctx: &[u8] = context.unwrap_or(b"");
        debug_assert!(ctx.len() <= 255, "The context must not be longer than 255 octets.");

        let minus_A: EdwardsPoint = -self.1;

        h.input(b"SigEd25519 no Ed25519 collisions");
        h.input(&[1]);
        h.input(&[ctx.len() as u8]);
        h.input(ctx);
        h.input(signature.R.as_bytes());
        h.input(self.as_octets());
        h.input(prehashed_message.result().as_slice());

        k = Scalar::from_hash(h);
        R = EdwardsPoint::vartime_double_scalar_mul_basepoint(&k, &(minus_A), &signature.s);

        if R.compress() == signature.R {
            Ok(())
        } else {
            Err(SignatureError(InternalError::VerifyError))
        }
    }
}

#[cfg(feature = "serde")]
impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.as_octets())
    }
}

#[cfg(feature = "serde")]
impl<'d> Deserialize<'d> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        struct PublicKeyVisitor;

        impl<'d> Visitor<'d> for PublicKeyVisitor {
            type Value = PublicKey;

            fn expecting(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                formatter.write_str(
                    "An ed25519 public key as a 32-byte compressed point, as specified in RFC8032",
                )
            }

            fn visit_bytes<E>(self, bytes: &[u8]) -> Result<PublicKey, E>
            where
                E: SerdeError,
            {
                PublicKey::from_octets(bytes).or(Err(SerdeError::invalid_length(bytes.len(), &self)))
            }
        }
        deserializer.deserialize_bytes(PublicKeyVisitor)
    }
}
