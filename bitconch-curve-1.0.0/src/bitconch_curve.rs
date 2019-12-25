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

pub use crate::constants::*;
pub use crate::errors::*;
pub use crate::public::*;
pub use crate::private::*;
pub use crate::signature::*;


#[cfg(any(feature = "alloc", feature = "std"))]
#[allow(non_snake_case)]
pub fn validate_volume(
    messages: &[&[u8]],
    signatures: &[Signature],
    public_keys: &[PublicKey],
) -> Result<(), SignatureError>
{
    const ASSERT_MESSAGE: &'static [u8] = b"The number of messages, signatures, and public keys must be equal.";
    assert!(signatures.len()  == messages.len(),    ASSERT_MESSAGE);
    assert!(signatures.len()  == public_keys.len(), ASSERT_MESSAGE);
    assert!(public_keys.len() == messages.len(),    ASSERT_MESSAGE);

    #[cfg(feature = "alloc")]
    use alloc::vec::Vec;
    #[cfg(feature = "std")]
    use std::vec::Vec;

    use core::iter::once;
    use rand::thread_rng;

    use curve25519_dalek::traits::IsIdentity;
    use curve25519_dalek::traits::VartimeMultiscalarMul;

    let zs: Vec<Scalar> = signatures
        .iter()
        .map(|_| Scalar::from(thread_rng().gen::<u128>()))
        .collect();

    let B_coefficient: Scalar = signatures
        .iter()
        .map(|sig| sig.s)
        .zip(zs.iter())
        .map(|(s, z)| z * s)
        .sum();

    let hrams = (0..signatures.len()).map(|i| {
        let mut h: Sha512 = Sha512::default();
        h.input(signatures[i].R.as_bytes());
        h.input(public_keys[i].as_octets());
        h.input(&messages[i]);
        Scalar::from_hash(h)
    });

    let zhrams = hrams.zip(zs.iter()).map(|(hram, z)| hram * z);

    let Rs = signatures.iter().map(|sig| sig.R.decompress());
    let As = public_keys.iter().map(|pk| Some(pk.1));
    let B = once(Some(constants::ED25519_BASEPOINT_POINT));

    let id = EdwardsPoint::optional_multiscalar_mul(
        once(-B_coefficient).chain(zs.iter().cloned()).chain(zhrams),
        B.chain(Rs).chain(As),
    ).ok_or_else(|| SignatureError(InternalError::VerifyError))?;

    if id.is_identity() {
        Ok(())
    } else {
        Err(SignatureError(InternalError::VerifyError))
    }
}

#[derive(Debug, Default)]
pub struct Keypair {
    pub secret: PrivateKey,
    pub public: PublicKey,
}

impl Keypair {

    pub fn to_octets(&self) -> [u8; KEYPAIR_LENGTH] {
        let mut bytes: [u8; KEYPAIR_LENGTH] = [0u8; KEYPAIR_LENGTH];

        bytes[..SECRET_KEY_LENGTH].copy_from_slice(self.secret.as_octets());
        bytes[SECRET_KEY_LENGTH..].copy_from_slice(self.public.as_octets());
        bytes
    }

    pub fn from_octets<'a>(bytes: &'a [u8]) -> Result<Keypair, SignatureError> {
        if bytes.len() != KEYPAIR_LENGTH {
            return Err(SignatureError(InternalError::BytesLengthError {
                name: "Keypair",
                length: KEYPAIR_LENGTH,
            }));
        }
        let secret = PrivateKey::from_octets(&bytes[..SECRET_KEY_LENGTH])?;
        let public = PublicKey::from_octets(&bytes[SECRET_KEY_LENGTH..])?;

        Ok(Keypair{ secret: secret, public: public })
    }

    pub fn create<R>(csprng: &mut R) -> Keypair
    where
        R: CryptoRng + Rng,
    {
        let sk: PrivateKey = PrivateKey::create(csprng);
        let pk: PublicKey = (&sk).into();

        Keypair{ public: pk, secret: sk }
    }

    pub fn sign(&self, message: &[u8]) -> Signature {
        let expanded: ExpandedSecretKey = (&self.secret).into();

        expanded.sign(&message, &self.public)
    }

    pub fn sign_already_hashed<D>(
        &self,
        prehashed_message: D,
        context: Option<&'static [u8]>,
    ) -> Signature
    where
        D: Digest<OutputSize = U64>,
    {
        let expanded: ExpandedSecretKey = (&self.secret).into();

        expanded.sign_already_hashed(prehashed_message, &self.public, context)
    }

    pub fn validate(
        &self,
        message: &[u8],
        signature: &Signature
    ) -> Result<(), SignatureError>
    {
        self.public.validate(message, signature)
    }

    pub fn validate_already_hashed<D>(
        &self,
        prehashed_message: D,
        context: Option<&[u8]>,
        signature: &Signature,
    ) -> Result<(), SignatureError>
    where
        D: Digest<OutputSize = U64>,
    {
        self.public.validate_already_hashed(prehashed_message, context, signature)
    }
}

impl PartialEq for Keypair {
    fn eq(&self, other: &Self) -> bool {
        self.public.as_ref() == other.public.as_ref()
    }
}

#[cfg(feature = "serde")]
impl Serialize for Keypair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.to_octets()[..])
    }
}

#[cfg(feature = "serde")]
impl<'d> Deserialize<'d> for Keypair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        struct KeypairVisitor;

        impl<'d> Visitor<'d> for KeypairVisitor {
            type Value = Keypair;

            fn expecting(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                formatter.write_str("An ed25519 keypair, 64 bytes in total where the secret key is \
                                     the first 32 bytes and is in unexpanded form, and the second \
                                     32 bytes is a compressed point for a public key.")
            }

            fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Keypair, E>
            where
                E: SerdeError,
            {
                let secret_key = PrivateKey::from_octets(&bytes[..SECRET_KEY_LENGTH]);
                let public_key = PublicKey::from_octets(&bytes[SECRET_KEY_LENGTH..]);

                if secret_key.is_ok() && public_key.is_ok() {
                    Ok(Keypair{ secret: secret_key.unwrap(), public: public_key.unwrap() })
                } else {
                    Err(SerdeError::invalid_length(bytes.len(), &self))
                }
            }
        }
        deserializer.deserialize_bytes(KeypairVisitor)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use clear_on_drop::clear::Clear;

    #[test]
    fn keypair_trim_upon_tick() {
        let mut keypair: Keypair = Keypair::from_octets(&[1u8; KEYPAIR_LENGTH][..]).unwrap();

        keypair.clear();

        fn as_octets<T>(x: &T) -> &[u8] {
            use std::mem;
            use std::slice;

            unsafe { slice::from_raw_parts(x as *const T as *const u8, mem::size_of_val(x)) }
        }

        assert!(!as_octets(&keypair).contains(&0x15));
    }
}
