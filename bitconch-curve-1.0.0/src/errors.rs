#![allow(non_snake_case)]

use core::fmt;
use core::fmt::Display;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum InternalError {
    PointDecompressionError,
    ScalarFormatError,
    BytesLengthError {
        name: &'static str,
        length: usize,
    },
    VerifyError,
}

impl Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            InternalError::PointDecompressionError
                => write!(f, "Cannot decompress Edwards point"),
            InternalError::ScalarFormatError
                => write!(f, "Cannot use scalar with high-bit set"),
            InternalError::BytesLengthError{ name: n, length: l}
                => write!(f, "{} must be {} bytes in length", n, l),
            InternalError::VerifyError
                => write!(f, "Verification equation was not satisfied"),
        }
    }
}

impl ::failure::Fail for InternalError {}


#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct SignatureError(pub(crate) InternalError);

impl Display for SignatureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ::failure::Fail for SignatureError {
    fn cause(&self) -> Option<&dyn (::failure::Fail)> {
        Some(&self.0)
    }
}
