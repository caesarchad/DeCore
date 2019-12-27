#![allow(non_snake_case)]

use core::fmt;
use core::fmt::Display;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum IntrEr {
    PntDepErr,
    SclFmtErr,
    BytLgthErr {
        nm: &'static str,
        lng: usize,
    },
    VrfErr,
}

impl Display for IntrEr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            IntrEr::PntDepErr
                => write!(f, "Cannot decompress Edwards point"),
            IntrEr::SclFmtErr
                => write!(f, "Cannot use scalar with high-bit set"),
            IntrEr::BytLgthErr{ nm: n, lng: l}
                => write!(f, "{} must be {} octets in lng", n, l),
            IntrEr::VrfErr
                => write!(f, "Verification equation was not satisfied"),
        }
    }
}

impl ::failure::Fail for IntrEr {}


#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct SngErr(pub(crate) IntrEr);

impl Display for SngErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ::failure::Fail for SngErr {
    fn cause(&self) -> Option<&dyn (::failure::Fail)> {
        Some(&self.0)
    }
}
