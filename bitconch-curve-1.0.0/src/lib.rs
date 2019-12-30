#![no_std]
#![warn(future_incompatible)]
#![warn(rust_2018_compatibility)]
#![warn(rust_2018_idioms)]

#[cfg(any(feature = "std", test))]
#[macro_use]
extern crate std;

extern crate clear_on_drop;
extern crate curve25519_dalek;
extern crate failure;
extern crate rand;
#[cfg(feature = "serde")]
extern crate serde;
extern crate sha2;

mod cstnts;
mod bitconch_curve;
mod errs;
mod pb;
mod scrt;
mod sgn;

pub use crate::bitconch_curve::*;
