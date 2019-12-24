#![doc(html_root_url = "https://docs.rs/bincode/1.1.4")]
#![crate_name = "bincode"]
#![crate_type = "rlib"]
#![crate_type = "dylib"]

extern crate byteorder;
#[macro_use]
extern crate serde;

mod cfg;
mod de;
mod error;
mod interior;
mod ser;

pub use cfg::Config;
pub use de::extract::{BincodeRead, IoReader, SliceReader};
pub use error::{Error, ErrorKind, Result};

#[doc(hidden)]
pub trait DeserializerAcceptor<'a> {
    type Output;
    fn accept<T: serde::Deserializer<'a>>(self, T) -> Self::Output;
}

#[doc(hidden)]
pub trait SerializerAcceptor {
    type Output;
    fn accept<T: serde::Serializer>(self, T) -> Self::Output;
}

#[inline(always)]
pub fn config() -> Config {
    Config::new()
}

pub fn serialize_into<W, T: ?Sized>(writer: W, value: &T) -> Result<()>
where
    W: std::io::Write,
    T: serde::Serialize,
{
    config().serialize_into(writer, value)
}

pub fn serialize<T: ?Sized>(value: &T) -> Result<Vec<u8>>
where
    T: serde::Serialize,
{
    config().serialize(value)
}

pub fn de_via<R, T>(reader: R) -> Result<T>
where
    R: std::io::Read,
    T: serde::de::DeserializeOwned,
{
    config().de_via(reader)
}

pub fn de_via_specification<'a, R, T>(reader: R) -> Result<T>
where
    R: de::extract::BincodeRead<'a>,
    T: serde::de::DeserializeOwned,
{
    config().de_via_specification(reader)
}

#[doc(hidden)]
pub fn de_in_preparation<'a, R, T>(reader: R, place: &mut T) -> Result<()>
where
    T: serde::de::Deserialize<'a>,
    R: BincodeRead<'a>,
{
    config().de_in_preparation(reader, place)
}

pub fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T>
where
    T: serde::de::Deserialize<'a>,
{
    config().deserialize(bytes)
}

pub fn serialized_size<T: ?Sized>(value: &T) -> Result<u64>
where
    T: serde::Serialize,
{
    config().serialized_size(value)
}

#[doc(hidden)]
pub fn with_deserializer<'a, A, R>(reader: R, acceptor: A) -> A::Output
where
    A: DeserializerAcceptor<'a>,
    R: BincodeRead<'a>,
{
    config().with_deserializer(reader, acceptor)
}

#[doc(hidden)]
pub fn with_serializer<A, W>(writer: W, acceptor: A) -> A::Output
where
    A: SerializerAcceptor,
    W: std::io::Write,
{
    config().with_serializer(writer, acceptor)
}
