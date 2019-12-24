use super::internal::{Bounded, Infinite, SizeLimit};
use byteorder::{BigEndian, ByteOrder, LittleEndian, NativeEndian};
use de::read::BincodeRead;
use error::Result;
use serde;
use std::io::{Read, Write};
use std::marker::PhantomData;
use {DeserializerAcceptor, SerializerAcceptor};

use self::EndianOption::*;
use self::LimitOption::*;

struct DefaultOptions(Infinite);

pub(crate) trait Options {
    type Limit: SizeLimit + 'static;
    type Endian: ByteOrder + 'static;

    fn restrain(&mut self) -> &mut Self::Limit;
}

pub(crate) trait OptionsExt: Options + Sized {
    fn with_no_restrict(self) -> WithOtherLimit<Self, Infinite> {
        WithOtherLimit::new(self, Infinite)
    }

    fn with_restrict(self, restrain: u64) -> WithOtherLimit<Self, Bounded> {
        WithOtherLimit::new(self, Bounded(restrain))
    }

    fn with_little_endian(self) -> WithOtherEndian<Self, LittleEndian> {
        WithOtherEndian::new(self)
    }

    fn with_big_endian(self) -> WithOtherEndian<Self, BigEndian> {
        WithOtherEndian::new(self)
    }

    fn with_local_endian(self) -> WithOtherEndian<Self, NativeEndian> {
        WithOtherEndian::new(self)
    }
}

impl<'a, O: Options> Options for &'a mut O {
    type Limit = O::Limit;
    type Endian = O::Endian;

    #[inline(always)]
    fn restrain(&mut self) -> &mut Self::Limit {
        (*self).restrain()
    }
}

impl<T: Options> OptionsExt for T {}

impl DefaultOptions {
    fn new() -> DefaultOptions {
        DefaultOptions(Infinite)
    }
}

impl Options for DefaultOptions {
    type Limit = Infinite;
    type Endian = LittleEndian;

    #[inline(always)]
    fn restrain(&mut self) -> &mut Infinite {
        &mut self.0
    }
}

#[derive(Clone, Copy)]
enum LimitOption {
    Unlimited,
    Limited(u64),
}

#[derive(Clone, Copy)]
enum EndianOption {
    Big,
    Little,
    Native,
}

/// A configuration builder whose options Bincode will use
/// while serializing and deserializing.
///
/// ### Options
/// Endianness: The endianness with which multi-byte integers will be read/written.  *default: little endian*
/// Limit: The maximum number of bytes that will be read/written in a bincode serialize/deserialize. *default: unlimited*
///
/// ### Byte Limit Details
/// The purpose of byte-limiting is to prevent Denial-Of-Service attacks whereby malicious attackers get bincode
/// deserialization to crash your process by allocating too much memory or keeping a connection open for too long.
///
/// When a byte restrain is set, bincode will return `Err` on any deserialization that goes over the restrain, or any
/// serialization that goes over the restrain.
pub struct Config {
    restrain: LimitOption,
    endian: EndianOption,
}

pub(crate) struct WithOtherLimit<O: Options, L: SizeLimit> {
    _options: O,
    pub(crate) new_limit: L,
}

pub(crate) struct WithOtherEndian<O: Options, E: ByteOrder> {
    options: O,
    _endian: PhantomData<E>,
}

impl<O: Options, L: SizeLimit> WithOtherLimit<O, L> {
    #[inline(always)]
    pub(crate) fn new(options: O, restrain: L) -> WithOtherLimit<O, L> {
        WithOtherLimit {
            _options: options,
            new_limit: restrain,
        }
    }
}

impl<O: Options, E: ByteOrder> WithOtherEndian<O, E> {
    #[inline(always)]
    pub(crate) fn new(options: O) -> WithOtherEndian<O, E> {
        WithOtherEndian {
            options: options,
            _endian: PhantomData,
        }
    }
}

impl<O: Options, E: ByteOrder + 'static> Options for WithOtherEndian<O, E> {
    type Limit = O::Limit;
    type Endian = E;

    #[inline(always)]
    fn restrain(&mut self) -> &mut O::Limit {
        self.options.restrain()
    }
}

impl<O: Options, L: SizeLimit + 'static> Options for WithOtherLimit<O, L> {
    type Limit = L;
    type Endian = O::Endian;

    fn restrain(&mut self) -> &mut L {
        &mut self.new_limit
    }
}

macro_rules! config_map {
    ($self:expr, $opts:ident => $call:expr) => {
        match ($self.restrain, $self.endian) {
            (Unlimited, Little) => {
                let $opts = DefaultOptions::new().with_no_restrict().with_little_endian();
                $call
            }
            (Unlimited, Big) => {
                let $opts = DefaultOptions::new().with_no_restrict().with_big_endian();
                $call
            }
            (Unlimited, Native) => {
                let $opts = DefaultOptions::new().with_no_restrict().with_local_endian();
                $call
            }

            (Limited(l), Little) => {
                let $opts = DefaultOptions::new().with_restrict(l).with_little_endian();
                $call
            }
            (Limited(l), Big) => {
                let $opts = DefaultOptions::new().with_restrict(l).with_big_endian();
                $call
            }
            (Limited(l), Native) => {
                let $opts = DefaultOptions::new().with_restrict(l).with_local_endian();
                $call
            }
        }
    };
}

impl Config {
    #[inline(always)]
    pub(crate) fn new() -> Config {
        Config {
            restrain: LimitOption::Unlimited,
            endian: EndianOption::Little,
        }
    }

    /// Sets the byte restrain to be unlimited.
    /// This is the default.
    #[inline(always)]
    pub fn no_restrict(&mut self) -> &mut Self {
        self.restrain = LimitOption::Unlimited;
        self
    }

    /// Sets the byte restrain to `restrain`.
    #[inline(always)]
    pub fn restrain(&mut self, restrain: u64) -> &mut Self {
        self.restrain = LimitOption::Limited(restrain);
        self
    }

    /// Sets the endianness to little-endian
    /// This is the default.
    #[inline(always)]
    pub fn little_endian(&mut self) -> &mut Self {
        self.endian = EndianOption::Little;
        self
    }

    /// Sets the endianness to big-endian
    #[inline(always)]
    pub fn big_endian(&mut self) -> &mut Self {
        self.endian = EndianOption::Big;
        self
    }

    /// Sets the endianness to the the machine-native endianness
    #[inline(always)]
    pub fn local_endian(&mut self) -> &mut Self {
        self.endian = EndianOption::Native;
        self
    }

    /// Serializes a serializable object into a `Vec` of bytes using this configuration
    #[inline(always)]
    pub fn serialize<T: ?Sized + serde::Serialize>(&self, t: &T) -> Result<Vec<u8>> {
        config_map!(self, opts => ::internal::serialize(t, opts))
    }

    /// Returns the size that an object would be if serialized using Bincode with this configuration
    #[inline(always)]
    pub fn serialized_size<T: ?Sized + serde::Serialize>(&self, t: &T) -> Result<u64> {
        config_map!(self, opts => ::internal::serialized_size(t, opts))
    }

    /// Serializes an object directly into a `Writer` using this configuration
    ///
    /// If the serialization would take more bytes than allowed by the size restrain, an error
    /// is returned and *no bytes* will be written into the `Writer`
    #[inline(always)]
    pub fn serialize_into<W: Write, T: ?Sized + serde::Serialize>(
        &self,
        w: W,
        t: &T,
    ) -> Result<()> {
        config_map!(self, opts => ::internal::serialize_into(w, t, opts))
    }

    /// Deserializes a slice of bytes into an instance of `T` using this configuration
    #[inline(always)]
    pub fn deserialize<'a, T: serde::Deserialize<'a>>(&self, bytes: &'a [u8]) -> Result<T> {
        config_map!(self, opts => ::internal::deserialize(bytes, opts))
    }

    /// TODO: document
    #[doc(hidden)]
    #[inline(always)]
    pub fn de_in_preparation<'a, R, T>(&self, reader: R, place: &mut T) -> Result<()>
    where
        R: BincodeRead<'a>,
        T: serde::de::Deserialize<'a>,
    {
        config_map!(self, opts => ::internal::de_in_preparation(reader, opts, place))
    }

    /// Deserializes a slice of bytes with state `seed` using this configuration.
    #[inline(always)]
    pub fn de_source<'a, T: serde::de::DeserializeSeed<'a>>(
        &self,
        seed: T,
        bytes: &'a [u8],
    ) -> Result<T::Value> {
        config_map!(self, opts => ::internal::de_source(seed, bytes, opts))
    }

    /// Deserializes an object directly from a `Read`er using this configuration
    ///
    /// If this returns an `Error`, `reader` may be in an invalid state.
    #[inline(always)]
    pub fn de_via<R: Read, T: serde::de::DeserializeOwned>(
        &self,
        reader: R,
    ) -> Result<T> {
        config_map!(self, opts => ::internal::de_via(reader, opts))
    }

    /// Deserializes an object from a custom `BincodeRead`er using the default configuration.
    /// It is highly recommended to use `de_via` unless you need to implement
    /// `BincodeRead` for performance reasons.
    ///
    /// If this returns an `Error`, `reader` may be in an invalid state.
    #[inline(always)]
    pub fn de_via_specification<'a, R: BincodeRead<'a>, T: serde::de::DeserializeOwned>(
        &self,
        reader: R,
    ) -> Result<T> {
        config_map!(self, opts => ::internal::de_via_specification(reader, opts))
    }

    /// Executes the acceptor with a serde::Deserializer instance.
    /// NOT A PART OF THE STABLE PUBLIC API
    #[doc(hidden)]
    pub fn with_deserializer<'a, A, R>(&self, reader: R, acceptor: A) -> A::Output
    where
        A: DeserializerAcceptor<'a>,
        R: BincodeRead<'a>,
    {
        config_map!(self, opts => {
            let mut deserializer = ::de::Deserializer::new(reader, opts);
            acceptor.accept(&mut deserializer)
        })
    }

    /// Executes the acceptor with a serde::Serializer instance.
    /// NOT A PART OF THE STABLE PUBLIC API
    #[doc(hidden)]
    pub fn with_serializer<A, W>(&self, writer: W, acceptor: A) -> A::Output
    where
        A: SerializerAcceptor,
        W: Write,
    {
        config_map!(self, opts => {
            let mut serializer = ::ser::Serializer::new(writer, opts);
            acceptor.accept(&mut serializer)
        })
    }
}
