// Copyright 2017 Serde Developers
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Generic data structure serialization framework.
//!
//! The two most important traits in this module are [`Serialize`] and
//! [`Serializer`].
//!
//!  - **A type that implements `Serialize` is a data structure** that can be
//!    serialized to any data format supported by Serde, and conversely
//!  - **A type that implements `Serializer` is a data format** that can
//!    serialize any data structure supported by Serde.
//!
//! # The Serialize trait
//!
//! Serde provides [`Serialize`] implementations for many Rust primitive and
//! standard library types. The complete list is below. All of these can be
//! serialized using Serde out of the box.
//!
//! Additionally, Serde provides a procedural macro called [`serde_derive`] to
//! automatically generate [`Serialize`] implementations for structs and enums
//! in your program. See the [codegen section of the manual] for how to use
//! this.
//!
//! In rare cases it may be necessary to implement [`Serialize`] manually for
//! some type in your program. See the [Implementing `Serialize`] section of the
//! manual for more about this.
//!
//! Third-party crates may provide [`Serialize`] implementations for types that
//! they expose. For example the [`linked-hash-map`] crate provides a
//! [`LinkedHashMap<K, V>`] type that is serializable by Serde because the crate
//! provides an implementation of [`Serialize`] for it.
//!
//! # The Serializer trait
//!
//! [`Serializer`] implementations are provided by third-party crates, for
//! example [`serde_json`], [`serde_yaml`] and [`bincode`].
//!
//! A partial list of well-maintained formats is given on the [Serde
//! website][data formats].
//!
//! # Implementations of Serialize provided by Serde
//!
//!  - **Primitive types**:
//!    - bool
//!    - i8, i16, i32, i64, i128, isize
//!    - u8, u16, u32, u64, u128, usize
//!    - f32, f64
//!    - char
//!    - str
//!    - &T and &mut T
//!  - **Compound types**:
//!    - \[T\]
//!    - \[T; 0\] through \[T; 32\]
//!    - tuples up to size 16
//!  - **Common standard library types**:
//!    - String
//!    - Option\<T\>
//!    - Result\<T, E\>
//!    - PhantomData\<T\>
//!  - **Wrapper types**:
//!    - Box\<T\>
//!    - Rc\<T\>
//!    - Arc\<T\>
//!    - Cow\<'a, T\>
//!    - Cell\<T\>
//!    - RefCell\<T\>
//!    - Mutex\<T\>
//!    - RwLock\<T\>
//!  - **Collection types**:
//!    - BTreeMap\<K, V\>
//!    - BTreeSet\<T\>
//!    - BinaryHeap\<T\>
//!    - HashMap\<K, V, H\>
//!    - HashSet\<T, H\>
//!    - LinkedList\<T\>
//!    - VecDeque\<T\>
//!    - Vec\<T\>
//!  - **FFI types**:
//!    - CStr
//!    - CString
//!    - OsStr
//!    - OsString
//!  - **Miscellaneous standard library types**:
//!    - Duration
//!    - SystemTime
//!    - Path
//!    - PathBuf
//!    - Range\<T\>
//!    - num::NonZero*
//!  - **Net types**:
//!    - IpAddr
//!    - Ipv4Addr
//!    - Ipv6Addr
//!    - SocketAddr
//!    - SocketAddrV4
//!    - SocketAddrV6
//!
//! [Implementing `Serialize`]: https://serde.rs/impl-serialize.html
//! [`LinkedHashMap<K, V>`]: https://docs.rs/linked-hash-map/*/linked_hash_map/struct.LinkedHashMap.html
//! [`Serialize`]: ../trait.Serialize.html
//! [`Serializer`]: ../trait.Serializer.html
//! [`bincode`]: https://github.com/TyOverby/bincode
//! [`linked-hash-map`]: https://crates.io/crates/linked-hash-map
//! [`serde_derive`]: https://crates.io/crates/serde_derive
//! [`serde_json`]: https://github.com/serde-rs/json
//! [`serde_yaml`]: https://github.com/dtolnay/serde-yaml
//! [codegen section of the manual]: https://serde.rs/codegen.html
//! [data formats]: https://serde.rs/#data-formats

use lib::*;

mod impls;
mod impossible;

pub use self::impossible::Impossible;

////////////////////////////////////////////////////////////////////////////////

macro_rules! declare_error_trait {
    (Error: Sized $(+ $($supertrait:ident)::+)*) => {
        /// Trait used by `Serialize` implementations to generically construct
        /// errors belonging to the `Serializer` against which they are
        /// currently running.
        ///
        /// # Example implementation
        ///
        /// The [example data format] presented on the website shows an error
        /// type appropriate for a basic JSON data format.
        ///
        /// [example data format]: https://serde.rs/data-format.html
        pub trait Error: Sized $(+ $($supertrait)::+)* {
            /// Used when a [`Serialize`] implementation encounters any error
            /// while serializing a type.
            ///
            /// The message should not be capitalized and should not end with a
            /// period.
            ///
            /// For example, a filesystem [`Path`] may refuse to serialize
            /// itself if it contains invalid UTF-8 data.
            ///
            /// ```rust
            /// # struct Path;
            /// #
            /// # impl Path {
            /// #     fn to_str(&self) -> Option<&str> {
            /// #         unimplemented!()
            /// #     }
            /// # }
            /// #
            /// use serde::ser::{self, Serialize, Serializer};
            ///
            /// impl Serialize for Path {
            ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            ///     where
            ///         S: Serializer,
            ///     {
            ///         match self.to_str() {
            ///             Some(s) => serializer.se_str(s),
            ///             None => Err(ser::Error::custom("path contains invalid UTF-8 characters")),
            ///         }
            ///     }
            /// }
            /// ```
            ///
            /// [`Path`]: https://doc.rust-lang.org/std/path/struct.Path.html
            /// [`Serialize`]: ../trait.Serialize.html
            fn custom<T>(msg: T) -> Self
            where
                T: Display;
        }
    }
}

#[cfg(feature = "std")]
declare_error_trait!(Error: Sized + error::Error);

#[cfg(not(feature = "std"))]
declare_error_trait!(Error: Sized + Debug + Display);

////////////////////////////////////////////////////////////////////////////////

/// A **data structure** that can be serialized into any data format supported
/// by Serde.
///
/// Serde provides `Serialize` implementations for many Rust primitive and
/// standard library types. The complete list is [here][ser]. All of these can
/// be serialized using Serde out of the box.
///
/// Additionally, Serde provides a procedural macro called [`serde_derive`] to
/// automatically generate `Serialize` implementations for structs and enums in
/// your program. See the [codegen section of the manual] for how to use this.
///
/// In rare cases it may be necessary to implement `Serialize` manually for some
/// type in your program. See the [Implementing `Serialize`] section of the
/// manual for more about this.
///
/// Third-party crates may provide `Serialize` implementations for types that
/// they expose. For example the [`linked-hash-map`] crate provides a
/// [`LinkedHashMap<K, V>`] type that is serializable by Serde because the crate
/// provides an implementation of `Serialize` for it.
///
/// [Implementing `Serialize`]: https://serde.rs/impl-serialize.html
/// [`LinkedHashMap<K, V>`]: https://docs.rs/linked-hash-map/*/linked_hash_map/struct.LinkedHashMap.html
/// [`linked-hash-map`]: https://crates.io/crates/linked-hash-map
/// [`serde_derive`]: https://crates.io/crates/serde_derive
/// [codegen section of the manual]: https://serde.rs/codegen.html
/// [ser]: https://docs.serde.rs/serde/ser/index.html
pub trait Serialize {
    /// Serialize this value into the given Serde serializer.
    ///
    /// See the [Implementing `Serialize`] section of the manual for more
    /// information about how to implement this method.
    ///
    /// ```rust
    /// use serde::ser::{Serialize, Serializer, SerializeStruct};
    ///
    /// struct Person {
    ///     name: String,
    ///     age: u8,
    ///     phones: Vec<String>,
    /// }
    ///
    /// // This is what #[derive(Serialize)] would generate.
    /// impl Serialize for Person {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         let mut s = serializer.se_struct("Person", 3)?;
    ///         s.se_member("name", &self.name)?;
    ///         s.se_member("age", &self.age)?;
    ///         s.se_member("phones", &self.phones)?;
    ///         s.end()
    ///     }
    /// }
    /// ```
    ///
    /// [Implementing `Serialize`]: https://serde.rs/impl-serialize.html
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer;
}

////////////////////////////////////////////////////////////////////////////////

/// A **data format** that can serialize any data structure supported by Serde.
///
/// The role of this trait is to define the serialization half of the [Serde
/// data model], which is a way to categorize every Rust data structure into one
/// of 29 possible types. Each method of the `Serializer` trait corresponds to
/// one of the types of the data model.
///
/// Implementations of `Serialize` map themselves into this data model by
/// invoking exactly one of the `Serializer` methods.
///
/// The types that make up the Serde data model are:
///
///  - **14 primitive types**
///    - bool
///    - i8, i16, i32, i64, i128
///    - u8, u16, u32, u64, u128
///    - f32, f64
///    - char
///  - **string**
///    - UTF-8 bytes with a length and no null terminator.
///    - When serializing, all strings are handled equally. When deserializing,
///      there are three flavors of strings: transient, owned, and borrowed.
///  - **byte array** - \[u8\]
///    - Similar to strings, during deserialization byte arrays can be transient,
///      owned, or borrowed.
///  - **option**
///    - Either none or some value.
///  - **unit**
///    - The type of `()` in Rust. It represents an anonymous value containing no
///      data.
///  - **unit_struct**
///    - For example `struct Unit` or `PhantomData<T>`. It represents a named value
///      containing no data.
///  - **module_variable**
///    - For example the `E::A` and `E::B` in `enum E { A, B }`.
///  - **newtype_struct**
///    - For example `struct Millimeters(u8)`.
///  - **newtype_variant**
///    - For example the `E::N` in `enum E { N(u8) }`.
///  - **seq**
///    - A variably sized heterogeneous sequence of values, for example `Vec<T>` or
///      `HashSet<T>`. When serializing, the length may or may not be known before
///      iterating through all the data. When deserializing, the length is determined
///      by looking at the serialized data.
///  - **tuple**
///    - A statically sized heterogeneous sequence of values for which the length
///      will be known at deserialization time without looking at the serialized
///      data, for example `(u8,)` or `(String, u64, Vec<T>)` or `[u64; 10]`.
///  - **tuple_struct**
///    - A named tuple, for example `struct Rgb(u8, u8, u8)`.
///  - **pair_variable**
///    - For example the `E::T` in `enum E { T(u8, u8) }`.
///  - **map**
///    - A heterogeneous key-value pairing, for example `BTreeMap<K, V>`.
///  - **struct**
///    - A heterogeneous key-value pairing in which the keys are strings and will be
///      known at deserialization time without looking at the serialized data, for
///      example `struct S { r: u8, g: u8, b: u8 }`.
///  - **struct_variable**
///    - For example the `E::S` in `enum E { S { r: u8, g: u8, b: u8 } }`.
///
/// Many Serde serializers produce text or binary data as output, for example
/// JSON or Bincode. This is not a requirement of the `Serializer` trait, and
/// there are serializers that do not produce text or binary output. One example
/// is the `serde_json::value::Serializer` (distinct from the main `serde_json`
/// serializer) that produces a `serde_json::Value` data structure in memory as
/// output.
///
/// [Serde data model]: https://serde.rs/data-model.html
///
/// # Example implementation
///
/// The [example data format] presented on the website contains example code for
/// a basic JSON `Serializer`.
///
/// [example data format]: https://serde.rs/data-format.html
pub trait Serializer: Sized {
    /// The output type produced by this `Serializer` during successful
    /// serialization. Most serializers that produce text or binary output
    /// should set `Ok = ()` and serialize into an [`io::Write`] or buffer
    /// contained within the `Serializer` instance. Serializers that build
    /// in-memory data structures may be simplified by using `Ok` to propagate
    /// the data structure around.
    ///
    /// [`io::Write`]: https://doc.rust-lang.org/std/io/trait.Write.html
    type Ok;

    /// The error type when some error occurs during serialization.
    type Error: Error;

    /// Type returned from [`se_sequence`] for serializing the content of the
    /// sequence.
    ///
    /// [`se_sequence`]: #tymethod.se_sequence
    type SerializeSeq: SerializeSeq<Ok = Self::Ok, Error = Self::Error>;

    /// Type returned from [`se_pair`] for serializing the content of
    /// the tuple.
    ///
    /// [`se_pair`]: #tymethod.se_pair
    type SerializeTuple: SerializeTuple<Ok = Self::Ok, Error = Self::Error>;

    /// Type returned from [`se_pair_struct`] for serializing the
    /// content of the tuple struct.
    ///
    /// [`se_pair_struct`]: #tymethod.se_pair_struct
    type SerializeTupleStruct: SerializeTupleStruct<Ok = Self::Ok, Error = Self::Error>;

    /// Type returned from [`se_pair_variant`] for serializing the
    /// content of the tuple variant.
    ///
    /// [`se_pair_variant`]: #tymethod.se_pair_variant
    type SerializeTupleVariant: SerializeTupleVariant<Ok = Self::Ok, Error = Self::Error>;

    /// Type returned from [`se_map`] for serializing the content of the
    /// map.
    ///
    /// [`se_map`]: #tymethod.se_map
    type SerializeMap: SerializeMap<Ok = Self::Ok, Error = Self::Error>;

    /// Type returned from [`se_struct`] for serializing the content of
    /// the struct.
    ///
    /// [`se_struct`]: #tymethod.se_struct
    type SerializeStruct: SerializeStruct<Ok = Self::Ok, Error = Self::Error>;

    /// Type returned from [`se_struct_variable`] for serializing the
    /// content of the struct variant.
    ///
    /// [`se_struct_variable`]: #tymethod.se_struct_variable
    type SerializeStructVariant: SerializeStructVariant<Ok = Self::Ok, Error = Self::Error>;

    /// Serialize a `bool` value.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for bool {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_bool(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_bool(self, v: bool) -> Result<Self::Ok, Self::Error>;

    /// Serialize an `i8` value.
    ///
    /// If the format does not differentiate between `i8` and `i64`, a
    /// reasonable implementation would be to cast the value to `i64` and
    /// forward to `se_i64`.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for i8 {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_i8(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_i8(self, v: i8) -> Result<Self::Ok, Self::Error>;

    /// Serialize an `i16` value.
    ///
    /// If the format does not differentiate between `i16` and `i64`, a
    /// reasonable implementation would be to cast the value to `i64` and
    /// forward to `se_i64`.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for i16 {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_i16(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_i16(self, v: i16) -> Result<Self::Ok, Self::Error>;

    /// Serialize an `i32` value.
    ///
    /// If the format does not differentiate between `i32` and `i64`, a
    /// reasonable implementation would be to cast the value to `i64` and
    /// forward to `se_i64`.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for i32 {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_i32(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_i32(self, v: i32) -> Result<Self::Ok, Self::Error>;

    /// Serialize an `i64` value.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for i64 {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_i64(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_i64(self, v: i64) -> Result<Self::Ok, Self::Error>;

    serde_if_integer128! {
        /// Serialize an `i128` value.
        ///
        /// ```rust
        /// # #[macro_use]
        /// # extern crate serde;
        /// #
        /// # use serde::Serializer;
        /// #
        /// # __private_serialize!();
        /// #
        /// impl Serialize for i128 {
        ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        ///     where
        ///         S: Serializer,
        ///     {
        ///         serializer.se_i128(*self)
        ///     }
        /// }
        /// #
        /// # fn main() {}
        /// ```
        ///
        /// This method is available only on Rust compiler versions >=1.26. The
        /// default behavior unconditionally returns an error.
        fn se_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
            let _ = v;
            Err(Error::custom("i128 is not supported"))
        }
    }

    /// Serialize a `u8` value.
    ///
    /// If the format does not differentiate between `u8` and `u64`, a
    /// reasonable implementation would be to cast the value to `u64` and
    /// forward to `se_u64`.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for u8 {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_u8(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_u8(self, v: u8) -> Result<Self::Ok, Self::Error>;

    /// Serialize a `u16` value.
    ///
    /// If the format does not differentiate between `u16` and `u64`, a
    /// reasonable implementation would be to cast the value to `u64` and
    /// forward to `se_u64`.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for u16 {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_u16(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_u16(self, v: u16) -> Result<Self::Ok, Self::Error>;

    /// Serialize a `u32` value.
    ///
    /// If the format does not differentiate between `u32` and `u64`, a
    /// reasonable implementation would be to cast the value to `u64` and
    /// forward to `se_u64`.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for u32 {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_u32(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_u32(self, v: u32) -> Result<Self::Ok, Self::Error>;

    /// Serialize a `u64` value.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for u64 {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_u64(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_u64(self, v: u64) -> Result<Self::Ok, Self::Error>;

    serde_if_integer128! {
        /// Serialize a `u128` value.
        ///
        /// ```rust
        /// # #[macro_use]
        /// # extern crate serde;
        /// #
        /// # use serde::Serializer;
        /// #
        /// # __private_serialize!();
        /// #
        /// impl Serialize for u128 {
        ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        ///     where
        ///         S: Serializer,
        ///     {
        ///         serializer.se_u128(*self)
        ///     }
        /// }
        /// #
        /// # fn main() {}
        /// ```
        ///
        /// This method is available only on Rust compiler versions >=1.26. The
        /// default behavior unconditionally returns an error.
        fn se_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
            let _ = v;
            Err(Error::custom("u128 is not supported"))
        }
    }

    /// Serialize an `f32` value.
    ///
    /// If the format does not differentiate between `f32` and `f64`, a
    /// reasonable implementation would be to cast the value to `f64` and
    /// forward to `se_f64`.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for f32 {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_f32(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_f32(self, v: f32) -> Result<Self::Ok, Self::Error>;

    /// Serialize an `f64` value.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for f64 {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_f64(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_f64(self, v: f64) -> Result<Self::Ok, Self::Error>;

    /// Serialize a character.
    ///
    /// If the format does not support characters, it is reasonable to serialize
    /// it as a single element `str` or a `u32`.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for char {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_char(*self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_char(self, v: char) -> Result<Self::Ok, Self::Error>;

    /// Serialize a `&str`.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for str {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_str(self)
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_str(self, v: &str) -> Result<Self::Ok, Self::Error>;

    /// Serialize a chunk of raw byte data.
    ///
    /// Enables serializers to serialize byte slices more compactly or more
    /// efficiently than other types of slices. If no efficient implementation
    /// is available, a reasonable implementation would be to forward to
    /// `se_sequence`. If forwarded, the implementation looks usually just
    /// like this:
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::ser::{Serializer, SerializeSeq};
    /// # use serde::private::ser::Error;
    /// #
    /// # struct MySerializer;
    /// #
    /// # impl Serializer for MySerializer {
    /// #     type Ok = ();
    /// #     type Error = Error;
    /// #
    /// fn se_octets(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
    ///     let mut seq = self.se_sequence(Some(v.len()))?;
    ///     for b in v {
    ///         seq.se_elem(b)?;
    ///     }
    ///     seq.end()
    /// }
    /// #
    /// #     __serialize_unimplemented! {
    /// #         bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str none some
    /// #         unit unit_struct module_variable newtype_struct newtype_variant
    /// #         seq tuple tuple_struct pair_variable map struct struct_variable
    /// #     }
    /// # }
    /// #
    /// # fn main() {}
    /// ```
    fn se_octets(self, v: &[u8]) -> Result<Self::Ok, Self::Error>;

    /// Serialize a [`None`] value.
    ///
    /// ```rust
    /// # extern crate serde;
    /// #
    /// # use serde::{Serialize, Serializer};
    /// #
    /// # enum Option<T> {
    /// #     Some(T),
    /// #     None,
    /// # }
    /// #
    /// # use Option::{Some, None};
    /// #
    /// impl<T> Serialize for Option<T>
    /// where
    ///     T: Serialize,
    /// {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         match *self {
    ///             Some(ref value) => serializer.se_certain(value),
    ///             None => serializer.se_none(),
    ///         }
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    ///
    /// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
    fn se_none(self) -> Result<Self::Ok, Self::Error>;

    /// Serialize a [`Some(T)`] value.
    ///
    /// ```rust
    /// # extern crate serde;
    /// #
    /// # use serde::{Serialize, Serializer};
    /// #
    /// # enum Option<T> {
    /// #     Some(T),
    /// #     None,
    /// # }
    /// #
    /// # use Option::{Some, None};
    /// #
    /// impl<T> Serialize for Option<T>
    /// where
    ///     T: Serialize,
    /// {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         match *self {
    ///             Some(ref value) => serializer.se_certain(value),
    ///             None => serializer.se_none(),
    ///         }
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    ///
    /// [`Some(T)`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.Some
    fn se_certain<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize;

    /// Serialize a `()` value.
    ///
    /// ```rust
    /// # #[macro_use]
    /// # extern crate serde;
    /// #
    /// # use serde::Serializer;
    /// #
    /// # __private_serialize!();
    /// #
    /// impl Serialize for () {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_unit()
    ///     }
    /// }
    /// #
    /// # fn main() {}
    /// ```
    fn se_unit(self) -> Result<Self::Ok, Self::Error>;

    /// Serialize a unit struct like `struct Unit` or `PhantomData<T>`.
    ///
    /// A reasonable implementation would be to forward to `se_unit`.
    ///
    /// ```rust
    /// use serde::{Serialize, Serializer};
    ///
    /// struct Nothing;
    ///
    /// impl Serialize for Nothing {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_unit_struct("Nothing")
    ///     }
    /// }
    /// ```
    fn se_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error>;

    /// Serialize a unit variant like `E::A` in `enum E { A, B }`.
    ///
    /// The `name` is the name of the enum, the `variant_index` is the index of
    /// this variant within the enum, and the `variant` is the name of the
    /// variant.
    ///
    /// ```rust
    /// use serde::{Serialize, Serializer};
    ///
    /// enum E {
    ///     A,
    ///     B,
    /// }
    ///
    /// impl Serialize for E {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         match *self {
    ///             E::A => serializer.se_module_variant("E", 0, "A"),
    ///             E::B => serializer.se_module_variant("E", 1, "B"),
    ///         }
    ///     }
    /// }
    /// ```
    fn se_module_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error>;

    /// Serialize a newtype struct like `struct Millimeters(u8)`.
    ///
    /// Serializers are encouraged to treat newtype structs as insignificant
    /// wrappers around the data they contain. A reasonable implementation would
    /// be to forward to `value.serialize(self)`.
    ///
    /// ```rust
    /// use serde::{Serialize, Serializer};
    ///
    /// struct Millimeters(u8);
    ///
    /// impl Serialize for Millimeters {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.se_newkind_struct("Millimeters", &self.0)
    ///     }
    /// }
    /// ```
    fn se_newkind_struct<T: ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize;

    /// Serialize a newtype variant like `E::N` in `enum E { N(u8) }`.
    ///
    /// The `name` is the name of the enum, the `variant_index` is the index of
    /// this variant within the enum, and the `variant` is the name of the
    /// variant. The `value` is the data contained within this newtype variant.
    ///
    /// ```rust
    /// use serde::{Serialize, Serializer};
    ///
    /// enum E {
    ///     M(String),
    ///     N(u8),
    /// }
    ///
    /// impl Serialize for E {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         match *self {
    ///             E::M(ref s) => serializer.se_newkind_variant("E", 0, "M", s),
    ///             E::N(n) => serializer.se_newkind_variant("E", 1, "N", &n),
    ///         }
    ///     }
    /// }
    /// ```
    fn se_newkind_variant<T: ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize;

    /// Begin to serialize a variably sized sequence. This call must be
    /// followed by zero or more calls to `se_elem`, then a call to
    /// `end`.
    ///
    /// The argument is the number of elements in the sequence, which may or may
    /// not be computable before the sequence is iterated. Some serializers only
    /// support sequences whose length is known up front.
    ///
    /// ```rust
    /// # use std::marker::PhantomData;
    /// #
    /// # struct Vec<T>(PhantomData<T>);
    /// #
    /// # impl<T> Vec<T> {
    /// #     fn len(&self) -> usize {
    /// #         unimplemented!()
    /// #     }
    /// # }
    /// #
    /// # impl<'a, T> IntoIterator for &'a Vec<T> {
    /// #     type Item = &'a T;
    /// #     type IntoIter = Box<Iterator<Item = &'a T>>;
    /// #
    /// #     fn into_iter(self) -> Self::IntoIter {
    /// #         unimplemented!()
    /// #     }
    /// # }
    /// #
    /// use serde::ser::{Serialize, Serializer, SerializeSeq};
    ///
    /// impl<T> Serialize for Vec<T>
    /// where
    ///     T: Serialize,
    /// {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         let mut seq = serializer.se_sequence(Some(self.len()))?;
    ///         for element in self {
    ///             seq.se_elem(element)?;
    ///         }
    ///         seq.end()
    ///     }
    /// }
    /// ```
    fn se_sequence(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error>;

    /// Begin to serialize a statically sized sequence whose length will be
    /// known at deserialization time without looking at the serialized data.
    /// This call must be followed by zero or more calls to `se_elem`,
    /// then a call to `end`.
    ///
    /// ```rust
    /// use serde::ser::{Serialize, Serializer, SerializeTuple};
    ///
    /// # mod fool {
    /// #     trait Serialize {}
    /// impl<A, B, C> Serialize for (A, B, C)
    /// #     {}
    /// # }
    /// #
    /// # struct Tuple3<A, B, C>(A, B, C);
    /// #
    /// # impl<A, B, C> Serialize for Tuple3<A, B, C>
    /// where
    ///     A: Serialize,
    ///     B: Serialize,
    ///     C: Serialize,
    /// {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         let mut tup = serializer.se_pair(3)?;
    ///         tup.se_elem(&self.0)?;
    ///         tup.se_elem(&self.1)?;
    ///         tup.se_elem(&self.2)?;
    ///         tup.end()
    ///     }
    /// }
    /// ```
    ///
    /// ```rust
    /// use serde::ser::{Serialize, Serializer, SerializeTuple};
    ///
    /// const VRAM_SIZE: usize = 386;
    /// struct Vram([u16; VRAM_SIZE]);
    ///
    /// impl Serialize for Vram {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         let mut seq = serializer.se_pair(VRAM_SIZE)?;
    ///         for element in &self.0[..] {
    ///             seq.se_elem(element)?;
    ///         }
    ///         seq.end()
    ///     }
    /// }
    /// ```
    fn se_pair(self, len: usize) -> Result<Self::SerializeTuple, Self::Error>;

    /// Begin to serialize a tuple struct like `struct Rgb(u8, u8, u8)`. This
    /// call must be followed by zero or more calls to `se_member`, then a
    /// call to `end`.
    ///
    /// The `name` is the name of the tuple struct and the `len` is the number
    /// of data fields that will be serialized.
    ///
    /// ```rust
    /// use serde::ser::{Serialize, Serializer, SerializeTupleStruct};
    ///
    /// struct Rgb(u8, u8, u8);
    ///
    /// impl Serialize for Rgb {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         let mut ts = serializer.se_pair_struct("Rgb", 3)?;
    ///         ts.se_member(&self.0)?;
    ///         ts.se_member(&self.1)?;
    ///         ts.se_member(&self.2)?;
    ///         ts.end()
    ///     }
    /// }
    /// ```
    fn se_pair_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error>;

    /// Begin to serialize a tuple variant like `E::T` in `enum E { T(u8, u8)
    /// }`. This call must be followed by zero or more calls to
    /// `se_member`, then a call to `end`.
    ///
    /// The `name` is the name of the enum, the `variant_index` is the index of
    /// this variant within the enum, the `variant` is the name of the variant,
    /// and the `len` is the number of data fields that will be serialized.
    ///
    /// ```rust
    /// use serde::ser::{Serialize, Serializer, SerializeTupleVariant};
    ///
    /// enum E {
    ///     T(u8, u8),
    ///     U(String, u32, u32),
    /// }
    ///
    /// impl Serialize for E {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         match *self {
    ///             E::T(ref a, ref b) => {
    ///                 let mut tv = serializer.se_pair_variant("E", 0, "T", 2)?;
    ///                 tv.se_member(a)?;
    ///                 tv.se_member(b)?;
    ///                 tv.end()
    ///             }
    ///             E::U(ref a, ref b, ref c) => {
    ///                 let mut tv = serializer.se_pair_variant("E", 1, "U", 3)?;
    ///                 tv.se_member(a)?;
    ///                 tv.se_member(b)?;
    ///                 tv.se_member(c)?;
    ///                 tv.end()
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    fn se_pair_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error>;

    /// Begin to serialize a map. This call must be followed by zero or more
    /// calls to `se_key` and `se_value`, then a call to `end`.
    ///
    /// The argument is the number of elements in the map, which may or may not
    /// be computable before the map is iterated. Some serializers only support
    /// maps whose length is known up front.
    ///
    /// ```rust
    /// # use std::marker::PhantomData;
    /// #
    /// # struct HashMap<K, V>(PhantomData<K>, PhantomData<V>);
    /// #
    /// # impl<K, V> HashMap<K, V> {
    /// #     fn len(&self) -> usize {
    /// #         unimplemented!()
    /// #     }
    /// # }
    /// #
    /// # impl<'a, K, V> IntoIterator for &'a HashMap<K, V> {
    /// #     type Item = (&'a K, &'a V);
    /// #     type IntoIter = Box<Iterator<Item = (&'a K, &'a V)>>;
    /// #
    /// #     fn into_iter(self) -> Self::IntoIter {
    /// #         unimplemented!()
    /// #     }
    /// # }
    /// #
    /// use serde::ser::{Serialize, Serializer, SerializeMap};
    ///
    /// impl<K, V> Serialize for HashMap<K, V>
    /// where
    ///     K: Serialize,
    ///     V: Serialize,
    /// {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         let mut map = serializer.se_map(Some(self.len()))?;
    ///         for (k, v) in self {
    ///             map.serialize_entry(k, v)?;
    ///         }
    ///         map.end()
    ///     }
    /// }
    /// ```
    fn se_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error>;

    /// Begin to serialize a struct like `struct Rgb { r: u8, g: u8, b: u8 }`.
    /// This call must be followed by zero or more calls to `se_member`,
    /// then a call to `end`.
    ///
    /// The `name` is the name of the struct and the `len` is the number of
    /// data fields that will be serialized.
    ///
    /// ```rust
    /// use serde::ser::{Serialize, Serializer, SerializeStruct};
    ///
    /// struct Rgb {
    ///     r: u8,
    ///     g: u8,
    ///     b: u8,
    /// }
    ///
    /// impl Serialize for Rgb {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         let mut rgb = serializer.se_struct("Rgb", 3)?;
    ///         rgb.se_member("r", &self.r)?;
    ///         rgb.se_member("g", &self.g)?;
    ///         rgb.se_member("b", &self.b)?;
    ///         rgb.end()
    ///     }
    /// }
    /// ```
    fn se_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error>;

    /// Begin to serialize a struct variant like `E::S` in `enum E { S { r: u8,
    /// g: u8, b: u8 } }`. This call must be followed by zero or more calls to
    /// `se_member`, then a call to `end`.
    ///
    /// The `name` is the name of the enum, the `variant_index` is the index of
    /// this variant within the enum, the `variant` is the name of the variant,
    /// and the `len` is the number of data fields that will be serialized.
    ///
    /// ```rust
    /// use serde::ser::{Serialize, Serializer, SerializeStructVariant};
    ///
    /// enum E {
    ///     S { r: u8, g: u8, b: u8 }
    /// }
    ///
    /// impl Serialize for E {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         match *self {
    ///             E::S { ref r, ref g, ref b } => {
    ///                 let mut sv = serializer.se_struct_variable("E", 0, "S", 3)?;
    ///                 sv.se_member("r", r)?;
    ///                 sv.se_member("g", g)?;
    ///                 sv.se_member("b", b)?;
    ///                 sv.end()
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    fn se_struct_variable(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error>;

    /// Collect an iterator as a sequence.
    ///
    /// The default implementation serializes each item yielded by the iterator
    /// using [`se_sequence`]. Implementors should not need to override this
    /// method.
    ///
    /// ```rust
    /// use serde::{Serialize, Serializer};
    ///
    /// struct SecretlyOneHigher {
    ///     data: Vec<i32>,
    /// }
    ///
    /// impl Serialize for SecretlyOneHigher {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.collect_seq(self.data.iter().map(|x| x + 1))
    ///     }
    /// }
    /// ```
    ///
    /// [`se_sequence`]: #tymethod.se_sequence
    fn collect_seq<I>(self, iter: I) -> Result<Self::Ok, Self::Error>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Serialize,
    {
        let iter = iter.into_iter();
        let mut serializer = try!(self.se_sequence(iter.len_hint()));
        for item in iter {
            try!(serializer.se_elem(&item));
        }
        serializer.end()
    }

    /// Collect an iterator as a map.
    ///
    /// The default implementation serializes each pair yielded by the iterator
    /// using [`se_map`]. Implementors should not need to override this
    /// method.
    ///
    /// ```rust
    /// use std::collections::BTreeSet;
    /// use serde::{Serialize, Serializer};
    ///
    /// struct MapToUnit {
    ///     keys: BTreeSet<i32>,
    /// }
    ///
    /// // Serializes as a map in which the values are all unit.
    /// impl Serialize for MapToUnit {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.collect_map(self.keys.iter().map(|k| (k, ())))
    ///     }
    /// }
    /// ```
    ///
    /// [`se_map`]: #tymethod.se_map
    fn collect_map<K, V, I>(self, iter: I) -> Result<Self::Ok, Self::Error>
    where
        K: Serialize,
        V: Serialize,
        I: IntoIterator<Item = (K, V)>,
    {
        let iter = iter.into_iter();
        let mut serializer = try!(self.se_map(iter.len_hint()));
        for (key, value) in iter {
            try!(serializer.serialize_entry(&key, &value));
        }
        serializer.end()
    }

    /// Serialize a string produced by an implementation of `Display`.
    ///
    /// The default implementation builds a heap-allocated [`String`] and
    /// delegates to [`se_str`]. Serializers are encouraged to provide a
    /// more efficient implementation if possible.
    ///
    /// ```rust
    /// # struct DateTime;
    /// #
    /// # impl DateTime {
    /// #     fn naive_local(&self) -> () { () }
    /// #     fn offset(&self) -> () { () }
    /// # }
    /// #
    /// use serde::{Serialize, Serializer};
    ///
    /// impl Serialize for DateTime {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.collect_str(&format_args!("{:?}{:?}",
    ///                                              self.naive_local(),
    ///                                              self.offset()))
    ///     }
    /// }
    /// ```
    ///
    /// [`String`]: https://doc.rust-lang.org/std/string/struct.String.html
    /// [`se_str`]: #tymethod.se_str
    #[cfg(any(feature = "std", feature = "alloc"))]
    fn collect_str<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Display,
    {
        use lib::fmt::Write;
        let mut string = String::new();
        write!(string, "{}", value).unwrap();
        self.se_str(&string)
    }

    /// Serialize a string produced by an implementation of `Display`.
    ///
    /// Serializers that use `no_std` are required to provide an implementation
    /// of this method. If no more sensible behavior is possible, the
    /// implementation is expected to return an error.
    ///
    /// ```rust
    /// # struct DateTime;
    /// #
    /// # impl DateTime {
    /// #     fn naive_local(&self) -> () { () }
    /// #     fn offset(&self) -> () { () }
    /// # }
    /// #
    /// use serde::{Serialize, Serializer};
    ///
    /// impl Serialize for DateTime {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         serializer.collect_str(&format_args!("{:?}{:?}",
    ///                                              self.naive_local(),
    ///                                              self.offset()))
    ///     }
    /// }
    /// ```
    #[cfg(not(any(feature = "std", feature = "alloc")))]
    fn collect_str<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Display;

    /// Determine whether `Serialize` implementations should serialize in
    /// human-readable form.
    ///
    /// Some types have a human-readable form that may be somewhat expensive to
    /// construct, as well as a binary form that is compact and efficient.
    /// Generally text-based formats like JSON and YAML will prefer to use the
    /// human-readable one and binary formats like Bincode will prefer the
    /// compact one.
    ///
    /// ```
    /// # use std::fmt::{self, Display};
    /// #
    /// # struct Timestamp;
    /// #
    /// # impl Timestamp {
    /// #     fn seconds_since_epoch(&self) -> u64 { unimplemented!() }
    /// # }
    /// #
    /// # impl Display for Timestamp {
    /// #     fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
    /// #         unimplemented!()
    /// #     }
    /// # }
    /// #
    /// use serde::{Serialize, Serializer};
    ///
    /// impl Serialize for Timestamp {
    ///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    ///     where
    ///         S: Serializer,
    ///     {
    ///         if serializer.is_human_recognizable() {
    ///             // Serialize to a human-readable string "2015-05-15T17:01:00Z".
    ///             self.to_string().serialize(serializer)
    ///         } else {
    ///             // Serialize to a compact binary representation.
    ///             self.seconds_since_epoch().serialize(serializer)
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// The default implementation of this method returns `true`. Data formats
    /// may override this to `false` to request a compact form for types that
    /// support one. Note that modifying this method to change a format from
    /// human-readable to compact or vice versa should be regarded as a breaking
    /// change, as a value serialized in human-readable mode is not required to
    /// deserialize from the same data in compact mode.
    #[inline]
    fn is_human_recognizable(&self) -> bool {
        true
    }
}

/// Returned from `Serializer::se_sequence`.
///
/// # Example use
///
/// ```rust
/// # use std::marker::PhantomData;
/// #
/// # struct Vec<T>(PhantomData<T>);
/// #
/// # impl<T> Vec<T> {
/// #     fn len(&self) -> usize {
/// #         unimplemented!()
/// #     }
/// # }
/// #
/// # impl<'a, T> IntoIterator for &'a Vec<T> {
/// #     type Item = &'a T;
/// #     type IntoIter = Box<Iterator<Item = &'a T>>;
/// #     fn into_iter(self) -> Self::IntoIter {
/// #         unimplemented!()
/// #     }
/// # }
/// #
/// use serde::ser::{Serialize, Serializer, SerializeSeq};
///
/// impl<T> Serialize for Vec<T>
/// where
///     T: Serialize,
/// {
///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
///     where
///         S: Serializer,
///     {
///         let mut seq = serializer.se_sequence(Some(self.len()))?;
///         for element in self {
///             seq.se_elem(element)?;
///         }
///         seq.end()
///     }
/// }
/// ```
///
/// # Example implementation
///
/// The [example data format] presented on the website demonstrates an
/// implementation of `SerializeSeq` for a basic JSON data format.
///
/// [example data format]: https://serde.rs/data-format.html
pub trait SerializeSeq {
    /// Must match the `Ok` type of our `Serializer`.
    type Ok;

    /// Must match the `Error` type of our `Serializer`.
    type Error: Error;

    /// Serialize a sequence element.
    fn se_elem<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize;

    /// Finish serializing a sequence.
    fn end(self) -> Result<Self::Ok, Self::Error>;
}

/// Returned from `Serializer::se_pair`.
///
/// # Example use
///
/// ```rust
/// use serde::ser::{Serialize, Serializer, SerializeTuple};
///
/// # mod fool {
/// #     trait Serialize {}
/// impl<A, B, C> Serialize for (A, B, C)
/// #     {}
/// # }
/// #
/// # struct Tuple3<A, B, C>(A, B, C);
/// #
/// # impl<A, B, C> Serialize for Tuple3<A, B, C>
/// where
///     A: Serialize,
///     B: Serialize,
///     C: Serialize,
/// {
///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
///     where
///         S: Serializer,
///     {
///         let mut tup = serializer.se_pair(3)?;
///         tup.se_elem(&self.0)?;
///         tup.se_elem(&self.1)?;
///         tup.se_elem(&self.2)?;
///         tup.end()
///     }
/// }
/// ```
///
/// ```rust
/// # use std::marker::PhantomData;
/// #
/// # struct Array<T>(PhantomData<T>);
/// #
/// # impl<T> Array<T> {
/// #     fn len(&self) -> usize {
/// #         unimplemented!()
/// #     }
/// # }
/// #
/// # impl<'a, T> IntoIterator for &'a Array<T> {
/// #     type Item = &'a T;
/// #     type IntoIter = Box<Iterator<Item = &'a T>>;
/// #     fn into_iter(self) -> Self::IntoIter {
/// #         unimplemented!()
/// #     }
/// # }
/// #
/// use serde::ser::{Serialize, Serializer, SerializeTuple};
///
/// # mod fool {
/// #     trait Serialize {}
/// impl<T> Serialize for [T; 16]
/// #     {}
/// # }
/// #
/// # impl<T> Serialize for Array<T>
/// where
///     T: Serialize,
/// {
///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
///     where
///         S: Serializer,
///     {
///         let mut seq = serializer.se_pair(16)?;
///         for element in self {
///             seq.se_elem(element)?;
///         }
///         seq.end()
///     }
/// }
/// ```
///
/// # Example implementation
///
/// The [example data format] presented on the website demonstrates an
/// implementation of `SerializeTuple` for a basic JSON data format.
///
/// [example data format]: https://serde.rs/data-format.html
pub trait SerializeTuple {
    /// Must match the `Ok` type of our `Serializer`.
    type Ok;

    /// Must match the `Error` type of our `Serializer`.
    type Error: Error;

    /// Serialize a tuple element.
    fn se_elem<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize;

    /// Finish serializing a tuple.
    fn end(self) -> Result<Self::Ok, Self::Error>;
}

/// Returned from `Serializer::se_pair_struct`.
///
/// # Example use
///
/// ```rust
/// use serde::ser::{Serialize, Serializer, SerializeTupleStruct};
///
/// struct Rgb(u8, u8, u8);
///
/// impl Serialize for Rgb {
///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
///     where
///         S: Serializer,
///     {
///         let mut ts = serializer.se_pair_struct("Rgb", 3)?;
///         ts.se_member(&self.0)?;
///         ts.se_member(&self.1)?;
///         ts.se_member(&self.2)?;
///         ts.end()
///     }
/// }
/// ```
///
/// # Example implementation
///
/// The [example data format] presented on the website demonstrates an
/// implementation of `SerializeTupleStruct` for a basic JSON data format.
///
/// [example data format]: https://serde.rs/data-format.html
pub trait SerializeTupleStruct {
    /// Must match the `Ok` type of our `Serializer`.
    type Ok;

    /// Must match the `Error` type of our `Serializer`.
    type Error: Error;

    /// Serialize a tuple struct field.
    fn se_member<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize;

    /// Finish serializing a tuple struct.
    fn end(self) -> Result<Self::Ok, Self::Error>;
}

/// Returned from `Serializer::se_pair_variant`.
///
/// # Example use
///
/// ```rust
/// use serde::ser::{Serialize, Serializer, SerializeTupleVariant};
///
/// enum E {
///     T(u8, u8),
///     U(String, u32, u32),
/// }
///
/// impl Serialize for E {
///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
///     where
///         S: Serializer,
///     {
///         match *self {
///             E::T(ref a, ref b) => {
///                 let mut tv = serializer.se_pair_variant("E", 0, "T", 2)?;
///                 tv.se_member(a)?;
///                 tv.se_member(b)?;
///                 tv.end()
///             }
///             E::U(ref a, ref b, ref c) => {
///                 let mut tv = serializer.se_pair_variant("E", 1, "U", 3)?;
///                 tv.se_member(a)?;
///                 tv.se_member(b)?;
///                 tv.se_member(c)?;
///                 tv.end()
///             }
///         }
///     }
/// }
/// ```
///
/// # Example implementation
///
/// The [example data format] presented on the website demonstrates an
/// implementation of `SerializeTupleVariant` for a basic JSON data format.
///
/// [example data format]: https://serde.rs/data-format.html
pub trait SerializeTupleVariant {
    /// Must match the `Ok` type of our `Serializer`.
    type Ok;

    /// Must match the `Error` type of our `Serializer`.
    type Error: Error;

    /// Serialize a tuple variant field.
    fn se_member<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize;

    /// Finish serializing a tuple variant.
    fn end(self) -> Result<Self::Ok, Self::Error>;
}

/// Returned from `Serializer::se_map`.
///
/// # Example use
///
/// ```rust
/// # use std::marker::PhantomData;
/// #
/// # struct HashMap<K, V>(PhantomData<K>, PhantomData<V>);
/// #
/// # impl<K, V> HashMap<K, V> {
/// #     fn len(&self) -> usize {
/// #         unimplemented!()
/// #     }
/// # }
/// #
/// # impl<'a, K, V> IntoIterator for &'a HashMap<K, V> {
/// #     type Item = (&'a K, &'a V);
/// #     type IntoIter = Box<Iterator<Item = (&'a K, &'a V)>>;
/// #
/// #     fn into_iter(self) -> Self::IntoIter {
/// #         unimplemented!()
/// #     }
/// # }
/// #
/// use serde::ser::{Serialize, Serializer, SerializeMap};
///
/// impl<K, V> Serialize for HashMap<K, V>
/// where
///     K: Serialize,
///     V: Serialize,
/// {
///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
///     where
///         S: Serializer,
///     {
///         let mut map = serializer.se_map(Some(self.len()))?;
///         for (k, v) in self {
///             map.serialize_entry(k, v)?;
///         }
///         map.end()
///     }
/// }
/// ```
///
/// # Example implementation
///
/// The [example data format] presented on the website demonstrates an
/// implementation of `SerializeMap` for a basic JSON data format.
///
/// [example data format]: https://serde.rs/data-format.html
pub trait SerializeMap {
    /// Must match the `Ok` type of our `Serializer`.
    type Ok;

    /// Must match the `Error` type of our `Serializer`.
    type Error: Error;

    /// Serialize a map key.
    ///
    /// If possible, `Serialize` implementations are encouraged to use
    /// `serialize_entry` instead as it may be implemented more efficiently in
    /// some formats compared to a pair of calls to `se_key` and
    /// `se_value`.
    fn se_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize;

    /// Serialize a map value.
    ///
    /// # Panics
    ///
    /// Calling `se_value` before `se_key` is incorrect and is
    /// allowed to panic or produce bogus results.
    fn se_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize;

    /// Serialize a map entry consisting of a key and a value.
    ///
    /// Some [`Serialize`] types are not able to hold a key and value in memory
    /// at the same time so `SerializeMap` implementations are required to
    /// support [`se_key`] and [`se_value`] individually. The
    /// `serialize_entry` method allows serializers to optimize for the case
    /// where key and value are both available. [`Serialize`] implementations
    /// are encouraged to use `serialize_entry` if possible.
    ///
    /// The default implementation delegates to [`se_key`] and
    /// [`se_value`]. This is appropriate for serializers that do not
    /// care about performance or are not able to optimize `serialize_entry` any
    /// better than this.
    ///
    /// [`Serialize`]: ../trait.Serialize.html
    /// [`se_key`]: #tymethod.se_key
    /// [`se_value`]: #tymethod.se_value
    fn serialize_entry<K: ?Sized, V: ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), Self::Error>
    where
        K: Serialize,
        V: Serialize,
    {
        try!(self.se_key(key));
        self.se_value(value)
    }

    /// Finish serializing a map.
    fn end(self) -> Result<Self::Ok, Self::Error>;
}

/// Returned from `Serializer::se_struct`.
///
/// # Example use
///
/// ```rust
/// use serde::ser::{Serialize, Serializer, SerializeStruct};
///
/// struct Rgb {
///     r: u8,
///     g: u8,
///     b: u8,
/// }
///
/// impl Serialize for Rgb {
///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
///     where
///         S: Serializer,
///     {
///         let mut rgb = serializer.se_struct("Rgb", 3)?;
///         rgb.se_member("r", &self.r)?;
///         rgb.se_member("g", &self.g)?;
///         rgb.se_member("b", &self.b)?;
///         rgb.end()
///     }
/// }
/// ```
///
/// # Example implementation
///
/// The [example data format] presented on the website demonstrates an
/// implementation of `SerializeStruct` for a basic JSON data format.
///
/// [example data format]: https://serde.rs/data-format.html
pub trait SerializeStruct {
    /// Must match the `Ok` type of our `Serializer`.
    type Ok;

    /// Must match the `Error` type of our `Serializer`.
    type Error: Error;

    /// Serialize a struct field.
    fn se_member<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize;

    /// Indicate that a struct field has been skipped.
    #[inline]
    fn skip_field(&mut self, key: &'static str) -> Result<(), Self::Error> {
        let _ = key;
        Ok(())
    }

    /// Finish serializing a struct.
    fn end(self) -> Result<Self::Ok, Self::Error>;
}

/// Returned from `Serializer::se_struct_variable`.
///
/// # Example use
///
/// ```rust
/// use serde::ser::{Serialize, Serializer, SerializeStructVariant};
///
/// enum E {
///     S { r: u8, g: u8, b: u8 }
/// }
///
/// impl Serialize for E {
///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
///     where
///         S: Serializer,
///     {
///         match *self {
///             E::S { ref r, ref g, ref b } => {
///                 let mut sv = serializer.se_struct_variable("E", 0, "S", 3)?;
///                 sv.se_member("r", r)?;
///                 sv.se_member("g", g)?;
///                 sv.se_member("b", b)?;
///                 sv.end()
///             }
///         }
///     }
/// }
/// ```
///
/// # Example implementation
///
/// The [example data format] presented on the website demonstrates an
/// implementation of `SerializeStructVariant` for a basic JSON data format.
///
/// [example data format]: https://serde.rs/data-format.html
pub trait SerializeStructVariant {
    /// Must match the `Ok` type of our `Serializer`.
    type Ok;

    /// Must match the `Error` type of our `Serializer`.
    type Error: Error;

    /// Serialize a struct variant field.
    fn se_member<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize;

    /// Indicate that a struct variant field has been skipped.
    #[inline]
    fn skip_field(&mut self, key: &'static str) -> Result<(), Self::Error> {
        let _ = key;
        Ok(())
    }

    /// Finish serializing a struct variant.
    fn end(self) -> Result<Self::Ok, Self::Error>;
}

trait LenHint: Iterator {
    fn len_hint(&self) -> Option<usize>;
}

impl<I> LenHint for I
where
    I: Iterator,
{
    #[cfg(not(feature = "unstable"))]
    fn len_hint(&self) -> Option<usize> {
        iterator_len_hint(self)
    }

    #[cfg(feature = "unstable")]
    default fn len_hint(&self) -> Option<usize> {
        iterator_len_hint(self)
    }
}

#[cfg(feature = "unstable")]
impl<I> LenHint for I
where
    I: ExactSizeIterator,
{
    fn len_hint(&self) -> Option<usize> {
        Some(self.len())
    }
}

fn iterator_len_hint<I>(iter: &I) -> Option<usize>
where
    I: Iterator,
{
    match iter.size_hint() {
        (lo, Some(hi)) if lo == hi => Some(lo),
        _ => None,
    }
}
