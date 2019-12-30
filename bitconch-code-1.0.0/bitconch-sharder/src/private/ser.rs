// Copyright 2017 Serde Developers
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use lib::*;

use ser::{self, Impossible, Serialize, SerializeMap, SerializeStruct, Serializer};

#[cfg(any(feature = "std", feature = "alloc"))]
use self::content::{
    Content, ContentSerializer, SerializeStructVariantAsMapValue, SerializeTupleVariantAsMapValue,
};

/// Used to check that serde(getter) attributes return the expected type.
/// Not public API.
pub fn constrain<T: ?Sized>(t: &T) -> &T {
    t
}

/// Not public API.
pub fn serialize_tagged_newtype<S, T>(
    serializer: S,
    type_ident: &'static str,
    variant_ident: &'static str,
    tag: &'static str,
    variant_name: &'static str,
    value: &T,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    value.serialize(TaggedSerializer {
        type_ident: type_ident,
        variant_ident: variant_ident,
        tag: tag,
        variant_name: variant_name,
        delegate: serializer,
    })
}

struct TaggedSerializer<S> {
    type_ident: &'static str,
    variant_ident: &'static str,
    tag: &'static str,
    variant_name: &'static str,
    delegate: S,
}

enum Unsupported {
    Boolean,
    Integer,
    Float,
    Char,
    String,
    ByteArray,
    Optional,
    Unit,
    #[cfg(any(feature = "std", feature = "alloc"))]
    UnitStruct,
    Sequence,
    Tuple,
    TupleStruct,
    Enum,
}

impl Display for Unsupported {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Unsupported::Boolean => formatter.write_str("a boolean"),
            Unsupported::Integer => formatter.write_str("an integer"),
            Unsupported::Float => formatter.write_str("a float"),
            Unsupported::Char => formatter.write_str("a char"),
            Unsupported::String => formatter.write_str("a string"),
            Unsupported::ByteArray => formatter.write_str("a byte array"),
            Unsupported::Optional => formatter.write_str("an optional"),
            Unsupported::Unit => formatter.write_str("unit"),
            #[cfg(any(feature = "std", feature = "alloc"))]
            Unsupported::UnitStruct => formatter.write_str("unit struct"),
            Unsupported::Sequence => formatter.write_str("a sequence"),
            Unsupported::Tuple => formatter.write_str("a tuple"),
            Unsupported::TupleStruct => formatter.write_str("a tuple struct"),
            Unsupported::Enum => formatter.write_str("an enum"),
        }
    }
}

impl<S> TaggedSerializer<S>
where
    S: Serializer,
{
    fn bad_type(self, what: Unsupported) -> S::Error {
        ser::Error::custom(format_args!(
            "cannot serialize tagged newtype variant {}::{} containing {}",
            self.type_ident, self.variant_ident, what
        ))
    }
}

impl<S> Serializer for TaggedSerializer<S>
where
    S: Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;

    type SerializeSeq = Impossible<S::Ok, S::Error>;
    type SerializeTuple = Impossible<S::Ok, S::Error>;
    type SerializeTupleStruct = Impossible<S::Ok, S::Error>;
    type SerializeMap = S::SerializeMap;
    type SerializeStruct = S::SerializeStruct;

    #[cfg(not(any(feature = "std", feature = "alloc")))]
    type SerializeTupleVariant = Impossible<S::Ok, S::Error>;
    #[cfg(any(feature = "std", feature = "alloc"))]
    type SerializeTupleVariant = SerializeTupleVariantAsMapValue<S::SerializeMap>;

    #[cfg(not(any(feature = "std", feature = "alloc")))]
    type SerializeStructVariant = Impossible<S::Ok, S::Error>;
    #[cfg(any(feature = "std", feature = "alloc"))]
    type SerializeStructVariant = SerializeStructVariantAsMapValue<S::SerializeMap>;

    fn se_bool(self, _: bool) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Boolean))
    }

    fn se_i8(self, _: i8) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_i16(self, _: i16) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_i32(self, _: i32) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_i64(self, _: i64) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_u8(self, _: u8) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_u16(self, _: u16) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_u32(self, _: u32) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_u64(self, _: u64) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_f32(self, _: f32) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Float))
    }

    fn se_f64(self, _: f64) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Float))
    }

    fn se_char(self, _: char) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Char))
    }

    fn se_str(self, _: &str) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::String))
    }

    fn se_octets(self, _: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::ByteArray))
    }

    fn se_none(self) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Optional))
    }

    fn se_certain<T: ?Sized>(self, _: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(self.bad_type(Unsupported::Optional))
    }

    fn se_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Unit))
    }

    fn se_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> {
        let mut map = try!(self.delegate.se_map(Some(1)));
        try!(map.serialize_entry(self.tag, self.variant_name));
        map.end()
    }

    fn se_module_variant(
        self,
        _: &'static str,
        _: u32,
        inner_variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        let mut map = try!(self.delegate.se_map(Some(2)));
        try!(map.serialize_entry(self.tag, self.variant_name));
        try!(map.serialize_entry(inner_variant, &()));
        map.end()
    }

    fn se_newkind_struct<T: ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn se_newkind_variant<T: ?Sized>(
        self,
        _: &'static str,
        _: u32,
        inner_variant: &'static str,
        inner_value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        let mut map = try!(self.delegate.se_map(Some(2)));
        try!(map.serialize_entry(self.tag, self.variant_name));
        try!(map.serialize_entry(inner_variant, inner_value));
        map.end()
    }

    fn se_sequence(self, _: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(self.bad_type(Unsupported::Sequence))
    }

    fn se_pair(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(self.bad_type(Unsupported::Tuple))
    }

    fn se_pair_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(self.bad_type(Unsupported::TupleStruct))
    }

    #[cfg(not(any(feature = "std", feature = "alloc")))]
    fn se_pair_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        // Lack of push-based serialization means we need to buffer the content
        // of the tuple variant, so it requires std.
        Err(self.bad_type(Unsupported::Enum))
    }

    #[cfg(any(feature = "std", feature = "alloc"))]
    fn se_pair_variant(
        self,
        _: &'static str,
        _: u32,
        inner_variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        let mut map = try!(self.delegate.se_map(Some(2)));
        try!(map.serialize_entry(self.tag, self.variant_name));
        try!(map.se_key(inner_variant));
        Ok(SerializeTupleVariantAsMapValue::new(
            map,
            inner_variant,
            len,
        ))
    }

    fn se_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let mut map = try!(self.delegate.se_map(len.map(|len| len + 1)));
        try!(map.serialize_entry(self.tag, self.variant_name));
        Ok(map)
    }

    fn se_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        let mut state = try!(self.delegate.se_struct(name, len + 1));
        try!(state.se_member(self.tag, self.variant_name));
        Ok(state)
    }

    #[cfg(not(any(feature = "std", feature = "alloc")))]
    fn se_struct_variable(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        // Lack of push-based serialization means we need to buffer the content
        // of the struct variant, so it requires std.
        Err(self.bad_type(Unsupported::Enum))
    }

    #[cfg(any(feature = "std", feature = "alloc"))]
    fn se_struct_variable(
        self,
        _: &'static str,
        _: u32,
        inner_variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        let mut map = try!(self.delegate.se_map(Some(2)));
        try!(map.serialize_entry(self.tag, self.variant_name));
        try!(map.se_key(inner_variant));
        Ok(SerializeStructVariantAsMapValue::new(
            map,
            inner_variant,
            len,
        ))
    }

    #[cfg(not(any(feature = "std", feature = "alloc")))]
    fn collect_str<T: ?Sized>(self, _: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Display,
    {
        Err(self.bad_type(Unsupported::String))
    }
}

/// Used only by Serde doc tests. Not public API.
#[doc(hidden)]
#[derive(Debug)]
pub struct Error;

impl ser::Error for Error {
    fn custom<T>(_: T) -> Self
    where
        T: Display,
    {
        unimplemented!()
    }
}

#[cfg(feature = "std")]
impl error::Error for Error {
    fn description(&self) -> &str {
        unimplemented!()
    }
}

impl Display for Error {
    fn fmt(&self, _: &mut fmt::Formatter) -> fmt::Result {
        unimplemented!()
    }
}

#[cfg(any(feature = "std", feature = "alloc"))]
mod content {
    use lib::*;

    use ser::{self, Serialize, Serializer};

    pub struct SerializeTupleVariantAsMapValue<M> {
        map: M,
        name: &'static str,
        fields: Vec<Content>,
    }

    impl<M> SerializeTupleVariantAsMapValue<M> {
        pub fn new(map: M, name: &'static str, len: usize) -> Self {
            SerializeTupleVariantAsMapValue {
                map: map,
                name: name,
                fields: Vec::with_capacity(len),
            }
        }
    }

    impl<M> ser::SerializeTupleVariant for SerializeTupleVariantAsMapValue<M>
    where
        M: ser::SerializeMap,
    {
        type Ok = M::Ok;
        type Error = M::Error;

        fn se_member<T: ?Sized>(&mut self, value: &T) -> Result<(), M::Error>
        where
            T: Serialize,
        {
            let value = try!(value.serialize(ContentSerializer::<M::Error>::new()));
            self.fields.push(value);
            Ok(())
        }

        fn end(mut self) -> Result<M::Ok, M::Error> {
            try!(
                self.map
                    .se_value(&Content::TupleStruct(self.name, self.fields))
            );
            self.map.end()
        }
    }

    pub struct SerializeStructVariantAsMapValue<M> {
        map: M,
        name: &'static str,
        fields: Vec<(&'static str, Content)>,
    }

    impl<M> SerializeStructVariantAsMapValue<M> {
        pub fn new(map: M, name: &'static str, len: usize) -> Self {
            SerializeStructVariantAsMapValue {
                map: map,
                name: name,
                fields: Vec::with_capacity(len),
            }
        }
    }

    impl<M> ser::SerializeStructVariant for SerializeStructVariantAsMapValue<M>
    where
        M: ser::SerializeMap,
    {
        type Ok = M::Ok;
        type Error = M::Error;

        fn se_member<T: ?Sized>(
            &mut self,
            key: &'static str,
            value: &T,
        ) -> Result<(), M::Error>
        where
            T: Serialize,
        {
            let value = try!(value.serialize(ContentSerializer::<M::Error>::new()));
            self.fields.push((key, value));
            Ok(())
        }

        fn end(mut self) -> Result<M::Ok, M::Error> {
            try!(
                self.map
                    .se_value(&Content::Struct(self.name, self.fields))
            );
            self.map.end()
        }
    }

    #[derive(Debug)]
    pub enum Content {
        Bool(bool),

        U8(u8),
        U16(u16),
        U32(u32),
        U64(u64),

        I8(i8),
        I16(i16),
        I32(i32),
        I64(i64),

        F32(f32),
        F64(f64),

        Char(char),
        String(String),
        Bytes(Vec<u8>),

        None,
        Some(Box<Content>),

        Unit,
        UnitStruct(&'static str),
        UnitVariant(&'static str, u32, &'static str),
        NewtypeStruct(&'static str, Box<Content>),
        NewtypeVariant(&'static str, u32, &'static str, Box<Content>),

        Seq(Vec<Content>),
        Tuple(Vec<Content>),
        TupleStruct(&'static str, Vec<Content>),
        TupleVariant(&'static str, u32, &'static str, Vec<Content>),
        Map(Vec<(Content, Content)>),
        Struct(&'static str, Vec<(&'static str, Content)>),
        StructVariant(
            &'static str,
            u32,
            &'static str,
            Vec<(&'static str, Content)>,
        ),
    }

    impl Serialize for Content {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match *self {
                Content::Bool(b) => serializer.se_bool(b),
                Content::U8(u) => serializer.se_u8(u),
                Content::U16(u) => serializer.se_u16(u),
                Content::U32(u) => serializer.se_u32(u),
                Content::U64(u) => serializer.se_u64(u),
                Content::I8(i) => serializer.se_i8(i),
                Content::I16(i) => serializer.se_i16(i),
                Content::I32(i) => serializer.se_i32(i),
                Content::I64(i) => serializer.se_i64(i),
                Content::F32(f) => serializer.se_f32(f),
                Content::F64(f) => serializer.se_f64(f),
                Content::Char(c) => serializer.se_char(c),
                Content::String(ref s) => serializer.se_str(s),
                Content::Bytes(ref b) => serializer.se_octets(b),
                Content::None => serializer.se_none(),
                Content::Some(ref c) => serializer.se_certain(&**c),
                Content::Unit => serializer.se_unit(),
                Content::UnitStruct(n) => serializer.se_unit_struct(n),
                Content::UnitVariant(n, i, v) => serializer.se_module_variant(n, i, v),
                Content::NewtypeStruct(n, ref c) => serializer.se_newkind_struct(n, &**c),
                Content::NewtypeVariant(n, i, v, ref c) => {
                    serializer.se_newkind_variant(n, i, v, &**c)
                }
                Content::Seq(ref elements) => elements.serialize(serializer),
                Content::Tuple(ref elements) => {
                    use ser::SerializeTuple;
                    let mut tuple = try!(serializer.se_pair(elements.len()));
                    for e in elements {
                        try!(tuple.se_elem(e));
                    }
                    tuple.end()
                }
                Content::TupleStruct(n, ref fields) => {
                    use ser::SerializeTupleStruct;
                    let mut ts = try!(serializer.se_pair_struct(n, fields.len()));
                    for f in fields {
                        try!(ts.se_member(f));
                    }
                    ts.end()
                }
                Content::TupleVariant(n, i, v, ref fields) => {
                    use ser::SerializeTupleVariant;
                    let mut tv = try!(serializer.se_pair_variant(n, i, v, fields.len()));
                    for f in fields {
                        try!(tv.se_member(f));
                    }
                    tv.end()
                }
                Content::Map(ref entries) => {
                    use ser::SerializeMap;
                    let mut map = try!(serializer.se_map(Some(entries.len())));
                    for &(ref k, ref v) in entries {
                        try!(map.serialize_entry(k, v));
                    }
                    map.end()
                }
                Content::Struct(n, ref fields) => {
                    use ser::SerializeStruct;
                    let mut s = try!(serializer.se_struct(n, fields.len()));
                    for &(k, ref v) in fields {
                        try!(s.se_member(k, v));
                    }
                    s.end()
                }
                Content::StructVariant(n, i, v, ref fields) => {
                    use ser::SerializeStructVariant;
                    let mut sv = try!(serializer.se_struct_variable(n, i, v, fields.len()));
                    for &(k, ref v) in fields {
                        try!(sv.se_member(k, v));
                    }
                    sv.end()
                }
            }
        }
    }

    pub struct ContentSerializer<E> {
        error: PhantomData<E>,
    }

    impl<E> ContentSerializer<E> {
        pub fn new() -> Self {
            ContentSerializer { error: PhantomData }
        }
    }

    impl<E> Serializer for ContentSerializer<E>
    where
        E: ser::Error,
    {
        type Ok = Content;
        type Error = E;

        type SerializeSeq = SerializeSeq<E>;
        type SerializeTuple = SerializeTuple<E>;
        type SerializeTupleStruct = SerializeTupleStruct<E>;
        type SerializeTupleVariant = SerializeTupleVariant<E>;
        type SerializeMap = SerializeMap<E>;
        type SerializeStruct = SerializeStruct<E>;
        type SerializeStructVariant = SerializeStructVariant<E>;

        fn se_bool(self, v: bool) -> Result<Content, E> {
            Ok(Content::Bool(v))
        }

        fn se_i8(self, v: i8) -> Result<Content, E> {
            Ok(Content::I8(v))
        }

        fn se_i16(self, v: i16) -> Result<Content, E> {
            Ok(Content::I16(v))
        }

        fn se_i32(self, v: i32) -> Result<Content, E> {
            Ok(Content::I32(v))
        }

        fn se_i64(self, v: i64) -> Result<Content, E> {
            Ok(Content::I64(v))
        }

        fn se_u8(self, v: u8) -> Result<Content, E> {
            Ok(Content::U8(v))
        }

        fn se_u16(self, v: u16) -> Result<Content, E> {
            Ok(Content::U16(v))
        }

        fn se_u32(self, v: u32) -> Result<Content, E> {
            Ok(Content::U32(v))
        }

        fn se_u64(self, v: u64) -> Result<Content, E> {
            Ok(Content::U64(v))
        }

        fn se_f32(self, v: f32) -> Result<Content, E> {
            Ok(Content::F32(v))
        }

        fn se_f64(self, v: f64) -> Result<Content, E> {
            Ok(Content::F64(v))
        }

        fn se_char(self, v: char) -> Result<Content, E> {
            Ok(Content::Char(v))
        }

        fn se_str(self, value: &str) -> Result<Content, E> {
            Ok(Content::String(value.to_owned()))
        }

        fn se_octets(self, value: &[u8]) -> Result<Content, E> {
            Ok(Content::Bytes(value.to_owned()))
        }

        fn se_none(self) -> Result<Content, E> {
            Ok(Content::None)
        }

        fn se_certain<T: ?Sized>(self, value: &T) -> Result<Content, E>
        where
            T: Serialize,
        {
            Ok(Content::Some(Box::new(try!(value.serialize(self)))))
        }

        fn se_unit(self) -> Result<Content, E> {
            Ok(Content::Unit)
        }

        fn se_unit_struct(self, name: &'static str) -> Result<Content, E> {
            Ok(Content::UnitStruct(name))
        }

        fn se_module_variant(
            self,
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
        ) -> Result<Content, E> {
            Ok(Content::UnitVariant(name, variant_index, variant))
        }

        fn se_newkind_struct<T: ?Sized>(
            self,
            name: &'static str,
            value: &T,
        ) -> Result<Content, E>
        where
            T: Serialize,
        {
            Ok(Content::NewtypeStruct(
                name,
                Box::new(try!(value.serialize(self))),
            ))
        }

        fn se_newkind_variant<T: ?Sized>(
            self,
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
            value: &T,
        ) -> Result<Content, E>
        where
            T: Serialize,
        {
            Ok(Content::NewtypeVariant(
                name,
                variant_index,
                variant,
                Box::new(try!(value.serialize(self))),
            ))
        }

        fn se_sequence(self, len: Option<usize>) -> Result<Self::SerializeSeq, E> {
            Ok(SerializeSeq {
                elements: Vec::with_capacity(len.unwrap_or(0)),
                error: PhantomData,
            })
        }

        fn se_pair(self, len: usize) -> Result<Self::SerializeTuple, E> {
            Ok(SerializeTuple {
                elements: Vec::with_capacity(len),
                error: PhantomData,
            })
        }

        fn se_pair_struct(
            self,
            name: &'static str,
            len: usize,
        ) -> Result<Self::SerializeTupleStruct, E> {
            Ok(SerializeTupleStruct {
                name: name,
                fields: Vec::with_capacity(len),
                error: PhantomData,
            })
        }

        fn se_pair_variant(
            self,
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
            len: usize,
        ) -> Result<Self::SerializeTupleVariant, E> {
            Ok(SerializeTupleVariant {
                name: name,
                variant_index: variant_index,
                variant: variant,
                fields: Vec::with_capacity(len),
                error: PhantomData,
            })
        }

        fn se_map(self, len: Option<usize>) -> Result<Self::SerializeMap, E> {
            Ok(SerializeMap {
                entries: Vec::with_capacity(len.unwrap_or(0)),
                key: None,
                error: PhantomData,
            })
        }

        fn se_struct(
            self,
            name: &'static str,
            len: usize,
        ) -> Result<Self::SerializeStruct, E> {
            Ok(SerializeStruct {
                name: name,
                fields: Vec::with_capacity(len),
                error: PhantomData,
            })
        }

        fn se_struct_variable(
            self,
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
            len: usize,
        ) -> Result<Self::SerializeStructVariant, E> {
            Ok(SerializeStructVariant {
                name: name,
                variant_index: variant_index,
                variant: variant,
                fields: Vec::with_capacity(len),
                error: PhantomData,
            })
        }
    }

    pub struct SerializeSeq<E> {
        elements: Vec<Content>,
        error: PhantomData<E>,
    }

    impl<E> ser::SerializeSeq for SerializeSeq<E>
    where
        E: ser::Error,
    {
        type Ok = Content;
        type Error = E;

        fn se_elem<T: ?Sized>(&mut self, value: &T) -> Result<(), E>
        where
            T: Serialize,
        {
            let value = try!(value.serialize(ContentSerializer::<E>::new()));
            self.elements.push(value);
            Ok(())
        }

        fn end(self) -> Result<Content, E> {
            Ok(Content::Seq(self.elements))
        }
    }

    pub struct SerializeTuple<E> {
        elements: Vec<Content>,
        error: PhantomData<E>,
    }

    impl<E> ser::SerializeTuple for SerializeTuple<E>
    where
        E: ser::Error,
    {
        type Ok = Content;
        type Error = E;

        fn se_elem<T: ?Sized>(&mut self, value: &T) -> Result<(), E>
        where
            T: Serialize,
        {
            let value = try!(value.serialize(ContentSerializer::<E>::new()));
            self.elements.push(value);
            Ok(())
        }

        fn end(self) -> Result<Content, E> {
            Ok(Content::Tuple(self.elements))
        }
    }

    pub struct SerializeTupleStruct<E> {
        name: &'static str,
        fields: Vec<Content>,
        error: PhantomData<E>,
    }

    impl<E> ser::SerializeTupleStruct for SerializeTupleStruct<E>
    where
        E: ser::Error,
    {
        type Ok = Content;
        type Error = E;

        fn se_member<T: ?Sized>(&mut self, value: &T) -> Result<(), E>
        where
            T: Serialize,
        {
            let value = try!(value.serialize(ContentSerializer::<E>::new()));
            self.fields.push(value);
            Ok(())
        }

        fn end(self) -> Result<Content, E> {
            Ok(Content::TupleStruct(self.name, self.fields))
        }
    }

    pub struct SerializeTupleVariant<E> {
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        fields: Vec<Content>,
        error: PhantomData<E>,
    }

    impl<E> ser::SerializeTupleVariant for SerializeTupleVariant<E>
    where
        E: ser::Error,
    {
        type Ok = Content;
        type Error = E;

        fn se_member<T: ?Sized>(&mut self, value: &T) -> Result<(), E>
        where
            T: Serialize,
        {
            let value = try!(value.serialize(ContentSerializer::<E>::new()));
            self.fields.push(value);
            Ok(())
        }

        fn end(self) -> Result<Content, E> {
            Ok(Content::TupleVariant(
                self.name,
                self.variant_index,
                self.variant,
                self.fields,
            ))
        }
    }

    pub struct SerializeMap<E> {
        entries: Vec<(Content, Content)>,
        key: Option<Content>,
        error: PhantomData<E>,
    }

    impl<E> ser::SerializeMap for SerializeMap<E>
    where
        E: ser::Error,
    {
        type Ok = Content;
        type Error = E;

        fn se_key<T: ?Sized>(&mut self, key: &T) -> Result<(), E>
        where
            T: Serialize,
        {
            let key = try!(key.serialize(ContentSerializer::<E>::new()));
            self.key = Some(key);
            Ok(())
        }

        fn se_value<T: ?Sized>(&mut self, value: &T) -> Result<(), E>
        where
            T: Serialize,
        {
            let key = self
                .key
                .take()
                .expect("se_value called before se_key");
            let value = try!(value.serialize(ContentSerializer::<E>::new()));
            self.entries.push((key, value));
            Ok(())
        }

        fn end(self) -> Result<Content, E> {
            Ok(Content::Map(self.entries))
        }

        fn serialize_entry<K: ?Sized, V: ?Sized>(&mut self, key: &K, value: &V) -> Result<(), E>
        where
            K: Serialize,
            V: Serialize,
        {
            let key = try!(key.serialize(ContentSerializer::<E>::new()));
            let value = try!(value.serialize(ContentSerializer::<E>::new()));
            self.entries.push((key, value));
            Ok(())
        }
    }

    pub struct SerializeStruct<E> {
        name: &'static str,
        fields: Vec<(&'static str, Content)>,
        error: PhantomData<E>,
    }

    impl<E> ser::SerializeStruct for SerializeStruct<E>
    where
        E: ser::Error,
    {
        type Ok = Content;
        type Error = E;

        fn se_member<T: ?Sized>(&mut self, key: &'static str, value: &T) -> Result<(), E>
        where
            T: Serialize,
        {
            let value = try!(value.serialize(ContentSerializer::<E>::new()));
            self.fields.push((key, value));
            Ok(())
        }

        fn end(self) -> Result<Content, E> {
            Ok(Content::Struct(self.name, self.fields))
        }
    }

    pub struct SerializeStructVariant<E> {
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        fields: Vec<(&'static str, Content)>,
        error: PhantomData<E>,
    }

    impl<E> ser::SerializeStructVariant for SerializeStructVariant<E>
    where
        E: ser::Error,
    {
        type Ok = Content;
        type Error = E;

        fn se_member<T: ?Sized>(&mut self, key: &'static str, value: &T) -> Result<(), E>
        where
            T: Serialize,
        {
            let value = try!(value.serialize(ContentSerializer::<E>::new()));
            self.fields.push((key, value));
            Ok(())
        }

        fn end(self) -> Result<Content, E> {
            Ok(Content::StructVariant(
                self.name,
                self.variant_index,
                self.variant,
                self.fields,
            ))
        }
    }
}

#[cfg(any(feature = "std", feature = "alloc"))]
pub struct FlatMapSerializer<'a, M: 'a>(pub &'a mut M);

#[cfg(any(feature = "std", feature = "alloc"))]
impl<'a, M> FlatMapSerializer<'a, M>
where
    M: SerializeMap + 'a,
{
    fn bad_type(self, what: Unsupported) -> M::Error {
        ser::Error::custom(format_args!(
            "can only flatten structs and maps (got {})",
            what
        ))
    }
}

#[cfg(any(feature = "std", feature = "alloc"))]
impl<'a, M> Serializer for FlatMapSerializer<'a, M>
where
    M: SerializeMap + 'a,
{
    type Ok = ();
    type Error = M::Error;

    type SerializeSeq = Impossible<Self::Ok, M::Error>;
    type SerializeTuple = Impossible<Self::Ok, M::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, M::Error>;
    type SerializeMap = FlatMapSerializeMap<'a, M>;
    type SerializeStruct = FlatMapSerializeStruct<'a, M>;
    type SerializeTupleVariant = Impossible<Self::Ok, M::Error>;
    type SerializeStructVariant = FlatMapSerializeStructVariantAsMapValue<'a, M>;

    fn se_bool(self, _: bool) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Boolean))
    }

    fn se_i8(self, _: i8) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_i16(self, _: i16) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_i32(self, _: i32) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_i64(self, _: i64) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_u8(self, _: u8) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_u16(self, _: u16) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_u32(self, _: u32) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_u64(self, _: u64) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Integer))
    }

    fn se_f32(self, _: f32) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Float))
    }

    fn se_f64(self, _: f64) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Float))
    }

    fn se_char(self, _: char) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Char))
    }

    fn se_str(self, _: &str) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::String))
    }

    fn se_octets(self, _: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::ByteArray))
    }

    fn se_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn se_certain<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn se_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Unit))
    }

    fn se_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::UnitStruct))
    }

    fn se_module_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(self.bad_type(Unsupported::Enum))
    }

    fn se_newkind_struct<T: ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn se_newkind_variant<T: ?Sized>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        try!(self.0.se_key(variant));
        self.0.se_value(value)
    }

    fn se_sequence(self, _: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(self.bad_type(Unsupported::Sequence))
    }

    fn se_pair(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(self.bad_type(Unsupported::Tuple))
    }

    fn se_pair_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(self.bad_type(Unsupported::TupleStruct))
    }

    fn se_pair_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(self.bad_type(Unsupported::Enum))
    }

    fn se_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(FlatMapSerializeMap(self.0))
    }

    fn se_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(FlatMapSerializeStruct(self.0))
    }

    fn se_struct_variable(
        self,
        _: &'static str,
        _: u32,
        inner_variant: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        try!(self.0.se_key(inner_variant));
        Ok(FlatMapSerializeStructVariantAsMapValue::new(
            self.0,
            inner_variant,
        ))
    }
}

#[cfg(any(feature = "std", feature = "alloc"))]
pub struct FlatMapSerializeMap<'a, M: 'a>(&'a mut M);

#[cfg(any(feature = "std", feature = "alloc"))]
impl<'a, M> ser::SerializeMap for FlatMapSerializeMap<'a, M>
where
    M: SerializeMap + 'a,
{
    type Ok = ();
    type Error = M::Error;

    fn se_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.0.se_key(key)
    }

    fn se_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.0.se_value(value)
    }

    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(any(feature = "std", feature = "alloc"))]
pub struct FlatMapSerializeStruct<'a, M: 'a>(&'a mut M);

#[cfg(any(feature = "std", feature = "alloc"))]
impl<'a, M> ser::SerializeStruct for FlatMapSerializeStruct<'a, M>
where
    M: SerializeMap + 'a,
{
    type Ok = ();
    type Error = M::Error;

    fn se_member<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.0.serialize_entry(key, value)
    }

    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(any(feature = "std", feature = "alloc"))]
pub struct FlatMapSerializeStructVariantAsMapValue<'a, M: 'a> {
    map: &'a mut M,
    name: &'static str,
    fields: Vec<(&'static str, Content)>,
}

#[cfg(any(feature = "std", feature = "alloc"))]
impl<'a, M> FlatMapSerializeStructVariantAsMapValue<'a, M>
where
    M: SerializeMap + 'a,
{
    fn new(map: &'a mut M, name: &'static str) -> FlatMapSerializeStructVariantAsMapValue<'a, M> {
        FlatMapSerializeStructVariantAsMapValue {
            map: map,
            name: name,
            fields: Vec::new(),
        }
    }
}

#[cfg(any(feature = "std", feature = "alloc"))]
impl<'a, M> ser::SerializeStructVariant for FlatMapSerializeStructVariantAsMapValue<'a, M>
where
    M: SerializeMap + 'a,
{
    type Ok = ();
    type Error = M::Error;

    fn se_member<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let value = try!(value.serialize(ContentSerializer::<M::Error>::new()));
        self.fields.push((key, value));
        Ok(())
    }

    fn end(self) -> Result<(), Self::Error> {
        try!(
            self.map
                .se_value(&Content::Struct(self.name, self.fields))
        );
        Ok(())
    }
}
