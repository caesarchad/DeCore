use cfg::Options;
use std::io::Read;

use self::extract::BincodeRead;
use byteorder::ReadBytesExt;
use interior::SizeLimit;
use serde;
use serde::de::Error as DeError;
use serde::de::IntoDeserializer;
use {Error, ErrorKind, Result};

pub mod extract;

pub(crate) struct Deserializer<R, O: Options> {
    reader: R,
    options: O,
}

impl<'de, R: BincodeRead<'de>, O: Options> Deserializer<R, O> {
    pub(crate) fn new(r: R, options: O) -> Deserializer<R, O> {
        Deserializer {
            reader: r,
            options: options,
        }
    }

    fn extract_bytes(&mut self, count: u64) -> Result<()> {
        self.options.restrain().add(count)
    }

    fn extract_type<T>(&mut self) -> Result<()> {
        use std::mem::size_of;
        self.extract_bytes(size_of::<T>() as u64)
    }

    fn extract_vec(&mut self) -> Result<Vec<u8>> {
        let len: usize = try!(serde::Deserialize::deserialize(&mut *self));
        self.extract_bytes(len as u64)?;
        self.reader.fetch_byte_buf(len)
    }

    fn extract_str(&mut self) -> Result<String> {
        let vec = self.extract_vec()?;
        String::from_utf8(vec).map_err(|e| ErrorKind::InvalidUtf8Encoding(e.utf8_error()).into())
    }
}

macro_rules! impl_nums {
    ($ty:ty, $dser_method:ident, $visitor_method:ident, $reader_method:ident) => {
        #[inline]
        fn $dser_method<V>(self, visitor: V) -> Result<V::Value>
            where V: serde::de::Visitor<'de>,
        {
            try!(self.extract_type::<$ty>());
            let value = try!(self.reader.$reader_method::<O::Endian>());
            visitor.$visitor_method(value)
        }
    }
}

impl<'de, 'a, R, O> serde::Deserializer<'de> for &'a mut Deserializer<R, O>
where
    R: BincodeRead<'de>,
    O: Options,
{
    type Error = Error;

    #[inline]
    fn de_whatever<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Box::new(ErrorKind::DeserializeAnyNotSupported))
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let value: u8 = try!(serde::Deserialize::deserialize(self));
        match value {
            1 => visitor.visit_bool(true),
            0 => visitor.visit_bool(false),
            value => Err(ErrorKind::InvalidBoolEncoding(value).into()),
        }
    }

    impl_nums!(u16, deserialize_u16, visit_u16, read_u16);
    impl_nums!(u32, deserialize_u32, visit_u32, read_u32);
    impl_nums!(u64, deserialize_u64, visit_u64, read_u64);
    impl_nums!(i16, deserialize_i16, visit_i16, read_i16);
    impl_nums!(i32, deserialize_i32, visit_i32, read_i32);
    impl_nums!(i64, deserialize_i64, visit_i64, read_i64);
    impl_nums!(f32, deserialize_f32, visit_f32, read_f32);
    impl_nums!(f64, deserialize_f64, visit_f64, read_f64);

    #[cfg(has_i128)]
    impl_nums!(u128, de_u128, visit_u128, read_u128);

    #[cfg(has_i128)]
    impl_nums!(i128, de_i128, visit_i128, read_i128);

    serde_if_integer128! {
        #[cfg(not(has_i128))]
        fn de_u128<V>(self, visitor: V) -> Result<V::Value>
        where
            V: serde::de::Visitor<'de>
        {
            let _ = visitor;
            Err(DeError::custom("u128 is not supported. Use Rustc ≥ 1.26."))
        }

        #[cfg(not(has_i128))]
        fn de_i128<V>(self, visitor: V) -> Result<V::Value>
        where
            V: serde::de::Visitor<'de>
        {
            let _ = visitor;
            Err(DeError::custom("i128 is not supported. Use Rustc ≥ 1.26."))
        }
    }

    #[inline]
    fn de_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        try!(self.extract_type::<u8>());
        visitor.visit_u8(try!(self.reader.read_u8()))
    }

    #[inline]
    fn de_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        try!(self.extract_type::<i8>());
        visitor.visit_i8(try!(self.reader.read_i8()))
    }

    fn de_module<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn de_byte<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        use std::str;

        let error = || ErrorKind::InvalidCharEncoding.into();

        let mut buf = [0u8; 4];

        // Look at the first byte to see how many bytes must be read
        let _ = try!(self.reader.read_exact(&mut buf[..1]));
        let width = utf8_char_width(buf[0]);
        if width == 1 {
            return visitor.visit_char(buf[0] as char);
        }
        if width == 0 {
            return Err(error());
        }

        if self.reader.read_exact(&mut buf[1..width]).is_err() {
            return Err(error());
        }

        let res = try!(
            str::from_utf8(&buf[..width])
                .ok()
                .and_then(|s| s.chars().next())
                .ok_or(error())
        );
        visitor.visit_char(res)
    }

    fn de_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let len: usize = try!(serde::Deserialize::deserialize(&mut *self));
        try!(self.extract_bytes(len as u64));
        self.reader.forward_extract_string(len, visitor)
    }

    fn de_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_string(try!(self.extract_str()))
    }

    fn de_octets<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let len: usize = try!(serde::Deserialize::deserialize(&mut *self));
        try!(self.extract_bytes(len as u64));
        self.reader.forward_extract_octets(len, visitor)
    }

    fn de_octet_buffer<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_byte_buf(try!(self.extract_vec()))
    }

    fn de_enumeration<V>(
        self,
        _enum: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        impl<'de, 'a, R: 'a, O> serde::de::EnumAccess<'de> for &'a mut Deserializer<R, O>
        where
            R: BincodeRead<'de>,
            O: Options,
        {
            type Error = Error;
            type Variant = Self;

            fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
            where
                V: serde::de::DeserializeSeed<'de>,
            {
                let idx: u32 = try!(serde::de::Deserialize::deserialize(&mut *self));
                let val: Result<_> = seed.deserialize(idx.into_deserializer());
                Ok((try!(val), self))
            }
        }

        visitor.visit_enum(self)
    }

    fn de_pair<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        struct Access<'a, R: Read + 'a, O: Options + 'a> {
            deserializer: &'a mut Deserializer<R, O>,
            len: usize,
        }

        impl<'de, 'a, 'b: 'a, R: BincodeRead<'de> + 'b, O: Options> serde::de::SeqAccess<'de>
            for Access<'a, R, O>
        {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: serde::de::DeserializeSeed<'de>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let value = try!(serde::de::DeserializeSeed::deserialize(
                        seed,
                        &mut *self.deserializer,
                    ));
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        visitor.visit_seq(Access {
            deserializer: self,
            len: len,
        })
    }

    fn de_selection<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let value: u8 = try!(serde::de::Deserialize::deserialize(&mut *self));
        match value {
            0 => visitor.visit_none(),
            1 => visitor.visit_some(&mut *self),
            v => Err(ErrorKind::InvalidTagEncoding(v as usize).into()),
        }
    }

    fn de_sequence<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let len = try!(serde::Deserialize::deserialize(&mut *self));

        self.de_pair(len, visitor)
    }

    fn de_mapping<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        struct Access<'a, R: Read + 'a, O: Options + 'a> {
            deserializer: &'a mut Deserializer<R, O>,
            len: usize,
        }

        impl<'de, 'a, 'b: 'a, R: BincodeRead<'de> + 'b, O: Options> serde::de::MapAccess<'de>
            for Access<'a, R, O>
        {
            type Error = Error;

            fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
            where
                K: serde::de::DeserializeSeed<'de>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let key = try!(serde::de::DeserializeSeed::deserialize(
                        seed,
                        &mut *self.deserializer,
                    ));
                    Ok(Some(key))
                } else {
                    Ok(None)
                }
            }

            fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
            where
                V: serde::de::DeserializeSeed<'de>,
            {
                let value = try!(serde::de::DeserializeSeed::deserialize(
                    seed,
                    &mut *self.deserializer,
                ));
                Ok(value)
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        let len = try!(serde::Deserialize::deserialize(&mut *self));

        visitor.visit_map(Access {
            deserializer: self,
            len: len,
        })
    }

    fn de_struct<V>(
        self,
        _name: &str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        self.de_pair(fields.len(), visitor)
    }

    fn de_id<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let message = "Bincode does not support Deserializer::de_id";
        Err(Error::custom(message))
    }

    fn de_newtype_struct<V>(self, _name: &str, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn de_module_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn de_pair_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        self.de_pair(len, visitor)
    }

    fn de_neglected_whatever<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let message = "Bincode does not support Deserializer::de_neglected_whatever";
        Err(Error::custom(message))
    }

    fn is_human_recognizable(&self) -> bool {
        false
    }
}

impl<'de, 'a, R, O> serde::de::VariantAccess<'de> for &'a mut Deserializer<R, O>
where
    R: BincodeRead<'de>,
    O: Options,
{
    type Error = Error;

    fn module_variable(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variable_source<T>(self, seed: T) -> Result<T::Value>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        serde::de::DeserializeSeed::deserialize(seed, self)
    }

    fn pair_variable<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        serde::de::Deserializer::de_pair(self, len, visitor)
    }

    fn struct_variable<V>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        serde::de::Deserializer::de_pair(self, fields.len(), visitor)
    }
}
static UTF8_CHAR_WIDTH: [u8; 256] = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, // 0x1F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, // 0x3F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, // 0x5F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, // 0x7F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, // 0x9F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, // 0xBF
    0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, // 0xDF
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // 0xEF
    4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0xFF
];

fn utf8_char_width(b: u8) -> usize {
    UTF8_CHAR_WIDTH[b as usize] as usize
}
