use std::io::Write;
use std::u32;

use serde;

use byteorder::WriteBytesExt;

use super::interior::SizeLimit;
use super::{Error, ErrorKind, Result};
use cfg::Options;

/// An Serializer that encodes values directly into a Writer.
///
/// The specified byte-order will impact the endianness that is
/// used during the encoding.
///
/// This struct should not be used often.
/// For most cases, prefer the `encode_into` function.
pub(crate) struct Serializer<W, O: Options> {
    writer: W,
    _options: O,
}

impl<W: Write, O: Options> Serializer<W, O> {
    /// Creates a new Serializer with the given `Write`r.
    pub fn new(w: W, options: O) -> Serializer<W, O> {
        Serializer {
            writer: w,
            _options: options,
        }
    }
}

impl<'a, W: Write, O: Options> serde::Serializer for &'a mut Serializer<W, O> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Compound<'a, W, O>;
    type SerializeTuple = Compound<'a, W, O>;
    type SerializeTupleStruct = Compound<'a, W, O>;
    type SerializeTupleVariant = Compound<'a, W, O>;
    type SerializeMap = Compound<'a, W, O>;
    type SerializeStruct = Compound<'a, W, O>;
    type SerializeStructVariant = Compound<'a, W, O>;

    fn se_unit(self) -> Result<()> {
        Ok(())
    }

    fn se_unit_struct(self, _: &'static str) -> Result<()> {
        Ok(())
    }

    fn se_bool(self, v: bool) -> Result<()> {
        self.writer
            .write_u8(if v { 1 } else { 0 })
            .map_err(Into::into)
    }

    fn se_u8(self, v: u8) -> Result<()> {
        self.writer.write_u8(v).map_err(Into::into)
    }

    fn se_u16(self, v: u16) -> Result<()> {
        self.writer.write_u16::<O::Endian>(v).map_err(Into::into)
    }

    fn se_u32(self, v: u32) -> Result<()> {
        self.writer.write_u32::<O::Endian>(v).map_err(Into::into)
    }

    fn se_u64(self, v: u64) -> Result<()> {
        self.writer.write_u64::<O::Endian>(v).map_err(Into::into)
    }

    fn se_i8(self, v: i8) -> Result<()> {
        self.writer.write_i8(v).map_err(Into::into)
    }

    fn se_i16(self, v: i16) -> Result<()> {
        self.writer.write_i16::<O::Endian>(v).map_err(Into::into)
    }

    fn se_i32(self, v: i32) -> Result<()> {
        self.writer.write_i32::<O::Endian>(v).map_err(Into::into)
    }

    fn se_i64(self, v: i64) -> Result<()> {
        self.writer.write_i64::<O::Endian>(v).map_err(Into::into)
    }

    #[cfg(has_i128)]
    fn se_u128(self, v: u128) -> Result<()> {
        self.writer.write_u128::<O::Endian>(v).map_err(Into::into)
    }

    #[cfg(has_i128)]
    fn se_i128(self, v: i128) -> Result<()> {
        self.writer.write_i128::<O::Endian>(v).map_err(Into::into)
    }

    serde_if_integer128! {
        #[cfg(not(has_i128))]
        fn se_u128(self, v: u128) -> Result<()> {
            use serde::ser::Error;

            let _ = v;
            Err(Error::custom("u128 is not supported. Use Rustc ≥ 1.26."))
        }

        #[cfg(not(has_i128))]
        fn se_i128(self, v: i128) -> Result<()> {
            use serde::ser::Error;

            let _ = v;
            Err(Error::custom("i128 is not supported. Use Rustc ≥ 1.26."))
        }
    }

    fn se_f32(self, v: f32) -> Result<()> {
        self.writer.write_f32::<O::Endian>(v).map_err(Into::into)
    }

    fn se_f64(self, v: f64) -> Result<()> {
        self.writer.write_f64::<O::Endian>(v).map_err(Into::into)
    }

    fn se_str(self, v: &str) -> Result<()> {
        try!(self.se_u64(v.len() as u64));
        self.writer.write_all(v.as_bytes()).map_err(Into::into)
    }

    fn se_char(self, c: char) -> Result<()> {
        self.writer
            .write_all(encode_utf8(c).as_slice())
            .map_err(Into::into)
    }

    fn se_octets(self, v: &[u8]) -> Result<()> {
        try!(self.se_u64(v.len() as u64));
        self.writer.write_all(v).map_err(Into::into)
    }

    fn se_none(self) -> Result<()> {
        self.writer.write_u8(0).map_err(Into::into)
    }

    fn se_certain<T: ?Sized>(self, v: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        try!(self.writer.write_u8(1));
        v.serialize(self)
    }

    fn se_sequence(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        let len = try!(len.ok_or(ErrorKind::SequenceMustHaveLength));
        try!(self.se_u64(len as u64));
        Ok(Compound { ser: self })
    }

    fn se_pair(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(Compound { ser: self })
    }

    fn se_pair_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(Compound { ser: self })
    }

    fn se_pair_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        try!(self.se_u32(variant_index));
        Ok(Compound { ser: self })
    }

    fn se_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        let len = try!(len.ok_or(ErrorKind::SequenceMustHaveLength));
        try!(self.se_u64(len as u64));
        Ok(Compound { ser: self })
    }

    fn se_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(Compound { ser: self })
    }

    fn se_struct_variable(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        try!(self.se_u32(variant_index));
        Ok(Compound { ser: self })
    }

    fn se_newkind_struct<T: ?Sized>(self, _name: &'static str, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(self)
    }

    fn se_newkind_variant<T: ?Sized>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        try!(self.se_u32(variant_index));
        value.serialize(self)
    }

    fn se_module_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        self.se_u32(variant_index)
    }

    fn is_human_recognizable(&self) -> bool {
        false
    }
}

pub(crate) struct SizeChecker<O: Options> {
    pub options: O,
}

impl<O: Options> SizeChecker<O> {
    pub fn new(options: O) -> SizeChecker<O> {
        SizeChecker { options: options }
    }

    fn add_plain(&mut self, size: u64) -> Result<()> {
        self.options.restrain().add(size)
    }

    fn add_value<T>(&mut self, t: T) -> Result<()> {
        use std::mem::size_of_val;
        self.add_plain(size_of_val(&t) as u64)
    }
}

impl<'a, O: Options> serde::Serializer for &'a mut SizeChecker<O> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = SizeCompound<'a, O>;
    type SerializeTuple = SizeCompound<'a, O>;
    type SerializeTupleStruct = SizeCompound<'a, O>;
    type SerializeTupleVariant = SizeCompound<'a, O>;
    type SerializeMap = SizeCompound<'a, O>;
    type SerializeStruct = SizeCompound<'a, O>;
    type SerializeStructVariant = SizeCompound<'a, O>;

    fn se_unit(self) -> Result<()> {
        Ok(())
    }

    fn se_unit_struct(self, _: &'static str) -> Result<()> {
        Ok(())
    }

    fn se_bool(self, _: bool) -> Result<()> {
        self.add_value(0 as u8)
    }

    fn se_u8(self, v: u8) -> Result<()> {
        self.add_value(v)
    }

    fn se_u16(self, v: u16) -> Result<()> {
        self.add_value(v)
    }

    fn se_u32(self, v: u32) -> Result<()> {
        self.add_value(v)
    }

    fn se_u64(self, v: u64) -> Result<()> {
        self.add_value(v)
    }

    fn se_i8(self, v: i8) -> Result<()> {
        self.add_value(v)
    }

    fn se_i16(self, v: i16) -> Result<()> {
        self.add_value(v)
    }

    fn se_i32(self, v: i32) -> Result<()> {
        self.add_value(v)
    }

    fn se_i64(self, v: i64) -> Result<()> {
        self.add_value(v)
    }

    serde_if_integer128! {
        fn se_u128(self, v: u128) -> Result<()> {
            self.add_value(v)
        }

        fn se_i128(self, v: i128) -> Result<()> {
            self.add_value(v)
        }
    }

    fn se_f32(self, v: f32) -> Result<()> {
        self.add_value(v)
    }

    fn se_f64(self, v: f64) -> Result<()> {
        self.add_value(v)
    }

    fn se_str(self, v: &str) -> Result<()> {
        try!(self.add_value(0 as u64));
        self.add_plain(v.len() as u64)
    }

    fn se_char(self, c: char) -> Result<()> {
        self.add_plain(encode_utf8(c).as_slice().len() as u64)
    }

    fn se_octets(self, v: &[u8]) -> Result<()> {
        try!(self.add_value(0 as u64));
        self.add_plain(v.len() as u64)
    }

    fn se_none(self) -> Result<()> {
        self.add_value(0 as u8)
    }

    fn se_certain<T: ?Sized>(self, v: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        try!(self.add_value(1 as u8));
        v.serialize(self)
    }

    fn se_sequence(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        let len = try!(len.ok_or(ErrorKind::SequenceMustHaveLength));

        try!(self.se_u64(len as u64));
        Ok(SizeCompound { ser: self })
    }

    fn se_pair(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(SizeCompound { ser: self })
    }

    fn se_pair_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(SizeCompound { ser: self })
    }

    fn se_pair_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        try!(self.add_value(variant_index));
        Ok(SizeCompound { ser: self })
    }

    fn se_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        let len = try!(len.ok_or(ErrorKind::SequenceMustHaveLength));

        try!(self.se_u64(len as u64));
        Ok(SizeCompound { ser: self })
    }

    fn se_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(SizeCompound { ser: self })
    }

    fn se_struct_variable(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        try!(self.add_value(variant_index));
        Ok(SizeCompound { ser: self })
    }

    fn se_newkind_struct<V: serde::Serialize + ?Sized>(
        self,
        _name: &'static str,
        v: &V,
    ) -> Result<()> {
        v.serialize(self)
    }

    fn se_module_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        self.add_value(variant_index)
    }

    fn se_newkind_variant<V: serde::Serialize + ?Sized>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &V,
    ) -> Result<()> {
        try!(self.add_value(variant_index));
        value.serialize(self)
    }

    fn is_human_recognizable(&self) -> bool {
        false
    }
}

pub(crate) struct Compound<'a, W: 'a, O: Options + 'a> {
    ser: &'a mut Serializer<W, O>,
}

impl<'a, W, O> serde::ser::SerializeSeq for Compound<'a, W, O>
where
    W: Write,
    O: Options,
{
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_elem<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W, O> serde::ser::SerializeTuple for Compound<'a, W, O>
where
    W: Write,
    O: Options,
{
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_elem<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W, O> serde::ser::SerializeTupleStruct for Compound<'a, W, O>
where
    W: Write,
    O: Options,
{
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_member<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W, O> serde::ser::SerializeTupleVariant for Compound<'a, W, O>
where
    W: Write,
    O: Options,
{
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_member<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W, O> serde::ser::SerializeMap for Compound<'a, W, O>
where
    W: Write,
    O: Options,
{
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_key<K: ?Sized>(&mut self, value: &K) -> Result<()>
    where
        K: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn se_value<V: ?Sized>(&mut self, value: &V) -> Result<()>
    where
        V: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W, O> serde::ser::SerializeStruct for Compound<'a, W, O>
where
    W: Write,
    O: Options,
{
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_member<T: ?Sized>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W, O> serde::ser::SerializeStructVariant for Compound<'a, W, O>
where
    W: Write,
    O: Options,
{
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_member<T: ?Sized>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

pub(crate) struct SizeCompound<'a, S: Options + 'a> {
    ser: &'a mut SizeChecker<S>,
}

impl<'a, O: Options> serde::ser::SerializeSeq for SizeCompound<'a, O> {
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_elem<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, O: Options> serde::ser::SerializeTuple for SizeCompound<'a, O> {
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_elem<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, O: Options> serde::ser::SerializeTupleStruct for SizeCompound<'a, O> {
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_member<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, O: Options> serde::ser::SerializeTupleVariant for SizeCompound<'a, O> {
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_member<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, O: Options + 'a> serde::ser::SerializeMap for SizeCompound<'a, O> {
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_key<K: ?Sized>(&mut self, value: &K) -> Result<()>
    where
        K: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn se_value<V: ?Sized>(&mut self, value: &V) -> Result<()>
    where
        V: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, O: Options> serde::ser::SerializeStruct for SizeCompound<'a, O> {
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_member<T: ?Sized>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, O: Options> serde::ser::SerializeStructVariant for SizeCompound<'a, O> {
    type Ok = ();
    type Error = Error;

    #[inline]
    fn se_member<T: ?Sized>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}
const TAG_CONT: u8 = 0b1000_0000;
const TAG_TWO_B: u8 = 0b1100_0000;
const TAG_THREE_B: u8 = 0b1110_0000;
const TAG_FOUR_B: u8 = 0b1111_0000;
const MAX_ONE_B: u32 = 0x80;
const MAX_TWO_B: u32 = 0x800;
const MAX_THREE_B: u32 = 0x10000;

fn encode_utf8(c: char) -> EncodeUtf8 {
    let code = c as u32;
    let mut buf = [0; 4];
    let pos = if code < MAX_ONE_B {
        buf[3] = code as u8;
        3
    } else if code < MAX_TWO_B {
        buf[2] = (code >> 6 & 0x1F) as u8 | TAG_TWO_B;
        buf[3] = (code & 0x3F) as u8 | TAG_CONT;
        2
    } else if code < MAX_THREE_B {
        buf[1] = (code >> 12 & 0x0F) as u8 | TAG_THREE_B;
        buf[2] = (code >> 6 & 0x3F) as u8 | TAG_CONT;
        buf[3] = (code & 0x3F) as u8 | TAG_CONT;
        1
    } else {
        buf[0] = (code >> 18 & 0x07) as u8 | TAG_FOUR_B;
        buf[1] = (code >> 12 & 0x3F) as u8 | TAG_CONT;
        buf[2] = (code >> 6 & 0x3F) as u8 | TAG_CONT;
        buf[3] = (code & 0x3F) as u8 | TAG_CONT;
        0
    };
    EncodeUtf8 { buf: buf, pos: pos }
}

struct EncodeUtf8 {
    buf: [u8; 4],
    pos: usize,
}

impl EncodeUtf8 {
    fn as_slice(&self) -> &[u8] {
        &self.buf[self.pos..]
    }
}
