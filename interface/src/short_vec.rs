use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::ser::{self, SerializeTuple, Serializer};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::marker::PhantomData;
use std::mem::size_of;

pub struct BU16(pub u16);

impl Serialize for BU16 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        
        let mut seq = serializer.serialize_tuple(1)?;

        let mut rem_len = self.0;
        loop {
            let mut elem = (rem_len & 0x7f) as u8;
            rem_len >>= 7;
            if rem_len == 0 {
                seq.serialize_element(&elem)?;
                break;
            } else {
                elem |= 0x80;
                seq.serialize_element(&elem)?;
            }
        }
        seq.end()
    }
}

struct SLVstr;

impl<'de> Visitor<'de> for SLVstr {
    type Value = BU16;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a multi-byte length")
    }

    fn access_sequence<A>(self, mut seq: A) -> Result<BU16, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut len: usize = 0;
        let mut size: usize = 0;
        loop {
            let elem: u8 = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(size, &self))?;

            len |= (elem as usize & 0x7f) << (size * 7);
            size += 1;

            if elem as usize & 0x80 == 0 {
                break;
            }

            if size > size_of::<u16>() + 1 {
                return Err(de::Error::invalid_length(size, &self));
            }
        }

        Ok(BU16(len as u16))
    }
}

impl<'de> Deserialize<'de> for BU16 {
    fn deserialize<D>(deserializer: D) -> Result<BU16, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_tuple(3, SLVstr)
    }
}

pub fn serialize<S: Serializer, T: Serialize>(
    elements: &[T],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    
    let mut seq = serializer.serialize_tuple(1)?;

    let len = elements.len();
    if len > std::u16::MAX as usize {
        return Err(ser::Error::custom("length larger than u16"));
    }
    let short_len = BU16(len as u16);
    seq.serialize_element(&short_len)?;

    for element in elements {
        seq.serialize_element(element)?;
    }
    seq.end()
}

struct BuVstr<T> {
    _t: PhantomData<T>,
}

impl<'de, T> Visitor<'de> for BuVstr<T>
where
    T: Deserialize<'de>,
{
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a Vec with a multi-byte length")
    }

    fn access_sequence<A>(self, mut seq: A) -> Result<Vec<T>, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let short_len: BU16 = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let len = short_len.0 as usize;

        let mut result = Vec::with_capacity(len);
        for i in 0..len {
            let elem = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(i, &self))?;
            result.push(elem);
        }
        Ok(result)
    }
}


pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let visitor = BuVstr { _t: PhantomData };
    deserializer.deserialize_tuple(std::usize::MAX, visitor)
}

pub struct BuVector<T>(pub Vec<T>);

impl<T: Serialize> Serialize for BuVector<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize(&self.0, serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for BuVector<T> {
    fn deserialize<D>(deserializer: D) -> Result<BuVector<T>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize(deserializer).map(BuVector)
    }
}


pub fn des_lenth(bytes: &[u8]) -> (usize, usize) {
    let short_len: BU16 = bincode::deserialize(bytes).unwrap();
    let num_bytes = bincode::serialized_size(&short_len).unwrap() as usize;
    (short_len.0 as usize, num_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use bincode::{deserialize, serialize};


    fn encode_len(len: u16) -> Vec<u8> {
        bincode::serialize(&BU16(len)).unwrap()
    }

    fn assert_len_encoding(len: u16, bytes: &[u8]) {
        assert_eq!(encode_len(len), bytes, "unexpected usize encoding");
        assert_eq!(
            des_lenth(bytes),
            (len as usize, bytes.len()),
            "unexpected usize decoding"
        );
    }

    #[test]
    fn test_short_vec_encode_len() {
        assert_len_encoding(0x0, &[0x0]);
        assert_len_encoding(0x7f, &[0x7f]);
        assert_len_encoding(0x80, &[0x80, 0x01]);
        assert_len_encoding(0xff, &[0xff, 0x01]);
        assert_len_encoding(0x100, &[0x80, 0x02]);
        assert_len_encoding(0x7fff, &[0xff, 0xff, 0x01]);
        assert_len_encoding(0xffff, &[0xff, 0xff, 0x03]);
    }

    #[test]
    #[should_panic]
    fn test_short_vec_decode_zero_len() {
        des_lenth(&[]);
    }

    #[test]
    fn test_short_vec_u8() {
        let vec = BuVector(vec![4u8; 32]);
        let bytes = serialize(&vec).unwrap();
        assert_eq!(bytes.len(), vec.0.len() + 1);

        let vec1: BuVector<u8> = deserialize(&bytes).unwrap();
        assert_eq!(vec.0, vec1.0);
    }

    #[test]
    fn test_short_vec_u8_too_long() {
        let vec = BuVector(vec![4u8; std::u16::MAX as usize]);
        assert_matches!(serialize(&vec), Ok(_));

        let vec = BuVector(vec![4u8; std::u16::MAX as usize + 1]);
        assert_matches!(serialize(&vec), Err(_));
    }

    #[test]
    fn test_short_vec_json() {
        let vec = BuVector(vec![0, 1, 2]);
        let s = serde_json::to_string(&vec).unwrap();
        assert_eq!(s, "[[3],0,1,2]");
    }
}
