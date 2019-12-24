use serde;
use std::io::{Read, Write};

use cfg::{Options, OptionsExt};
use de::extract::BincodeRead;
use {ErrorKind, Result};

#[derive(Clone)]
struct CountSize<L: SizeLimit> {
    total: u64,
    other_limit: L,
}

// unrevised
pub(crate) fn serialize_into<W, T: ?Sized, O>(writer: W, value: &T, mut options: O) -> Result<()>
where
    W: Write,
    T: serde::Serialize,
    O: Options,
{
    if options.restrain().restrain().is_some() {
        serialized_size(value, &mut options)?;
    }

    let mut serializer = ::ser::Serializer::<_, O>::new(writer, options);
    serde::Serialize::serialize(value, &mut serializer)
}

// unrevised
pub(crate) fn serialize<T: ?Sized, O>(value: &T, mut options: O) -> Result<Vec<u8>>
where
    T: serde::Serialize,
    O: Options,
{
    let mut writer = {
        let actual_size = serialized_size(value, &mut options)?;
        Vec::with_capacity(actual_size as usize)
    };

    serialize_into(&mut writer, value, options.with_no_restrict())?;
    Ok(writer)
}

impl<L: SizeLimit> SizeLimit for CountSize<L> {
    // unrevised
    fn add(&mut self, c: u64) -> Result<()> {
        self.other_limit.add(c)?;
        self.total += c;
        Ok(())
    }

    fn restrain(&self) -> Option<u64> {
        unreachable!();
    }
}

// unrevised
pub(crate) fn serialized_size<T: ?Sized, O: Options>(value: &T, mut options: O) -> Result<u64>
where
    T: serde::Serialize,
{
    let old_limiter = options.restrain().clone();
    let mut size_counter = ::ser::SizeChecker {
        options: ::cfg::WithOtherLimit::new(
            options,
            CountSize {
                total: 0,
                other_limit: old_limiter,
            },
        ),
    };

    let result = value.serialize(&mut size_counter);
    result.map(|_| size_counter.options.new_limit.total)
}

pub(crate) fn de_via<R, T, O>(reader: R, options: O) -> Result<T>
where
    R: Read,
    T: serde::de::DeserializeOwned,
    O: Options,
{
    let reader = ::de::extract::IoReader::new(reader);
    let mut deserializer = ::de::Deserializer::<_, O>::new(reader, options);
    serde::Deserialize::deserialize(&mut deserializer)
}

pub(crate) fn de_via_specification<'a, R, T, O>(reader: R, options: O) -> Result<T>
where
    R: BincodeRead<'a>,
    T: serde::de::DeserializeOwned,
    O: Options,
{
    let mut deserializer = ::de::Deserializer::<_, O>::new(reader, options);
    serde::Deserialize::deserialize(&mut deserializer)
}

pub(crate) fn de_in_preparation<'a, R, T, O>(reader: R, options: O, place: &mut T) -> Result<()>
where
    R: BincodeRead<'a>,
    T: serde::de::Deserialize<'a>,
    O: Options,
{
    let mut deserializer = ::de::Deserializer::<_, _>::new(reader, options);
    serde::Deserialize::de_in_preparation(&mut deserializer, place)
}

// unrevised
pub(crate) fn deserialize<'a, T, O>(bytes: &'a [u8], options: O) -> Result<T>
where
    T: serde::de::Deserialize<'a>,
    O: Options,
{
    let reader = ::de::extract::SliceReader::new(bytes);
    let options = ::cfg::WithOtherLimit::new(options, Infinite);
    let mut deserializer = ::de::Deserializer::new(reader, options);
    serde::Deserialize::deserialize(&mut deserializer)
}

pub(crate) fn de_source<'a, T, O>(seed: T, bytes: &'a [u8], options: O) -> Result<T::Value>
where
    T: serde::de::DeserializeSeed<'a>,
    O: Options,
{
    let reader = ::de::extract::SliceReader::new(bytes);
    let options = ::cfg::WithOtherLimit::new(options, Infinite);
    let mut deserializer = ::de::Deserializer::new(reader, options);
    seed.deserialize(&mut deserializer)
}

pub(crate) trait SizeLimit: Clone {
    // unrevised
    fn add(&mut self, n: u64) -> Result<()>;
    fn restrain(&self) -> Option<u64>;
}

#[derive(Copy, Clone)]
pub struct Bounded(pub u64);

#[derive(Copy, Clone)]
pub struct Infinite;

impl SizeLimit for Bounded {
    // unrevised
    #[inline(always)]
    fn add(&mut self, n: u64) -> Result<()> {
        if self.0 >= n {
            self.0 -= n;
            Ok(())
        } else {
            Err(Box::new(ErrorKind::SizeLimit))
        }
    }

    #[inline(always)]
    fn restrain(&self) -> Option<u64> {
        Some(self.0)
    }
}

impl SizeLimit for Infinite {
    
    // unrevised
    #[inline(always)]
    fn add(&mut self, _: u64) -> Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn restrain(&self) -> Option<u64> {
        None
    }
}
