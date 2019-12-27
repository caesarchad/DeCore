use error::Result;
use serde;
use std::io;

pub trait BincodeRead<'storage>: io::Read {
    fn forward_extract_string<V>(&mut self, length: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'storage>;

    fn fetch_byte_buf(&mut self, length: usize) -> Result<Vec<u8>>;

    fn forward_extract_octets<V>(&mut self, length: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'storage>;
}

#[doc(hidden)]
pub struct SliceReader<'storage> {
    slice: &'storage [u8],
}

#[doc(hidden)]
pub struct IoReader<R> {
    reader: R,
    temp_buffer: Vec<u8>,
}

impl<'storage> SliceReader<'storage> {
    pub fn new(bytes: &'storage [u8]) -> SliceReader<'storage> {
        SliceReader { slice: bytes }
    }
}

impl<R> IoReader<R> {
    pub fn new(r: R) -> IoReader<R> {
        IoReader {
            reader: r,
            temp_buffer: vec![],
        }
    }
}

impl<'storage> io::Read for SliceReader<'storage> {
    #[inline(always)]
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        (&mut self.slice).read(out)
    }
    #[inline(always)]
    fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        (&mut self.slice).read_exact(out)
    }
}

impl<R: io::Read> io::Read for IoReader<R> {
    #[inline(always)]
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.reader.read(out)
    }
    #[inline(always)]
    fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(out)
    }
}

impl<'storage> SliceReader<'storage> {
    #[inline(always)]
    fn unanticipated_eof() -> Box<::ErrorKind> {
        return Box::new(::ErrorKind::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "",
        )));
    }
}

impl<'storage> BincodeRead<'storage> for SliceReader<'storage> {
    #[inline(always)]
    fn forward_extract_string<V>(&mut self, length: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'storage>,
    {
        use ErrorKind;
        if length > self.slice.len() {
            return Err(SliceReader::unanticipated_eof());
        }

        let string = match ::std::str::from_utf8(&self.slice[..length]) {
            Ok(s) => s,
            Err(e) => return Err(ErrorKind::InvalidUtf8Encoding(e).into()),
        };
        let r = visitor.visit_borrowed_str(string);
        self.slice = &self.slice[length..];
        r
    }

    #[inline(always)]
    fn fetch_byte_buf(&mut self, length: usize) -> Result<Vec<u8>> {
        if length > self.slice.len() {
            return Err(SliceReader::unanticipated_eof());
        }

        let r = &self.slice[..length];
        self.slice = &self.slice[length..];
        Ok(r.to_vec())
    }

    #[inline(always)]
    fn forward_extract_octets<V>(&mut self, length: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'storage>,
    {
        if length > self.slice.len() {
            return Err(SliceReader::unanticipated_eof());
        }

        let r = visitor.visit_borrowed_bytes(&self.slice[..length]);
        self.slice = &self.slice[length..];
        r
    }
}

impl<R> IoReader<R>
where
    R: io::Read,
{
    fn fill_buf(&mut self, length: usize) -> Result<()> {
        let current_length = self.temp_buffer.len();
        if length > current_length {
            self.temp_buffer.reserve_exact(length - current_length);
        }

        unsafe {
            self.temp_buffer.set_len(length);
        }

        self.reader.read_exact(&mut self.temp_buffer)?;
        Ok(())
    }
}

impl<R> BincodeRead<'static> for IoReader<R>
where
    R: io::Read,
{
    fn forward_extract_string<V>(&mut self, length: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'static>,
    {
        self.fill_buf(length)?;

        let string = match ::std::str::from_utf8(&self.temp_buffer[..]) {
            Ok(s) => s,
            Err(e) => return Err(::ErrorKind::InvalidUtf8Encoding(e).into()),
        };

        let r = visitor.visit_str(string);
        r
    }

    fn fetch_byte_buf(&mut self, length: usize) -> Result<Vec<u8>> {
        self.fill_buf(length)?;
        Ok(::std::mem::replace(&mut self.temp_buffer, Vec::new()))
    }

    fn forward_extract_octets<V>(&mut self, length: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'static>,
    {
        self.fill_buf(length)?;
        let r = visitor.visit_bytes(&self.temp_buffer[..]);
        r
    }
}
