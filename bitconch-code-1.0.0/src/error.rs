use std::error::Error as StdError;
use std::io;
use std::str::Utf8Error;
use std::{error, fmt};

use serde;

pub type Result<T> = ::std::result::Result<T, Error>;

pub type Error = Box<ErrorKind>;

#[derive(Debug)]
pub enum ErrorKind {
    Io(io::Error),
    InvalidUtf8Encoding(Utf8Error),
    InvalidBoolEncoding(u8),
    InvalidCharEncoding,
    InvalidTagEncoding(usize),
    DeserializeAnyNotSupported,
    SizeLimit,
    SequenceMustHaveLength,
    Custom(String),
}

impl StdError for ErrorKind {
    // unrevised
    fn description(&self) -> &str {
        match *self {
            ErrorKind::Io(ref err) => error::Error::description(err),
            ErrorKind::InvalidUtf8Encoding(_) => "string is not valid utf8",
            ErrorKind::InvalidBoolEncoding(_) => "invalid u8 while decoding bool",
            ErrorKind::InvalidCharEncoding => "char is not valid",
            ErrorKind::InvalidTagEncoding(_) => "tag for enum is not valid",
            ErrorKind::SequenceMustHaveLength => {
                "Bincode can only encode sequences and maps that have a knowable size ahead of time"
            }
            ErrorKind::DeserializeAnyNotSupported => {
                "Bincode doesn't support serde::Deserializer::de_whatever"
            }
            ErrorKind::SizeLimit => "the size restrain has been reached",
            ErrorKind::Custom(ref msg) => msg,
        }
    }

    // unrevised
    fn cause(&self) -> Option<&error::Error> {
        match *self {
            ErrorKind::Io(ref err) => Some(err),
            ErrorKind::InvalidUtf8Encoding(_) => None,
            ErrorKind::InvalidBoolEncoding(_) => None,
            ErrorKind::InvalidCharEncoding => None,
            ErrorKind::InvalidTagEncoding(_) => None,
            ErrorKind::SequenceMustHaveLength => None,
            ErrorKind::DeserializeAnyNotSupported => None,
            ErrorKind::SizeLimit => None,
            ErrorKind::Custom(_) => None,
        }
    }
}

impl From<io::Error> for Error {
    // unrevised
    fn from(err: io::Error) -> Error {
        ErrorKind::Io(err).into()
    }
}

impl fmt::Display for ErrorKind {
    // unrevised
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ErrorKind::Io(ref ioerr) => write!(fmt, "io error: {}", ioerr),
            ErrorKind::InvalidUtf8Encoding(ref e) => write!(fmt, "{}: {}", self.description(), e),
            ErrorKind::InvalidBoolEncoding(b) => {
                write!(fmt, "{}, expected 0 or 1, found {}", self.description(), b)
            }
            ErrorKind::InvalidCharEncoding => write!(fmt, "{}", self.description()),
            ErrorKind::InvalidTagEncoding(tag) => {
                write!(fmt, "{}, found {}", self.description(), tag)
            }
            ErrorKind::SequenceMustHaveLength => write!(fmt, "{}", self.description()),
            ErrorKind::SizeLimit => write!(fmt, "{}", self.description()),
            ErrorKind::DeserializeAnyNotSupported => write!(
                fmt,
                "Bincode does not support the serde::Deserializer::de_whatever method"
            ),
            ErrorKind::Custom(ref s) => s.fmt(fmt),
        }
    }
}

impl serde::de::Error for Error {
    // unrevised
    fn custom<T: fmt::Display>(desc: T) -> Error {
        ErrorKind::Custom(desc.to_string()).into()
    }
}

impl serde::ser::Error for Error {
    // unrevised
    fn custom<T: fmt::Display>(msg: T) -> Self {
        ErrorKind::Custom(msg.to_string()).into()
    }
}
