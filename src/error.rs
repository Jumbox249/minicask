use std::fmt;
use std::io;

/// Everything that can go wrong inside the store.
#[derive(Debug)]
pub enum Error {
    /// The underlying filesystem said no.
    Io(io::Error),
    /// A record failed its checksum, or its header described a record that
    /// runs past the end of the file. Carries enough context to find it.
    Corrupt {
        file_id: u64,
        offset: u64,
        detail: &'static str,
    },
    /// Keys are length-prefixed with a u32 and must be non-empty.
    InvalidKey(&'static str),
    /// Values are length-prefixed with a u32.
    ValueTooLarge(usize),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Corrupt {
                file_id,
                offset,
                detail,
            } => write!(
                f,
                "corrupt record in {file_id:010}.log at offset {offset}: {detail}"
            ),
            Error::InvalidKey(why) => write!(f, "invalid key: {why}"),
            Error::ValueTooLarge(n) => {
                write!(f, "value of {n} bytes exceeds the u32 length prefix")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
