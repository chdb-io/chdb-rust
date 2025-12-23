use std::ffi::NulError;
use std::string::FromUtf8Error;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("An unknown error has occurred")]
    Unknown,
    #[error("No result")]
    NoResult,
    #[error("Connection failed")]
    ConnectionFailed,
    #[error("Invalid data: {0}")]
    InvalidData(String),
    #[error("Invalid path")]
    PathError,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Nul(#[from] NulError),
    #[error("Insufficient dir permissions")]
    InsufficientPermissions,
    #[error("Non UTF-8 sequence: {0}")]
    NonUtf8Sequence(FromUtf8Error),
    #[error("{0}")]
    QueryError(String),
}

pub type Result<T, Err = Error> = std::result::Result<T, Err>;
