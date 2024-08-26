use std::ffi::NulError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("An unknown error has occurred")]
    Unknown,
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
    NonUtf8Sequence(String),
    #[error("{0}")]
    QueryError(String),
}
