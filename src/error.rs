//! Error types for chdb-rust.
//!
//! This module defines the error types used throughout the crate.

use std::ffi::NulError;
use std::string::FromUtf8Error;

/// Errors that can occur when using chdb-rust.
///
/// This enum represents all possible errors that can be returned by the library.
/// Most errors are self-explanatory, with `QueryError` containing the actual error
/// message from the underlying chDB library.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An unknown error has occurred.
    #[error("An unknown error has occurred")]
    Unknown,
    /// No result was returned from the query.
    #[error("No result")]
    NoResult,
    /// Failed to establish a connection to chDB.
    #[error("Connection failed")]
    ConnectionFailed,
    /// The engine is already bound to a different data path.
    ///
    /// One engine per process serves one storage path. An in-memory connection
    /// counts as a path of its own, since the engine binds `:memory:` when none
    /// is given.
    #[error(
        "the engine is already open on {active}; chDB serves one data path per \
         process, so {requested} cannot be opened until every connection to \
         {active} is closed"
    )]
    PathConflict {
        /// The path the engine is bound to.
        active: String,
        /// The path that was asked for.
        requested: String,
    },
    /// Invalid data was encountered.
    #[error("Invalid data: {0}")]
    InvalidData(String),
    /// Invalid path was provided.
    #[error("Invalid path")]
    PathError,
    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A null byte was found in a string where it's not allowed.
    #[error(transparent)]
    Nul(#[from] NulError),
    /// Insufficient permissions to access the directory.
    #[error("Insufficient dir permissions")]
    InsufficientPermissions,
    /// The data contains invalid UTF-8 sequences.
    #[error("Non UTF-8 sequence: {0}")]
    NonUtf8Sequence(FromUtf8Error),
    /// The linked library cannot report which chdb-core release it is.
    ///
    /// `chdb_version()` arrived in chdb-core v26.7.0; an older library does not
    /// export it, so there is nothing to call. Deliberately not answered with
    /// `SELECT version()`, which reports a ClickHouse version — a different
    /// scheme, and so not a substitute.
    #[error("the linked libchdb does not export chdb_version(); it predates chdb-core v26.7.0")]
    EngineVersionUnavailable,
    /// A query execution error occurred.
    ///
    /// This contains the error message from the underlying chDB library,
    /// which typically includes details about SQL syntax errors, missing tables, etc.
    #[error("{0}")]
    QueryError(String),
}

/// A type alias for `Result<T, Error>`.
///
/// This is the standard result type used throughout the crate.
pub type Result<T, Err = Error> = std::result::Result<T, Err>;
