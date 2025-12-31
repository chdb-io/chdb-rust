//! # chdb-rust
//!
//! Rust FFI bindings for [chDB](https://github.com/chdb-io/chdb), an embedded ClickHouse database.
//!
//! ## Overview
//!
//! This crate provides a safe Rust interface to chDB, allowing you to execute ClickHouse SQL queries
//! either statelessly (in-memory) or with persistent storage using sessions.
//!
//! ## Quick Start
//!
//! ```no_run
//! use chdb_rust::execute;
//! use chdb_rust::arg::Arg;
//! use chdb_rust::format::OutputFormat;
//!
//! // Execute a simple query
//! let result = execute("SELECT 1 + 1 AS sum", None)?;
//! println!("Result: {}", result.data_utf8_lossy());
//! # Ok::<(), chdb_rust::error::Error>(())
//! ```
//!
//! ## Features
//!
//! - **Stateless queries**: Execute one-off queries without persistent storage
//! - **Stateful sessions**: Create databases and tables with persistent storage
//! - **Multiple output formats**: JSON, CSV, TabSeparated, and more
//! - **Thread-safe**: Connections and results can be safely sent between threads
//!
//! ## Examples
//!
//! See the [`examples`](https://github.com/chdb-io/chdb-rust/tree/main/examples) directory for more detailed examples.
//!
//! ## Safety
//!
//! This crate uses `unsafe` code to interface with the C library, but provides a safe Rust API.
//! All public functions are safe to call, and the crate ensures proper resource cleanup.

pub mod arg;
#[allow(
    dead_code,
    unused,
    non_snake_case,
    non_camel_case_types,
    non_upper_case_globals
)]
mod bindings;
pub mod connection;
pub mod error;
pub mod format;
pub mod log_level;
pub mod query_result;
pub mod session;

use crate::arg::{extract_output_format, Arg};
use crate::connection::Connection;
use crate::error::Result;
use crate::query_result::QueryResult;

/// Execute a one-off query using an in-memory connection.
///
/// This function creates a temporary in-memory database connection, executes the query,
/// and returns the result. It's suitable for queries that don't require persistent storage.
///
/// # Arguments
///
/// * `query` - The SQL query string to execute
/// * `query_args` - Optional array of query arguments (e.g., output format)
///
/// # Returns
///
/// Returns a [`QueryResult`] containing the query output, or an [`Error`](error::Error) if
/// the query fails.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::execute;
/// use chdb_rust::arg::Arg;
/// use chdb_rust::format::OutputFormat;
///
/// // Simple query with default format
/// let result = execute("SELECT 1 + 1 AS sum", None)?;
/// println!("{}", result.data_utf8_lossy());
///
/// // Query with JSON output format
/// let result = execute(
///     "SELECT 'Hello' AS greeting, 42 AS answer",
///     Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
/// )?;
/// println!("{}", result.data_utf8_lossy());
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
///
/// # Errors
///
/// This function will return an error if:
/// - The query syntax is invalid
/// - The connection cannot be established
/// - The query execution fails
pub fn execute(query: &str, query_args: Option<&[Arg]>) -> Result<QueryResult> {
    let conn = Connection::open_in_memory()?;
    let fmt = extract_output_format(query_args);
    conn.query(query, fmt)
}
