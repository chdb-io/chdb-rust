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
//! - **Parameterized queries**: Bind ClickHouse `{name:Type}` placeholders with [`execute_with_params`] and [`QueryParam`], across buffered, streaming, Arrow-streaming and insert statements. Values are bound in the chDB library and never spliced into the SQL text
//! - **Multiple output formats**: JSON, CSV, TabSeparated, and more
//! - **Query result streaming**: Read large result sets in chunks with constant memory
//! - **Streaming INSERT** ([`insert_stream`]): push rows in chunks in any input format, with engine backpressure; the stream also implements [`std::io::Write`]
//! - **Arrow bulk insert** (feature `arrow`, on by default): [`insert_record_batch`](arrow_insert::insert_record_batch) via `ArrowStream('name')`. Use [`chdb_rust::arrow`](arrow) types so your Arrow version matches the crate.
//! - **Arrow batch streaming** (with the `arrow` feature): Stream query results as `RecordBatch` values via the Arrow C Data Interface
//! - **One-shot Arrow export** (with the `arrow` feature): take a whole result as one Arrow stream via [`connection::Connection::query_arrow`], with [`arrow_options::ArrowOptions`] controlling the type mapping on both the one-shot and streaming paths
//! - **Thread-safe**: Connections and results can be safely sent between threads
//! - **Version accessors** ([`version`]): which chdb-core release is linked, where it came from, and which ClickHouse it carries
//! - **Backup, restore and statement analysis** ([`admin`]): the chdb-core management ABI, on any engine that exports it
//! - **Durable objects** (feature `durable`, [`durable`]): a database whose authoritative state is a checkpoint plus a statement WAL in storage you own
//! - **Runtime control** ([`runtime`]): decline chDB's process-wide signal handlers, and shut the engine down cleanly so no engine thread outlives your teardown
//!
//! ## Examples
//!
//! See the [`examples`](https://github.com/chdb-io/chdb-rust/tree/main/examples) directory for more detailed examples.
//!
//! ## Safety
//!
//! This crate uses `unsafe` code to interface with the C library, but provides a safe Rust API.
//! All public functions are safe to call, and the crate ensures proper resource cleanup.

#[cfg(has_durable_abi)]
pub mod admin;
pub mod arg;
#[cfg(feature = "arrow")]
pub mod arrow_insert;
#[cfg(feature = "arrow")]
pub mod arrow_options;
#[cfg(feature = "arrow")]
pub mod arrow_query_stream;
#[cfg(feature = "arrow")]
pub mod arrow_stream;
#[cfg(feature = "arrow")]
pub use arrow;
#[cfg(feature = "arrow")]
pub use arrow_insert::{
    insert_record_batch, insert_record_batch_direct, insert_record_batch_reader,
    insert_record_batches,
};
#[cfg(feature = "arrow")]
pub use arrow_options::{ArrowOptions, InsertOptions};
#[cfg(feature = "arrow")]
pub use arrow_query_stream::{ArrowQueryStream, ArrowReader};
#[cfg(feature = "arrow")]
pub use arrow_stream::arrow_stream_table_sql;
#[allow(
    dead_code,
    unused,
    non_snake_case,
    non_camel_case_types,
    non_upper_case_globals
)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
pub mod connection;
// The `durable` feature needs the chdb-core management ABI, and a library that
// does not export it cannot serve a durable object at all. Saying so at compile
// time beats a link error naming three symbols.
#[cfg(all(feature = "durable", not(has_durable_abi)))]
compile_error!(
    "the `durable` feature needs a libchdb with the Durable V1 ABI (chdb_backup_database_n, \
     chdb_restore_database_n, chdb_classify_query_n), which is chdb-core v26.7.2-rc.2 or newer. \
     Update the engine with ./update_libchdb.sh, or point CHDB_LIB_DIR at a newer one."
);
#[cfg(all(feature = "durable", has_durable_abi))]
pub mod durable;
pub mod error;
pub mod format;
pub mod insert_stream;
pub use insert_stream::{InsertStream, WriteStats};
pub mod log_level;
pub mod query_param;
pub mod query_result;
pub mod query_stream;
pub(crate) mod registry;
pub mod runtime;
pub mod session;
pub mod version;

pub use query_param::{QueryParam, QueryParams};
pub use query_result::QueryResult;

#[cfg(test)]
mod test_utils;

use crate::arg::{extract_output_format, Arg};
use crate::connection::Connection;
use crate::error::Result;
use crate::format::OutputFormat;
use crate::query_stream::QueryStream;

pub(crate) const CHDB_PROGRAM_NAME: &str = "clickhouse";

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
/// - A connection is already open on a data path, since the in-memory
///   connection this opens would be a second one. See
///   [`Error::PathConflict`](error::Error::PathConflict).
pub fn execute(query: &str, query_args: Option<&[Arg]>) -> Result<QueryResult> {
    let conn = Connection::open_in_memory()?;
    let fmt = extract_output_format(query_args, OutputFormat::TabSeparated);
    conn.query(query, fmt)
}

/// The data path the embedded engine is bound to, or `None` when no connection
/// is open. `:memory:` for an in-memory engine, a directory otherwise.
pub fn active_engine_path() -> Option<String> {
    registry::active_path()
}

/// How many open connections are holding the embedded engine. Zero means the
/// next connection may bind any path.
pub fn active_engine_refs() -> usize {
    registry::refs()
}

/// Execute a one-off query with ClickHouse `{name:Type}` parameter binding.
///
/// Counterpart to [`execute`]. Parameter values are bound in the chDB library
/// and never interpolated into the SQL text. Names in `params` need not match placeholder
/// order. An empty `params` list is a plain query: placeholders then fail with a
/// substitution error.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::arg::Arg;
/// use chdb_rust::execute_with_params;
/// use chdb_rust::format::OutputFormat;
///
/// let result = execute_with_params(
///     "SELECT {x:UInt64} AS v",
///     Some(&[Arg::OutputFormat(OutputFormat::CSV)]),
///     [("x", 7_u64)],
/// )?;
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
///
/// # Errors
///
/// This function will return an error if:
/// - The query syntax is invalid
/// - A `{name:Type}` placeholder has no matching param
/// - A value cannot be parsed as the type declared in the placeholder
/// - The connection cannot be established
/// - The query execution fails
/// - A connection is already open on a data path, since the in-memory
///   connection this opens would be a second one. See
///   [`Error::PathConflict`](error::Error::PathConflict).
pub fn execute_with_params(
    query: &str,
    query_args: Option<&[Arg]>,
    params: impl Into<QueryParams>,
) -> Result<QueryResult> {
    let conn = Connection::open_in_memory()?;
    let fmt = extract_output_format(query_args, OutputFormat::TabSeparated);
    conn.query_with_params(query, fmt, params)
}

/// Execute a one-off streaming query using an in-memory connection.
///
/// This function creates a temporary in-memory database connection, starts a
/// streaming query, and returns a [`QueryStream`] that owns the connection.
/// The connection is kept alive until the stream is dropped.
///
/// Primarily useful for reading external data: CSV files, S3, Parquet, etc.
///
/// # Arguments
///
/// * `query` - The SQL query string to execute
/// * `query_args` - Optional array of query arguments (e.g., output format)
///
/// # Returns
///
/// Returns a [`QueryStream`] that owns the temporary connection, or an
/// [`Error`](error::Error) if the query cannot be started.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::execute_stream;
/// use chdb_rust::arg::Arg;
/// use chdb_rust::format::OutputFormat;
///
/// let mut stream = execute_stream(
///     "SELECT number FROM numbers(100_000)",
///     Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
/// )?;
///
/// while let Some(chunk) = stream.next_chunk()? {
///     print!("{}", chunk.data_utf8_lossy());
/// }
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
///
/// # Errors
///
/// This function will return an error if:
/// - The query syntax is invalid
/// - The connection cannot be established
/// - The query execution fails
pub fn execute_stream(query: &str, query_args: Option<&[Arg]>) -> Result<QueryStream<'static>> {
    let conn = Connection::open_in_memory()?;
    let fmt = extract_output_format(query_args, OutputFormat::TabSeparated);
    QueryStream::start_owned(conn, query, fmt)
}

/// Execute a one-off query with ClickHouse `{name:Type}` parameter binding and stream text chunks.
///
/// Counterpart to [`execute_stream`]. Syntax errors fail when the stream is
/// created. A missing placeholder binding is reported on the first
/// [`QueryStream::next_chunk`], not at start.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::arg::Arg;
/// use chdb_rust::execute_stream_with_params;
/// use chdb_rust::format::OutputFormat;
///
/// let mut stream = execute_stream_with_params(
///     "SELECT {x:UInt64} AS v",
///     Some(&[Arg::OutputFormat(OutputFormat::CSV)]),
///     [("x", 11_u64)],
/// )?;
/// while let Some(chunk) = stream.next_chunk()? {
///     print!("{}", chunk.data_utf8_lossy());
/// }
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
///
/// # Errors
///
/// This function will return an error if:
/// - The query syntax is invalid
/// - The connection cannot be established
/// - The query cannot be started
/// - A connection is already open on a data path. See
///   [`Error::PathConflict`](error::Error::PathConflict).
pub fn execute_stream_with_params(
    query: &str,
    query_args: Option<&[Arg]>,
    params: impl Into<QueryParams>,
) -> Result<QueryStream<'static>> {
    let conn = Connection::open_in_memory()?;
    let fmt = extract_output_format(query_args, OutputFormat::TabSeparated);
    QueryStream::start_owned_with_params(conn, query, fmt, params)
}

/// Execute a one-off Arrow streaming query using an in-memory connection.
///
/// Returns an [`ArrowQueryStream`] that owns the temporary connection and yields
/// [`arrow::record_batch::RecordBatch`] values via the Arrow C Data Interface.
///
/// Available when the crate is built with the `arrow` feature.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::execute_stream_arrow;
///
/// let mut stream = execute_stream_arrow("SELECT number FROM numbers(100_000)")?;
/// while let Some(batch) = stream.next_batch()? {
///     println!("rows: {}", batch.num_rows());
/// }
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
#[cfg(feature = "arrow")]
pub fn execute_stream_arrow(query: &str) -> Result<ArrowQueryStream<'static>> {
    let conn = Connection::open_in_memory()?;
    ArrowQueryStream::start_owned(conn, query, None)
}

/// Execute a one-off query with ClickHouse `{name:Type}` parameter binding and stream Arrow batches.
///
/// Counterpart to [`execute_stream_arrow`]. Syntax errors fail when the stream is
/// created. A missing placeholder binding is reported on the first
/// [`ArrowQueryStream::next_batch`], not at start.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::execute_stream_arrow_with_params;
///
/// let mut stream =
///     execute_stream_arrow_with_params("SELECT {x:UInt64} AS v", [("x", 11_u64)])?;
/// while let Some(batch) = stream.next_batch()? {
///     println!("rows: {}", batch.num_rows());
/// }
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
///
/// # Errors
///
/// This function will return an error if:
/// - The query syntax is invalid
/// - The connection cannot be established
/// - The query cannot be started
/// - A connection is already open on a data path. See
///   [`Error::PathConflict`](error::Error::PathConflict).
#[cfg(feature = "arrow")]
pub fn execute_stream_arrow_with_params(
    query: &str,
    params: impl Into<QueryParams>,
) -> Result<ArrowQueryStream<'static>> {
    let conn = Connection::open_in_memory()?;
    ArrowQueryStream::start_owned_with_params(conn, query, params, None)
}
