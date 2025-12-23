pub mod arg;
#[allow(
    dead_code,
    unused,
    non_snake_case,
    non_camel_case_types,
    non_upper_case_globals
)]
mod bindings;
mod connection;
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
pub fn execute(query: &str, query_args: Option<&[Arg]>) -> Result<QueryResult> {
    let conn = Connection::open_in_memory()?;
    let fmt = extract_output_format(query_args);
    conn.query(query, fmt)
}
