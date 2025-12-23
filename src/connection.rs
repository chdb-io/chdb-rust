use std::ffi::{c_char, CString};

use crate::bindings;
use crate::error::{Error, Result};
use crate::format::OutputFormat;
use crate::query_result::QueryResult;

/// A connection to chDB database.
pub struct Connection {
    // Pointer to chdb_connection (which is *mut chdb_connection_)
    inner: *mut bindings::chdb_connection,
}

// Safety: Connection is safe to send between threads
unsafe impl Send for Connection {}

impl Connection {
    /// Connect to chDB with the given arguments.
    /// Use `--path=<db_path>` to specify database location, default is `:memory:`.
    pub fn open(args: &[&str]) -> Result<Self> {
        let c_args: Vec<CString> = args
            .iter()
            .map(|s| CString::new(*s))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut argv: Vec<*mut c_char> = c_args.iter().map(|s| s.as_ptr() as *mut c_char).collect();

        let conn_ptr = unsafe { bindings::chdb_connect(argv.len() as i32, argv.as_mut_ptr()) };

        if conn_ptr.is_null() {
            return Err(Error::ConnectionFailed);
        }

        // Check if the connection itself is null
        let conn = unsafe { *conn_ptr };
        if conn.is_null() {
            return Err(Error::ConnectionFailed);
        }

        Ok(Self { inner: conn_ptr })
    }

    /// Connect to an in-memory database.
    pub fn open_in_memory() -> Result<Self> {
        Self::open(&["clickhouse"])
    }

    /// Connect to a database at the given path.
    pub fn open_with_path(path: &str) -> Result<Self> {
        let path_arg = format!("--path={}", path);
        Self::open(&["clickhouse", &path_arg])
    }

    /// Execute a query and return the result.
    pub fn query(&self, sql: &str, format: OutputFormat) -> Result<QueryResult> {
        let query_cstr = CString::new(sql)?;
        let format_cstr = CString::new(format.as_str())?;

        // chdb_query takes chdb_connection (which is *mut chdb_connection_)
        let conn = unsafe { *self.inner };
        let result_ptr = unsafe {
            bindings::chdb_query(conn, query_cstr.as_ptr(), format_cstr.as_ptr())
        };

        if result_ptr.is_null() {
            return Err(Error::NoResult);
        }

        let result = QueryResult::new(result_ptr);
        result.check_error()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe { bindings::chdb_close_conn(self.inner) };
        }
    }
}
