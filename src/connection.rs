//! Connection management for chDB.
//!
//! This module provides the [`Connection`] type for managing connections to chDB databases.

use std::ffi::{c_char, CString};

use crate::bindings;
use crate::error::{Error, Result};
use crate::format::OutputFormat;
use crate::query_result::QueryResult;

/// A connection to a chDB database.
///
/// A `Connection` represents an active connection to a chDB database instance.
/// Connections can be created for in-memory databases or persistent databases
/// stored on disk.
///
/// # Thread Safety
///
/// `Connection` implements `Send`, meaning it can be safely transferred between threads.
/// However, the underlying chDB library may have limitations on concurrent access.
/// It's recommended to use one connection per thread or implement proper synchronization.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::connection::Connection;
/// use chdb_rust::format::OutputFormat;
///
/// // Create an in-memory connection
/// let conn = Connection::open_in_memory()?;
///
/// // Execute a query
/// let result = conn.query("SELECT 1", OutputFormat::JSONEachRow)?;
/// println!("{}", result.data_utf8_lossy());
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
#[derive(Debug)]
pub struct Connection {
    // Pointer to chdb_connection (which is *mut chdb_connection_)
    inner: *mut bindings::chdb_connection,
}

// Safety: Connection is safe to send between threads
// The underlying chDB library is thread-safe for query execution
unsafe impl Send for Connection {}

impl Connection {
    /// Connect to chDB with the given command-line arguments.
    ///
    /// This is a low-level function that allows you to pass arbitrary arguments
    /// to the chDB connection. For most use cases, prefer [`open_in_memory`](Self::open_in_memory)
    /// or [`open_with_path`](Self::open_with_path).
    ///
    /// # Arguments
    ///
    /// * `args` - Array of command-line arguments (e.g., `["clickhouse", "--path=/tmp/db"]`)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    ///
    /// // Connect with custom arguments
    /// let conn = Connection::open(&["clickhouse", "--path=/tmp/mydb"])?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConnectionFailed`] if the
    /// connection cannot be established.
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
    ///
    /// Creates a connection to a temporary in-memory database. Data stored in this
    /// database will be lost when the connection is closed.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConnectionFailed`] if the
    /// connection cannot be established.
    pub fn open_in_memory() -> Result<Self> {
        Self::open(&["clickhouse"])
    }

    /// Connect to a database at the given path.
    ///
    /// Creates a connection to a persistent database stored at the specified path.
    /// The directory will be created if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `path` - The filesystem path where the database should be stored
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    ///
    /// let conn = Connection::open_with_path("/tmp/mydb")?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConnectionFailed`] if the
    /// connection cannot be established.
    pub fn open_with_path(path: &str) -> Result<Self> {
        let path_arg = format!("--path={path}");
        Self::open(&["clickhouse", &path_arg])
    }

    /// Execute a query and return the result.
    ///
    /// Executes a SQL query against the database and returns the result in the
    /// specified output format.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query string to execute
    /// * `format` - The desired output format for the result
    ///
    /// # Returns
    ///
    /// Returns a [`QueryResult`] containing the query output, or an [`Error`]
    /// if the query fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    /// use chdb_rust::format::OutputFormat;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// let result = conn.query("SELECT 1 + 1 AS sum", OutputFormat::JSONEachRow)?;
    /// println!("{}", result.data_utf8_lossy());
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The query syntax is invalid
    /// - The query references non-existent tables or columns
    /// - The query execution fails for any other reason
    pub fn query(&self, sql: &str, format: OutputFormat) -> Result<QueryResult> {
        let query_cstr = CString::new(sql)?;
        let format_cstr = CString::new(format.as_str())?;

        // chdb_query takes chdb_connection (which is *mut chdb_connection_)
        let conn = unsafe { *self.inner };
        let result_ptr =
            unsafe { bindings::chdb_query(conn, query_cstr.as_ptr(), format_cstr.as_ptr()) };

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
