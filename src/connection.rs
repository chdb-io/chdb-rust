//! Connection management for chDB.
//!
//! This module provides the [`Connection`] type for managing connections to chDB databases.

use std::ffi::{c_char, CString};

use crate::arrow_stream::{ArrowArray, ArrowSchema, ArrowStream};
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

    /// Register an Arrow stream as a table function with the given name.
    ///
    /// This function registers an Arrow stream as a virtual table that can be queried
    /// using SQL. The table will be available for queries until it is unregistered.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name to register for the Arrow stream table function
    /// * `arrow_stream` - The Arrow stream handle to register
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an [`Error`] if registration fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    /// use chdb_rust::arrow_stream::ArrowStream;
    ///
    /// let conn = Connection::open_in_memory()?;
    ///
    /// // Assuming you have an Arrow stream handle
    /// // let arrow_stream = ArrowStream::from_raw(stream_ptr);
    /// // conn.register_arrow_stream("my_data", &arrow_stream)?;
    ///
    /// // Now you can query it
    /// // let result = conn.query("SELECT * FROM my_data", OutputFormat::JSONEachRow)?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table name contains invalid characters
    /// - The Arrow stream handle is invalid
    /// - Registration fails for any other reason
    pub fn register_arrow_stream(
        &self,
        table_name: &str,
        arrow_stream: &ArrowStream,
    ) -> Result<()> {
        let table_name_cstr = CString::new(table_name)?;
        let conn = unsafe { *self.inner };

        let state = unsafe {
            bindings::chdb_arrow_scan(conn, table_name_cstr.as_ptr(), arrow_stream.as_raw())
        };

        if state == bindings::chdb_state_CHDBSuccess {
            Ok(())
        } else {
            Err(Error::QueryError(format!(
                "Failed to register Arrow stream as table '{}'",
                table_name
            )))
        }
    }

    /// Register an Arrow array as a table function with the given name.
    ///
    /// This function registers an Arrow array (with its schema) as a virtual table
    /// that can be queried using SQL. The table will be available for queries until
    /// it is unregistered.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name to register for the Arrow array table function
    /// * `arrow_schema` - The Arrow schema handle describing the array structure
    /// * `arrow_array` - The Arrow array handle containing the data
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an [`Error`] if registration fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    /// use chdb_rust::arrow_stream::{ArrowSchema, ArrowArray};
    ///
    /// let conn = Connection::open_in_memory()?;
    ///
    /// // Assuming you have Arrow schema and array handles
    /// // let arrow_schema = ArrowSchema::from_raw(schema_ptr);
    /// // let arrow_array = ArrowArray::from_raw(array_ptr);
    /// // conn.register_arrow_array("my_data", &arrow_schema, &arrow_array)?;
    ///
    /// // Now you can query it
    /// // let result = conn.query("SELECT * FROM my_data", OutputFormat::JSONEachRow)?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table name contains invalid characters
    /// - The Arrow schema or array handles are invalid
    /// - Registration fails for any other reason
    pub fn register_arrow_array(
        &self,
        table_name: &str,
        arrow_schema: &ArrowSchema,
        arrow_array: &ArrowArray,
    ) -> Result<()> {
        let table_name_cstr = CString::new(table_name)?;
        let conn = unsafe { *self.inner };

        let state = unsafe {
            bindings::chdb_arrow_array_scan(
                conn,
                table_name_cstr.as_ptr(),
                arrow_schema.as_raw(),
                arrow_array.as_raw(),
            )
        };

        if state == bindings::chdb_state_CHDBSuccess {
            Ok(())
        } else {
            Err(Error::QueryError(format!(
                "Failed to register Arrow array as table '{}'",
                table_name
            )))
        }
    }

    /// Unregister an Arrow stream table function that was previously registered.
    ///
    /// This function removes a previously registered Arrow stream table function,
    /// making it no longer available for queries.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the Arrow stream table function to unregister
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an [`Error`] if unregistration fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    /// use chdb_rust::arrow_stream::ArrowStream;
    ///
    /// let conn = Connection::open_in_memory()?;
    ///
    /// // Register a table
    /// // let arrow_stream = ArrowStream::from_raw(stream_ptr);
    /// // conn.register_arrow_stream("my_data", &arrow_stream)?;
    ///
    /// // Use it...
    ///
    /// // Unregister when done
    /// // conn.unregister_arrow_table("my_data")?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table name contains invalid characters
    /// - The table was not previously registered
    /// - Unregistration fails for any other reason
    pub fn unregister_arrow_table(&self, table_name: &str) -> Result<()> {
        let table_name_cstr = CString::new(table_name)?;
        let conn = unsafe { *self.inner };

        let state =
            unsafe { bindings::chdb_arrow_unregister_table(conn, table_name_cstr.as_ptr()) };

        if state == bindings::chdb_state_CHDBSuccess {
            Ok(())
        } else {
            Err(Error::QueryError(format!(
                "Failed to unregister Arrow table '{}'",
                table_name
            )))
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe { bindings::chdb_close_conn(self.inner) };
        }
    }
}
