//! Connection management for chDB.
//!
//! This module provides the [`Connection`] type for managing connections to chDB databases.

use std::ffi::{c_char, CString};

#[cfg(all(feature = "arrow", direct_arrow_insert))]
use crate::arrow_options::InsertOptions;
#[cfg(feature = "arrow")]
use crate::arrow_stream::{ArrowArray, ArrowSchema, ArrowStream};
use crate::error::{Error, Result};
use crate::format::OutputFormat;
use crate::query_result::QueryResult;
use crate::{bindings, registry, CHDB_PROGRAM_NAME};

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
    /// Holds this connection's claim on the process-wide engine. Dropping it
    /// lets a later connection bind a different data path.
    slot: registry::Slot,
}

// Safety: Connection is safe to send between threads
// The underlying chDB library is thread-safe for query execution
unsafe impl Send for Connection {}

impl Connection {
    /// Connect to chDB with the given command-line arguments.
    ///
    /// Use [crate::session::SessionBuilder] for a higher-level API that supports
    /// sessions and persistent storage.
    ///
    /// # Arguments
    ///
    /// * `args` - Array of command-line arguments (e.g., `["--path=/tmp/db"]`)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    ///
    /// // Connect with custom arguments
    /// let conn = Connection::open(&["--path=/tmp/mydb"])?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConnectionFailed`] if the
    /// connection cannot be established.
    pub fn open(args: &[&str]) -> Result<Self> {
        let c_args: Vec<CString> = std::iter::once(CHDB_PROGRAM_NAME)
            .chain(args.iter().copied())
            .map(CString::new)
            .collect::<std::result::Result<_, _>>()?;

        // Claimed before connecting, so that a second data path is refused with
        // the reason rather than by the engine, which reports a refusal as a
        // null connection and nothing else.
        let slot = registry::acquire(registry::key_from_args(args))?;

        let argv: Vec<*const c_char> = c_args.iter().map(|s| s.as_ptr()).collect();
        let conn_ptr =
            unsafe { bindings::chdb_connect(argv.len() as i32, argv.as_ptr() as *mut *mut c_char) };

        if conn_ptr.is_null() {
            return Err(Error::ConnectionFailed);
        }

        // Check if the connection itself is null
        let conn = unsafe { *conn_ptr };
        if conn.is_null() {
            return Err(Error::ConnectionFailed);
        }

        Ok(Self {
            inner: conn_ptr,
            slot,
        })
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
        Self::open(&[])
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
    #[deprecated(note = "Use `SessionBuilder` instead")]
    pub fn open_with_path(path: &str) -> Result<Self> {
        let path_arg = format!("--path={path}");
        Self::open(&[&path_arg])
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

    /// Register an Arrow C Data Interface stream for use with `ArrowStream('name')`.
    #[cfg(feature = "arrow")]
    ///
    /// Pass a raw `ArrowArrayStream*` (see [`ArrowStream`](crate::arrow_stream::ArrowStream)).
    /// Registered names are **not** ordinary tables; query them with the
    /// [`arrow_stream_table_sql`](crate::arrow_stream::arrow_stream_table_sql) helper, e.g.
    /// `SELECT * FROM ArrowStream('my_data')`.
    ///
    /// The stream pointer must stay valid until [`unregister_arrow_table`](Self::unregister_arrow_table)
    /// is called or the connection is dropped.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::arrow_stream::{arrow_stream_table_sql, ArrowStream};
    /// use chdb_rust::connection::Connection;
    /// use chdb_rust::format::OutputFormat;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// // let stream_ptr: *mut arrow::ffi::FFI_ArrowArrayStream = ...;
    /// // let arrow_stream = unsafe { ArrowStream::from_raw(stream_ptr) };
    /// // conn.register_arrow_stream("my_data", &arrow_stream)?;
    /// // let sql = format!("SELECT * FROM {}", arrow_stream_table_sql("my_data"));
    /// // let _ = conn.query(&sql, OutputFormat::JSONEachRow)?;
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

    /// Register Arrow C Data Interface schema + array for use with `ArrowStream('name')`.
    #[cfg(feature = "arrow")]
    ///
    /// libchdb wraps the pair in a one-shot stream. Query via
    /// [`arrow_stream_table_sql`](crate::arrow_stream::arrow_stream_table_sql).
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name to register for the Arrow stream table function
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
    /// use chdb_rust::arrow_stream::{arrow_stream_table_sql, ArrowArray, ArrowSchema};
    /// use chdb_rust::connection::Connection;
    /// use chdb_rust::format::OutputFormat;
    ///
    /// let conn = Connection::open_in_memory()?;
    ///
    /// // Assuming you have Arrow C Data Interface schema and array handles
    /// // let arrow_schema = unsafe { ArrowSchema::from_raw(schema_ptr) };
    /// // let arrow_array = unsafe { ArrowArray::from_raw(array_ptr) };
    /// // conn.register_arrow_array("my_data", &arrow_schema, &arrow_array)?;
    ///
    /// // Query via the ArrowStream table function
    /// // let sql = format!("SELECT * FROM {}", arrow_stream_table_sql("my_data"));
    /// // let result = conn.query(&sql, OutputFormat::JSONEachRow)?;
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
    #[cfg(feature = "arrow")]
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

    /// Insert rows from a registered Arrow schema+array directly into `dest_table`.
    ///
    /// Requires libchdb built with `chdb_insert_arrow_array` (see `direct_arrow_insert` cfg).
    #[cfg(all(feature = "arrow", direct_arrow_insert))]
    pub fn insert_arrow_array(
        &self,
        dest_table: &str,
        arrow_schema: &ArrowSchema,
        arrow_array: &ArrowArray,
        options: &InsertOptions,
    ) -> Result<()> {
        let dest_cstr = CString::new(dest_table)?;
        let (options_ptr, _settings) = build_insert_options_c(options)?;

        let conn = unsafe { *self.inner };
        let result_ptr = unsafe {
            bindings::chdb_insert_arrow_array(
                conn,
                dest_cstr.as_ptr(),
                arrow_schema.as_raw(),
                arrow_array.as_raw(),
                options_ptr,
            )
        };

        check_insert_result(result_ptr)
    }

    /// Insert rows from a registered Arrow stream directly into `dest_table`.
    ///
    /// Requires libchdb built with `chdb_insert_arrow_stream` (see `direct_arrow_insert` cfg).
    #[cfg(all(feature = "arrow", direct_arrow_insert))]
    pub fn insert_arrow_stream(
        &self,
        dest_table: &str,
        arrow_stream: &ArrowStream,
        options: &InsertOptions,
    ) -> Result<()> {
        let dest_cstr = CString::new(dest_table)?;
        let (options_ptr, _settings) = build_insert_options_c(options)?;

        let conn = unsafe { *self.inner };
        let result_ptr = unsafe {
            bindings::chdb_insert_arrow_stream(
                conn,
                dest_cstr.as_ptr(),
                arrow_stream.as_raw(),
                options_ptr,
            )
        };

        check_insert_result(result_ptr)
    }
}

#[cfg(all(feature = "arrow", direct_arrow_insert))]
fn build_insert_options_c(
    options: &InsertOptions,
) -> Result<(*const bindings::chdb_arrow_insert_options, Option<CString>)> {
    let settings_cstr = options.settings_clause().map(CString::new).transpose()?;
    let c_options = settings_cstr
        .as_ref()
        .map(|settings| bindings::chdb_arrow_insert_options {
            settings: settings.as_ptr(),
        });
    let options_ptr = c_options
        .as_ref()
        .map(|opts| opts as *const bindings::chdb_arrow_insert_options)
        .unwrap_or(std::ptr::null());
    Ok((options_ptr, settings_cstr))
}

#[cfg(all(feature = "arrow", direct_arrow_insert))]
fn check_insert_result(result_ptr: *mut bindings::chdb_result) -> Result<()> {
    if result_ptr.is_null() {
        return Err(Error::NoResult);
    }

    let result = QueryResult::new(result_ptr);
    result.check_error().map(|_| ())
}

impl Connection {
    /// Removes `dir` once this is the last connection on its data path.
    ///
    /// The removal happens while the engine record is locked, so a connection
    /// cannot attach to the path in between and lose its data.
    pub(crate) fn remove_dir_on_last(&mut self, dir: std::path::PathBuf) {
        self.slot.remove_on_last(dir);
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe { bindings::chdb_close_conn(self.inner) };
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{error::Result, test_utils::tempdir};

    #[test]
    fn test_connection_open_with_explicit_data_path() -> Result<()> {
        let tmp = tempdir();
        let path_arg = format!(
            "--path={}",
            tmp.path().to_str().expect("temp path is not valid UTF-8")
        );
        Connection::open(&[&path_arg])?;

        assert!(
            tmp.path().read_dir()?.next().is_some(),
            "expected chDB to create files in the data dir"
        );

        Ok(())
    }
}
