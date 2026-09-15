//! Connection management for chDB.
//!
//! This module provides the [`Connection`] type for managing connections to chDB databases.

use std::ffi::{c_char, CString};

#[cfg(feature = "arrow")]
use crate::arrow_options::ArrowOptions;
#[cfg(all(feature = "arrow", direct_arrow_insert))]
use crate::arrow_options::InsertOptions;
#[cfg(feature = "arrow")]
use crate::arrow_query_stream::ArrowQueryStream;
#[cfg(feature = "arrow")]
use crate::arrow_stream::{ArrowArray, ArrowSchema, ArrowStream};
use crate::error::{Error, Result};
use crate::format::{InputFormat, OutputFormat};
use crate::query_param::{EncodedParams, QueryParams};
use crate::query_result::QueryResult;
use crate::query_stream::QueryStream;
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
        // A post-shutdown connect fails inside the engine as a null connection
        // and nothing else, so it is caught here where the reason is known.
        crate::runtime::ensure_running()?;

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

    /// Get the underlying chDB connection handle.
    ///
    /// Returns the `chdb_connection` value passed to chDB C API functions. This is
    /// used by crate-internal code such as [`QueryStream`](crate::query_stream::QueryStream).
    ///
    /// # Returns
    ///
    /// Returns the raw `chdb_connection` handle.
    pub(crate) fn handle(&self) -> bindings::chdb_connection {
        unsafe { *self.inner }
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
        let conn = unsafe { *self.inner };
        let format = format.as_str();

        // chdb_query_n takes pointer + length for both strings, so neither has
        // to be NUL-terminated and neither is copied. It returns an owned
        // chdb_result handle (or null on failure): QueryResult::new below takes
        // ownership of that pointer, and QueryResult's Drop impl frees it via
        // chdb_destroy_query_result.
        let result_ptr = unsafe {
            bindings::chdb_query_n(
                conn,
                sql.as_ptr() as *const c_char,
                sql.len(),
                format.as_ptr() as *const c_char,
                format.len(),
            )
        };

        if result_ptr.is_null() {
            return Err(Error::NoResult);
        }

        let result = QueryResult::new(result_ptr);
        result.check_error()
    }

    /// Execute a query and return a streaming result.
    ///
    /// Unlike [`query`](Self::query), this returns a [`QueryStream`] that yields
    /// result data in chunks. This is useful for large result sets that should not
    /// be fully materialized in memory.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query string to execute
    /// * `format` - The desired output format for each chunk
    ///
    /// # Returns
    ///
    /// Returns a [`QueryStream`] tied to this connection, or an [`Error`] if the query
    /// cannot be started.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    /// use chdb_rust::format::OutputFormat;
    ///
    /// let mut conn = Connection::open_in_memory()?;
    /// let mut stream = conn.query_stream(
    ///     "SELECT number FROM numbers(100_000)",
    ///     OutputFormat::JSONEachRow,
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
    /// Returns an error if:
    /// - The query syntax is invalid
    /// - The query references non-existent tables or columns
    /// - The query execution fails for any other reason
    pub fn query_stream<'a>(
        &'a mut self,
        sql: &str,
        format: OutputFormat,
    ) -> Result<QueryStream<'a>> {
        QueryStream::start_borrowed(self, sql, format)
    }

    /// Execute a query with ClickHouse `{name:Type}` parameter binding and stream text chunks.
    ///
    /// Like [`Self::query_stream`], but SQL placeholders are bound from `params`
    /// before streaming begins. The connection is exclusively borrowed for the
    /// stream's lifetime. Syntax errors fail when the stream is created. A
    /// missing placeholder binding is reported on the first
    /// [`QueryStream::next_chunk`], not at start.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    /// use chdb_rust::format::OutputFormat;
    ///
    /// let mut conn = Connection::open_in_memory()?;
    /// let mut stream = conn.query_stream_with_params(
    ///     "SELECT {x:UInt64} AS v",
    ///     OutputFormat::CSV,
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
    /// Returns an error if:
    /// - The query syntax is invalid
    /// - The query cannot be started
    pub fn query_stream_with_params<'a>(
        &'a mut self,
        sql: &str,
        format: OutputFormat,
        params: impl Into<QueryParams>,
    ) -> Result<QueryStream<'a>> {
        QueryStream::start_borrowed_with_params(self, sql, format, params)
    }

    /// Execute a query and stream the result as Arrow record batches.
    ///
    /// Each call to [`Iterator::next`] or [`ArrowQueryStream::next_batch`] on the
    /// returned stream pulls a single [`arrow::record_batch::RecordBatch`] via the
    /// Arrow C Data Interface.
    ///
    /// Available when the crate is built with the `arrow` feature.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    ///
    /// let mut conn = Connection::open_in_memory()?;
    /// let mut stream = conn.query_stream_arrow("SELECT number FROM numbers(100_000)")?;
    /// while let Some(batch) = stream.next_batch()? {
    ///     println!("rows: {}", batch.num_rows());
    /// }
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    #[cfg(feature = "arrow")]
    pub fn query_stream_arrow<'a>(
        &'a mut self,
        sql: &str,
    ) -> Result<crate::arrow_query_stream::ArrowQueryStream<'a>> {
        crate::arrow_query_stream::ArrowQueryStream::start_borrowed(self, sql, None)
    }

    /// Execute a query and take the whole result as one Arrow stream.
    ///
    /// Zero-copy where the engine can manage it: no IPC serialization and no
    /// compression round-trip. Prefer
    /// [`query_stream_arrow`](Self::query_stream_arrow) when the result is too
    /// large to hold at once.
    ///
    /// The returned reader owns the Arrow stream. The C ABI transfers
    /// `out_stream->release` to the caller, so the reader can be drained after
    /// this connection is dropped.
    ///
    /// Available when the crate is built with the `arrow` feature.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// let reader = conn.query_arrow("SELECT number FROM numbers(1000)")?;
    /// for batch in reader {
    ///     println!("rows: {}", batch?.num_rows());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[cfg(feature = "arrow")]
    pub fn query_arrow(&self, sql: &str) -> Result<crate::arrow_query_stream::ArrowReader> {
        self.query_arrow_inner(sql, None)
    }

    /// [`query_arrow`](Self::query_arrow) with explicit type-mapping options.
    #[cfg(feature = "arrow")]
    pub fn query_arrow_with_opts(
        &self,
        sql: &str,
        opts: &ArrowOptions,
    ) -> Result<crate::arrow_query_stream::ArrowReader> {
        self.query_arrow_inner(sql, Some(opts))
    }

    #[cfg(feature = "arrow")]
    fn query_arrow_inner(
        &self,
        sql: &str,
        opts: Option<&ArrowOptions>,
    ) -> Result<crate::arrow_query_stream::ArrowReader> {
        use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

        let conn = unsafe { *self.inner };
        let c_opts = opts.map(|o| o.to_c());
        let opts_ptr = c_opts.as_ref().map_or(std::ptr::null(), |o| {
            o as *const bindings::chdb_arrow_options
        });

        let mut ffi_stream = FFI_ArrowArrayStream::empty();

        // Wraps chdb_query_arrow_n. The engine fills `ffi_stream` and transfers
        // ownership of its release callback; the returned chdb_result carries
        // only metrics and an error slot, and is destroyed here.
        let result_ptr = unsafe {
            bindings::chdb_query_arrow_n(
                conn,
                sql.as_ptr() as *const c_char,
                sql.len(),
                (&mut ffi_stream as *mut FFI_ArrowArrayStream).cast(),
                opts_ptr,
            )
        };

        if result_ptr.is_null() {
            return Err(Error::NoResult);
        }
        // Freed when it drops; the data lives in `ffi_stream`, not in it.
        QueryResult::new(result_ptr).check_error()?;

        let reader = ArrowArrayStreamReader::try_new(ffi_stream)
            .map_err(|e| Error::InvalidData(e.to_string()))?;
        Ok(crate::arrow_query_stream::ArrowReader::new(reader))
    }

    /// Stream a query's result as Arrow record batches with explicit
    /// type-mapping options.
    #[cfg(feature = "arrow")]
    pub fn query_stream_arrow_with_opts<'a>(
        &'a mut self,
        sql: &str,
        opts: &ArrowOptions,
    ) -> Result<crate::arrow_query_stream::ArrowQueryStream<'a>> {
        crate::arrow_query_stream::ArrowQueryStream::start_borrowed(self, sql, Some(opts))
    }

    /// Open a streaming INSERT.
    ///
    /// The write-side counterpart of [`query_stream`](Self::query_stream): send
    /// the INSERT statement here, then push the rows in chunks. The statement
    /// must carry no `FORMAT` clause and no inline data — the format is the
    /// `format` argument, and the data goes through
    /// [`InsertStream::append`](crate::insert_stream::InsertStream::append).
    ///
    /// The connection is exclusively borrowed until the stream is finished,
    /// cancelled or dropped, because it accepts no other statement meanwhile.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    /// use chdb_rust::format::InputFormat;
    ///
    /// let mut conn = Connection::open_in_memory()?;
    /// let mut ins = conn.insert_stream("INSERT INTO t (a, b)", InputFormat::CSV)?;
    /// ins.append(b"1,\"one\"\n")?;
    /// let stats = ins.finish()?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::QueryError`] if the statement is invalid — a missing
    /// table, say — which is reported when the stream is opened.
    pub fn insert_stream<'a>(
        &'a mut self,
        sql: &str,
        format: InputFormat,
    ) -> Result<crate::insert_stream::InsertStream<'a>> {
        crate::insert_stream::InsertStream::start(self, sql, format)
    }

    /// Open a streaming INSERT whose statement carries `{name:Type}`
    /// placeholders.
    ///
    /// The motivating case is `INSERT INTO FUNCTION file({path:String}, ...)`,
    /// where the destination itself is a bound value. See
    /// [`query_with_params`](Self::query_with_params) for binding rules.
    pub fn insert_stream_with_params<'a>(
        &'a mut self,
        sql: &str,
        format: InputFormat,
        params: impl Into<QueryParams>,
    ) -> Result<crate::insert_stream::InsertStream<'a>> {
        crate::insert_stream::InsertStream::start_with_params(self, sql, format, params)
    }

    /// Execute a query with ClickHouse `{name:Type}` parameter binding.
    ///
    /// Parameter values are encoded here and bound in the chDB library; they
    /// are never interpolated into the SQL text. Names in `params` need not match placeholder
    /// order in the query. An empty `params` list is a plain query.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    /// use chdb_rust::format::OutputFormat;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// let result = conn.query_with_params(
    ///     "SELECT {x:UInt64} + {y:UInt64} AS total",
    ///     OutputFormat::CSV,
    ///     [("y", 5_u64), ("x", 7_u64)],
    /// )?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The query syntax is invalid
    /// - A `{name:Type}` placeholder has no matching param
    /// - A value cannot be parsed as the type declared in the placeholder
    /// - The query execution fails for any other reason
    pub fn query_with_params(
        &self,
        sql: &str,
        format: OutputFormat,
        params: impl Into<QueryParams>,
    ) -> Result<QueryResult> {
        let format = format.as_str();
        let encoded = EncodedParams::encode(params)?;

        // chdb_query_with_params_n takes pointer + length for query, format, and
        // each bound value, so none of them need to be NUL-terminated. It returns
        // an owned chdb_result handle (or null on failure): QueryResult::new below
        // takes ownership, and QueryResult's Drop impl frees it via
        // chdb_destroy_query_result.
        let conn = unsafe { *self.inner };
        let result_ptr = unsafe {
            bindings::chdb_query_with_params_n(
                conn,
                sql.as_ptr() as *const c_char,
                sql.len(),
                format.as_ptr() as *const c_char,
                format.len(),
                encoded.names_ptr(),
                encoded.name_lens_ptr(),
                encoded.values_ptr(),
                encoded.value_lens_ptr(),
                encoded.len(),
            )
        };

        if result_ptr.is_null() {
            return Err(Error::NoResult);
        }

        let result = QueryResult::new(result_ptr);
        result.check_error()
    }

    #[cfg(feature = "arrow")]
    /// Execute a query with ClickHouse `{name:Type}` parameter binding and stream Arrow batches.
    ///
    /// Like [`Self::query_stream_arrow`], but SQL placeholders are bound from `params`
    /// in the chDB library before streaming begins. The connection is exclusively borrowed
    /// for the stream's lifetime. Syntax errors fail when the stream is created.
    /// A missing placeholder binding is reported on the first
    /// [`ArrowQueryStream::next_batch`], not at start.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    ///
    /// let mut conn = Connection::open_in_memory()?;
    /// let mut stream = conn.query_stream_arrow_with_params(
    ///     "SELECT {x:UInt64} AS v",
    ///     [("x", 11_u64)],
    ///     None,
    /// )?;
    /// while let Some(batch) = stream.next_batch()? {
    ///     println!("rows: {}", batch.num_rows());
    /// }
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The query syntax is invalid
    /// - The query cannot be started
    ///
    /// Pass `opts` to control Arrow type mapping alongside the bindings; `None`
    /// selects the engine's default contract.
    pub fn query_stream_arrow_with_params<'a>(
        &'a mut self,
        sql: &str,
        params: impl Into<QueryParams>,
        opts: Option<&ArrowOptions>,
    ) -> Result<ArrowQueryStream<'a>> {
        ArrowQueryStream::start_borrowed_with_params(self, sql, params, opts)
    }

    #[cfg(feature = "arrow")]
    /// Register an Arrow C Data Interface stream for use with `ArrowStream('name')`.
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

    #[cfg(feature = "arrow")]
    /// Register Arrow C Data Interface schema + array for use with `ArrowStream('name')`.
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

    #[cfg(feature = "arrow")]
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
    /// The engine handle behind this connection, for the FFI calls in sibling
    /// modules. `chdb_connect` hands back a pointer to the handle, and every
    /// entry point takes the handle itself.
    pub(crate) fn raw(&self) -> bindings::chdb_connection {
        // SAFETY: `inner` is non-null for the lifetime of a Connection —
        // `open` returns an error rather than a handle when either the outer
        // or the inner pointer is null.
        unsafe { *self.inner }
    }

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
