//! Query result handling for chDB.
//!
//! This module provides the [`QueryResult`] type for accessing query execution results.

use core::slice;
use std::borrow::Cow;
use std::ffi::CStr;
use std::time::Duration;

use crate::bindings;
use crate::error::Error;
use crate::error::Result;

/// The result of a query execution.
///
/// `QueryResult` contains the output data from a query execution, along with
/// metadata such as execution time and number of rows read.
///
/// # Thread Safety
///
/// `QueryResult` implements `Send`, meaning it can be safely transferred between threads.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::execute;
/// use chdb_rust::format::OutputFormat;
/// use chdb_rust::arg::Arg;
///
/// let result = execute(
///     "SELECT number FROM numbers(10)",
///     Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
/// )?;
///
/// // Access the data as a string
/// println!("Data: {}", result.data_utf8_lossy());
///
/// // Access metadata
/// println!("Rows read: {}", result.rows_read());
/// println!("Bytes read: {}", result.bytes_read());
/// println!("Elapsed time: {:?}", result.elapsed());
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
#[derive(Debug)]
pub struct QueryResult {
    inner: *mut bindings::chdb_result,
}

// Safety: QueryResult is safe to send between threads
// The underlying chDB result structure is thread-safe for read access
unsafe impl Send for QueryResult {}

impl QueryResult {
    pub(crate) fn new(inner: *mut bindings::chdb_result) -> Self {
        Self { inner }
    }

    /// Get the result data as a UTF-8 string.
    ///
    /// This method validates that the data is valid UTF-8. If the data contains
    /// invalid UTF-8 sequences, it returns an error.
    ///
    /// # Returns
    ///
    /// Returns a `String` containing the query result, or an error if the data
    /// contains invalid UTF-8 sequences.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::execute;
    ///
    /// let result = execute("SELECT 'Hello, World!' AS greeting", None)?;
    /// let data = result.data_utf8()?;
    /// println!("{}", data);
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::NonUtf8Sequence`] if the
    /// result data contains invalid UTF-8 sequences. Use [`data_utf8_lossy`](Self::data_utf8_lossy)
    /// if you want to handle invalid UTF-8 gracefully.
    pub fn data_utf8(&self) -> Result<String> {
        let buf = self.data_ref();
        String::from_utf8(buf.to_vec()).map_err(Error::NonUtf8Sequence)
    }

    /// Get the result data as a UTF-8 string, replacing invalid sequences.
    ///
    /// This method converts the result data to a string, replacing any invalid UTF-8
    /// sequences with the Unicode replacement character (U+FFFD).
    ///
    /// # Returns
    ///
    /// Returns a `Cow<str>` containing the query result. Invalid UTF-8 sequences
    /// are replaced with the replacement character.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::execute;
    ///
    /// let result = execute("SELECT 'Hello, World!' AS greeting", None)?;
    /// let data = result.data_utf8_lossy();
    /// println!("{}", data);
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    pub fn data_utf8_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.data_ref())
    }

    /// Get the result data as a UTF-8 string without validation.
    ///
    /// # Safety
    ///
    /// This function is marked as safe, but it will produce invalid UTF-8 strings
    /// if the underlying data contains non-UTF-8 bytes. Only use this if you're
    /// certain the data is valid UTF-8, or if you're prepared to handle potentially
    /// invalid strings.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::execute;
    ///
    /// let result = execute("SELECT 'Hello' AS greeting", None)?;
    /// let data = result.data_utf8_unchecked();
    /// println!("{}", data);
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    pub fn data_utf8_unchecked(&self) -> String {
        unsafe { String::from_utf8_unchecked(self.data_ref().to_vec()) }
    }

    /// Get a reference to the raw result data as bytes.
    ///
    /// This method returns a byte slice containing the raw query result data.
    /// The data is in the format specified when executing the query (e.g., JSON, CSV, etc.).
    ///
    /// # Returns
    ///
    /// Returns a byte slice containing the query result data. Returns an empty slice
    /// if there's no data or if the buffer pointer is null.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::execute;
    ///
    /// let result = execute("SELECT 1 AS value", None)?;
    /// let bytes = result.data_ref();
    /// println!("Data length: {} bytes", bytes.len());
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    pub fn data_ref(&self) -> &[u8] {
        let buf = unsafe { bindings::chdb_result_buffer(self.inner) };
        let len = unsafe { bindings::chdb_result_length(self.inner) };
        if buf.is_null() || len == 0 {
            return &[];
        }
        unsafe { slice::from_raw_parts(buf as *const u8, len) }
    }

    /// Get the number of rows read by the query.
    ///
    /// This returns the total number of rows that were read from storage during
    /// query execution.
    ///
    /// # Returns
    ///
    /// Returns the number of rows read as a `u64`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::execute;
    ///
    /// let result = execute("SELECT number FROM numbers(100)", None)?;
    /// println!("Rows read: {}", result.rows_read());
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    pub fn rows_read(&self) -> u64 {
        unsafe { bindings::chdb_result_rows_read(self.inner) }
    }

    /// Get the number of bytes read by the query.
    ///
    /// This returns the total number of bytes that were read from storage during
    /// query execution.
    ///
    /// # Returns
    ///
    /// Returns the number of bytes read as a `u64`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::execute;
    ///
    /// let result = execute("SELECT number FROM numbers(100)", None)?;
    /// println!("Bytes read: {}", result.bytes_read());
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    pub fn bytes_read(&self) -> u64 {
        unsafe { bindings::chdb_result_bytes_read(self.inner) }
    }

    /// Get the elapsed time for query execution.
    ///
    /// This returns the time it took to execute the query, measured from when
    /// the query was submitted until the result was ready.
    ///
    /// # Returns
    ///
    /// Returns a [`Duration`] representing the elapsed time.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::execute;
    ///
    /// let result = execute("SELECT number FROM numbers(1000)", None)?;
    /// println!("Query took: {:?}", result.elapsed());
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    pub fn elapsed(&self) -> Duration {
        let elapsed = unsafe { bindings::chdb_result_elapsed(self.inner) };
        Duration::from_secs_f64(elapsed)
    }

    pub(crate) fn check_error(self) -> Result<Self> {
        self.check_error_ref()?;
        Ok(self)
    }

    pub(crate) fn check_error_ref(&self) -> Result<()> {
        let err_ptr = unsafe { bindings::chdb_result_error(self.inner) };

        if err_ptr.is_null() {
            return Ok(());
        }

        let err_msg = unsafe { CStr::from_ptr(err_ptr).to_string_lossy().to_string() };
        if err_msg.is_empty() {
            return Ok(());
        }

        Err(Error::QueryError(err_msg))
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        unsafe { bindings::chdb_destroy_query_result(self.inner) };
    }
}
