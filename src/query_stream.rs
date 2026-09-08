//! Streaming query result handling for chDB.
//!
//! This module provides the [`QueryStream`] type for reading large query results
//! in chunks without materializing the entire output in memory.

use std::ffi::CString;
use std::mem::ManuallyDrop;

use crate::bindings;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::format::OutputFormat;
use crate::query_result::QueryResult;

enum QueryStreamConnection<'a> {
    Borrowed(&'a Connection),
    Owned(Connection),
}

/// A streaming query result that yields data in chunks.
///
/// `QueryStream` is returned by [`Connection::query_stream`](crate::connection::Connection::query_stream),
/// [`Session::execute_stream`](crate::session::Session::execute_stream), and
/// [`execute_stream`](crate::execute_stream). Each chunk is a [`QueryResult`] that the caller owns
/// and must drop before requesting the next chunk.
///
/// `QueryStream` also implements [`Iterator`], so you can collect chunks with adapter methods
/// such as [`Iterator::collect`].
///
/// # Thread Safety
///
/// Only owned streams (for example from [`execute_stream`](crate::execute_stream)) implement
/// [`Send`]. Streams tied to a borrowed [`Connection`](crate::connection::Connection) or
/// [`Session`](crate::session::Session) do not, because [`Connection`] is [`Send`] but not
/// [`Sync`]. Concurrent use from multiple threads is not recommended without external
/// synchronization.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::connection::Connection;
/// use chdb_rust::format::OutputFormat;
///
/// let conn = Connection::open_in_memory()?;
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
pub struct QueryStream<'a> {
    conn: QueryStreamConnection<'a>,
    stream: *mut bindings::chdb_result,
    finished: bool,
}

impl<'a> QueryStream<'a> {
    pub(crate) fn start_borrowed(
        conn: &'a Connection,
        sql: &str,
        format: OutputFormat,
    ) -> Result<Self> {
        let stream = Self::start_query(conn.handle(), sql, format)?;
        Ok(Self {
            conn: QueryStreamConnection::Borrowed(conn),
            stream,
            finished: false,
        })
    }

    pub(crate) fn start_owned(conn: Connection, sql: &str, format: OutputFormat) -> Result<Self> {
        let stream = Self::start_query(conn.handle(), sql, format)?;
        Ok(Self {
            conn: QueryStreamConnection::Owned(conn),
            stream,
            finished: false,
        })
    }

    fn start_query(
        conn: bindings::chdb_connection,
        sql: &str,
        format: OutputFormat,
    ) -> Result<*mut bindings::chdb_result> {
        let query_cstr = CString::new(sql)?;
        let format_cstr = CString::new(format.as_str())?;

        let stream_ptr =
            unsafe { bindings::chdb_stream_query(conn, query_cstr.as_ptr(), format_cstr.as_ptr()) };

        if stream_ptr.is_null() {
            return Err(Error::NoResult);
        }

        let probe = ManuallyDrop::new(QueryResult::new(stream_ptr));
        if let Err(e) = probe.check_error_ref() {
            drop(ManuallyDrop::into_inner(probe));
            return Err(e);
        }
        std::mem::forget(ManuallyDrop::into_inner(probe));

        Ok(stream_ptr)
    }

    fn conn_handle(&self) -> bindings::chdb_connection {
        match &self.conn {
            QueryStreamConnection::Borrowed(conn) => conn.handle(),
            QueryStreamConnection::Owned(conn) => conn.handle(),
        }
    }

    /// Fetch the next chunk of query results.
    ///
    /// Each call returns one chunk of output in the format specified when the stream was
    /// started. Drop the previous chunk before calling this method again.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(chunk))` while data remains, or `Ok(None)` once the stream is
    /// exhausted. Each returned chunk is an independent [`QueryResult`] that is freed
    /// when dropped.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::connection::Connection;
    /// use chdb_rust::format::OutputFormat;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// let mut stream = conn.query_stream(
    ///     "SELECT number FROM numbers(10)",
    ///     OutputFormat::JSONEachRow,
    /// )?;
    ///
    /// while let Some(chunk) = stream.next_chunk()? {
    ///     println!("rows in chunk: {}", chunk.rows_read());
    /// }
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The underlying fetch call fails
    /// - A chunk contains a query error
    pub fn next_chunk(&mut self) -> Result<Option<QueryResult>> {
        if self.finished || self.stream.is_null() {
            return Ok(None);
        }

        let chunk_ptr =
            unsafe { bindings::chdb_stream_fetch_result(self.conn_handle(), self.stream) };

        if chunk_ptr.is_null() {
            self.finished = true;
            return Err(Error::NoResult);
        }

        let chunk = QueryResult::new(chunk_ptr);

        if let Err(e) = chunk.check_error_ref() {
            self.finished = true;
            return Err(e);
        }

        if chunk.rows_read() == 0 {
            drop(chunk);
            self.finished = true;
            return Ok(None);
        }

        Ok(Some(chunk))
    }

    /// Cancel the streaming query and release resources.
    ///
    /// Stops the query on the server and frees the stream handle. After cancellation,
    /// further calls to [`next_chunk`](Self::next_chunk) return `Ok(None)`.
    ///
    /// This is called automatically when the stream is dropped. Call this explicitly to
    /// stop early without waiting for the stream to be dropped.
    pub fn cancel(&mut self) {
        if self.stream.is_null() {
            return;
        }

        let conn = self.conn_handle();
        unsafe {
            if !self.finished {
                bindings::chdb_stream_cancel_query(conn, self.stream);
            }
            bindings::chdb_destroy_query_result(self.stream);
        }
        self.stream = std::ptr::null_mut();
        self.finished = true;
    }
}

impl<'a> Iterator for QueryStream<'a> {
    type Item = Result<QueryResult>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_chunk() {
            Ok(Some(chunk)) => Some(Ok(chunk)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

impl Drop for QueryStream<'_> {
    fn drop(&mut self) {
        if !self.stream.is_null() {
            self.cancel();
        }
    }
}

// Safety: Only the owned variant (`QueryStream<'static>` from `execute_stream`) is Send.
// It owns the Connection outright. Borrowed streams hold `&Connection` and must stay on
// the thread that owns the connection because Connection is Send but !Sync.
unsafe impl Send for QueryStream<'static> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bindings;
    use crate::session::SessionBuilder;
    use crate::test_utils::tempdir;

    #[test]
    fn test_large_stream_row_count_and_chunking() -> Result<()> {
        const ROWS: u64 = 100_000;
        let conn = Connection::open_in_memory()?;
        let mut stream = conn.query_stream(
            &format!("SELECT number FROM numbers({ROWS})"),
            OutputFormat::JSONEachRow,
        )?;

        let mut chunks = 0usize;
        let mut rows = 0u64;
        while let Some(chunk) = stream.next_chunk()? {
            chunks += 1;
            rows += chunk.rows_read();
        }

        assert_eq!(rows, ROWS, "streamed row count should match query size");
        assert!(
            chunks > 1,
            "expected multiple chunks for large result set, got {chunks}"
        );
        Ok(())
    }

    #[test]
    fn test_large_stream_matches_materialized_query() -> Result<()> {
        use crate::arg::Arg;
        use crate::execute;

        const ROWS: u64 = 100_000;
        let query = format!("SELECT number FROM numbers({ROWS})");
        let args = Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)] as &[Arg]);

        let materialized = execute(&query, args)?;
        let mut streamed = String::new();
        let conn = Connection::open_in_memory()?;
        let mut stream = conn.query_stream(&query, OutputFormat::JSONEachRow)?;
        while let Some(chunk) = stream.next_chunk()? {
            streamed.push_str(&chunk.data_utf8_lossy());
        }

        assert_eq!(streamed, materialized.data_utf8_lossy());
        Ok(())
    }

    #[test]
    fn test_raw_stream_ffi_matches_c() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let query = CString::new("SELECT number FROM numbers(5)")?;
        let format = CString::new("JSONEachRow")?;

        let stream =
            unsafe { bindings::chdb_stream_query(conn.handle(), query.as_ptr(), format.as_ptr()) };
        assert!(!stream.is_null());

        let chunk = unsafe { bindings::chdb_stream_fetch_result(conn.handle(), stream) };
        assert!(!chunk.is_null(), "fetch returned null");

        let result = QueryResult::new(chunk);
        result.check_error_ref()?;
        assert_eq!(result.rows_read(), 5);
        assert!(result.data_utf8_lossy().contains("\"number\":0"));

        unsafe {
            bindings::chdb_stream_cancel_query(conn.handle(), stream);
            bindings::chdb_destroy_query_result(stream);
        }
        Ok(())
    }

    #[test]
    fn test_execute_stream_matches_query() -> Result<()> {
        use crate::arg::Arg;
        use crate::execute;
        use crate::execute_stream;

        let query = "SELECT number FROM numbers(10)";
        let args = Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)] as &[Arg]);

        let materialized = execute(query, args)?;
        let mut streamed = String::new();
        let mut stream = execute_stream(query, args)?;
        while let Some(chunk) = stream.next_chunk()? {
            streamed.push_str(&chunk.data_utf8_lossy());
        }

        assert_eq!(streamed, materialized.data_utf8_lossy());
        Ok(())
    }

    #[test]
    fn test_query_stream_basic() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let mut stream =
            conn.query_stream("SELECT number FROM numbers(5)", OutputFormat::JSONEachRow)?;

        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next_chunk()? {
            chunks.push(chunk.data_utf8_lossy().to_string());
        }

        let combined: String = chunks.concat();
        assert!(combined.contains("\"number\":0"));
        assert!(combined.contains("\"number\":4"));
        Ok(())
    }

    #[test]
    fn test_query_stream_iterator() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let stream = conn.query_stream("SELECT 1 AS a UNION ALL SELECT 2", OutputFormat::CSV)?;

        let chunks: Vec<_> = stream.collect::<Result<Vec<_>>>()?;
        assert!(!chunks.is_empty());
        Ok(())
    }

    #[test]
    fn test_query_stream_error_then_retry_returns_none() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let mut stream =
            conn.query_stream("SELECT * FROM nonexistent_table", OutputFormat::JSONEachRow)?;

        assert!(stream.next_chunk().is_err());
        assert!(stream.next_chunk()?.is_none());
        Ok(())
    }

    #[test]
    fn test_query_stream_syntax_error_fails_at_start() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let result = conn.query_stream("SELECT invalid syntax here", OutputFormat::JSONEachRow);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_session_execute_stream() -> Result<()> {
        let tmp = tempdir();
        let session = SessionBuilder::new()
            .with_data_path(tmp.path())
            .with_auto_cleanup(true)
            .build()?;

        session.execute(
            "CREATE TABLE items (id UInt64) ENGINE = MergeTree() ORDER BY id",
            None,
        )?;
        session.execute("INSERT INTO items VALUES (1), (2), (3)", None)?;

        let mut stream = session.execute_stream("SELECT * FROM items", None)?;
        let mut total_rows = 0usize;
        while let Some(chunk) = stream.next_chunk()? {
            total_rows += chunk.data_ref().iter().filter(|&&b| b == b'\n').count();
        }

        assert_eq!(total_rows, 3);
        Ok(())
    }
}
