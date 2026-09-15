//! Arrow C Data Interface batch streaming for chDB query results.
//!
//! This module wraps `chdb_stream_query_arrow` / `chdb_stream_fetch_arrow` and
//! yields [`arrow::record_batch::RecordBatch`] values one engine block at a time,
//! without Arrow IPC serialization.
//!
//! Available when the crate is built with the `arrow` feature.

use std::ffi::CStr;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;

use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::record_batch::RecordBatch;

use crate::arrow_options::ArrowOptions;
use crate::bindings;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::query_param::{EncodedParams, QueryParams};
use crate::query_result::QueryResult;

enum ArrowQueryStreamConnection<'a> {
    Borrowed(&'a mut Connection),
    Owned(Connection),
}

/// A one-shot Arrow reader over a query's entire result.
///
/// Returned by [`Connection::query_arrow`](crate::connection::Connection::query_arrow)
/// and [`Connection::query_arrow_with_opts`](crate::connection::Connection::query_arrow_with_opts).
///
/// The C ABI transfers ownership of the stream's `release` callback to the
/// caller. The engine materializes a standalone Arrow table, so this reader
/// does not borrow the [`Connection`] that produced it and can be drained
/// after that connection is dropped.
pub struct ArrowReader {
    inner: ArrowArrayStreamReader,
}

impl ArrowReader {
    pub(crate) fn new(inner: ArrowArrayStreamReader) -> Self {
        Self { inner }
    }
}

impl Iterator for ArrowReader {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl arrow::array::RecordBatchReader for ArrowReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

/// A streaming Arrow query that yields [`RecordBatch`] values one block at a time.
///
/// Batches come from the Arrow C Data Interface (no Arrow IPC). Create a stream from a
/// [`Connection`], [`Session`](crate::session::Session), or the crate-level
/// `execute_stream_arrow*` helpers. Implements [`Iterator`].
///
/// # Thread Safety
///
/// While a borrowed stream is active, its [`Connection`](crate::connection::Connection)
/// is exclusively borrowed and cannot be used for other queries. This prevents concurrent
/// access to a non-[`Sync`] handle. Streams from [`execute_stream_arrow`](crate::execute_stream_arrow)
/// own their connection outright.
pub struct ArrowQueryStream<'a> {
    conn: ArrowQueryStreamConnection<'a>,
    inner: *mut bindings::chdb_result,
    finished: bool,
}

impl<'a> ArrowQueryStream<'a> {
    pub(crate) fn start_borrowed(
        conn: &'a mut Connection,
        sql: &str,
        opts: Option<&ArrowOptions>,
    ) -> Result<Self> {
        let inner = Self::start_query(conn.handle(), sql, opts)?;
        Ok(Self {
            conn: ArrowQueryStreamConnection::Borrowed(conn),
            inner,
            finished: false,
        })
    }

    pub(crate) fn start_owned(
        conn: Connection,
        sql: &str,
        opts: Option<&ArrowOptions>,
    ) -> Result<Self> {
        let inner = Self::start_query(conn.handle(), sql, opts)?;
        Ok(Self {
            conn: ArrowQueryStreamConnection::Owned(conn),
            inner,
            finished: false,
        })
    }

    pub(crate) fn start_borrowed_with_params(
        conn: &'a mut Connection,
        sql: &str,
        params: impl Into<QueryParams>,
        opts: Option<&ArrowOptions>,
    ) -> Result<Self> {
        let inner = Self::start_query_with_params(conn.handle(), sql, params, opts)?;
        Ok(Self {
            conn: ArrowQueryStreamConnection::Borrowed(conn),
            inner,
            finished: false,
        })
    }

    pub(crate) fn start_owned_with_params(
        conn: Connection,
        sql: &str,
        params: impl Into<QueryParams>,
        opts: Option<&ArrowOptions>,
    ) -> Result<Self> {
        let inner = Self::start_query_with_params(conn.handle(), sql, params, opts)?;
        Ok(Self {
            conn: ArrowQueryStreamConnection::Owned(conn),
            inner,
            finished: false,
        })
    }

    fn start_query(
        conn: bindings::chdb_connection,
        sql: &str,
        opts: Option<&ArrowOptions>,
    ) -> Result<*mut bindings::chdb_result> {
        // chdb_stream_query_arrow_n takes pointer + length for the query text,
        // so it does not have to be NUL-terminated. The C struct must outlive
        // the call, so it is materialized here rather than in a temporary
        // inside the argument list; a null options pointer asks for the
        // engine's default type mapping. It returns an owned streaming
        // chdb_result handle (or null on failure) that the caller must both
        // cancel with chdb_stream_cancel_query and free with
        // chdb_destroy_query_result once done — ArrowQueryStream::cancel
        // (called from Drop) does both. `check_start`, below, probes the
        // handle for a start-up error without taking ownership of it.
        let c_opts = opts.map(|o| o.to_c());
        let opts_ptr = c_opts.as_ref().map_or(std::ptr::null(), |o| {
            o as *const bindings::chdb_arrow_options
        });

        let stream_ptr = unsafe {
            bindings::chdb_stream_query_arrow_n(
                conn,
                sql.as_ptr() as *const c_char,
                sql.len(),
                opts_ptr,
            )
        };

        Self::check_start(stream_ptr)
    }

    /// Wraps `chdb_stream_query_arrow_with_params_n`. A null options pointer
    /// asks for the engine's default type mapping. It returns an owned
    /// streaming `chdb_result` handle (or null on failure) that the caller
    /// must both cancel with `chdb_stream_cancel_query` and free with
    /// `chdb_destroy_query_result` once done — `ArrowQueryStream::cancel`
    /// (called from `Drop`) does both.
    /// A non-null stream handle may still carry an initialisation error, so the
    /// handle is probed once before it is handed out. The probe must not free
    /// the handle on the success path, hence the `ManuallyDrop` dance.
    fn check_start(stream_ptr: *mut bindings::chdb_result) -> Result<*mut bindings::chdb_result> {
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

    fn start_query_with_params(
        conn: bindings::chdb_connection,
        sql: &str,
        params: impl Into<QueryParams>,
        opts: Option<&ArrowOptions>,
    ) -> Result<*mut bindings::chdb_result> {
        let encoded = EncodedParams::encode(params)?;
        // Materialized into a named local so the pointer handed to C outlives the call.
        let c_opts = opts.map(|o| o.to_c());
        let opts_ptr = c_opts.as_ref().map_or(std::ptr::null(), |o| {
            o as *const bindings::chdb_arrow_options
        });

        // chdb_stream_query_arrow_with_params_n takes pointer + length for the query
        // and each bound value. A null options pointer selects the engine default
        // type mapping. It returns an owned streaming chdb_result handle (or null
        // on failure) that ArrowQueryStream::cancel (called from Drop) cancels and
        // frees.
        let stream_ptr = unsafe {
            bindings::chdb_stream_query_arrow_with_params_n(
                conn,
                sql.as_ptr() as *const c_char,
                sql.len(),
                opts_ptr,
                encoded.names_ptr(),
                encoded.name_lens_ptr(),
                encoded.values_ptr(),
                encoded.value_lens_ptr(),
                encoded.len(),
            )
        };

        Self::check_start(stream_ptr)
    }

    fn conn_handle(&self) -> bindings::chdb_connection {
        match &self.conn {
            ArrowQueryStreamConnection::Borrowed(conn) => conn.handle(),
            ArrowQueryStreamConnection::Owned(conn) => conn.handle(),
        }
    }

    /// Fetch the next Arrow record batch from the query stream.
    ///
    /// Returns `Ok(None)` when the stream is exhausted, including empty result sets
    /// (libchdb currently returns a null-schema stream for zero-row queries).
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished || self.inner.is_null() {
            return Ok(None);
        }

        let mut ffi_stream = FFI_ArrowArrayStream::empty();
        let out_ptr = (&mut ffi_stream as *mut FFI_ArrowArrayStream).cast();

        let state =
            unsafe { bindings::chdb_stream_fetch_arrow(self.conn_handle(), self.inner, out_ptr) };

        if state != bindings::chdb_state_CHDBSuccess {
            self.finished = true;
            let err_msg = unsafe { bindings::chdb_result_error(self.inner) };
            let detail = if err_msg.is_null() {
                "chdb_stream_fetch_arrow failed".to_string()
            } else {
                unsafe { CStr::from_ptr(err_msg) }
                    .to_string_lossy()
                    .into_owned()
            };
            return Err(Error::QueryError(detail));
        }

        if is_empty_result_stream(&ffi_stream) {
            self.finished = true;
            return Ok(None);
        }

        let mut reader = match ArrowArrayStreamReader::try_new(ffi_stream) {
            Ok(reader) => reader,
            Err(e) => {
                self.finished = true;
                return Err(Error::QueryError(e.to_string()));
            }
        };

        match reader.next() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => {
                self.finished = true;
                Err(Error::QueryError(e.to_string()))
            }
            None => {
                self.finished = true;
                Ok(None)
            }
        }
    }

    /// Cancel the streaming query and release resources.
    pub fn cancel(&mut self) {
        if self.inner.is_null() {
            return;
        }

        if !self.finished {
            unsafe {
                bindings::chdb_stream_cancel_query(self.conn_handle(), self.inner);
            }
        }
        unsafe {
            bindings::chdb_destroy_query_result(self.inner);
        }
        self.inner = std::ptr::null_mut();
        self.finished = true;
    }
}

/// libchdb emits a one-batch stream with a null schema for empty result sets;
/// the stream's `get_schema` callback returns a non-zero code until that is fixed upstream.
fn is_empty_result_stream(ffi_stream: &FFI_ArrowArrayStream) -> bool {
    let Some(get_schema) = ffi_stream.get_schema else {
        return true;
    };
    let mut schema = FFI_ArrowSchema::empty();
    let stream_ptr = (ffi_stream as *const FFI_ArrowArrayStream).cast_mut();
    let ret_code = unsafe { get_schema(stream_ptr, &mut schema) };
    ret_code != 0
}

impl Iterator for ArrowQueryStream<'_> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_batch() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

impl Drop for ArrowQueryStream<'_> {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            self.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::SessionBuilder;
    use crate::test_utils::tempdir;

    #[test]
    fn test_arrow_query_stream_row_count_and_chunking() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        let mut stream = conn.query_stream_arrow("SELECT number FROM numbers(100_000)")?;

        let mut batches = 0usize;
        let mut rows = 0usize;
        while let Some(batch) = stream.next_batch()? {
            batches += 1;
            rows += batch.num_rows();
        }

        assert_eq!(rows, 100_000);
        assert!(batches > 1, "expected multiple batches, got {batches}");
        Ok(())
    }

    #[test]
    fn test_execute_stream_arrow() -> Result<()> {
        use crate::execute_stream_arrow;

        let mut stream = execute_stream_arrow("SELECT number FROM numbers(10)")?;
        let mut rows = 0usize;
        while let Some(batch) = stream.next_batch()? {
            rows += batch.num_rows();
        }

        assert_eq!(rows, 10);
        Ok(())
    }

    #[test]
    fn test_session_execute_stream_arrow() -> Result<()> {
        let tmp = tempdir();
        let mut session = SessionBuilder::new()
            .with_data_path(tmp.path())
            .with_auto_cleanup(true)
            .build()?;

        session.execute(
            "CREATE TABLE items (id UInt64) ENGINE = MergeTree() ORDER BY id",
            None,
        )?;
        session.execute("INSERT INTO items VALUES (1), (2), (3)", None)?;

        let mut stream = session.execute_stream_arrow("SELECT * FROM items")?;
        let mut rows = 0usize;
        while let Some(batch) = stream.next_batch()? {
            rows += batch.num_rows();
        }

        assert_eq!(rows, 3);
        Ok(())
    }

    #[test]
    fn test_arrow_query_stream_iterator() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        let stream = conn.query_stream_arrow("SELECT number FROM numbers(5)")?;

        let batches: Vec<_> = stream.collect::<Result<Vec<_>>>()?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        Ok(())
    }

    #[test]
    fn test_arrow_query_stream_empty_filter_returns_none() -> Result<()> {
        let tmp = tempdir();
        let mut session = SessionBuilder::new()
            .with_data_path(tmp.path())
            .with_auto_cleanup(true)
            .build()?;

        session.execute(
            "CREATE TABLE items (id UInt64) ENGINE = MergeTree() ORDER BY id",
            None,
        )?;
        session.execute("INSERT INTO items VALUES (1), (2), (3)", None)?;

        let mut stream = session.execute_stream_arrow("SELECT * FROM items WHERE id > 100")?;
        assert!(stream.next_batch()?.is_none());
        assert!(stream.next_batch()?.is_none());
        Ok(())
    }

    #[test]
    fn test_arrow_query_stream_error_then_retry_returns_none() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        let mut stream = conn.query_stream_arrow("SELECT * FROM nonexistent_table")?;

        assert!(stream.next_batch().is_err());
        assert!(stream.next_batch()?.is_none());
        Ok(())
    }

    #[test]
    fn test_arrow_query_stream_syntax_error_fails_at_start() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        let result = conn.query_stream_arrow("SELECT invalid syntax here");
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_arrow_query_stream_with_params_empty_filter_returns_none() -> Result<()> {
        let tmp = tempdir();
        let mut session = SessionBuilder::new()
            .with_data_path(tmp.path())
            .with_auto_cleanup(true)
            .build()?;

        session.execute(
            "CREATE TABLE items (id UInt64) ENGINE = MergeTree() ORDER BY id",
            None,
        )?;
        session.execute("INSERT INTO items VALUES (1), (2), (3)", None)?;

        let mut stream = session.execute_stream_arrow_with_params(
            "SELECT * FROM items WHERE id > {min_id:UInt64}",
            [("min_id", 100_u64)],
            None,
        )?;
        assert!(stream.next_batch()?.is_none());
        assert!(stream.next_batch()?.is_none());
        Ok(())
    }

    #[test]
    fn test_arrow_query_stream_with_params_error_then_retry_returns_none() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        let mut stream = conn.query_stream_arrow_with_params(
            "SELECT * FROM nonexistent_table WHERE id = {id:UInt64}",
            [("id", 1_u64)],
            None,
        )?;

        assert!(stream.next_batch().is_err());
        assert!(stream.next_batch()?.is_none());
        Ok(())
    }

    #[test]
    fn test_arrow_query_stream_with_params_syntax_error_fails_at_start() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        let result =
            conn.query_stream_arrow_with_params("SELECT invalid syntax here", [("x", 1_u64)], None);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_arrow_query_stream_early_drop() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        let mut stream = conn.query_stream_arrow("SELECT number FROM numbers(1_000_000)")?;
        let first = stream.next_batch()?.expect("expected a batch");
        assert!(first.num_rows() > 0);
        drop(stream);
        Ok(())
    }
}
