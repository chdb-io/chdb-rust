//! Arrow C Data Interface batch streaming for chDB query results.
//!
//! This module wraps `chdb_stream_query_arrow` / `chdb_stream_fetch_arrow` and
//! yields [`arrow::record_batch::RecordBatch`] values one engine block at a time,
//! without Arrow IPC serialization.
//!
//! Available when the crate is built with the `arrow` feature.

use std::ffi::{CStr, CString};
use std::mem::ManuallyDrop;

use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::record_batch::RecordBatch;

use crate::bindings;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::query_result::QueryResult;

enum ArrowQueryStreamConnection<'a> {
    Borrowed(&'a Connection),
    Owned(Connection),
}

/// A streaming Arrow query result that yields record batches.
///
/// Returned by [`Connection::query_stream_arrow`](crate::connection::Connection::query_stream_arrow),
/// [`Session::execute_stream_arrow`](crate::session::Session::execute_stream_arrow), and
/// [`execute_stream_arrow`](crate::execute_stream_arrow). Each batch is produced via the
/// Arrow C Data Interface.
///
/// `ArrowQueryStream` also implements [`Iterator`].
///
/// # Thread Safety
///
/// Only owned streams (for example from [`execute_stream_arrow`](crate::execute_stream_arrow))
/// implement [`Send`]. Streams tied to a borrowed [`Connection`](crate::connection::Connection)
/// or [`Session`](crate::session::Session) do not, because [`Connection`] is [`Send`] but not
/// [`Sync`]. Concurrent use from multiple threads is not recommended without external
/// synchronization.
pub struct ArrowQueryStream<'a> {
    conn: ArrowQueryStreamConnection<'a>,
    inner: *mut bindings::chdb_result,
    finished: bool,
}

// Safety: Only the owned variant (`ArrowQueryStream<'static>` from `execute_stream_arrow`) is
// Send. It owns the Connection outright. Borrowed streams hold `&Connection` and must stay on
// the thread that owns the connection because Connection is Send but !Sync.
unsafe impl Send for ArrowQueryStream<'static> {}

impl<'a> ArrowQueryStream<'a> {
    pub(crate) fn start_borrowed(conn: &'a Connection, sql: &str) -> Result<Self> {
        let inner = Self::start_query(conn.handle(), sql)?;
        Ok(Self {
            conn: ArrowQueryStreamConnection::Borrowed(conn),
            inner,
            finished: false,
        })
    }

    pub(crate) fn start_owned(conn: Connection, sql: &str) -> Result<Self> {
        let inner = Self::start_query(conn.handle(), sql)?;
        Ok(Self {
            conn: ArrowQueryStreamConnection::Owned(conn),
            inner,
            finished: false,
        })
    }

    fn start_query(
        conn: bindings::chdb_connection,
        sql: &str,
    ) -> Result<*mut bindings::chdb_result> {
        let query_cstr = CString::new(sql)?;

        let stream_ptr = unsafe {
            bindings::chdb_stream_query_arrow(conn, query_cstr.as_ptr(), std::ptr::null())
        };

        if stream_ptr.is_null() {
            return Err(Error::NoResult);
        }

        let probe = ManuallyDrop::new(QueryResult::new(stream_ptr));
        if let Err(e) = probe.check_error_ref() {
            let _ = ManuallyDrop::into_inner(probe);
            return Err(e);
        }

        Ok(stream_ptr)
    }

    fn conn_handle(&self) -> bindings::chdb_connection {
        match &self.conn {
            ArrowQueryStreamConnection::Borrowed(conn) => conn.handle(),
            ArrowQueryStreamConnection::Owned(conn) => conn.handle(),
        }
    }

    /// Fetch the next Arrow record batch from the query stream.
    ///
    /// Returns `Ok(None)` when the stream is exhausted.
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

        let mut reader = ArrowArrayStreamReader::try_new(ffi_stream)
            .map_err(|e| Error::QueryError(e.to_string()))?;

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
        let conn = Connection::open_in_memory()?;
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
        let session = SessionBuilder::new()
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
        let conn = Connection::open_in_memory()?;
        let stream = conn.query_stream_arrow("SELECT number FROM numbers(5)")?;

        let batches: Vec<_> = stream.collect::<Result<Vec<_>>>()?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        Ok(())
    }

    #[test]
    fn test_arrow_query_stream_early_drop() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let mut stream = conn.query_stream_arrow("SELECT number FROM numbers(1_000_000)")?;
        let first = stream.next_batch()?.expect("expected a batch");
        assert!(first.num_rows() > 0);
        drop(stream);
        Ok(())
    }
}
