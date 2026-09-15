//! Write-side streaming INSERT.
//!
//! The counterpart to [`QueryStream`](crate::query_stream::QueryStream): send
//! an INSERT statement without data, then push the rows in chunks in any input
//! format the engine understands. `append` blocks when the engine applies
//! backpressure, so a producer faster than the engine is throttled rather than
//! buffered without bound.
//!
//! # Ownership
//!
//! `chdb_stream_insert_n` returns a handle that is **never null** — an
//! initialisation failure is reported through `chdb_stream_insert_error`, not
//! by a null return. The handle must be destroyed on every path, including
//! error paths, and neither finishing nor cancelling frees it. [`Drop`] handles
//! the paths [`InsertStream::finish`] and [`InsertStream::cancel`] do not.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::time::Duration;

use crate::bindings;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::format::InputFormat;
use crate::query_param::{EncodedParams, QueryParams};
use crate::query_result::QueryResult;

/// What a finished insert wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteStats {
    /// Rows committed by this stream.
    pub rows_written: u64,
    /// Bytes committed by this stream.
    pub bytes_written: u64,
    /// Wall-clock time the engine spent on the insert.
    pub elapsed: Duration,
}

/// An open streaming INSERT.
///
/// The connection is exclusively borrowed for the life of the stream: chDB
/// accepts no other statement on a connection while one is open.
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
/// println!("wrote {} rows", stats.rows_written);
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
#[derive(Debug)]
pub struct InsertStream<'a> {
    handle: bindings::chdb_insert_stream,
    /// Held so the connection cannot be used while the stream is open.
    _conn: &'a mut Connection,
    /// Set by `finish`/`cancel` so `Drop` does not act a second time.
    done: bool,
}

impl<'a> InsertStream<'a> {
    pub(crate) fn start(conn: &'a mut Connection, sql: &str, format: InputFormat) -> Result<Self> {
        let handle = {
            let format = format.as_str();
            // Wraps chdb_stream_insert_n. The handle is never null; an init
            // failure is reported through chdb_stream_insert_error.
            unsafe {
                bindings::chdb_stream_insert_n(
                    conn.handle(),
                    sql.as_ptr() as *const c_char,
                    sql.len(),
                    format.as_ptr() as *const c_char,
                    format.len(),
                )
            }
        };

        let stream = Self {
            handle,
            _conn: conn,
            done: false,
        };

        // Checked through the constructed value so that a failed init still
        // destroys the handle when `stream` is dropped here.
        stream.check_error()?;
        Ok(stream)
    }

    pub(crate) fn start_with_params(
        conn: &'a mut Connection,
        sql: &str,
        format: InputFormat,
        params: impl Into<QueryParams>,
    ) -> Result<Self> {
        let format = format.as_str();
        let encoded = EncodedParams::encode(params)?;

        let handle = {
            // Wraps chdb_stream_insert_with_params_n. The handle is never null;
            // an init failure is reported through chdb_stream_insert_error.
            // Bindings are captured during initialisation and cleared when it
            // returns, so `encoded` need only outlive this call. When
            // `encoded.len() == 0` both pointer args are null, which the C API
            // accepts for an empty parameter list.
            unsafe {
                bindings::chdb_stream_insert_with_params_n(
                    conn.handle(),
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
            }
        };

        let stream = Self {
            handle,
            _conn: conn,
            done: false,
        };
        stream.check_error()?;
        Ok(stream)
    }

    /// The stream's pending error, if it has one.
    fn check_error(&self) -> Result<()> {
        // Wraps chdb_stream_insert_error. The returned string is owned by the
        // stream and is valid until the handle is destroyed.
        let err = unsafe { bindings::chdb_stream_insert_error(self.handle) };
        if err.is_null() {
            return Ok(());
        }

        let detail = unsafe { CStr::from_ptr(err) }
            .to_string_lossy()
            .into_owned();
        Err(Error::QueryError(detail))
    }

    /// Append one chunk of format-encoded data.
    ///
    /// The bytes must be in the input format the stream was opened with. Chunk
    /// boundaries need not align to row boundaries. The data is copied by the
    /// engine, so the caller keeps ownership.
    ///
    /// May block while the engine applies backpressure.
    ///
    /// # Errors
    ///
    /// Returns [`Error::QueryError`] if the stream has already failed — a
    /// malformed row in an earlier chunk, for instance — or was finalised.
    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        // Wraps chdb_stream_append.
        let state = unsafe {
            bindings::chdb_stream_append(
                self.handle,
                data.as_ptr() as *const std::ffi::c_void,
                data.len(),
            )
        };

        if state != bindings::chdb_state_CHDBSuccess {
            // The state says only that it failed; the reason is on the stream.
            self.check_error()?;
            return Err(Error::Unknown);
        }

        Ok(())
    }

    /// Finalise the insert and commit.
    ///
    /// # Errors
    ///
    /// Returns [`Error::QueryError`] if the engine rejected any of the data.
    pub fn finish(mut self) -> Result<WriteStats> {
        self.done = true;

        // Wraps chdb_stream_done. Returns a result that must be destroyed, and
        // does NOT free the stream handle — Drop still does that.
        let result_ptr = unsafe { bindings::chdb_stream_done(self.handle) };
        if result_ptr.is_null() {
            self.check_error()?;
            return Err(Error::NoResult);
        }

        let result = QueryResult::new(result_ptr).check_error()?;
        Ok(WriteStats {
            rows_written: result.rows_written(),
            bytes_written: result.bytes_written(),
            elapsed: result.elapsed(),
        })
    }

    /// Abort without committing.
    ///
    /// ClickHouse-default semantics: no rollback of anything already flushed.
    pub fn cancel(mut self) {
        self.done = true;
        // Wraps chdb_stream_cancel_insert. Does not free the handle.
        unsafe { bindings::chdb_stream_cancel_insert(self.handle) };
    }
}

impl Drop for InsertStream<'_> {
    fn drop(&mut self) {
        if !self.done {
            // Wraps chdb_stream_cancel_insert. An unfinished stream is
            // abandoned, not committed.
            unsafe { bindings::chdb_stream_cancel_insert(self.handle) };
        }
        // Wraps chdb_destroy_insert_stream. Required on every path, finished
        // or not.
        unsafe { bindings::chdb_destroy_insert_stream(self.handle) };
    }
}

/// Lets anything that writes bytes — `serde_json::to_writer`, `csv::Writer`,
/// `write!` — feed the stream directly.
///
/// Errors from [`append`](InsertStream::append) are surfaced as
/// [`std::io::ErrorKind::Other`] carrying the engine's message, because that is
/// the only shape `io::Write` has for them.
impl std::io::Write for InsertStream<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.append(buf)
            .map(|()| buf.len())
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// A no-op: the engine buffers, and there is nothing held on this side.
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
