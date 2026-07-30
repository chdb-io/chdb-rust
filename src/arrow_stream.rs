//! Arrow streaming support for chDB.
//!
//! Registered streams are queried via the ClickHouse table function
//! `ArrowStream('name')`, not as bare table identifiers.
//!
//! libchdb's `chdb_arrow_scan` / `chdb_arrow_array_scan` take raw Arrow C Data
//! Interface pointers (`ArrowArrayStream*`, `ArrowSchema*`, `ArrowArray*`).
//! The `chdb_arrow_*` typedefs in `chdb.h` are misleading wrappers; pass pointers
//! to the actual FFI structs (e.g. from the `arrow` crate).
//!
//! # Overview
//!
//! Arrow streaming allows you to:
//! - Register Arrow streams as virtual table functions queryable with SQL
//! - Register Arrow schema+array pairs as one-shot streams
//! - Unregister names when done
//!
//! # Examples
//!
//! ```no_run
//! use chdb_rust::arrow_stream::{arrow_stream_table_sql, ArrowStream};
//! use chdb_rust::connection::Connection;
//! use chdb_rust::format::OutputFormat;
//!
//! let conn = Connection::open_in_memory()?;
//!
//! // Register an Arrow stream (assuming you have an Arrow C Data Interface handle)
//! // let arrow_stream = unsafe { ArrowStream::from_raw(stream_ptr) };
//! // conn.register_arrow_stream("my_data", &arrow_stream)?;
//!
//! // Query via the ArrowStream table function, not a bare table name
//! // let sql = format!("SELECT * FROM {}", arrow_stream_table_sql("my_data"));
//! // let result = conn.query(&sql, OutputFormat::JSONEachRow)?;
//!
//! // Unregister when done
//! // conn.unregister_arrow_table("my_data")?;
//! # Ok::<(), chdb_rust::error::Error>(())
//! ```

use crate::bindings;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;

/// Raw Arrow C Data Interface stream pointer expected by `chdb_arrow_scan`.
pub type RawArrowArrayStream = *mut FFI_ArrowArrayStream;

/// Raw Arrow C Data Interface schema pointer expected by `chdb_arrow_array_scan`.
pub type RawArrowSchema = *mut FFI_ArrowSchema;

/// Raw Arrow C Data Interface array pointer expected by `chdb_arrow_array_scan`.
pub type RawArrowArray = *mut FFI_ArrowArray;

/// A handle to an Arrow C Data Interface `ArrowArrayStream`.
///
/// Arrow streams are typically created from the `arrow` crate or other Arrow
/// implementations that export the C Data Interface.
///
/// # Safety
///
/// The pointer must remain valid until the stream is unregistered or the
/// connection is closed. chDB does not take ownership (`chdb_arrow_scan` passes
/// `is_owner = false`).
#[derive(Debug, Clone, Copy)]
pub struct ArrowStream {
    inner: RawArrowArrayStream,
}

unsafe impl Send for ArrowStream {}

impl ArrowStream {
    /// Create an [`ArrowStream`] from a raw `ArrowArrayStream` pointer.
    ///
    /// # Safety
    ///
    /// `stream` must be a valid, non-null `ArrowArrayStream` for the lifetime of
    /// this handle (and until chDB unregisters the name).
    ///
    /// # Arguments
    ///
    /// * `stream` - A raw pointer to an `FFI_ArrowArrayStream`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::arrow_stream::ArrowStream;
    ///
    /// // Assuming you have a valid Arrow C Data Interface stream pointer
    /// // let stream_ptr: *mut arrow::ffi_stream::FFI_ArrowArrayStream = ...;
    /// // let arrow_stream = unsafe { ArrowStream::from_raw(stream_ptr) };
    /// ```
    pub unsafe fn from_raw(stream: RawArrowArrayStream) -> Self {
        Self { inner: stream }
    }

    /// Returns the raw pointer passed through to `chdb_arrow_scan`.
    ///
    /// # Returns
    ///
    /// Returns the raw `ArrowArrayStream` pointer.
    pub fn as_raw(&self) -> bindings::chdb_arrow_stream {
        self.inner.cast()
    }
}

/// A handle to an Arrow C Data Interface `ArrowSchema`.
///
/// Arrow schemas define the structure of Arrow data.
///
/// # Safety
///
/// The pointer must remain valid through registration and any queries that read
/// the registered stream.
#[derive(Debug, Clone, Copy)]
pub struct ArrowSchema {
    inner: RawArrowSchema,
}

unsafe impl Send for ArrowSchema {}

impl ArrowSchema {
    /// Create an [`ArrowSchema`] from a raw `ArrowSchema` pointer.
    ///
    /// # Safety
    ///
    /// `schema` must be a valid, non-null `ArrowSchema` pointer.
    ///
    /// # Arguments
    ///
    /// * `schema` - A raw pointer to an `FFI_ArrowSchema`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::arrow_stream::ArrowSchema;
    ///
    /// // Assuming you have a valid Arrow C Data Interface schema pointer
    /// // let schema_ptr: *mut arrow::ffi::FFI_ArrowSchema = ...;
    /// // let arrow_schema = unsafe { ArrowSchema::from_raw(schema_ptr) };
    /// ```
    pub unsafe fn from_raw(schema: RawArrowSchema) -> Self {
        Self { inner: schema }
    }

    /// Returns the raw pointer passed through to `chdb_arrow_array_scan`.
    ///
    /// # Returns
    ///
    /// Returns the raw `ArrowSchema` pointer.
    pub fn as_raw(&self) -> bindings::chdb_arrow_schema {
        self.inner.cast()
    }
}

/// A handle to an Arrow C Data Interface `ArrowArray`.
///
/// Arrow arrays contain the actual data in Arrow format.
///
/// # Safety
///
/// The pointer must remain valid through registration and any queries that read
/// the registered stream.
#[derive(Debug, Clone, Copy)]
pub struct ArrowArray {
    inner: RawArrowArray,
}

unsafe impl Send for ArrowArray {}

impl ArrowArray {
    /// Create an [`ArrowArray`] from a raw `ArrowArray` pointer.
    ///
    /// # Safety
    ///
    /// `array` must be a valid, non-null `ArrowArray` pointer.
    ///
    /// # Arguments
    ///
    /// * `array` - A raw pointer to an `FFI_ArrowArray`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::arrow_stream::ArrowArray;
    ///
    /// // Assuming you have a valid Arrow C Data Interface array pointer
    /// // let array_ptr: *mut arrow::ffi::FFI_ArrowArray = ...;
    /// // let arrow_array = unsafe { ArrowArray::from_raw(array_ptr) };
    /// ```
    pub unsafe fn from_raw(array: RawArrowArray) -> Self {
        Self { inner: array }
    }

    /// Returns the raw pointer passed through to `chdb_arrow_array_scan`.
    ///
    /// # Returns
    ///
    /// Returns the raw `ArrowArray` pointer.
    pub fn as_raw(&self) -> bindings::chdb_arrow_array {
        self.inner.cast()
    }
}

/// SQL table-expression for a registered Arrow stream name.
///
/// Registered names are exposed as the `ArrowStream` table function, not as
/// ordinary tables. Use this when building `INSERT ... SELECT` or ad-hoc queries.
///
/// # Example
///
/// ```
/// use chdb_rust::arrow_stream::arrow_stream_table_sql;
///
/// let sql = format!(
///     "INSERT INTO dest SELECT * FROM {}",
///     arrow_stream_table_sql("my_batch")
/// );
/// assert_eq!(sql, "INSERT INTO dest SELECT * FROM ArrowStream('my_batch')");
/// ```
pub fn arrow_stream_table_sql(table_name: &str) -> String {
    let escaped = table_name.replace('\'', "''");
    format!("ArrowStream('{escaped}')")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_stream_table_sql_escapes_quotes() {
        assert_eq!(arrow_stream_table_sql("a'b"), "ArrowStream('a''b')");
    }

    #[test]
    fn arrow_stream_from_raw_accepts_null() {
        let stream = unsafe { ArrowStream::from_raw(std::ptr::null_mut()) };
        assert!(stream.as_raw().is_null());
    }

    #[test]
    fn arrow_schema_from_raw_accepts_null() {
        let schema = unsafe { ArrowSchema::from_raw(std::ptr::null_mut()) };
        assert!(schema.as_raw().is_null());
    }

    #[test]
    fn arrow_array_from_raw_accepts_null() {
        let array = unsafe { ArrowArray::from_raw(std::ptr::null_mut()) };
        assert!(array.as_raw().is_null());
    }
}
