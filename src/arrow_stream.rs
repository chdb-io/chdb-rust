//! Arrow streaming support for chDB.
//!
//! This module provides types and functions for registering Arrow streams and arrays
//! as table functions in chDB, enabling efficient data transfer between Arrow
//! and ClickHouse formats.
//!
//! # Overview
//!
//! Arrow streaming allows you to:
//! - Register Arrow streams as virtual tables that can be queried with SQL
//! - Register Arrow arrays as virtual tables
//! - Unregister tables when done
//!
//! # Examples
//!
//! ```no_run
//! use chdb_rust::connection::Connection;
//! use chdb_rust::arrow_stream::{ArrowStream, ArrowSchema, ArrowArray};
//!
//! // Create a connection
//! let conn = Connection::open_in_memory()?;
//!
//! // Register an Arrow stream as a table (assuming you have an Arrow stream handle)
//! // let arrow_stream = ArrowStream::from_raw(stream_ptr);
//! // conn.register_arrow_stream("my_table", &arrow_stream)?;
//!
//! // Query the registered table
//! // let result = conn.query("SELECT * FROM my_table", OutputFormat::JSONEachRow)?;
//!
//! // Unregister when done
//! // conn.unregister_arrow_table("my_table")?;
//! # Ok::<(), chdb_rust::error::Error>(())
//! ```

use crate::bindings;

/// A handle to an Arrow stream.
///
/// This is a wrapper around the opaque `chdb_arrow_stream` pointer type.
/// Arrow streams are typically created from Arrow C++ or other Arrow implementations.
///
/// # Safety
///
/// The underlying pointer must be valid and must remain valid for the lifetime
/// of this handle. The handle does not take ownership of the Arrow stream.
#[derive(Debug, Clone, Copy)]
pub struct ArrowStream {
    inner: bindings::chdb_arrow_stream,
}

unsafe impl Send for ArrowStream {}

impl ArrowStream {
    /// Create an `ArrowStream` from a raw `chdb_arrow_stream` pointer.
    ///
    /// # Safety
    ///
    /// The pointer must be valid and point to a valid Arrow stream handle.
    /// The caller is responsible for ensuring the stream remains valid for
    /// the lifetime of this handle.
    ///
    /// # Arguments
    ///
    /// * `stream` - A raw pointer to a `chdb_arrow_stream`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::arrow_stream::ArrowStream;
    ///
    /// // Assuming you have a valid Arrow stream pointer from Arrow C++
    /// // let stream_ptr: *mut chdb_arrow_stream_ = ...;
    /// // let arrow_stream = unsafe { ArrowStream::from_raw(stream_ptr) };
    /// ```
    pub unsafe fn from_raw(stream: bindings::chdb_arrow_stream) -> Self {
        Self { inner: stream }
    }

    /// Get the raw pointer to the underlying Arrow stream handle.
    ///
    /// # Returns
    ///
    /// Returns the raw `chdb_arrow_stream` pointer.
    pub fn as_raw(&self) -> bindings::chdb_arrow_stream {
        self.inner
    }
}

/// A handle to an Arrow schema.
///
/// This is a wrapper around the opaque `chdb_arrow_schema` pointer type.
/// Arrow schemas define the structure of Arrow data.
///
/// # Safety
///
/// The underlying pointer must be valid and must remain valid for the lifetime
/// of this handle. The handle does not take ownership of the Arrow schema.
#[derive(Debug, Clone, Copy)]
pub struct ArrowSchema {
    inner: bindings::chdb_arrow_schema,
}

unsafe impl Send for ArrowSchema {}

impl ArrowSchema {
    /// Create an `ArrowSchema` from a raw `chdb_arrow_schema` pointer.
    ///
    /// # Safety
    ///
    /// The pointer must be valid and point to a valid Arrow schema handle.
    /// The caller is responsible for ensuring the schema remains valid for
    /// the lifetime of this handle.
    ///
    /// # Arguments
    ///
    /// * `schema` - A raw pointer to a `chdb_arrow_schema`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::arrow_stream::ArrowSchema;
    ///
    /// // Assuming you have a valid Arrow schema pointer from Arrow C++
    /// // let schema_ptr: *mut chdb_arrow_schema_ = ...;
    /// // let arrow_schema = unsafe { ArrowSchema::from_raw(schema_ptr) };
    /// ```
    pub unsafe fn from_raw(schema: bindings::chdb_arrow_schema) -> Self {
        Self { inner: schema }
    }

    /// Get the raw pointer to the underlying Arrow schema handle.
    ///
    /// # Returns
    ///
    /// Returns the raw `chdb_arrow_schema` pointer.
    pub fn as_raw(&self) -> bindings::chdb_arrow_schema {
        self.inner
    }
}

/// A handle to an Arrow array.
///
/// This is a wrapper around the opaque `chdb_arrow_array` pointer type.
/// Arrow arrays contain the actual data in Arrow format.
///
/// # Safety
///
/// The underlying pointer must be valid and must remain valid for the lifetime
/// of this handle. The handle does not take ownership of the Arrow array.
#[derive(Debug, Clone, Copy)]
pub struct ArrowArray {
    inner: bindings::chdb_arrow_array,
}

unsafe impl Send for ArrowArray {}

impl ArrowArray {
    /// Create an `ArrowArray` from a raw `chdb_arrow_array` pointer.
    ///
    /// # Safety
    ///
    /// The pointer must be valid and point to a valid Arrow array handle.
    /// The caller is responsible for ensuring the array remains valid for
    /// the lifetime of this handle.
    ///
    /// # Arguments
    ///
    /// * `array` - A raw pointer to a `chdb_arrow_array`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::arrow_stream::ArrowArray;
    ///
    /// // Assuming you have a valid Arrow array pointer from Arrow C++
    /// // let array_ptr: *mut chdb_arrow_array_ = ...;
    /// // let arrow_array = unsafe { ArrowArray::from_raw(array_ptr) };
    /// ```
    pub unsafe fn from_raw(array: bindings::chdb_arrow_array) -> Self {
        Self { inner: array }
    }

    /// Get the raw pointer to the underlying Arrow array handle.
    ///
    /// # Returns
    ///
    /// Returns the raw `chdb_arrow_array` pointer.
    pub fn as_raw(&self) -> bindings::chdb_arrow_array {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_stream_from_raw() {
        // Test that we can create an ArrowStream from a null pointer
        // (this is just testing the API, not actual functionality)
        let null_ptr = std::ptr::null_mut();
        let stream = unsafe { ArrowStream::from_raw(null_ptr) };
        assert!(stream.as_raw().is_null());
    }

    #[test]
    fn test_arrow_schema_from_raw() {
        let null_ptr = std::ptr::null_mut();
        let schema = unsafe { ArrowSchema::from_raw(null_ptr) };
        assert!(schema.as_raw().is_null());
    }

    #[test]
    fn test_arrow_array_from_raw() {
        let null_ptr = std::ptr::null_mut();
        let array = unsafe { ArrowArray::from_raw(null_ptr) };
        assert!(array.as_raw().is_null());
    }
}
