use core::slice;
use std::borrow::Cow;
use std::ffi::CStr;
use std::time::Duration;

use crate::bindings;
use crate::error::Error;
use crate::error::Result;

#[derive(Debug)]
pub struct QueryResult {
    inner: *mut bindings::chdb_result,
}

// Safety: QueryResult is safe to send between threads
unsafe impl Send for QueryResult {}

impl QueryResult {
    pub(crate) fn new(inner: *mut bindings::chdb_result) -> Self {
        Self { inner }
    }

    pub fn data_utf8(&self) -> Result<String> {
        let buf = self.data_ref();
        String::from_utf8(buf.to_vec()).map_err(Error::NonUtf8Sequence)
    }

    pub fn data_utf8_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.data_ref())
    }

    pub fn data_utf8_unchecked(&self) -> String {
        unsafe { String::from_utf8_unchecked(self.data_ref().to_vec()) }
    }

    pub fn data_ref(&self) -> &[u8] {
        let buf = unsafe { bindings::chdb_result_buffer(self.inner) };
        let len = unsafe { bindings::chdb_result_length(self.inner) };
        if buf.is_null() || len == 0 {
            return &[];
        }
        unsafe { slice::from_raw_parts(buf as *const u8, len) }
    }

    pub fn rows_read(&self) -> u64 {
        unsafe { bindings::chdb_result_rows_read(self.inner) }
    }

    pub fn bytes_read(&self) -> u64 {
        unsafe { bindings::chdb_result_bytes_read(self.inner) }
    }

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
