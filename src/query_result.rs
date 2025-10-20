use core::slice;
use std::borrow::Cow;
use std::ffi::CStr;
use std::time::Duration;

use crate::bindings;
use crate::error::Error;
use crate::error::Result;
#[derive(Clone)]
pub struct QueryResult {
    inner: *mut bindings::local_result_v2,
}

impl QueryResult {
    pub(crate) fn new(inner: *mut bindings::local_result_v2) -> Self {
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
        let inner = self.inner;
        let buf = unsafe { (*inner).buf };
        let len = unsafe { (*inner).len };
        let bytes: &[u8] = unsafe { slice::from_raw_parts(buf as *const u8, len) };
        bytes
    }

    pub fn rows_read(&self) -> u64 {
        let inner = self.inner;
        unsafe { *inner }.rows_read
    }

    pub fn bytes_read(&self) -> u64 {
        let inner = self.inner;
        unsafe { *inner }.bytes_read
    }

    pub fn elapsed(&self) -> Duration {
        let elapsed = unsafe { (*self.inner).elapsed };
        Duration::from_secs_f64(elapsed)
    }

    pub(crate) fn check_error(self) -> Result<Self> {
        self.check_error_ref()?;
        Ok(self)
    }
    pub(crate) fn check_error_ref(&self) -> Result<()> {
        let err_ptr = unsafe { (*self.inner).error_message };

        if err_ptr.is_null() {
            return Ok(());
        }

        Err(Error::QueryError(unsafe {
            CStr::from_ptr(err_ptr).to_string_lossy().to_string()
        }))
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        unsafe { bindings::free_result_v2(self.inner) };
    }
}
