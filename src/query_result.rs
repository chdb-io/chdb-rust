use core::slice;
use std::time::Duration;

use crate::bindings;
use crate::error::Error;

#[derive(Clone)]
pub struct QueryResult(pub(crate) *mut bindings::local_result_v2);

impl QueryResult {
    pub fn data_utf8(&self) -> Result<String, Error> {
        String::from_utf8(self.data_ref().to_vec())
            .map_err(|e| Error::NonUtf8Sequence(e.to_string()))
    }

    pub fn data_utf8_unchecked(&self) -> String {
        unsafe { String::from_utf8_unchecked(self.data_ref().to_vec()) }
    }

    pub fn data_ref(&self) -> &[u8] {
        let buf = unsafe { (*self.0).buf };
        let len = unsafe { (*self.0).len };
        let bytes: &[u8] = unsafe { slice::from_raw_parts(buf as *const u8, len) };
        bytes
    }

    pub fn rows_read(&self) -> u64 {
        (unsafe { *self.0 }).rows_read
    }

    pub fn bytes_read(&self) -> u64 {
        unsafe { (*self.0).bytes_read }
    }

    pub fn elapsed(&self) -> Duration {
        let elapsed = unsafe { (*self.0).elapsed };
        Duration::from_secs_f64(elapsed)
    }

    pub(crate) fn check_error(self) -> Result<Self, Error> {
        let buf = unsafe { (*self.0).error_message };

        if buf.is_null() {
            return Ok(self);
        }

        let len = unsafe { (*self.0).len };
        let bytes: &[u8] = unsafe { slice::from_raw_parts(buf as *const u8, len) };

        Err(Error::QueryError(
            String::from_utf8_lossy(bytes).to_string(),
        ))
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        unsafe { bindings::free_result_v2(self.0) };
    }
}
