use std::{
    ffi::{c_char, CString},
    slice,
    time::Duration,
};

use crate::bindings;

pub fn query(query: &str, format: &str) -> Option<LocalResult> {
    let mut argv: Vec<String> = Vec::new();
    argv.push("clickhouse".to_string());
    argv.push("--multiquery".to_string());
    argv.push(format!("--output-format={format}"));
    argv.push(format!("--query={query}"));

    let argc = argv.len() as i32;
    let mut argv: Vec<*mut c_char> = argv
        .into_iter()
        .map(|arg| CString::new(arg).unwrap().into_raw())
        .collect();

    let argv = argv.as_mut_ptr();
    let local = unsafe { bindings::query_stable(argc, argv) };
    if local.is_null() {
        return None;
    }

    Some(LocalResult { local })
}

#[derive(Debug, Clone)]
pub struct LocalResult {
    pub(crate) local: *mut bindings::local_result,
}

impl LocalResult {
    pub fn rows_read(&self) -> u64 {
        (unsafe { *self.local }).rows_read
    }

    pub fn bytes_read(&self) -> u64 {
        unsafe { (*self.local).bytes_read }
    }

    pub fn buf(&self) -> &[u8] {
        let buf = unsafe { (*self.local).buf };
        let len = unsafe { (*self.local).len };
        let bytes: &[u8] = unsafe { slice::from_raw_parts(buf as *const u8, len) };
        bytes
    }

    pub fn elapsed(&self) -> Duration {
        let elapsed = unsafe { (*self.local).elapsed };
        Duration::from_secs_f64(elapsed)
    }
}

impl Drop for LocalResult {
    fn drop(&mut self) {
        unsafe { bindings::free_result(self.local) };
    }
}
