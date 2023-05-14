extern crate libc;

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

#[repr(C)]
struct local_result {
    buf: *mut c_char,
    size: usize,
}

#[link(name = "chdb")] 
extern "C" {
    fn query_stable(argc: i32, argv: *const *const c_char) -> *mut local_result;
}

pub fn execute(query: &str, format: &str) -> Option<String> {
    let mut argv: [*const c_char; 4] = [
        CString::new("clickhouse").unwrap().into_raw(),
        CString::new("--multiquery").unwrap().into_raw(),
        CString::new("--output-format=CSV").unwrap().into_raw(),
        CString::new("--query=").unwrap().into_raw(),
    ];

    let data_format = format!("--format={}", format);
    argv[2] = CString::new(data_format).unwrap().into_raw();

    let local_query = format!("--query={}", query);
    argv[3] = CString::new(local_query).unwrap().into_raw();

    let result = unsafe { query_stable(4, argv.as_ptr()) };

    unsafe {
        drop(CString::from_raw(argv[2] as *mut c_char));
        drop(CString::from_raw(argv[3] as *mut c_char));
    }

    if result.is_null() {
        return None;
    }

    let c_str = unsafe { CStr::from_ptr((*result).buf) };
    let output = c_str.to_string_lossy().into_owned();

    Some(output)
}

