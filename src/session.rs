use std::ffi::{c_char, CString};

use crate::{LocalResult, bindings};

pub struct Session {
    pub(crate) format: String,
    pub(crate) data_path: String,
    pub(crate) udf_path: String,
    pub(crate) log_level: String,
}

impl Session {
    pub fn execute(&self, query: impl Into<String>) -> Option<LocalResult> {
        let argv = vec![
            "clickhouse".to_string(),
            "--multiquery".to_string(),
            format!("--output-format={}", self.format),
            format!("--query={}", query.into()),
            format!("--path={}", self.data_path),
            format!("--log-level={}", self.log_level)
//            format!("--user_scripts_path={}", self.udf_path),
//            format!("--user_defined_executable_functions_config={}/*.xml", self.udf_path),
        ];

        let argc = argv.len() as i32;
        
        let mut argv: Vec<*mut c_char> = argv
        .into_iter()
        .map(|arg| CString::new(arg).unwrap().into_raw())
        .collect();

        let argv = argv.as_mut_ptr();
        let local = unsafe { bindings::query_stable(argc, argv) };
        if local.is_null() {
            return None
        }
        
        Some(LocalResult { local })
    }
}
