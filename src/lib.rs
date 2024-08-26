pub mod arg;
#[allow(
    dead_code,
    unused,
    non_snake_case,
    non_camel_case_types,
    non_upper_case_globals
)]
mod bindings;
pub mod error;
pub mod format;
pub mod log_level;
pub mod query_result;
pub mod session;

use std::ffi::{c_char, CString};

use crate::arg::Arg;
use crate::error::Error;
use crate::query_result::QueryResult;

pub fn execute(query: &str, query_args: Option<&[Arg]>) -> Result<Option<QueryResult>, Error> {
    let mut argv = Vec::with_capacity(query_args.as_ref().map_or(0, |v| v.len()) + 2);
    argv.push(arg_clickhouse()?.into_raw());

    if let Some(args) = query_args {
        for arg in args {
            argv.push(arg.to_cstring()?.into_raw());
        }
    }

    argv.push(arg_query(query)?.into_raw());
    call_chdb(argv)
}

fn call_chdb(mut argv: Vec<*mut c_char>) -> Result<Option<QueryResult>, Error> {
    let argc = argv.len() as i32;
    let argv = argv.as_mut_ptr();
    let result_ptr = unsafe { bindings::query_stable_v2(argc, argv) };

    if result_ptr.is_null() {
        return Ok(None);
    }

    Ok(Some(QueryResult(result_ptr).check_error()?))
}

fn arg_clickhouse() -> Result<CString, Error> {
    Ok(CString::new("clickhouse")?)
}

fn arg_data_path(value: &str) -> Result<CString, Error> {
    Ok(CString::new(format!("--path={}", value))?)
}

fn arg_query(value: &str) -> Result<CString, Error> {
    Ok(CString::new(format!("--query={}", value))?)
}
