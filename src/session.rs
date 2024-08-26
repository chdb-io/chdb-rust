use std::ffi::CString;
use std::fs;
use std::path::PathBuf;

use crate::arg::Arg;
use crate::arg_clickhouse;
use crate::arg_data_path;
use crate::arg_query;
use crate::call_chdb;
use crate::error::Error;
use crate::query_result::QueryResult;

pub struct SessionBuilder<'a> {
    data_path: PathBuf,
    default_args: Vec<Arg<'a>>,
    auto_cleanup: bool,
}

#[derive(Clone)]
pub struct Session {
    default_args: Vec<CString>,
    data_path: String,
    auto_cleanup: bool,
}

impl<'a> SessionBuilder<'a> {
    pub fn new() -> Self {
        let mut data_path = std::env::current_dir().unwrap();
        data_path.push("chdb");

        Self {
            data_path,
            default_args: Vec::new(),
            auto_cleanup: false,
        }
    }

    pub fn with_data_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_path = path.into();
        self
    }

    pub fn with_arg(mut self, arg: Arg<'a>) -> Self {
        self.default_args.push(arg);
        self
    }

    /// If set Session will delete data directory before it is dropped.
    pub fn with_auto_cleanup(mut self, value: bool) -> Self {
        self.auto_cleanup = value;
        self
    }

    pub fn build(self) -> Result<Session, Error> {
        let data_path = self.data_path.to_str().ok_or(Error::PathError)?.to_string();

        fs::create_dir_all(&self.data_path)?;
        if fs::metadata(&self.data_path)?.permissions().readonly() {
            return Err(Error::InsufficientPermissions);
        }

        let mut default_args = Vec::with_capacity(self.default_args.len() + 2);
        default_args.push(arg_clickhouse()?);
        default_args.push(arg_data_path(&data_path)?);

        for default_arg in self.default_args {
            default_args.push(default_arg.to_cstring()?);
        }

        Ok(Session {
            data_path,
            default_args,
            auto_cleanup: self.auto_cleanup,
        })
    }
}

impl<'a> Default for SessionBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl Session {
    pub fn execute(
        &self,
        query: &str,
        query_args: Option<&[Arg]>,
    ) -> Result<Option<QueryResult>, Error> {
        let mut argv = Vec::with_capacity(
            self.default_args.len() + query_args.as_ref().map_or(0, |v| v.len()) + 1,
        );

        for arg in &self.default_args {
            argv.push(arg.clone().into_raw())
        }

        if let Some(args) = query_args {
            for arg in args {
                argv.push(arg.to_cstring()?.into_raw());
            }
        }

        argv.push(arg_query(query)?.into_raw());
        call_chdb(argv)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if self.auto_cleanup {
            fs::remove_dir_all(&self.data_path).ok();
        }
    }
}
