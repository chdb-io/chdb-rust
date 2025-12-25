use std::fs;
use std::path::PathBuf;

use crate::arg::Arg;
use crate::connection::Connection;
use crate::error::Error;
use crate::format::OutputFormat;
use crate::query_result::QueryResult;

pub struct SessionBuilder<'a> {
    data_path: PathBuf,
    default_format: OutputFormat,
    _marker: std::marker::PhantomData<&'a ()>,
    auto_cleanup: bool,
}

#[derive(Debug)]
pub struct Session {
    conn: Connection,
    data_path: String,
    default_format: OutputFormat,
    auto_cleanup: bool,
}

impl<'a> SessionBuilder<'a> {
    pub fn new() -> Self {
        let mut data_path = std::env::current_dir().unwrap();
        data_path.push("chdb");

        Self {
            data_path,
            default_format: OutputFormat::TabSeparated,
            _marker: std::marker::PhantomData,
            auto_cleanup: false,
        }
    }

    pub fn with_data_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_path = path.into();
        self
    }

    pub fn with_arg(mut self, arg: Arg<'a>) -> Self {
        // Only OutputFormat is supported with the new API
        if let Some(fmt) = arg.as_output_format() {
            self.default_format = fmt;
        }
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

        let conn = Connection::open_with_path(&data_path)?;

        Ok(Session {
            conn,
            data_path,
            default_format: self.default_format,
            auto_cleanup: self.auto_cleanup,
        })
    }
}

impl Default for SessionBuilder<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl Session {
    pub fn execute(&self, query: &str, query_args: Option<&[Arg]>) -> Result<QueryResult, Error> {
        let fmt = query_args
            .and_then(|args| args.iter().find_map(|a| a.as_output_format()))
            .unwrap_or(self.default_format);
        self.conn.query(query, fmt)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if self.auto_cleanup {
            fs::remove_dir_all(&self.data_path).ok();
        }
    }
}
