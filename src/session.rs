//! Session management for persistent chDB databases.
//!
//! This module provides the [`Session`] and [`SessionBuilder`] types for managing
//! persistent database connections with automatic cleanup.

use std::path::PathBuf;
use std::{fs, io};

use crate::arg::{extract_output_format, Arg};
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::format::OutputFormat;
use crate::query_result::QueryResult;

/// Builder for creating [`Session`] instances.
///
/// `SessionBuilder` provides a fluent API for configuring and creating sessions.
/// Use [`new`](Self::new) to create a new builder, configure it with the desired
/// options, and call [`build`](Self::build) to create the session.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::session::SessionBuilder;
///
/// // Create a session with default settings
/// let session = SessionBuilder::new()
///     .with_data_path("/tmp/mydb")
///     .with_auto_cleanup(true)
///     .build()?;
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
pub struct SessionBuilder<'a> {
    data_path: PathBuf,
    default_format: OutputFormat,
    arguments: Vec<Arg<'a>>,
    auto_cleanup: bool,
}

/// A session representing a persistent connection to a chDB database.
///
/// A `Session` manages a connection to a persistent database stored on disk.
/// Unlike stateless queries, sessions allow you to create tables, insert data,
/// and maintain state across multiple queries.
///
/// # Thread Safety
///
/// `Session` contains a [`Connection`] which implements
/// `Send`, so sessions can be safely transferred between threads. However, concurrent
/// access to the same session should be synchronized.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::session::SessionBuilder;
/// use chdb_rust::arg::Arg;
/// use chdb_rust::format::OutputFormat;
///
/// let session = SessionBuilder::new()
///     .with_data_path("/tmp/mydb")
///     .with_auto_cleanup(true)
///     .build()?;
///
/// // Create a table
/// session.execute(
///     "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id",
///     None
/// )?;
///
/// // Insert data
/// session.execute("INSERT INTO users VALUES (1, 'Alice')", None)?;
///
/// // Query data
/// let result = session.execute(
///     "SELECT * FROM users",
///     Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
/// )?;
/// println!("{}", result.data_utf8_lossy());
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
#[derive(Debug)]
pub struct Session {
    conn: Connection,
    data_path: PathBuf,
    default_format: OutputFormat,
    auto_cleanup: bool,
}

impl<'a> SessionBuilder<'a> {
    /// Create a new `SessionBuilder` with default settings.
    ///
    /// The default settings are:
    /// - Data path: current working directory. The `build` method will create a `chdb`
    ///   subdirectory if the path is not set.
    /// - Output format: `TabSeparated`
    /// - Auto cleanup: `false`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::session::SessionBuilder;
    ///
    /// let builder = SessionBuilder::new();
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    pub fn new() -> Self {
        Self {
            data_path: PathBuf::new(),
            default_format: OutputFormat::TabSeparated,
            arguments: Vec::new(),
            auto_cleanup: false,
        }
    }

    /// Set the data path for the session.
    ///
    /// This specifies the filesystem path where the database will be stored.
    /// The directory will be created if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `path` - The path where the database should be stored
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::session::SessionBuilder;
    ///
    /// let builder = SessionBuilder::new()
    ///     .with_data_path("/tmp/mydb");
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    pub fn with_data_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_path = path.into();
        self
    }

    /// Add a query argument to the session builder.
    ///
    /// If `OutputFormat` argument is provided it will be used
    /// as the default output format for queries executed on this session.
    ///
    /// Moreover, `OutputFormat` is consumed by the session layer and is not
    /// forwarded as a command-line argument to the connection.
    ///
    /// # Arguments
    ///
    /// * `arg` - The argument to add
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::session::SessionBuilder;
    /// use chdb_rust::arg::Arg;
    /// use chdb_rust::format::OutputFormat;
    /// use std::borrow::Cow;
    ///
    /// let builder = SessionBuilder::new()
    ///     .with_arg(Arg::OutputFormat(OutputFormat::JSONEachRow))
    ///     .with_arg(Arg::ConfigFilePath(Cow::from("chdb_ssl.xml")))
    ///     .with_arg(Arg::Custom(Cow::from("progress"), Some(Cow::from("err"))));
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    /// Custom arguments can be used to specify any additional command-line arguments
    /// that can be found by running `clickhouse-client --help` in
    /// the terminal.
    pub fn with_arg(mut self, arg: Arg<'a>) -> Self {
        if let Some(fmt) = arg.as_output_format() {
            self.default_format = fmt;
        } else {
            self.arguments.push(arg);
        }
        self
    }

    /// Enable or disable automatic cleanup of the data directory.
    ///
    /// If set to `true`, the session will automatically delete the data directory
    /// when it is dropped. This is useful for temporary databases.
    ///
    /// # Arguments
    ///
    /// * `value` - Whether to enable automatic cleanup
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::session::SessionBuilder;
    ///
    /// // Session will clean up data directory on drop
    /// let session = SessionBuilder::new()
    ///     .with_data_path("/tmp/tempdb")
    ///     .with_auto_cleanup(true)
    ///     .build()?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    pub fn with_auto_cleanup(mut self, value: bool) -> Self {
        self.auto_cleanup = value;
        self
    }

    /// Build the session with the configured settings.
    ///
    /// This creates the data directory if it doesn't exist and establishes
    /// a connection to the database. All configured arguments are passed to
    /// the connection.
    ///
    /// # Returns
    ///
    /// Returns a [`Session`] if successful, or an [`Error`] if
    /// the session cannot be created.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The data path cannot be created
    /// - The data path has insufficient permissions
    /// - The connection cannot be established
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::session::SessionBuilder;
    ///
    /// let session = SessionBuilder::new()
    ///     .with_data_path("/tmp/mydb")
    ///     .build()?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    pub fn build(mut self) -> Result<Session> {
        if self.data_path.as_os_str().is_empty() {
            let mut default_path = std::env::current_dir()?;
            default_path.push("chdb");
            self.data_path = default_path;
        }

        let dir_already_existed = self.data_path.exists();
        fs::create_dir_all(&self.data_path)?;
        let probe_path = self.data_path.join(".write_probe");
        if let Err(e) = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&probe_path)
        {
            if !dir_already_existed {
                fs::remove_dir(&self.data_path).ok();
            }
            return Err(match e.kind() {
                io::ErrorKind::PermissionDenied => Error::InsufficientPermissions,
                _ => Error::Io(e),
            });
        }
        fs::remove_file(&probe_path).ok(); // best-effort cleanup of write probe

        let data_path = self.data_path.to_str().ok_or(Error::PathError)?;
        let path_arg = format!("--path={data_path}");
        let owned: Vec<String> = self.arguments.iter().map(|a| a.to_string()).collect();
        let mut args: Vec<&str> = Vec::with_capacity(1 + owned.len());
        args.push(&path_arg);
        args.extend(owned.iter().map(String::as_str));

        let conn = Connection::open(&args)?;

        Ok(Session {
            conn,
            data_path: self.data_path,
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
    /// Execute a query on this session.
    ///
    /// This executes a SQL query against the database associated with this session.
    /// The query can create tables, insert data, or query existing data.
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query string to execute
    /// * `query_args` - Optional array of query arguments (e.g., output format).
    ///
    /// Only `OutputFormat` is currently supported and will override the
    /// session's default output format for this query.
    ///
    /// # Returns
    ///
    /// Returns a [`QueryResult`] containing the query output,
    /// or an [`Error`] if the query fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::session::SessionBuilder;
    /// use chdb_rust::arg::Arg;
    /// use chdb_rust::format::OutputFormat;
    ///
    /// let session = SessionBuilder::new()
    ///     .with_data_path("/tmp/mydb")
    ///     .with_auto_cleanup(true)
    ///     .build()?;
    ///
    /// // Create a table
    /// session.execute(
    ///     "CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY id",
    ///     None
    /// )?;
    ///
    /// // Query with JSON output
    /// let result = session.execute(
    ///     "SELECT * FROM test",
    ///     Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
    /// )?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The query syntax is invalid
    /// - The query references non-existent tables or columns
    /// - The query execution fails for any other reason
    pub fn execute(&self, query: &str, query_args: Option<&[Arg]>) -> Result<QueryResult> {
        let fmt = extract_output_format(query_args, self.default_format);
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

#[cfg(test)]
mod test {
    use crate::test_utils::tempdir;

    use super::*;

    #[test]
    fn test_session_builder_no_args_still_builds() -> Result<()> {
        let tmp = tempdir();
        SessionBuilder::new().with_data_path(tmp.path()).build()?;
        Ok(())
    }

    #[test]
    fn test_query_uses_default_output_format() -> Result<()> {
        let tmp = tempdir();
        let session = SessionBuilder::new().with_data_path(tmp.path()).build()?;

        let resp = session.execute("SELECT 'foo' AS name, 1 AS count", None)?;

        assert_eq!(resp.data_utf8_lossy(), "foo\t1\n");

        Ok(())
    }

    #[test]
    fn test_query_output_format_overrides_session_builder_output_format() -> Result<()> {
        let tmp = tempdir();
        let session = SessionBuilder::new()
            .with_data_path(tmp.path())
            .with_arg(Arg::OutputFormat(OutputFormat::Parquet))
            .build()?;

        let resp = session.execute(
            "SELECT 1 AS count",
            Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
        )?;

        assert_eq!(resp.data_utf8_lossy(), "{\"count\":1}\n");

        Ok(())
    }
}
