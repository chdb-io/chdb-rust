//! Session management for persistent chDB databases.
//!
//! This module provides the [`Session`] and [`SessionBuilder`] types for managing
//! persistent database connections with automatic cleanup.

use std::fs;
use std::path::PathBuf;

use crate::arg::Arg;
use crate::connection::Connection;
use crate::error::Error;
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
    _marker: std::marker::PhantomData<&'a ()>,
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
    data_path: String,
    default_format: OutputFormat,
    auto_cleanup: bool,
}

impl<'a> SessionBuilder<'a> {
    /// Create a new `SessionBuilder` with default settings.
    ///
    /// The default settings are:
    /// - Data path: `./chdb` in the current working directory
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
        let mut data_path = std::env::current_dir().unwrap();
        data_path.push("chdb");

        Self {
            data_path,
            default_format: OutputFormat::TabSeparated,
            _marker: std::marker::PhantomData,
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
    /// Currently, only `OutputFormat` arguments are supported and will be used
    /// as the default output format for queries executed on this session.
    ///
    /// # Arguments
    ///
    /// * `arg` - The argument to add (currently only `OutputFormat` is supported)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chdb_rust::session::SessionBuilder;
    /// use chdb_rust::arg::Arg;
    /// use chdb_rust::format::OutputFormat;
    ///
    /// let builder = SessionBuilder::new()
    ///     .with_arg(Arg::OutputFormat(OutputFormat::JSONEachRow));
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    pub fn with_arg(mut self, arg: Arg<'a>) -> Self {
        // Only OutputFormat is supported with the new API
        if let Some(fmt) = arg.as_output_format() {
            self.default_format = fmt;
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
    /// a connection to the database.
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
    /// Execute a query on this session.
    ///
    /// This executes a SQL query against the database associated with this session.
    /// The query can create tables, insert data, or query existing data.
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query string to execute
    /// * `query_args` - Optional array of query arguments (e.g., output format)
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
