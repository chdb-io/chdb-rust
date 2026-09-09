//! Backup, restore and statement analysis: chdb-core's management ABI.
//!
//! These are the three entry points the [chDB Durable V1 contract][contract]
//! requires of an engine, and they exist for one reason: some questions can
//! only be answered by the ClickHouse parser, and some statements can only be
//! built safely by the engine that will run them.
//!
//! * [`Connection::backup_database`] and [`Connection::restore_database`] take
//!   a database name and a path as *arguments*. The engine builds the AST and
//!   quotes both, so a database called `` my`db `` or a path holding an
//!   apostrophe cannot change what runs. Nothing in this crate assembles
//!   `BACKUP` or `RESTORE` text.
//! * [`Connection::classify_query`] reports what a statement would do without
//!   running it: how many executable statements the text holds, what class
//!   they are, whether every persistent write lands in one named database, and
//!   whether the text embeds a credential. A prefix list cannot answer any of
//!   those — it cannot see through `INSERT ... FORMAT` inline data, and it
//!   cannot resolve an unqualified table name against the current database.
//!
//! The whole module is compiled only when the linked library declares all
//! three, which is chdb-core v26.7.2-rc.2 or newer. See [`crate::durable`] for
//! the protocol built on top of them.
//!
//! [contract]: https://github.com/chdb-io/chdb/blob/main/dev-docs/CHDB_DURABLE_V1_CONTRACT.md

use std::path::Path;

use crate::bindings;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::query_result::QueryResult;

/// What a statement does to state that outlives it.
///
/// The classes answer two questions at once: does this change anything after
/// the statement returns, and would `BACKUP DATABASE` carry the change?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryClass {
    /// `SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`, `EXISTS`, `CHECK`: leaves no trace.
    ReadOnly,
    /// `INSERT`, `CREATE`, `ALTER`, `DROP`, `TRUNCATE`, `RENAME`, `UPDATE`,
    /// `DELETE`, `OPTIMIZE`: changes a database, and `BACKUP DATABASE` captures
    /// the change.
    Mutating,
    /// Global UDFs, named collections, workloads, resources, access management
    /// and writes into `system`: persistent, replayable, and outside every
    /// database a checkpoint could capture.
    MutatingGlobal,
    /// `USE`, `SET`, `ATTACH`, `DETACH`, `SYSTEM`, `BACKUP`, `RESTORE`, `KILL`,
    /// transaction control, and statements writing outside the engine
    /// (`INTO OUTFILE`, `INSERT INTO FUNCTION`).
    Control,
    /// Did not parse, or parsed into a statement this engine does not classify.
    /// A caller gating writes on the class has to treat it as a refusal.
    Unknown,
}

impl QueryClass {
    fn from_abi(value: u32) -> Self {
        match value {
            bindings::chdb_query_class_CHDB_QUERY_READ_ONLY => Self::ReadOnly,
            bindings::chdb_query_class_CHDB_QUERY_MUTATING => Self::Mutating,
            bindings::chdb_query_class_CHDB_QUERY_MUTATING_GLOBAL => Self::MutatingGlobal,
            bindings::chdb_query_class_CHDB_QUERY_CONTROL => Self::Control,
            // Including any class a later engine adds: an unrecognised answer
            // is exactly as unproven as text that did not parse.
            _ => Self::Unknown,
        }
    }

    /// The contract's wire name for this class, which every binding reports
    /// identically so a cross-binding conformance run can compare them.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "READ_ONLY",
            Self::Mutating => "MUTATING",
            Self::MutatingGlobal => "MUTATING_GLOBAL",
            Self::Control => "CONTROL",
            Self::Unknown => "UNKNOWN",
        }
    }
}

impl std::fmt::Display for QueryClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What [`Connection::classify_query`] reports about a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryAnalysis {
    /// What the statement does to state that outlives it.
    pub class: QueryClass,
    /// How many executable statements the text holds. Zero for empty input or
    /// text that did not parse; a `PARALLEL WITH` arm counts as one, because
    /// both arms execute.
    pub statement_count: u32,
    /// The text carries a credential: a named collection's key, a password, an
    /// access key handed to a table function. Never set for [`QueryClass::Unknown`],
    /// since nothing was proven about text that did not parse.
    pub has_secrets: bool,
    /// Every persistent write lands in the database named in the call. Set only
    /// when the engine can prove it, and never set when no target database was
    /// named. A statement that writes nothing sets it vacuously.
    pub writes_only_target_database: bool,
    /// The statement creates, drops or renames a database rather than acting
    /// inside one — a change to the container rather than its contents.
    pub changes_database_lifecycle: bool,
}

impl Connection {
    /// Write a full backup of `database` into `file_path`.
    ///
    /// The archive is a single file, named by its extension: `.tar.gz` is a
    /// gzipped tar, and a path without a recognised archive extension is
    /// treated as a directory backup instead.
    ///
    /// `file_path` must be absolute, its parent directory must exist, and it
    /// must be inside the connection's `backups.allowed_path` — which is a
    /// connection argument, so a connection that never set it cannot back
    /// anything up:
    ///
    /// ```no_run
    /// use std::path::Path;
    /// use chdb_rust::connection::Connection;
    ///
    /// let conn = Connection::open(&["--path=/tmp/db", "--backups.allowed_path=/tmp/archives"])?;
    /// conn.query("CREATE DATABASE IF NOT EXISTS mem", chdb_rust::format::OutputFormat::CSV)?;
    /// conn.backup_database("mem", Path::new("/tmp/archives/mem-1.tar.gz"))?;
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// An existing destination is never overwritten — the call fails instead,
    /// so give every backup its own name. Also returns an error if the path is
    /// relative, outside the allowed path, or if the backup itself fails.
    pub fn backup_database(&self, database: &str, file_path: &Path) -> Result<()> {
        let path = absolute_str(file_path, "backup")?;
        let result_ptr = unsafe {
            bindings::chdb_backup_database_n(
                self.raw(),
                database.as_ptr().cast(),
                database.len(),
                path.as_ptr().cast(),
                path.len(),
                // V1 checkpoints are always full. An incremental archive
                // records the *local path* of its base, which does not survive
                // the trip through object storage.
                std::ptr::null(),
                0,
            )
        };
        check(result_ptr)
    }

    /// Restore `database` from an archive written by [`Self::backup_database`].
    ///
    /// The path rules match `backup_database`, and the archive has to exist.
    /// The connection's current database is left alone: restoring into `mem`
    /// does not make `mem` current.
    ///
    /// # Errors
    ///
    /// `RESTORE` appends to an existing table rather than replacing it, so
    /// restoring into a database that already holds the archive's tables
    /// duplicates rows rather than failing. Restore into an empty database.
    pub fn restore_database(&self, database: &str, file_path: &Path) -> Result<()> {
        let path = absolute_str(file_path, "restore")?;
        let result_ptr = unsafe {
            bindings::chdb_restore_database_n(
                self.raw(),
                database.as_ptr().cast(),
                database.len(),
                path.as_ptr().cast(),
                path.len(),
            )
        };
        check(result_ptr)
    }

    /// Say what a statement would do, without running it.
    ///
    /// The connection's own parser, dialect and current database are used, so
    /// an unqualified name resolves the way it would if the statement ran.
    /// Nothing is executed: no current database change, no settings change, no
    /// query log entry.
    ///
    /// `target_database` is the database the caller considers its own, and is
    /// what [`QueryAnalysis::writes_only_target_database`] is judged against.
    /// `None` skips that judgement, and the flag is then never set.
    ///
    /// ```no_run
    /// use chdb_rust::admin::QueryClass;
    /// use chdb_rust::connection::Connection;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// let analysis = conn.classify_query("INSERT INTO t VALUES (1)", Some("mem"))?;
    /// assert_eq!(analysis.class, QueryClass::Mutating);
    /// assert_eq!(analysis.statement_count, 1);
    /// # Ok::<(), chdb_rust::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// SQL that does not parse is not an error: it reports
    /// [`QueryClass::Unknown`] with a statement count of zero, because what it
    /// is, is the answer. An error means the engine could not be asked at all.
    pub fn classify_query(
        &self,
        sql: &str,
        target_database: Option<&str>,
    ) -> Result<QueryAnalysis> {
        let mut analysis = bindings::chdb_query_analysis_v1 {
            struct_size: std::mem::size_of::<bindings::chdb_query_analysis_v1>() as u32,
            statement_count: 0,
            flags: 0,
            query_class: bindings::chdb_query_class_CHDB_QUERY_UNKNOWN,
        };
        let (target_ptr, target_len) = match target_database {
            Some(database) => (database.as_ptr().cast(), database.len()),
            None => (std::ptr::null(), 0),
        };

        let state = unsafe {
            bindings::chdb_classify_query_n(
                self.raw(),
                sql.as_ptr().cast(),
                sql.len(),
                target_ptr,
                target_len,
                &mut analysis,
            )
        };
        if state != bindings::chdb_state_CHDBSuccess {
            return Err(Error::QueryError(
                // Deliberately without the statement: analysis is run on text
                // that may embed a credential, and this path cannot yet know
                // whether it does.
                "the engine could not analyse the statement".to_string(),
            ));
        }

        let class = QueryClass::from_abi(analysis.query_class);
        Ok(QueryAnalysis {
            class,
            statement_count: analysis.statement_count,
            has_secrets: has_flag(
                analysis.flags,
                bindings::chdb_query_analysis_flag_CHDB_QUERY_HAS_SECRETS,
            ),
            writes_only_target_database: has_flag(
                analysis.flags,
                bindings::chdb_query_analysis_flag_CHDB_QUERY_WRITES_ONLY_TARGET_DATABASE,
            ),
            changes_database_lifecycle: has_flag(
                analysis.flags,
                bindings::chdb_query_analysis_flag_CHDB_QUERY_CHANGES_DATABASE_LIFECYCLE,
            ),
        })
    }
}

fn has_flag(flags: u32, flag: bindings::chdb_query_analysis_flag) -> bool {
    flags & flag != 0
}

/// The path as UTF-8, refused unless it is absolute.
///
/// The engine checks this too, but its refusal names `backups.allowed_path`
/// rather than the path, because a relative destination is resolved against the
/// data directory before it gets there. Saying it here keeps the message about
/// the argument the caller passed.
fn absolute_str<'a>(path: &'a Path, what: &str) -> Result<&'a str> {
    if !path.is_absolute() {
        return Err(Error::InvalidData(format!(
            "{what} path must be absolute, got {}",
            path.display()
        )));
    }
    path.to_str().ok_or(Error::PathError)
}

/// Turn a management result into `Ok(())` or the engine's own error.
fn check(result_ptr: *mut bindings::chdb_result) -> Result<()> {
    if result_ptr.is_null() {
        return Err(Error::NoResult);
    }
    QueryResult::new(result_ptr).check_error().map(|_| ())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::tempdir;

    /// A connection on its own data directory, with `tmp/archives` as the one
    /// place it may read or write an archive.
    fn connection(tmp: &std::path::Path, data: &str) -> Result<Connection> {
        let data_dir = tmp.join(data);
        let archives = tmp.join("archives");
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(&archives)?;
        let data = format!("--path={}", data_dir.to_str().expect("a UTF-8 path"));
        let allowed = format!(
            "--backups.allowed_path={}",
            archives.to_str().expect("a UTF-8 path")
        );
        Connection::open(&[&data, &allowed])
    }

    #[test]
    fn classification_sees_through_inline_data_and_unqualified_names() -> Result<()> {
        let tmp = tempdir();
        let conn = connection(tmp.path(), "data")?;
        conn.query("CREATE DATABASE mem", crate::format::OutputFormat::CSV)?;
        conn.query("USE mem", crate::format::OutputFormat::CSV)?;

        // A semicolon inside VALUES is data, not a statement boundary.
        let inline = conn.classify_query("INSERT INTO t VALUES ('a;b')", Some("mem"))?;
        assert_eq!(inline.statement_count, 1);
        assert_eq!(inline.class, QueryClass::Mutating);
        assert!(inline.writes_only_target_database);

        // `t` is unqualified, so it resolves through the current database —
        // which is `mem`, not the target named here.
        let elsewhere = conn.classify_query("INSERT INTO t VALUES (1)", Some("other"))?;
        assert!(!elsewhere.writes_only_target_database);

        let two = conn.classify_query("SELECT 1; SELECT 2", Some("mem"))?;
        assert_eq!(two.statement_count, 2);

        let nonsense = conn.classify_query("this is not sql", Some("mem"))?;
        assert_eq!(nonsense.class, QueryClass::Unknown);
        assert_eq!(nonsense.statement_count, 0);
        assert!(!nonsense.has_secrets);

        Ok(())
    }

    #[test]
    fn a_backup_round_trips_through_an_archive() -> Result<()> {
        let tmp = tempdir();
        let archive = tmp.path().join("archives").join("mem.tar.gz");

        {
            let conn = connection(tmp.path(), "origin")?;
            conn.query("CREATE DATABASE mem", crate::format::OutputFormat::CSV)?;
            conn.query(
                "CREATE TABLE mem.t (n UInt64) ENGINE = MergeTree ORDER BY n",
                crate::format::OutputFormat::CSV,
            )?;
            conn.query(
                "INSERT INTO mem.t VALUES (7)",
                crate::format::OutputFormat::CSV,
            )?;

            conn.backup_database("mem", &archive)?;
            assert!(archive.exists());

            // The same name a second time is refused rather than overwritten.
            assert!(conn.backup_database("mem", &archive).is_err());
        }

        // A different data directory, which is what a restore is for. The
        // archive names the database it holds, so it comes back as `mem`, and
        // RESTORE appends rather than replaces — hence a directory with no `mem`
        // in it.
        let conn = connection(tmp.path(), "recovered")?;
        conn.restore_database("mem", &archive)?;
        let rows = conn.query("SELECT n FROM mem.t", crate::format::OutputFormat::CSV)?;
        assert_eq!(rows.data_utf8_lossy().trim(), "7");

        Ok(())
    }

    #[test]
    fn a_relative_archive_path_is_refused_before_the_engine_sees_it() -> Result<()> {
        let tmp = tempdir();
        let conn = connection(tmp.path(), "data")?;
        let err = conn
            .backup_database("mem", Path::new("relative.tar.gz"))
            .expect_err("a relative backup path is refused");
        assert!(err.to_string().contains("must be absolute"), "{err}");
        Ok(())
    }
}
