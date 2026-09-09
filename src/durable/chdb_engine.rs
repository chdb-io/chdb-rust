//! The real engine: a chDB connection on the object's scratch directory.
//!
//! Going through the crate's own [`Connection`] rather than the FFI directly is
//! deliberate. The engine registry already models the constraint the contract
//! states in §3.6 — chDB binds one data path per process — so a second durable
//! object in the same process is refused by the registry with an error naming
//! both paths, instead of failing somewhere deeper and less legibly. Nothing
//! here maintains a second path state machine.

use std::path::Path;

use crate::admin::QueryAnalysis;
use crate::connection::Connection;
use crate::format::OutputFormat;

use super::engine::{Engine, EngineStartOptions};
use super::errors::{engine_err, err, Category, Result};
use super::types::BACKUP_FORMAT_BASELINE;

/// The settings a durable writer cannot leave to chance.
///
/// Asynchronous inserts and non-synchronous mutations both mean "the statement
/// returned before its effect landed", which would put a statement in the WAL
/// whose local effect is not yet in the database the next checkpoint archives.
/// The public entry gate refuses statement settings that would relax these,
/// because core classifies them as `CONTROL`.
const SYNCHRONOUS_SETTINGS: [&str; 4] = [
    "--async_insert=0",
    "--wait_for_async_insert=1",
    "--mutations_sync=2",
    "--alter_sync=2",
];

/// Drives one durable object through a chDB connection.
#[derive(Debug, Default)]
pub struct ChdbEngine {
    /// Extra connection arguments, for a caller that needs to tune the engine
    /// for its workload. They cannot weaken the synchronous-write settings
    /// above: those are applied after, and the public surface cannot change
    /// them either, since `SET` classifies as `CONTROL`.
    extra_args: Vec<String>,
    connection: Option<Connection>,
}

impl ChdbEngine {
    /// An engine with no extra connection arguments.
    pub fn new() -> Self {
        Self::default()
    }

    /// An engine that passes `args` (in `--name=value` form) to every
    /// connection it opens.
    pub fn with_args(args: impl IntoIterator<Item = String>) -> Self {
        Self {
            extra_args: args.into_iter().collect(),
            connection: None,
        }
    }

    fn borrow(&self) -> Result<&Connection> {
        self.connection
            .as_ref()
            .ok_or_else(|| err(Category::Engine, "durable: the engine is not started"))
    }
}

/// Backtick-quotes a ClickHouse identifier, so a database name holding a dash —
/// or anything else needing quoting — is valid in DDL and cannot break out of
/// the statement.
///
/// This is only for the two statements core has no argument-taking entry point
/// for, `CREATE DATABASE` and `USE`. Backup and restore take the name as an
/// argument and quote it themselves, which is why neither appears here.
fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

impl Engine for ChdbEngine {
    fn version(&mut self) -> Result<String> {
        crate::version::engine_version()
            .map(str::to_string)
            .map_err(|e| engine_err(e, "durable: cannot read the engine version"))
    }

    fn backup_format(&mut self) -> Result<u64> {
        // The C ABI exposes no accessor for the archive-format generation, so
        // every release reports the V1 baseline. When core adds one, this is
        // the single place that changes, and the reader gate starts refusing a
        // future generation instead of assuming it can restore it.
        Ok(BACKUP_FORMAT_BASELINE)
    }

    fn start(&mut self, options: EngineStartOptions) -> Result<()> {
        if self.connection.is_some() {
            return Err(err(
                Category::Engine,
                "durable: the engine is already started",
            ));
        }
        let data = options
            .data_path
            .to_str()
            .ok_or_else(|| err(Category::Engine, "durable: the scratch path is not UTF-8"))?;
        let backups = options
            .backups_allowed_path
            .to_str()
            .ok_or_else(|| err(Category::Engine, "durable: the archive path is not UTF-8"))?;

        // `backups.allowed_path` is what makes backup and restore possible at
        // all: core refuses an archive path outside it, and a connection that
        // never set it cannot back anything up. Scoping it to this object's own
        // scratch also means a restore cannot read an archive some other object
        // left on disk.
        let owned: Vec<String> = [
            format!("--path={data}"),
            format!("--backups.allowed_path={backups}"),
        ]
        .into_iter()
        .chain(SYNCHRONOUS_SETTINGS.iter().map(|s| s.to_string()))
        .chain(self.extra_args.iter().cloned())
        .collect();
        let args: Vec<&str> = owned.iter().map(String::as_str).collect();

        self.connection = Some(Connection::open(&args).map_err(|e| {
            // chDB binds one data path per process, so a second durable object
            // in the same process arrives here. The registry's message names
            // both paths, which is more use than anything this layer could add.
            engine_err(e, format!("durable: cannot open an engine on {data}"))
        })?);
        Ok(())
    }

    fn create_database(&mut self, database: &str) -> Result<()> {
        let sql = format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            quote_identifier(database)
        );
        self.run(&sql)
    }

    fn use_database(&mut self, database: &str) -> Result<()> {
        let sql = format!("USE {}", quote_identifier(database));
        self.run(&sql)
    }

    fn analyze(&mut self, sql: &str, target_database: &str) -> Result<QueryAnalysis> {
        self.borrow()?
            .classify_query(sql, Some(target_database))
            // The message deliberately does not include the SQL: analysis runs
            // on statements that may embed a credential, and that is exactly
            // the case the caller is about to be told about.
            .map_err(|e| engine_err(e, "durable: the engine could not analyse the statement"))
    }

    fn query(&mut self, sql: &str, format: OutputFormat) -> Result<Vec<u8>> {
        let result = self
            .borrow()?
            .query(sql, format)
            .map_err(|e| engine_err(e, "durable: the engine refused a read"))?;
        Ok(result.data_ref().to_vec())
    }

    fn run(&mut self, sql: &str) -> Result<()> {
        self.borrow()?
            .query(sql, OutputFormat::CSV)
            .map(|_| ())
            .map_err(|e| engine_err(e, "durable: the engine refused a statement"))
    }

    fn backup_database(&mut self, database: &str, file_path: &Path) -> Result<()> {
        self.borrow()?
            .backup_database(database, file_path)
            .map_err(|e| engine_err(e, format!("durable: backing up {database:?} failed")))
    }

    fn restore_database(&mut self, database: &str, file_path: &Path) -> Result<()> {
        self.borrow()?
            .restore_database(database, file_path)
            .map_err(|e| engine_err(e, format!("durable: restoring {database:?} failed")))
    }

    fn close(&mut self) -> Result<()> {
        // Tolerates never having started: an open that fails partway through
        // still has to release whatever was acquired.
        drop(self.connection.take());
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn an_identifier_is_quoted_rather_than_interpolated() {
        assert_eq!(quote_identifier("mem"), "`mem`");
        assert_eq!(quote_identifier("my-db"), "`my-db`");
        // The two characters that could end the quoting are escaped, so a name
        // cannot carry the statement anywhere else.
        assert_eq!(quote_identifier("we`ird"), "`we\\`ird`");
        assert_eq!(quote_identifier("back\\slash"), "`back\\\\slash`");
    }

    #[test]
    fn an_engine_that_never_started_says_so_rather_than_panicking() {
        let mut engine = ChdbEngine::new();
        let error = engine.run("SELECT 1").expect_err("no connection yet");
        assert_eq!(error.category(), Category::Engine);
        assert!(
            engine.close().is_ok(),
            "closing an unstarted engine is fine"
        );
    }
}
