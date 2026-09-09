//! The engine seam (contract §3), and the two entry gates built on it.
//!
//! The durable control plane never touches a native library directly.
//! Everything it needs from the engine goes through [`Engine`], which is why
//! the same state machine can drive a real chDB connection or a fake in a unit
//! test without either knowing about the other.
//!
//! Two rules shape the trait, and both are contract requirements rather than
//! taste:
//!
//! 1. **The binding never builds management SQL.** [`Engine::backup_database`]
//!    and [`Engine::restore_database`] take an identifier and a path; quoting
//!    and AST construction happen in core. A binding that concatenated
//!    `"BACKUP DATABASE " + name` would be one unusual database name away from
//!    injection, in four languages independently.
//! 2. **The binding never classifies SQL itself.** No prefix lists, no regular
//!    expressions. [`Engine::analyze`] is ClickHouse's own parser answering
//!    questions only it can answer — how many executable statements are in this
//!    text, and does every persistent write land in the database we own. A
//!    regex cannot see through `INSERT ... FORMAT` inline data, and it cannot
//!    resolve an unqualified table name against the session's current database.
//!
//! [`Engine::backup_database`] deliberately has no incremental-base parameter
//! even though the C ABI accepts one. V1 checkpoints are always full: an
//! incremental archive records the *path* of its base, and that path does not
//! exist on the machine that restores it (§3.2).

use std::path::Path;

use crate::admin::{QueryAnalysis, QueryClass};
use crate::format::OutputFormat;

use super::errors::{err, Category, Result};

/// Where an engine puts its state for one durable object.
#[derive(Debug, Clone)]
pub struct EngineStartOptions {
    /// A fresh, empty, private data directory for this object.
    pub data_path: std::path::PathBuf,
    /// An absolute, already-created directory the engine may read and write
    /// archives in.
    pub backups_allowed_path: std::path::PathBuf,
}

/// What a durable object needs from chDB.
///
/// Implementations own their native resources entirely; the control plane only
/// calls these methods and [`Engine::close`].
pub trait Engine: Send {
    /// The exact `chdb_version()` of the loaded engine.
    ///
    /// It is recorded in the head as the producer version, and it is not an
    /// exact-match gate: compatibility is checked with `backup_format` and
    /// `min_reader`, so later chdb-core releases can restore earlier V1 full
    /// backups. It must be answerable before [`Engine::start`] — an
    /// incompatible engine is refused before the object takes a lease or
    /// creates a scratch directory.
    fn version(&mut self) -> Result<String>;

    /// The highest archive-format generation this engine can restore.
    ///
    /// Every release so far reports the V1 baseline, because the C ABI has no
    /// accessor for it; once core exposes one, this starts refusing archives
    /// from a future generation instead of assuming.
    fn backup_format(&mut self) -> Result<u64>;

    /// Brings up a connection on a fresh scratch path. Called once, before
    /// anything else.
    fn start(&mut self, options: EngineStartOptions) -> Result<()>;

    /// Creates the object's database, with core doing the quoting.
    fn create_database(&mut self, database: &str) -> Result<()>;

    /// Pins the connection's current database. Called once after restore; the
    /// public surface can never change it, because `USE` classifies as
    /// [`QueryClass::Control`].
    fn use_database(&mut self, database: &str) -> Result<()>;

    /// Says what a statement would do, judged against `target_database`,
    /// without executing it.
    fn analyze(&mut self, sql: &str, target_database: &str) -> Result<QueryAnalysis>;

    /// Runs a read query and returns its formatted bytes.
    ///
    /// Bytes rather than a string, because a caller is free to ask for Parquet
    /// or Arrow, and a lossy conversion of those is not a result.
    fn query(&mut self, sql: &str, format: OutputFormat) -> Result<Vec<u8>>;

    /// Runs a statement for effect.
    ///
    /// This is the *internal* path: it performs no analysis and appends nothing
    /// to a WAL. Replay uses it, which is exactly why it must not be reachable
    /// from the public surface — a replayed statement that re-entered `execute`
    /// would be logged a second time.
    fn run(&mut self, sql: &str) -> Result<()>;

    /// Writes a full archive to a new absolute path that must not already
    /// exist.
    fn backup_database(&mut self, database: &str, file_path: &Path) -> Result<()>;

    /// Restores an archive into a database that does not already hold its
    /// tables.
    fn restore_database(&mut self, database: &str, file_path: &Path) -> Result<()>;

    /// Releases the native connection. Must be safe to call after a failed
    /// [`Engine::start`].
    fn close(&mut self) -> Result<()>;
}

/// Builds the engine for one durable object.
///
/// A namespace holds one and calls it per open, so a caller can substitute a
/// fake for tests or a differently configured connection for a workload.
pub type EngineFactory = Box<dyn Fn() -> Result<Box<dyn Engine>> + Send + Sync>;

/// Applies the frozen query gate (§3.4).
///
/// Note what is *not* here: a secret check. A read-only statement never reaches
/// the WAL, so a credential inside it is not a durability problem — only a
/// logging one, which this crate handles by never echoing SQL.
pub(crate) fn assert_query_allowed(analysis: &QueryAnalysis) -> Result<()> {
    if analysis.class == QueryClass::Unknown {
        // First, because text that did not parse also counts zero statements,
        // and "could not classify" is the useful half of that answer.
        return Err(err(
            Category::ClassificationRefused,
            "durable: core could not classify this statement; refusing rather than running \
             something whose effect is unknown",
        ));
    }
    if analysis.statement_count != 1 {
        return Err(err(
            Category::ClassificationRefused,
            format!(
                "durable: query takes exactly one statement, core counted {}",
                analysis.statement_count
            ),
        ));
    }
    if analysis.class != QueryClass::ReadOnly {
        return Err(err(
            Category::ClassificationRefused,
            format!(
                "durable: query accepts only READ_ONLY statements, core classified this as {}",
                analysis.class
            ),
        ));
    }
    Ok(())
}

/// Applies the frozen execute gate (§3.4).
///
/// The checks are ordered so the message names the most actionable fact first.
/// The secret check is last and has its own category because it is the one
/// failure a caller fixes by rewriting the statement rather than by using a
/// different method — and because its message must describe the refusal without
/// quoting the statement that triggered it.
pub(crate) fn assert_execute_allowed(analysis: &QueryAnalysis, database: &str) -> Result<()> {
    if analysis.class == QueryClass::Unknown {
        return Err(err(
            Category::ClassificationRefused,
            "durable: core could not classify this statement; refusing rather than logging \
             something whose effect is unknown",
        ));
    }
    if analysis.statement_count != 1 {
        return Err(err(
            Category::ClassificationRefused,
            format!(
                "durable: execute takes exactly one statement, core counted {}. A WAL record is \
                 one statement, so a batch has no replayable form in V1",
                analysis.statement_count
            ),
        ));
    }
    if analysis.class != QueryClass::Mutating {
        let mut message = format!(
            "durable: execute accepts only MUTATING statements, core classified this as {}",
            analysis.class
        );
        match analysis.class {
            QueryClass::ReadOnly => {
                message.push_str(". Use query(), which does not write the WAL");
            }
            QueryClass::MutatingGlobal => {
                message.push_str(
                    ". Global state lives outside every database, so a checkpoint cannot carry \
                     it and V1 refuses it",
                );
            }
            QueryClass::Control => {
                message.push_str(
                    ". USE/SET/SYSTEM/BACKUP/RESTORE, and writes outside the engine, are managed \
                     by the durable object and cannot be issued through it",
                );
            }
            _ => {}
        }
        return Err(err(Category::ClassificationRefused, message));
    }
    if analysis.changes_database_lifecycle {
        return Err(err(
            Category::ClassificationRefused,
            format!(
                "durable: execute cannot create, drop or rename a database; the object owns \
                 {database:?} and its lifecycle is not a logged mutation"
            ),
        ));
    }
    if !analysis.writes_only_target_database {
        return Err(err(
            Category::ClassificationRefused,
            format!(
                "durable: core could not prove every write lands in {database:?}. A write to \
                 another database, to system, to a table function or to a file is not captured \
                 by this object's checkpoint"
            ),
        ));
    }
    if analysis.has_secrets {
        return Err(err(
            Category::SecretRefused,
            "durable: refusing to log a mutation that embeds a credential; the WAL outlives the \
             statement, so the credential would outlive it too",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    fn analysis(class: QueryClass) -> QueryAnalysis {
        QueryAnalysis {
            class,
            statement_count: 1,
            has_secrets: false,
            writes_only_target_database: true,
            changes_database_lifecycle: false,
        }
    }

    #[test]
    fn the_query_gate_takes_one_read_only_statement_and_nothing_else() {
        assert!(assert_query_allowed(&analysis(QueryClass::ReadOnly)).is_ok());

        for class in [
            QueryClass::Mutating,
            QueryClass::MutatingGlobal,
            QueryClass::Control,
            QueryClass::Unknown,
        ] {
            let error = assert_query_allowed(&analysis(class)).expect_err("not a read");
            assert_eq!(error.category(), Category::ClassificationRefused, "{class}");
        }

        let mut batch = analysis(QueryClass::ReadOnly);
        batch.statement_count = 2;
        assert_eq!(
            assert_query_allowed(&batch).unwrap_err().category(),
            Category::ClassificationRefused
        );
    }

    #[test]
    fn the_execute_gate_takes_one_local_mutation_and_nothing_else() {
        assert!(assert_execute_allowed(&analysis(QueryClass::Mutating), "mem").is_ok());

        for class in [
            QueryClass::ReadOnly,
            QueryClass::MutatingGlobal,
            QueryClass::Control,
            QueryClass::Unknown,
        ] {
            let error = assert_execute_allowed(&analysis(class), "mem").expect_err("not a write");
            assert_eq!(error.category(), Category::ClassificationRefused, "{class}");
        }
    }

    #[test]
    fn a_write_the_checkpoint_would_not_carry_is_refused() {
        let mut elsewhere = analysis(QueryClass::Mutating);
        elsewhere.writes_only_target_database = false;
        assert_eq!(
            assert_execute_allowed(&elsewhere, "mem")
                .unwrap_err()
                .category(),
            Category::ClassificationRefused
        );

        let mut lifecycle = analysis(QueryClass::Mutating);
        lifecycle.changes_database_lifecycle = true;
        assert_eq!(
            assert_execute_allowed(&lifecycle, "mem")
                .unwrap_err()
                .category(),
            Category::ClassificationRefused
        );
    }

    #[test]
    fn a_credential_bearing_mutation_is_refused_without_echoing_it() {
        let mut secret = analysis(QueryClass::Mutating);
        secret.has_secrets = true;
        let error = assert_execute_allowed(&secret, "mem").expect_err("a credential in the WAL");
        assert_eq!(error.category(), Category::SecretRefused);
        assert!(
            !error.to_string().to_lowercase().contains("password"),
            "the refusal must not quote the statement: {error}"
        );
    }

    #[test]
    fn a_batch_is_refused_before_its_class_is_even_considered() {
        let mut batch = analysis(QueryClass::Mutating);
        batch.statement_count = 2;
        let error = assert_execute_allowed(&batch, "mem").expect_err("a batch has no WAL record");
        assert!(error.to_string().contains("one statement"), "{error}");
    }
}
