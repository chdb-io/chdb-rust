//! Durable V1 conformance: the frozen format, the entry gates, and the fault
//! matrix (contract §7).
//!
//! These run against a fake engine and a fault-injecting backend, which is what
//! lets them cover the cases a real engine cannot reach in one process: a second
//! live writer, a lost commit response, a takeover. What the *real* engine
//! decides — statement classification, backup and restore — is covered in
//! `durable_engine.rs`, against ClickHouse's own parser.
//!
//! The fixtures are hand-written `head.json` documents rather than documents
//! this crate produced, for the same reason: an object another binding wrote is
//! the case that matters, and a round-trip through our own writer would not
//! exercise it.

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chdb_rust::admin::{QueryAnalysis, QueryClass};
use chdb_rust::durable::{
    Backend, Category, Digest, DurableObject, Engine, EngineStartOptions, LocalBackend, Namespace,
    OpenOptions, PutOutcome, ReplaceOutcome, Result, Tagged, Tuning, HEAD_KEY,
};
use chdb_rust::format::OutputFormat;

mod common;

// ---------------------------------------------------------------- fake engine

/// A stand-in for chDB: it records the statements applied to it and can archive
/// and restore that record.
///
/// Its classification is a prefix match, which is exactly what the contract
/// forbids a real binding from doing — the point of a fake is to make the state
/// machine testable without an engine, and `durable_engine.rs` is where the
/// gates meet the real parser.
#[derive(Clone, Default)]
struct Applied(Arc<Mutex<Vec<String>>>);

impl Applied {
    fn statements(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

struct FakeEngine {
    version: String,
    backup_format: u64,
    applied: Applied,
    started: bool,
    /// Fails the next restore, for the partial-recovery test.
    fail_restore: bool,
    /// Fails any statement whose text contains this, for the replay-failure test.
    fail_statements_containing: Option<String>,
}

impl FakeEngine {
    fn factory(applied: &Applied) -> chdb_rust::durable::EngineFactory {
        Self::factory_with(applied, "26.7.2", 1, false, None)
    }

    fn factory_with(
        applied: &Applied,
        version: &str,
        backup_format: u64,
        fail_restore: bool,
        fail_statements_containing: Option<&str>,
    ) -> chdb_rust::durable::EngineFactory {
        let applied = applied.clone();
        let version = version.to_string();
        let fail_statements_containing = fail_statements_containing.map(str::to_string);
        Box::new(move || {
            Ok(Box::new(FakeEngine {
                version: version.clone(),
                backup_format,
                applied: applied.clone(),
                started: false,
                fail_restore,
                fail_statements_containing: fail_statements_containing.clone(),
            }))
        })
    }
}

fn engine_error(message: &str) -> chdb_rust::durable::Error {
    chdb_rust::durable::Error::new(Category::Engine, message.to_string())
}

impl Engine for FakeEngine {
    fn version(&mut self) -> Result<String> {
        Ok(self.version.clone())
    }

    fn backup_format(&mut self) -> Result<u64> {
        Ok(self.backup_format)
    }

    fn start(&mut self, options: EngineStartOptions) -> Result<()> {
        assert!(options.data_path.is_dir(), "the scratch data path exists");
        assert!(
            options.backups_allowed_path.is_dir(),
            "the archive path exists"
        );
        self.started = true;
        self.applied.0.lock().unwrap().clear();
        Ok(())
    }

    fn create_database(&mut self, _database: &str) -> Result<()> {
        Ok(())
    }

    fn use_database(&mut self, _database: &str) -> Result<()> {
        Ok(())
    }

    fn analyze(&mut self, sql: &str, target_database: &str) -> Result<QueryAnalysis> {
        Ok(classify(sql, target_database))
    }

    fn query(&mut self, _sql: &str, _format: OutputFormat) -> Result<Vec<u8>> {
        Ok(self.applied.statements().join("\n").into_bytes())
    }

    fn run(&mut self, sql: &str) -> Result<()> {
        if let Some(needle) = &self.fail_statements_containing {
            if sql.contains(needle.as_str()) {
                return Err(engine_error("the fake engine refused a statement"));
            }
        }
        self.applied.0.lock().unwrap().push(sql.to_string());
        Ok(())
    }

    fn backup_database(&mut self, _database: &str, file_path: &Path) -> Result<()> {
        assert!(file_path.is_absolute(), "an archive path is absolute");
        assert!(!file_path.exists(), "an archive is never overwritten");
        fs::write(file_path, self.applied.statements().join("\n"))
            .map_err(|_| engine_error("the fake engine could not write an archive"))
    }

    fn restore_database(&mut self, _database: &str, file_path: &Path) -> Result<()> {
        if self.fail_restore {
            return Err(engine_error("the fake engine refused to restore"));
        }
        let body = fs::read_to_string(file_path)
            .map_err(|_| engine_error("the fake engine could not read an archive"))?;
        let mut applied = self.applied.0.lock().unwrap();
        applied.extend(body.lines().filter(|l| !l.is_empty()).map(str::to_string));
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.started = false;
        Ok(())
    }
}

fn classify(sql: &str, target_database: &str) -> QueryAnalysis {
    let statement_count = sql
        .split(';')
        .filter(|part| !part.trim().is_empty())
        .count() as u32;
    let head = sql.trim_start().to_uppercase();
    let (class, lifecycle) = if head.starts_with("SELECT") || head.starts_with("SHOW") {
        (QueryClass::ReadOnly, false)
    } else if head.starts_with("CREATE DATABASE")
        || head.starts_with("DROP DATABASE")
        || head.starts_with("RENAME DATABASE")
    {
        (QueryClass::Mutating, true)
    } else if head.starts_with("CREATE FUNCTION") || head.starts_with("CREATE USER") {
        (QueryClass::MutatingGlobal, false)
    } else if head.starts_with("USE")
        || head.starts_with("SET")
        || head.starts_with("SYSTEM")
        || head.starts_with("BACKUP")
        || head.starts_with("RESTORE")
    {
        (QueryClass::Control, false)
    } else if head.starts_with("INSERT")
        || head.starts_with("CREATE TABLE")
        || head.starts_with("ALTER")
        || head.starts_with("DROP TABLE")
        || head.starts_with("OPTIMIZE")
    {
        (QueryClass::Mutating, false)
    } else {
        (QueryClass::Unknown, false)
    };
    QueryAnalysis {
        class,
        statement_count: if class == QueryClass::Unknown {
            0
        } else {
            statement_count
        },
        has_secrets: sql.to_uppercase().contains("PASSWORD"),
        writes_only_target_database: !sql.contains("elsewhere.") && !target_database.is_empty(),
        changes_database_lifecycle: lifecycle,
    }
}

// ------------------------------------------------------------ fault backend

/// What a wrapped call should do instead of what it would have done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Fault {
    #[default]
    None,
    /// Do the work, then claim not to know whether it happened.
    LoseTheResponse,
    /// Do nothing and claim not to know whether it happened.
    Unknowable,
    /// Do nothing and report a lost race.
    Refuse,
}

/// A [`LocalBackend`] that can be told to misbehave, for the §7.4 matrix.
struct FaultyBackend {
    inner: LocalBackend,
    put: Mutex<Fault>,
    replace: Mutex<Fault>,
    replaces: AtomicUsize,
}

impl FaultyBackend {
    fn new(root: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            inner: LocalBackend::new(root).expect("a local backend"),
            put: Mutex::new(Fault::None),
            replace: Mutex::new(Fault::None),
            replaces: AtomicUsize::new(0),
        })
    }

    fn on_put(&self, fault: Fault) {
        *self.put.lock().unwrap() = fault;
    }

    fn on_replace(&self, fault: Fault) {
        *self.replace.lock().unwrap() = fault;
    }
}

impl Backend for FaultyBackend {
    fn describe(&self) -> String {
        self.inner.describe()
    }

    fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get_bytes(key)
    }

    fn get_bytes_with_etag(&self, key: &str) -> Result<Option<Tagged>> {
        self.inner.get_bytes_with_etag(key)
    }

    fn open_reader(&self, key: &str) -> Result<Option<Box<dyn Read + Send>>> {
        self.inner.open_reader(key)
    }

    fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> Result<PutOutcome> {
        match *self.put.lock().unwrap() {
            Fault::None => self.inner.put_bytes_if_absent(key, data),
            Fault::LoseTheResponse => {
                self.inner.put_bytes_if_absent(key, data)?;
                Ok(PutOutcome::Ambiguous)
            }
            Fault::Unknowable | Fault::Refuse => Ok(PutOutcome::Ambiguous),
        }
    }

    fn put_file_if_absent(
        &self,
        key: &str,
        local_path: &Path,
        digest: &Digest,
    ) -> Result<PutOutcome> {
        match *self.put.lock().unwrap() {
            Fault::None => self.inner.put_file_if_absent(key, local_path, digest),
            Fault::LoseTheResponse => {
                self.inner.put_file_if_absent(key, local_path, digest)?;
                Ok(PutOutcome::Ambiguous)
            }
            Fault::Unknowable | Fault::Refuse => Ok(PutOutcome::Ambiguous),
        }
    }

    fn replace_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<ReplaceOutcome> {
        self.replaces.fetch_add(1, Ordering::SeqCst);
        match *self.replace.lock().unwrap() {
            Fault::None => self.inner.replace_if_match(key, data, etag),
            Fault::LoseTheResponse => {
                self.inner.replace_if_match(key, data, etag)?;
                Ok(ReplaceOutcome::Ambiguous)
            }
            Fault::Unknowable => Ok(ReplaceOutcome::Ambiguous),
            Fault::Refuse => Ok(ReplaceOutcome::NotMatched),
        }
    }
}

// ----------------------------------------------------------------- helpers

/// Tuning that keeps a whole test inside a few hundred milliseconds.
fn brisk() -> Tuning {
    Tuning {
        lease_ttl: std::time::Duration::from_secs(3),
        heartbeat_interval: std::time::Duration::from_secs(1),
        clock_skew_allowance: std::time::Duration::from_millis(50),
        commit_deadline: std::time::Duration::from_millis(400),
        max_commit_attempts: 3,
    }
}

fn namespace(root: &Path, applied: &Applied) -> Namespace {
    Namespace::new(root.to_str().expect("a UTF-8 root"))
        .expect("a local namespace")
        .with_engine(FakeEngine::factory(applied))
        .with_owner("conformance")
        .with_tuning(brisk())
}

fn writer(root: &Path, applied: &Applied) -> (DurableObject, bool) {
    namespace(root, applied)
        .open(
            "tenant",
            OpenOptions {
                database: Some("mem".to_string()),
                ..OpenOptions::default()
            },
        )
        .expect("a writer")
}

/// The object's directory inside the namespace root.
fn object_dir(root: &Path) -> PathBuf {
    root.join("tenant")
}

fn head_json(root: &Path) -> serde_json::Value {
    let backend = LocalBackend::new(object_dir(root)).expect("a backend");
    let bytes = backend
        .get_bytes(HEAD_KEY)
        .expect("a readable head")
        .expect("a head");
    serde_json::from_slice(&bytes).expect("valid JSON")
}

/// Writes a hand-made `head.json`, as another binding would have left it.
fn plant_head(root: &Path, document: &str) {
    let dir = object_dir(root);
    fs::create_dir_all(&dir).expect("an object directory");
    fs::write(dir.join(HEAD_KEY), document).expect("a planted head");
}

const COLD_DOCUMENT: &str = r#"{
  "protocol": {"version": 1, "reader_features": [], "writer_features": []},
  "engine": {"name": "chdb", "version": "26.7.2-rc.2", "backup_format": 1,
             "min_reader": "26.7.2-rc.2"},
  "lease": {"generation": 4, "owner": null, "instance": null, "expires_at": null},
  "manifest": {"db": "mem", "base": null, "wal": [], "seq": 0}
}"#;

fn document_with(replacements: &[(&str, &str)]) -> String {
    let mut out = COLD_DOCUMENT.to_string();
    for (from, to) in replacements {
        assert!(out.contains(from), "the fixture has no {from:?} to replace");
        out = out.replace(from, to);
    }
    out
}

// ------------------------------------------------------- §7.2 format fixtures

#[test]
fn a_cold_object_is_created_and_reopened_with_its_state() {
    let tmp = common::tempdir();
    let applied = Applied::default();

    let (object, existed) = writer(tmp.path(), &applied);
    assert!(!existed, "nothing was there");
    assert_eq!(
        object.generation(),
        1,
        "a cold object starts at generation 1"
    );
    assert_eq!(object.database(), "mem");

    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    object.execute("INSERT INTO t VALUES (2)").expect("a write");
    assert_eq!(object.stats().pending_statements, 2);
    let segment = object.flush().expect("a flush").expect("a segment");
    assert!(segment.key.starts_with("wal/1-1-"), "{}", segment.key);
    assert_eq!(object.stats().pending_statements, 0);
    object.close().expect("a clean close");

    // The head is released, and the manifest names the segment.
    let head = head_json(tmp.path());
    assert!(head["lease"]["owner"].is_null(), "the lease was released");
    assert_eq!(head["lease"]["generation"], 1);
    assert_eq!(head["manifest"]["wal"].as_array().unwrap().len(), 1);
    assert_eq!(head["manifest"]["seq"], 1);

    // A second open replays the WAL onto an empty engine.
    let reopened = Applied::default();
    let (object, existed) = writer(tmp.path(), &reopened);
    assert!(existed, "the object was there this time");
    assert_eq!(
        reopened.statements(),
        vec!["INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)"]
    );
    assert_eq!(object.generation(), 2, "taking the lease moved the fence");
    object.close().expect("a clean close");
}

#[test]
fn a_checkpoint_folds_the_wal_into_a_base_that_restores_alone() {
    let tmp = common::tempdir();
    let applied = Applied::default();

    let (object, _) = writer(tmp.path(), &applied);
    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    object.flush().expect("a flush");
    object.execute("INSERT INTO t VALUES (2)").expect("a write");
    let base = object.checkpoint().expect("a checkpoint");
    assert!(base.key.starts_with("checkpoints/1-"), "{}", base.key);
    assert_eq!(
        object.stats().pending_statements,
        0,
        "the backup already holds the buffered statement"
    );
    object.close().expect("a clean close");

    let head = head_json(tmp.path());
    assert_eq!(head["manifest"]["base"]["key"], base.key.as_str());
    assert!(
        head["manifest"]["wal"].as_array().unwrap().is_empty(),
        "the WAL list is cleared by a checkpoint"
    );

    let reopened = Applied::default();
    let (object, _) = writer(tmp.path(), &reopened);
    assert_eq!(
        reopened.statements(),
        vec!["INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)"],
        "both statements come back, one from the base rather than the WAL"
    );
    object.close().expect("a clean close");
}

#[test]
fn a_read_only_open_of_a_missing_object_is_not_found_and_creates_nothing() {
    let tmp = common::tempdir();
    let applied = Applied::default();
    let namespace = namespace(tmp.path(), &applied);

    let error = namespace
        .open(
            "tenant",
            OpenOptions {
                read_only: true,
                ..OpenOptions::default()
            },
        )
        .expect_err("a reader never creates the object it came to read");
    assert_eq!(error.category(), Category::NotFound);
    assert!(
        !object_dir(tmp.path()).join(HEAD_KEY).exists(),
        "nothing was published"
    );

    let error = namespace
        .open(
            "tenant",
            OpenOptions {
                existing_only: true,
                ..OpenOptions::default()
            },
        )
        .expect_err("existing_only means existing");
    assert_eq!(error.category(), Category::NotFound);
}

#[test]
fn a_read_only_handle_serves_the_manifest_and_refuses_to_write() {
    let tmp = common::tempdir();
    let applied = Applied::default();

    let (object, _) = writer(tmp.path(), &applied);
    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    object.flush().expect("a flush");
    object.close().expect("a clean close");

    let replayed = Applied::default();
    let (reader, existed) = namespace(tmp.path(), &replayed)
        .open(
            "tenant",
            OpenOptions {
                read_only: true,
                ..OpenOptions::default()
            },
        )
        .expect("a reader");
    assert!(existed);
    assert!(reader.read_only());
    assert_eq!(replayed.statements(), vec!["INSERT INTO t VALUES (1)"]);
    assert_eq!(
        reader.generation(),
        1,
        "a reader takes no lease, so the generation does not move"
    );

    let error = reader
        .execute("INSERT INTO t VALUES (2)")
        .expect_err("a reader cannot write");
    assert_eq!(error.category(), Category::ClassificationRefused);
    reader.close().expect("a clean close");

    // And the lease it never took is still free for a writer.
    let (writer, _) = writer(tmp.path(), &Applied::default());
    writer.close().expect("a clean close");
}

#[test]
fn a_manifest_naming_something_that_is_not_there_is_corrupt() {
    let tmp = common::tempdir();
    let applied = Applied::default();

    let (object, _) = writer(tmp.path(), &applied);
    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    let segment = object.flush().expect("a flush").expect("a segment");
    object.close().expect("a clean close");

    fs::remove_file(object_dir(tmp.path()).join(&segment.key)).expect("remove the segment");

    let error = namespace(tmp.path(), &Applied::default())
        .open("tenant", OpenOptions::default())
        .expect_err("a missing segment is incomplete state, not an empty object");
    assert_eq!(error.category(), Category::Corrupt);
}

#[test]
fn an_object_whose_bytes_changed_under_the_manifest_is_corrupt() {
    let tmp = common::tempdir();

    for damage in ["truncate", "replace"] {
        let tmp = tmp.path().join(damage);
        fs::create_dir_all(&tmp).expect("a root per case");
        let (object, _) = writer(&tmp, &Applied::default());
        object.execute("INSERT INTO t VALUES (1)").expect("a write");
        let segment = object.flush().expect("a flush").expect("a segment");
        object.close().expect("a clean close");

        let path = object_dir(&tmp).join(&segment.key);
        let original = fs::read(&path).expect("the segment");
        fs::remove_file(&path).expect("the published segment is immutable, so replace it");
        match damage {
            // A truncated upload has the right prefix and the wrong length.
            "truncate" => fs::write(&path, &original[..original.len() - 5]),
            // A replaced object can have the right length and the wrong bytes.
            "replace" => fs::write(
                &path,
                original
                    .iter()
                    .map(|b| if *b == b'1' { b'2' } else { *b })
                    .collect::<Vec<u8>>(),
            ),
            _ => unreachable!(),
        }
        .expect("damage the segment");

        let error = namespace(&tmp, &Applied::default())
            .open("tenant", OpenOptions::default())
            .expect_err("a segment that is not what the head describes");
        assert_eq!(error.category(), Category::Corrupt, "{damage}");
    }
}

#[test]
fn an_unreadable_head_is_corrupt_rather_than_a_cold_object() {
    let tmp = common::tempdir();
    let cases = [
        ("not json at all", "{"),
        ("not an object", "[1, 2, 3]"),
        (
            "a manifest with no database",
            &document_with(&[(r#""db": "mem""#, r#""db": """#)]),
        ),
        (
            "a lease that is neither held nor released",
            &document_with(&[(
                r#""owner": null, "instance": null, "expires_at": null"#,
                r#""owner": "someone", "instance": null, "expires_at": null"#,
            )]),
        ),
        (
            "a reference that could leave the object",
            &document_with(&[(
                r#""base": null"#,
                r#""base": {"key": "../elsewhere.tar.gz", "size": 1,
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000"}"#,
            )]),
        ),
        (
            "a reference with no digest",
            &document_with(&[(
                r#""base": null"#,
                r#""base": {"key": "checkpoints/1-1-aaaaaaaa.tar.gz", "size": 1}"#,
            )]),
        ),
    ];

    for (what, document) in cases {
        let root = tmp.path().join(what.replace(' ', "-"));
        plant_head(&root, document);
        let error = namespace(&root, &Applied::default())
            .open("tenant", OpenOptions::default())
            .expect_err("a head that does not mean what it says");
        assert_eq!(error.category(), Category::Corrupt, "{what}");
    }
}

#[test]
fn the_negotiation_fields_decide_what_opens() {
    let tmp = common::tempdir();

    // A producer on a different but compatible release opens.
    let root = tmp.path().join("compatible");
    plant_head(&root, COLD_DOCUMENT);
    let (object, existed) = writer(&root, &Applied::default());
    assert!(existed, "the planted object was adopted");
    assert_eq!(
        object.generation(),
        5,
        "the released lease is taken at the next generation"
    );
    object.close().expect("a clean close");

    let refusals = [
        (
            "future-protocol-version",
            document_with(&[(
                r#""version": 1, "reader_features""#,
                r#""version": 2, "reader_features""#,
            )]),
            Category::ProtocolUnsupported,
        ),
        (
            "unknown-reader-feature",
            document_with(&[(
                r#""reader_features": []"#,
                r#""reader_features": ["parquet-wal"]"#,
            )]),
            Category::ProtocolUnsupported,
        ),
        (
            "engine-reader-too-old",
            document_with(&[(
                r#""min_reader": "26.7.2-rc.2""#,
                r#""min_reader": "27.1.0""#,
            )]),
            Category::EngineIncompatible,
        ),
        (
            "backup-format-too-new",
            document_with(&[(r#""backup_format": 1"#, r#""backup_format": 2"#)]),
            Category::EngineIncompatible,
        ),
        (
            "another-engine",
            document_with(&[(r#""name": "chdb""#, r#""name": "duckdb""#)]),
            Category::EngineIncompatible,
        ),
    ];
    for (what, document, expected) in refusals {
        let root = tmp.path().join(what);
        plant_head(&root, &document);
        match namespace(&root, &Applied::default()).open("tenant", OpenOptions::default()) {
            Ok(_) => panic!("{what} must not open"),
            Err(error) => assert_eq!(error.category(), expected, "{what}"),
        }
    }
}

#[test]
fn an_unknown_writer_feature_allows_a_reader_and_refuses_the_lease() {
    let tmp = common::tempdir();
    let document = document_with(&[(
        r#""writer_features": []"#,
        r#""writer_features": ["preamble"]"#,
    )]);
    plant_head(tmp.path(), &document);

    let error = namespace(tmp.path(), &Applied::default())
        .open("tenant", OpenOptions::default())
        .expect_err("writing needs semantics this build does not have");
    assert_eq!(error.category(), Category::ProtocolUnsupported);
    assert_eq!(error.features(), ["preamble"]);

    let (reader, _) = namespace(tmp.path(), &Applied::default())
        .open(
            "tenant",
            OpenOptions {
                read_only: true,
                ..OpenOptions::default()
            },
        )
        .expect("reading is still sound");
    reader.close().expect("a clean close");
}

#[test]
fn a_field_this_build_does_not_know_survives_the_whole_lifecycle() {
    let tmp = common::tempdir();
    let document = document_with(&[
        (
            r#""manifest": {"db""#,
            r#""tenancy": {"shard": 7}, "manifest": {"db""#,
        ),
        (
            r#""seq": 0"#,
            r#""seq": 0, "compaction": {"policy": "tiered"}"#,
        ),
    ]);
    plant_head(tmp.path(), &document);

    let (object, _) = writer(tmp.path(), &Applied::default());
    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    object.flush().expect("a flush");
    object.checkpoint().expect("a checkpoint");
    object.close().expect("a clean close");

    let head = head_json(tmp.path());
    assert_eq!(head["tenancy"]["shard"], 7, "a top-level unknown survived");
    assert_eq!(
        head["manifest"]["compaction"]["policy"], "tiered",
        "an unknown inside a known block survived"
    );
    assert!(
        head["manifest"]["base"].is_object(),
        "and the known fields moved on"
    );
}

#[test]
fn a_document_in_another_key_order_reads_the_same() {
    let tmp = common::tempdir();
    plant_head(
        tmp.path(),
        r#"{"manifest":{"seq":0,"wal":[],"base":null,"db":"mem"},
            "lease":{"expires_at":null,"instance":null,"owner":null,"generation":1},
            "engine":{"min_reader":"26.7.2","backup_format":1,"version":"26.7.2","name":"chdb"},
            "protocol":{"writer_features":[],"reader_features":[],"version":1}}"#,
    );
    let (object, existed) = writer(tmp.path(), &Applied::default());
    assert!(existed);
    assert_eq!(object.database(), "mem");
    object.close().expect("a clean close");
}

#[test]
fn a_quoted_database_name_travels_through_the_manifest() {
    let tmp = common::tempdir();
    let applied = Applied::default();
    let (object, _) = namespace(tmp.path(), &applied)
        .open(
            "tenant",
            OpenOptions {
                database: Some("my-db one`two".to_string()),
                ..OpenOptions::default()
            },
        )
        .expect("a writer");
    assert_eq!(object.database(), "my-db one`two");
    object.close().expect("a clean close");

    assert_eq!(head_json(tmp.path())["manifest"]["db"], "my-db one`two");
    let (reopened, _) = writer(tmp.path(), &Applied::default());
    assert_eq!(
        reopened.database(),
        "my-db one`two",
        "the object owns its database name, not the caller"
    );
    reopened.close().expect("a clean close");
}

// -------------------------------------------------------- §7.3 entry gates

#[test]
fn the_method_name_is_not_the_gate() {
    let tmp = common::tempdir();
    let (object, _) = writer(tmp.path(), &Applied::default());

    // execute refuses everything that is not one local mutation …
    for (sql, expected) in [
        ("SELECT 1", Category::ClassificationRefused),
        (
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)",
            Category::ClassificationRefused,
        ),
        ("USE other", Category::ClassificationRefused),
        ("CREATE DATABASE other", Category::ClassificationRefused),
        (
            "CREATE FUNCTION f AS x -> x",
            Category::ClassificationRefused,
        ),
        (
            "INSERT INTO elsewhere.t VALUES (1)",
            Category::ClassificationRefused,
        ),
        ("blah blah", Category::ClassificationRefused),
        (
            "CREATE USER bob IDENTIFIED BY 'password'",
            Category::ClassificationRefused,
        ),
    ] {
        let error = object.execute(sql).expect_err("refused by analysis");
        assert_eq!(error.category(), expected, "{sql}");
    }

    // … and query refuses everything that is not one read.
    for sql in [
        "INSERT INTO t VALUES (1)",
        "SELECT 1; SELECT 2",
        "blah blah",
    ] {
        let error = object
            .query(sql, OutputFormat::CSV)
            .expect_err("refused by analysis");
        assert_eq!(error.category(), Category::ClassificationRefused, "{sql}");
    }

    assert_eq!(
        object.stats().executed_statements,
        0,
        "a refusal runs nothing"
    );
    object.close().expect("a clean close");
}

#[test]
fn a_credential_bearing_mutation_is_refused_without_being_echoed() {
    let tmp = common::tempdir();
    let (object, _) = writer(tmp.path(), &Applied::default());

    let error = object
        .execute("ALTER TABLE t MODIFY SETTING s = 'PASSWORD hunter2'")
        .expect_err("the WAL outlives the statement");
    assert_eq!(error.category(), Category::SecretRefused);
    assert!(
        !error.to_string().contains("hunter2"),
        "the refusal must not carry the credential: {error}"
    );
    object.close().expect("a clean close");
}

#[test]
fn a_statement_over_the_frozen_limit_is_refused_before_it_runs() {
    let tmp = common::tempdir();
    let applied = Applied::default();
    let (object, _) = writer(tmp.path(), &applied);

    let huge = format!(
        "INSERT INTO t VALUES ('{}')",
        "x".repeat(chdb_rust::durable::MAX_SQL_BYTES as usize)
    );
    let error = object
        .execute(&huge)
        .expect_err("over the per-statement limit");
    assert_eq!(error.category(), Category::LimitExceeded);
    assert!(
        applied.statements().is_empty(),
        "a statement the WAL cannot hold must not reach the engine"
    );
    object.close().expect("a clean close");
}

// -------------------------------------------------------- §7.4 fault matrix

/// A namespace over a backend that can be told to misbehave.
fn faulty(root: &Path, applied: &Applied, backend: &Arc<FaultyBackend>) -> Namespace {
    let backend = Arc::clone(backend);
    Namespace::with_backend(Box::new(move |_id| {
        Ok(Arc::clone(&backend) as Arc<dyn Backend>)
    }))
    .with_engine(FakeEngine::factory(applied))
    .with_owner("conformance")
    .with_tuning(brisk())
    .with_scratch_root(root.to_path_buf())
}

#[test]
fn a_wal_upload_that_lands_with_a_lost_response_is_reconciled_as_committed() {
    let tmp = common::tempdir();
    let backend = FaultyBackend::new(tmp.path().join("object"));
    let namespace = faulty(tmp.path(), &Applied::default(), &backend);
    let (object, _) = namespace
        .open("tenant", OpenOptions::default())
        .expect("a writer");

    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    backend.on_put(Fault::LoseTheResponse);
    let segment = object
        .flush()
        .expect("re-reading the unique key settles it")
        .expect("a segment");
    backend.on_put(Fault::None);

    assert_eq!(object.manifest().wal[0].key, segment.key);
    assert_eq!(object.stats().pending_statements, 0);
    object.close().expect("a clean close");
}

#[test]
fn a_head_commit_that_lands_with_a_lost_response_is_reconciled_as_committed() {
    let tmp = common::tempdir();
    let backend = FaultyBackend::new(tmp.path().join("object"));
    let namespace = faulty(tmp.path(), &Applied::default(), &backend);
    let (object, _) = namespace
        .open("tenant", OpenOptions::default())
        .expect("a writer");

    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    backend.on_replace(Fault::LoseTheResponse);
    let segment = object
        .flush()
        .expect("the manifest names the key, so the commit landed")
        .expect("a segment");
    backend.on_replace(Fault::None);

    assert_eq!(object.manifest().wal[0].key, segment.key);
    assert_eq!(object.stats().committed_statements, 1);
    object.close().expect("a clean close");
}

#[test]
fn a_commit_that_cannot_be_proven_is_reported_as_ambiguous() {
    let tmp = common::tempdir();
    let backend = FaultyBackend::new(tmp.path().join("object"));
    let namespace = faulty(tmp.path(), &Applied::default(), &backend);
    let (object, _) = namespace
        .open("tenant", OpenOptions::default())
        .expect("a writer");

    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    backend.on_replace(Fault::Unknowable);
    let error = object.flush().expect_err("nothing proves the outcome");
    backend.on_replace(Fault::None);

    assert_eq!(error.category(), Category::CommitAmbiguous);
    assert_eq!(
        object.stats().pending_statements,
        1,
        "an unproven commit keeps the buffer"
    );
    object.close().expect("a close that flushes what was kept");
}

#[test]
fn a_wal_upload_whose_commit_is_refused_leaves_the_old_manifest_authoritative() {
    let tmp = common::tempdir();
    let backend = FaultyBackend::new(tmp.path().join("object"));
    let namespace = faulty(tmp.path(), &Applied::default(), &backend);
    let (object, _) = namespace
        .open("tenant", OpenOptions::default())
        .expect("a writer");

    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    backend.on_replace(Fault::Refuse);
    let error = object.flush().expect_err("the head never moved");
    backend.on_replace(Fault::None);

    assert_eq!(error.category(), Category::Timeout);
    assert!(
        object.manifest().wal.is_empty(),
        "the old manifest is still the authoritative one"
    );
    assert_eq!(
        object.stats().pending_statements,
        1,
        "and the statement is still ours to publish"
    );

    // The retry publishes a fresh unique segment rather than overwriting.
    let segment = object.flush().expect("a retry").expect("a segment");
    assert_eq!(object.manifest().wal[0].key, segment.key);
    object.close().expect("a clean close");
}

#[test]
fn a_checkpoint_whose_commit_is_refused_leaves_the_old_base_restorable() {
    let tmp = common::tempdir();
    let backend = FaultyBackend::new(tmp.path().join("object"));
    let applied = Applied::default();
    let namespace = faulty(tmp.path(), &applied, &backend);
    let (object, _) = namespace
        .open("tenant", OpenOptions::default())
        .expect("a writer");

    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    object.flush().expect("a flush");
    let committed = object.manifest();

    object.execute("INSERT INTO t VALUES (2)").expect("a write");
    backend.on_replace(Fault::Refuse);
    let error = object.checkpoint().expect_err("the head never moved");
    backend.on_replace(Fault::None);
    assert_eq!(error.category(), Category::Timeout);

    assert_eq!(
        object.manifest(),
        committed,
        "the old base and WAL are still what a recovery would use"
    );
    assert_eq!(object.stats().pending_statements, 1);
    object.close().expect("a clean close");
}

#[test]
fn only_one_writer_wins_a_race_for_a_cold_object() {
    let tmp = common::tempdir();
    let backend = FaultyBackend::new(tmp.path().join("object"));

    let first = faulty(tmp.path(), &Applied::default(), &backend)
        .open("tenant", OpenOptions::default())
        .expect("the first writer");
    let error = faulty(tmp.path(), &Applied::default(), &backend)
        .open("tenant", OpenOptions::default())
        .expect_err("the second writer finds it held");
    assert_eq!(error.category(), Category::LeaseHeld);
    assert_eq!(error.owner(), Some("conformance"));

    first.0.close().expect("a clean close");

    // Released, so the next writer is free to take it.
    let second = faulty(tmp.path(), &Applied::default(), &backend)
        .open("tenant", OpenOptions::default())
        .expect("the lease was given back");
    second.0.close().expect("a clean close");
}

#[test]
fn a_force_takeover_fences_the_writer_it_displaced() {
    let tmp = common::tempdir();
    let backend = FaultyBackend::new(tmp.path().join("object"));

    let (displaced, _) = faulty(tmp.path(), &Applied::default(), &backend)
        .open("tenant", OpenOptions::default())
        .expect("the first writer");
    displaced
        .execute("INSERT INTO t VALUES (1)")
        .expect("a write nobody will see");

    let (taker, _) = faulty(tmp.path(), &Applied::default(), &backend)
        .open(
            "tenant",
            OpenOptions {
                force: true,
                ..OpenOptions::default()
            },
        )
        .expect("an administrative takeover");
    assert_eq!(taker.generation(), 2);

    let error = displaced
        .flush()
        .expect_err("the fence is the compare-and-set");
    assert_eq!(error.category(), Category::LeaseFenced);
    assert!(displaced.is_fenced());

    // A fenced writer still reports its loss on close, rather than silently
    // dropping the statement it never published.
    let error = displaced.close().expect_err("unflushed work was lost");
    assert_eq!(error.category(), Category::LeaseFenced);
    taker.close().expect("a clean close");
}

#[test]
fn a_writer_that_cannot_confirm_its_lease_fences_itself() {
    let tmp = common::tempdir();
    let backend = FaultyBackend::new(tmp.path().join("object"));
    let namespace = Namespace::with_backend({
        let backend = Arc::clone(&backend);
        Box::new(move |_id| Ok(Arc::clone(&backend) as Arc<dyn Backend>))
    })
    .with_engine(FakeEngine::factory(&Applied::default()))
    .with_scratch_root(tmp.path().to_path_buf())
    .with_tuning(Tuning {
        lease_ttl: std::time::Duration::from_millis(300),
        heartbeat_interval: std::time::Duration::from_millis(90),
        clock_skew_allowance: std::time::Duration::from_millis(10),
        commit_deadline: std::time::Duration::from_millis(100),
        max_commit_attempts: 2,
    });
    let (object, _) = namespace
        .open("tenant", OpenOptions::default())
        .expect("a writer");

    // Every renewal from here on fails, so the locally believed validity window
    // runs out.
    backend.on_replace(Fault::Refuse);
    std::thread::sleep(std::time::Duration::from_millis(500));

    let error = object
        .execute("INSERT INTO t VALUES (1)")
        .expect_err("past its own expiry, a writer stops writing");
    assert_eq!(error.category(), Category::LeaseFenced);
    assert!(object.is_fenced());

    backend.on_replace(Fault::None);
    // Reads are still served: §5.7 fences writes, not reads.
    object
        .query("SELECT 1", OutputFormat::CSV)
        .expect("a fenced writer may still read its own local state");
    let _ = object.close();
}

#[test]
fn a_restore_that_fails_hands_back_no_session_and_no_lease() {
    let tmp = common::tempdir();
    let applied = Applied::default();

    let (object, _) = writer(tmp.path(), &applied);
    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    object.checkpoint().expect("a checkpoint");
    object.close().expect("a clean close");

    let scratch_root = tmp.path().join("scratch");
    let error = Namespace::new(tmp.path().to_str().expect("a UTF-8 root"))
        .expect("a namespace")
        .with_engine(FakeEngine::factory_with(
            &Applied::default(),
            "26.7.2",
            1,
            true,
            None,
        ))
        .with_scratch_root(&scratch_root)
        .with_tuning(brisk())
        .open("tenant", OpenOptions::default())
        .expect_err("a partial recovery is not a session");
    assert_eq!(error.category(), Category::EngineIncompatible);

    assert_eq!(
        fs::read_dir(&scratch_root).into_iter().flatten().count(),
        0,
        "the scratch tree of a failed open goes with it"
    );

    // The lease it took on the way in was given back, so the next writer is not
    // blocked for a TTL.
    let (object, _) = writer(tmp.path(), &Applied::default());
    object.close().expect("a clean close");
}

#[test]
fn a_replay_that_fails_stops_the_open() {
    let tmp = common::tempdir();
    let (object, _) = writer(tmp.path(), &Applied::default());
    object
        .execute("INSERT INTO t VALUES (1)")
        .expect("a write that will not replay");
    object.flush().expect("a flush");
    object.close().expect("a clean close");

    let error = Namespace::new(tmp.path().to_str().expect("a UTF-8 root"))
        .expect("a namespace")
        .with_engine(FakeEngine::factory_with(
            &Applied::default(),
            "26.7.2",
            1,
            false,
            Some("VALUES (1)"),
        ))
        .with_tuning(brisk())
        .open("tenant", OpenOptions::default())
        .expect_err("a statement that will not replay stops the open");
    assert_eq!(error.category(), Category::Engine);
}

#[test]
fn close_takes_the_handle_and_the_scratch_tree_with_it() {
    let tmp = common::tempdir();
    let applied = Applied::default();
    let namespace = namespace(tmp.path(), &applied);
    let (object, _) = namespace
        .open(
            "tenant",
            OpenOptions {
                database: Some("mem".to_string()),
                ..OpenOptions::default()
            },
        )
        .expect("a writer");
    let scratch = object.scratch_path().to_path_buf();
    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    object.close().expect("a clean close");

    assert!(!scratch.exists(), "close removes the scratch tree");
}

#[test]
fn a_dropped_handle_reclaims_locally_without_pretending_to_be_a_close() {
    let tmp = common::tempdir();
    let applied = Applied::default();
    let scratch;
    {
        let (object, _) = writer(tmp.path(), &applied);
        scratch = object.scratch_path().to_path_buf();
        object.execute("INSERT INTO t VALUES (1)").expect("a write");
        // No close: the handle just goes.
    }
    assert!(!scratch.exists(), "the scratch tree is still reclaimed");

    let head = head_json(tmp.path());
    assert_eq!(
        head["lease"]["owner"], "conformance",
        "the lease is left to expire rather than released, because Drop is not a barrier"
    );
    assert!(
        head["manifest"]["wal"].as_array().unwrap().is_empty(),
        "and the unflushed statement was never published"
    );
}

#[test]
fn concurrent_writers_on_one_handle_serialize_and_coalesce() {
    let tmp = common::tempdir();
    let applied = Applied::default();
    let (object, _) = writer(tmp.path(), &applied);
    let object = Arc::new(object);

    let mut threads = Vec::new();
    for n in 0..8 {
        let object = Arc::clone(&object);
        threads.push(std::thread::spawn(move || {
            let ticket = object
                .execute(&format!("INSERT INTO t VALUES ({n})"))
                .expect("a write");
            object.flush_through(ticket).expect("a barrier");
        }));
    }
    for thread in threads {
        thread.join().expect("a thread");
    }

    let stats = object.stats();
    assert_eq!(stats.executed_statements, 8);
    assert_eq!(stats.committed_statements, 8);
    assert_eq!(stats.pending_statements, 0);
    assert!(
        stats.wal_segments <= 8,
        "callers that arrive together share a segment"
    );
    assert_eq!(applied.statements().len(), 8);

    // Sequence numbers advance once per published reference, never skipping.
    let manifest = object.manifest();
    assert_eq!(manifest.seq as usize, manifest.wal.len());

    Arc::try_unwrap(object)
        .expect("the threads are done")
        .close()
        .expect("a clean close");
}

#[test]
fn what_this_writer_publishes_is_what_the_frozen_layout_says() {
    let tmp = common::tempdir();
    let (object, _) = writer(tmp.path(), &Applied::default());
    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    object.flush().expect("a flush");
    object.checkpoint().expect("a checkpoint");
    object.close().expect("a clean close");

    let dir = object_dir(tmp.path());
    let mut published: HashMap<String, usize> = HashMap::new();
    for entry in walk(&dir) {
        let relative = entry
            .strip_prefix(&dir)
            .unwrap()
            .to_string_lossy()
            .to_string();
        let bucket = relative.split('/').next().unwrap_or_default().to_string();
        *published.entry(bucket).or_default() += 1;
    }
    assert_eq!(published.get("wal").copied(), Some(1), "one WAL segment");
    assert_eq!(
        published.get("checkpoints").copied(),
        Some(1),
        "one checkpoint"
    );
    assert!(published.contains_key(HEAD_KEY), "and one head");

    let head = head_json(tmp.path());
    let base = head["manifest"]["base"]["key"]
        .as_str()
        .expect("a base key");
    assert!(
        base.starts_with("checkpoints/") && base.ends_with(".tar.gz"),
        "{base}"
    );
    assert_eq!(
        head["manifest"]["base"]["sha256"].as_str().unwrap().len(),
        64
    );
    assert_eq!(head["protocol"]["version"], 1);
    assert_eq!(head["engine"]["name"], "chdb");
    assert_eq!(head["engine"]["backup_format"], 1);
}

/// Every file under `dir`, ignoring the local backend's own bookkeeping.
fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue; // .head-versions and .tmp belong to the backend, not the protocol
        }
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}

#[test]
fn a_base_and_the_wal_on_top_of_it_are_replayed_in_order() {
    let tmp = common::tempdir();

    let (object, _) = writer(tmp.path(), &Applied::default());
    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    object.checkpoint().expect("a checkpoint");
    object.execute("INSERT INTO t VALUES (2)").expect("a write");
    object.flush().expect("a flush");
    object.execute("INSERT INTO t VALUES (3)").expect("a write");
    object.flush().expect("a second flush");
    object.close().expect("a clean close");

    let head = head_json(tmp.path());
    assert!(head["manifest"]["base"].is_object());
    assert_eq!(head["manifest"]["wal"].as_array().unwrap().len(), 2);

    let restored = Applied::default();
    let (object, _) = writer(tmp.path(), &restored);
    assert_eq!(
        restored.statements(),
        vec![
            "INSERT INTO t VALUES (1)",
            "INSERT INTO t VALUES (2)",
            "INSERT INTO t VALUES (3)",
        ],
        "the base first, then every segment in manifest order"
    );
    object.close().expect("a clean close");
}

#[test]
fn a_base_that_is_missing_or_damaged_stops_the_open() {
    let tmp = common::tempdir();

    for damage in ["missing", "truncated", "replaced"] {
        let root = tmp.path().join(damage);
        fs::create_dir_all(&root).expect("a root per case");
        let (object, _) = writer(&root, &Applied::default());
        object.execute("INSERT INTO t VALUES (1)").expect("a write");
        let base = object.checkpoint().expect("a checkpoint");
        object.close().expect("a clean close");

        let path = object_dir(&root).join(&base.key);
        let original = fs::read(&path).expect("the archive");
        fs::remove_file(&path).expect("a published archive is immutable, so replace it");
        match damage {
            "missing" => Ok(()),
            "truncated" => fs::write(&path, &original[..original.len() - 3]),
            "replaced" => fs::write(&path, vec![b'x'; original.len()]),
            _ => unreachable!(),
        }
        .expect("damage the archive");

        let error = namespace(&root, &Applied::default())
            .open("tenant", OpenOptions::default())
            .expect_err("a base that is not what the head describes");
        assert_eq!(
            error.category(),
            Category::Corrupt,
            "{damage}: the engine must never be handed unverified bytes"
        );
    }
}

#[test]
fn a_heartbeat_running_through_commits_does_not_disturb_them() {
    let tmp = common::tempdir();
    let applied = Applied::default();
    // A heartbeat every 60ms, so several renewals land inside the loop below
    // and contend for the same head as the flushes do.
    let (object, _) = Namespace::new(tmp.path().to_str().expect("a UTF-8 root"))
        .expect("a namespace")
        .with_engine(FakeEngine::factory(&applied))
        .with_tuning(Tuning {
            lease_ttl: std::time::Duration::from_millis(300),
            heartbeat_interval: std::time::Duration::from_millis(60),
            clock_skew_allowance: std::time::Duration::from_millis(50),
            commit_deadline: std::time::Duration::from_millis(500),
            max_commit_attempts: 5,
        })
        .open(
            "tenant",
            OpenOptions {
                database: Some("mem".to_string()),
                ..OpenOptions::default()
            },
        )
        .expect("a writer");

    for n in 0..12 {
        object
            .execute(&format!("INSERT INTO t VALUES ({n})"))
            .expect("a write");
        object.flush().expect("a flush");
        std::thread::sleep(std::time::Duration::from_millis(30));
    }

    let manifest = object.manifest();
    assert_eq!(manifest.wal.len(), 12, "one segment per flush");
    assert_eq!(
        manifest.seq, 12,
        "the sequence advances once per published reference, and a heartbeat is not one"
    );
    assert!(!object.is_fenced(), "renewals kept the lease");
    object.close().expect("a clean close");
}
