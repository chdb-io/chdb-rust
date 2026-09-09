//! Durable objects against the real engine.
//!
//! `durable_conformance.rs` drives the state machine with a fake, which is what
//! lets it reach a second writer and a lost commit response. This file is the
//! other half: what ClickHouse's own parser decides, what a real `BACKUP` and
//! `RESTORE` carry, and what the one-data-path-per-process constraint means for
//! a caller.
//!
//! Every test here needs the process engine to itself, so each takes [`engine`]
//! first and closes its object before returning.

use std::time::Duration;

use chdb_rust::durable::{Category, DurableObject, Namespace, OpenOptions, Tuning};
use chdb_rust::format::OutputFormat;
use chdb_rust::{active_engine_path, active_engine_refs};

mod common;

/// Takes the process engine for one test.
///
/// chdb-core binds one data path per process, so these cannot overlap.
/// Serializing here rather than relying on `--test-threads=1` means a plain
/// `cargo test` reports real failures instead of collisions.
fn engine() -> std::sync::MutexGuard<'static, ()> {
    static ENGINE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    ENGINE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn namespace(root: &std::path::Path) -> Namespace {
    Namespace::new(root.to_str().expect("a UTF-8 root"))
        .expect("a local namespace")
        .with_owner("engine-test")
        .with_tuning(Tuning {
            lease_ttl: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(3),
            ..Tuning::default()
        })
}

fn open(root: &std::path::Path) -> (DurableObject, bool) {
    namespace(root)
        .open(
            "tenant",
            OpenOptions {
                database: Some("mem".to_string()),
                ..OpenOptions::default()
            },
        )
        .expect("a writer")
}

fn text(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes)
        .expect("a UTF-8 result")
        .trim()
        .to_string()
}

#[test]
fn a_database_survives_the_process_that_wrote_it() {
    let _engine = engine();
    let tmp = common::tempdir();

    let (object, existed) = open(tmp.path());
    assert!(!existed);
    object
        .execute("CREATE TABLE events (id UInt64, tag String) ENGINE = MergeTree ORDER BY id")
        .expect("DDL is a mutation like any other");
    object
        .execute("INSERT INTO events VALUES (1, 'first')")
        .expect("a write");
    let ticket = object
        .execute("INSERT INTO events VALUES (2, 'second')")
        .expect("a write");

    assert_eq!(
        text(
            object
                .query("SELECT count() FROM events", OutputFormat::CSV)
                .expect("a read")
        ),
        "2",
        "a write is visible locally the moment it returns"
    );

    object.flush_through(ticket).expect("a durability barrier");
    object.close().expect("a clean close");

    // A different handle, a different scratch directory, the same data.
    let (object, existed) = open(tmp.path());
    assert!(existed);
    assert_eq!(
        text(
            object
                .query("SELECT tag FROM events ORDER BY id", OutputFormat::CSV)
                .expect("a read")
        ),
        "\"first\"\n\"second\"",
        "the WAL replayed onto an empty engine"
    );

    // And once folded into a base, the WAL is no longer what carries it.
    object.checkpoint().expect("a checkpoint");
    assert!(object.manifest().wal.is_empty());
    object.close().expect("a clean close");

    let (object, _) = open(tmp.path());
    assert_eq!(
        text(
            object
                .query("SELECT count() FROM events", OutputFormat::CSV)
                .expect("a read")
        ),
        "2",
        "restored from the base alone"
    );
    object.close().expect("a clean close");
}

#[test]
fn the_parser_decides_what_a_durable_object_will_run() {
    let _engine = engine();
    let tmp = common::tempdir();
    let (object, _) = open(tmp.path());
    object
        .execute("CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n")
        .expect("DDL");

    let refused = [
        // Two statements have no single replayable WAL record.
        (
            "a batch",
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)",
        ),
        // Session state is the object's to manage, not the caller's.
        ("a current-database change", "USE default"),
        ("a settings change", "SET max_threads = 1"),
        (
            "an async-insert relaxation",
            "INSERT INTO t SETTINGS async_insert = 1 VALUES (1)",
        ),
        // The object owns one database; its container is not a logged mutation.
        ("a database lifecycle change", "CREATE DATABASE other"),
        // A checkpoint of one database cannot carry state that lives beside it.
        ("a global UDF", "CREATE FUNCTION plus_one AS (x) -> x + 1"),
        // Writes a checkpoint would not capture.
        (
            "a write to another database",
            "INSERT INTO other.t VALUES (1)",
        ),
        (
            "a write outside the engine",
            "INSERT INTO FUNCTION file('out.csv', CSV) SELECT 1",
        ),
        // Nothing was proven about text that did not parse.
        ("text that is not SQL", "this is not sql"),
        // A read is not a write, and has its own entry point.
        ("a read", "SELECT 1"),
    ];
    for (what, sql) in refused {
        let error = object.execute(sql).expect_err("refused by the parser");
        assert_eq!(
            error.category(),
            Category::ClassificationRefused,
            "{what}: {error}"
        );
    }

    // A semicolon inside inline data is data, not a statement boundary — which
    // is the case no prefix list or regular expression can get right.
    object
        .execute("INSERT INTO t VALUES (1)")
        .expect("one statement");
    object
        .execute("ALTER TABLE t ADD COLUMN tag String DEFAULT 'a;b'")
        .expect("a semicolon in a literal is not a second statement");

    // And query refuses anything that is not exactly one read. Note what is
    // *not* in this list: `blah blah` parses, as an implicit SELECT of two
    // identifiers, and is a perfectly good read that then fails on an unknown
    // column. Only the parser can tell those apart, which is the argument for
    // asking it.
    for sql in [
        "INSERT INTO t VALUES (2)",
        "SELECT 1; SELECT 2",
        "this is not sql",
    ] {
        let error = object
            .query(sql, OutputFormat::CSV)
            .expect_err("refused by the parser");
        assert_eq!(error.category(), Category::ClassificationRefused, "{sql}");
    }

    object.close().expect("a clean close");
}

#[test]
fn a_credential_may_be_read_with_but_never_logged() {
    let _engine = engine();
    let tmp = common::tempdir();
    let (object, _) = open(tmp.path());
    object
        .execute("CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n")
        .expect("DDL");
    object
        .flush()
        .expect("a flush, so what follows is measured from empty");

    let secret_write = "INSERT INTO t SELECT 1 FROM \
         s3('https://example.invalid/x.csv', 'AKIAEXAMPLE', 'hunter2', 'CSV', 'n UInt64')";
    let error = object
        .execute(secret_write)
        .expect_err("a WAL segment is stored in plain text");
    assert_eq!(error.category(), Category::SecretRefused);
    assert!(
        !error.to_string().contains("hunter2"),
        "the refusal must not carry the credential: {error}"
    );

    // A read carrying the same credential is allowed through the gate: it never
    // reaches the WAL. It then fails on the unreachable host, and that failure
    // must not echo the statement either.
    let error = object
        .query(
            "SELECT * FROM s3('https://example.invalid/x.csv', 'AKIAEXAMPLE', 'hunter2', 'CSV', 'n UInt64')",
            OutputFormat::CSV,
        )
        .expect_err("example.invalid does not resolve");
    assert_eq!(
        error.category(),
        Category::Engine,
        "the gate let it through; the network did not"
    );

    assert_eq!(
        object.stats().pending_statements,
        0,
        "neither the refused write nor the failed read reached the WAL"
    );
    object.close().expect("a clean close");
}

#[test]
fn a_database_name_that_needs_quoting_round_trips_through_backup_and_restore() {
    let _engine = engine();
    let tmp = common::tempdir();

    let (object, _) = namespace(tmp.path())
        .open(
            "tenant",
            OpenOptions {
                database: Some("my-db".to_string()),
                ..OpenOptions::default()
            },
        )
        .expect("a writer");
    assert_eq!(object.database(), "my-db");
    object
        .execute("CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n")
        .expect("an unqualified name resolves to the object's database");
    object
        .execute("INSERT INTO t VALUES (42)")
        .expect("a write");
    object.checkpoint().expect("a checkpoint of a quoted name");
    object.close().expect("a clean close");

    let (object, _) = namespace(tmp.path())
        .open("tenant", OpenOptions::default())
        .expect("a writer");
    assert_eq!(
        object.database(),
        "my-db",
        "the object owns its database name, not the caller"
    );
    assert_eq!(
        text(
            object
                .query("SELECT n FROM t", OutputFormat::CSV)
                .expect("a read")
        ),
        "42"
    );
    object.close().expect("a clean close");
}

#[test]
fn a_second_object_in_one_process_is_refused_with_both_paths_named() {
    let _engine = engine();
    let tmp = common::tempdir();

    let (first, _) = open(tmp.path());
    let error = namespace(tmp.path())
        .open(
            "other-tenant",
            OpenOptions {
                database: Some("mem".to_string()),
                ..OpenOptions::default()
            },
        )
        .expect_err("chdb-core binds one data path per process");
    assert_eq!(error.category(), Category::Engine);
    let message = error.to_string();
    assert!(
        message.contains("one data path per process"),
        "the registry's own message should reach the caller: {message}"
    );

    first.close().expect("a clean close");
    assert_eq!(
        active_engine_refs(),
        0,
        "closing the first object frees the engine"
    );
    assert_eq!(active_engine_path(), None);

    // And now the second one opens, which is the half that proves the failed
    // open left nothing behind.
    let (second, _) = namespace(tmp.path())
        .open(
            "other-tenant",
            OpenOptions {
                database: Some("mem".to_string()),
                ..OpenOptions::default()
            },
        )
        .expect("the engine is free");
    second.close().expect("a clean close");
}

#[test]
fn a_read_only_open_serves_what_was_committed_and_takes_no_lease() {
    let _engine = engine();
    let tmp = common::tempdir();

    let (object, _) = open(tmp.path());
    object
        .execute("CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n")
        .expect("DDL");
    object.execute("INSERT INTO t VALUES (1)").expect("a write");
    object.flush().expect("a flush");
    object
        .execute("INSERT INTO t VALUES (2)")
        .expect("a write nobody else will see yet");
    object
        .close()
        .expect("a clean close, which flushes the rest");

    let (reader, _) = namespace(tmp.path())
        .open(
            "tenant",
            OpenOptions {
                read_only: true,
                ..OpenOptions::default()
            },
        )
        .expect("a reader");
    assert_eq!(
        text(
            reader
                .query("SELECT count() FROM t", OutputFormat::CSV)
                .expect("a read")
        ),
        "2",
        "close published what execute had only buffered"
    );
    assert!(reader
        .execute("INSERT INTO t VALUES (3)")
        .is_err_and(|e| e.category() == Category::ClassificationRefused));
    reader.close().expect("a clean close");
}

#[test]
fn a_missing_object_is_not_found_rather_than_created() {
    let _engine = engine();
    let tmp = common::tempdir();

    let error = namespace(tmp.path())
        .open(
            "never-written",
            OpenOptions {
                read_only: true,
                ..OpenOptions::default()
            },
        )
        .expect_err("a reader never creates the object it came to read");
    assert_eq!(error.category(), Category::NotFound);
    assert_eq!(active_engine_refs(), 0, "and it started no engine");
}
