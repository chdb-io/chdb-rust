//! What the process-wide engine record says, and when it refuses.
//!
//! These tests need the engine to themselves, so each leaves it idle and the
//! harness runs them one at a time.

use chdb_rust::connection::Connection;
use chdb_rust::error::Error;
use chdb_rust::session::SessionBuilder;
use chdb_rust::{active_engine_path, active_engine_refs};

mod common;

#[test]
fn the_record_counts_every_open_handle() {
    assert_eq!(
        active_engine_refs(),
        0,
        "a previous test left a handle open"
    );
    assert_eq!(active_engine_path(), None);

    let dir = common::tempdir();
    let first = SessionBuilder::new()
        .with_data_path(dir.path())
        .build()
        .expect("first session");
    assert_eq!(active_engine_refs(), 1);
    let bound = active_engine_path().expect("a path once something is open");

    let second = SessionBuilder::new()
        .with_data_path(dir.path())
        .build()
        .expect("a second session on the same path");
    assert_eq!(active_engine_refs(), 2);
    assert_eq!(active_engine_path().as_deref(), Some(bound.as_str()));

    drop(second);
    assert_eq!(active_engine_refs(), 1);
    assert_eq!(
        active_engine_path().as_deref(),
        Some(bound.as_str()),
        "the engine stays bound while a handle remains"
    );

    drop(first);
    assert_eq!(active_engine_refs(), 0);
    assert_eq!(
        active_engine_path(),
        None,
        "the last handle releases the path"
    );
}

#[test]
fn a_second_data_path_is_refused_by_name() {
    assert_eq!(active_engine_refs(), 0);
    let held = common::tempdir();
    let other = common::tempdir();

    let session = SessionBuilder::new()
        .with_data_path(held.path())
        .build()
        .expect("session");
    let bound = active_engine_path().expect("bound");

    let refused = SessionBuilder::new()
        .with_data_path(other.path())
        .build()
        .expect_err("a second data path cannot be opened");

    match &refused {
        Error::PathConflict { active, requested } => {
            assert_eq!(active, &bound);
            assert!(
                requested.ends_with(
                    other
                        .path()
                        .file_name()
                        .and_then(|name| name.to_str())
                        .expect("temp dir name")
                ),
                "the refusal should name what was asked for, got {requested}"
            );
        }
        other => panic!("expected PathConflict, got {other}"),
    }

    // The reason has to survive being turned into a message.
    let message = refused.to_string();
    assert!(message.contains(&bound), "{message}");

    drop(session);
    assert_eq!(active_engine_refs(), 0);
}

/// An in-memory connection is a data path of its own, so it cannot join a
/// session on disk. That is what makes `execute` refuse while one is open.
#[test]
fn an_in_memory_connection_is_a_data_path() {
    assert_eq!(active_engine_refs(), 0);
    let dir = common::tempdir();
    let session = SessionBuilder::new()
        .with_data_path(dir.path())
        .build()
        .expect("session");

    for refused in [
        Connection::open_in_memory().expect_err("connection refused"),
        chdb_rust::execute("SELECT 1", None).expect_err("execute refused"),
    ] {
        match refused {
            Error::PathConflict { requested, .. } => assert_eq!(requested, ":memory:"),
            other => panic!("expected PathConflict, got {other}"),
        }
    }

    drop(session);

    // And the other way round, once the engine is idle.
    let memory = Connection::open_in_memory().expect("in-memory once idle");
    assert_eq!(active_engine_path().as_deref(), Some(":memory:"));
    let refused = SessionBuilder::new()
        .with_data_path(dir.path())
        .build()
        .expect_err("on-disk cannot join an in-memory engine");
    assert!(matches!(refused, Error::PathConflict { .. }));
    drop(memory);
    assert_eq!(active_engine_refs(), 0);
}

#[test]
fn the_next_path_is_free_once_the_last_handle_goes() {
    assert_eq!(active_engine_refs(), 0);
    let first = common::tempdir();
    let second = common::tempdir();

    let session = SessionBuilder::new()
        .with_data_path(first.path())
        .build()
        .expect("first path");
    drop(session);

    let session = SessionBuilder::new()
        .with_data_path(second.path())
        .build()
        .expect("a different path once the engine is idle");
    assert_eq!(
        session
            .execute("SELECT 1", None)
            .expect("query")
            .data_utf8_lossy()
            .trim(),
        "1"
    );
    drop(session);
    assert_eq!(active_engine_refs(), 0);
}

/// Two spellings of one directory converge before the engine sees them, so the
/// second session attaches instead of being refused.
#[test]
fn one_directory_named_two_ways_shares_the_engine() {
    assert_eq!(active_engine_refs(), 0);
    let dir = common::tempdir();
    let plain = dir.path().to_path_buf();
    let trailing_slash = format!("{}/", plain.display());

    let first = SessionBuilder::new()
        .with_data_path(&plain)
        .build()
        .expect("first spelling");
    let second = SessionBuilder::new()
        .with_data_path(&trailing_slash)
        .build()
        .expect("a trailing slash names the same directory");
    assert_eq!(active_engine_refs(), 2);

    // One database, not two that coexist.
    first
        .execute(
            "CREATE TABLE shared (x UInt32) ENGINE = MergeTree ORDER BY x",
            None,
        )
        .expect("create");
    first
        .execute("INSERT INTO shared VALUES (1)", None)
        .expect("insert");
    assert_eq!(
        second
            .execute("SELECT count() FROM shared", None)
            .expect("read back")
            .data_utf8_lossy()
            .trim(),
        "1"
    );

    drop(second);
    drop(first);
    assert_eq!(active_engine_refs(), 0);
}

/// A session with auto-cleanup removes its directory only as the last handle,
/// so a sibling's data survives.
#[test]
fn auto_cleanup_leaves_a_shared_path_alone() {
    assert_eq!(active_engine_refs(), 0);
    let dir = common::tempdir();
    let path = dir.path().to_path_buf();

    let keeper = SessionBuilder::new()
        .with_data_path(&path)
        .build()
        .expect("keeper");
    keeper
        .execute(
            "CREATE TABLE kept (x UInt32) ENGINE = MergeTree ORDER BY x",
            None,
        )
        .expect("create");
    keeper
        .execute("INSERT INTO kept VALUES (1)", None)
        .expect("insert");

    {
        let transient = SessionBuilder::new()
            .with_data_path(&path)
            .with_auto_cleanup(true)
            .build()
            .expect("transient");
        assert_eq!(active_engine_refs(), 2);
        assert_eq!(
            transient
                .execute("SELECT count() FROM kept", None)
                .expect("shared read")
                .data_utf8_lossy()
                .trim(),
            "1"
        );
    }

    assert!(
        keeper.path().exists(),
        "a session that is not the last handle must not remove the directory"
    );
    assert_eq!(
        keeper
            .execute("SELECT count() FROM kept", None)
            .expect("still readable")
            .data_utf8_lossy()
            .trim(),
        "1"
    );

    // And the last handle, which does carry the flag, does remove it.
    let path_again = keeper.path().to_path_buf();
    drop(keeper);
    assert_eq!(active_engine_refs(), 0);
    assert!(
        path_again.exists(),
        "the last handle here had no flag, so nothing should have been removed"
    );

    let solo = SessionBuilder::new()
        .with_data_path(&path_again)
        .with_auto_cleanup(true)
        .build()
        .expect("solo");
    drop(solo);
    assert!(
        !path_again.exists(),
        "the only handle carried the flag, so the directory should be gone"
    );
}

/// `cleanup` ignores the flag but still yields to siblings.
#[test]
fn cleanup_is_explicit_and_still_yields_to_siblings() {
    assert_eq!(active_engine_refs(), 0);
    let dir = common::tempdir();
    let path = dir.path().to_path_buf();

    let keeper = SessionBuilder::new()
        .with_data_path(&path)
        .build()
        .expect("keeper");
    let other = SessionBuilder::new()
        .with_data_path(&path)
        .build()
        .expect("other");

    other.cleanup().expect("cleanup with a sibling open");
    assert!(
        path.exists(),
        "cleanup must not delete from under a sibling"
    );
    assert_eq!(active_engine_refs(), 1);

    keeper.cleanup().expect("cleanup as the last handle");
    assert!(
        !path.exists(),
        "the last handle's cleanup removes the directory"
    );
    assert_eq!(active_engine_refs(), 0);
}

/// Several sessions on one path, querying at the same time, each owned by the
/// thread using it.
#[test]
fn sessions_on_one_path_query_from_several_threads() {
    assert_eq!(active_engine_refs(), 0);
    let dir = common::tempdir();
    let path = dir.path().to_path_buf();

    let writer = SessionBuilder::new()
        .with_data_path(&path)
        .build()
        .expect("writer");
    writer
        .execute(
            "CREATE TABLE t (x UInt32) ENGINE = MergeTree ORDER BY x",
            None,
        )
        .expect("create");
    writer
        .execute("INSERT INTO t SELECT number FROM numbers(1000)", None)
        .expect("insert");

    let readers: Vec<_> = (0..4)
        .map(|id| {
            let path = path.clone();
            std::thread::spawn(move || -> Result<String, String> {
                let session = SessionBuilder::new()
                    .with_data_path(&path)
                    .build()
                    .map_err(|e| format!("thread {id} could not attach: {e}"))?;
                let mut last = String::new();
                for _ in 0..5 {
                    last = session
                        .execute("SELECT count(), sum(x) FROM t", None)
                        .map_err(|e| format!("thread {id} query: {e}"))?
                        .data_utf8_lossy()
                        .trim()
                        .to_string();
                }
                Ok(last)
            })
        })
        .collect();

    for reader in readers {
        let answer = reader.join().expect("thread").expect("query");
        assert_eq!(answer, "1000\t499500");
    }

    assert_eq!(
        active_engine_refs(),
        1,
        "every reader should have released its handle"
    );
    drop(writer);
    assert_eq!(active_engine_refs(), 0);
}
