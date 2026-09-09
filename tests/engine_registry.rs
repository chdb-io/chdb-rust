//! What the process-wide engine record says, and when it refuses.
//!
//! These tests need the engine to themselves, so each takes [`engine`] first
//! and leaves the engine idle afterwards.

use chdb_rust::connection::Connection;
use chdb_rust::error::Error;
use chdb_rust::session::SessionBuilder;
use chdb_rust::{active_engine_path, active_engine_refs};

mod common;

/// Takes the engine for one test.
///
/// The engine is process-wide, so these tests cannot overlap: each asserts it
/// starts from an idle engine. Serializing here rather than relying on
/// `--test-threads=1` means a plain `cargo test` reports real failures instead
/// of collisions.
fn engine() -> std::sync::MutexGuard<'static, ()> {
    static ENGINE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    ENGINE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[test]
fn the_record_counts_every_open_handle() {
    let _engine = engine();
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
    let _engine = engine();
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
    let _engine = engine();
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
    let _engine = engine();
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
    let _engine = engine();
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
    let _engine = engine();
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
    let _engine = engine();
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

    other.cleanup();
    assert!(
        path.exists(),
        "cleanup must not delete from under a sibling"
    );
    assert_eq!(active_engine_refs(), 1);

    keeper.cleanup();
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
    let _engine = engine();
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

/// A symlinked data path is not followed when the directory is removed: the
/// link goes, the directory it pointed at stays.
#[test]
fn auto_cleanup_does_not_follow_a_symlink() {
    let _engine = engine();
    assert_eq!(active_engine_refs(), 0);
    let real = common::tempdir();
    let target = real.path().join("target");
    std::fs::create_dir(&target).expect("target");
    std::fs::write(target.join("keep-me"), b"x").expect("marker");

    let link = real.path().join("link");
    std::os::unix::fs::symlink(&target, &link).expect("symlink");

    let session = SessionBuilder::new()
        .with_data_path(&link)
        .with_auto_cleanup(true)
        .build()
        .expect("session on a symlink");
    drop(session);

    assert!(
        target.join("keep-me").exists(),
        "the directory the link pointed at must survive"
    );
}

/// Two sessions releasing at the same time: exactly one of them is last, and
/// the directory goes with it.
#[test]
fn concurrent_drops_still_remove_the_directory() {
    let _engine = engine();
    assert_eq!(active_engine_refs(), 0);
    let dir = common::tempdir();
    let path = dir.path().to_path_buf();

    let mut threads = Vec::new();
    for _ in 0..2 {
        let path = path.clone();
        threads.push(std::thread::spawn(move || {
            let session = SessionBuilder::new()
                .with_data_path(&path)
                .with_auto_cleanup(true)
                .build()
                .expect("session");
            session.execute("SELECT 1", None).expect("query");
            // Both release at roughly the same moment, which is the point.
            drop(session);
        }));
    }
    for thread in threads {
        thread.join().expect("thread");
    }

    assert_eq!(active_engine_refs(), 0);
    assert!(
        !path.exists(),
        "the last handle to go should have removed the directory"
    );
}

/// Cleanup cannot delete the directory out from under a connection that
/// attaches while it is releasing.
#[test]
fn cleanup_does_not_race_a_new_connection() {
    let _engine = engine();
    assert_eq!(active_engine_refs(), 0);
    let dir = common::tempdir();
    let path = dir.path().to_path_buf();

    let holder = SessionBuilder::new()
        .with_data_path(&path)
        .build()
        .expect("holder");
    let leaving = SessionBuilder::new()
        .with_data_path(&path)
        .build()
        .expect("leaving");

    leaving.cleanup();

    assert!(path.exists(), "a sibling was still holding the path");
    assert_eq!(
        holder
            .execute("SELECT 1", None)
            .expect("still usable")
            .data_utf8_lossy()
            .trim(),
        "1"
    );
    drop(holder);
    assert_eq!(active_engine_refs(), 0);
}
