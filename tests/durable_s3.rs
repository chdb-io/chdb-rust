//! Durable objects against a real S3-compatible bucket.
//!
//! Everything here is skipped unless `CHDB_DURABLE_S3_BUCKET` names one, so a
//! machine without credentials still runs the rest of the suite. Point it at a
//! bucket you own:
//!
//! ```sh
//! eval "$(aws configure export-credentials --profile my-profile --format env)"
//! export CHDB_DURABLE_S3_BUCKET=my-bucket
//! export CHDB_DURABLE_S3_REGION=eu-central-1
//! cargo test --features durable-s3 --test durable_s3 -- --test-threads=1
//! ```
//!
//! `CHDB_DURABLE_S3_ENDPOINT` points the same tests at MinIO or R2.
//!
//! These are the tests the local backend cannot stand in for. Its conditional
//! operations are an honest implementation of the same contract, but only a
//! real provider can settle whether `If-None-Match: *` and `If-Match: <etag>`
//! behave the way the protocol assumes — and that assumption is what the whole
//! single-writer guarantee rests on.
//!
//! V1 has no destroy and no garbage collection, so a run leaves its objects
//! behind under a unique prefix. Give the test bucket a lifecycle rule, or
//! delete the prefix afterwards.

use std::sync::MutexGuard;
use std::time::Duration;

use chdb_rust::durable::{
    Backend, Category, Namespace, OpenOptions, PutOutcome, ReplaceOutcome, S3Backend, S3Options,
    Tuning, HEAD_KEY,
};
use chdb_rust::format::OutputFormat;

/// The bucket to run against, or `None` when this machine has none.
fn bucket() -> Option<String> {
    std::env::var("CHDB_DURABLE_S3_BUCKET")
        .ok()
        .filter(|bucket| !bucket.is_empty())
}

fn region() -> Option<String> {
    std::env::var("CHDB_DURABLE_S3_REGION")
        .ok()
        .filter(|region| !region.is_empty())
}

fn endpoint() -> Option<String> {
    std::env::var("CHDB_DURABLE_S3_ENDPOINT")
        .ok()
        .filter(|endpoint| !endpoint.is_empty())
}

/// A prefix nothing else is using: one per run, one per test.
///
/// Unique rather than cleaned up, because V1 has no delete — two runs sharing a
/// prefix would have the second one restoring the first one's database and
/// reporting a failure that is really a collision.
fn prefix(test: &str) -> String {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_micros())
        .unwrap_or_default();
    format!("chdb-rust-test/{stamp}-{test}")
}

/// Takes the process engine for one test, as the engine tests do: chdb-core
/// binds one data path per process.
fn engine() -> MutexGuard<'static, ()> {
    static ENGINE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    ENGINE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A backend straight onto one prefix, for the tests that exercise the two
/// conditional operations without an engine in the way.
fn backend(prefix: &str) -> Option<S3Backend> {
    let bucket = bucket()?;
    Some(
        S3Backend::new(S3Options {
            bucket,
            prefix: prefix.to_string(),
            region: region(),
            endpoint: endpoint(),
            timeout: Some(Duration::from_secs(60)),
            ..S3Options::default()
        })
        .expect("an S3 backend"),
    )
}

fn namespace(prefix: &str) -> Option<Namespace> {
    let bucket = bucket()?;
    let mut url = format!("s3://{bucket}/{prefix}");
    let mut query = Vec::new();
    if let Some(region) = region() {
        query.push(format!("region={region}"));
    }
    if let Some(endpoint) = endpoint() {
        query.push(format!("endpoint={endpoint}"));
    }
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query.join("&"));
    }
    Some(
        Namespace::new(&url)
            .expect("an s3 namespace")
            .with_owner("s3-test")
            .with_tuning(Tuning {
                lease_ttl: Duration::from_secs(30),
                heartbeat_interval: Duration::from_secs(10),
                ..Tuning::default()
            }),
    )
}

/// Prints why a test did nothing, so a skipped run does not read as a passing
/// one.
macro_rules! require_bucket {
    ($value:expr) => {
        match $value {
            Some(value) => value,
            None => {
                eprintln!("skipped: set CHDB_DURABLE_S3_BUCKET to run the S3 tests");
                return;
            }
        }
    };
}

#[test]
fn a_conditional_create_lets_exactly_one_writer_win() {
    let backend = require_bucket!(backend(&prefix("create")));

    assert_eq!(
        backend
            .put_bytes_if_absent(HEAD_KEY, b"first")
            .expect("a create"),
        PutOutcome::Created
    );
    assert_eq!(
        backend
            .put_bytes_if_absent(HEAD_KEY, b"second")
            .expect("a second create"),
        PutOutcome::AlreadyExists,
        "If-None-Match: * is what makes this a race nobody can both win"
    );

    let tagged = backend
        .get_bytes_with_etag(HEAD_KEY)
        .expect("a read")
        .expect("the object");
    assert_eq!(
        tagged.data, b"first",
        "the loser never overwrote the winner"
    );
    assert!(!tagged.etag.is_empty(), "a CAS token has to come back");
}

#[test]
fn a_compare_and_swap_refuses_a_stale_token() {
    let backend = require_bucket!(backend(&prefix("cas")));

    backend
        .put_bytes_if_absent(HEAD_KEY, b"generation-1")
        .expect("a create");
    let first = backend
        .get_bytes_with_etag(HEAD_KEY)
        .expect("a read")
        .expect("the object");

    let second = match backend
        .replace_if_match(HEAD_KEY, b"generation-2", &first.etag)
        .expect("a replace")
    {
        ReplaceOutcome::Done { etag } => etag,
        other => panic!("expected the first replace to apply, got {other:?}"),
    };
    assert_ne!(second, first.etag, "a committed write moves the token");

    // The token the first reader held describes a version that is gone. This is
    // the fence: a superseded writer's next commit cannot land.
    assert_eq!(
        backend
            .replace_if_match(HEAD_KEY, b"generation-3", &first.etag)
            .expect("a replace"),
        ReplaceOutcome::NotMatched
    );
    assert!(
        matches!(
            backend
                .replace_if_match(HEAD_KEY, b"generation-3", &second)
                .expect("a replace"),
            ReplaceOutcome::Done { .. }
        ),
        "the current token still works"
    );

    assert_eq!(
        backend
            .get_bytes(HEAD_KEY)
            .expect("a read")
            .expect("the object"),
        b"generation-3"
    );
}

#[test]
fn an_absent_key_is_an_answer_rather_than_a_failure() {
    let backend = require_bucket!(backend(&prefix("absent")));
    assert!(backend.get_bytes(HEAD_KEY).expect("a read").is_none());
    assert!(backend
        .get_bytes_with_etag(HEAD_KEY)
        .expect("a read")
        .is_none());
    assert!(backend.open_reader(HEAD_KEY).expect("an open").is_none());
}

#[test]
fn a_file_is_uploaded_whole_and_streams_back() {
    use std::io::Read as _;

    let backend = require_bucket!(backend(&prefix("file")));
    let tmp = std::env::temp_dir().join(format!("chdb-s3-{}.bin", std::process::id()));
    // Larger than one buffer, so this exercises the streaming path rather than
    // a single write.
    let body: Vec<u8> = (0..(3 * 1024 * 1024u32)).map(|n| n as u8).collect();
    std::fs::write(&tmp, &body).expect("a local archive");

    let digest = chdb_rust::durable::Digest::of(&body);

    let key = "checkpoints/1-1-aaaaaaaa.tar.gz";
    assert_eq!(
        backend
            .put_file_if_absent(key, &tmp, &digest)
            .expect("an upload"),
        PutOutcome::Created
    );
    let mut reader = backend
        .open_reader(key)
        .expect("an open")
        .expect("the archive");
    let mut back = Vec::new();
    reader.read_to_end(&mut back).expect("a download");
    assert_eq!(back.len(), body.len());
    assert_eq!(back, body, "an archive has to come back byte for byte");

    std::fs::remove_file(&tmp).ok();
}

#[test]
fn a_database_survives_in_a_bucket() {
    let _engine = engine();
    let prefix = prefix("roundtrip");
    let namespace = require_bucket!(namespace(&prefix));

    let (object, existed) = namespace
        .open(
            "tenant",
            OpenOptions {
                database: Some("mem".to_string()),
                ..OpenOptions::default()
            },
        )
        .expect("a writer");
    assert!(!existed, "a fresh prefix holds nothing");

    object
        .execute("CREATE TABLE events (id UInt64, tag String) ENGINE = MergeTree ORDER BY id")
        .expect("DDL");
    object
        .execute("INSERT INTO events VALUES (1, 'first')")
        .expect("a write");
    let ticket = object
        .execute("INSERT INTO events VALUES (2, 'second')")
        .expect("a write");
    object.flush_through(ticket).expect("a durability barrier");
    object.close().expect("a clean close");

    // A different handle, a different scratch directory, the same bucket.
    let (object, existed) = namespace
        .open("tenant", OpenOptions::default())
        .expect("a second writer");
    assert!(existed);
    let rows = object
        .query("SELECT tag FROM events ORDER BY id", OutputFormat::CSV)
        .expect("a read");
    assert_eq!(
        String::from_utf8_lossy(&rows).trim(),
        "\"first\"\n\"second\"",
        "the WAL replayed out of S3"
    );

    // And a checkpoint, which is the path that uploads a file rather than a
    // buffer.
    let base = object.checkpoint().expect("a checkpoint");
    assert!(base.key.starts_with("checkpoints/"), "{}", base.key);
    assert!(object.manifest().wal.is_empty());
    object.close().expect("a clean close");

    let (object, _) = namespace
        .open("tenant", OpenOptions::default())
        .expect("a third writer");
    let rows = object
        .query("SELECT count() FROM events", OutputFormat::CSV)
        .expect("a read");
    assert_eq!(
        String::from_utf8_lossy(&rows).trim(),
        "2",
        "restored from the checkpoint alone"
    );
    object.close().expect("a clean close");
}

#[test]
fn a_second_writer_finds_the_lease_held() {
    let _engine = engine();
    let prefix = prefix("lease");
    let namespace = require_bucket!(namespace(&prefix));

    let (held, _) = namespace
        .open(
            "tenant",
            OpenOptions {
                database: Some("mem".to_string()),
                ..OpenOptions::default()
            },
        )
        .expect("the first writer");

    // The second open is refused while taking the lease, before it ever starts
    // an engine — which is also why this does not collide with the one data
    // path the first writer holds.
    let error = namespace
        .open("tenant", OpenOptions::default())
        .expect_err("the lease is someone else's");
    assert_eq!(error.category(), Category::LeaseHeld);
    assert_eq!(error.owner(), Some("s3-test"));

    held.close().expect("a clean close");

    // Released on close, so the next writer is free to take it.
    let (next, _) = namespace
        .open("tenant", OpenOptions::default())
        .expect("the lease was given back");
    assert_eq!(next.generation(), 2, "every takeover moves the fence");
    next.close().expect("a clean close");
}

#[test]
fn a_reader_sees_what_a_writer_committed_and_takes_no_lease() {
    let _engine = engine();
    let prefix = prefix("reader");
    let namespace = require_bucket!(namespace(&prefix));

    let (writer, _) = namespace
        .open(
            "tenant",
            OpenOptions {
                database: Some("mem".to_string()),
                ..OpenOptions::default()
            },
        )
        .expect("a writer");
    writer
        .execute("CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n")
        .expect("DDL");
    writer.execute("INSERT INTO t VALUES (7)").expect("a write");
    writer.close().expect("a clean close");

    let (reader, existed) = namespace
        .open(
            "tenant",
            OpenOptions {
                read_only: true,
                ..OpenOptions::default()
            },
        )
        .expect("a reader");
    assert!(existed);
    let rows = reader
        .query("SELECT n FROM t", OutputFormat::CSV)
        .expect("a read");
    assert_eq!(String::from_utf8_lossy(&rows).trim(), "7");
    assert!(reader
        .execute("INSERT INTO t VALUES (8)")
        .is_err_and(|e| e.category() == Category::ClassificationRefused));
    reader.close().expect("a clean close");
}

#[test]
fn a_read_only_open_of_a_prefix_nobody_has_written_is_not_found() {
    let _engine = engine();
    let namespace = require_bucket!(namespace(&prefix("missing")));
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
}
