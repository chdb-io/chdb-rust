//! The runtime surfaces a duplicated C++ runtime breaks.
//!
//! Static linking puts a copy of the C++ standard library inside the artifact.
//! If its symbols are visible, the loader may resolve a call made inside the
//! engine to a different implementation — the system's libc++ — and then one
//! copy constructs an object the other destroys. Nothing about that is visible
//! at link time, and `SELECT 1` passes straight through it: the failure needs a
//! query that touches the state the two copies disagree about.
//!
//! Four groups, ten cases. Each group is a different piece of shared state:
//! standard library globals (locale, the stream objects, the filesystem
//! character conversions), type identity (the `typeinfo` addresses that decide
//! whether a `catch` matches), the allocator and thread-local storage (which
//! side of the language boundary allocated the memory being freed), and process
//! exit (globals are destroyed when no query is running and there is nowhere to
//! report an error).
//!
//! # Why each case is its own process
//!
//! The four things that have to be checked are properties of a process, not of
//! an assertion: the exit code must be 0, stdout must match exactly, a
//! wall-clock limit must be enforced, and stderr must reach the log. A hang is
//! the central failure mode here, and one hung case inside a shared test binary
//! takes every other case with it — so the parent runs each case as a child,
//! bounds it, and compares what it printed.
//!
//! Run one case directly with `cargo test --test runtime_faces -- <name>`.
//! Unrecognised flags are ignored, so the runner tolerates being handed
//! `--test-threads=1` by CI.

use std::fmt::Write as _;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

mod common;

use chdb_rust::connection::Connection;
use chdb_rust::error::Error;
use chdb_rust::format::OutputFormat;
use chdb_rust::session::SessionBuilder;

/// Names the case a child process should run. Its presence is what tells a
/// process it is a child.
const CASE_ENV: &str = "CHDB_RUNTIME_CASE";

/// Per-case wall-clock limit. Generous on purpose: this bounds a hang, it does
/// not measure performance, and a loaded CI runner starting a several-hundred
/// megabyte binary is slow for reasons that are not interesting here.
const CASE_LIMIT: Duration = Duration::from_secs(60);

struct Case {
    name: &'static str,
    /// What the case must print, byte for byte.
    expect: &'static str,
    run: fn(),
}

fn cases() -> Vec<Case> {
    let mut cases = vec![
        // --- Group 1: globals in the standard library -----------------------
        // The locale and the iostream globals must exist once per process. Two
        // copies means state set in one is invisible to the other, and the
        // symptom is output that is subtly wrong rather than absent.
        Case {
            name: "locale_formatting",
            expect: "1.23 million|117.74 MiB|-1.5|1234.5678\n",
            run: locale_formatting,
        },
        Case {
            name: "wide_character_output",
            expect: "日本語テキスト|3|éfac|Ελλ\n",
            run: wide_character_output,
        },
        // std::filesystem and the character conversions it goes through.
        Case {
            name: "non_ascii_path",
            expect: "created|3|6\n",
            run: non_ascii_path,
        },
        // Regular expressions consult the locale internally.
        Case {
            name: "regexp_functions",
            expect: "1|a·b·c|1,2,3\n",
            run: regexp_functions,
        },
        // --- Group 2: type identity -----------------------------------------
        // C++ decides type identity by comparing typeinfo addresses. Split
        // those and `catch` stops matching: an engine exception passes through
        // every handler and terminates the process.
        Case {
            name: "caught_engine_errors",
            expect:
                "syntax=Code: 62|divide=Code: 153|mismatch=Code: 43|rethrown=Code: 62|alive=1\n",
            run: caught_engine_errors,
        },
        // A path dense with virtual dispatch: a table written to disk and read
        // back through the storage layer.
        Case {
            name: "session_disk_roundtrip",
            expect: "rows=50000|sum=1249975000|parts>=1\n",
            run: session_disk_roundtrip,
        },
        // --- Group 3: the allocator and thread-local storage ----------------
        // The engine allocates the result buffer and we free it. If operator
        // new and operator delete are not the same copy, this aborts.
        Case {
            name: "large_result_buffer",
            expect: "bytes=1288890|lines=200000|last=199999\n",
            run: large_result_buffer,
        },
        Case {
            name: "concurrent_connections",
            expect: "0=1000|1=1000|2=1000|3=1000\n",
            run: concurrent_connections,
        },
        // --- Group 4: process exit ------------------------------------------
        // Everything above happens while a query is running. Globals are
        // destroyed after the last one returns, where there is no query to fail
        // and nowhere to report an error: the symptom is a process that will not
        // exit, or exits non-zero having printed every correct answer.
        Case {
            name: "exit_after_close",
            expect: "first=1|second=2\n",
            run: exit_after_close,
        },
    ];

    // The release callback in the Arrow C Data Interface frees memory that was
    // allocated on the other side of the boundary, which is the same hazard in
    // the opposite direction.
    #[cfg(feature = "arrow")]
    cases.push(Case {
        name: "arrow_release_callback",
        expect: "inserted=4|sum=100|released\n",
        run: arrow_release_callback,
    });

    cases
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

/// One row, CSV, pipe-joined, with no trailing separator.
fn scalar_row(sql: &str) -> String {
    let conn = Connection::open_in_memory().expect("connect");
    let result = conn.query(sql, OutputFormat::TabSeparated).expect(sql);
    let text = result.data_utf8().expect("utf-8");
    text.trim_end_matches('\n').replace('\t', "|")
}

fn locale_formatting() {
    // Numbers through the engine's formatting paths: a word scale, a binary
    // scale, a negative float and a fixed-point decimal.
    println!(
        "{}",
        scalar_row(
            "SELECT formatReadableQuantity(1234567), \
                    formatReadableSize(123456789), \
                    toString(-1.5), \
                    toString(toDecimal64(1234.5678, 4))"
        )
    );
}

fn wide_character_output() {
    // Text that comes out wrong in a recognisable way if the character tables
    // or the stream globals are duplicated: literal multi-byte output, a
    // codepoint count over astral characters, and two operations that have to
    // respect codepoint boundaries rather than byte boundaries.
    //
    // No case mapping here: upperUTF8 and lowerUTF8 need ICU and this engine is
    // built without it (`Function with name `upperUTF8` does not exist`).
    println!(
        "{}",
        scalar_row(concat!(
            "SELECT '日本語テキスト', ",
            "lengthUTF8('🦀🦀🦀'), ",
            "reverseUTF8('café'), ",
            "substringUTF8('Ελληνικά', 1, 3)"
        ))
    );
}

fn non_ascii_path() {
    // A data directory the engine has to hand to std::filesystem and convert.
    let dir = scratch_dir("数据-café-Ω");
    let session = SessionBuilder::new()
        .with_data_path(&dir)
        .with_auto_cleanup(true)
        .build()
        .expect("session on a non-ASCII path");

    session
        .execute("CREATE DATABASE IF NOT EXISTS d", None)
        .expect("create database");
    session
        .execute(
            "CREATE TABLE IF NOT EXISTS d.t (x UInt32) ENGINE = MergeTree ORDER BY x",
            None,
        )
        .expect("create table");
    session
        .execute("INSERT INTO d.t VALUES (1), (2), (3)", None)
        .expect("insert");

    let created = if dir.exists() { "created" } else { "missing" };
    let counted = session
        .execute("SELECT count(), sum(x) FROM d.t", None)
        .expect("read back")
        .data_utf8()
        .expect("utf-8")
        .trim_end_matches('\n')
        .replace('\t', "|");

    println!("{created}|{counted}");
}

fn regexp_functions() {
    // concat! rather than a continued string: inside a raw string a backslash
    // before a newline is just a backslash, and it lands in the SQL.
    println!(
        "{}",
        scalar_row(concat!(
            r"SELECT match('café-2026', '^caf.-\\d{4}$'), ",
            r"replaceRegexpAll('aXbXc', 'X', '·'), ",
            r"arrayStringConcat(extractAll('日本1語2テキスト3', '\\d'), ',')"
        ))
    );
}

/// The leading `Code: N` of an engine error, without the message or the version
/// that follows it — those move between releases, the code does not.
fn error_code(error: &Error) -> String {
    let text = error.to_string();
    match text.find('.') {
        Some(end) if text.starts_with("Code: ") => text[..end].to_string(),
        _ => format!("unexpected error shape: {text}"),
    }
}

/// Turns an engine error into a different variant of our own, from a frame the
/// caller has to unwind through.
fn wrap_engine_error(conn: &Connection) -> Result<(), Error> {
    conn.query("SELECT FROM WHERE", OutputFormat::TabSeparated)
        .map(|_| ())
        .map_err(|e| Error::InvalidData(e.to_string()))
}

fn caught_engine_errors() {
    let conn = Connection::open_in_memory().expect("connect");

    let mut line = String::new();
    for (label, sql) in [
        ("syntax", "SELECT FROM WHERE"),
        ("divide", "SELECT intDiv(1, 0)"),
        ("mismatch", "SELECT [1, 2, 3] + 1"),
    ] {
        let error = conn
            .query(sql, OutputFormat::TabSeparated)
            .expect_err(sql)
            .to_string();
        let code = error_code(&Error::QueryError(error));
        write!(line, "{label}={code}|").expect("write");
    }

    // One error caught, wrapped and propagated out of another frame, so an
    // exception has to survive being handled rather than only being thrown.
    let rethrown = wrap_engine_error(&conn).expect_err("rethrow");
    let inner = match &rethrown {
        Error::InvalidData(text) => error_code(&Error::QueryError(text.clone())),
        other => format!("unexpected: {other}"),
    };
    write!(line, "rethrown={inner}|").expect("write");

    // The connection has to still work after all of that.
    let alive = scalar_row_on(&conn, "SELECT 1");
    println!("{line}alive={alive}");
}

fn scalar_row_on(conn: &Connection, sql: &str) -> String {
    conn.query(sql, OutputFormat::TabSeparated)
        .expect(sql)
        .data_utf8()
        .expect("utf-8")
        .trim_end_matches('\n')
        .replace('\t', "|")
}

fn session_disk_roundtrip() {
    let dir = scratch_dir("disk-roundtrip");
    let session = SessionBuilder::new()
        .with_data_path(&dir)
        .with_auto_cleanup(true)
        .build()
        .expect("session");

    session
        .execute("CREATE DATABASE IF NOT EXISTS d", None)
        .expect("create database");
    session
        .execute(
            "CREATE TABLE d.t (x UInt32, s String) ENGINE = MergeTree ORDER BY x",
            None,
        )
        .expect("create table");
    session
        .execute(
            "INSERT INTO d.t SELECT number, toString(number) FROM numbers(50000)",
            None,
        )
        .expect("insert");

    let counted = session
        .execute("SELECT count(), sum(x) FROM d.t", None)
        .expect("aggregate")
        .data_utf8()
        .expect("utf-8")
        .trim_end_matches('\n')
        .replace('\t', "|");

    // Written through the storage layer rather than held in memory.
    let parts: u64 = session
        .execute(
            "SELECT count() FROM system.parts WHERE database = 'd' AND table = 't' AND active",
            None,
        )
        .expect("system.parts")
        .data_utf8()
        .expect("utf-8")
        .trim()
        .parse()
        .expect("part count");

    let (rows, sum) = counted.split_once('|').expect("two columns");
    println!(
        "rows={rows}|sum={sum}|parts{}",
        if parts >= 1 { ">=1" } else { "=0" }
    );
}

fn large_result_buffer() {
    // Megabytes of result, allocated by the engine and freed by us.
    //
    // 1288890 bytes is the whole answer, not an observation: the decimal digits
    // of 0..199999 are 10*1 + 90*2 + 900*3 + 9000*4 + 90000*5 + 100000*6 =
    // 1088890, plus one newline per row.
    let conn = Connection::open_in_memory().expect("connect");
    let result = conn
        .query("SELECT number FROM numbers(200000)", OutputFormat::CSV)
        .expect("query");

    let bytes = result.data_ref().len();
    let text = result.data_utf8().expect("utf-8");
    let lines = text.lines().count();
    let last = text.lines().next_back().unwrap_or_default().to_string();

    drop(result);
    drop(conn);

    println!("bytes={bytes}|lines={lines}|last={last}");
}

fn concurrent_connections() {
    // A connection per thread, running at the same time. Thread-local storage
    // that exists twice gets its initialisation order wrong here.
    let handles: Vec<_> = (0..4)
        .map(|id| {
            std::thread::spawn(move || {
                let conn = Connection::open_in_memory().expect("connect");
                let counted = scalar_row_on(&conn, "SELECT count() FROM numbers(1000)");
                format!("{id}={counted}")
            })
        })
        .collect();

    let mut answers: Vec<String> = handles
        .into_iter()
        .map(|handle| handle.join().expect("thread"))
        .collect();
    answers.sort();
    println!("{}", answers.join("|"));
}

fn exit_after_close() {
    // Connect, query, close. Twice. Then return from main and let the globals
    // be destroyed with no query in flight.
    let mut line = String::new();
    for (label, sql) in [("first", "SELECT 1"), ("second", "SELECT 2")] {
        let conn = Connection::open_in_memory().expect("connect");
        let value = scalar_row_on(&conn, sql);
        drop(conn);
        if !line.is_empty() {
            line.push('|');
        }
        write!(line, "{label}={value}").expect("write");
    }
    println!("{line}");
}

#[cfg(feature = "arrow")]
fn arrow_release_callback() {
    use chdb_rust::arrow::array::{Int32Array, RecordBatch};
    use chdb_rust::arrow::datatypes::{DataType, Field, Schema};
    use chdb_rust::InsertOptions;
    use std::sync::Arc;

    let dir = scratch_dir("arrow-release");
    let session = SessionBuilder::new()
        .with_data_path(&dir)
        .with_auto_cleanup(true)
        .build()
        .expect("session");

    session
        .execute("CREATE DATABASE IF NOT EXISTS d", None)
        .expect("create database");
    session
        .execute(
            "CREATE TABLE d.t (x Int32) ENGINE = MergeTree ORDER BY x",
            None,
        )
        .expect("create table");

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![10, 20, 30, 40]))],
    )
    .expect("batch");

    // The batch crosses into the engine through the C Data Interface, and the
    // release callback the engine invokes frees memory this side allocated.
    let rows = batch.num_rows();
    chdb_rust::insert_record_batch(
        session.connection(),
        "d.t",
        "release_probe",
        batch,
        InsertOptions::default(),
    )
    .expect("insert");

    let sum = scalar_row_on(session.connection(), "SELECT sum(x) FROM d.t");
    println!("inserted={rows}|sum={sum}|released");
}

/// A scratch directory named `suffix`, removed if a previous run left one.
fn scratch_dir(suffix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("chdb-runtime-{}-{suffix}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

fn main() {
    match std::env::var(CASE_ENV) {
        Ok(name) => run_one(&name),
        Err(_) => run_all(),
    }
}

/// Child: run the named case and let its output and exit status be the answer.
fn run_one(name: &str) {
    let cases = cases();
    let case = cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("no case named {name}"));
    (case.run)();
    std::io::stdout().flush().expect("flush");
}

/// Parent: run every case as a child and check the four properties.
fn run_all() {
    // Anything starting with `-` is somebody else's flag — CI hands this binary
    // `--test-threads=1`. A bare word selects a subset by substring.
    let filter = std::env::args().skip(1).find(|arg| !arg.starts_with('-'));

    let exe = std::env::current_exe().expect("current_exe");
    let cases = cases();
    let selected: Vec<&Case> = cases
        .iter()
        .filter(|case| filter.as_deref().is_none_or(|f| case.name.contains(f)))
        .collect();

    println!("running {} runtime cases", selected.len());

    let mut failures = Vec::new();
    for case in &selected {
        match run_child(&exe, case) {
            Ok(elapsed) => println!("  {} ... ok ({} ms)", case.name, elapsed.as_millis()),
            Err(reason) => {
                println!("  {} ... FAILED", case.name);
                failures.push((case.name, reason));
            }
        }
    }

    if failures.is_empty() {
        println!("all {} runtime cases passed", selected.len());
        return;
    }

    println!("\nfailures:");
    for (name, reason) in &failures {
        println!("\n---- {name} ----\n{reason}");
    }
    std::process::exit(1);
}

/// Runs one case in a child process and checks its exit status, its stdout and
/// its wall-clock time.
fn run_child(exe: &Path, case: &Case) -> Result<Duration, String> {
    // stdout through a pipe drained by its own thread. A case that writes more
    // than the pipe holds cannot block against a parent that is not reading it
    // yet, and nothing here depends on a writable temp directory. stderr is
    // inherited: the diagnostics are there, and swallowing them is how a failure
    // becomes unexplainable.
    let mut command = common::command(exe);
    command
        .env(CASE_ENV, case.name)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    let mut child = command.spawn().map_err(|e| format!("cannot spawn: {e}"))?;

    let mut pipe = child.stdout.take().ok_or("stdout was not piped")?;
    let reader = std::thread::spawn(move || {
        let mut captured = Vec::new();
        let _ = pipe.read_to_end(&mut captured);
        captured
    });

    let started = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if started.elapsed() >= CASE_LIMIT {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!(
                        "still running after {} s, killed. A case that does not \
                         return is the failure this suite exists to catch.",
                        CASE_LIMIT.as_secs()
                    ));
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => return Err(format!("cannot wait: {e}")),
        }
    };
    let elapsed = started.elapsed();

    // The child is gone, so the write end of the pipe is closed and the reader
    // has seen EOF.
    let captured = reader.join().map_err(|_| "the capture thread panicked")?;
    // Lossy rather than an error: output mangled into invalid UTF-8 is one of
    // the symptoms, and showing it beats refusing to compare it.
    let actual = String::from_utf8_lossy(&captured);

    // Exit status first: a correct answer from a process that then died is not a
    // pass, and it is the shape process-exit faults take.
    if !status.success() {
        return Err(format!(
            "exited with {status}\n  stdout was: {actual:?}\n  expected:   {:?}",
            case.expect
        ));
    }

    if actual != case.expect {
        return Err(format!(
            "stdout does not match\n  expected: {:?}\n  actual:   {:?}",
            case.expect, actual
        ));
    }

    Ok(elapsed)
}
