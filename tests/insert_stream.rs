//! Write-side streaming INSERT: open, push chunks, finalise.

mod common;

use chdb_rust::connection::Connection;
use chdb_rust::format::{InputFormat, OutputFormat};
use chdb_rust::session::SessionBuilder;

fn conn_with_table() -> Connection {
    let conn = Connection::open_in_memory().expect("open");
    conn.query(
        "CREATE TABLE t (a UInt64, b String) ENGINE = MergeTree ORDER BY a",
        OutputFormat::TabSeparated,
    )
    .expect("create");
    conn
}

fn count(conn: &Connection) -> u64 {
    conn.query("SELECT count() FROM t", OutputFormat::TabSeparated)
        .expect("count")
        .data_utf8_lossy()
        .trim()
        .parse()
        .expect("parse count")
}

#[test]
fn a_csv_stream_commits_every_chunk() {
    let mut conn = conn_with_table();

    let mut ins = conn
        .insert_stream("INSERT INTO t (a, b)", InputFormat::CSV)
        .expect("open stream");
    ins.append(b"1,\"one\"\n").expect("append");
    ins.append(b"2,\"two\"\n3,\"three\"\n").expect("append");
    let stats = ins.finish().expect("finish");

    assert_eq!(stats.rows_written, 3);
    assert!(stats.bytes_written > 0);
    assert_eq!(count(&conn), 3);
}

#[test]
fn a_json_each_row_stream_commits() {
    let mut conn = conn_with_table();

    let mut ins = conn
        .insert_stream("INSERT INTO t (a, b)", InputFormat::JSONEachRow)
        .expect("open stream");
    ins.append(br#"{"a":1,"b":"one"}"#).expect("append");
    ins.append(b"\n").expect("append");
    let stats = ins.finish().expect("finish");

    assert_eq!(stats.rows_written, 1);
    assert_eq!(count(&conn), 1);
}

#[test]
fn a_malformed_chunk_surfaces_an_error() {
    let mut conn = conn_with_table();

    let mut ins = conn
        .insert_stream("INSERT INTO t (a, b)", InputFormat::CSV)
        .expect("open stream");

    // "not-a-number" cannot parse as UInt64. The engine may reject it on the
    // append that carries it or when the stream is finalised; either is a
    // failure, and neither may be silent.
    let appended = ins.append(b"not-a-number,\"x\"\n");
    let finished = ins.finish();
    assert!(
        appended.is_err() || finished.is_err(),
        "a malformed row must fail on append or on finish"
    );

    assert_eq!(count(&conn), 0);
}

#[test]
fn dropping_without_finishing_commits_nothing() {
    let mut conn = conn_with_table();

    {
        let mut ins = conn
            .insert_stream("INSERT INTO t (a, b)", InputFormat::CSV)
            .expect("open stream");
        ins.append(b"1,\"one\"\n").expect("append");
        // Dropped here without finish(): cancelled, then destroyed.
    }

    assert_eq!(count(&conn), 0);
}

#[test]
fn the_connection_is_usable_again_after_finishing() {
    let mut conn = conn_with_table();

    let mut ins = conn
        .insert_stream("INSERT INTO t (a, b)", InputFormat::CSV)
        .expect("open stream");
    ins.append(b"1,\"one\"\n").expect("append");
    ins.finish().expect("finish");

    assert_eq!(count(&conn), 1);
}

#[test]
fn opening_a_bad_statement_fails_at_open() {
    let mut conn = conn_with_table();

    let err = conn
        .insert_stream("INSERT INTO no_such_table (a)", InputFormat::CSV)
        .expect_err("a missing table must fail when the stream is opened");

    assert!(
        matches!(err, chdb_rust::error::Error::QueryError(_)),
        "expected QueryError, got {err:?}"
    );
}

#[test]
fn cancelling_commits_nothing() {
    let mut conn = conn_with_table();

    let mut ins = conn
        .insert_stream("INSERT INTO t (a, b)", InputFormat::CSV)
        .expect("open stream");
    ins.append(b"1,\"one\"\n").expect("append");
    ins.cancel();

    assert_eq!(count(&conn), 0);

    // The connection must still be usable after a cancel.
    conn.query("SELECT 1", OutputFormat::TabSeparated)
        .expect("connection still usable after cancel");
}

#[test]
fn a_stream_is_an_io_write_sink() {
    use std::io::Write as _;

    let mut conn = conn_with_table();

    let mut ins = conn
        .insert_stream("INSERT INTO t (a, b)", InputFormat::JSONEachRow)
        .expect("open stream");

    for i in 1..=3u64 {
        writeln!(ins, r#"{{"a":{i},"b":"row-{i}"}}"#).expect("write");
    }
    ins.flush().expect("flush");

    let stats = ins.finish().expect("finish");
    assert_eq!(stats.rows_written, 3);
    assert_eq!(count(&conn), 3);
}

#[test]
fn an_insert_statement_can_bind_parameters() {
    let mut conn = conn_with_table();

    // A parameterised INSERT INTO FUNCTION is the motivating case; a plain
    // table insert with a bound literal exercises the same binding path.
    let mut ins = conn
        .insert_stream_with_params(
            "INSERT INTO t (a, b) SETTINGS min_insert_block_size_rows = {n:UInt64}",
            InputFormat::CSV,
            [("n", "1024")],
        )
        .expect("open stream");

    ins.append(b"1,\"one\"\n").expect("append");
    let stats = ins.finish().expect("finish");

    assert_eq!(stats.rows_written, 1);
    assert_eq!(count(&conn), 1);
}

#[test]
fn a_session_can_open_an_insert_stream() {
    let tmp = common::tempdir();
    let mut session = SessionBuilder::new()
        .with_data_path(tmp.path())
        .with_auto_cleanup(true)
        .build()
        .expect("build session");

    session
        .execute(
            "CREATE TABLE t (a UInt64, b String) ENGINE = MergeTree ORDER BY a",
            None,
        )
        .expect("create table");

    let mut ins = session
        .connection_mut()
        .insert_stream("INSERT INTO t (a, b)", InputFormat::CSV)
        .expect("open stream");
    ins.append(b"1,\"one\"\n").expect("append");
    ins.append(b"2,\"two\"\n").expect("append");
    let stats = ins.finish().expect("finish");

    assert_eq!(stats.rows_written, 2);

    let result = session
        .execute("SELECT count() FROM t", None)
        .expect("count");
    assert_eq!(result.data_utf8_lossy().trim(), "2");
}
