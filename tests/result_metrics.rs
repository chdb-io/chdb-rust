//! The engine reports seven counters on a result; these are the four that were
//! previously unreachable from Rust.

use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;

fn conn_with_table() -> Connection {
    let conn = Connection::open_in_memory().expect("open");
    conn.query(
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a",
        OutputFormat::TabSeparated,
    )
    .expect("create");
    conn
}

#[test]
fn an_insert_reports_rows_and_bytes_written() {
    let conn = conn_with_table();

    let result = conn
        .query(
            "INSERT INTO t SELECT number FROM numbers(1000)",
            OutputFormat::TabSeparated,
        )
        .expect("insert");

    assert_eq!(result.rows_written(), 1000);
    assert!(
        result.bytes_written() > 0,
        "bytes_written was {}",
        result.bytes_written()
    );
}

#[test]
fn a_read_reports_storage_counters() {
    let conn = conn_with_table();
    conn.query(
        "INSERT INTO t SELECT number FROM numbers(1000)",
        OutputFormat::TabSeparated,
    )
    .expect("insert");

    let result = conn
        .query("SELECT sum(a) FROM t", OutputFormat::TabSeparated)
        .expect("select");

    assert_eq!(result.storage_rows_read(), 1000);
    assert!(
        result.storage_bytes_read() > 0,
        "storage_bytes_read was {}",
        result.storage_bytes_read()
    );
}

#[test]
fn count_is_answered_from_metadata_without_reading_storage() {
    // MergeTree metadata shortcut: count() can be answered from part statistics
    // without scanning actual rows. This test verifies that distinction — the
    // rows_read counter counts pipeline rows, but storage_rows_read counts what
    // actually came off disk.
    let conn = conn_with_table();
    conn.query(
        "INSERT INTO t SELECT number FROM numbers(1000)",
        OutputFormat::TabSeparated,
    )
    .expect("insert");

    let result = conn
        .query("SELECT count() FROM t", OutputFormat::TabSeparated)
        .expect("select");

    assert!(
        result.storage_rows_read() < 10,
        "count() should use metadata, but storage_rows_read was {}",
        result.storage_rows_read()
    );
}
