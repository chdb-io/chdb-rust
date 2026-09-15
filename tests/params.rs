//! Server-side {name:Type} binding. Values reach the engine as strings and are
//! never interpolated into SQL.

use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;

#[test]
fn a_typed_placeholder_round_trips() {
    let conn = Connection::open_in_memory().expect("open");

    let result = conn
        .query_with_params(
            "SELECT {x:Int64} + 1 AS v",
            OutputFormat::TabSeparated,
            [("x", "41")],
        )
        .expect("query");

    assert_eq!(result.data_utf8_lossy().trim(), "42");
}

#[test]
fn several_parameters_bind_by_name_not_position() {
    let conn = Connection::open_in_memory().expect("open");

    let result = conn
        .query_with_params(
            "SELECT concat({b:String}, {a:String}) AS v",
            OutputFormat::TabSeparated,
            [("a", "world"), ("b", "hello ")],
        )
        .expect("query");

    assert_eq!(result.data_utf8_lossy().trim(), "hello world");
}

#[test]
fn an_injection_payload_is_bound_as_data_and_never_executed() {
    let conn = Connection::open_in_memory().expect("open");

    conn.query(
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a",
        OutputFormat::TabSeparated,
    )
    .expect("create table");

    // If this were interpolated into the SQL text it would close the string
    // literal and run a second statement, dropping `t`. Bound, it is just a
    // String value: `t` surviving below is the actual proof the payload never
    // executed, not a byte-for-byte round-trip of the value.
    let result = conn
        .query_with_params(
            "SELECT {s:String} AS v",
            OutputFormat::JSONEachRow,
            [("s", "'; DROP TABLE t; --")],
        )
        .expect("query");

    let survives = conn
        .query(
            "SELECT count() FROM system.tables WHERE name = 't'",
            OutputFormat::TabSeparated,
        )
        .expect("check table survival");
    assert_eq!(
        survives.data_utf8_lossy().trim(),
        "1",
        "table t must survive an unexecuted DROP"
    );

    assert_eq!(
        result.data_utf8_lossy().trim(),
        "{\"v\":\"'; DROP TABLE t; --\"}"
    );
}

#[test]
fn an_empty_slice_behaves_like_a_plain_query() {
    let conn = Connection::open_in_memory().expect("open");

    let result = conn
        .query_with_params(
            "SELECT 7 AS v",
            OutputFormat::TabSeparated,
            chdb_rust::query_param::QueryParams::new(),
        )
        .expect("query");

    assert_eq!(result.data_utf8_lossy().trim(), "7");
}

#[test]
fn a_missing_parameter_is_an_error_not_a_panic() {
    let conn = Connection::open_in_memory().expect("open");

    let err = conn
        .query_with_params(
            "SELECT {x:Int64} AS v",
            OutputFormat::TabSeparated,
            chdb_rust::query_param::QueryParams::new(),
        )
        .expect_err("an unbound placeholder must fail");

    assert!(
        matches!(err, chdb_rust::error::Error::QueryError(_)),
        "expected QueryError, got {err:?}"
    );
}

#[test]
fn a_format_stream_binds_parameters() {
    let mut conn = Connection::open_in_memory().expect("open");

    let mut stream = conn
        .query_stream_with_params(
            "SELECT number FROM numbers({n:UInt64})",
            OutputFormat::TabSeparated,
            [("n", "5")],
        )
        .expect("stream");

    let mut rows = 0usize;
    while let Some(chunk) = stream.next_chunk().expect("chunk") {
        rows += chunk.data_utf8_lossy().lines().count();
    }

    assert_eq!(rows, 5);
}

#[test]
fn a_parameter_value_containing_an_interior_nul_binds_binary_safe() {
    let conn = Connection::open_in_memory().expect("open");

    // The _n parameter path passes explicit byte lengths, so an interior NUL in
    // the bound value must round-trip rather than truncate at the first NUL.
    let result = conn
        .query_with_params(
            "SELECT length({s:String}) AS v",
            OutputFormat::TabSeparated,
            [("s", "a\0b")],
        )
        .expect("binary-safe bind");

    assert_eq!(result.data_utf8_lossy().trim(), "3");
}
