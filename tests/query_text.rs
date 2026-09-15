//! Query text is passed to the engine by pointer and length, so a NUL byte
//! inside a string literal is data rather than a terminator.

use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;

#[test]
fn interior_nul_in_a_string_literal_is_data() {
    let conn = Connection::open_in_memory().expect("open");

    // The literal holds a real NUL. Under CString this returned Error::Nul
    // before the query ever reached the engine.
    let sql = "SELECT length('a\0b') AS n";

    let result = conn
        .query(sql, OutputFormat::TabSeparated)
        .expect("query with an interior NUL should reach the engine");

    assert_eq!(result.data_utf8_lossy().trim(), "3");
}

#[test]
fn plain_queries_are_unaffected() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn
        .query("SELECT 1 + 1 AS sum", OutputFormat::TabSeparated)
        .expect("query");
    assert_eq!(result.data_utf8_lossy().trim(), "2");
}

#[test]
fn interior_nul_in_a_format_stream_is_data() {
    let mut conn = Connection::open_in_memory().expect("open");
    let sql = "SELECT length('a\0b') AS n";

    let mut stream = conn
        .query_stream(sql, OutputFormat::TabSeparated)
        .expect("stream with an interior NUL should reach the engine");

    let chunk = stream.next_chunk().expect("chunk").expect("one row");
    assert_eq!(chunk.data_utf8_lossy().trim(), "3");
}

#[cfg(feature = "arrow")]
#[test]
fn interior_nul_in_an_arrow_stream_is_data() {
    use arrow::array::UInt64Array;

    let mut conn = Connection::open_in_memory().expect("open");
    let sql = "SELECT length('a\0b') AS n";

    let mut stream = conn
        .query_stream_arrow(sql)
        .expect("arrow stream with an interior NUL should reach the engine");

    let batch = stream.next_batch().expect("batch").expect("one row");
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap_or_else(|| {
            panic!(
                "length() should be UInt64, got {:?}",
                batch.schema().field(0).data_type()
            )
        });
    assert_eq!(values.value(0), 3);
}
