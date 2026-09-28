use chdb_rust::connection::Connection;
use chdb_rust::error::Error;
use chdb_rust::format::OutputFormat;

#[test]
fn data_str_borrows_the_result_without_copying() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn
        .query("SELECT number FROM numbers(3)", OutputFormat::JSONEachRow)
        .expect("query");

    let text = result.data_str().expect("valid UTF-8");
    assert_eq!(text, result.data_utf8().expect("valid UTF-8"));
    // Same memory as the raw result buffer, so nothing was copied.
    assert_eq!(text.as_ptr(), result.data_ref().as_ptr());
}

#[test]
fn data_str_rejects_binary_output() {
    let conn = Connection::open_in_memory().expect("open");
    // RowBinary writes the byte 0xFF as is, which is never valid UTF-8.
    let result = conn
        .query("SELECT unhex('FF') AS b", OutputFormat::RowBinary)
        .expect("query");

    match result.data_str() {
        Err(Error::InvalidUtf8(_)) => {}
        other => panic!("expected InvalidUtf8, got {other:?}"),
    }
}

#[test]
fn data_str_on_an_empty_result_is_empty() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn
        .query("SELECT 1 WHERE 0", OutputFormat::JSONEachRow)
        .expect("query");

    assert_eq!(result.data_str().expect("valid UTF-8"), "");
}
