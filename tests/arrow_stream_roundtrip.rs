//! End-to-end: register Arrow C Data Interface arrays and query via ArrowStream().

use std::sync::Arc;

use arrow::array::RecordBatchIterator;
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::to_ffi;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use chdb_rust::arrow_stream::{arrow_stream_table_sql, ArrowArray, ArrowSchema, ArrowStream};
use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;

#[test]
fn registered_arrow_array_is_queryable_via_arrow_stream_table_function() {
    let conn = Connection::open_in_memory().expect("conn");

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![10i64, 20, 30]))],
    )
    .expect("batch");

    // Keep source data alive while FFI structs are registered.
    let struct_array = arrow::array::StructArray::from(batch);
    let (mut ffi_array, mut ffi_schema) = to_ffi(&struct_array.into()).expect("to_ffi");

    let arrow_schema = unsafe { ArrowSchema::from_raw(&mut ffi_schema) };
    let arrow_array = unsafe { ArrowArray::from_raw(&mut ffi_array) };

    conn.register_arrow_array("probe_batch", &arrow_schema, &arrow_array)
        .expect("register");

    let sql = format!(
        "SELECT sum(id) AS s FROM {}",
        arrow_stream_table_sql("probe_batch")
    );
    let result = conn
        .query(&sql, OutputFormat::TabSeparated)
        .expect("query registered stream");

    assert_eq!(
        result.data_utf8().expect("utf8").trim(),
        "60",
        "expected sum(id)=60 from registered Arrow stream"
    );

    conn.unregister_arrow_table("probe_batch")
        .expect("unregister");
}

#[test]
fn registered_arrow_c_stream_is_queryable() {
    let conn = Connection::open_in_memory().expect("conn");

    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7i64, 8]))],
    )
    .expect("batch");

    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    let mut ffi_stream = FFI_ArrowArrayStream::new(Box::new(reader));

    let arrow_stream = unsafe { ArrowStream::from_raw(&mut ffi_stream) };
    conn.register_arrow_stream("stream_probe", &arrow_stream)
        .expect("register stream");

    let sql = format!(
        "SELECT count() FROM {}",
        arrow_stream_table_sql("stream_probe")
    );
    let count = conn
        .query(&sql, OutputFormat::TabSeparated)
        .expect("query stream")
        .data_utf8()
        .expect("utf8");
    assert_eq!(count.trim(), "2");

    conn.unregister_arrow_table("stream_probe")
        .expect("unregister");
}

#[test]
fn bare_table_name_still_unknown_for_registered_stream() {
    let conn = Connection::open_in_memory().expect("conn");

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1i64]))],
    )
    .expect("batch");

    let struct_array = arrow::array::StructArray::from(batch);
    let (mut ffi_array, mut ffi_schema) = to_ffi(&struct_array.into()).expect("to_ffi");

    let arrow_schema = unsafe { ArrowSchema::from_raw(&mut ffi_schema) };
    let arrow_array = unsafe { ArrowArray::from_raw(&mut ffi_array) };

    conn.register_arrow_array("bare_name_test", &arrow_schema, &arrow_array)
        .expect("register");

    let err = conn
        .query("SELECT * FROM bare_name_test", OutputFormat::TabSeparated)
        .expect_err("bare identifier must not resolve");

    let msg = format!("{err:?}");
    assert!(
        msg.contains("UNKNOWN_TABLE") || msg.contains("Unknown table"),
        "expected UNKNOWN_TABLE for bare name, got: {msg}"
    );
}
