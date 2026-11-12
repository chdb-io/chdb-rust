//! The owned registration keeps a lazy input alive until its query finishes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;
use chdb_rust::{insert_record_batch_reader, InsertOptions};

fn reader() -> Box<dyn arrow::array::RecordBatchReader + Send> {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batches = [1_i64, 2, 3].map(|value| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .expect("batch")
    });
    Box::new(RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema,
    ))
}

struct CountingReader {
    inner: Box<dyn RecordBatchReader + Send>,
    drops: Arc<AtomicUsize>,
}

impl Iterator for CountingReader {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl RecordBatchReader for CountingReader {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Drop for CountingReader {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn scoped_reader_survives_lazy_scan_and_unregisters_on_drop() {
    let mut conn = Connection::open_in_memory().expect("open");
    let sql;
    {
        let mut registration = conn.register_arrow_reader(reader()).expect("register");
        sql = format!("SELECT n FROM {} ORDER BY n", registration.sql_expr());
        let mut stream = registration.query_stream_arrow(&sql).expect("start");
        let mut values = Vec::new();
        while let Some(batch) = stream.next_batch().expect("fetch") {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            values.extend(column.values().iter().copied());
        }
        assert_eq!(values, vec![1, 2, 3]);
    }
    let error = conn.query(&sql, OutputFormat::TabSeparated).unwrap_err();
    assert!(format!("{error}").contains("not found") || format!("{error}").contains("Unknown"));
}

#[test]
fn early_drop_cancels_before_explicit_close_and_reuses_connection() {
    let mut conn = Connection::open_in_memory().expect("open");
    let drops = Arc::new(AtomicUsize::new(0));
    let mut registration = conn
        .register_arrow_reader(Box::new(CountingReader {
            inner: reader(),
            drops: Arc::clone(&drops),
        }))
        .expect("register");
    let sql = format!("SELECT n FROM {}", registration.sql_expr());
    {
        let mut stream = registration.query_stream_arrow(&sql).expect("start");
        assert!(stream.next_batch().expect("first").is_some());
    }
    assert_eq!(drops.load(Ordering::SeqCst), 0, "source still registered");
    // A one-shot reader need not support another scan. A query unrelated to
    // the registration still proves that the cancelled connection is usable.
    registration.close().expect("unregister");
    assert_eq!(drops.load(Ordering::SeqCst), 1, "source released once");
    assert_eq!(
        conn.query("SELECT 1", OutputFormat::TabSeparated)
            .expect("query after cancellation")
            .data_utf8()
            .expect("utf8")
            .trim(),
        "1"
    );
}

#[test]
fn materialized_result_outlives_scoped_input() {
    let mut conn = Connection::open_in_memory().expect("open");
    let registration = conn.register_arrow_reader(reader()).expect("register");
    let sql = format!("SELECT sum(n) AS total FROM {}", registration.sql_expr());
    let result = registration.query_arrow(&sql).expect("query");
    registration.close().expect("unregister");
    let rows: usize = result.map(|batch| batch.expect("batch").num_rows()).sum();
    assert_eq!(rows, 1);
}

#[test]
fn failed_insert_reports_query_error_and_cleans_registration() {
    let conn = Connection::open_in_memory().expect("open");
    let err = insert_record_batch_reader(
        &conn,
        "missing_arrow_destination",
        "failed_arrow_insert_source",
        reader(),
        InsertOptions::default(),
    )
    .expect_err("missing destination");
    assert!(err.to_string().contains("missing_arrow_destination"));

    conn.query(
        "CREATE TABLE arrow_destination (n Int64) ENGINE=MergeTree ORDER BY n",
        OutputFormat::Null,
    )
    .expect("create");
    insert_record_batch_reader(
        &conn,
        "arrow_destination",
        "failed_arrow_insert_source",
        reader(),
        InsertOptions::default(),
    )
    .expect("same source name reusable after cleanup");
}
