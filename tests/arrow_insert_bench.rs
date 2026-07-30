//! Compare insert paths on identical row counts (manual / release bench).
//!
//! ```text
//! LD_LIBRARY_PATH=. cargo test --release --test arrow_insert_bench -- --ignored --nocapture --test-threads=1
//! ```

use std::io::Write as _;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use chdb_rust::arrow_insert::insert_record_batch;
use chdb_rust::arrow_options::InsertOptions;
use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;
use tempfile::NamedTempFile;

const ROWS: i64 = 50_000;

fn bulk_opts() -> InsertOptions {
    InsertOptions::default_bulk()
}

fn make_batch() -> RecordBatch {
    let ids: Vec<i64> = (0..ROWS).collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(ids))],
    )
    .expect("batch")
}

/// Wide telemetry schema for conversion-cost benchmarking.
fn make_wide_batch() -> RecordBatch {
    let n = ROWS as usize;
    let times: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000).collect();
    let origins = vec![1u64; n];
    let seqs: Vec<u64> = (0..n as u64).collect();
    let sids: Vec<u64> = (0..n as u64).map(|i| i % 100).collect();
    let values: Vec<f64> = (0..n as i64).map(|i| i as f64 * 0.5).collect();
    let tags: Vec<&str> = (0..n)
        .map(|i| if i % 2 == 0 { "host-a" } else { "host-b" })
        .collect();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("origin_node_id", DataType::UInt64, false),
            Field::new("ingest_seq", DataType::UInt64, false),
            Field::new("series_id", DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
            Field::new("host", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(TimestampNanosecondArray::from(times).with_timezone("UTC")),
            Arc::new(UInt64Array::from(origins)),
            Arc::new(UInt64Array::from(seqs)),
            Arc::new(UInt64Array::from(sids)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(tags)),
        ],
    )
    .expect("wide batch")
}

fn setup_table(conn: &Connection) {
    conn.query("DROP TABLE IF EXISTS bench_t", OutputFormat::TabSeparated)
        .ok();
    conn.query(
        "CREATE TABLE bench_t (id Int64) ENGINE=MergeTree ORDER BY id",
        OutputFormat::TabSeparated,
    )
    .expect("create");
}

fn setup_wide_table(conn: &Connection) {
    conn.query(
        "DROP TABLE IF EXISTS bench_wide",
        OutputFormat::TabSeparated,
    )
    .ok();
    conn.query(
        "CREATE TABLE bench_wide (
            time DateTime64(9, 'UTC'),
            origin_node_id UInt64,
            ingest_seq UInt64,
            series_id UInt64,
            value Nullable(Float64),
            host Nullable(String)
        ) ENGINE=MergeTree ORDER BY time",
        OutputFormat::TabSeparated,
    )
    .expect("create wide");
}

fn count_rows(conn: &Connection) -> i64 {
    conn.query("SELECT count() FROM bench_t", OutputFormat::TabSeparated)
        .expect("count")
        .data_utf8()
        .expect("utf8")
        .trim()
        .parse()
        .expect("parse count")
}

fn count_wide_rows(conn: &Connection) -> i64 {
    conn.query("SELECT count() FROM bench_wide", OutputFormat::TabSeparated)
        .expect("count")
        .data_utf8()
        .expect("utf8")
        .trim()
        .parse()
        .expect("parse count")
}

#[test]
#[ignore = "manual ingest benchmark; run with --release --ignored"]
fn bench_insert_values() {
    let conn = Connection::open_in_memory().expect("conn");
    setup_table(&conn);

    let start = Instant::now();
    let sql = format!("INSERT INTO bench_t SELECT number AS id FROM numbers({ROWS})");
    conn.query(&sql, OutputFormat::TabSeparated)
        .expect("insert");
    let elapsed = start.elapsed();

    assert_eq!(count_rows(&conn), ROWS);
    eprintln!("INSERT SELECT numbers({ROWS}): {elapsed:?}");
}

#[test]
#[ignore = "manual ingest benchmark; run with --release --ignored"]
fn bench_insert_arrowstream_file() {
    let conn = Connection::open_in_memory().expect("conn");
    setup_table(&conn);
    let batch = make_batch();

    let temp = NamedTempFile::new().expect("temp");
    {
        let mut writer = StreamWriter::try_new(temp.as_file(), &batch.schema()).expect("writer");
        writer.write(&batch).expect("write");
        writer.finish().expect("finish");
        temp.as_file().flush().expect("flush");
    }
    let path = temp.path().display().to_string().replace('\\', "\\\\");

    let start = Instant::now();
    let sql = format!("INSERT INTO bench_t SELECT * FROM file('{path}', 'ArrowStream')");
    conn.query(&sql, OutputFormat::TabSeparated)
        .expect("insert");
    let elapsed = start.elapsed();

    assert_eq!(count_rows(&conn), ROWS);
    eprintln!("INSERT via file(ArrowStream) {ROWS} rows: {elapsed:?}");
}

#[test]
#[ignore = "manual ingest benchmark; run with --release --ignored"]
fn bench_insert_register_arrow() {
    let conn = Connection::open_in_memory().expect("conn");
    setup_table(&conn);
    let batch = make_batch();

    let start = Instant::now();
    insert_record_batch(&conn, "bench_t", "bench_reg", batch, bulk_opts()).expect("insert");
    let elapsed = start.elapsed();

    assert_eq!(count_rows(&conn), ROWS);
    eprintln!("insert_record_batch (register_arrow_array) {ROWS} rows: {elapsed:?}");
}

#[test]
#[ignore = "manual ingest benchmark; run with --release --ignored"]
fn bench_insert_wide_schema_with_settings() {
    let conn = Connection::open_in_memory().expect("conn");
    setup_wide_table(&conn);
    let batch = make_wide_batch();

    let opts = InsertOptions {
        max_threads: Some(4),
        max_insert_block_size: Some(1_048_576),
        min_insert_block_size_rows: Some(65_536),
    };

    let start = Instant::now();
    insert_record_batch(&conn, "bench_wide", "bench_wide_reg", batch, opts).expect("insert");
    let elapsed = start.elapsed();

    assert_eq!(count_wide_rows(&conn), ROWS);
    eprintln!("insert_record_batch wide schema {ROWS} rows: {elapsed:?}");
}
