//! End-to-end Arrow bulk insert into MergeTree tables.

use std::sync::Arc;

use arrow::array::TimestampNanosecondArray;
use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chdb_rust::arrow_insert::{
    insert_record_batch, insert_record_batch_direct, insert_record_batches,
};
use chdb_rust::arrow_options::InsertOptions;
use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;
use chdb_rust::session::SessionBuilder;

fn bulk_opts() -> InsertOptions {
    InsertOptions::default_bulk()
}

#[test]
fn insert_record_batch_direct_writes_to_merge_tree() {
    let conn = Connection::open_in_memory().expect("conn");
    conn.query(
        "CREATE TABLE direct_dest (id Int64) ENGINE=MergeTree ORDER BY id",
        OutputFormat::TabSeparated,
    )
    .expect("create");

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![300i64, 400, 500]))],
    )
    .expect("batch");

    insert_record_batch_direct(&conn, "direct_dest", batch, bulk_opts()).expect("insert");

    let count = conn
        .query(
            "SELECT count() FROM direct_dest",
            OutputFormat::TabSeparated,
        )
        .expect("count")
        .data_utf8()
        .expect("utf8");
    assert_eq!(count.trim(), "3");
}

#[test]
fn insert_record_batch_helper_writes_to_merge_tree() {
    let conn = Connection::open_in_memory().expect("conn");
    conn.query(
        "CREATE TABLE dest (id Int64) ENGINE=MergeTree ORDER BY id",
        OutputFormat::TabSeparated,
    )
    .expect("create");

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![100i64, 200]))],
    )
    .expect("batch");

    insert_record_batch(&conn, "dest", "helper_batch", batch, bulk_opts()).expect("insert");

    let count = conn
        .query("SELECT count() FROM dest", OutputFormat::TabSeparated)
        .expect("count")
        .data_utf8()
        .expect("utf8");
    assert_eq!(count.trim(), "2");
}

#[test]
fn insert_record_batches_writes_all_rows() {
    let conn = Connection::open_in_memory().expect("conn");
    conn.query(
        "CREATE TABLE multi_dest (id Int64) ENGINE=MergeTree ORDER BY id",
        OutputFormat::TabSeparated,
    )
    .expect("create");

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batches = vec![
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
        )
        .expect("batch 1"),
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![3i64, 4, 5]))],
        )
        .expect("batch 2"),
    ];

    insert_record_batches(
        &conn,
        "multi_dest",
        "multi_stream",
        schema,
        batches,
        bulk_opts(),
    )
    .expect("insert");

    let count = conn
        .query("SELECT count() FROM multi_dest", OutputFormat::TabSeparated)
        .expect("count")
        .data_utf8()
        .expect("utf8");
    assert_eq!(count.trim(), "5");

    let sum = conn
        .query("SELECT sum(id) FROM multi_dest", OutputFormat::TabSeparated)
        .expect("sum")
        .data_utf8()
        .expect("utf8");
    assert_eq!(sum.trim(), "15");
}

#[test]
fn session_insert_record_batch_writes_to_merge_tree() {
    let session = SessionBuilder::new()
        .with_data_path(std::env::temp_dir().join("chdb-rust-session-insert-test"))
        .with_auto_cleanup(true)
        .build()
        .expect("session");

    session
        .execute(
            "CREATE TABLE session_dest (id Int64) ENGINE=MergeTree ORDER BY id",
            None,
        )
        .expect("create");

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![42i64, 43]))],
    )
    .expect("batch");

    session
        .insert_record_batch("session_dest", "session_stream", batch, bulk_opts())
        .expect("insert");

    let count = session
        .execute("SELECT count() FROM session_dest", None)
        .expect("count")
        .data_utf8()
        .expect("utf8");
    assert_eq!(count.trim(), "2");
}

/// The Arrow insert must map batch columns to destination columns by NAME, not
/// by position. A positional mapping silently scatters values into the wrong
/// columns whenever the table's physical column order differs from the batch's
/// (e.g. after `ALTER TABLE ... ADD COLUMN`, which appends).
#[test]
fn arrow_insert_maps_columns_by_name_not_position() {
    let conn = Connection::open_in_memory().expect("conn");
    conn.query(
        "CREATE TABLE t (a Int64, b Int64) ENGINE=Memory",
        OutputFormat::TabSeparated,
    )
    .expect("create");

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, false),
            Field::new("a", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![2i64])),
            Arc::new(Int64Array::from(vec![1i64])),
        ],
    )
    .expect("batch");

    insert_record_batch_direct(&conn, "t", batch, bulk_opts()).expect("insert");

    let out = conn
        .query("SELECT a, b FROM t", OutputFormat::TabSeparated)
        .expect("select")
        .data_utf8()
        .expect("utf8")
        .trim()
        .to_string();

    assert_eq!(
        out, "1\t2",
        "Arrow insert must map columns by name, not position"
    );
}

/// ClickHouse MATERIALIZED VIEW fires on Arrow C Data Interface inserts.
#[test]
fn materialized_view_triggers_on_arrow_insert() {
    let conn = Connection::open_in_memory().expect("conn");

    conn.query(
        "CREATE TABLE mv_source (
            time DateTime64(9, 'UTC'),
            origin_node_id UInt64,
            ingest_seq UInt64,
            series_id UInt64,
            value Nullable(Float64)
        ) ENGINE = ReplacingMergeTree(ingest_seq)
        ORDER BY (series_id, time)",
        OutputFormat::TabSeparated,
    )
    .expect("create source");

    conn.query(
        "CREATE TABLE mv_dest (
            time DateTime64(9, 'UTC'),
            origin_node_id UInt64,
            ingest_seq UInt64,
            series_id UInt64,
            value Nullable(Float64)
        ) ENGINE = ReplacingMergeTree(ingest_seq)
        ORDER BY (series_id, time)",
        OutputFormat::TabSeparated,
    )
    .expect("create dest");

    conn.query(
        "CREATE MATERIALIZED VIEW mv_fact TO mv_dest AS
         SELECT
            toStartOfInterval(time, INTERVAL 5 MINUTE) AS time,
            any(origin_node_id) AS origin_node_id,
            max(ingest_seq) AS ingest_seq,
            series_id,
            avg(value) AS value
         FROM mv_source
         GROUP BY series_id, time",
        OutputFormat::TabSeparated,
    )
    .expect("create fact mv");

    conn.query(
        "CREATE TABLE mv_source_series (
            series_id UInt64,
            host LowCardinality(String)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (series_id)",
        OutputFormat::TabSeparated,
    )
    .expect("create source series");

    conn.query(
        "CREATE TABLE mv_dest_series (
            series_id UInt64,
            host LowCardinality(String)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (series_id)",
        OutputFormat::TabSeparated,
    )
    .expect("create dest series");

    conn.query(
        "CREATE MATERIALIZED VIEW mv_series TO mv_dest_series AS
         SELECT * FROM mv_source_series",
        OutputFormat::TabSeparated,
    )
    .expect("create series mv");

    let fact_schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("origin_node_id", DataType::UInt64, false),
        Field::new("ingest_seq", DataType::UInt64, false),
        Field::new("series_id", DataType::UInt64, false),
        Field::new("value", DataType::Float64, true),
    ]));

    let ts: i64 = 1_700_000_000_000_000_000;
    let fact_batch = RecordBatch::try_new(
        Arc::clone(&fact_schema),
        vec![
            Arc::new(
                TimestampNanosecondArray::from(vec![ts, ts + 60_000_000_000]).with_timezone("UTC"),
            ),
            Arc::new(UInt64Array::from(vec![1u64, 1])),
            Arc::new(UInt64Array::from(vec![1u64, 2])),
            Arc::new(UInt64Array::from(vec![100u64, 100])),
            Arc::new(Float64Array::from(vec![Some(10.0), Some(20.0)])),
        ],
    )
    .expect("fact batch");

    insert_record_batch_direct(&conn, "mv_source", fact_batch, bulk_opts()).expect("fact insert");

    let series_schema = Arc::new(Schema::new(vec![
        Field::new("series_id", DataType::UInt64, false),
        Field::new("host", DataType::Utf8, false),
    ]));
    let series_batch = RecordBatch::try_new(
        series_schema,
        vec![
            Arc::new(UInt64Array::from(vec![100u64])),
            Arc::new(StringArray::from(vec!["h1"])),
        ],
    )
    .expect("series batch");

    insert_record_batch_direct(&conn, "mv_source_series", series_batch, bulk_opts())
        .expect("series insert");

    let dest_count = conn
        .query("SELECT count() FROM mv_dest", OutputFormat::TabSeparated)
        .expect("dest count")
        .data_utf8()
        .expect("utf8");
    assert_eq!(
        dest_count.trim(),
        "1",
        "aggregating MV should produce one 5m bucket from two source rows"
    );

    let dest_avg = conn
        .query("SELECT value FROM mv_dest", OutputFormat::TabSeparated)
        .expect("dest avg")
        .data_utf8()
        .expect("utf8");
    assert_eq!(dest_avg.trim(), "15", "expected avg(10, 20) = 15 in dest");

    let series_count = conn
        .query(
            "SELECT count() FROM mv_dest_series",
            OutputFormat::TabSeparated,
        )
        .expect("series count")
        .data_utf8()
        .expect("utf8");
    assert_eq!(
        series_count.trim(),
        "1",
        "passthrough series MV should copy dimension row"
    );
}
