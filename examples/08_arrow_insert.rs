//! Fast bulk insert: RecordBatch → ArrowStream registration → MergeTree.
//!
//! Run: cargo run --example 08_arrow_insert

use std::sync::Arc;

use chdb_rust::arrow::array::{Float64Array, Int64Array, RecordBatch};
use chdb_rust::arrow::datatypes::{DataType, Field, Schema};

use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
use chdb_rust::session::SessionBuilder;
use chdb_rust::InsertOptions;

fn main() -> Result<(), chdb_rust::error::Error> {
    let data_dir = std::env::temp_dir().join("chdb-rust-arrow-insert-example");
    let session = SessionBuilder::new()
        .with_data_path(&data_dir)
        .with_auto_cleanup(true)
        .build()?;

    session.execute(
        "CREATE TABLE IF NOT EXISTS metrics (id Int64, value Float64) \
         ENGINE = MergeTree ORDER BY id",
        None,
    )?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
            Arc::new(Float64Array::from(vec![
                10.0, 20.0, 30.0, 40.0, 50.0,
            ])),
        ],
    )
    .map_err(|e| chdb_rust::error::Error::QueryError(e.to_string()))?;

    session.insert_record_batch(
        "metrics",
        "example_batch",
        batch,
        InsertOptions::default_bulk(),
    )?;

    let count = session.execute(
        "SELECT count() AS c FROM metrics",
        Some(&[Arg::OutputFormat(OutputFormat::TabSeparated)]),
    )?;
    println!("Row count after Arrow insert: {}", count.data_utf8_lossy());

    let sum = session.execute(
        "SELECT sum(value) FROM metrics",
        Some(&[Arg::OutputFormat(OutputFormat::TabSeparated)]),
    )?;
    println!("Sum(value): {}", sum.data_utf8_lossy());

    Ok(())
}
