//! Example: streaming Arrow record batches via the C Data Interface
//!
//! Unlike example 09, which streams Arrow IPC bytes through [`QueryStream`],
//! this example uses [`Session::execute_stream_arrow`] to pull typed
//! [`arrow::record_batch::RecordBatch`] values directly from chDB with no
//! IPC serialization.
//!
//! Run with: cargo run --example 11_arrow_query_stream

use arrow::util::pretty::pretty_format_batches;
use chdb_rust::session::SessionBuilder;

const PREVIEW_ROWS: usize = 5;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = std::env::temp_dir().join("chdb-arrow-stream-example");
    let session = SessionBuilder::new()
        .with_data_path(tmp)
        .with_auto_cleanup(true)
        .build()?;

    session.execute(
        "CREATE TABLE num (n UInt64, two_n UInt64) ENGINE = MergeTree() ORDER BY n",
        None,
    )?;
    session.execute(
        "INSERT INTO num SELECT number, number * 2 FROM numbers(300_000)",
        None,
    )?;

    let stream = session.execute_stream_arrow("SELECT n, two_n FROM num ORDER BY n")?;

    println!("Streaming query results (Arrow C Data Interface):\n");
    let mut batch_count = 0;
    let mut total_rows = 0usize;
    for batch in stream {
        let batch = batch?;
        let row_count = batch.num_rows();
        total_rows += row_count;

        let preview = if row_count > PREVIEW_ROWS {
            batch.slice(0, PREVIEW_ROWS)
        } else {
            batch
        };

        println!("batch {batch_count}: {row_count} rows");
        println!("{}", pretty_format_batches(&[preview])?);
        if row_count > PREVIEW_ROWS {
            println!(
                "  ... ({remaining} more rows)",
                remaining = row_count - PREVIEW_ROWS
            );
        }
        batch_count += 1;
    }

    println!("\nReceived {batch_count} batches, {total_rows} rows total.");
    Ok(())
}
