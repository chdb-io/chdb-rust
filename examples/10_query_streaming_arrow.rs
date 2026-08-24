//! Example: streaming query results
//!
//! Each chunk from the stream is raw Arrow IPC stream bytes. This example
//! decodes those bytes into record batches and prints them as human-readable tables.
//!
//! Run with: cargo run --example 10_query_streaming_arrow

use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow::util::pretty::pretty_format_batches;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
use chdb_rust::session::SessionBuilder;

const PREVIEW_ROWS: usize = 5;

fn print_arrow_chunk(chunk_index: usize, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let schema = reader.schema();

    println!("chunk {chunk_index} (schema: {schema})");

    for (batch_index, batch) in reader.enumerate() {
        let batch = batch?;
        let row_count = batch.num_rows();
        let preview = if row_count > PREVIEW_ROWS {
            batch.slice(0, PREVIEW_ROWS)
        } else {
            batch
        };

        println!("  batch {batch_index}: {row_count} rows");
        println!("{}", pretty_format_batches(&[preview])?);
        if row_count > PREVIEW_ROWS {
            println!(
                "  ... ({remaining} more rows)",
                remaining = row_count - PREVIEW_ROWS
            );
        }
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = std::env::temp_dir().join("chdb-stream-example");
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

    let stream = session.execute_stream(
        "SELECT n, two_n FROM num ORDER BY n",
        Some(&[Arg::OutputFormat(OutputFormat::ArrowStream)]),
    )?;

    println!("Streaming query results (Arrow IPC stream):\n");
    let mut total_rows = 0u64;
    let mut num_chunks = 0;
    for (chunk_index, chunk) in stream.enumerate() {
        let chunk = chunk?;
        total_rows += chunk.rows_read();
        print_arrow_chunk(chunk_index, chunk.data_ref())?;
        num_chunks = chunk_index + 1;
    }

    println!("\nReceived {num_chunks} chunks, {total_rows} rows total.");
    Ok(())
}
