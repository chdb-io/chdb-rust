//! Example: streaming query results
//!
//! Run with: cargo run --example 09_query_streaming

use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
use chdb_rust::session::SessionBuilder;

const EDGE_LINES: usize = 3;

fn main() -> Result<(), chdb_rust::error::Error> {
    let tmp = std::env::temp_dir().join("chdb-stream-example");
    let session = SessionBuilder::new()
        .with_data_path(tmp)
        .with_auto_cleanup(true)
        .build()?;

    session.execute(
        "CREATE TABLE num (n UInt64) ENGINE = MergeTree() ORDER BY n",
        None,
    )?;
    session.execute("INSERT INTO num SELECT number FROM numbers(300_000)", None)?;

    let stream = session.execute_stream(
        // Without this second ORDER BY, the chunks are not ordered.
        "SELECT * FROM num ORDER BY n",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    for (chunk_num, chunk) in stream.enumerate() {
        let chunk = chunk?;
        let rows_read = chunk.rows_read() as usize;
        println!("Chunk {chunk_num}, rows read: {}", rows_read);

        for (n, line) in chunk.data_utf8_lossy().lines().enumerate() {
            if n < EDGE_LINES {
                println!("{line}");
            } else if n == EDGE_LINES && n < rows_read - EDGE_LINES {
                println!("...");
            } else if n >= rows_read - EDGE_LINES {
                println!("{line}");
            }
        }
    }

    Ok(())
}
