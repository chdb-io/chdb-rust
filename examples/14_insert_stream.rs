//! Streaming INSERT: push rows in chunks, with backpressure from the engine.

use std::io::Write as _;

use chdb_rust::connection::Connection;
use chdb_rust::format::{InputFormat, OutputFormat};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::open_in_memory()?;
    conn.query(
        "CREATE TABLE events (id UInt64, name String) ENGINE = MergeTree ORDER BY id",
        OutputFormat::TabSeparated,
    )?;

    let mut ins = conn.insert_stream("INSERT INTO events (id, name)", InputFormat::JSONEachRow)?;
    for id in 1..=10_000u64 {
        writeln!(ins, r#"{{"id":{id},"name":"event-{id}"}}"#)?;
    }
    let stats = ins.finish()?;

    println!(
        "wrote {} rows / {} bytes in {:?}",
        stats.rows_written, stats.bytes_written, stats.elapsed
    );

    let count = conn.query("SELECT count() FROM events", OutputFormat::TabSeparated)?;
    println!("table now holds {}", count.data_utf8_lossy().trim());

    Ok(())
}
