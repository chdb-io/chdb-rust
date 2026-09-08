# chdb-rust Examples

This document provides simple and easy-to-follow examples for using chdb-rust, a Rust wrapper for chDB (embedded ClickHouse).

## Table of Contents

1. [Basic Setup](#basic-setup)
2. [Stateless Queries](#stateless-queries)
3. [Stateful Sessions](#stateful-sessions)
4. [Working with Query Results](#working-with-query-results)
5. [Output Formats](#output-formats)
6. [Reading from Files](#reading-from-files)
7. [Error Handling](#error-handling)
8. [Fast Bulk Inserts (Arrow)](#fast-bulk-inserts-arrow)

## Basic Setup

First, add `chdb-rust` to your `Cargo.toml`:

```toml
[dependencies]
chdb-rust = "1.0.0"
```

Make sure you have `libchdb` installed on your system. See the main README for installation instructions.

## Stateless Queries

For one-off queries that don't require persistent storage, use the `execute` function:

```rust
use chdb_rust::execute;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;

fn main() -> Result<(), chdb_rust::error::Error> {
    // Simple query with default format (TabSeparated)
    let result = execute("SELECT 1 + 1 AS sum", None)?;
    println!("Result: {}", result.data_utf8_lossy());
    
    // Query with JSON output format
    let result = execute(
        "SELECT 'Hello' AS greeting, 42 AS answer",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
    )?;
    println!("JSON Result: {}", result.data_utf8_lossy());
    
    Ok(())
}
```

## Stateful Sessions

For queries that need persistent storage (creating tables, inserting data, etc.), use a `Session`:

```rust
use chdb_rust::session::SessionBuilder;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
use std::path::PathBuf;

fn main() -> Result<(), chdb_rust::error::Error> {
    // Create a session with a temporary directory
    let tmp_dir = std::env::temp_dir().join("chdb-example");
    let session = SessionBuilder::new()
        .with_data_path(tmp_dir)
        .with_auto_cleanup(true) // Automatically delete data on drop
        .build()?;
    
    // Create a database
    session.execute(
        "CREATE DATABASE mydb; USE mydb",
        Some(&[Arg::MultiQuery])
    )?;
    
    // Create a table
    session.execute(
        "CREATE TABLE users (id UInt64, name String, age UInt8) \
         ENGINE = MergeTree() ORDER BY id",
        None
    )?;
    
    // Insert data
    session.execute(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)",
        None
    )?;
    
    // Query data
    let result = session.execute(
        "SELECT * FROM users",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
    )?;
    
    println!("Users: {}", result.data_utf8_lossy());
    
    Ok(())
}
```

## Working with Query Results

The `QueryResult` type provides several methods to access query results:

```rust
use chdb_rust::execute;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;

fn main() -> Result<(), chdb_rust::error::Error> {
    let result = execute(
        "SELECT number, number * 2 AS doubled FROM numbers(5)",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
    )?;
    
    // Get result as UTF-8 string (returns error if invalid UTF-8)
    match result.data_utf8() {
        Ok(data) => println!("UTF-8: {}", data),
        Err(e) => eprintln!("Error: {}", e),
    }
    
    // Get result as UTF-8 string (lossy conversion for invalid UTF-8)
    println!("Lossy UTF-8: {}", result.data_utf8_lossy());
    
    // Get raw bytes
    let bytes = result.data_ref();
    println!("Bytes length: {}", bytes.len());
    
    // Get query statistics
    println!("Rows read: {}", result.rows_read());
    println!("Bytes read: {}", result.bytes_read());
    println!("Elapsed time: {:?}", result.elapsed());
    
    Ok(())
}
```

### Streaming Results

For large result sets, avoid loading everything into a `QueryResult` at once. Use streaming instead:

- **Text or other output formats** — [`examples/09_query_streaming.rs`](../examples/09_query_streaming.rs) shows how to pull result chunks with `Session::execute_stream` and a format such as `JSONEachRow`.
- **Arrow IPC stream bytes** — [`examples/10_query_streaming_arrow.rs`](../examples/10_query_streaming_arrow.rs) streams raw Arrow IPC data through `execute_stream` with `OutputFormat::ArrowStream`, then decodes each chunk with the Arrow `StreamReader`.
- **Typed Arrow record batches** — [`examples/11_arrow_query_stream.rs`](../examples/11_arrow_query_stream.rs) uses `Session::execute_stream_arrow` to receive `RecordBatch` values directly via the Arrow C Data Interface, with no IPC serialization. Requires the `arrow` feature: `cargo run --example 11_arrow_query_stream`.

## Output Formats

chdb-rust supports many output formats. Here are some common ones:

```rust
use chdb_rust::execute;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;

fn main() -> Result<(), chdb_rust::error::Error> {
    let query = "SELECT 1 AS a, 'test' AS b";
    
    // JSONEachRow - one JSON object per line
    let result = execute(
        query,
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
    )?;
    println!("JSONEachRow:\n{}", result.data_utf8_lossy());
    
    // CSV with column names
    let result = execute(
        query,
        Some(&[Arg::OutputFormat(OutputFormat::CSVWithNames)])
    )?;
    println!("CSV:\n{}", result.data_utf8_lossy());
    
    // Pretty format (human-readable table)
    let result = execute(
        query,
        Some(&[Arg::OutputFormat(OutputFormat::Pretty)])
    )?;
    println!("Pretty:\n{}", result.data_utf8_lossy());
    
    // TabSeparated (default)
    let result = execute(query, None)?;
    println!("TabSeparated:\n{}", result.data_utf8_lossy());
    
    Ok(())
}
```

## Reading from Files

You can query data directly from files using ClickHouse's `file()` function:

```rust
use chdb_rust::execute;
use chdb_rust::arg::Arg;
use chdb_rust::format::{InputFormat, OutputFormat};

fn main() -> Result<(), chdb_rust::error::Error> {
    // Read from a CSV file
    let query = format!(
        "SELECT * FROM file('data.csv', {})",
        InputFormat::CSV.as_str()
    );
    
    let result = execute(
        &query,
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
    )?;
    
    println!("CSV data:\n{}", result.data_utf8_lossy());
    
    // Read from a JSON file
    let query = format!(
        "SELECT * FROM file('data.json', {})",
        InputFormat::JSONEachRow.as_str()
    );
    
    let result = execute(
        &query,
        Some(&[Arg::OutputFormat(OutputFormat::Pretty)])
    )?;
    
    println!("JSON data:\n{}", result.data_utf8_lossy());
    
    Ok(())
}
```

## Error Handling

Always handle errors properly:

```rust
use chdb_rust::execute;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
use chdb_rust::error::Error;

fn main() {
    match execute(
        "SELECT * FROM nonexistent_table",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
    ) {
        Ok(result) => {
            println!("Success: {}", result.data_utf8_lossy());
        }
        Err(Error::QueryError(msg)) => {
            eprintln!("Query error: {}", msg);
        }
        Err(Error::ConnectionFailed) => {
            eprintln!("Failed to connect to database");
        }
        Err(e) => {
            eprintln!("Other error: {}", e);
        }
    }
}
```

## Complete Example: Building a Simple Analytics Query

Here's a complete example that demonstrates a typical use case:

```rust
use chdb_rust::session::SessionBuilder;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;

fn main() -> Result<(), chdb_rust::error::Error> {
    // Create session
    let tmp_dir = std::env::temp_dir().join("chdb-analytics");
    let session = SessionBuilder::new()
        .with_data_path(tmp_dir)
        .with_auto_cleanup(true)
        .build()?;
    
    // Create database and table
    session.execute(
        "CREATE DATABASE analytics; USE analytics",
        Some(&[Arg::MultiQuery])
    )?;
    
    session.execute(
        "CREATE TABLE events (
            id UInt64,
            event_type String,
            timestamp DateTime,
            value Float64
        ) ENGINE = MergeTree() ORDER BY timestamp",
        None
    )?;
    
    // Insert sample events
    session.execute(
        "INSERT INTO events VALUES
        (1, 'page_view', '2024-01-01 10:00:00', 1.0),
        (2, 'click', '2024-01-01 10:05:00', 2.5),
        (3, 'page_view', '2024-01-01 10:10:00', 1.0),
        (4, 'purchase', '2024-01-01 10:15:00', 99.99),
        (5, 'page_view', '2024-01-01 10:20:00', 1.0)",
        None
    )?;
    
    // Aggregate query
    let result = session.execute(
        "SELECT 
            event_type,
            COUNT(*) AS count,
            SUM(value) AS total_value,
            AVG(value) AS avg_value
        FROM events
        GROUP BY event_type
        ORDER BY count DESC",
        Some(&[Arg::OutputFormat(OutputFormat::Pretty)])
    )?;
    
    println!("Event Statistics:\n{}", result.data_utf8_lossy());
    
    // Time-based query
    let result = session.execute(
        "SELECT 
            toStartOfHour(timestamp) AS hour,
            COUNT(*) AS events_per_hour
        FROM events
        GROUP BY hour
        ORDER BY hour",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
    )?;
    
    println!("\nHourly Events:\n{}", result.data_utf8_lossy());
    
    Ok(())
}
```

## Fast Bulk Inserts (Arrow)

For high throughput and low latency, register in-memory Arrow data and insert with `INSERT ... SELECT` from the `ArrowStream` table function. This avoids temp-file Arrow IPC and SQL `VALUES` parsing.

### Ranked ingest paths

| Rank | Method | Notes |
|------|--------|-------|
| 1 | `register_arrow_stream` + `INSERT INTO dest SELECT * FROM ArrowStream('n')` | Multi-batch; no disk hop |
| 2 | `register_arrow_array` + same SQL | Single `RecordBatch` |
| 3 | `file(path, 'Native')` | Pre-encoded Native files |
| 4 | `file(path, 'RowBinary')` | Dense binary; requires `structure` in SQL |
| 5 | `file(path, 'ArrowStream')` | Arrow IPC on disk |
| 6 | `file(path, 'Parquet')` | Cold bulk loads |
| 7 | `INSERT ... VALUES` | Fine for demos; worst at scale |

`OutputFormat` on `query()` / `execute()` affects **result** encoding only, not ingest speed.

### SQL syntax

Registration exposes a **table function**, not a MergeTree table:

```sql
-- Correct
INSERT INTO dest SELECT * FROM ArrowStream('my_batch');

-- Wrong (UNKNOWN_TABLE)
SELECT * FROM my_batch;
```

In Rust, build the expression with `arrow_stream_table_sql("my_batch")`.

### FFI lifetime rules

- Pass raw `FFI_ArrowArrayStream` / `FFI_ArrowSchema` / `FFI_ArrowArray` pointers via [`ArrowStream::from_raw`](https://docs.rs/chdb-rust/latest/chdb_rust/arrow_stream/struct.ArrowStream.html) (not the opaque `chdb_arrow_stream_` typedef in `chdb.h`).
- Keep backing `RecordBatch` data and FFI structs alive until `unregister_arrow_table` or the connection closes.
- `chdb_arrow_scan` does not take ownership of the stream; do not free it before unregister.

### High-level helpers

```rust
use std::sync::Arc;
use chdb_rust::arrow::array::{Int64Array, RecordBatch};
use chdb_rust::arrow::datatypes::{DataType, Field, Schema};
use chdb_rust::arrow_insert::insert_record_batch;
use chdb_rust::session::SessionBuilder;

let session = SessionBuilder::new()
    .with_data_path("/tmp/chdb-ingest")
    .with_auto_cleanup(true)
    .build()?;

session.execute(
    "CREATE TABLE IF NOT EXISTS metrics (id Int64) ENGINE=MergeTree ORDER BY id",
    None,
)?;

let batch = RecordBatch::try_new(
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
    vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
)?;

session.insert_record_batch("metrics", "flush_1", &batch)?;
```

`insert_record_batches` accepts multiple batches via an Arrow `RecordBatchReader`.

`dest_table` must be a bare ClickHouse table identifier (e.g. `metrics`); dotted names (`db.table`) and reserved words are not quoted automatically.

See `examples/08_arrow_insert.rs` for a runnable program.

## Additional Resources

- For more information about chDB, visit: https://github.com/chdb-io/chdb
- For ClickHouse SQL reference: https://clickhouse.com/docs/en/sql-reference/
- Check the `tests/examples.rs` file in this repository for more examples

