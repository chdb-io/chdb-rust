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
9. [Durable Objects](#durable-objects)

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

## Durable Objects

Enabled by `--features durable`. A durable object is a chDB database whose
authoritative state lives in storage you own — a full checkpoint plus a
statement write-ahead log under one compare-and-set `head.json` — in the layout
the Python, Node and Go bindings share, so any of them can recover an object the
others wrote.

```rust
use chdb_rust::durable::{Namespace, OpenOptions};
use chdb_rust::format::OutputFormat;

let namespace = Namespace::new("file:///var/lib/chdb-durable")?
    .with_owner("worker-1");

// `existed` tells "restored" from "created" without a second round-trip.
let (object, existed) = namespace.open("tenant-123", OpenOptions {
    database: Some("mem".to_string()),
    ..OpenOptions::default()
})?;

if !existed {
    object.execute(
        "CREATE TABLE events (id UInt64, tag String) ENGINE = MergeTree ORDER BY id",
    )?;
}

// execute() runs the statement here and buffers it for the WAL. It is not a
// durability barrier; flush_through() is.
let ticket = object.execute("INSERT INTO events VALUES (1, 'first')")?;
object.flush_through(ticket)?;

let rows = object.query("SELECT count() FROM events", OutputFormat::CSV)?;
println!("{}", String::from_utf8_lossy(&rows));

// Fold the WAL into a fresh full checkpoint, so recovery reads one archive
// rather than replaying a long chain.
object.checkpoint()?;

// close() drains, flushes and releases the lease. Dropping the handle instead
// reclaims local resources but leaves the lease to expire on its own.
object.close()?;
# Ok::<(), chdb_rust::durable::Error>(())
```

### What can be logged

Recovery re-executes the logged SQL, so a statement has to mean the same thing
twice:

```rust
// DO: compute the value in the caller and log the literal.
let now = "2026-09-07 12:00:00";
object.execute(&format!("INSERT INTO events VALUES (1, '{now}')"))?;

// DON'T: now(), rand(), generateUUIDv4(), or INSERT ... SELECT from anything
// that can change underneath you. Replay would produce different rows.
# Ok::<(), chdb_rust::durable::Error>(())
```

Every statement goes to ClickHouse's own parser before it runs, and the answer
is the gate: exactly one statement, the right class, every persistent write
inside this object's database, no embedded credential. Refusals come back as
`Category::ClassificationRefused` or `Category::SecretRefused`, and the message
never quotes the statement.

### Handling failure

```rust
use chdb_rust::durable::Category;

match object.flush() {
    Ok(published) => { /* committed, and another process can recover it */ }
    Err(e) if e.category() == Category::CommitAmbiguous => {
        // The remote may or may not have committed. Never retried blindly:
        // reopen the object and look at the manifest.
    }
    Err(e) if e.category() == Category::LeaseFenced => {
        // Another writer took the object. This handle is done.
    }
    Err(e) => return Err(e),
}
# Ok::<(), chdb_rust::durable::Error>(())
```

### Backends

A local directory (`file:///path`, `local:/path`, or a bare absolute path) comes
with `--features durable`, and is for development, tests and single-host use.

`--features durable-s3` adds S3-compatible storage — AWS, R2, MinIO — which is
what lets a different machine recover the object:

```rust
use chdb_rust::durable::Namespace;

let namespace = Namespace::new("s3://my-bucket/durable?region=eu-central-1")?;
# Ok::<(), chdb_rust::durable::Error>(())
```

Credentials come from the environment or `~/.aws/credentials`, never from the
URL. For an SSO profile, export them first:

```bash
eval "$(aws configure export-credentials --profile my-profile --format env)"
```

Any other provider is plugged in by implementing `durable::Backend` — six
methods, of which the two conditional writes carry the whole protocol — and
handing it to `Namespace::with_backend`.

See `examples/09_durable_object.rs` for a runnable program, and
[CHDB_DURABLE_V1_CONTRACT.md](https://github.com/chdb-io/chdb/blob/main/dev-docs/CHDB_DURABLE_V1_CONTRACT.md)
for the protocol itself, which is the source of truth rather than this crate.

## Additional Resources

- For more information about chDB, visit: https://github.com/chdb-io/chdb
- For ClickHouse SQL reference: https://clickhouse.com/docs/en/sql-reference/
- Check the `tests/examples.rs` file in this repository for more examples

