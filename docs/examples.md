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

## Additional Resources

- For more information about chDB, visit: https://github.com/chdb-io/chdb
- For ClickHouse SQL reference: https://clickhouse.com/docs/en/sql-reference/
- Check the `tests/examples.rs` file in this repository for more examples

