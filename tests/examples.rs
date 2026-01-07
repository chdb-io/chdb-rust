//! Test examples for chdb-rust
//!
//! Note: These tests may fail when run in parallel due to connection resource contention
//! in the chDB library. Run with `RUST_TEST_THREADS=1 cargo test --test examples`
//! to ensure reliable execution.

use chdb_rust::arg::Arg;
use chdb_rust::error::Result;
use chdb_rust::execute;
use chdb_rust::format::InputFormat;
use chdb_rust::format::OutputFormat;
use chdb_rust::log_level::LogLevel;
use chdb_rust::session::SessionBuilder;
use std::fs;

#[test]
fn test_stateful() -> Result<()> {
    //
    // Create session.
    //
    let tmp = tempdir::TempDir::new("chdb-rust")?;
    let session = SessionBuilder::new()
        .with_data_path(tmp.path())
        .with_arg(Arg::LogLevel(LogLevel::Debug))
        .with_arg(Arg::Custom("priority".into(), Some("1".into())))
        .with_auto_cleanup(true)
        .build()?;

    //
    // Create database.
    //

    session.execute("CREATE DATABASE demo; USE demo", Some(&[Arg::MultiQuery]))?;

    //
    // Create table.
    //

    session.execute(
        "CREATE TABLE logs (id UInt64, msg String) ENGINE = MergeTree() ORDER BY id",
        None,
    )?;

    //
    // Insert into table.
    //

    session.execute("INSERT INTO logs (id, msg) VALUES (1, 'test')", None)?;

    //
    // Select from table.
    //
    let len = session.execute(
        "SELECT COUNT(*) FROM logs",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    assert_eq!(len.data_utf8_lossy(), "{\"COUNT()\":1}\n");

    let result = session.execute(
        "SELECT * FROM logs",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;
    assert_eq!(result.data_utf8_lossy(), "{\"id\":1,\"msg\":\"test\"}\n");
    Ok(())
}

#[test]
fn test_stateless() -> Result<()> {
    let query = format!(
        "SELECT * FROM file('tests/logs.csv', {})",
        InputFormat::CSV.as_str()
    );

    let result = execute(
        &query,
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    assert_eq!(result.data_utf8_lossy(), "{\"id\":1,\"msg\":\"test\"}\n");
    Ok(())
}

#[test]
fn test_sql_syntax_error() {
    let result = execute("aaa", Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]));
    assert!(result.is_err(), "Expected error for invalid SQL");
}

#[test]
fn test_output_formats() -> Result<()> {
    let query = "SELECT 1 AS a, 'test' AS b, 3.14 AS pi";

    // Test JSONEachRow
    let result = execute(query, Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]))?;
    let json_output = result.data_utf8_lossy();
    assert!(json_output.contains("\"a\":1"));
    assert!(json_output.contains("\"b\":\"test\""));

    // Test CSVWithNames
    let result = execute(
        query,
        Some(&[Arg::OutputFormat(OutputFormat::CSVWithNames)]),
    )?;
    let csv_output = result.data_utf8_lossy();
    // CSV format may have quotes or different formatting, so check for key elements
    assert!(csv_output.contains("a") && csv_output.contains("b") && csv_output.contains("pi"));
    assert!(csv_output.contains("1") && csv_output.contains("test"));
    assert!(!csv_output.is_empty());

    // Test Pretty format
    let result = execute(query, Some(&[Arg::OutputFormat(OutputFormat::Pretty)]))?;
    let pretty_output = result.data_utf8_lossy();
    assert!(!pretty_output.is_empty());

    // Test TabSeparated (default)
    let result = execute(query, None)?;
    let ts_output = result.data_utf8_lossy();
    assert!(ts_output.contains("1"));
    assert!(ts_output.contains("test"));

    Ok(())
}

#[test]
fn test_query_result_statistics() -> Result<()> {
    let result = execute(
        "SELECT number FROM numbers(100)",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    // Check that we read rows
    assert!(result.rows_read() > 0);
    assert_eq!(result.rows_read(), 100);

    // Check that we read bytes
    assert!(result.bytes_read() > 0);

    // Check elapsed time (should be very small but >= 0)
    let elapsed = result.elapsed();
    assert!(elapsed.as_secs_f64() >= 0.0);

    // Check data is not empty
    let data = result.data_utf8_lossy();
    assert!(!data.is_empty());
    assert!(data.contains("\"number\":0"));
    assert!(data.contains("\"number\":99"));

    Ok(())
}

#[test]
fn test_query_result_data_methods() -> Result<()> {
    let result = execute(
        "SELECT 'Hello, World!' AS greeting",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    // Test data_utf8_lossy
    let lossy = result.data_utf8_lossy();
    assert!(lossy.contains("Hello, World!"));

    // Test data_utf8 (should work for valid UTF-8)
    let result = execute(
        "SELECT 'Hello, World!' AS greeting",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;
    let utf8 = result.data_utf8()?;
    assert!(utf8.contains("Hello, World!"));

    // Test data_ref
    let result = execute(
        "SELECT 'Hello' AS msg",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;
    let bytes = result.data_ref();
    assert!(!bytes.is_empty());

    Ok(())
}

#[test]
fn test_multiple_inserts_and_aggregation() -> Result<()> {
    let tmp = tempdir::TempDir::new("chdb-rust")?;
    let session = SessionBuilder::new()
        .with_data_path(tmp.path())
        .with_auto_cleanup(true)
        .build()?;

    session.execute(
        "CREATE DATABASE testdb; USE testdb",
        Some(&[Arg::MultiQuery]),
    )?;

    session.execute(
        "CREATE TABLE products (id UInt64, name String, price Float64) \
         ENGINE = MergeTree() ORDER BY id",
        None,
    )?;

    // Insert multiple rows
    session.execute(
        "INSERT INTO products VALUES \
         (1, 'Apple', 1.50), \
         (2, 'Banana', 0.75), \
         (3, 'Orange', 2.00), \
         (4, 'Apple', 1.50)",
        None,
    )?;

    // Test aggregation
    let result = session.execute(
        "SELECT name, COUNT(*) AS count, SUM(price) AS total, AVG(price) AS avg_price \
         FROM products GROUP BY name ORDER BY name",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    let output = result.data_utf8_lossy();
    assert!(output.contains("Apple"));
    assert!(output.contains("Banana"));
    assert!(output.contains("Orange"));
    assert_eq!(result.rows_read(), 3);

    // Test filtering
    let result = session.execute(
        "SELECT COUNT(*) FROM products WHERE price > 1.0",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;
    assert!(result.data_utf8_lossy().contains("\"COUNT()\":3"));

    Ok(())
}

#[test]
fn test_different_data_types() -> Result<()> {
    let tmp = tempdir::TempDir::new("chdb-rust")?;
    let session = SessionBuilder::new()
        .with_data_path(tmp.path())
        .with_auto_cleanup(true)
        .build()?;

    session.execute(
        "CREATE DATABASE testdb; USE testdb",
        Some(&[Arg::MultiQuery]),
    )?;

    session.execute(
        "CREATE TABLE types_test (
            id UInt64,
            name String,
            age UInt8,
            salary Float64,
            active Bool,
            created Date
        ) ENGINE = MergeTree() ORDER BY id",
        None,
    )?;

    session.execute(
        "INSERT INTO types_test VALUES \
         (1, 'Alice', 30, 50000.5, true, '2024-01-01'), \
         (2, 'Bob', 25, 45000.0, false, '2024-01-02')",
        None,
    )?;

    let result = session.execute(
        "SELECT * FROM types_test ORDER BY id",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    let output = result.data_utf8_lossy();
    assert!(output.contains("\"id\":1"));
    assert!(output.contains("\"name\":\"Alice\""));
    assert!(output.contains("\"age\":30"));
    assert!(output.contains("\"active\":true"));
    assert_eq!(result.rows_read(), 2);

    Ok(())
}

#[test]
fn test_error_handling_table_not_found() {
    let result = execute(
        "SELECT * FROM nonexistent_table_12345",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    );
    assert!(result.is_err(), "Expected error for non-existent table");

    if let Err(e) = result {
        match e {
            chdb_rust::error::Error::QueryError(_) => {}
            _ => {
                panic!("Expected QueryError, got {e:?}");
            }
        }
    }
}

#[test]
fn test_error_handling_invalid_syntax() {
    let result = execute(
        "SELECT * FROM WHERE invalid",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    );
    assert!(result.is_err(), "Expected error for invalid SQL syntax");
}

#[test]
fn test_session_auto_cleanup() -> Result<()> {
    let tmp = tempdir::TempDir::new("chdb-rust")?;
    let data_path = tmp.path().to_path_buf();

    {
        let session = SessionBuilder::new()
            .with_data_path(&data_path)
            .with_auto_cleanup(true)
            .build()?;

        session.execute(
            "CREATE DATABASE testdb; USE testdb",
            Some(&[Arg::MultiQuery]),
        )?;
        session.execute(
            "CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY id",
            None,
        )?;
        session.execute("INSERT INTO test VALUES (1)", None)?;

        // Session should exist and work
        let result = session.execute("SELECT COUNT(*) FROM test", None)?;
        assert_eq!(result.rows_read(), 1);

        // Check the folder was created
        assert!(fs::metadata(&data_path).is_ok());
    } // Session dropped here, auto_cleanup should trigger

    // Check the folder was deleted
    assert!(fs::metadata(&data_path).is_err());

    Ok(())
}

#[test]
fn test_complex_query_with_joins() -> Result<()> {
    let tmp = tempdir::TempDir::new("chdb-rust")?;
    let session = SessionBuilder::new()
        .with_data_path(tmp.path())
        .with_auto_cleanup(true)
        .build()?;

    session.execute(
        "CREATE DATABASE testdb; USE testdb",
        Some(&[Arg::MultiQuery]),
    )?;

    // Create orders table
    session.execute(
        "CREATE TABLE orders (id UInt64, customer_id UInt64, total Float64) \
         ENGINE = MergeTree() ORDER BY id",
        None,
    )?;

    // Create customers table
    session.execute(
        "CREATE TABLE customers (id UInt64, name String) \
         ENGINE = MergeTree() ORDER BY id",
        None,
    )?;

    session.execute(
        "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')",
        None,
    )?;

    session.execute(
        "INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 50.0), (3, 2, 75.0)",
        None,
    )?;

    // Test join query
    let result = session.execute(
        "SELECT c.name, COUNT(o.id) AS order_count, SUM(o.total) AS total_spent \
         FROM customers c \
         LEFT JOIN orders o ON c.id = o.customer_id \
         GROUP BY c.name \
         ORDER BY c.name",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    let output = result.data_utf8_lossy();
    assert!(output.contains("Alice"));
    assert!(output.contains("Bob"));
    assert_eq!(result.rows_read(), 2);

    Ok(())
}

#[test]
fn test_numbers_function() -> Result<()> {
    // Test ClickHouse numbers() function
    let result = execute(
        "SELECT number, number * 2 AS doubled FROM numbers(10) WHERE number < 5",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    assert_eq!(result.rows_read(), 5);
    let output = result.data_utf8_lossy();
    assert!(output.contains("\"number\":0"));
    assert!(output.contains("\"number\":4"));
    assert!(output.contains("\"doubled\":0"));
    assert!(output.contains("\"doubled\":8"));

    Ok(())
}

#[test]
fn test_default_output_format() -> Result<()> {
    // Test that default format (TabSeparated) works
    let result = execute("SELECT 1 AS a, 'test' AS b", None)?;
    let output = result.data_utf8_lossy();
    assert!(!output.is_empty());
    assert!(output.contains("1"));
    assert!(output.contains("test"));

    Ok(())
}

#[test]
fn test_session_without_auto_cleanup() -> Result<()> {
    let tmp = tempdir::TempDir::new("chdb-rust")?;
    let session = SessionBuilder::new()
        .with_data_path(tmp.path())
        .with_auto_cleanup(false) // Explicitly disable cleanup
        .build()?;

    session.execute(
        "CREATE DATABASE testdb; USE testdb",
        Some(&[Arg::MultiQuery]),
    )?;
    session.execute(
        "CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY id",
        None,
    )?;
    session.execute("INSERT INTO test VALUES (1), (2), (3)", None)?;

    let result = session.execute("SELECT COUNT(*) FROM test", None)?;
    assert_eq!(result.rows_read(), 1);

    // Check the folder was not deleted
    assert!(fs::metadata(tmp.path()).is_ok());

    Ok(())
}
