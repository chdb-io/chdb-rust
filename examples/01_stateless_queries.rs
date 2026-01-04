use chdb_rust::arg::Arg;
/// Example: Stateless Queries
///
/// This example demonstrates how to execute one-off queries that don't require
/// persistent storage using the `execute` function.
use chdb_rust::execute;
use chdb_rust::format::OutputFormat;

fn main() -> Result<(), chdb_rust::error::Error> {
    println!("=== Stateless Query Examples ===\n");

    // Simple query with default format (TabSeparated)
    println!("1. Simple query with default format:");
    let result = execute("SELECT 1 + 1 AS sum", None)?;
    println!("Result: {}", result.data_utf8_lossy());
    println!();

    // Query with JSON output format
    println!("2. Query with JSON output format:");
    let result = execute(
        "SELECT 'Hello' AS greeting, 42 AS answer",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;
    println!("JSON Result: {}", result.data_utf8_lossy());
    println!();

    // More complex query
    println!("3. Complex query with calculations:");
    let result = execute(
        "SELECT number, number * 2 AS doubled, number * number AS squared FROM numbers(5)",
        Some(&[Arg::OutputFormat(OutputFormat::Pretty)]),
    )?;
    println!("{}", result.data_utf8_lossy());

    Ok(())
}
