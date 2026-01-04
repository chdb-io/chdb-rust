use chdb_rust::arg::Arg;
use chdb_rust::error::Error;
/// Example: Error Handling
///
/// This example demonstrates how to properly handle errors when working
/// with chdb-rust.
use chdb_rust::execute;
use chdb_rust::format::OutputFormat;

fn main() {
    println!("=== Error Handling Examples ===\n");

    println!("1. Handling query errors:");
    match execute(
        "SELECT * FROM nonexistent_table",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    ) {
        Ok(result) => {
            println!("Success: {}", result.data_utf8_lossy());
        }
        Err(Error::QueryError(msg)) => {
            println!("Query error caught: {msg}");
        }
        Err(Error::ConnectionFailed) => {
            eprintln!("Failed to connect to database");
        }
        Err(e) => {
            eprintln!("Other error: {e}");
        }
    }
    println!();

    println!("2. Handling syntax errors:");
    match execute(
        "SELECT * FROM WHERE invalid syntax",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    ) {
        Ok(result) => {
            println!("Unexpected success: {}", result.data_utf8_lossy());
        }
        Err(Error::QueryError(msg)) => {
            println!("Syntax error caught: {msg}");
        }
        Err(e) => {
            println!("Error: {e}");
        }
    }
    println!();

    println!("3. Successful query:");
    match execute(
        "SELECT 1 + 1 AS result",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    ) {
        Ok(result) => {
            println!("Success: {}", result.data_utf8_lossy());
        }
        Err(e) => {
            eprintln!("Unexpected error: {e}");
        }
    }
    println!();

    println!("4. Using ? operator in a function:");
    if let Err(e) = demonstrate_error_propagation() {
        println!("Error propagated: {e}");
    }
}

fn demonstrate_error_propagation() -> Result<(), Error> {
    // This function demonstrates how errors can be propagated using ?
    let result = execute(
        "SELECT number FROM numbers(3)",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    println!("Result from function: {}", result.data_utf8_lossy());
    Ok(())
}
