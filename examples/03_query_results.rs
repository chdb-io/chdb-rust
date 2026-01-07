use chdb_rust::arg::Arg;
/// Example: Working with Query Results
///
/// This example demonstrates the various methods available on QueryResult
/// to access query results and statistics.
use chdb_rust::execute;
use chdb_rust::format::OutputFormat;

fn main() -> Result<(), chdb_rust::error::Error> {
    println!("=== Query Results Examples ===\n");

    let result = execute(
        "SELECT number, number * 2 AS doubled FROM numbers(5)",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    println!("1. Getting result as UTF-8 string:");
    // Get result as UTF-8 string (returns error if invalid UTF-8)
    match result.data_utf8() {
        Ok(data) => println!("UTF-8: {data}"),
        Err(e) => eprintln!("Error: {e}"),
    }
    println!();

    // Note: We need to execute again because data_utf8() consumes the result
    let result = execute(
        "SELECT number, number * 2 AS doubled FROM numbers(5)",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    println!("2. Getting result as UTF-8 string (lossy conversion):");
    // Get result as UTF-8 string (lossy conversion for invalid UTF-8)
    println!("Lossy UTF-8: {}", result.data_utf8_lossy());
    println!();

    println!("3. Getting raw bytes:");
    // Get raw bytes
    let bytes = result.data_ref();
    println!("Bytes length: {}", bytes.len());
    println!("First 50 bytes: {:?}", &bytes[..bytes.len().min(50)]);
    println!();

    println!("4. Query statistics:");
    // Get query statistics
    println!("Rows read: {}", result.rows_read());
    println!("Bytes read: {}", result.bytes_read());
    println!("Elapsed time: {:?}", result.elapsed());

    Ok(())
}
