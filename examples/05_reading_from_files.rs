use chdb_rust::arg::Arg;
/// Example: Reading from Files
///
/// This example demonstrates how to query data directly from files using
/// ClickHouse's `file()` function.
use chdb_rust::execute;
use chdb_rust::format::{InputFormat, OutputFormat};

fn main() -> Result<(), chdb_rust::error::Error> {
    println!("=== Reading from Files Examples ===\n");

    // Note: This example assumes you have a CSV file at tests/logs.csv
    // You can modify the path to point to your own file

    println!("1. Reading from CSV file:");
    let query = format!(
        "SELECT * FROM file('tests/logs.csv', {})",
        InputFormat::CSV.as_str()
    );

    match execute(
        &query,
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    ) {
        Ok(result) => {
            println!("CSV data:\n{}", result.data_utf8_lossy());
        }
        Err(e) => {
            println!("Note: Could not read tests/logs.csv: {e}");
            println!("This is expected if the file doesn't exist.");
            println!("You can create a CSV file and update the path in this example.");
        }
    }
    println!();

    // Example with inline CSV data using VALUES
    println!("2. Using inline data (simulating file read):");
    let result = execute(
        "SELECT * FROM (SELECT 1 AS id, 'test' AS msg) FORMAT JSONEachRow",
        Some(&[Arg::OutputFormat(OutputFormat::Pretty)]),
    )?;
    println!("{}", result.data_utf8_lossy());
    println!();

    println!("3. Reading from JSON file (example query):");
    let query = format!(
        "SELECT * FROM file('data.json', {}) LIMIT 10",
        InputFormat::JSONEachRow.as_str()
    );
    println!("Query would be: {query}");
    println!("(Update the path to point to your JSON file)");

    Ok(())
}
