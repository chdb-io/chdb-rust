/// Example: Output Formats
/// 
/// This example demonstrates the various output formats supported by chdb-rust.

use chdb_rust::execute;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;

fn main() -> Result<(), chdb_rust::error::Error> {
    println!("=== Output Formats Examples ===\n");
    
    let query = "SELECT 1 AS a, 'test' AS b, 3.14 AS pi";
    
    println!("1. JSONEachRow - one JSON object per line:");
    let result = execute(
        query,
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)])
    )?;
    println!("{}", result.data_utf8_lossy());
    println!();
    
    println!("2. CSV with column names:");
    let result = execute(
        query,
        Some(&[Arg::OutputFormat(OutputFormat::CSVWithNames)])
    )?;
    println!("{}", result.data_utf8_lossy());
    println!();
    
    println!("3. Pretty format (human-readable table):");
    let result = execute(
        query,
        Some(&[Arg::OutputFormat(OutputFormat::Pretty)])
    )?;
    println!("{}", result.data_utf8_lossy());
    println!();
    
    println!("4. TabSeparated (default):");
    let result = execute(query, None)?;
    println!("{}", result.data_utf8_lossy());
    println!();
    
    println!("5. JSON (single JSON object):");
    let result = execute(
        query,
        Some(&[Arg::OutputFormat(OutputFormat::JSON)])
    )?;
    println!("{}", result.data_utf8_lossy());
    println!();
    
    println!("6. Markdown table:");
    let result = execute(
        query,
        Some(&[Arg::OutputFormat(OutputFormat::Markdown)])
    )?;
    println!("{}", result.data_utf8_lossy());
    
    Ok(())
}

