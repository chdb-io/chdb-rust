use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
/// Example: Stateful Sessions
///
/// This example demonstrates how to use sessions for queries that need
/// persistent storage (creating tables, inserting data, etc.).
use chdb_rust::session::SessionBuilder;

fn main() -> Result<(), chdb_rust::error::Error> {
    println!("=== Stateful Session Examples ===\n");

    // Create a session with a temporary directory
    let tmp_dir = std::env::temp_dir().join("chdb-example");
    let session = SessionBuilder::new()
        .with_data_path(tmp_dir)
        .with_auto_cleanup(true) // Automatically delete data on drop
        .build()?;

    println!("1. Creating database and table...");

    // Create a database
    session.execute("CREATE DATABASE mydb; USE mydb", Some(&[Arg::MultiQuery]))?;

    // Create a table
    session.execute(
        "CREATE TABLE users (id UInt64, name String, age UInt8) \
         ENGINE = MergeTree() ORDER BY id",
        None,
    )?;

    println!("2. Inserting data...");

    // Insert data
    session.execute(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)",
        None,
    )?;

    println!("3. Querying data...\n");

    // Query data
    let result = session.execute(
        "SELECT * FROM users",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;

    println!("Users:");
    println!("{}", result.data_utf8_lossy());

    // Query with aggregation
    println!("4. Aggregated query:");
    let result = session.execute(
        "SELECT COUNT(*) AS total_users, AVG(age) AS avg_age FROM users",
        Some(&[Arg::OutputFormat(OutputFormat::Pretty)]),
    )?;
    println!("{}", result.data_utf8_lossy());

    Ok(())
}
