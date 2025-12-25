/// Example: Building a Simple Analytics Query
/// 
/// This is a complete example that demonstrates a typical analytics use case
/// with event tracking, aggregation, and time-based queries.

use chdb_rust::session::SessionBuilder;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;

fn main() -> Result<(), chdb_rust::error::Error> {
    println!("=== Analytics Example ===\n");
    
    // Create session
    let tmp_dir = std::env::temp_dir().join("chdb-analytics");
    let session = SessionBuilder::new()
        .with_data_path(tmp_dir)
        .with_auto_cleanup(true)
        .build()?;
    
    println!("1. Creating database and table...");
    
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
    
    println!("2. Inserting sample events...");
    
    // Insert sample events
    session.execute(
        "INSERT INTO events VALUES
        (1, 'page_view', '2024-01-01 10:00:00', 1.0),
        (2, 'click', '2024-01-01 10:05:00', 2.5),
        (3, 'page_view', '2024-01-01 10:10:00', 1.0),
        (4, 'purchase', '2024-01-01 10:15:00', 99.99),
        (5, 'page_view', '2024-01-01 10:20:00', 1.0),
        (6, 'click', '2024-01-01 10:25:00', 1.5),
        (7, 'purchase', '2024-01-01 10:30:00', 49.99),
        (8, 'page_view', '2024-01-01 11:00:00', 1.0),
        (9, 'click', '2024-01-01 11:05:00', 3.0),
        (10, 'page_view', '2024-01-01 11:10:00', 1.0)",
        None
    )?;
    
    println!("3. Event statistics by type:\n");
    
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
    
    println!("{}", result.data_utf8_lossy());
    println!();
    
    println!("4. Hourly event distribution:\n");
    
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
    
    println!("{}", result.data_utf8_lossy());
    println!();
    
    println!("5. Conversion funnel:\n");
    
    // Conversion funnel analysis
    let result = session.execute(
        "SELECT 
            event_type,
            COUNT(*) AS count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM events), 2) AS percentage
        FROM events
        GROUP BY event_type
        ORDER BY 
            CASE event_type
                WHEN 'page_view' THEN 1
                WHEN 'click' THEN 2
                WHEN 'purchase' THEN 3
                ELSE 4
            END",
        Some(&[Arg::OutputFormat(OutputFormat::Pretty)])
    )?;
    
    println!("{}", result.data_utf8_lossy());
    
    Ok(())
}

