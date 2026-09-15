//! One-shot Arrow export, and the type-mapping options that control it.

use arrow::array::RecordBatchReader;
use chdb_rust::arrow_options::ArrowOptions;
use chdb_rust::connection::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;

    let reader = conn.query_arrow("SELECT number, toString(number) AS s FROM numbers(1000)")?;
    println!("schema: {}", reader.schema());
    let rows: usize = reader.map(|b| b.map(|b| b.num_rows()).unwrap_or(0)).sum();
    println!("rows: {rows}");

    // LowCardinality is materialized to its base type by default; ask for a
    // dictionary array instead.
    let opts = ArrowOptions {
        low_cardinality_as_dictionary: true,
        ..ArrowOptions::default()
    };
    let reader = conn.query_arrow_with_opts(
        "SELECT CAST(name, 'LowCardinality(String)') AS name
         FROM (SELECT 'alpha' AS name UNION ALL SELECT 'beta')",
        &opts,
    )?;
    println!("dictionary schema: {}", reader.schema());

    Ok(())
}
