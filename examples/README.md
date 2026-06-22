# chdb-rust Examples

This directory contains runnable example programs demonstrating how to use chdb-rust.

## Running Examples

You can run any example using Cargo:

```bash
cargo run --example 01_stateless_queries
cargo run --example 02_stateful_sessions
cargo run --example 03_query_results
cargo run --example 04_output_formats
cargo run --example 05_reading_from_files
cargo run --example 06_error_handling
cargo run --example 07_analytics
cargo run --example 08_arrow_insert
cargo run --example 08_query_streaming
cargo run --example 09_query_streaming_arrow
cargo run --features arrow --example 10_arrow_query_stream
```

## Example Files

1. **01_stateless_queries.rs** - Basic stateless queries using the `execute` function
2. **02_stateful_sessions.rs** - Creating sessions, databases, and tables with persistent storage
3. **03_query_results.rs** - Working with query results and accessing statistics
4. **04_output_formats.rs** - Demonstrating different output formats (JSON, CSV, Pretty, etc.)
5. **05_reading_from_files.rs** - Querying data from CSV and JSON files
6. **06_error_handling.rs** - Proper error handling patterns
7. **07_analytics.rs** - Complete analytics example with event tracking and aggregation
8. **08_arrow_insert.rs** - Fast bulk insert via Arrow C Data Interface (`insert_record_batch`)
9. **08_query_streaming.rs** - Streaming large query results in chunks without materializing the full output
10. **09_query_streaming_arrow.rs** - Streaming large query results in chunks, decoding Arrow IPC bytes into human-readable tables
11. **10_arrow_query_stream.rs** - Streaming large query results as Arrow `RecordBatch` values via the C Data Interface (requires `--features arrow`)

## Prerequisites

Make sure you have `libchdb` installed on your system. See the main README for installation instructions.

## Building All Examples

To build all examples without running them:

```bash
cargo build --examples
```

