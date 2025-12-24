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
```

## Example Files

1. **01_stateless_queries.rs** - Basic stateless queries using the `execute` function
2. **02_stateful_sessions.rs** - Creating sessions, databases, and tables with persistent storage
3. **03_query_results.rs** - Working with query results and accessing statistics
4. **04_output_formats.rs** - Demonstrating different output formats (JSON, CSV, Pretty, etc.)
5. **05_reading_from_files.rs** - Querying data from CSV and JSON files
6. **06_error_handling.rs** - Proper error handling patterns
7. **07_analytics.rs** - Complete analytics example with event tracking and aggregation

## Prerequisites

Make sure you have `libchdb` installed on your system. See the main README for installation instructions.

## Building All Examples

To build all examples without running them:

```bash
cargo build --examples
```

