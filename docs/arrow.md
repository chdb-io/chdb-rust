# Arrow in chdb-rust

Use this page to choose an Apache Arrow path in `chdb-rust` 2.2.x. The `arrow` feature is enabled by default:

```toml
[dependencies]
chdb-rust = "2.2"
```

The tested native engine is [chdb-core v26.11.1](https://github.com/chdb-io/chdb-core/releases/tag/v26.11.1). A normal build downloads its matching `libchdb` and header for Linux or macOS on x86_64 or arm64. If you supply `CHDB_LIB_DIR`, also supply the matching `chdb.h` via `CHDB_INCLUDE_DIR` when necessary; the automatic runtime version check applies to a fetched engine, not to a caller-supplied library. Import Arrow types through `chdb_rust::arrow` to use the same Arrow version as the binding.

## Query an in-memory reader, insert a batch, and read typed results

This complete example uses a one-shot input reader as a temporary SQL source, then inserts its batch and retrieves typed Arrow results:

```rust
use std::sync::Arc;

use chdb_rust::arrow::array::{Int64Array, RecordBatch, RecordBatchIterator};
use chdb_rust::arrow::datatypes::{DataType, Field, Schema};
use chdb_rust::arrow_insert::insert_record_batch_direct;
use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;
use chdb_rust::InsertOptions;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )?;
    let input = RecordBatchIterator::new(vec![Ok(batch.clone())].into_iter(), schema);
    let mut conn = Connection::open_in_memory()?;

    let mut source = conn.register_arrow_reader(Box::new(input))?;
    let sql = format!("SELECT n FROM {} ORDER BY n", source.sql_expr());
    let mut scan = source.query_stream_arrow(&sql)?;
    let mut input_rows = 0;
    while let Some(batch) = scan.next_batch()? {
        input_rows += batch.num_rows();
    }
    assert_eq!(input_rows, 3);
    drop(scan); // finish or cancel the query before unregistering its input
    source.close()?;

    conn.query("CREATE TABLE events (n Int64) ENGINE=Memory", OutputFormat::Null)?;
    insert_record_batch_direct(&conn, "events", batch, InsertOptions::default_bulk())?;

    // Whole-query output: an owned Arrow reader, not Arrow IPC bytes.
    let whole = conn.query_arrow("SELECT n FROM events ORDER BY n")?;
    let rows: usize = whole.map(|batch| batch.map(|b| b.num_rows())).sum::<Result<_, _>>()?;
    assert_eq!(rows, 3);

    // Batch output: pull on demand, and cancel or drop when stopping early.
    let mut output = conn.query_stream_arrow("SELECT n FROM events ORDER BY n")?;
    if let Some(batch) = output.next_batch()? {
        println!("first batch: {} rows", batch.num_rows());
    }
    output.cancel();
    Ok(())
}
```

For an application already producing batches lazily, pass its `RecordBatchReader` directly rather than collecting it first. That reader is consumed once; do not assume a second scan can replay it. The registration owns the reader and generates a process-unique SQL name. Rust's mutable borrow prevents closing or dropping the registration during an active stream. Finish, cancel, or drop the stream first, then call `close()` or let the registration drop. The raw `register_arrow_stream` and `register_arrow_array` APIs leave C Data pointer and buffer lifetimes to the caller.

For insertion from several batches, use `insert_record_batches`; for a lazy reader, use `insert_record_batch_reader`. A single-batch `insert_record_batch_direct` uses the direct native insert entry point when available, otherwise registration plus SQL. These helpers map columns by name and report insert errors. See [bulk insert details](examples.md#fast-bulk-inserts-arrow) and the [runnable insert example](../examples/08_arrow_insert.rs).

`query_arrow` materializes the entire result before yielding its reader. `query_stream_arrow` fetches batches incrementally and supports `cancel()` or drop on early exit; query errors are returned rather than treated as end-of-stream. `ArrowOptions` controls unsupported types, `LowCardinality` dictionaries, and String/Binary mapping. The direct C Data result path avoids Arrow Inter-Process Communication (IPC) serialization, but conversion between ClickHouse columns and Arrow buffers may still copy data. The [IPC byte-stream example](../examples/11_query_streaming_arrow.rs) uses a different path.

If your application already uses a verified Arrow Database Connectivity (ADBC) driver, it may suit standard typed results or bulk ingest. General ADBC `BindStream` does not register an in-memory SQL table for `FROM` queries.

## Further references

- [Whole-result example](../examples/15_arrow_query.rs), [typed streaming example](../examples/12_arrow_query_stream.rs), and [input registration tests](../tests/arrow_registration.rs).
- [Arrow result and type tests](../tests/arrow_query.rs), [insert tests](../tests/arrow_insert.rs), and the [chdb-core v26.11.1 native Arrow contract](https://github.com/chdb-io/chdb-core/blob/v26.11.1/docs/NATIVE_ARROW_BINDING_CONTRACT.md).
