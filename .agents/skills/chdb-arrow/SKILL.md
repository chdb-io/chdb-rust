---
name: chdb-arrow
description: Choose chdb-rust Arrow reader registration, typed result streams, bulk insert, or an existing standard Arrow database driver. Not for ordinary SQL queries.
---

# Rust Arrow path selection

Arrow Database Connectivity (ADBC) is a separate standard database-driver API. Here a reader supplies columnar batches on demand; scoped registration makes those batches available as a temporary SQL source while the reader remains alive.

- For in-memory SQL over a one-shot reader use `conn.register_arrow_reader(Box<dyn RecordBatchReader + Send>)`, its `sql_expr()`, and the registration's `query_arrow(sql)` / `query_stream_arrow(sql)`; then `close()` or drop it. Prefer this scoped API to raw `register_arrow_stream` / `register_arrow_array`, whose native-pointer lifetimes belong to the caller.
- Typed results: `conn.query_arrow(sql)` for a whole query's Arrow reader, or `conn.query_stream_arrow(sql)` with `next_batch()` and `cancel` / drop on early exit. Bulk insert: `insert_record_batch` or `insert_record_batch_reader` with `InsertOptions`; the reader path may use the foreign-function interface (FFI) directly or registered-stream SQL according to the selected engine build.
- If an application already has a verified ADBC client/driver pairing, ADBC suits standard result streams or bulk ingest. General `BindStream` does not turn the stream into `FROM` input. The `arrow` crate feature is on by default; keep input and output owners alive until native consumption ends. Start with `docs/arrow.md` for this binding's documentation map and examples.
