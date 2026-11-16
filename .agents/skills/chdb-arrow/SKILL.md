---
name: chdb-arrow
description: Choose chdb-rust Arrow reader registration, typed result streams, bulk insert, or an existing standard Arrow database driver. Not for ordinary SQL queries.
---

# Rust Arrow path selection

Arrow Database Connectivity (ADBC) is a separate standard database-driver API. Here a reader supplies columnar batches on demand; scoped registration makes those batches available as a temporary SQL source while the reader remains alive.

- In `chdb-rust` 2.2.x the `arrow` feature is on by default. The tested native baseline is `chdb-core v26.11.1`; a normal build fetches its matching library and header. A caller-supplied `CHDB_LIB_DIR` needs a matching header and is not covered by the fetched-engine runtime version check.
- For in-memory SQL over a one-shot reader use `conn.register_arrow_reader(Box<dyn RecordBatchReader + Send>)`, its `sql_expr()`, and the registration's `query_arrow(sql)` / `query_stream_arrow(sql)`. Finish, cancel, or drop the stream before `close()` or drop of the registration; the mutable borrow prevents an early close. Prefer this scoped API to raw `register_arrow_stream` / `register_arrow_array`, whose native-pointer lifetimes belong to the caller.
- Typed results: `conn.query_arrow(sql)` for a whole query's Arrow reader, or `conn.query_stream_arrow(sql)` with `next_batch()` and `cancel` / drop on early exit. Bulk insert: `insert_record_batch_direct` for one batch, `insert_record_batches` for several, or `insert_record_batch_reader` for a lazy reader, with `InsertOptions`. The insert path may use direct foreign-function interface (FFI) calls or registered-stream SQL according to the engine build; neither result path needs Arrow IPC serialization, though engine conversion may copy buffers.
- If an application already has a verified ADBC client/driver pairing, ADBC suits standard result streams or bulk ingest. General `BindStream` does not turn the stream into `FROM` input. The `arrow` crate feature is on by default; keep input and output owners alive until native consumption ends. Start with `docs/arrow.md` for this binding's documentation map and examples.
