# Arrow in chdb-rust: documentation map

Start here for Rust Arrow usage. The `arrow` crate feature is enabled by default. Its native path exchanges Arrow column buffers and batches with chDB; the older Arrow Inter-Process Communication (IPC) format serializes batches as bytes. A typed result preserves the Arrow schema and column types. Do not treat examples of one path as evidence for the other.

| What you need | Where to look |
| --- | --- |
| Build an Arrow `RecordBatch` and bulk-insert it | [README Arrow section](../README.md#arrow-bulk-insert), [examples guide](examples.md#fast-bulk-inserts-arrow), [runnable insert example](../examples/08_arrow_insert.rs) |
| Register a one-shot Arrow reader as an in-memory SQL source | [`Connection::register_arrow_reader`](../src/connection.rs) and [scoped registration tests](../tests/arrow_registration.rs); keep the registration through the query |
| Read a typed SQL result all at once or batch by batch | [One-shot Arrow export](examples.md#one-shot-arrow-export), [whole-query example](../examples/15_arrow_query.rs), [typed streaming example](../examples/12_arrow_query_stream.rs) |
| Read or write the Arrow IPC format instead | [`11_query_streaming_arrow.rs`](../examples/11_query_streaming_arrow.rs) streams **IPC bytes**; it is not the typed C Data path in example 12 |
| Choose Arrow Database Connectivity (ADBC), the standard driver API | Use an existing, verified Rust ADBC client and the chDB driver for Arrow results/bulk ingest. General ADBC `BindStream` binds query parameters, not a temporary SQL table in `FROM`. |
| Check options and native ownership | [`ArrowOptions`](../src/arrow_options.rs), [query tests](../tests/arrow_query.rs), [insert tests](../tests/arrow_insert.rs), and the [core contract](https://github.com/chdb-io/chdb-core/blob/main/docs/NATIVE_ARROW_BINDING_CONTRACT.md) |

The source files and tests are technical references; the linked examples are the starting point for application code. Avoid copying the same long example into several documents.
