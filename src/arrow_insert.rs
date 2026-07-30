//! High-throughput inserts via Arrow C Data Interface registration.
//!
//! Wraps [`Connection::register_arrow_stream`](crate::connection::Connection::register_arrow_stream),
//! [`Connection::register_arrow_array`](crate::connection::Connection::register_arrow_array),
//! and `INSERT INTO … SELECT * FROM ArrowStream('name')`.

use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatchReader;
use arrow::array::{RecordBatch, RecordBatchIterator, StructArray};
use arrow::datatypes::SchemaRef;
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;

use crate::arrow_options::InsertOptions;
use crate::arrow_stream::{arrow_stream_table_sql, ArrowArray, ArrowSchema, ArrowStream};
use crate::connection::Connection;
use crate::error::Result;
use crate::format::OutputFormat;

static FALLBACK_STREAM_SEQ: AtomicU64 = AtomicU64::new(0);

/// Owns Arrow C Data Interface structs for the lifetime of a registration or insert.
struct OwnedArrowArray {
    ffi_schema: FFI_ArrowSchema,
    ffi_array: FFI_ArrowArray,
}

impl OwnedArrowArray {
    fn from_batch(batch: RecordBatch) -> Result<Self> {
        let struct_array = StructArray::from(batch);
        let (ffi_array, ffi_schema) = to_ffi(&struct_array.into())
            .map_err(|e| crate::error::Error::QueryError(e.to_string()))?;
        Ok(Self {
            ffi_schema,
            ffi_array,
        })
    }

    fn schema(&self) -> ArrowSchema {
        unsafe { ArrowSchema::from_raw(&self.ffi_schema as *const _ as *mut _) }
    }

    fn array(&self) -> ArrowArray {
        unsafe { ArrowArray::from_raw(&self.ffi_array as *const _ as *mut _) }
    }
}

/// Backtick-quoted `(col1, col2, ...)` clause from an Arrow schema, or empty when
/// the schema has no fields. Emitted on the `INSERT` so ClickHouse maps the Arrow
/// columns to destination columns **by name**. Without it, `SELECT *` maps by
/// position, which silently scatters values into the wrong columns whenever the
/// table's physical column order differs from the batch's (e.g. after
/// `ALTER TABLE ... ADD COLUMN`, which appends rather than preserving order).
///
/// The direct FFI path (`chdb_insert_arrow_array`) already matches by name via
/// ClickHouse's Arrow input format; this keeps the SQL fallback consistent.
fn column_list_clause(schema: &arrow::datatypes::Schema) -> String {
    if schema.fields().is_empty() {
        return String::new();
    }
    let cols = schema
        .fields()
        .iter()
        .map(|f| format!("`{}`", f.name().replace('`', "``")))
        .collect::<Vec<_>>()
        .join(", ");
    format!(" ({cols})")
}

fn insert_from_registered(
    conn: &Connection,
    dest_table: &str,
    stream_name: &str,
    columns: &str,
    options: &InsertOptions,
) -> Result<()> {
    let mut sql = format!(
        "INSERT INTO {dest_table}{columns} SELECT * FROM {}",
        arrow_stream_table_sql(stream_name)
    );
    if let Some(settings) = options.settings_clause() {
        sql.push_str(" SETTINGS ");
        sql.push_str(&settings);
    }
    conn.query(&sql, OutputFormat::Null)?;
    Ok(())
}

fn insert_via_registered_array(
    conn: &Connection,
    dest_table: &str,
    stream_name: &str,
    arrow_schema: &ArrowSchema,
    arrow_array: &ArrowArray,
    columns: &str,
    options: &InsertOptions,
) -> Result<()> {
    conn.register_arrow_array(stream_name, arrow_schema, arrow_array)?;
    let result = insert_from_registered(conn, dest_table, stream_name, columns, options);
    conn.unregister_arrow_table(stream_name)?;
    result
}

/// Insert a single [`RecordBatch`] into `dest_table` via `register_arrow_array`.
///
/// `stream_name` is a temporary registration label (not a MergeTree table name).
/// `dest_table` must be a bare ClickHouse table identifier (e.g. `metrics`);
/// dotted names (`db.table`) and reserved identifiers are not quoted automatically.
/// Uses the fastest one-shot Arrow path for a single in-memory batch.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use arrow::array::{Int64Array, RecordBatch};
/// use arrow::datatypes::{DataType, Field, Schema};
/// use chdb_rust::arrow_insert::insert_record_batch;
/// use chdb_rust::InsertOptions;
/// use chdb_rust::connection::Connection;
///
/// let conn = Connection::open_in_memory()?;
/// conn.query(
///     "CREATE TABLE metrics (id Int64) ENGINE=MergeTree ORDER BY id",
///     chdb_rust::format::OutputFormat::TabSeparated,
/// )?;
///
/// let batch = RecordBatch::try_new(
///     Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
///     vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
/// )
/// .map_err(|e| chdb_rust::error::Error::QueryError(e.to_string()))?;
///
/// insert_record_batch(&conn, "metrics", "flush_1", batch, InsertOptions::default_bulk())?;
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
///
/// # Errors
///
/// Returns an error if registration, the insert query, or unregistration fails.
pub fn insert_record_batch(
    conn: &Connection,
    dest_table: &str,
    stream_name: &str,
    batch: RecordBatch,
    options: InsertOptions,
) -> Result<()> {
    let columns = column_list_clause(batch.schema().as_ref());
    let handles = OwnedArrowArray::from_batch(batch)?;
    insert_via_registered_array(
        conn,
        dest_table,
        stream_name,
        &handles.schema(),
        &handles.array(),
        &columns,
        &options,
    )
}

/// Insert a single [`RecordBatch`] using the direct `chdb_insert_arrow_array` FFI when
/// available, falling back to register + SQL + unregister otherwise.
///
/// Unlike [`insert_record_batch`], no temporary `stream_name` is required on the direct path.
/// This is the recommended entry point for single-batch ingest.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use arrow::array::{Int64Array, RecordBatch};
/// use arrow::datatypes::{DataType, Field, Schema};
/// use chdb_rust::arrow_insert::insert_record_batch_direct;
/// use chdb_rust::InsertOptions;
/// use chdb_rust::connection::Connection;
///
/// let conn = Connection::open_in_memory()?;
/// conn.query(
///     "CREATE TABLE metrics (id Int64) ENGINE=MergeTree ORDER BY id",
///     chdb_rust::format::OutputFormat::TabSeparated,
/// )?;
///
/// let batch = RecordBatch::try_new(
///     Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
///     vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
/// )
/// .map_err(|e| chdb_rust::error::Error::QueryError(e.to_string()))?;
///
/// insert_record_batch_direct(&conn, "metrics", batch, InsertOptions::default_bulk())?;
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
pub fn insert_record_batch_direct(
    conn: &Connection,
    dest_table: &str,
    batch: RecordBatch,
    options: InsertOptions,
) -> Result<()> {
    let columns = column_list_clause(batch.schema().as_ref());
    let handles = OwnedArrowArray::from_batch(batch)?;

    #[cfg(direct_arrow_insert)]
    {
        let _ = &columns;
        return conn.insert_arrow_array(dest_table, &handles.schema(), &handles.array(), &options);
    }

    #[cfg(not(direct_arrow_insert))]
    {
        let stream_name = format!(
            "__rust_direct_{}",
            FALLBACK_STREAM_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        insert_via_registered_array(
            conn,
            dest_table,
            &stream_name,
            &handles.schema(),
            &handles.array(),
            &columns,
            &options,
        )
    }
}

/// Insert multiple [`RecordBatch`] values via `register_arrow_stream`.
///
/// `batches` must share the same schema as `schema`. Prefer this when a flush
/// spans more than one batch. See [`insert_record_batch`] for `dest_table` constraints.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use arrow::array::{Int64Array, RecordBatch};
/// use arrow::datatypes::{DataType, Field, Schema};
/// use chdb_rust::arrow_insert::insert_record_batches;
/// use chdb_rust::InsertOptions;
/// use chdb_rust::connection::Connection;
///
/// let conn = Connection::open_in_memory()?;
/// conn.query(
///     "CREATE TABLE metrics (id Int64) ENGINE=MergeTree ORDER BY id",
///     chdb_rust::format::OutputFormat::TabSeparated,
/// )?;
///
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// let batches = vec![
///     RecordBatch::try_new(
///         Arc::clone(&schema),
///         vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
///     )
///     .map_err(|e| chdb_rust::error::Error::QueryError(e.to_string()))?,
///     RecordBatch::try_new(
///         Arc::clone(&schema),
///         vec![Arc::new(Int64Array::from(vec![3i64, 4]))],
///     )
///     .map_err(|e| chdb_rust::error::Error::QueryError(e.to_string()))?,
/// ];
///
/// insert_record_batches(
///     &conn,
///     "metrics",
///     "flush_multi",
///     schema,
///     batches,
///     InsertOptions::default_bulk(),
/// )?;
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
pub fn insert_record_batches(
    conn: &Connection,
    dest_table: &str,
    stream_name: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    options: InsertOptions,
) -> Result<()> {
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    insert_record_batch_reader(conn, dest_table, stream_name, Box::new(reader), options)
}

/// Insert from any [`RecordBatchReader`] via `register_arrow_stream`, or direct FFI when available.
///
/// See [`insert_record_batch`] for `dest_table` constraints.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use arrow::array::{Int64Array, RecordBatch, RecordBatchIterator};
/// use arrow::datatypes::{DataType, Field, Schema};
/// use chdb_rust::arrow_insert::insert_record_batch_reader;
/// use chdb_rust::InsertOptions;
/// use chdb_rust::connection::Connection;
///
/// let conn = Connection::open_in_memory()?;
/// conn.query(
///     "CREATE TABLE metrics (id Int64) ENGINE=MergeTree ORDER BY id",
///     chdb_rust::format::OutputFormat::TabSeparated,
/// )?;
///
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// let batch = RecordBatch::try_new(
///     Arc::clone(&schema),
///     vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
/// )
/// .map_err(|e| chdb_rust::error::Error::QueryError(e.to_string()))?;
/// let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
///
/// insert_record_batch_reader(
///     &conn,
///     "metrics",
///     "flush_reader",
///     Box::new(reader),
///     InsertOptions::default_bulk(),
/// )?;
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
pub fn insert_record_batch_reader(
    conn: &Connection,
    dest_table: &str,
    stream_name: &str,
    reader: Box<dyn RecordBatchReader + Send>,
    options: InsertOptions,
) -> Result<()> {
    let columns = column_list_clause(reader.schema().as_ref());
    let mut ffi_stream = FFI_ArrowArrayStream::new(reader);
    let arrow_stream = unsafe { ArrowStream::from_raw(&mut ffi_stream) };

    #[cfg(direct_arrow_insert)]
    {
        let _ = (stream_name, &columns);
        return conn.insert_arrow_stream(dest_table, &arrow_stream, &options);
    }

    #[cfg(not(direct_arrow_insert))]
    {
        conn.register_arrow_stream(stream_name, &arrow_stream)?;
        let result = insert_from_registered(conn, dest_table, stream_name, &columns, &options);
        conn.unregister_arrow_table(stream_name)?;
        result
    }
}
