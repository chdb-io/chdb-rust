//! Integration tests for ClickHouse query parameter binding via the safe Rust API.
//!
//! Mirrors chdb-core's `tests/test_c_api_query_with_params.py` scenarios.
//!
//! Run with: `RUST_TEST_THREADS=1 cargo test --test query_with_params`

use arrow::array::{Array, UInt64Array};
use chdb_rust::arg::Arg;
use chdb_rust::connection::Connection;
use chdb_rust::error::{Error, Result};
use chdb_rust::execute_stream_arrow_with_params;
use chdb_rust::execute_stream_with_params;
use chdb_rust::execute_with_params;
use chdb_rust::format::OutputFormat;
use chdb_rust::query_param::QueryParams;
use chdb_rust::session::SessionBuilder;

mod common {
    use super::*;

    #[test]
    fn int64_param_returns_bound_value() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let result =
            conn.query_with_params("SELECT {x:Int64} AS v", OutputFormat::CSV, [("x", 42_i64)])?;
        assert_eq!(result.data_utf8_lossy(), "42\n");
        Ok(())
    }

    #[test]
    fn string_param_returns_bound_value() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let result = conn.query_with_params(
            "SELECT {s:String} AS v",
            OutputFormat::CSV,
            [("s", "hello")],
        )?;
        assert_eq!(result.data_utf8_lossy(), "\"hello\"\n");
        Ok(())
    }

    #[test]
    fn string_param_preserves_literal_backslash_sequences() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let result = conn.query_with_params(
            r"SELECT {s:String} = 'C:\\temp' AS v",
            OutputFormat::CSV,
            [("s", r"C:\temp")],
        )?;
        assert_eq!(result.data_utf8_lossy(), "1\n");
        Ok(())
    }

    #[test]
    fn string_param_preserves_literal_tab() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let result = conn.query_with_params(
            r"SELECT {s:String} = 'a\tb' AS v",
            OutputFormat::CSV,
            [("s", "a\tb")],
        )?;
        assert_eq!(result.data_utf8_lossy(), "1\n");
        Ok(())
    }

    #[test]
    fn date_param_arithmetic_returns_expected_value() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let result = conn.query_with_params(
            "SELECT toDate({d:Date}) + 1 AS d",
            OutputFormat::CSV,
            [("d", "2025-01-01")],
        )?;
        assert_eq!(result.data_utf8_lossy(), "\"2025-01-02\"\n");
        Ok(())
    }

    #[test]
    fn multiple_params_bind_by_name() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let result = conn.query_with_params(
            "SELECT {x:UInt64} - {y:UInt64} AS total",
            OutputFormat::CSV,
            [("x", 5_u64), ("y", 7_u64)],
        )?;
        assert_eq!(result.data_utf8_lossy(), "-2\n");
        Ok(())
    }

    #[test]
    fn missing_param_returns_substitution_error() {
        let conn = Connection::open_in_memory().expect("connection");
        let err = conn
            .query_with_params(
                "SELECT {x:UInt64} AS v",
                OutputFormat::CSV,
                QueryParams::new(),
            )
            .unwrap_err();
        assert!(matches!(err, Error::QueryError(msg) if msg.contains("Substitution")));
    }

    #[test]
    fn invalid_type_returns_parse_error() {
        let conn = Connection::open_in_memory().expect("connection");
        let err = conn
            .query_with_params(
                "SELECT {x:UInt64} AS v",
                OutputFormat::CSV,
                [("x", "not-a-number")],
            )
            .unwrap_err();

        assert!(
            matches!(err, Error::QueryError(msg) if msg.contains("cannot be parsed as UInt64"))
        );
    }

    #[test]
    fn params_cleared_after_call() -> Result<()> {
        let conn = Connection::open_in_memory()?;

        let first =
            conn.query_with_params("SELECT {x:Int64} AS v", OutputFormat::CSV, [("x", 1_i64)])?;
        assert_eq!(first.data_utf8_lossy(), "1\n");

        let second =
            conn.query_with_params("SELECT {x:Int64} AS v", OutputFormat::CSV, [("x", 99_i64)])?;
        assert_eq!(second.data_utf8_lossy(), "99\n");

        let err = conn
            .query_with_params(
                "SELECT {x:Int64} AS v",
                OutputFormat::CSV,
                QueryParams::new(),
            )
            .unwrap_err();

        assert!(matches!(err, Error::QueryError(msg) if msg.contains("Substitution")));

        Ok(())
    }

    #[test]
    fn empty_params_falls_through_to_plain_query() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let result =
            conn.query_with_params("SELECT 1 AS v", OutputFormat::CSV, QueryParams::new())?;
        assert_eq!(result.data_utf8_lossy(), "1\n");
        Ok(())
    }

    #[test]
    fn duplicate_param_names_last_wins() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let result = conn.query_with_params(
            "SELECT {x:Int64} AS v",
            OutputFormat::CSV,
            [("x", 1_i64), ("x", 42_i64)],
        )?;
        assert_eq!(result.data_utf8_lossy(), "42\n");
        Ok(())
    }

    #[test]
    fn array_param_for_in_clause() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let result = conn.query_with_params(
            "SELECT sum(arrayJoin({ids:Array(UInt32)})) AS total",
            OutputFormat::CSV,
            [("ids", vec![1_u64, 3, 5])],
        )?;
        assert_eq!(result.data_utf8_lossy(), "9\n");
        Ok(())
    }

    #[test]
    fn session_execute_with_params_uses_bound_values() -> Result<()> {
        let session = SessionBuilder::new()
            .with_data_path(std::env::temp_dir().join("chdb-rust-query-params-session"))
            .with_auto_cleanup(true)
            .build()?;

        session.execute(
        "CREATE TABLE events (event_type String, created_at String) ENGINE = MergeTree() ORDER BY tuple()",
            None,
        )?;
        session.execute(
            "INSERT INTO events VALUES ('purchase', '2025-01-15'), ('view', '2025-01-01')",
            None,
        )?;

        let result = session.execute_with_params(
        "SELECT count() FROM events WHERE event_type = {event:String} AND created_at >= {since:String}",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
        [("event", "purchase"), ("since", "2025-01-01")],
    )?;
        assert_eq!(result.data_utf8_lossy(), "{\"count()\":1}\n");

        Ok(())
    }

    #[test]
    fn execute_with_params_stateless_entry_point() -> Result<()> {
        let result = execute_with_params(
            "SELECT {x:UInt64} AS v",
            Some(&[Arg::OutputFormat(OutputFormat::CSV)]),
            [("x", 7_u64)],
        )?;
        assert_eq!(result.data_utf8_lossy(), "7\n");
        Ok(())
    }
}

mod text_stream {
    use super::*;

    #[test]
    fn stream_with_params_returns_bound_scalar() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        let mut stream = conn.query_stream_with_params(
            "SELECT {x:UInt64} AS v",
            OutputFormat::CSV,
            [("x", 11_u64)],
        )?;

        let chunk = stream.next_chunk()?.expect("expected a chunk");
        assert_eq!(chunk.data_utf8_lossy(), "11\n");
        assert!(stream.next_chunk()?.is_none());

        Ok(())
    }

    #[test]
    fn stream_with_params_limits_row_count() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        let mut stream = conn.query_stream_with_params(
            "SELECT number FROM numbers({n:UInt64})",
            OutputFormat::CSV,
            [("n", 5_u64)],
        )?;

        let mut rows = 0_u64;
        while let Some(chunk) = stream.next_chunk()? {
            rows += chunk.rows_read();
        }
        assert_eq!(rows, 5);

        Ok(())
    }

    #[test]
    fn stream_with_params_missing_param_returns_substitution_error() {
        let mut conn = Connection::open_in_memory().expect("connection");
        let mut stream = conn
            .query_stream_with_params(
                "SELECT {x:UInt64} AS v",
                OutputFormat::CSV,
                QueryParams::new(),
            )
            .expect("stream start succeeds; bind error arrives on fetch");
        let err = stream.next_chunk().unwrap_err();
        assert!(matches!(err, Error::QueryError(msg) if msg.contains("Substitution")));
    }

    #[test]
    fn execute_stream_with_params_stateless_entry_point() -> Result<()> {
        let mut stream = execute_stream_with_params(
            "SELECT {x:UInt64} AS v",
            Some(&[Arg::OutputFormat(OutputFormat::CSV)]),
            [("x", 7_u64)],
        )?;
        let chunk = stream.next_chunk()?.expect("expected a chunk");
        assert_eq!(chunk.data_utf8_lossy(), "7\n");
        Ok(())
    }

    #[test]
    fn session_execute_stream_with_params() -> Result<()> {
        let mut session = SessionBuilder::new()
            .with_data_path(std::env::temp_dir().join("chdb-rust-query-params-text-stream"))
            .with_auto_cleanup(true)
            .build()?;

        session.execute(
            "CREATE TABLE items (id UInt64) ENGINE = MergeTree() ORDER BY id",
            None,
        )?;
        session.execute("INSERT INTO items VALUES (1), (2), (3)", None)?;

        let mut stream = session.execute_stream_with_params(
            "SELECT id FROM items WHERE id >= {min_id:UInt64}",
            Some(&[Arg::OutputFormat(OutputFormat::CSV)]),
            [("min_id", 2_u64)],
        )?;

        let mut rows = 0_u64;
        while let Some(chunk) = stream.next_chunk()? {
            rows += chunk.rows_read();
        }
        assert_eq!(rows, 2);

        Ok(())
    }
}

#[cfg(feature = "arrow")]
mod arrow_stream {
    use super::*;

    #[test]
    fn stream_arrow_with_params_returns_bound_scalar() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        let mut stream =
            conn.query_stream_arrow_with_params("SELECT {x:UInt64} AS v", [("x", 11_u64)], None)?;

        let batch = stream.next_batch()?.expect("expected a batch");
        assert_eq!(batch.num_rows(), 1);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64 column");
        assert_eq!(col.value(0), 11);
        assert!(stream.next_batch()?.is_none());

        Ok(())
    }

    #[test]
    fn stream_arrow_with_params_limits_row_count() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        let mut stream = conn.query_stream_arrow_with_params(
            "SELECT number FROM numbers({n:UInt64})",
            [("n", 5_u64)],
            None,
        )?;

        let mut rows = 0_usize;
        while let Some(batch) = stream.next_batch()? {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 5);

        Ok(())
    }

    #[test]
    fn stream_arrow_missing_param_returns_substitution_error() {
        let mut conn = Connection::open_in_memory().expect("connection");
        let mut stream = conn
            .query_stream_arrow_with_params("SELECT {x:UInt64} AS v", QueryParams::new(), None)
            .expect("stream start succeeds; bind error arrives on fetch");
        let err = stream.next_batch().unwrap_err();
        assert!(matches!(err, Error::QueryError(msg) if msg.contains("Substitution")));
    }

    #[test]
    fn execute_stream_arrow_with_params_stateless_entry_point() -> Result<()> {
        let mut stream =
            execute_stream_arrow_with_params("SELECT {x:UInt64} AS v", [("x", 7_u64)])?;
        let batch = stream.next_batch()?.expect("expected a batch");
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }

    #[test]
    fn session_execute_stream_arrow_with_params() -> Result<()> {
        let mut session = SessionBuilder::new()
            .with_data_path(std::env::temp_dir().join("chdb-rust-query-params-arrow-stream"))
            .with_auto_cleanup(true)
            .build()?;

        session.execute(
            "CREATE TABLE items (id UInt64) ENGINE = MergeTree() ORDER BY id",
            None,
        )?;
        session.execute("INSERT INTO items VALUES (1), (2), (3)", None)?;

        let mut stream = session.execute_stream_arrow_with_params(
            "SELECT id FROM items WHERE id >= {min_id:UInt64}",
            [("min_id", 2_u64)],
            None,
        )?;

        let mut rows = 0_usize;
        while let Some(batch) = stream.next_batch()? {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 2);

        Ok(())
    }
}
