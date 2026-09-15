//! Arrow-side entry points: parameter binding on the streaming path, one-shot
//! export, and type-mapping options.

use chdb_rust::connection::Connection;

#[test]
fn an_arrow_stream_binds_parameters() {
    let mut conn = Connection::open_in_memory().expect("open");

    let mut stream = conn
        .query_stream_arrow_with_params(
            "SELECT number FROM numbers({n:UInt64})",
            [("n", "3")],
            None,
        )
        .expect("stream");

    let mut rows = 0usize;
    while let Some(batch) = stream.next_batch().expect("batch") {
        rows += batch.num_rows();
    }

    assert_eq!(rows, 3);
}

use arrow::array::RecordBatchReader;
use arrow::datatypes::DataType;
use chdb_rust::arrow_options::ArrowOptions;

#[test]
fn a_one_shot_query_returns_every_row() {
    let conn = Connection::open_in_memory().expect("open");

    let reader = conn
        .query_arrow("SELECT number FROM numbers(1000)")
        .expect("query_arrow");

    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 1000);
}

/// The C ABI transfers ownership of `out_stream->release` to the caller; the
/// engine materializes a standalone Arrow table. Dropping the connection
/// before draining the reader must still yield every row. If this fails at
/// runtime, the `'a` borrow on `ArrowReader` is a real hazard and must stay.
#[test]
fn a_one_shot_reader_outlives_its_connection() {
    let conn = Connection::open_in_memory().expect("open");
    let reader = conn
        .query_arrow("SELECT number FROM numbers(1000)")
        .expect("query_arrow");
    drop(conn);

    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 1000);
}

#[test]
fn a_one_shot_query_matches_the_streamed_result() {
    let mut conn = Connection::open_in_memory().expect("open");
    let sql = "SELECT number, toString(number) AS s FROM numbers(500)";

    let one_shot: usize = conn
        .query_arrow(sql)
        .expect("query_arrow")
        .map(|b| b.expect("batch").num_rows())
        .sum();

    let mut stream = conn.query_stream_arrow(sql).expect("stream");
    let mut streamed = 0usize;
    while let Some(batch) = stream.next_batch().expect("batch") {
        streamed += batch.num_rows();
    }

    assert_eq!(one_shot, 500);
    assert_eq!(one_shot, streamed);
}

#[test]
fn the_default_options_match_passing_none() {
    // AggregateFunction has no faithful Arrow mapping, so it exercises
    // `unsupported_as_binary`, per chdb.h's own list of unsupported types
    // (JSON/Object, Dynamic, AggregateFunction).
    let sql = "SELECT sumState(number) AS s FROM numbers(3)";

    let conn = Connection::open_in_memory().expect("open");
    let none_result = conn
        .query_arrow(sql)
        .map(|r| r.schema().field(0).data_type().clone());

    let conn2 = Connection::open_in_memory().expect("open2");
    let default_result = conn2
        .query_arrow_with_opts(sql, &ArrowOptions::default())
        .map(|r| r.schema().field(0).data_type().clone());

    match (&none_result, &default_result) {
        (Err(_), Err(_)) => {}
        (Ok(a), Ok(b)) => assert_eq!(
            a, b,
            "query_arrow and query_arrow_with_opts(default) diverged"
        ),
        _ => panic!(
            "query_arrow and query_arrow_with_opts(default) diverged: {:?} vs {:?}",
            none_result.is_ok(),
            default_result.is_ok()
        ),
    }
    // Observed: both error, since the engine's own default
    // (unsupported_as_binary = 0) throws UNKNOWN_TYPE for AggregateFunction.
    assert!(none_result.is_err(), "got {:?}", none_result);

    let conn3 = Connection::open_in_memory().expect("open3");
    let opts = ArrowOptions {
        unsupported_as_binary: true,
        ..ArrowOptions::default()
    };
    let binary = conn3.query_arrow_with_opts(sql, &opts).expect("binary");
    assert_eq!(binary.schema().field(0).data_type(), &DataType::Binary);
}

#[test]
fn low_cardinality_becomes_a_dictionary_when_asked() {
    let conn = Connection::open_in_memory().expect("open");
    let sql = "SELECT CAST('x', 'LowCardinality(String)') AS c";

    let default = conn.query_arrow(sql).expect("default");
    assert!(
        !matches!(
            default.schema().field(0).data_type(),
            DataType::Dictionary(..)
        ),
        "the engine default materializes LowCardinality to its base type"
    );

    let opts = ArrowOptions {
        low_cardinality_as_dictionary: true,
        ..ArrowOptions::default()
    };
    let dict = conn.query_arrow_with_opts(sql, &opts).expect("dictionary");
    assert!(
        matches!(dict.schema().field(0).data_type(), DataType::Dictionary(..)),
        "got {:?}",
        dict.schema().field(0).data_type()
    );
}

#[test]
fn strings_can_be_emitted_as_binary() {
    let conn = Connection::open_in_memory().expect("open");
    let sql = "SELECT 'hello' AS s";

    let opts = ArrowOptions {
        string_as_string: false,
        ..ArrowOptions::default()
    };
    let reader = conn.query_arrow_with_opts(sql, &opts).expect("binary");

    assert_eq!(reader.schema().field(0).data_type(), &DataType::Binary);
}

#[test]
fn options_reach_the_streaming_path_too() {
    let mut conn = Connection::open_in_memory().expect("open");
    let opts = ArrowOptions {
        low_cardinality_as_dictionary: true,
        ..ArrowOptions::default()
    };

    let mut stream = conn
        .query_stream_arrow_with_opts("SELECT CAST('x', 'LowCardinality(String)') AS c", &opts)
        .expect("stream");

    let batch = stream.next_batch().expect("batch").expect("one batch");
    assert!(matches!(
        batch.schema().field(0).data_type(),
        DataType::Dictionary(..)
    ));
}

#[test]
fn params_and_options_combine_on_the_arrow_stream() {
    let mut conn = Connection::open_in_memory().expect("open");
    let opts = ArrowOptions {
        low_cardinality_as_dictionary: true,
        ..ArrowOptions::default()
    };

    let mut stream = conn
        .query_stream_arrow_with_params(
            "SELECT CAST(toString(number), 'LowCardinality(String)') AS c \
             FROM numbers({n:UInt64})",
            [("n", "7")],
            Some(&opts),
        )
        .expect("stream");

    let mut rows = 0usize;
    let mut saw_dictionary = false;
    while let Some(batch) = stream.next_batch().expect("batch") {
        if matches!(
            batch.schema().field(0).data_type(),
            DataType::Dictionary(..)
        ) {
            saw_dictionary = true;
        }
        rows += batch.num_rows();
    }

    assert!(saw_dictionary, "expected a Dictionary-typed field");
    assert_eq!(
        rows, 7,
        "the {{n:UInt64}} parameter must control the row count"
    );
}

#[test]
fn a_one_shot_query_with_no_rows_yields_no_batches() {
    let conn = Connection::open_in_memory().expect("open");

    let reader = conn
        .query_arrow("SELECT number FROM numbers(0)")
        .expect("query_arrow");

    assert_eq!(reader.schema().field(0).name(), "number");
    assert_eq!(reader.schema().field(0).data_type(), &DataType::UInt64);

    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 0);
}
