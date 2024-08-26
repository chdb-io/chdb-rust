use chdb_rust::arg::Arg;
use chdb_rust::execute;
use chdb_rust::format::InputFormat;
use chdb_rust::format::OutputFormat;
use chdb_rust::log_level::LogLevel;
use chdb_rust::session::SessionBuilder;

#[test]
fn stateful() {
    //
    // Create session.
    //

    let session = SessionBuilder::new()
        .with_data_path("/tmp/chdb")
        .with_arg(Arg::LogLevel(LogLevel::Debug))
        .with_arg(Arg::Custom("priority".into(), Some("1".into())))
        .with_auto_cleanup(true)
        .build()
        .unwrap();

    //
    // Create database.
    //

    session
        .execute("CREATE DATABASE demo; USE demo", Some(&[Arg::MultiQuery]))
        .unwrap();

    //
    // Create table.
    //

    session
        .execute(
            "CREATE TABLE logs (id UInt64, msg String) ENGINE = MergeTree ORDER BY id",
            None,
        )
        .unwrap();

    //
    // Insert into table.
    //

    session
        .execute("INSERT INTO logs (id, msg) VALUES (1, 'test')", None)
        .unwrap();

    //
    // Select from table.
    //

    let result = session
        .execute(
            "SELECT * FROM logs",
            Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
        )
        .unwrap()
        .unwrap();

    assert_eq!(result.data_utf8_lossy(), "{\"id\":1,\"msg\":\"test\"}\n");
}

#[test]
fn stateless() {
    let query = format!(
        "SELECT * FROM file('tests/logs.csv', {})",
        InputFormat::CSV.as_str()
    );

    let result = execute(
        &query,
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )
    .unwrap()
    .unwrap();

    assert_eq!(result.data_utf8_lossy(), "{\"id\":1,\"msg\":\"test\"}\n");
}
