use chdb_rust::*;

use tracing::*;

#[derive(clap::Parser)]
#[command(version)]
#[command(about = "")]
#[command(author = "tekjar <raviteja@bytebeam.io>")]
struct CommandLine {
    /// log level (v: info, vv: debug, vvv: trace)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    verbose: u8,
    /// Table name
    #[arg(short = 'c', long = "create")]
    create_imu: bool,
    /// Path to load IMU data from
    #[arg(short = 'l', long = "load")]
    load_imu: Option<String>,
    /// /// Query IMU data
    #[arg(short = 'q', long = "query")]
    query: String,
}

fn level(verbose: u8) -> String {
    let level = match verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };

    level.to_owned()
}

fn init() -> CommandLine {
    let commandline: CommandLine = CommandLine::parse();
    use clap::Parser;
    let level = level(commandline.verbose);

    // tracing syntax ->
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(&level)
        .with_filter_reloading();

    // let reload_handle = builder.reload_handle();

    builder
        .try_init()
        .expect("initialized subscriber succesfully");

    commandline
}

fn create_imu(session: &Session) {
    session.execute(
        "
          CREATE DATABASE IF NOT EXISTS demo;
          CREATE TABLE IF NOT EXISTS demo.imu
          (
              ax Float32,
              ay Float32,
              az Float32,
              magx Float32,
              magy Float32,
              magz Float32,
              roll Float32,
              pitch Float32,
              yaw Float32,
              id String,
              date Date DEFAULT toDate(timestamp),
              timestamp DateTime64(3),
              sequence UInt32
          )
          ENGINE = MergeTree
          PARTITION BY date
          ORDER BY id
          SETTINGS index_granularity = 8192",
    );
}

fn load_imu(session: &Session, dir: &str) {
    session.execute(format!(
        "
        set input_format_parquet_allow_missing_columns = 1;
        INSERT INTO demo.imu SELECT * FROM file('{dir}/*.parquet', Parquet)
        "
    ));
}

fn main() {
    let cli = init();
    let level = level(cli.verbose);

    error!("error..");
    info!("info..");
    debug!("debug..");

    let session = SessionBuilder::new().log_level(&level).build().unwrap();
    if cli.create_imu {
        create_imu(&session);
    }

    if let Some(dir) = cli.load_imu {
        load_imu(&session, &dir);
    }

    let v = session.execute(cli.query).unwrap();
    println!("Rows = {}, Bytes = {}, Elapsed = {:?}", v.rows_read(), v.bytes_read(), v.elapsed());

    let result = String::from_utf8(v.buf().to_vec()).unwrap();
    println!("{}", result);
}
