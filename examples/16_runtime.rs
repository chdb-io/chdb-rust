//! Process-wide engine lifecycle: signal handlers and clean shutdown.

use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Decline chDB's deadly-signal handlers before any query runs.
    chdb_rust::runtime::signal_handlers(false);

    let conn = Connection::open_in_memory()?;
    let result = conn.query("SELECT 1 + 1 AS sum", OutputFormat::TabSeparated)?;
    println!("sum={}", result.data_utf8_lossy().trim());

    // Restore SIG_DFL without setting the disable flag. Subsequent queries on
    // this connection do not put the handlers back; a later connect might.
    // signal_handlers(false) above is the opt-out that also covers that.
    chdb_rust::runtime::reset_signal_handlers();

    drop(conn);

    // Join every engine thread so nothing outlives process teardown.
    chdb_rust::runtime::shutdown()?;
    println!("engine stopped");

    Ok(())
}
