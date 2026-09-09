/// Example: Durable Objects
///
/// A durable object is a chDB database whose authoritative state lives in
/// storage you own — a full checkpoint plus a statement WAL under a leased
/// `head.json`, in the layout every chDB binding shares. This example writes
/// one, loses the handle, and recovers the database from storage alone.
///
/// Run it with the feature on:
///
///     cargo run --features durable --example 09_durable_object
use chdb_rust::durable::{Namespace, OpenOptions};
use chdb_rust::format::OutputFormat;

fn main() -> Result<(), chdb_rust::durable::Error> {
    println!("=== Durable Object Examples ===\n");

    // One directory per namespace, one subdirectory per object. A cloud backend
    // is what makes an object recoverable on *another* machine; a directory is
    // for development and for seeing the layout.
    let root = std::env::temp_dir().join("chdb-durable-example");
    let namespace =
        Namespace::new(root.to_str().expect("a UTF-8 temp dir"))?.with_owner("example-writer");

    println!("1. Opening the object (creating it if this is the first run)...");
    let (object, existed) = namespace.open(
        "tenant-123",
        OpenOptions {
            database: Some("mem".to_string()),
            ..OpenOptions::default()
        },
    )?;
    println!(
        "   {} at {}",
        if existed { "restored" } else { "created" },
        root.display()
    );

    if !existed {
        object.execute(
            "CREATE TABLE events (id UInt64, tag String) ENGINE = MergeTree ORDER BY id",
        )?;
    }

    println!("2. Writing...");
    // Log literals, never now() or rand(): recovery re-executes the statement,
    // and V1 promises ordered replay, not the same answer twice.
    let ticket = object.execute("INSERT INTO events VALUES (1, 'first')")?;

    // execute() ran the statement here. flush_through() is what makes it
    // survive losing this machine — wait for it before answering anyone.
    println!("3. Flushing, which is the durability barrier...");
    object.flush_through(ticket)?;

    println!("4. Folding the WAL into a fresh checkpoint...");
    let base = object.checkpoint()?;
    println!("   base is now {}", base.key);

    let stats = object.stats();
    println!(
        "   generation {}, seq {}, {} statement(s) committed",
        stats.generation, stats.committed_seq, stats.committed_statements
    );

    // close() drains, flushes and releases the lease. Dropping the handle
    // instead reclaims local resources but leaves the lease to expire.
    object.close()?;

    println!("\n5. Reopening from storage alone...");
    let (object, _) = namespace.open("tenant-123", OpenOptions::default())?;
    let rows = object.query("SELECT id, tag FROM events ORDER BY id", OutputFormat::CSV)?;
    println!("{}", String::from_utf8_lossy(&rows));
    object.close()?;

    println!("Run it again: the row count grows, and each run recovers the last.");
    Ok(())
}
