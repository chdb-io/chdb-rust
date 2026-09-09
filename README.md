<img src="https://avatars.githubusercontent.com/u/132536224" width=130 />

[![Rust](https://github.com/chdb-io/chdb-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/chdb-io/chdb-rust/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/chdb-rust.svg)](https://crates.io/crates/chdb-rust)
[![docs.rs](https://docs.rs/chdb-rust/badge.svg)](https://docs.rs/chdb-rust)

# chdb-rust <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d5/Rust_programming_language_black_logo.svg/1024px-Rust_programming_language_black_logo.svg.png" height=20 />

Experimental [chDB](https://github.com/chdb-io/chdb) FFI bindings for Rust.

## Documentation

**[Full API Documentation](https://docs.rs/crate/chdb-rust/latest)** - Complete Rust API reference on docs.rs

## Status

**Experimental** - This library is currently experimental, unstable, and subject to changes.

The library automatically downloads and manages [`libchdb`](https://github.com/chdb-io/chdb) dependencies during the build process.

## Quick Start

Add `chdb-rust` to your `Cargo.toml`:

```toml
[dependencies]
chdb-rust = "1.1.0"
```

The library will automatically download the required `libchdb` binary during the build process.

## Supported Platforms

- **Linux**: x86_64, aarch64
- **macOS**: x86_64, arm64 (Apple Silicon)

## Building

### Standard Build

```bash
cargo build
```

### Verbose Build (for debugging)

```bash
RUST_BACKTRACE=full cargo build --verbose
```

### Manual Installation (Optional)

If you prefer to install `libchdb` manually instead of automatic download:

**System-wide installation:**
```bash
./update_libchdb.sh --global
```

**Local directory installation:**
```bash
./update_libchdb.sh --local
```

### Building Against an Existing libchdb

Two environment variables point the build at an engine this crate did not
download — a local `chdb-core` build, a system package, a vendored copy:

| Variable | Meaning |
| --- | --- |
| `CHDB_LIB_DIR` | directory holding the library to link |
| `CHDB_INCLUDE_DIR` | directory holding the matching `chdb.h`, when it is not next to the library |

Which file is looked for follows the linkage: `libchdb.a` with
`--features static`, otherwise `libchdb.so` (which is the name `chdb-core` uses
on macOS too) or `libchdb.dylib`.

The two are separate because a `chdb-core` build tree does not keep them
together — `build_static_lib.sh` leaves `libchdb.a` at the repository root while
the header stays in `programs/local`:

```bash
CHDB_LIB_DIR=../chdb-core \
CHDB_INCLUDE_DIR=../chdb-core/programs/local \
    cargo build --features static
```

A directory that is set but has no usable library is an error rather than a
fall-through to the download, so a build never quietly links a different engine
than the one asked for. The build re-runs when the library file changes, which is
what makes iterating against an engine that is still being rebuilt work.

### Where the Engine Is Cached

A downloaded engine is kept outside `target/`, keyed by release tag and platform:

| | |
| --- | --- |
| macOS | `~/Library/Caches/chdb-rust/<tag>/<asset>/` |
| Linux | `${XDG_CACHE_HOME:-~/.cache}/chdb-rust/<tag>/<asset>/` |

`CHDB_ENGINE_CACHE_DIR` moves it, and deleting the directory clears it. Nothing
is ever evicted — an entry is only added under a key it does not already have —
so it grows by one engine per release and linkage you build against.

Cargo hands out a fresh `OUT_DIR` per profile and per feature combination, so
without this the same engine is fetched again for every combination and lost
entirely to `cargo clean`. Measured in this repository before the cache existed:
4.9 GB under `target/`, seven copies of two engines.

On CI, cache this directory rather than `target/`. It is keyed by the pinned
engine, so it only changes when the pin does.

## Linking

Two ways to link the engine, chosen per situation.

| | artifact | at run time |
| --- | --- | --- |
| dynamic (default) | 448 KB binary, and a 326 MB `libchdb.so` beside it | the library has to be findable |
| `--features static` | one file: 490 MB, or 361 MB stripped | nothing to find |

Sizes are a release build of `examples/01_stateless_queries` against chdb-core
v26.7.0 on macOS arm64; the engine is 432 MB on linux-aarch64. Dynamic gives you
a small artifact, static gives you one you can copy anywhere.

### A Dynamically Linked Binary Does Not Run On Its Own

`cargo run` and `cargo test` work because Cargo puts an rpath into the binaries
it is about to run itself. Nothing else does:

```console
$ ./target/release/my-tool
dyld[19233]: Library not loaded: @rpath/libchdb.so
  Referenced from: /path/to/my-tool
  Reason: no LC_RPATH's found
```

On Linux the same situation reads `error while loading shared libraries:
libchdb.so: cannot open shared object file`.

Three ways out, all of which work on both platforms:

1. Install the library where the loader already looks:
   `./update_libchdb.sh --global` puts it in `/usr/local/lib`, which is on the
   default search path for both loaders.
2. Point the loader at it for the run: `DYLD_LIBRARY_PATH` on macOS,
   `LD_LIBRARY_PATH` on Linux.
3. Bake an rpath into your own binary, from your own `build.rs`, and ship the
   library next to the executable:

   ```rust
   let origin = if cfg!(target_os = "macos") { "@loader_path" } else { "$ORIGIN" };
   println!("cargo:rustc-link-arg=-Wl,-rpath,{origin}");
   ```

This is where chdb-rust is behind the other chDB bindings: chdb-python,
chdb-node and chdb-go all run as soon as they are installed. If "copy it over
and it runs" is what you need, `--features static` is the shorter path.

### Strip the Symbol Table From a Static Artifact

A static artifact carries a large symbol table that the program never reads at
run time. Only you can remove it, in your own `Cargo.toml`, because Cargo
ignores a dependency's `[profile]`:

```toml
[profile.release]
strip = "symbols"
```

Measured on the artifact above: 490 MB down to 361 MB, 26% for one line. The
engine's own debug information is already gone before it reaches you — chdb-core
strips the archive when it builds it — so what is left is the symbol table of
the final link, which is why only the final link can drop it.

## Sessions

chDB embeds one engine per process, serving one data path. Any number of
sessions may be open on that path at once and they query concurrently, sharing
one database. A session on a *different* path is refused until every existing
session and connection is closed.

An in-memory connection is a data path of its own, since the engine binds
`:memory:` when none is given. So `execute`, which opens one, fails while a
session is open, and the reverse also holds. Both report `Error::PathConflict`,
naming the path the engine is on and the one that was asked for.
`active_engine_path()` and `active_engine_refs()` report the same two facts.

### Concurrency

Give each thread its own session. A connection is `Send`, so a session can be
moved to the thread that will use it:

```rust
let readers: Vec<_> = (0..4)
    .map(|_| {
        let path = path.clone();
        std::thread::spawn(move || {
            let session = SessionBuilder::new().with_data_path(&path).build()?;
            session.execute("SELECT count() FROM t", None)
        })
    })
    .collect();
```

A single session is not shared between threads: a connection is `Send` but not
`Sync`. `chdb_query` is thread-safe, but the Arrow registration calls a session
also makes say nothing about concurrent use.

### Cleaning Up

`SessionBuilder::with_auto_cleanup(true)` removes the data directory when the
session is dropped, but only when it is the last handle on that path: a session
with a sibling still open removes nothing.

So one rule: **if you use `with_auto_cleanup`, set it on every session you open
on that path.** A mixture leaves the outcome to drop order, since the session
holding the flag may not be the one that closes last:

```rust
let a = SessionBuilder::new().with_data_path(&dir).with_auto_cleanup(true).build()?;
let b = SessionBuilder::new().with_data_path(&dir).build()?;  // no flag
drop(a);  // not the last handle, so nothing is removed
drop(b);  // the last handle, but no flag, so nothing is removed
```

`Session::cleanup()` is the deterministic form: it removes the directory
whether or not the flag was set, and still only as the last handle.

## Which Engine Is Linked

Three accessors, each reporting exactly one versioning scheme. A chdb-core
release number is not a ClickHouse one — `X.Y` is the ClickHouse minor line the
release sits on and `Z` is chdb-core's own counter — so the two cannot be
compared, and none of these answers with the other's number when its own source
is unavailable.

| | scheme | resolved |
| --- | --- | --- |
| `version::EXPECTED_ENGINE_VERSION` | chdb-core | at compile time, `Option<&str>` |
| `version::engine_version()` | chdb-core | by the linked library |
| `version::clickhouse_version()` | ClickHouse | by `SELECT version()` |

```rust
use chdb_rust::version::{clickhouse_version, engine_version, ENGINE_SOURCE};

println!("engine    {}", engine_version()?);       // 26.7.0
println!("clickhouse {}", clickhouse_version()?);  // 26.7.2.1
println!("from      {}", ENGINE_SOURCE);           // download: chdb-core v26.7.0
```

`EXPECTED_ENGINE_VERSION` is `None` whenever the build linked a library it did
not fetch — a `CHDB_LIB_DIR` build, a copy already installed on the machine. The
pinned version says nothing about an artifact that came from somewhere else, so
it reports nothing rather than reporting the pin; `ENGINE_SOURCE` says where the
library came from. `engine_version()` is the only one that describes the artifact
actually loaded, which makes it the way to confirm which engine a binary carries.

It needs `chdb_version()`, which arrived in chdb-core v26.7.0; against an older
library it returns `Error::EngineVersionUnavailable` rather than falling back to
`SELECT version()`.

## Testing

Run the test suite:

```bash
cargo test -- --test-threads=1
```

## Examples

- **Runnable examples**: See the [examples/](examples/) directory
  ```bash
  cargo run --example <name>
  ```
- **Detailed documentation**: See [docs/examples.md](docs/examples.md) for comprehensive examples and explanations
- **Test examples**: See [tests/](tests/) directory for additional usage examples

## Arrow Bulk Insert

Apache Arrow bulk insert is enabled by default via the `arrow` feature (Arrow 59). Import Arrow types through `chdb_rust::arrow` so your `RecordBatch` types match the crate:

```rust
use chdb_rust::arrow::array::{Int64Array, RecordBatch};
use chdb_rust::arrow::datatypes::{DataType, Field, Schema};
```

SQL-only users can disable it to avoid building Arrow:

```toml
chdb-rust = { version = "1.4", default-features = false }
```

See [docs/examples.md](docs/examples.md#fast-bulk-inserts-arrow) for usage, or run:

```bash
cargo run --example 08_arrow_insert
```

## Durable Objects

`--features durable` turns a database into an object in storage you own: a full
checkpoint plus a statement write-ahead log under one compare-and-set
`head.json`, in the layout the Python, Node and Go bindings share. A different
process — or a different machine, given a shared backend — restores it from
those files alone.

```rust
use chdb_rust::durable::{Namespace, OpenOptions};
use chdb_rust::format::OutputFormat;

let namespace = Namespace::new("file:///var/lib/chdb-durable")?.with_owner("worker-1");
let (object, existed) = namespace.open("tenant-123", OpenOptions {
    database: Some("mem".to_string()),
    ..OpenOptions::default()
})?;

if !existed {
    object.execute("CREATE TABLE events (id UInt64) ENGINE = MergeTree ORDER BY id")?;
}
let ticket = object.execute("INSERT INTO events VALUES (1)")?;
object.flush_through(ticket)?;   // now it survives losing this machine
let rows = object.query("SELECT count() FROM events", OutputFormat::CSV)?;
object.checkpoint()?;            // fold base + WAL into a fresh base
object.close()?;                 // flush, release the lease, reclaim the scratch
```

Four things to know before using it:

- **`execute` is not a durability barrier.** It runs the statement locally and
  buffers it; `flush` (or `flush_through` for one statement) is what publishes
  it. A service that answers a client before flushing is choosing to lose that
  write on a crash.
- **Replay re-executes your SQL.** Log literals — compute a timestamp or an id
  in the caller — not `now()`, `rand()`, `generateUUIDv4()` or an
  `INSERT ... SELECT` from a volatile source.
- **One object per process.** chdb-core binds one data path per process, so
  opening a second object returns an error naming both paths. Fan out across
  worker processes.
- **The lease is coordination, not security.** Anyone who can write the object's
  prefix can read, modify or take it; access control is your storage's.

Every `query` and `execute` is put to ClickHouse's own parser first — statement
count, class, write targets, embedded credentials — and the answer is the gate.
The same three engine entry points are available directly, without the feature,
on any engine that exports them: see
[`Connection::backup_database`, `restore_database` and `classify_query`](src/admin.rs).

A local directory is the only backend that ships; a cloud provider is plugged in
by implementing `durable::Backend` and passing it to `Namespace::with_backend`.
The protocol is specified in [CHDB_DURABLE_V1_CONTRACT.md][contract] in the chdb
repository, which is the source of truth rather than this implementation.

```bash
cargo run --features durable --example 09_durable_object
cargo test --features durable
```

Needs chdb-core v26.7.2-rc.2 or newer, which is where backup, restore and
statement analysis were added; an older engine fails the build with a message
saying so.

[contract]: https://github.com/chdb-io/chdb/blob/main/dev-docs/CHDB_DURABLE_V1_CONTRACT.md

## Contributing

We welcome contributions! Here's how you can help:

### Getting Started

1. **Fork the repository** and clone your fork
   ```bash
   git clone https://github.com/YOUR_USERNAME/chdb-rust.git
   cd chdb-rust
   ```

2. **Create a branch** for your changes
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

3. **Make your changes** and ensure they work
   - Run tests: `cargo test`
   - Check formatting: `cargo fmt --check`
   - Run clippy: `cargo clippy`

4. **Commit your changes** with clear, descriptive commit messages
   ```bash
   git commit -m "Add feature: description of what you did"
   ```

5. **Push to your fork** and open a Pull Request
   ```bash
   git push origin feature/your-feature-name
   ```

### Development Guidelines

- **Code Style**: Follow Rust conventions and run `cargo fmt` before committing
- **Testing**: Add tests for new features and ensure all existing tests pass
- **Documentation**: Update relevant documentation for user-facing changes
- **Commit Messages**: Write clear, descriptive commit messages
- **Pull Requests**: 
  - Provide a clear description of your changes
  - Reference any related issues
  - Ensure CI checks pass

### Reporting Issues

Found a bug or have a feature request? Please open an issue on GitHub with:
- A clear description of the problem or feature
- Steps to reproduce (for bugs)
- Expected vs actual behavior
- Your environment (OS, Rust version, etc.)

### Questions?

Feel free to open a discussion or issue if you have questions about contributing!
