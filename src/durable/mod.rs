//! chDB **Durable V1**: a database whose authoritative state lives in storage
//! you own.
//!
//! A durable object is one chDB database whose committed state is a full
//! checkpoint plus a chain of write-ahead-log segments, published as immutable
//! objects under a prefix, with one compare-and-set `head.json` naming the
//! current manifest and holding the writer lease. Local MergeTree is the hot
//! working copy; the object itself is a folder of open-format files you can
//! move between machines.
//!
//! ```no_run
//! use chdb_rust::durable::{Namespace, OpenOptions};
//! use chdb_rust::format::OutputFormat;
//!
//! let namespace = Namespace::new("file:///var/lib/chdb-durable")?.with_owner("worker-1");
//! let (object, existed) = namespace.open("tenant-123", OpenOptions {
//!     database: Some("mem".to_string()),
//!     ..OpenOptions::default()
//! })?;
//!
//! if !existed {
//!     object.execute("CREATE TABLE events (id UInt64, at DateTime) ENGINE = MergeTree ORDER BY id")?;
//! }
//! let ticket = object.execute("INSERT INTO events VALUES (1, '2026-09-07 00:00:00')")?;
//! object.flush_through(ticket)?;        // now it survives losing this machine
//! let rows = object.query("SELECT count() FROM events", OutputFormat::CSV)?;
//! object.checkpoint()?;                 // fold base + WAL into a fresh base
//! object.close()?;
//! # Ok::<(), chdb_rust::durable::Error>(())
//! ```
//!
//! # What execute guarantees, and what it does not
//!
//! [`DurableObject::execute`] means the statement ran locally and joined the WAL
//! buffer. It does not mean the write left the machine. Durability is
//! [`DurableObject::flush`], or [`DurableObject::flush_through`] for one
//! statement's watermark. A service that answers a client before flushing is
//! choosing to lose that write if the process dies, and it should choose that
//! knowingly rather than by accident.
//!
//! # What the engine decides, and what this module decides
//!
//! Whether a statement may run at all is not this module's judgement. Every
//! [`query`](DurableObject::query) and [`execute`](DurableObject::execute) is
//! put to ClickHouse's own parser first — how many executable statements is
//! this, what class are they, does every persistent write land in the database
//! this object owns, does the text embed a credential — and the answer is the
//! gate. There is no prefix list and no regular expression anywhere here,
//! because neither can see through `INSERT ... FORMAT` inline data or resolve an
//! unqualified table name. Likewise `BACKUP` and `RESTORE` are never assembled
//! as text: core takes the database name and the path as arguments and does its
//! own quoting. See [`crate::admin`] for those three entry points.
//!
//! That needs chdb-core v26.7.2-rc.2 or later, which is where they were added.
//!
//! # Determinism is the caller's job
//!
//! Recovery re-executes logged SQL, so a statement must produce the same result
//! on replay as it did originally. Log literals: compute a timestamp or an id in
//! the caller and log the value, not `now()`, `rand()`, `generateUUIDv4()`, or
//! an `INSERT ... SELECT` from a volatile source. V1 promises ordered replay of
//! the original statement text and nothing more. Non-deterministic or bulk
//! transformations belong in a [`checkpoint`](DurableObject::checkpoint), which
//! snapshots actual state.
//!
//! # One object per process
//!
//! chdb-core binds one data path per process, so one process holds one open
//! durable object at a time. Opening a second returns an error naming both
//! paths. Fan-out across many objects is sequential, or spread across worker
//! processes; this module does not pretend otherwise.
//!
//! # Version compatibility
//!
//! An object records the exact `chdb_version()` that wrote it, and that is not a
//! gate. Compatibility is decided by two explicit fields — the archive-format
//! generation and the minimum reader version — so an object written by
//! v26.7.2-rc.2 opens on every later chdb-core release that can still restore
//! its archive.
//!
//! # Security scope
//!
//! This module provides single-writer *coordination* — the lease plus the
//! compare-and-set fence — not security. Access control is entirely your
//! storage's: anyone who can write the object's prefix can read, modify or take
//! its lease. There is no application-level auth, no client-side encryption, and
//! no tamper protection beyond the length and SHA-256 checks that detect a
//! damaged archive. For multi-tenant use, give each tenant credentials scoped to
//! its own prefix.
//!
//! # Where the specification lives
//!
//! The protocol is specified in [`CHDB_DURABLE_V1_CONTRACT.md`][contract] in the
//! chdb repository, and that document — not this implementation — is the source
//! of truth. Semantics change there first, and the same fixtures are read by the
//! Python, Node and Go bindings.
//!
//! [contract]: https://github.com/chdb-io/chdb/blob/main/dev-docs/CHDB_DURABLE_V1_CONTRACT.md

mod backend;
mod backends;
mod chdb_engine;
mod digest;
mod engine;
mod errors;
mod head;
mod keys;
mod lease;
mod namespace;
mod negotiate;
mod object;
mod types;
mod version;
mod wal;

pub use backend::{Backend, PutOutcome, ReplaceOutcome, Tagged};
pub use backends::LocalBackend;
pub use chdb_engine::ChdbEngine;
pub use digest::Digest;
pub use engine::{Engine, EngineFactory, EngineStartOptions};
pub use errors::{Category, Error, Result};
pub use keys::{is_valid_object_key, HEAD_KEY};
pub use namespace::{BackendFactory, Namespace};
pub use object::{DurableObject, OpenOptions, Stats, Tuning, WriteTicket};
pub use types::{
    EngineIdentity, Head, Lease, Manifest, ObjectRef, Protocol, BACKUP_FORMAT_BASELINE,
    ENGINE_NAME, MAX_HEAD_BYTES, MAX_SQL_BYTES, MAX_WAL_SEGMENT_BYTES, PROTOCOL_VERSION,
};
pub use version::compare_engine_versions;
