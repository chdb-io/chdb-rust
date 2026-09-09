//! The Durable V1 object state machine (contract §5).
//!
//! Everything the protocol calls hard lives here: lease acquisition and
//! fencing, the operation queue, WAL publication, checkpoint, and the reconcile
//! that decides whether a request whose response vanished actually committed.
//! None of it touches a native library — the engine arrives as an
//! [`Engine`](super::Engine), the object store as a [`Backend`].
//!
//! A few invariants are worth stating up front, because most of the code below
//! exists to hold one of them:
//!
//! * **[`DurableObject::execute`] succeeding does not mean the write is
//!   durable.** It means the statement ran locally and joined the buffer.
//!   Durability is [`DurableObject::flush`]. A product that answers a client
//!   before flushing is choosing to lose that write on a crash, and it should
//!   choose that knowingly.
//! * **Nothing is ever reported as committed without proof.** Every commit path
//!   can end in [`Category::CommitAmbiguous`], which is an honest answer.
//!   Reporting a lost response as success would be the one failure mode a
//!   caller cannot defend against.
//! * **A writer that cannot confirm its lease stops writing.** Not on the next
//!   error — at the moment its locally believed validity window lapses. The
//!   alternative is two processes each convinced they are the only writer.
//! * **Local resources are always released.** A close that fails to flush still
//!   closes the connection and removes the scratch directory, and still reports
//!   the failure.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use serde_json::{Map, Value};

use crate::format::OutputFormat;

use super::backend::{Backend, PutOutcome};
use super::digest::{
    assert_digest, digest_file, digest_of, drain_digest, engine_io_err, stream_to_verified_file,
    Digest,
};
use super::engine::{assert_execute_allowed, assert_query_allowed, Engine, EngineStartOptions};
use super::errors::{backend_err, err, Category, Result};
use super::head::{parse_head, serialize_head, HeadSnapshot};
use super::keys::{checkpoint_key, uuid8, wal_key, HEAD_KEY};
use super::lease::{
    acquire_lease, create_cold, new_instance_id, now_seconds, read_head, release_lease, ColdParams,
    LeaseParams,
};
use super::negotiate::{
    assert_engine_compatible, assert_readable, assert_writable, raise_compatibility_floor,
    RunningEngine,
};
use super::types::{Head, Lease, Manifest, ObjectRef, MAX_WAL_SEGMENT_BYTES};
use super::wal::{assert_statement_within_limit, decode_segment, encode_segment, line_bytes};
use super::EngineFactory;

/// The lease and commit parameters.
///
/// The contract lets a binding choose these but requires the defaults, their
/// units and their rules to be documented — so they are, here, and the same
/// values drive the tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuning {
    /// How long a lease stays valid after a successful head write. Default 30s.
    pub lease_ttl: Duration,
    /// How often the writer renews. Must be at most a third of the TTL, so two
    /// consecutive heartbeat failures still leave a window to notice and fence.
    /// Default 10s.
    pub heartbeat_interval: Duration,
    /// How far past a recorded expiry another writer waits before treating a
    /// lease as abandoned. This is a bound on disagreement between two
    /// machines' clocks, not a grace period for a slow writer. Default 5s.
    pub clock_skew_allowance: Duration,
    /// How long a single commit may spend retrying and reconciling. Default 30s.
    pub commit_deadline: Duration,
    /// How many attempts a commit makes inside that deadline before giving up.
    /// Default 5.
    pub max_commit_attempts: u32,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            lease_ttl: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
            clock_skew_allowance: Duration::from_secs(5),
            commit_deadline: Duration::from_secs(30),
            max_commit_attempts: 5,
        }
    }
}

impl Tuning {
    fn validate(&self) -> Result<()> {
        // Every one of these ends up in arithmetic that decides whether this
        // process still owns the object, so a nonsensical value is refused
        // rather than propagated into an expiry nobody can take over.
        for (name, value) in [
            ("lease_ttl", self.lease_ttl),
            ("heartbeat_interval", self.heartbeat_interval),
            ("clock_skew_allowance", self.clock_skew_allowance),
            ("commit_deadline", self.commit_deadline),
        ] {
            if value.is_zero() {
                return Err(err(
                    Category::Backend,
                    format!("durable: Tuning::{name} must be a positive duration"),
                ));
            }
        }
        if self.max_commit_attempts == 0 {
            return Err(err(
                Category::Backend,
                "durable: Tuning::max_commit_attempts must be positive",
            ));
        }
        if self.heartbeat_interval * 3 > self.lease_ttl {
            return Err(err(
                Category::Backend,
                format!(
                    "durable: Tuning::heartbeat_interval ({:?}) must be at most a third of \
                     lease_ttl ({:?}); the contract requires room for a retry before expiry",
                    self.heartbeat_interval, self.lease_ttl
                ),
            ));
        }
        Ok(())
    }
}

/// How one object is opened.
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
    /// Open without a writer lease. A read-only handle serves the manifest as
    /// it stood at open.
    pub read_only: bool,
    /// Take an unexpired lease from its current holder.
    ///
    /// This is an administrator action: the previous writer's unflushed local
    /// work is lost, and it learns this only when its next commit is fenced.
    /// Never reach for it as a retry.
    pub force: bool,
    /// Refuse to create the object if it does not exist.
    pub existing_only: bool,
    /// The visible writer name recorded in the lease. Observability only.
    /// Defaults to a name derived from the process id.
    pub owner: Option<String>,
    /// The database this object holds. Only used when creating a cold object;
    /// an existing object's head is authoritative. Defaults to `default`.
    pub database: Option<String>,
    /// The parent directory for the scratch tree. Defaults to the system
    /// temporary directory.
    pub scratch_root: Option<PathBuf>,
    /// The lease and commit parameters.
    pub tuning: Tuning,
}

/// A watermark for one executed statement.
///
/// [`DurableObject::flush_through`] turns it into a durability barrier, which is
/// what lets a caller that expands one request into several statements answer
/// the request as a whole without the protocol having to know about requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WriteTicket {
    /// The ordinal of the statement within this open session, starting at 1.
    pub statement: u64,
}

/// A consistent snapshot of an object's runtime state.
///
/// Taken as a whole rather than field by field, so what it reports is
/// internally consistent: reading a generation and a sequence number through
/// separate accessors can straddle a commit and describe a state that never
/// existed.
///
/// It carries no credentials and no SQL, so it is safe to log verbatim.
#[derive(Debug, Clone)]
pub struct Stats {
    /// The object's id within its namespace.
    pub id: String,
    /// The database this object holds.
    pub database: String,
    /// Whether this handle holds a lease.
    pub read_only: bool,
    /// `open`, `closing` or `closed`.
    pub state: &'static str,
    /// True once this writer has lost, or given up on, its lease.
    pub fenced: bool,
    /// The lease generation currently recorded in the head.
    pub generation: u64,
    /// The visible writer name.
    pub owner: String,
    /// This live instance's id, which is what the fence compares.
    pub instance: String,
    /// `manifest.seq` as last committed.
    pub committed_seq: u64,
    /// The current base checkpoint, if the object has one.
    pub base_key: Option<String>,
    /// How many WAL segments the manifest names.
    pub wal_segments: usize,
    /// How many statements this handle has run since it opened.
    pub executed_statements: u64,
    /// How many of those are in a committed WAL segment or a committed base.
    pub committed_statements: u64,
    /// How many statements have run locally but are not yet published.
    pub pending_statements: usize,
    /// The encoded size of those statements, so a caller can apply its own
    /// backpressure rather than discovering the segment ceiling by hitting it.
    pub pending_bytes: u64,
    /// When the last flush committed.
    pub last_flush_at: Option<SystemTime>,
    /// When the last checkpoint committed.
    pub last_checkpoint_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Open,
    Closing,
    Closed,
}

impl Lifecycle {
    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closing => "closing",
            Self::Closed => "closed",
        }
    }
}

/// The object's private directory tree.
#[derive(Debug, Clone)]
struct Scratch {
    root: PathBuf,
    data: PathBuf,
    backups: PathBuf,
    staging: PathBuf,
}

fn make_scratch(root: Option<&Path>) -> std::io::Result<Scratch> {
    let parent = root.map_or_else(std::env::temp_dir, Path::to_path_buf);
    fs::create_dir_all(&parent)?;
    let base = parent.join(format!("chdb-durable-{}", uuid8()));
    // A unique, empty directory per open (§5.2 step 4). create_dir refuses an
    // existing one, so two opens can never share a data path.
    fs::create_dir(&base)?;
    let scratch = Scratch {
        data: base.join("data"),
        backups: base.join("backups"),
        staging: base.join("staging"),
        root: base,
    };
    // The engine validates that a backup target's parent directory exists, and
    // its allowed-path guard resolves a relative value somewhere nobody wants,
    // so all three are created up front and always absolute.
    for dir in [&scratch.data, &scratch.backups, &scratch.staging] {
        if let Err(e) = fs::create_dir_all(dir) {
            let _ = fs::remove_dir_all(&scratch.root);
            return Err(e);
        }
    }
    Ok(scratch)
}

/// Everything that changes while an object is open.
struct State {
    head: Head,
    etag: String,
    raw: Map<String, Value>,
    wal_buffer: Vec<String>,
    wal_buffer_bytes: u64,
    statement_counter: u64,
    committed_statements: u64,
    last_flush_at: Option<SystemTime>,
    last_checkpoint_at: Option<SystemTime>,
    lifecycle: Lifecycle,
    fenced: bool,
    /// When this writer stops believing its lease. `None` for a read-only open.
    lease_deadline: Option<Instant>,
}

/// The shared half of an object: everything the heartbeat thread also touches.
struct Core {
    id: String,
    read_only: bool,
    backend: Arc<dyn Backend>,
    engine: Mutex<Box<dyn Engine>>,
    scratch: Scratch,
    tuning: Tuning,
    instance: String,
    owner: String,
    running: RunningEngine,

    /// Serializes whole logical operations: execute, query, flush, checkpoint,
    /// close.
    ///
    /// There are two gates rather than one, and the split is the interesting
    /// part. If one gate covered both, a checkpoint of a large database would
    /// block the heartbeat for the whole backup and upload, and the writer
    /// would fence itself out of an object it was legitimately checkpointing.
    /// With the split, the heartbeat only ever contends for `head_gate`, which
    /// is held for a single conditional write — while the checkpoint's own
    /// final commit takes that same gate and therefore sees the ETag the
    /// heartbeat just produced.
    ops: Mutex<()>,
    /// Serializes a single compare-and-swap against `head.json`, including the
    /// heartbeat's.
    head_gate: Mutex<()>,

    state: Mutex<State>,
    stop: (Mutex<bool>, Condvar),
    heartbeat: Mutex<Option<JoinHandle<()>>>,
    /// Set once local resources have gone, so `Drop` does not repeat the work.
    reclaimed: AtomicBool,
}

/// Locks that survive a poisoned mutex.
///
/// A panic while a durable object holds one of these leaves the process with an
/// engine and a scratch tree to release; refusing to take the lock afterwards
/// would strand both.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl Core {
    // ---------------------------------------------------------- guards

    /// Refuses an operation on a closed or fenced object.
    ///
    /// `Closing` is refused too, so a caller that arrived while close was
    /// draining the queue does not get to run one more statement against an
    /// object whose connection is about to go. Close's own flush passes
    /// `allow_closing`, being the one operation that belongs in that window.
    fn assert_usable(&self, allow_closing: bool) -> Result<()> {
        let state = lock(&self.state);
        let usable = state.lifecycle == Lifecycle::Open
            || (allow_closing && state.lifecycle == Lifecycle::Closing);
        if !usable {
            return Err(err(
                Category::Closed,
                format!("durable: object {} is closed", self.id),
            ));
        }
        if state.fenced {
            return Err(err(
                Category::LeaseFenced,
                format!(
                    "durable: object {} lost its lease (generation {}); this handle cannot be \
                     used again",
                    self.id, state.head.lease.generation
                ),
            ));
        }
        Ok(())
    }

    /// Additionally refuses a read-only handle, and self-fences a writer whose
    /// lease has lapsed.
    fn assert_writer(&self, allow_closing: bool) -> Result<()> {
        self.assert_usable(allow_closing)?;
        if self.read_only {
            return Err(err(
                Category::ClassificationRefused,
                format!(
                    "durable: object {} is open read-only; only READ_ONLY statements are accepted",
                    self.id
                ),
            ));
        }
        let lapsed = lock(&self.state)
            .lease_deadline
            .is_none_or(|deadline| Instant::now() >= deadline);
        if lapsed {
            // Self-fence. The lease may in fact still be ours, but we cannot
            // show that it is, and "probably still the writer" is not a state
            // to write from.
            self.fence();
            return Err(err(
                Category::LeaseFenced,
                format!(
                    "durable: object {} could not confirm its lease before it lapsed; the writer \
                     has fenced itself",
                    self.id
                ),
            ));
        }
        Ok(())
    }

    /// Marks this writer as no longer the writer, and stops renewing.
    ///
    /// It signals the heartbeat rather than waiting for it, because a failed
    /// renewal is one of the ways an object gets fenced — and that runs *on*
    /// the heartbeat thread, which cannot wait for itself.
    fn fence(&self) {
        lock(&self.state).fenced = true;
        self.signal_stop();
    }

    fn signal_stop(&self) {
        let (flag, condvar) = &self.stop;
        *lock(flag) = true;
        condvar.notify_all();
    }

    // ------------------------------------------------------------ restore

    /// Brings the manifest's state into the scratch engine: base, then WAL in
    /// order.
    ///
    /// Both are verified against length and SHA-256 before use, and a missing or
    /// mismatched object stops the open rather than yielding a partially
    /// recovered database (§4.5).
    fn restore(&self) -> Result<()> {
        let head = lock(&self.state).head.clone();
        let db = head.manifest.db.clone();
        let mut engine = lock(&self.engine);

        match &head.manifest.base {
            None => engine.create_database(&db)?,
            Some(base) => {
                let mut reader = self.backend.open_reader(&base.key)?.ok_or_else(|| {
                    err(
                        Category::Corrupt,
                        format!(
                            "durable: manifest names base {}, which is not present at {}",
                            base.key,
                            self.backend.describe()
                        ),
                    )
                })?;
                let staged = self.scratch.staging.join(format!("base-{}.part", uuid8()));
                // Keep the archive extension: ClickHouse decides "archive or
                // directory" from it, so a .tar.gz restored from a path without
                // one is reported as simply not being a backup.
                let archive = self
                    .scratch
                    .backups
                    .join(format!("base-{}.tar.gz", uuid8()));
                stream_to_verified_file(&mut reader, base, &staged, &archive, "base checkpoint")?;
                drop(reader);

                // A restore is where an incompatible archive actually surfaces.
                // The gate already established that this engine is allowed to
                // read the object, so a RESTORE that fails anyway means the
                // compatibility promise was violated rather than that the
                // engine hit an ordinary error. The distinction is the caller's
                // next move: upgrade the engine, or report a core defect.
                engine.restore_database(&db, &archive).map_err(|cause| {
                    err(
                        Category::EngineIncompatible,
                        format!(
                            "durable: restoring base {} into {db:?} failed on chdb {}. The \
                             archive was produced by {} and the object declares min_reader {} at \
                             archive format {}, so this restore was expected to be supported: \
                             {cause}",
                            base.key,
                            self.running.version,
                            head.engine.version,
                            head.engine.min_reader,
                            head.engine.backup_format
                        ),
                    )
                    .with_bounds(head.engine.version.clone(), self.running.version.clone())
                })?;
                let _ = fs::remove_file(&archive);
            }
        }

        engine.use_database(&db)?;

        for reference in &head.manifest.wal {
            let segment = self.backend.get_bytes(&reference.key)?.ok_or_else(|| {
                err(
                    Category::Corrupt,
                    format!(
                        "durable: manifest names WAL segment {}, which is not present at {}",
                        reference.key,
                        self.backend.describe()
                    ),
                )
            })?;
            assert_digest(reference, &digest_of(&segment), "WAL segment")?;
            for sql in decode_segment(&segment, &reference.key)? {
                // Replay goes straight to the engine. Routing it back through
                // the public execute would re-analyse statements core already
                // accepted and, worse, append every one of them to the WAL a
                // second time.
                engine.run(&sql)?;
            }
        }
        Ok(())
    }

    // ------------------------------------------------------ read and write

    fn query(&self, sql: &str, format: OutputFormat) -> Result<Vec<u8>> {
        let _ops = lock(&self.ops);
        // A fenced writer may still read: the local database is exactly what
        // this instance restored and wrote, and §5.7 fences writes, not reads.
        // So this is the lifecycle check on its own, not assert_usable.
        if lock(&self.state).lifecycle != Lifecycle::Open {
            return Err(err(
                Category::Closed,
                format!("durable: object {} is closed", self.id),
            ));
        }
        let database = self.database();
        let mut engine = lock(&self.engine);
        let analysis = engine.analyze(sql, &database)?;
        assert_query_allowed(&analysis)?;
        engine.query(sql, format)
    }

    fn execute(&self, sql: &str) -> Result<WriteTicket> {
        let _ops = lock(&self.ops);
        self.assert_writer(false)?;
        let database = self.database();

        let analysis = lock(&self.engine).analyze(sql, &database)?;
        assert_execute_allowed(&analysis, &database)?;

        // Both limits are checked here rather than at flush, and before the
        // statement runs rather than after.
        //
        // The ordering is the point. A statement that has executed cannot be
        // un-executed, so a buffer that has grown past what a segment can hold
        // can no longer be flushed at all — every flush would fail encoding
        // while the local database has already moved on. Checkpoint can still
        // rescue it, since it archives the database rather than the buffer, but
        // a caller has to know that, and nothing would have warned it. Worse,
        // that state is reachable from a single transient failure: a flush that
        // fails leaves the buffer intact by design, and a caller that keeps
        // writing walks straight into it.
        assert_statement_within_limit(sql)?;
        let line = line_bytes(sql);
        let projected = lock(&self.state).wal_buffer_bytes + line;
        if projected > MAX_WAL_SEGMENT_BYTES {
            return Err(err(
                Category::LimitExceeded,
                format!(
                    "durable: this statement would take the unflushed buffer to {projected} \
                     bytes, over the {MAX_WAL_SEGMENT_BYTES}-byte WAL segment limit; flush or \
                     checkpoint first"
                ),
            )
            .with_limit(MAX_WAL_SEGMENT_BYTES, projected));
        }

        lock(&self.engine).run(sql)?;

        let mut state = lock(&self.state);
        state.wal_buffer.push(sql.to_string());
        state.wal_buffer_bytes += line;
        state.statement_counter += 1;
        Ok(WriteTicket {
            statement: state.statement_counter,
        })
    }

    fn flush(&self) -> Result<Option<ObjectRef>> {
        let _ops = lock(&self.ops);
        self.flush_locked(false)
    }

    fn flush_through(&self, ticket: WriteTicket) -> Result<()> {
        if ticket.statement <= lock(&self.state).committed_statements {
            return Ok(());
        }
        let _ops = lock(&self.ops);
        // Checked again inside the queue: the flush that covers this ticket may
        // be the one that just finished ahead of us, which is what makes
        // concurrent callers coalesce onto a single head write.
        if ticket.statement <= lock(&self.state).committed_statements {
            return Ok(());
        }
        self.flush_locked(false).map(|_| ())
    }

    /// Publishes the buffered statements as one immutable WAL segment and
    /// commits the reference. Assumes the operation gate is held.
    fn flush_locked(&self, closing: bool) -> Result<Option<ObjectRef>> {
        self.assert_writer(closing)?;

        let (statements, generation, next_seq) = {
            let state = lock(&self.state);
            (
                state.wal_buffer.clone(),
                state.head.lease.generation,
                state.head.manifest.seq + 1,
            )
        };
        if statements.is_empty() {
            return Ok(None);
        }

        let data = encode_segment(&statements)?;
        let digest = digest_of(&data);
        let reference = ObjectRef {
            key: wal_key(generation, next_seq),
            size: digest.size,
            sha256: digest.sha256,
        };

        self.publish_bytes(&reference, &data)?;
        // The upload is I/O and can outlast the lease. Checking only at entry
        // would let a writer that was required to self-fence mid-upload go on
        // to commit the manifest anyway.
        self.assert_writer(closing)?;

        let published = reference.clone();
        self.commit_head(
            &reference.key,
            |current| {
                let mut next = current.clone();
                next.manifest.wal.push(published.clone());
                next.manifest.seq = current.manifest.seq + 1;
                next
            },
            |observed| observed.manifest.wal.iter().any(|w| w.key == reference.key),
            false,
        )?;

        let mut state = lock(&self.state);
        state.wal_buffer.drain(..statements.len());
        state.wal_buffer_bytes = state.wal_buffer.iter().map(|sql| line_bytes(sql)).sum();
        state.committed_statements += statements.len() as u64;
        state.last_flush_at = Some(SystemTime::now());
        Ok(Some(reference))
    }

    fn checkpoint(&self) -> Result<ObjectRef> {
        let _ops = lock(&self.ops);
        self.checkpoint_locked()
    }

    /// Replaces the base with a full backup of the current local database and
    /// clears the WAL list. Assumes the operation gate is held.
    ///
    /// V1 has no garbage collection, by design — it also has no destroy, so
    /// nothing here may delete. A superseded base, the folded WAL segments and
    /// any orphans left by an indeterminate commit all stay in object storage;
    /// only the current base and live WAL are referenced.
    fn checkpoint_locked(&self) -> Result<ObjectRef> {
        self.assert_writer(false)?;

        let (database, generation, next_seq) = {
            let state = lock(&self.state);
            (
                state.head.manifest.db.clone(),
                state.head.lease.generation,
                state.head.manifest.seq + 1,
            )
        };

        let archive = self
            .scratch
            .backups
            .join(format!("checkpoint-{}.tar.gz", uuid8()));
        // Core builds the BACKUP statement and quotes both the database and the
        // path itself (§3.2) — no SQL is assembled here.
        lock(&self.engine)
            .backup_database(&database, &archive)
            .inspect_err(|_| {
                let _ = fs::remove_file(&archive);
            })?;

        let outcome = self.publish_checkpoint(&archive, generation, next_seq);
        let _ = fs::remove_file(&archive);
        let reference = outcome?;

        // A full backup plus its upload is the longest thing this object does,
        // easily longer than a lease TTL. Ownership was checked before it
        // started; it has to hold now, when the commit actually happens.
        self.assert_writer(false)?;

        let covered = lock(&self.state).wal_buffer.len();
        let published = reference.clone();
        self.commit_head(
            &reference.key,
            |current| {
                let mut next = current.clone();
                next.manifest.base = Some(published.clone());
                next.manifest.wal = Vec::new();
                next.manifest.seq = current.manifest.seq + 1;
                next
            },
            |observed| {
                observed
                    .manifest
                    .base
                    .as_ref()
                    .is_some_and(|base| base.key == reference.key)
            },
            false,
        )?;

        // Only now. Until the head names this base, the old base plus the old
        // WAL is still the authoritative state, and these statements are only
        // recoverable from the buffer.
        let mut state = lock(&self.state);
        state.wal_buffer.drain(..covered);
        state.wal_buffer_bytes = state.wal_buffer.iter().map(|sql| line_bytes(sql)).sum();
        state.committed_statements += covered as u64;
        state.last_checkpoint_at = Some(SystemTime::now());
        Ok(reference)
    }

    fn publish_checkpoint(
        &self,
        archive: &Path,
        generation: u64,
        next_seq: u64,
    ) -> Result<ObjectRef> {
        let digest = digest_file(archive)
            .map_err(|e| engine_io_err(e, "durable: cannot read the archive just written"))?;
        let reference = ObjectRef {
            key: checkpoint_key(generation, next_seq),
            size: digest.size,
            sha256: digest.sha256.clone(),
        };
        self.publish_file(&reference, archive, &digest)?;
        Ok(reference)
    }

    // -------------------------------------------------- immutable publish

    fn publish_bytes(&self, reference: &ObjectRef, data: &[u8]) -> Result<()> {
        match self.backend.put_bytes_if_absent(&reference.key, data)? {
            PutOutcome::Created => Ok(()),
            outcome => self.reconcile_upload(reference, outcome),
        }
    }

    fn publish_file(&self, reference: &ObjectRef, local: &Path, digest: &Digest) -> Result<()> {
        match self
            .backend
            .put_file_if_absent(&reference.key, local, digest)?
        {
            PutOutcome::Created => Ok(()),
            outcome => self.reconcile_upload(reference, outcome),
        }
    }

    /// Settles an upload that did not cleanly create (§5.8).
    ///
    /// The key is unique to this attempt, so "already exists" can only mean an
    /// earlier try by this same writer landed. Re-reading and comparing the
    /// digest is what turns a lost response into a fact: matching bytes are the
    /// bytes we meant to publish, different bytes are corruption, and an absent
    /// object means the write genuinely did not happen.
    fn reconcile_upload(&self, reference: &ObjectRef, outcome: PutOutcome) -> Result<()> {
        let Some(mut reader) = self.backend.open_reader(&reference.key)? else {
            if outcome == PutOutcome::AlreadyExists {
                return Err(err(
                    Category::Corrupt,
                    format!(
                        "durable: {} was reported as existing but cannot be read from {}",
                        reference.key,
                        self.backend.describe()
                    ),
                ));
            }
            return Err(err(
                Category::CommitAmbiguous,
                format!(
                    "durable: could not determine whether {} was uploaded to {}",
                    reference.key,
                    self.backend.describe()
                ),
            )
            .with_key(reference.key.clone()));
        };
        let observed = drain_digest(&mut reader)
            .map_err(|e| backend_err(e, format!("durable: cannot re-read {}", reference.key)))?;
        assert_digest(reference, &observed, "published object")
    }

    // ------------------------------------------------------- head commit

    /// The single path through which this object writes `head.json`.
    ///
    /// Every caller supplies two things: how to build the next head, and how to
    /// recognise its own intent in a head it did not write. The second is what
    /// makes a lost response recoverable — after re-reading, either the intent
    /// is visible and this committed, or ownership is gone and this is fenced,
    /// or neither is true and it can try again inside the deadline.
    ///
    /// `skip_ownership_after` allows a commit to be recognised as landed even
    /// though this instance no longer owns the lease. Only lease release sets
    /// it, because success there *is* the loss of ownership.
    fn commit_head(
        &self,
        key: &str,
        build: impl Fn(&Head) -> Head,
        committed: impl Fn(&Head) -> bool,
        skip_ownership_after: bool,
    ) -> Result<()> {
        let _gate = lock(&self.head_gate);
        let deadline = Instant::now() + self.tuning.commit_deadline;
        let mut saw_ambiguous = false;

        for attempt in 1.. {
            let (current, etag, raw, generation) = {
                let state = lock(&self.state);
                (
                    state.head.clone(),
                    state.etag.clone(),
                    state.raw.clone(),
                    state.head.lease.generation,
                )
            };

            let candidate = raise_compatibility_floor(build(&current), &self.running)?;
            let body = serialize_head(&candidate, Some(&raw))?;

            match self.backend.replace_if_match(HEAD_KEY, &body, &etag)? {
                super::backend::ReplaceOutcome::Done { etag } => {
                    self.adopt(candidate, etag, None);
                    return Ok(());
                }
                super::backend::ReplaceOutcome::Ambiguous => saw_ambiguous = true,
                super::backend::ReplaceOutcome::NotMatched => {}
            }

            // Either someone else wrote, or the answer was lost. Both are
            // settled the same way: look at what is actually there.
            let fresh = read_head(&*self.backend)?.ok_or_else(|| {
                err(
                    Category::Corrupt,
                    format!(
                        "durable: {HEAD_KEY} disappeared from {} while committing",
                        self.backend.describe()
                    ),
                )
            })?;

            let still_ours = fresh.head.lease.instance_is(&self.instance)
                && fresh.head.lease.generation == generation;

            if committed(&fresh.head) && (still_ours || skip_ownership_after) {
                self.adopt(fresh.head, fresh.etag, Some(fresh.raw));
                return Ok(());
            }
            if !still_ours {
                self.fence();
                return Err(err(
                    Category::LeaseFenced,
                    format!(
                        "durable: object {} was taken over (generation {}); this writer can no \
                         longer commit",
                        self.id, fresh.head.lease.generation
                    ),
                ));
            }

            // Ownership intact and the intent is not there: our ETag was stale.
            // Adopt the current one and retry within the deadline.
            self.adopt(fresh.head, fresh.etag, Some(fresh.raw));

            if attempt >= self.tuning.max_commit_attempts || Instant::now() >= deadline {
                // Two different answers, and the caller acts on them
                // differently. If every attempt came back as a definite
                // refusal, the commit provably did not happen and retrying
                // later is safe. If any attempt was ambiguous, it may have
                // landed, and a blind retry could publish twice.
                if saw_ambiguous {
                    return Err(err(
                        Category::CommitAmbiguous,
                        format!(
                            "durable: gave up committing {key} after {attempt} attempts without \
                             proving the outcome"
                        ),
                    )
                    .with_key(key.to_string()));
                }
                return Err(err(
                    Category::Timeout,
                    format!(
                        "durable: could not commit {key} within {:?} ({attempt} attempts, each \
                         definitively refused); nothing was committed",
                        self.tuning.commit_deadline
                    ),
                ));
            }
        }
        unreachable!("the attempt loop returns from inside")
    }

    /// Records a head as the committed one.
    ///
    /// When `raw` is `None` the candidate was this build's own construction, so
    /// the raw document is re-derived from it — the same merge that produced the
    /// bytes just written, which keeps the unknown fields that went out with
    /// them.
    fn adopt(&self, head: Head, etag: String, raw: Option<Map<String, Value>>) {
        let mut state = lock(&self.state);
        let raw = raw.or_else(|| {
            // Neither step can fail here: these are the inputs serialize_head
            // accepted a moment ago, and its output is what parse_head
            // validates. If one somehow did, keeping the previous raw is
            // harmless rather than lossy — every known field is patched from
            // the typed head on the next write.
            serialize_head(&head, Some(&state.raw))
                .ok()
                .and_then(|body| parse_head(&body).ok())
                .map(|(_, parsed)| parsed)
        });
        if let Some(raw) = raw {
            state.raw = raw;
        }
        if !self.read_only {
            // The stored expiry is authoritative, including when a commit
            // reconciled onto a head written by an earlier attempt whose expiry
            // is older than this one's. A released lease leaves the deadline
            // alone; release_lease_locked clears it explicitly.
            if let Some(deadline) = lease_deadline_from(&head.lease, self.tuning.lease_ttl) {
                state.lease_deadline = Some(deadline);
            }
        }
        state.head = head;
        state.etag = etag;
    }

    // ------------------------------------------------------------- lease

    /// Extends the expiry, leaving generation and seq untouched.
    fn renew_lease(&self) -> Result<()> {
        {
            let state = lock(&self.state);
            if self.read_only || state.fenced || state.lifecycle == Lifecycle::Closed {
                return Ok(());
            }
        }
        let owner = self.owner.clone();
        let instance = self.instance.clone();
        let ttl = self.tuning.lease_ttl.as_secs_f64();

        self.commit_head(
            HEAD_KEY,
            move |current| {
                // The expiry is computed inside the commit, not before waiting
                // for the head gate. Computed early and then delayed behind a
                // long checkpoint commit, the value written could already be in
                // the past — while the local deadline was refreshed from the
                // current clock. The object would then keep writing under a
                // lease other writers are entitled to take.
                let mut next = current.clone();
                next.lease.owner = Some(owner.clone());
                next.lease.instance = Some(instance.clone());
                next.lease.expires_at = Some(now_seconds() + ttl);
                next
            },
            |observed| observed.lease.instance_is(&self.instance),
            false,
        )?;

        // Believe what is actually stored, not what a fresh clock would allow:
        // reconciliation can settle on a head written by an earlier attempt
        // whose expiry is older than this one's.
        let mut state = lock(&self.state);
        state.lease_deadline = lease_deadline_from(&state.head.lease, self.tuning.lease_ttl);
        Ok(())
    }

    fn release_lease_locked(&self) -> Result<()> {
        self.commit_head(
            HEAD_KEY,
            |current| {
                let mut next = current.clone();
                next.lease = Lease::released(current.lease.generation);
                next
            },
            |observed| observed.lease.instance.is_none(),
            // Releasing is the last thing this instance does; after it succeeds
            // the ownership check would fail by construction.
            true,
        )?;
        let mut state = lock(&self.state);
        state.fenced = false;
        state.lease_deadline = None;
        Ok(())
    }

    // ------------------------------------------------------------- close

    fn close(&self) -> Result<()> {
        {
            let mut state = lock(&self.state);
            if state.lifecycle != Lifecycle::Open {
                return Ok(());
            }
            state.lifecycle = Lifecycle::Closing;
        }
        self.stop_heartbeat();

        // Taking the queue is the drain: whatever was in flight finishes
        // first, and it is held until the engine is gone, so nothing can slip
        // in behind the durability barrier.
        let _ops = lock(&self.ops);
        let mut failure = None;

        if !self.read_only {
            // Read inside the queue rather than before it: another thread may
            // have executed or been fenced while close was waiting.
            let (fenced, pending) = {
                let state = lock(&self.state);
                (state.fenced, state.wal_buffer.len())
            };
            if fenced {
                if pending > 0 {
                    failure = Some(err(
                        Category::LeaseFenced,
                        format!(
                            "durable: object {} lost its lease; {pending} buffered statement(s) \
                             were not persisted",
                            self.id
                        ),
                    ));
                }
            } else {
                failure = self
                    .flush_locked(true)
                    .and_then(|_| self.release_lease_locked())
                    .err();
            }
        }

        // The native connection and the scratch tree go back whatever happened
        // above: a remote failure is a durability problem, not a reason to leak
        // a connection or a temp tree.
        if let Err(e) = self.reclaim() {
            failure = failure.or(Some(e));
        }
        lock(&self.state).lifecycle = Lifecycle::Closed;
        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Releases the engine and the scratch tree. Touches no network.
    fn reclaim(&self) -> Result<()> {
        if self.reclaimed.swap(true, AtomicOrdering::SeqCst) {
            return Ok(());
        }
        let closed = lock(&self.engine).close();
        let removed = fs::remove_dir_all(&self.scratch.root).map_err(|e| {
            backend_err(
                e,
                "durable: cannot remove the scratch directory".to_string(),
            )
        });
        closed.and(removed)
    }

    fn stop_heartbeat(&self) {
        self.signal_stop();
        if let Some(handle) = lock(&self.heartbeat).take() {
            let _ = handle.join();
        }
    }

    // -------------------------------------------------------- accessors

    fn database(&self) -> String {
        lock(&self.state).head.manifest.db.clone()
    }

    fn stats(&self) -> Stats {
        let state = lock(&self.state);
        Stats {
            id: self.id.clone(),
            database: state.head.manifest.db.clone(),
            read_only: self.read_only,
            state: state.lifecycle.as_str(),
            fenced: state.fenced,
            generation: state.head.lease.generation,
            owner: self.owner.clone(),
            instance: self.instance.clone(),
            committed_seq: state.head.manifest.seq,
            base_key: state.head.manifest.base.as_ref().map(|b| b.key.clone()),
            wal_segments: state.head.manifest.wal.len(),
            executed_statements: state.statement_counter,
            committed_statements: state.committed_statements,
            pending_statements: state.wal_buffer.len(),
            pending_bytes: state.wal_buffer_bytes,
            last_flush_at: state.last_flush_at,
            last_checkpoint_at: state.last_checkpoint_at,
        }
    }
}

/// When this writer stops believing a lease it just wrote.
///
/// The earlier of what is stored and what the local clock allows. Trusting only
/// the stored value would extend the deadline past the TTL if a remote clock
/// runs fast; trusting only the local clock would extend it past what another
/// writer will honour.
fn lease_deadline_from(lease: &Lease, ttl: Duration) -> Option<Instant> {
    let expires_at = lease.expires_at?;
    let local = Instant::now() + ttl;
    let remaining = expires_at - now_seconds();
    if remaining <= 0.0 {
        return Some(Instant::now());
    }
    let stored = Instant::now() + Duration::from_secs_f64(remaining);
    Some(stored.min(local))
}

/// One open durable object.
///
/// Every public operation serializes on the object's own queue, together with
/// the heartbeat's head compare-and-swap (§5.3). The contract is explicit that
/// "the runtime happens to serialize this" is not an argument, so the queue is
/// real rather than notional, and the handle is `Send + Sync`.
pub struct DurableObject {
    core: Arc<Core>,
}

impl std::fmt::Debug for DurableObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableObject")
            .field("id", &self.core.id)
            .field("read_only", &self.core.read_only)
            .finish_non_exhaustive()
    }
}

impl DurableObject {
    /// The object's id within its namespace.
    pub fn id(&self) -> &str {
        &self.core.id
    }

    /// Whether this handle holds a lease.
    pub fn read_only(&self) -> bool {
        self.core.read_only
    }

    /// The database this object holds, fixed for its lifetime.
    pub fn database(&self) -> String {
        self.core.database()
    }

    /// The lease generation currently recorded in the head.
    pub fn generation(&self) -> u64 {
        lock(&self.core.state).head.lease.generation
    }

    /// A copy of the committed manifest. Observability; never authority.
    pub fn manifest(&self) -> Manifest {
        lock(&self.core.state).head.manifest.clone()
    }

    /// The absolute path of this object's private scratch tree.
    pub fn scratch_path(&self) -> &Path {
        &self.core.scratch.root
    }

    /// Whether this writer has lost, or given up on, its lease.
    pub fn is_fenced(&self) -> bool {
        lock(&self.core.state).fenced
    }

    /// A point-in-time snapshot for a status endpoint or a log line.
    pub fn stats(&self) -> Stats {
        self.core.stats()
    }

    /// Runs one read-only statement and returns its formatted bytes.
    ///
    /// It is refused unless core proves the text is exactly one `READ_ONLY`
    /// statement: the method name is not the gate, the analysis is. Nothing is
    /// added to the WAL, and read-only SQL carrying a credential runs — but a
    /// failure from it is reported without the statement text.
    pub fn query(&self, sql: &str, format: OutputFormat) -> Result<Vec<u8>> {
        self.core.query(sql, format)
    }

    /// Runs one mutating statement locally and buffers it for the WAL.
    ///
    /// Returning does **not** mean the statement is durable — it means it ran
    /// here and is queued to be published. The returned ticket is the watermark
    /// to pass to [`Self::flush_through`] when a caller needs the write to
    /// survive losing this machine before it answers someone.
    pub fn execute(&self, sql: &str) -> Result<WriteTicket> {
        self.core.execute(sql)
    }

    /// Publishes the buffered statements as one immutable WAL segment and
    /// commits the reference (§5.4).
    ///
    /// Returns the published reference, or `None` when there was nothing
    /// buffered. A returned reference means the head commit is confirmed — this
    /// is the recovery point another process would restore to.
    pub fn flush(&self) -> Result<Option<ObjectRef>> {
        self.core.flush()
    }

    /// A durability barrier for one ticket.
    ///
    /// Returns immediately if that statement is already committed, which is
    /// what makes concurrent callers coalesce onto a single head write: the
    /// first through the queue publishes the segment covering all of them, and
    /// the rest find their watermark already met.
    pub fn flush_through(&self, ticket: WriteTicket) -> Result<()> {
        self.core.flush_through(ticket)
    }

    /// Folds base and WAL into a fresh base and clears the WAL list (§5.5).
    ///
    /// Checkpoints are *full* snapshots. An incremental backup records its base
    /// by local path, which does not survive object storage, so a portable
    /// incremental chain is V2 work.
    ///
    /// It holds the operation queue for its whole duration, so nothing new is
    /// executed into a database that is being archived. The heartbeat is
    /// unaffected: it contends only for the head gate, which this takes just
    /// for the final commit.
    pub fn checkpoint(&self) -> Result<ObjectRef> {
        self.core.checkpoint()
    }

    /// Drains, flushes, releases the lease, then releases local resources.
    ///
    /// Local cleanup happens whether or not the remote steps worked, and a
    /// remote failure is still returned: a close that swallowed a failed flush
    /// would be reporting a durability barrier it did not reach.
    ///
    /// This consumes the handle, because it is the durability barrier and there
    /// is nothing useful to do with the object afterwards. Dropping without
    /// closing reclaims local resources only — see the `Drop` implementation.
    pub fn close(self) -> Result<()> {
        self.core.close()
    }
}

impl Drop for DurableObject {
    /// Reclaims local resources only — never a stand-in for [`Self::close`].
    ///
    /// §5.6 allows a destructor to tidy up but not to impersonate a durability
    /// barrier, so this touches no network: it will not flush, and it will not
    /// release the lease, which then expires on its own TTL. It exists so a
    /// forgotten `close` does not leave a multi-gigabyte scratch directory and
    /// an open engine behind.
    fn drop(&mut self) {
        if lock(&self.core.state).lifecycle == Lifecycle::Closed {
            return;
        }
        self.core.stop_heartbeat();
        let _ = self.core.reclaim();
        lock(&self.core.state).lifecycle = Lifecycle::Closed;
    }
}

/// Runs the writer and read-only open sequences of contract §5.2.
pub(crate) fn open_object(
    id: &str,
    backend: Arc<dyn Backend>,
    factory: &EngineFactory,
    options: OpenOptions,
) -> Result<(DurableObject, bool)> {
    let tuning = options.tuning.clone();
    tuning.validate()?;

    let owner = options
        .owner
        .clone()
        .unwrap_or_else(|| format!("chdb-rust-{}", std::process::id()));
    let instance = new_instance_id();
    let mut engine = factory()?;

    // Compatibility is settled before anything is created or claimed: an object
    // this engine cannot read should cost nothing but two strings.
    let probed = (|| -> Result<(RunningEngine, Option<HeadSnapshot>)> {
        let running = RunningEngine {
            version: engine.version()?,
            backup_format: engine.backup_format()?,
        };
        let existing = read_head(&*backend)?;
        match &existing {
            Some(snapshot) => {
                assert_readable(&snapshot.head)?;
                assert_engine_compatible(&snapshot.head, &running)?;
                if !options.read_only {
                    assert_writable(&snapshot.head)?;
                }
            }
            None if options.read_only || options.existing_only => {
                return Err(err(
                    Category::NotFound,
                    format!(
                        "durable: object {id} does not exist at {}",
                        backend.describe()
                    ),
                ))
            }
            None => {}
        }
        Ok((running, existing))
    })();
    let (running, existing) = match probed {
        Ok(value) => value,
        Err(e) => {
            let _ = engine.close();
            return Err(e);
        }
    };
    let existed = existing.is_some();

    // Take the lease (or create the object) before any local resource exists,
    // so losing the race costs nothing but the round-trips.
    let snapshot = if options.read_only {
        existing.expect("a read-only open of a missing object was refused above")
    } else {
        let database = options.database.clone().unwrap_or_else(|| "default".into());
        let taken = match existing {
            Some(existing) => acquire_lease(
                &*backend,
                existing,
                LeaseParams {
                    instance: &instance,
                    owner: &owner,
                    tuning: &tuning,
                    force: options.force,
                },
            ),
            None => create_cold(
                &*backend,
                ColdParams {
                    id,
                    database: &database,
                    running: &running,
                    instance: &instance,
                    owner: &owner,
                    tuning: &tuning,
                },
            ),
        };
        match taken {
            Ok(snapshot) => snapshot,
            Err(e) => {
                let _ = engine.close();
                return Err(e);
            }
        }
    };

    let scratch = match make_scratch(options.scratch_root.as_deref()) {
        Ok(scratch) => scratch,
        Err(e) => {
            let _ = engine.close();
            if !options.read_only {
                let _ = release_lease(&*backend, &instance);
            }
            return Err(backend_err(
                e,
                "durable: cannot create a scratch directory".to_string(),
            ));
        }
    };

    if let Err(e) = engine.start(EngineStartOptions {
        data_path: scratch.data.clone(),
        backups_allowed_path: scratch.backups.clone(),
    }) {
        let _ = engine.close();
        let _ = fs::remove_dir_all(&scratch.root);
        if !options.read_only {
            let _ = release_lease(&*backend, &instance);
        }
        return Err(e);
    }

    let lease_deadline = if options.read_only {
        None
    } else {
        lease_deadline_from(&snapshot.head.lease, tuning.lease_ttl)
    };
    let core = Arc::new(Core {
        id: id.to_string(),
        read_only: options.read_only,
        backend,
        engine: Mutex::new(engine),
        scratch,
        tuning,
        instance,
        owner,
        running,
        ops: Mutex::new(()),
        head_gate: Mutex::new(()),
        state: Mutex::new(State {
            head: snapshot.head,
            etag: snapshot.etag,
            raw: snapshot.raw,
            wal_buffer: Vec::new(),
            wal_buffer_bytes: 0,
            statement_counter: 0,
            committed_statements: 0,
            last_flush_at: None,
            last_checkpoint_at: None,
            lifecycle: Lifecycle::Open,
            fenced: false,
            lease_deadline,
        }),
        stop: (Mutex::new(false), Condvar::new()),
        heartbeat: Mutex::new(None),
        reclaimed: AtomicBool::new(false),
    });

    let opened = core.restore().and_then(|()| {
        if core.read_only {
            // No lease, no heartbeat: the manifest read above is the snapshot
            // this handle serves for its whole life. Immutable references make
            // that safe even while a writer keeps committing.
            return Ok(());
        }
        // Restore can outlast a lease. Confirming ownership before the handle
        // escapes is what stops a writer from starting work on a database
        // someone else has already taken over (§5.2 step 7).
        core.renew_lease()
    });

    if let Err(e) = opened {
        let _ = core.reclaim();
        lock(&core.state).lifecycle = Lifecycle::Closed;
        if !core.read_only {
            // Leaving a lease stranded until its TTL would block the next
            // writer for no reason — the object was never opened.
            let _ = release_lease(&*core.backend, &core.instance);
        }
        return Err(e);
    }

    if !core.read_only {
        start_heartbeat(&core);
    }
    Ok((DurableObject { core }, existed))
}

/// Renews the lease on a cadence, and stops writing if renewal fails.
///
/// A renewal is a head commit like any other, so it queues behind whatever
/// commit is running rather than racing it for the ETag (§5.7). A renewal that
/// fails is not fatal on its own — the next attempt may succeed. What is fatal
/// is reaching the locally believed expiry without a confirmation, and
/// `assert_writer` checks exactly that before every write.
fn start_heartbeat(core: &Arc<Core>) {
    let worker = Arc::clone(core);
    let interval = core.tuning.heartbeat_interval;
    let handle = std::thread::Builder::new()
        .name(format!("chdb-durable-lease-{}", core.id))
        .spawn(move || loop {
            let (flag, condvar) = &worker.stop;
            let stopped = {
                let guard = lock(flag);
                let (guard, _timeout) = condvar
                    .wait_timeout(guard, interval)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *guard
            };
            if stopped {
                return;
            }
            {
                let state = lock(&worker.state);
                if state.lifecycle != Lifecycle::Open || state.fenced {
                    return;
                }
            }
            if let Err(e) = worker.renew_lease() {
                if e.category() == Category::LeaseFenced {
                    return; // commit_head has already fenced this writer
                }
            }
        });
    match handle {
        Ok(handle) => *lock(&core.heartbeat) = Some(handle),
        // A process that cannot spawn a thread still has a valid lease for one
        // TTL; without renewal the writer will fence itself when it lapses,
        // which is the safe outcome rather than a silent unbounded lease.
        Err(_) => core.fence(),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn a_heartbeat_slower_than_a_third_of_the_ttl_is_refused() {
        let tuning = Tuning {
            lease_ttl: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(11),
            ..Tuning::default()
        };
        let error = tuning.validate().expect_err("no room for a lost renewal");
        assert!(error.to_string().contains("at most a third"), "{error}");

        assert!(Tuning::default().validate().is_ok());
    }

    #[test]
    fn a_zero_duration_would_put_an_expiry_in_the_past() {
        for tuning in [
            Tuning {
                lease_ttl: Duration::ZERO,
                ..Tuning::default()
            },
            Tuning {
                heartbeat_interval: Duration::ZERO,
                ..Tuning::default()
            },
            Tuning {
                commit_deadline: Duration::ZERO,
                ..Tuning::default()
            },
            Tuning {
                max_commit_attempts: 0,
                ..Tuning::default()
            },
        ] {
            assert!(tuning.validate().is_err(), "{tuning:?}");
        }
    }

    #[test]
    fn a_lease_deadline_is_the_earlier_of_the_clock_and_the_document() {
        let ttl = Duration::from_secs(30);
        // An expiry an hour out is not believed past the local TTL.
        let generous = Lease {
            generation: 1,
            owner: Some("w".into()),
            instance: Some("i".into()),
            expires_at: Some(now_seconds() + 3600.0),
        };
        let deadline = lease_deadline_from(&generous, ttl).expect("a held lease has a deadline");
        assert!(deadline <= Instant::now() + ttl + Duration::from_secs(1));

        // An expiry already behind us is not extended by the local clock.
        let lapsed = Lease {
            expires_at: Some(now_seconds() - 1.0),
            ..generous
        };
        assert!(lease_deadline_from(&lapsed, ttl).expect("still a deadline") <= Instant::now());

        assert!(lease_deadline_from(&Lease::released(1), ttl).is_none());
    }
}
