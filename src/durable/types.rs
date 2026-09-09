//! The frozen wire types of Durable V1 (contract §4).
//!
//! Everything here is FROZEN: another binding has to be able to read what this
//! one writes. The JSON is compared semantically, not byte for byte — key order
//! and whitespace are free — but field names, types and meanings are not.

/// The protocol baseline this build implements. A higher version in a head
/// means "do not open".
pub const PROTOCOL_VERSION: u64 = 1;

/// V1 defines no non-empty feature names, so anything appearing in a head's
/// feature lists comes from a future revision and the negotiation rules in
/// [`super::negotiate`] apply.
pub const KNOWN_READER_FEATURES: &[&str] = &[];

/// The writer-side counterpart of [`KNOWN_READER_FEATURES`].
pub const KNOWN_WRITER_FEATURES: &[&str] = &[];

/// What a chDB writer records in `head.engine.name`.
pub const ENGINE_NAME: &str = "chdb";

/// The archive-format generation this build understands. V1's baseline is 1.
///
/// It exists to be the one explicit signal that the backward-compatibility
/// promise has been withdrawn: core increments it when a later release can no
/// longer restore earlier full backups, and a reader refuses anything above its
/// own baseline. Without it a reader compares version numbers, sees a larger
/// one, concludes it is fine, and walks into a `RESTORE` that fails halfway
/// through recovery.
///
/// The running engine's own value is not available yet — the C ABI exposes no
/// accessor — so every engine reports this baseline until one does.
pub const BACKUP_FORMAT_BASELINE: u64 = 1;

/// The ceiling on one statement, in UTF-8 bytes (§4.4).
pub const MAX_SQL_BYTES: u64 = 64 * 1024 * 1024;

/// The ceiling on one uncompressed WAL segment (§4.4).
pub const MAX_WAL_SEGMENT_BYTES: u64 = 128 * 1024 * 1024;

/// The ceiling on `head.json` (§4.5).
pub const MAX_HEAD_BYTES: u64 = 1024 * 1024;

/// The largest integer every JSON implementation holds exactly.
///
/// The contract requires all integers in a head to stay inside it, so a value
/// beyond it is refused rather than read differently by the next binding to
/// open the object.
pub const MAX_SAFE_INTEGER: u64 = (1u64 << 53) - 1;

/// The version and feature negotiation block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Protocol {
    /// The protocol revision the object was written against.
    pub version: u64,
    /// Features a reader must implement to interpret the object at all.
    pub reader_features: Vec<String>,
    /// Features a writer must implement to commit to the object.
    pub writer_features: Vec<String>,
}

impl Default for Protocol {
    fn default() -> Self {
        Self {
            version: PROTOCOL_VERSION,
            reader_features: Vec::new(),
            writer_features: Vec::new(),
        }
    }
}

/// Which engine produced the object, and what a reader needs to restore it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineIdentity {
    /// Always `chdb` for an object this crate can open.
    pub name: String,
    /// `chdb_version()` of the writer that last touched the object. Recorded
    /// for diagnosis and audit; explicitly *not* the compatibility gate.
    pub version: String,
    /// The archive-format generation. A reader refuses anything above its own
    /// baseline.
    pub backup_format: u64,
    /// The oldest chDB release that may read the current state.
    pub min_reader: String,
}

/// A reference to one immutable object.
///
/// The size and digest are not decoration: base and WAL are verified against
/// both before anything is restored or replayed, and they are what makes an
/// ambiguous upload resolvable (§5.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRef {
    /// Relative to `<namespace>/<object-id>/`, `/`-separated, no leading slash.
    pub key: String,
    /// Length in bytes.
    pub size: u64,
    /// The lowercase full hex SHA-256.
    pub sha256: String,
}

/// The writer lease.
///
/// A released lease is the all-null form: owner, instance and expiry absent
/// while the generation stays, so the next acquirer knows what to increment
/// past.
#[derive(Debug, Clone, PartialEq)]
pub struct Lease {
    /// Moves forward only on a real change of owner: acquiring from released,
    /// taking over an expired lease, or a force takeover. A heartbeat renews
    /// the expiry and leaves this alone.
    pub generation: u64,
    /// A human-visible name. Observability only; never an authority check.
    pub owner: Option<String>,
    /// One live instance. This, with the generation, is the fence.
    pub instance: Option<String>,
    /// Epoch seconds, fractional allowed.
    pub expires_at: Option<f64>,
}

impl Lease {
    /// A lease nobody holds, at the given generation.
    pub(crate) fn released(generation: u64) -> Self {
        Self {
            generation,
            owner: None,
            instance: None,
            expires_at: None,
        }
    }

    /// Does this lease name a live owner?
    pub(crate) fn is_held(&self) -> bool {
        self.owner.is_some()
    }

    /// Does this lease belong to the given instance?
    pub(crate) fn instance_is(&self, instance: &str) -> bool {
        self.instance.as_deref() == Some(instance)
    }
}

/// The object's committed state: one database, one base, and the WAL to replay
/// over it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// The one database this object holds.
    pub db: String,
    /// The full checkpoint the WAL is replayed onto, or `None` for an object
    /// that has never been checkpointed.
    pub base: Option<ObjectRef>,
    /// Segments in replay order.
    pub wal: Vec<ObjectRef>,
    /// Advances on every published reference, which is what makes a lost CAS
    /// response resolvable.
    pub seq: u64,
}

/// The typed view of `head.json`.
///
/// Unknown fields are not represented here — they travel separately in the raw
/// document, because dropping them would silently strip a future revision's
/// state (§4.2, §4.3).
#[derive(Debug, Clone, PartialEq)]
pub struct Head {
    /// Version and feature negotiation.
    pub protocol: Protocol,
    /// Who wrote the object, and who may read it.
    pub engine: EngineIdentity,
    /// The writer lease.
    pub lease: Lease,
    /// The committed manifest.
    pub manifest: Manifest,
}

impl Head {
    /// The head a brand-new object starts from: no base, no WAL, generation 1.
    ///
    /// The lease is left released here and filled in by the creating writer,
    /// since a cold create and lease acquisition are one conditional write
    /// (§5.2) — publishing an unheld head first would leave a window in which a
    /// second process could take a lease on a manifest nobody has restored yet.
    pub(crate) fn cold(db: &str, engine_version: &str, backup_format: u64) -> Self {
        Self {
            protocol: Protocol::default(),
            engine: EngineIdentity {
                name: ENGINE_NAME.to_string(),
                version: engine_version.to_string(),
                backup_format,
                // A fresh object can only be read by this engine or later: its
                // base will be produced by this engine.
                min_reader: engine_version.to_string(),
            },
            lease: Lease::released(1),
            manifest: Manifest {
                db: db.to_string(),
                base: None,
                wal: Vec::new(),
                seq: 0,
            },
        }
    }
}
