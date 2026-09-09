//! The object-storage contract every durable backend must satisfy (§5.1).
//!
//! The trait is small on purpose. Only two properties are load-bearing, and
//! both are properties a provider either has or does not:
//!
//! 1. **Real conditional operations.** [`Backend::put_bytes_if_absent`] must be
//!    an atomic create and [`Backend::replace_if_match`] an atomic
//!    compare-and-swap. Simulating either with a read followed by a write is
//!    not a weaker implementation, it is a broken one: the window between the
//!    two is exactly where two writers both conclude they are the only writer.
//! 2. **Streaming.** A checkpoint is a full database archive. Requiring it to
//!    pass through a `Vec<u8>` puts a ceiling on database size that has nothing
//!    to do with the database. So checkpoints move as files and readers; only
//!    the head and WAL segments — both bounded by the protocol — move as bytes.
//!
//! A third property is expressed in the return values rather than the methods:
//! every mutating call can answer "ambiguous". A request whose response was lost
//! is not a failure, and reporting it as one would make a caller retry a commit
//! that already happened. The state machine resolves ambiguity by re-reading
//! (§5.8), which is only possible if the backend admits it.
//!
//! Deleting is absent by design: V1 has no destroy and no garbage collection, so
//! nothing in the protocol has the authority to remove an object.

use std::io::Read;
use std::path::Path;

use super::digest::Digest;
use super::errors::Result;

/// The result of a conditional create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    /// The object was created by this call.
    Created,
    /// The key was already there. For a key minted per attempt this means *we*
    /// created it earlier, on a try whose response never arrived.
    AlreadyExists,
    /// The request may or may not have landed, and the caller has to re-read to
    /// find out.
    Ambiguous,
}

/// The result of a conditional replace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplaceOutcome {
    /// The compare-and-swap succeeded; the new CAS token is attached.
    Done {
        /// The token the next replace has to present.
        etag: String,
    },
    /// The stored token no longer matched: someone else wrote first.
    NotMatched,
    /// The outcome is unknown.
    Ambiguous,
}

/// One object read whole, with the CAS token describing that same version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tagged {
    /// The object's bytes.
    pub data: Vec<u8>,
    /// An opaque compare-and-swap token. Never assumed to be a content MD5.
    pub etag: String,
}

/// A key/value store scoped to one object's prefix.
///
/// Keys are relative, `/`-separated and validated by
/// [`is_valid_object_key`](super::is_valid_object_key): they arrive from a
/// `head.json` that came out of object storage, so they are untrusted input,
/// and an implementation that resolves them against a directory must refuse a
/// traversal rather than trust the caller.
///
/// `Option` distinguishes an absent key from a failure. Absence is a normal,
/// expected answer at nearly every call site — a cold object, a probe for a
/// head — and a caller that has to unwrap an error to learn something ordinary
/// eventually forgets to.
///
/// An implementation is shared across the heartbeat thread and the caller's, so
/// it has to be `Send + Sync`.
pub trait Backend: Send + Sync {
    /// A human-readable location of the object prefix, for logs and errors. It
    /// must never contain credentials.
    fn describe(&self) -> String;

    /// Reads a whole object.
    fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Reads a whole object together with its CAS token.
    ///
    /// The two must describe the same version: pairing an old body with a new
    /// token would let a compare-and-swap succeed against state nobody read.
    fn get_bytes_with_etag(&self, key: &str) -> Result<Option<Tagged>>;

    /// Opens a byte stream for a potentially large object, so a checkpoint
    /// download never has to be resident.
    fn open_reader(&self, key: &str) -> Result<Option<Box<dyn Read + Send>>>;

    /// Atomically creates an object from bytes. It never overwrites.
    fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> Result<PutOutcome>;

    /// Atomically creates an object by uploading a local file. It never
    /// overwrites.
    ///
    /// `digest` is the file's already-computed length and SHA-256 — the
    /// protocol requires both to be known before publishing, so they are passed
    /// rather than recomputed. A backend may use them to sign or verify the
    /// upload without a second pass over the file.
    fn put_file_if_absent(
        &self,
        key: &str,
        local_path: &Path,
        digest: &Digest,
    ) -> Result<PutOutcome>;

    /// Atomically replaces an object only if its stored token still equals
    /// `etag`.
    fn replace_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<ReplaceOutcome>;
}
