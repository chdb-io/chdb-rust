//! Object key construction and validation (contract §4.1).
//!
//! Two separate jobs live here, and they are separate on purpose:
//!
//! * *Minting* a key for something this writer is about to publish. Every
//!   attempt gets a fresh key, including a retry of an attempt that may already
//!   have landed. That is what makes an ambiguous upload resolvable rather than
//!   destructive: a retry can never overwrite the bytes the first try published
//!   (§5.8).
//! * *Validating* a key read out of someone else's head. A reference is a
//!   relative key inside the object prefix and nothing else. Rejecting `..`,
//!   absolute paths and empty segments here is what stops a hostile or broken
//!   head from steering a download outside the object — the local backend
//!   resolves keys against a directory, so a traversal would be a real escape.

use uuid::Uuid;

use super::errors::{err, Category, Result};

/// The one mutable key in an object.
pub const HEAD_KEY: &str = "head.json";

/// The first 8 hex digits of a UUID4, per the frozen key shape.
///
/// Random rather than a counter because two writers minting the same suffix in
/// the same generation and sequence would collide on a key that has to be
/// unique per attempt.
pub(crate) fn uuid8() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

/// Mints `checkpoints/<generation>-<seq>-<uuid8>.tar.gz`.
pub(crate) fn checkpoint_key(generation: u64, seq: u64) -> String {
    format!("checkpoints/{generation}-{seq}-{}.tar.gz", uuid8())
}

/// Mints `wal/<generation>-<seq>-<uuid8>.jsonl`.
pub(crate) fn wal_key(generation: u64, seq: u64) -> String {
    format!("wal/{generation}-{seq}-{}.jsonl", uuid8())
}

/// Is this a relative, `/`-separated key with no empty, `.` or `..` segments?
///
/// Backslashes are rejected too: on a POSIX filesystem a backslash is an
/// ordinary character, so a key holding one would name a different file here
/// than it does on a provider that normalises it — and an object is supposed to
/// mean the same thing wherever it is opened.
pub fn is_valid_object_key(key: &str) -> bool {
    if key.is_empty() || key.starts_with('/') {
        return false;
    }
    if key.contains('\\') || key.contains('\0') {
        return false;
    }
    key.split('/')
        .all(|segment| !matches!(segment, "" | "." | ".."))
}

/// An object id must be a single path segment, so it cannot climb out of its
/// namespace or contain another id's prefix.
pub(crate) fn validate_object_id(id: &str) -> Result<()> {
    let bad = id.is_empty()
        || id.contains('/')
        || id.contains('\\')
        || id.contains('\0')
        || id == "."
        || id == "..";
    if bad {
        return Err(err(
            Category::Backend,
            format!("durable: object id must be a single non-empty path segment, got {id:?}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn a_minted_key_carries_its_generation_sequence_and_a_fresh_suffix() {
        let first = wal_key(3, 9);
        let second = wal_key(3, 9);
        assert!(first.starts_with("wal/3-9-"), "{first}");
        assert!(first.ends_with(".jsonl"), "{first}");
        assert_ne!(first, second, "every attempt has to mint its own key");
        assert!(checkpoint_key(3, 9).starts_with("checkpoints/3-9-"));
        assert!(checkpoint_key(3, 9).ends_with(".tar.gz"));
        assert!(is_valid_object_key(&first));
    }

    #[test]
    fn a_key_that_could_leave_the_object_is_not_a_key() {
        for key in [
            "",
            "/checkpoints/1.tar.gz",
            "../other/head.json",
            "wal/../../escape",
            "wal//1.jsonl",
            "wal/./1.jsonl",
            "wal\\1.jsonl",
        ] {
            assert!(!is_valid_object_key(key), "{key:?} should be refused");
        }
        assert!(is_valid_object_key("head.json"));
        assert!(is_valid_object_key("wal/3-9-acde5678.jsonl"));
    }

    #[test]
    fn an_object_id_is_one_segment() {
        for id in ["", ".", "..", "a/b", "a\\b"] {
            assert!(validate_object_id(id).is_err(), "{id:?} should be refused");
        }
        assert!(validate_object_id("tenant-123").is_ok());
    }
}
