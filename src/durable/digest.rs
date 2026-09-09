//! Length and SHA-256 verification (contract §4.5).
//!
//! Both are checked before a base is restored or a WAL segment is parsed, and
//! neither is redundant: they fail on different things. A truncated upload has
//! the right prefix, and a replaced object has the right length.
//!
//! Checkpoints are hashed by streaming. A full backup must never have to sit in
//! memory in one piece just to be measured (§5.1), so the helper here hashes
//! and counts as it writes, and publishes a downloaded file to its final name
//! only once both match — a caller never sees a scratch path holding unverified
//! bytes.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::Path;

use sha2::{Digest as _, Sha256};

use super::errors::{backend_err, err, Category, Error, Result};
use super::types::ObjectRef;

const CHUNK: usize = 1024 * 1024;

/// The length and content hash of one object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digest {
    /// Length in bytes.
    pub size: u64,
    /// The lowercase full hex SHA-256.
    pub sha256: String,
}

impl Digest {
    /// Measures a buffer.
    ///
    /// Public because [`Backend`](super::Backend) is implementable outside this
    /// crate, and `put_file_if_absent` takes the digest its caller already
    /// computed — an implementation being tested needs a way to produce one.
    pub fn of(data: &[u8]) -> Self {
        digest_of(data)
    }
}

/// Hashes a buffer.
pub(crate) fn digest_of(data: &[u8]) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update(data);
    Digest {
        size: data.len() as u64,
        sha256: hex(&hasher.finalize()),
    }
}

/// Streams a local file through SHA-256 without holding it in memory.
pub(crate) fn digest_file(path: &Path) -> io::Result<Digest> {
    let mut file = File::open(path)?;
    let mut hasher = Counting::new();
    io::copy(&mut file, &mut hasher)?;
    Ok(hasher.finish())
}

/// Reads `source` to the end and reports what went past, without keeping it.
///
/// Used to settle an upload whose response was lost: the question is whether
/// the bytes at that key are ours, and the answer is a digest.
pub(crate) fn drain_digest(source: &mut dyn Read) -> io::Result<Digest> {
    let mut hasher = Counting::new();
    io::copy(source, &mut hasher)?;
    Ok(hasher.finish())
}

/// Refuses an object that is not the one the head describes.
pub(crate) fn assert_digest(reference: &ObjectRef, observed: &Digest, what: &str) -> Result<()> {
    if reference.size != observed.size {
        return Err(err(
            Category::Corrupt,
            format!(
                "durable: {what} ({}) has size {}, head says {}",
                reference.key, observed.size, reference.size
            ),
        ));
    }
    if reference.sha256 != observed.sha256 {
        return Err(err(
            Category::Corrupt,
            format!(
                "durable: {what} ({}) sha256 {} does not match head {}",
                reference.key, observed.sha256, reference.sha256
            ),
        ));
    }
    Ok(())
}

/// Writes `source` into `staging`, verifies it against `reference`, then renames
/// it to `final_path`.
///
/// On any mismatch the scratch file is removed and `final_path` is never
/// created, so a failed verify cannot leave behind something a later step
/// mistakes for a good archive.
pub(crate) fn stream_to_verified_file(
    source: &mut dyn Read,
    reference: &ObjectRef,
    staging: &Path,
    final_path: &Path,
    what: &str,
) -> Result<()> {
    let observed = copy_and_hash(source, staging).map_err(|e| {
        let _ = fs::remove_file(staging);
        backend_err(e, format!("durable: downloading {} failed", reference.key))
    })?;

    if let Err(mismatch) = assert_digest(reference, &observed, what) {
        let _ = fs::remove_file(staging);
        return Err(mismatch);
    }

    fs::rename(staging, final_path).map_err(|e| {
        let _ = fs::remove_file(staging);
        backend_err(
            e,
            format!("durable: publishing verified {} failed", reference.key),
        )
    })
}

fn copy_and_hash(source: &mut dyn Read, staging: &Path) -> io::Result<Digest> {
    let mut file = File::options().create_new(true).write(true).open(staging)?;
    let mut hasher = Counting::new();
    let mut buffer = vec![0u8; CHUNK];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        file.write_all(&buffer[..read])?;
        hasher.write_all(&buffer[..read])?;
    }
    // Flush before the name is published: bytes only in the page cache would
    // let a crash roll back an archive this call has already reported as good.
    file.sync_all()?;
    Ok(hasher.finish())
}

/// Accumulates length and SHA-256 as bytes flow past.
struct Counting {
    hasher: Sha256,
    size: u64,
}

impl Counting {
    fn new() -> Self {
        Self {
            hasher: Sha256::new(),
            size: 0,
        }
    }

    fn finish(self) -> Digest {
        Digest {
            size: self.size,
            sha256: hex(&self.hasher.finalize()),
        }
    }
}

impl Write for Counting {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        self.size += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// An engine-side failure reading an archive this process just wrote.
pub(crate) fn engine_io_err(cause: io::Error, message: impl Into<String>) -> Error {
    Error::wrap(Category::Engine, cause, message)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::tempdir;

    fn reference(key: &str, digest: &Digest) -> ObjectRef {
        ObjectRef {
            key: key.to_string(),
            size: digest.size,
            sha256: digest.sha256.clone(),
        }
    }

    #[test]
    fn the_empty_digest_is_the_known_one() {
        let digest = digest_of(b"");
        assert_eq!(digest.size, 0);
        assert_eq!(
            digest.sha256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn a_file_and_a_buffer_of_the_same_bytes_agree() {
        let tmp = tempdir();
        let path = tmp.path().join("payload");
        let bytes = vec![7u8; CHUNK + 13];
        fs::write(&path, &bytes).expect("a scratch file");
        assert_eq!(digest_file(&path).expect("a digest"), digest_of(&bytes));
    }

    #[test]
    fn a_download_that_does_not_verify_publishes_nothing() {
        let tmp = tempdir();
        let staging = tmp.path().join("incoming.part");
        let published = tmp.path().join("base.tar.gz");
        let expected = reference("checkpoints/1-1-aaaaaaaa.tar.gz", &digest_of(b"the base"));

        let error = stream_to_verified_file(
            &mut &b"not the base"[..],
            &expected,
            &staging,
            &published,
            "base checkpoint",
        )
        .expect_err("a body that does not match the manifest is corrupt");

        assert_eq!(error.category(), Category::Corrupt);
        assert!(!published.exists(), "nothing may be published");
        assert!(!staging.exists(), "the scratch file goes too");
    }

    #[test]
    fn a_download_that_verifies_is_published_under_its_final_name() -> Result<()> {
        let tmp = tempdir();
        let staging = tmp.path().join("incoming.part");
        let published = tmp.path().join("base.tar.gz");
        let expected = reference("checkpoints/1-1-aaaaaaaa.tar.gz", &digest_of(b"the base"));

        stream_to_verified_file(
            &mut &b"the base"[..],
            &expected,
            &staging,
            &published,
            "base checkpoint",
        )?;

        assert_eq!(fs::read(&published).unwrap(), b"the base");
        assert!(!staging.exists());
        Ok(())
    }
}
