//! Local-filesystem backend.
//!
//! This is the backend every test uses, and the one a developer gets from a
//! `file://` namespace URL. That makes its conditional operations load-bearing
//! rather than a convenience: if they were approximations, every scenario that
//! passes here would prove nothing about the ones that run against a real object
//! store.
//!
//! # Conditional create
//!
//! `link(2)` is the primitive. It fails with `EEXIST` atomically, so writing to
//! a unique scratch file and then linking it into place is a true
//! create-if-absent — with the full contents already in the file at the moment
//! the name appears. Rename would have been simpler and wrong: it clobbers.
//!
//! # Conditional replace
//!
//! POSIX has no compare-and-swap on file contents, and the usual workarounds are
//! worse than the problem. A lock file turns a crash into a stuck object that
//! needs a staleness heuristic to recover; read-compare-rename has the race it
//! is meant to prevent.
//!
//! So the one mutable key is stored as a chain of immutable versions with a
//! symlink naming the current one:
//!
//! ```text
//! head.json                -> symlink to .head-versions/7.json
//! .head-versions/7.json    immutable
//! .head-versions/6.json    immutable
//! ```
//!
//! The ETag is the version number. Replacing against ETag `v7` means creating
//! `.head-versions/8.json`, and `link(2)` lets exactly one racer do that — so
//! the `EEXIST` *is* the compare-and-swap failure. The symlink is then swapped
//! in with an atomic rename. A writer that wins the create and dies before the
//! rename has still legitimately won: a racer reading version 7 will fail to
//! create 8 and correctly report not-replaced.
//!
//! Version numbers only ever move forward, because creating version N+1 requires
//! having read version N, which requires the symlink to already point at it.
//!
//! # Reading what another binding wrote
//!
//! An object produced elsewhere has a plain `head.json` file and no version
//! chain. That reads fine — the ETag is then a content digest — and the first
//! `replace_if_match` adopts it into the chain: verify the plain file still
//! hashes to the ETag, create `.head-versions/1.json` exclusively, swap the
//! symlink. Adoption is atomic against other adopters, since only one can win
//! the create.
//!
//! # What this backend is for, and what that rules out
//!
//! Development and conformance. Recovery on another machine needs storage
//! neither machine owns, which is an object-store backend; a directory on one
//! host cannot be a remote authority. That scope decides which hardening belongs
//! here and which is noise. What it still defends, because both are real
//! regardless of scope, is keys arriving out of an untrusted `head.json`, and
//! losing data it has already reported as written.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::durable::backend::{Backend, PutOutcome, ReplaceOutcome, Tagged};
use crate::durable::digest::{digest_of, Digest};
use crate::durable::errors::{backend_err, err, Category, Error, Result};
use crate::durable::keys::{is_valid_object_key, uuid8, HEAD_KEY};

/// Where the mutable key's version chain lives, relative to the object prefix.
///
/// One directory, because V1 has exactly one mutable key. It is dot-prefixed so
/// it cannot collide with a protocol key, and a reader that only understands
/// plain files still sees a correct `head.json` through the symlink.
const VERSIONS_DIR: &str = ".head-versions";

/// Scratch for partially written files.
const TMP_DIR: &str = ".tmp";

/// One durable object stored as a directory.
#[derive(Debug)]
pub struct LocalBackend {
    root: PathBuf,
}

impl LocalBackend {
    /// Binds a backend to one object's directory, creating it if needed.
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let root = if root.is_absolute() {
            root.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| backend_err(e, format!("durable: cannot resolve {}", root.display())))?
                .join(root)
        };
        fs::create_dir_all(&root)
            .map_err(|e| backend_err(e, format!("durable: cannot create {}", root.display())))?;
        Ok(Self { root })
    }

    /// Resolves a protocol key under the object root, refusing anything that is
    /// not a plain relative key.
    fn path_for(&self, key: &str) -> Result<PathBuf> {
        if !is_valid_object_key(key) {
            return Err(err(
                Category::Backend,
                format!("durable: refusing to resolve invalid key {key:?}"),
            ));
        }
        Ok(self.root.join(key))
    }

    fn versions_dir(&self) -> PathBuf {
        self.root.join(VERSIONS_DIR)
    }

    fn version_path(&self, version: u64) -> PathBuf {
        self.versions_dir().join(format!("{version}.json"))
    }

    /// Which version the head symlink names, if it is a chain at all.
    fn current_version(&self) -> Option<u64> {
        let target = fs::read_link(self.root.join(HEAD_KEY)).ok()?;
        let target = target.to_str()?;
        let number = target
            .strip_prefix(VERSIONS_DIR)?
            .strip_prefix('/')?
            .strip_suffix(".json")?;
        number.parse().ok()
    }

    /// Which version a replace against `etag` would create, or `None` when the
    /// token no longer describes the stored head.
    fn next_version_for(&self, etag: &str) -> Result<Option<u64>> {
        if let Some(current) = self.current_version() {
            if etag != version_etag(current) {
                return Ok(None);
            }
            return Ok(Some(current + 1));
        }

        // No chain yet: this is either a cold object created through
        // put_bytes_if_absent or one another binding wrote. Adopting it into
        // the chain is only sound if the plain file is still exactly what the
        // caller read.
        match fs::read(self.root.join(HEAD_KEY)) {
            Ok(data) if etag == digest_etag(&digest_of(&data)) => Ok(Some(1)),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(backend_err(e, format!("durable: cannot read {HEAD_KEY}"))),
        }
    }

    /// Swaps the head symlink to name one version, atomically.
    fn point_head_at(&self, version: u64) -> std::io::Result<()> {
        let tmp = self.root.join(TMP_DIR);
        fs::create_dir_all(&tmp)?;
        let link = tmp.join(format!("head-{}.link", uuid8()));
        let relative = format!("{VERSIONS_DIR}/{version}.json");
        std::os::unix::fs::symlink(relative, &link)?;
        if let Err(e) = fs::rename(&link, self.root.join(HEAD_KEY)) {
            let _ = fs::remove_file(&link);
            return Err(e);
        }
        sync_dir(&self.root);
        Ok(())
    }

    /// Writes content to a unique scratch file, flushed, and returns its path.
    /// The caller publishes or removes it.
    fn stage(&self, write: impl FnOnce(&mut File) -> std::io::Result<()>) -> Result<PathBuf> {
        let dir = self.root.join(TMP_DIR);
        fs::create_dir_all(&dir).map_err(|e| {
            backend_err(e, "durable: cannot create a staging directory".to_string())
        })?;
        let path = dir.join(format!("{}.part", uuid8()));
        let staged = (|| -> std::io::Result<()> {
            let mut file = File::options().create_new(true).write(true).open(&path)?;
            write(&mut file)?;
            // Flush before the name appears. Publishing bytes that are only in
            // the page cache would let a crash roll back a write this backend
            // has already reported as done.
            file.sync_all()
        })();
        if let Err(e) = staged {
            let _ = fs::remove_file(&path);
            return Err(backend_err(e, "durable: cannot stage a write".to_string()));
        }
        Ok(path)
    }

    /// Links a staged file into place, reporting whether the name was free.
    fn publish(&self, staged: &Path, target: &Path, key: &str) -> Result<PutOutcome> {
        let parent = target.parent().unwrap_or(&self.root);
        fs::create_dir_all(parent)
            .map_err(|e| backend_err(e, format!("durable: cannot create a directory for {key}")))?;
        match fs::hard_link(staged, target) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                return Ok(PutOutcome::AlreadyExists)
            }
            Err(e) => return Err(backend_err(e, format!("durable: cannot publish {key}"))),
        }
        // Flush the directory that gained the name, and the root above it: a
        // name in a directory that is itself a fresh, unflushed entry is no
        // more durable than the entry above it.
        sync_dir(parent);
        sync_dir(&self.root);
        Ok(PutOutcome::Created)
    }
}

fn version_etag(version: u64) -> String {
    format!("v{version}")
}

fn digest_etag(digest: &Digest) -> String {
    format!("sha256:{}", digest.sha256)
}

/// Flushes a directory entry, so a name this backend created survives power
/// loss.
///
/// Best effort by design: some filesystems refuse `fsync` on a directory, and
/// that is not a reason to fail a publish whose data is already flushed. The
/// name may just be less durable than the bytes.
fn sync_dir(dir: &Path) {
    let _ = File::open(dir).and_then(|handle| handle.sync_all());
}

impl Backend for LocalBackend {
    fn describe(&self) -> String {
        format!("file://{}", self.root.display())
    }

    fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.path_for(key)?;
        match fs::read(&path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(backend_err(e, format!("durable: cannot read {key}"))),
        }
    }

    fn get_bytes_with_etag(&self, key: &str) -> Result<Option<Tagged>> {
        let path = self.path_for(key)?;
        if key == HEAD_KEY {
            if let Some(version) = self.current_version() {
                // The symlink names a version that is not there. Reading that
                // as absent would hand a fresh writer a cold object on top of a
                // live one, so it is a failure instead.
                let data = fs::read(self.version_path(version)).map_err(|e| {
                    Error::wrap(
                        Category::Corrupt,
                        e,
                        format!("durable: {key} points at version {version}, which cannot be read"),
                    )
                })?;
                return Ok(Some(Tagged {
                    data,
                    etag: version_etag(version),
                }));
            }
        }
        match fs::read(&path) {
            Ok(data) => {
                let etag = digest_etag(&digest_of(&data));
                Ok(Some(Tagged { data, etag }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(backend_err(e, format!("durable: cannot read {key}"))),
        }
    }

    fn open_reader(&self, key: &str) -> Result<Option<Box<dyn Read + Send>>> {
        let path = self.path_for(key)?;
        match File::open(&path) {
            Ok(file) => Ok(Some(Box::new(file))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(backend_err(e, format!("durable: cannot open {key}"))),
        }
    }

    fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> Result<PutOutcome> {
        let path = self.path_for(key)?;
        let staged = self.stage(|file| file.write_all(data))?;
        let outcome = self.publish(&staged, &path, key);
        let _ = fs::remove_file(&staged);
        outcome
    }

    fn put_file_if_absent(
        &self,
        key: &str,
        local_path: &Path,
        _digest: &Digest,
    ) -> Result<PutOutcome> {
        // The digest is not used here: a local publish is a copy inside one
        // filesystem, so there is nothing to sign and nothing a second read of
        // the same bytes would establish. It is part of the trait because a
        // remote backend does need it.
        let path = self.path_for(key)?;
        let mut source = File::open(local_path).map_err(|e| {
            backend_err(e, format!("durable: cannot read {}", local_path.display()))
        })?;
        let staged = self.stage(|file| std::io::copy(&mut source, file).map(|_| ()))?;
        let outcome = self.publish(&staged, &path, key);
        let _ = fs::remove_file(&staged);
        outcome
    }

    fn replace_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<ReplaceOutcome> {
        if key != HEAD_KEY {
            // Every other key in the protocol is immutable, so a conditional
            // replace against one is a bug rather than a race. Reporting it as
            // a failed compare-and-swap would send the caller into a reconcile
            // that can never settle.
            return Err(err(
                Category::Backend,
                format!("durable: {key} is immutable; only {HEAD_KEY} can be replaced"),
            ));
        }
        self.path_for(key)?;
        fs::create_dir_all(self.versions_dir())
            .map_err(|e| backend_err(e, "durable: cannot create the version chain".to_string()))?;

        let Some(next) = self.next_version_for(etag)? else {
            return Ok(ReplaceOutcome::NotMatched);
        };

        let staged = self.stage(|file| file.write_all(data))?;
        let target = self.version_path(next);
        let outcome = self.publish(&staged, &target, key);
        let _ = fs::remove_file(&staged);

        if outcome? != PutOutcome::Created {
            // Somebody else created this version. That is the compare-and-swap
            // losing, told by the filesystem rather than guessed at.
            return Ok(ReplaceOutcome::NotMatched);
        }

        if self.point_head_at(next).is_err() {
            // The version exists and is durable; only the pointer is behind. A
            // reader still sees the previous head, and another writer holding
            // the previous ETag will fail to create this same version — so the
            // compare-and-swap has been won but not published, which is exactly
            // what ambiguous means.
            return Ok(ReplaceOutcome::Ambiguous);
        }
        Ok(ReplaceOutcome::Done {
            etag: version_etag(next),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::tempdir;

    fn backend(root: &Path) -> LocalBackend {
        LocalBackend::new(root.join("object")).expect("a backend on a fresh directory")
    }

    #[test]
    fn a_conditional_create_lets_exactly_one_writer_win() -> Result<()> {
        let tmp = tempdir();
        let backend = backend(tmp.path());
        assert_eq!(
            backend.put_bytes_if_absent("wal/1-1-aaaaaaaa.jsonl", b"first")?,
            PutOutcome::Created
        );
        assert_eq!(
            backend.put_bytes_if_absent("wal/1-1-aaaaaaaa.jsonl", b"second")?,
            PutOutcome::AlreadyExists
        );
        assert_eq!(
            backend.get_bytes("wal/1-1-aaaaaaaa.jsonl")?.as_deref(),
            Some(&b"first"[..]),
            "an immutable object is never overwritten"
        );
        Ok(())
    }

    #[test]
    fn a_replace_against_a_stale_token_does_not_apply() -> Result<()> {
        let tmp = tempdir();
        let backend = backend(tmp.path());
        backend.put_bytes_if_absent(HEAD_KEY, b"cold")?;

        let first = backend.get_bytes_with_etag(HEAD_KEY)?.expect("a head");
        let second = match backend.replace_if_match(HEAD_KEY, b"one", &first.etag)? {
            ReplaceOutcome::Done { etag } => etag,
            other => panic!("expected the first replace to apply, got {other:?}"),
        };

        // The token the first reader held is now stale.
        assert_eq!(
            backend.replace_if_match(HEAD_KEY, b"two", &first.etag)?,
            ReplaceOutcome::NotMatched
        );
        assert_eq!(
            backend.replace_if_match(HEAD_KEY, b"two", &second)?,
            ReplaceOutcome::Done {
                etag: "v2".to_string()
            }
        );
        assert_eq!(backend.get_bytes(HEAD_KEY)?.as_deref(), Some(&b"two"[..]));
        Ok(())
    }

    #[test]
    fn a_head_another_binding_wrote_is_adopted_into_the_chain() -> Result<()> {
        let tmp = tempdir();
        let backend = backend(tmp.path());
        // A plain file, as a binding without the version chain would leave it.
        fs::write(tmp.path().join("object").join(HEAD_KEY), b"foreign").unwrap();

        let tagged = backend.get_bytes_with_etag(HEAD_KEY)?.expect("a head");
        assert!(tagged.etag.starts_with("sha256:"), "{}", tagged.etag);
        assert_eq!(
            backend.replace_if_match(HEAD_KEY, b"ours", &tagged.etag)?,
            ReplaceOutcome::Done {
                etag: "v1".to_string()
            }
        );
        assert_eq!(backend.get_bytes(HEAD_KEY)?.as_deref(), Some(&b"ours"[..]));
        Ok(())
    }

    #[test]
    fn a_key_out_of_an_untrusted_head_cannot_leave_the_object() {
        let tmp = tempdir();
        let backend = backend(tmp.path());
        for key in ["../escape", "/etc/passwd", "wal/../../escape"] {
            let error = backend.get_bytes(key).expect_err("a traversal is refused");
            assert_eq!(error.category(), Category::Backend, "{key}");
        }
    }

    #[test]
    fn only_the_head_may_be_replaced() {
        let tmp = tempdir();
        let backend = backend(tmp.path());
        let error = backend
            .replace_if_match("wal/1-1-aaaaaaaa.jsonl", b"x", "v1")
            .expect_err("an immutable key has no compare-and-swap");
        assert_eq!(error.category(), Category::Backend);
    }

    #[test]
    fn a_file_is_published_whole_and_streams_back() -> Result<()> {
        let tmp = tempdir();
        let backend = backend(tmp.path());
        let archive = tmp.path().join("checkpoint.tar.gz");
        fs::write(&archive, b"archive bytes").unwrap();
        let digest = digest_of(b"archive bytes");

        assert_eq!(
            backend.put_file_if_absent("checkpoints/1-1-aaaaaaaa.tar.gz", &archive, &digest)?,
            PutOutcome::Created
        );
        let mut reader = backend
            .open_reader("checkpoints/1-1-aaaaaaaa.tar.gz")?
            .expect("the archive is there");
        let mut back = Vec::new();
        reader.read_to_end(&mut back).unwrap();
        assert_eq!(back, b"archive bytes");
        Ok(())
    }

    #[test]
    fn an_absent_key_is_an_answer_rather_than_a_failure() -> Result<()> {
        let tmp = tempdir();
        let backend = backend(tmp.path());
        assert!(backend.get_bytes(HEAD_KEY)?.is_none());
        assert!(backend.get_bytes_with_etag(HEAD_KEY)?.is_none());
        assert!(backend.open_reader(HEAD_KEY)?.is_none());
        Ok(())
    }
}
