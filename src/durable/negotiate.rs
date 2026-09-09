//! Version and feature negotiation (contract §4.3) and engine identity (§4.2).
//!
//! V1 uses named features rather than a monotonic minimum-version number. A
//! monotonic number requires features to be linearly ordered, and with several
//! bindings developed in parallel they are not: a client can implement B
//! without A, and under a version floor it would be locked out of an object
//! that only ever used B.
//!
//! The asymmetry between the two feature lists is the whole point. An unknown
//! *reader* feature means bytes in this object cannot be interpreted, so the
//! object does not open at all. An unknown *writer* feature means only that
//! writing correctly needs something this build cannot do — reading is still
//! sound, so a read-only open is allowed and only the lease is refused.

use std::cmp::Ordering;

use super::errors::{err, Category, Result};
use super::types::{
    Head, ENGINE_NAME, KNOWN_READER_FEATURES, KNOWN_WRITER_FEATURES, PROTOCOL_VERSION,
};
use super::version::{compare_engine_versions, max_engine_version};

/// What the engine in this process can offer, for the compatibility gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunningEngine {
    /// `chdb_version()` of the loaded library.
    pub(crate) version: String,
    /// The highest archive-format generation it can restore.
    pub(crate) backup_format: u64,
}

fn unknown_features(declared: &[String], known: &[&str]) -> Vec<String> {
    declared
        .iter()
        .filter(|feature| !known.contains(&feature.as_str()))
        .cloned()
        .collect()
}

/// Gates a read: refuses a protocol version above this baseline, or any
/// unrecognised reader feature, naming the offenders so the operator knows
/// which build they need.
pub(crate) fn assert_readable(head: &Head) -> Result<()> {
    if head.protocol.version > PROTOCOL_VERSION {
        return Err(err(
            Category::ProtocolUnsupported,
            format!(
                "durable: object uses protocol version {}, this build implements {PROTOCOL_VERSION}",
                head.protocol.version
            ),
        ));
    }
    let missing = unknown_features(&head.protocol.reader_features, KNOWN_READER_FEATURES);
    if !missing.is_empty() {
        return Err(err(
            Category::ProtocolUnsupported,
            format!(
                "durable: object requires reader features this build does not implement: {}",
                missing.join(", ")
            ),
        )
        .with_features(missing));
    }
    Ok(())
}

/// Gates taking the writer lease. Assumes [`assert_readable`] has already
/// passed, and only adds the writer-side feature check.
pub(crate) fn assert_writable(head: &Head) -> Result<()> {
    let missing = unknown_features(&head.protocol.writer_features, KNOWN_WRITER_FEATURES);
    if !missing.is_empty() {
        return Err(err(
            Category::ProtocolUnsupported,
            format!(
                "durable: object requires writer features this build does not implement: {}; \
                 it can still be opened read-only",
                missing.join(", ")
            ),
        )
        .with_features(missing));
    }
    Ok(())
}

/// Applies the frozen engine gate:
///
/// ```text
/// backup_format > reader baseline   -> engine_incompatible
/// running_version < min_reader      -> engine_incompatible
/// otherwise                         -> open
/// ```
///
/// Note what is deliberately absent: a comparison against `engine.version`.
/// That field records which build produced the object, for diagnosis, and is
/// explicitly not a gate. An exact match would refuse every later release,
/// which is the opposite of what the compatibility promise says — a newer
/// chdb-core restores full backups made by an earlier one, so an object written
/// by 26.7.2-rc.2 opens on 26.7.3 and everything after it.
///
/// The two checks guard different failures and neither subsumes the other.
/// `min_reader` catches a reader that is simply too old. `backup_format` is the
/// escape hatch for the day the promise itself is withdrawn: version numbers
/// keep increasing whether or not the format still works, so a broken format
/// needs its own signal, or a reader would compare a larger version, conclude
/// it is fine, and discover otherwise partway through `RESTORE`.
pub(crate) fn assert_engine_compatible(head: &Head, running: &RunningEngine) -> Result<()> {
    if head.engine.name != ENGINE_NAME {
        return Err(err(
            Category::EngineIncompatible,
            format!(
                "durable: object was written by engine {:?}, not {ENGINE_NAME:?}",
                head.engine.name
            ),
        )
        .with_bounds(head.engine.name.clone(), ENGINE_NAME));
    }
    if head.engine.backup_format > running.backup_format {
        return Err(err(
            Category::EngineIncompatible,
            format!(
                "durable: object uses archive format generation {}, and this engine restores up \
                 to {}; a newer chdb-core is required, since the format generation only moves \
                 when older archives can no longer be restored",
                head.engine.backup_format, running.backup_format
            ),
        )
        .with_bounds(
            head.engine.backup_format.to_string(),
            running.backup_format.to_string(),
        ));
    }
    if compare_engine_versions(&running.version, &head.engine.min_reader)? == Ordering::Less {
        return Err(err(
            Category::EngineIncompatible,
            format!(
                "durable: object requires chdb {} or later to read, and this process runs {} \
                 (written by {})",
                head.engine.min_reader, running.version, head.engine.version
            ),
        )
        .with_bounds(head.engine.min_reader.clone(), running.version.clone()));
    }
    Ok(())
}

/// Records this engine's requirements on a head being written, without ever
/// relaxing what is already stored.
///
/// The protocol says a writer must not lower either requirement. Taking the
/// maximum rather than overwriting is what enforces that: an object whose base
/// was produced by a newer engine keeps demanding that engine even if an older
/// one somehow reaches a write, instead of quietly advertising itself as
/// readable by a build that cannot restore it.
pub(crate) fn raise_compatibility_floor(mut head: Head, running: &RunningEngine) -> Result<Head> {
    head.engine.version = running.version.clone();
    if running.backup_format > head.engine.backup_format {
        head.engine.backup_format = running.backup_format;
    }
    head.engine.min_reader = max_engine_version(&head.engine.min_reader, &running.version)?;
    Ok(head)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::durable::types::BACKUP_FORMAT_BASELINE;

    fn running() -> RunningEngine {
        RunningEngine {
            version: "26.7.2".to_string(),
            backup_format: BACKUP_FORMAT_BASELINE,
        }
    }

    fn head() -> Head {
        Head::cold("mem", "26.7.2-rc.2", BACKUP_FORMAT_BASELINE)
    }

    #[test]
    fn a_producer_on_an_earlier_release_is_not_a_refusal() {
        // The whole point of min_reader: 26.7.2 opens what 26.7.2-rc.2 wrote.
        assert!(assert_engine_compatible(&head(), &running()).is_ok());
    }

    #[test]
    fn a_reader_below_min_reader_is_refused() {
        let mut head = head();
        head.engine.min_reader = "26.8.0".to_string();
        let error = assert_engine_compatible(&head, &running()).expect_err("too old to read");
        assert_eq!(error.category(), Category::EngineIncompatible);
        assert_eq!(error.expected(), Some("26.8.0"));
        assert_eq!(error.actual(), Some("26.7.2"));
    }

    #[test]
    fn an_archive_generation_beyond_this_engine_is_refused() {
        let mut head = head();
        head.engine.backup_format = BACKUP_FORMAT_BASELINE + 1;
        let error = assert_engine_compatible(&head, &running()).expect_err("format too new");
        assert_eq!(error.category(), Category::EngineIncompatible);
    }

    #[test]
    fn another_engines_object_is_not_ours_to_open() {
        let mut head = head();
        head.engine.name = "duckdb".to_string();
        let error = assert_engine_compatible(&head, &running()).expect_err("not chdb");
        assert_eq!(error.category(), Category::EngineIncompatible);
    }

    #[test]
    fn an_unknown_reader_feature_closes_the_object_and_a_writer_one_only_closes_the_lease() {
        let mut reader_side = head();
        reader_side.protocol.reader_features = vec!["parquet-wal".to_string()];
        let error = assert_readable(&reader_side).expect_err("cannot interpret the bytes");
        assert_eq!(error.category(), Category::ProtocolUnsupported);
        assert_eq!(error.features(), ["parquet-wal"]);

        let mut writer_side = head();
        writer_side.protocol.writer_features = vec!["preamble".to_string()];
        assert!(assert_readable(&writer_side).is_ok(), "reading stays sound");
        assert_eq!(
            assert_writable(&writer_side).unwrap_err().category(),
            Category::ProtocolUnsupported
        );
    }

    #[test]
    fn a_future_protocol_version_closes_the_object() {
        let mut head = head();
        head.protocol.version = PROTOCOL_VERSION + 1;
        assert_eq!(
            assert_readable(&head).unwrap_err().category(),
            Category::ProtocolUnsupported
        );
    }

    #[test]
    fn the_floor_only_ever_rises() {
        let mut head = head();
        head.engine.min_reader = "26.9.0".to_string();
        let raised = raise_compatibility_floor(head, &running()).unwrap();
        // The running engine is older than the stored floor, so the floor stays
        // where the newer producer left it.
        assert_eq!(raised.engine.min_reader, "26.9.0");
        assert_eq!(
            raised.engine.version, "26.7.2",
            "the producer is still recorded"
        );
    }
}
