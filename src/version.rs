//! Which engine this build carries.
//!
//! Three accessors, each reporting exactly one versioning scheme. Nothing here
//! falls back to another scheme when its own source is unavailable: a chdb-core
//! release number and a ClickHouse version number cannot be compared, so
//! answering one question with the other number is worse than answering with an
//! error.
//!
//! | | scheme | resolved | example |
//! | --- | --- | --- | --- |
//! | [`EXPECTED_ENGINE_VERSION`] | chdb-core | at compile time | `26.7.0` |
//! | [`engine_version`] | chdb-core | by the linked library | `26.7.0` |
//! | [`clickhouse_version`] | ClickHouse | by a query | `26.5.1.1` |
//!
//! A chdb-core release number is not a ClickHouse one: `X.Y` is the ClickHouse
//! minor line the release sits on and `Z` is chdb-core's own counter, so the two
//! agree on the first two fields at most.

use crate::error::{Error, Result};

/// The chdb-core release this build fetched, without the leading `v`.
///
/// `None` when the build linked a library it did not fetch — a `CHDB_LIB_DIR`
/// build, a copy already installed on the machine, a docs.rs build. The pinned
/// version says nothing about an artifact that came from somewhere else, so this
/// reports nothing rather than reporting the pin. [`ENGINE_SOURCE`] says where
/// the library came from, and [`engine_version`] asks the library itself.
///
/// ```no_run
/// use chdb_rust::version::{engine_version, EXPECTED_ENGINE_VERSION};
///
/// // A build that fetched its own engine can check the two agree.
/// if let Some(expected) = EXPECTED_ENGINE_VERSION {
///     assert_eq!(expected, engine_version()?);
/// }
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
pub const EXPECTED_ENGINE_VERSION: Option<&str> = {
    let declared = env!("CHDB_EXPECTED_ENGINE_VERSION");
    if declared.is_empty() {
        None
    } else {
        Some(declared)
    }
};

/// Where the linked library came from, as the build script resolved it.
///
/// One of `download: chdb-core <tag>`, `CHDB_LIB_DIR: <path>`,
/// `local: <path>`, or `none: docs.rs build`. Diagnostic only — the shape is not
/// part of the API.
pub const ENGINE_SOURCE: &str = env!("CHDB_ENGINE_SOURCE");

/// The chdb-core release the linked library reports, without the leading `v`.
///
/// This is the only accessor that describes the artifact actually loaded, which
/// is what makes it the way to tell which engine a binary carries when the build
/// did not fetch it.
///
/// # Errors
///
/// Returns [`Error::EngineVersionUnavailable`] when the library predates
/// `chdb_version()`, which arrived in chdb-core v26.7.0.
pub fn engine_version() -> Result<&'static str> {
    #[cfg(has_chdb_version)]
    {
        // SAFETY: chdb_version() takes no arguments and returns a pointer to a
        // string constant in the library, valid for as long as it is loaded.
        let raw = unsafe { crate::bindings::chdb_version() };
        if raw.is_null() {
            return Err(Error::InvalidData(
                "chdb_version() returned NULL".to_string(),
            ));
        }
        unsafe { std::ffi::CStr::from_ptr(raw) }
            .to_str()
            .map_err(|e| Error::InvalidData(format!("chdb_version() is not UTF-8: {e}")))
    }
    #[cfg(not(has_chdb_version))]
    {
        Err(Error::EngineVersionUnavailable)
    }
}

/// The ClickHouse version the linked engine reports, as `SELECT version()` gives
/// it.
///
/// Opens a temporary in-memory connection, the same as [`crate::execute`]. Use
/// `SELECT version()` on a connection you already hold if you have one.
pub fn clickhouse_version() -> Result<String> {
    let result = crate::execute("SELECT version()", None)?;
    Ok(result.data_utf8()?.trim().to_string())
}
