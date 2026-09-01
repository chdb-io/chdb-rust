//! The version accessors, and the invariant that ties the compile-time one to
//! the run-time one.

use chdb_rust::version::{
    clickhouse_version, engine_version, ENGINE_SOURCE, EXPECTED_ENGINE_VERSION,
};

#[test]
fn engine_version_reports_a_chdb_core_release() {
    let version = engine_version().unwrap_or_else(|e| panic!("engine from {ENGINE_SOURCE}: {e}"));

    assert!(!version.is_empty(), "engine from {ENGINE_SOURCE}");
    // The `v` belongs to the release tag and nowhere else: everything compares
    // these by plain string equality, which needs one spelling.
    assert!(
        !version.starts_with('v'),
        "{version} still carries the tag's v"
    );
    assert!(
        version.starts_with(|c: char| c.is_ascii_digit()),
        "{version} does not start with a version field"
    );
}

#[test]
fn expected_and_reported_engine_versions_agree() {
    let Some(expected) = EXPECTED_ENGINE_VERSION else {
        // The build linked a library it did not fetch, so there is no second
        // number to compare against. ENGINE_SOURCE says which case this is.
        return;
    };

    let reported = engine_version().unwrap_or_else(|e| panic!("engine from {ENGINE_SOURCE}: {e}"));
    assert_eq!(
        expected, reported,
        "the build fetched {expected} but the linked library reports {reported} ({ENGINE_SOURCE})"
    );
}

/// The failure this guards against is a constant that describes the pin while
/// some other artifact is what actually got linked — a claim about the binary
/// that is not true of the binary.
#[test]
fn expected_engine_version_only_speaks_for_a_fetched_library() {
    if ENGINE_SOURCE.starts_with("download:") {
        assert!(
            EXPECTED_ENGINE_VERSION.is_some(),
            "{ENGINE_SOURCE} fetched an engine, so its release is known"
        );
    } else {
        assert!(
            EXPECTED_ENGINE_VERSION.is_none(),
            "{ENGINE_SOURCE} is not a download, so no chdb-core release can be claimed"
        );
    }
}

#[test]
fn clickhouse_version_reports_the_clickhouse_scheme() {
    let version = clickhouse_version().expect("SELECT version()");
    let fields: Vec<&str> = version.split('.').collect();

    assert!(
        fields.len() >= 3,
        "{version} does not look like a ClickHouse version"
    );
    assert!(
        fields
            .iter()
            .take(3)
            .all(|field| !field.is_empty() && field.chars().all(|c| c.is_ascii_digit())),
        "{version} does not look like a ClickHouse version"
    );
}

#[test]
fn engine_source_is_recorded() {
    assert!(!ENGINE_SOURCE.is_empty());
}
