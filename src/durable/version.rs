//! chDB release precedence.
//!
//! The V1 engine gate compares versions, and it compares them by release
//! precedence rather than as strings. The difference is not academic: as
//! strings, `26.10.0 < 26.7.0` and `26.7.2-rc.2 > 26.7.2`, both of which are
//! backwards, and both of which would let a reader open an object it cannot
//! actually restore.
//!
//! The ordering is semver's:
//!
//! ```text
//! 26.7.2-rc.1  <  26.7.2-rc.2  <  26.7.2  <  26.7.3  <  26.8.1  <  27.0.0
//! ```
//!
//! A pre-release sorts *below* the release it leads to, which is what makes an
//! object written by 26.7.2-rc.2 — the release that first exported the durable
//! ABI, and what `chdb_version()` reports there — readable by 26.7.2 and
//! everything after it.
//!
//! The parser is semver-shaped rather than a match on the two shapes chDB
//! happens to ship today. A future `26.7.2-beta.1` sorts correctly here, where
//! a tighter pattern would refuse an object it could have opened safely — the
//! wrong place to fail closed. Failing closed belongs where nothing can be
//! concluded: a string that does not parse at all is refused rather than
//! guessed at, because an unrecognised version is not evidence of
//! compatibility.

use std::cmp::Ordering;

use super::errors::{err, Category, Result};

/// A version string decomposed for comparison.
#[derive(Debug, PartialEq, Eq)]
struct Parsed {
    /// The numeric components, e.g. `[26, 7, 2]`. At least one.
    release: Vec<u64>,
    /// The dot-separated pre-release identifiers, e.g. `["rc", "2"]`. `None`
    /// for a final release, which sorts above every pre-release of the same
    /// numbers.
    prerelease: Option<Vec<PreIdent>>,
}

#[derive(Debug, PartialEq, Eq)]
enum PreIdent {
    Numeric(u64),
    Text(String),
}

/// Decomposes a chDB version string, or reports that it is not one.
fn parse(value: &str) -> Option<Parsed> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    // Build metadata takes no part in precedence, as semver requires, but it
    // still has to be well formed to be ignored.
    let (value, build) = match value.split_once('+') {
        Some((head, build)) => (head, Some(build)),
        None => (value, None),
    };
    if let Some(build) = build {
        if build.is_empty() || !build.chars().all(is_identifier_char) {
            return None;
        }
    }

    let (release, prerelease) = match value.split_once('-') {
        Some((release, pre)) => (release, Some(pre)),
        None => (value, None),
    };

    let mut numbers = Vec::new();
    for part in release.split('.') {
        if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        numbers.push(part.parse::<u64>().ok()?);
    }

    let identifiers = match prerelease {
        None => None,
        Some(pre) => {
            if pre.is_empty() {
                return None;
            }
            let mut out = Vec::new();
            for identifier in pre.split('.') {
                if identifier.is_empty() || !identifier.chars().all(is_identifier_char) {
                    return None;
                }
                out.push(match identifier.parse::<u64>() {
                    Ok(number) => PreIdent::Numeric(number),
                    Err(_) => PreIdent::Text(identifier.to_string()),
                });
            }
            Some(out)
        }
    };

    Some(Parsed {
        release: numbers,
        prerelease: identifiers,
    })
}

fn is_identifier_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '.'
}

fn compare_prerelease(left: &[PreIdent], right: &[PreIdent]) -> Ordering {
    for index in 0..left.len().max(right.len()) {
        // A shorter identifier list sorts lower when all shared parts are
        // equal: rc.1 comes before rc.1.1.
        let (a, b) = match (left.get(index), right.get(index)) {
            (None, _) => return Ordering::Less,
            (_, None) => return Ordering::Greater,
            (Some(a), Some(b)) => (a, b),
        };
        let ordering = match (a, b) {
            (PreIdent::Numeric(a), PreIdent::Numeric(b)) => a.cmp(b),
            // Numeric identifiers always sort below alphanumeric ones.
            (PreIdent::Numeric(_), PreIdent::Text(_)) => Ordering::Less,
            (PreIdent::Text(_), PreIdent::Numeric(_)) => Ordering::Greater,
            (PreIdent::Text(a), PreIdent::Text(b)) => a.cmp(b),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_precedence(left: &Parsed, right: &Parsed) -> Ordering {
    for index in 0..left.release.len().max(right.release.len()) {
        // A missing component is zero, so 26.7 and 26.7.0 are one release.
        let a = left.release.get(index).copied().unwrap_or(0);
        let b = right.release.get(index).copied().unwrap_or(0);
        if a != b {
            return a.cmp(&b);
        }
    }
    match (&left.prerelease, &right.prerelease) {
        (None, None) => Ordering::Equal,
        // No pre-release outranks any pre-release of the same numbers.
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) => compare_prerelease(a, b),
    }
}

/// Orders two chDB version strings by release precedence, refusing either one
/// it cannot parse rather than guessing.
///
/// ```
/// use std::cmp::Ordering;
/// use chdb_rust::durable::compare_engine_versions;
///
/// assert_eq!(compare_engine_versions("26.7.2-rc.2", "26.7.2")?, Ordering::Less);
/// assert_eq!(compare_engine_versions("26.10.0", "26.7.0")?, Ordering::Greater);
/// assert_eq!(compare_engine_versions("26.7", "26.7.0")?, Ordering::Equal);
/// assert!(compare_engine_versions("not-a-version...", "26.7.0").is_err());
/// # Ok::<(), chdb_rust::durable::Error>(())
/// ```
///
/// # Errors
///
/// Returns [`Category::EngineIncompatible`] when either string cannot be
/// ordered. An unreadable version is not evidence of compatibility, and
/// treating it as one is how a reader restores an archive from a release
/// nothing has tested it against.
pub fn compare_engine_versions(left: &str, right: &str) -> Result<Ordering> {
    match (parse(left), parse(right)) {
        (Some(a), Some(b)) => Ok(compare_precedence(&a, &b)),
        (a, _) => {
            let unreadable = if a.is_some() { right } else { left };
            Err(err(
                Category::EngineIncompatible,
                format!(
                    "durable: cannot order chdb version {unreadable:?} by release precedence, so \
                     compatibility cannot be established; refusing rather than guessing"
                ),
            ))
        }
    }
}

/// The later of two version strings by precedence.
pub(crate) fn max_engine_version(left: &str, right: &str) -> Result<String> {
    Ok(match compare_engine_versions(left, right)? {
        Ordering::Less => right.to_string(),
        _ => left.to_string(),
    })
}

#[cfg(test)]
mod test {
    use super::*;

    fn ordered(pairs: &[(&str, &str)]) {
        for (earlier, later) in pairs {
            assert_eq!(
                compare_engine_versions(earlier, later).unwrap(),
                Ordering::Less,
                "{earlier} should precede {later}"
            );
            assert_eq!(
                compare_engine_versions(later, earlier).unwrap(),
                Ordering::Greater,
                "{later} should follow {earlier}"
            );
        }
    }

    #[test]
    fn a_prerelease_precedes_the_release_it_leads_to() {
        ordered(&[
            ("26.7.2-rc.1", "26.7.2-rc.2"),
            ("26.7.2-rc.2", "26.7.2"),
            ("26.7.2-alpha.1", "26.7.2-rc.1"),
            ("26.7.2-rc.1", "26.7.2-rc.1.1"),
        ]);
    }

    #[test]
    fn release_numbers_order_numerically_not_lexically() {
        ordered(&[
            ("26.7.0", "26.10.0"),
            ("26.7.2", "26.7.3"),
            ("26.7.2", "26.8.1"),
            ("26.8.1", "27.0.0"),
            ("26.7.2", "26.7.2.59"),
        ]);
    }

    #[test]
    fn a_missing_component_is_zero() {
        assert_eq!(
            compare_engine_versions("26.7", "26.7.0").unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            compare_engine_versions("26.7.2", "26.7.2+build.7").unwrap(),
            Ordering::Equal
        );
    }

    #[test]
    fn an_unreadable_version_is_refused_rather_than_ordered() {
        for bad in [
            "", "   ", "v26.7.2", "26.7.x", "26..7", "26.7.2-", "26.7.2+",
        ] {
            let error = compare_engine_versions(bad, "26.7.2")
                .expect_err("an unorderable version cannot establish compatibility");
            assert_eq!(error.category(), Category::EngineIncompatible);
        }
    }

    #[test]
    fn the_maximum_never_lowers_a_floor() {
        assert_eq!(
            max_engine_version("26.7.2", "26.7.2-rc.2").unwrap(),
            "26.7.2"
        );
        assert_eq!(
            max_engine_version("26.7.2-rc.2", "26.7.2").unwrap(),
            "26.7.2"
        );
        assert_eq!(max_engine_version("26.7.2", "26.7.2").unwrap(), "26.7.2");
    }
}
