//! The V1 error model.
//!
//! The contract freezes a set of error *categories* (§6) and requires a caller
//! to be able to tell them apart programmatically. It deliberately does not
//! freeze type names, so this is one error type carrying a [`Category`] rather
//! than a tree of types: the category is the wire name a cross-binding
//! conformance run compares, and the remaining fields carry the specifics a
//! caller would otherwise have to parse out of a message.
//!
//! ```no_run
//! use chdb_rust::durable::{Category, Namespace};
//!
//! # let namespace = Namespace::new("file:///var/lib/chdb-durable")?;
//! match namespace.open("tenant-1", Default::default()) {
//!     Ok((object, _existed)) => drop(object),
//!     Err(e) if e.category() == Category::LeaseHeld => { /* another writer has it */ }
//!     Err(e) => return Err(e),
//! }
//! # Ok::<(), chdb_rust::durable::Error>(())
//! ```
//!
//! Two rules the contract states outright, and this module encodes:
//!
//! 1. A provider precondition failure is a compare-and-set race first. It is
//!    resolved against lease and manifest state into [`Category::LeaseHeld`],
//!    [`Category::LeaseFenced`] or a retry — never reported as a plain
//!    [`Category::Backend`], which is for failures that really are the
//!    provider's: network, auth, quota.
//! 2. Messages carry no secret-bearing SQL, no credentials and no unredacted
//!    connection parameters. [`Category::SecretRefused`] in particular says
//!    what was refused without echoing the statement that caused it.

use std::fmt;

/// A frozen V1 error category.
///
/// Cross-binding conformance asserts on the strings [`Category::as_str`]
/// returns, so they are wire names rather than prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Category {
    /// A read-only open of an object that does not exist, or an open that
    /// required an existing one.
    NotFound,
    /// Another writer holds an unexpired lease.
    LeaseHeld,
    /// This instance no longer owns the generation it was writing under —
    /// through a takeover, or through fencing itself after it could not
    /// confirm its lease in time.
    LeaseFenced,
    /// The object's archive format or minimum reader is beyond this engine, or
    /// it was written by an engine that is not chDB.
    EngineIncompatible,
    /// A protocol version above this baseline, or a feature name this build
    /// does not know.
    ProtocolUnsupported,
    /// A head that fails schema validation, or a referenced immutable object
    /// that is missing or whose length or digest does not match. Never
    /// downgraded into opening an older state.
    Corrupt,
    /// Core analysis refused a statement at a public entry point: wrong
    /// statement count, wrong class, or a write outside the object's database.
    ClassificationRefused,
    /// A mutation embeds a credential. V1 has nowhere to put it other than the
    /// WAL, which outlives the statement, so it is refused outright.
    SecretRefused,
    /// A core query, backup or restore failed.
    Engine,
    /// A provider network, auth or non-conditional failure.
    Backend,
    /// A deadline passed while the operation was provably still uncommitted.
    Timeout,
    /// Reconcile could not prove whether the remote committed. The one thing it
    /// must never do is report success.
    CommitAmbiguous,
    /// SQL, a WAL segment, a head or a provider object over a declared V1 limit.
    LimitExceeded,
    /// An operation on an object whose close has completed.
    Closed,
}

impl Category {
    /// The frozen wire name, as the contract's table spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::LeaseHeld => "lease_held",
            Self::LeaseFenced => "lease_fenced",
            Self::EngineIncompatible => "engine_incompatible",
            Self::ProtocolUnsupported => "protocol_unsupported",
            Self::Corrupt => "corrupt",
            Self::ClassificationRefused => "classification_refused",
            Self::SecretRefused => "secret_refused",
            Self::Engine => "engine",
            Self::Backend => "backend",
            Self::Timeout => "timeout",
            Self::CommitAmbiguous => "commit_ambiguous",
            Self::LimitExceeded => "limit_exceeded",
            Self::Closed => "closed",
        }
    }
}

impl fmt::Display for Category {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Every failure the durable control plane reports.
///
/// [`Error::category`] is what callers branch on. The other accessors are
/// populated only where they mean something, so a caller can act on the
/// specifics without parsing the message.
#[derive(Debug)]
pub struct Error {
    category: Category,
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
    /// Boxed, and absent on most errors: the specifics are read by a handful of
    /// callers, while every `Result` in this module pays for the size of the
    /// error variant whether or not it ever holds one.
    details: Option<Box<Details>>,
}

#[derive(Debug, Default)]
struct Details {
    owner: Option<String>,
    key: Option<String>,
    features: Vec<String>,
    expected: Option<String>,
    actual: Option<String>,
    limit: Option<u64>,
    observed: Option<u64>,
}

impl Error {
    /// A new error of one category.
    ///
    /// Public because [`Backend`](super::Backend) and [`Engine`](super::Engine)
    /// are implementable outside this crate, and an implementation has to be
    /// able to report a failure in the vocabulary the contract freezes. Pick the
    /// category the contract's §6 table gives for what went wrong — a provider
    /// failure is [`Category::Backend`], a refused statement is
    /// [`Category::Engine`] — and leave the lease and commit categories to the
    /// state machine, which is the only thing that can know them.
    pub fn new(category: Category, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
            source: None,
            details: None,
        }
    }

    fn details(&mut self) -> &mut Details {
        self.details.get_or_insert_with(Box::default)
    }

    /// [`Error::new`] with a cause attached, reachable through
    /// [`std::error::Error::source`].
    pub fn wrap(
        category: Category,
        cause: impl std::error::Error + Send + Sync + 'static,
        message: impl Into<String>,
    ) -> Self {
        Self::new(category, message).with_source(cause)
    }

    /// Attaches a cause to an error that does not have one.
    pub fn with_source(mut self, cause: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(cause));
        self
    }

    pub(crate) fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.details().owner = Some(owner.into());
        self
    }

    pub(crate) fn with_key(mut self, key: impl Into<String>) -> Self {
        self.details().key = Some(key.into());
        self
    }

    pub(crate) fn with_features(mut self, features: Vec<String>) -> Self {
        self.details().features = features;
        self
    }

    pub(crate) fn with_bounds(
        mut self,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        let details = self.details();
        details.expected = Some(expected.into());
        details.actual = Some(actual.into());
        self
    }

    pub(crate) fn with_limit(mut self, limit: u64, observed: u64) -> Self {
        let details = self.details();
        details.limit = Some(limit);
        details.observed = Some(observed);
        self
    }

    /// The frozen V1 category.
    pub fn category(&self) -> Category {
        self.category
    }

    /// The visible name recorded on a lease that blocked this operation.
    /// Observability only — never an authority check.
    pub fn owner(&self) -> Option<&str> {
        self.details.as_ref()?.owner.as_deref()
    }

    /// The object key whose commit state could not be settled, for triage of a
    /// [`Category::CommitAmbiguous`].
    pub fn key(&self) -> Option<&str> {
        self.details.as_ref()?.key.as_deref()
    }

    /// The unrecognised protocol feature names that blocked an open, so an
    /// operator learns which build they need.
    pub fn features(&self) -> &[String] {
        self.details.as_ref().map_or(&[], |d| &d.features)
    }

    /// What the object demands, for a compatibility refusal.
    pub fn expected(&self) -> Option<&str> {
        self.details.as_ref()?.expected.as_deref()
    }

    /// What this process offers, for a compatibility refusal.
    pub fn actual(&self) -> Option<&str> {
        self.details.as_ref()?.actual.as_deref()
    }

    /// The two sides of a limit refusal, in bytes: the limit, and what was
    /// measured against it.
    pub fn limit(&self) -> Option<(u64, u64)> {
        let details = self.details.as_ref()?;
        details.limit.zip(details.observed)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(cause) => write!(f, "{}: {cause}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref() as _)
    }
}

/// The durable control plane's result type.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn err(category: Category, message: impl Into<String>) -> Error {
    Error::new(category, message)
}

/// An engine failure, with the crate error underneath.
///
/// The message never carries SQL: everything reaching this has been through
/// analysis, and some of it embeds a credential.
pub(crate) fn engine_err(cause: crate::error::Error, message: impl Into<String>) -> Error {
    Error::wrap(Category::Engine, cause, message)
}

pub(crate) fn backend_err(cause: std::io::Error, message: impl Into<String>) -> Error {
    Error::wrap(Category::Backend, cause, message)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn every_frozen_category_keeps_its_wire_name() {
        // The contract's §6 table, verbatim. A rename here is a protocol
        // change, not a refactor, so it has to fail a test.
        let names: Vec<&str> = [
            Category::NotFound,
            Category::LeaseHeld,
            Category::LeaseFenced,
            Category::EngineIncompatible,
            Category::ProtocolUnsupported,
            Category::Corrupt,
            Category::ClassificationRefused,
            Category::SecretRefused,
            Category::Engine,
            Category::Backend,
            Category::Timeout,
            Category::CommitAmbiguous,
            Category::LimitExceeded,
            Category::Closed,
        ]
        .iter()
        .map(|c| c.as_str())
        .collect();

        assert_eq!(
            names,
            vec![
                "not_found",
                "lease_held",
                "lease_fenced",
                "engine_incompatible",
                "protocol_unsupported",
                "corrupt",
                "classification_refused",
                "secret_refused",
                "engine",
                "backend",
                "timeout",
                "commit_ambiguous",
                "limit_exceeded",
                "closed",
            ]
        );
    }

    #[test]
    fn a_wrapped_cause_shows_in_the_message_and_the_source_chain() {
        let cause = std::io::Error::other("disk on fire");
        let error = Error::wrap(Category::Backend, cause, "durable: cannot read head.json");
        assert_eq!(error.category(), Category::Backend);
        assert_eq!(
            error.to_string(),
            "durable: cannot read head.json: disk on fire"
        );
        assert!(std::error::Error::source(&error).is_some());
    }
}
