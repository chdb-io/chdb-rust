//! Backends this crate ships.
//!
//! One, for now: a directory. The conditional-write contract in
//! [`Backend`](super::Backend) is what a provider has to satisfy, and the local
//! backend satisfies it with `link(2)` and a version chain rather than
//! approximating it — see [`local`] for why that matters even for a backend
//! whose scope is development and conformance.
//!
//! An S3-compatible backend is a separate piece of work, and deliberately not
//! in this crate's dependency graph until it exists: a caller that only ever
//! uses a directory should not carry a cloud SDK. Until then, a provider is
//! plugged in by implementing [`Backend`](super::Backend) and handing it to
//! [`Namespace::with_backend`](super::Namespace::with_backend).

pub mod local;

pub use local::LocalBackend;
