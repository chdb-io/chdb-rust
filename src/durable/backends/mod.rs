//! Backends this crate ships.
//!
//! One, for now: a directory. The conditional-write contract in
//! [`Backend`](super::Backend) is what a provider has to satisfy, and the local
//! backend satisfies it with `link(2)` and a version chain rather than
//! approximating it — see [`local`] for why that matters even for a backend
//! whose scope is development and conformance.
//!
//! The S3-compatible backend is behind its own feature, `durable-s3`, so a
//! caller that only ever uses a directory does not carry an HTTP stack and a
//! TLS implementation. Any other provider is plugged in by implementing
//! [`Backend`](super::Backend) and handing it to
//! [`Namespace::with_backend`](super::Namespace::with_backend).

pub mod local;
#[cfg(feature = "durable-s3")]
pub mod s3;
#[cfg(feature = "durable-s3")]
mod sigv4;

pub use local::LocalBackend;
#[cfg(feature = "durable-s3")]
pub use s3::{S3Backend, S3Options, MAX_SINGLE_PUT_BYTES};
#[cfg(feature = "durable-s3")]
pub use sigv4::Credentials;
