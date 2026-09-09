//! Namespaces: the entry point to the durable control plane.
//!
//! A namespace is a location plus an engine factory. The location says where
//! objects live and which backend implements the conditional operations; the
//! factory says how to bring up an engine for whichever object is being opened.
//!
//! One constraint is not the namespace's to hide: chdb-core binds one data path
//! per process, so one process holds one open durable object at a time. Opening
//! a second returns an error naming both paths. Fan-out across objects is
//! therefore sequential, or spread across worker processes; a registry here
//! that pretended otherwise would only move the failure somewhere less obvious
//! (§3.6).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::backend::Backend;
use super::backends::LocalBackend;
use super::engine::EngineFactory;
use super::errors::{err, Category, Result};
use super::keys::validate_object_id;
use super::object::{open_object, DurableObject, OpenOptions, Tuning};
use super::ChdbEngine;

/// Builds a backend for one object, given its id.
///
/// A namespace owns one and hands each object its own scoped backend, so no
/// code below the namespace can address a key outside its object. Supplying one
/// directly is also how a provider this crate does not ship — an S3-compatible
/// store, a fault-injecting wrapper — is plugged in.
pub type BackendFactory = Box<dyn Fn(&str) -> Result<Arc<dyn Backend>> + Send + Sync>;

/// Addresses durable objects on one backend.
pub struct Namespace {
    location: String,
    backend_factory: BackendFactory,
    engine_factory: EngineFactory,
    owner: Option<String>,
    scratch_root: Option<PathBuf>,
    tuning: Tuning,
}

impl std::fmt::Debug for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Namespace")
            .field("location", &self.location)
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

/// The query parameters of a namespace URL, percent-decoded.
#[cfg(feature = "durable-s3")]
///
/// Decoded because an `endpoint` value carries `://`, and a caller that
/// escaped it — as any URL builder would — should get the same backend as one
/// who did not.
fn query_of(url: &str) -> Vec<(String, String)> {
    let Some((_, query)) = url.split_once('?') else {
        return Vec::new();
    };
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(name), percent_decode(value))
        })
        .collect()
}

#[cfg(feature = "durable-s3")]
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        index += 3;
                    }
                    // Not an escape after all: a literal `%` is not an error,
                    // and mangling it would be worse than passing it through.
                    Err(_) => {
                        out.push(b'%');
                        index += 1;
                    }
                }
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The bucket and base prefix an `s3://` URL names.
#[cfg(feature = "durable-s3")]
fn s3_location(url: &str) -> Result<(String, String)> {
    let location = url
        .trim_start_matches("s3://")
        .split('?')
        .next()
        .unwrap_or_default();
    let (bucket, prefix) = match location.split_once('/') {
        Some((bucket, prefix)) => (bucket, prefix.trim_matches('/')),
        None => (location, ""),
    };
    if bucket.is_empty() {
        return Err(err(
            Category::Backend,
            format!("durable: an s3 namespace URL needs a bucket, got {url:?}"),
        ));
    }
    Ok((bucket.to_string(), prefix.to_string()))
}

/// The directory a `file://` or `local:` URL names, or a plain absolute path.
///
/// A typo in a configuration file fails here rather than at the first open.
fn local_root(url: &str) -> Result<PathBuf> {
    let url = url.split('?').next().unwrap_or(url);
    let path = url
        .strip_prefix("file://")
        .or_else(|| url.strip_prefix("file:"))
        .or_else(|| url.strip_prefix("local://"))
        .or_else(|| url.strip_prefix("local:"))
        .unwrap_or(url);
    if let Some(scheme_end) = path.find("://") {
        return Err(err(
            Category::Backend,
            format!(
                "durable: this build has no backend for the {:?} scheme; pass a backend factory \
                 with Namespace::with_backend for anything but a local directory",
                &path[..scheme_end]
            ),
        ));
    }
    let path = Path::new(path);
    if !path.is_absolute() {
        return Err(err(
            Category::Backend,
            format!("durable: a local namespace needs an absolute path, got {url:?}"),
        ));
    }
    Ok(path.to_path_buf())
}

impl Namespace {
    /// Binds a namespace to a local directory.
    ///
    /// ```no_run
    /// use chdb_rust::durable::Namespace;
    ///
    /// let namespace = Namespace::new("file:///var/lib/chdb-durable")?;
    /// # Ok::<(), chdb_rust::durable::Error>(())
    /// ```
    ///
    /// `file:///abs/path`, `local:/abs/path` and a bare `/abs/path` all name a
    /// directory holding one subdirectory per object. That backend is for
    /// development, tests and single-host use: recovery on another machine
    /// needs storage neither machine owns, which is an object store. Point a
    /// namespace at one with [`Self::with_backend`].
    ///
    /// # Errors
    ///
    /// Returns [`Category::Backend`] for a relative path or a scheme this build
    /// has no backend for.
    pub fn new(url: &str) -> Result<Self> {
        if url.starts_with("s3://") {
            return Self::s3(url);
        }
        let root = local_root(url)?;
        Ok(Self::with_backend(Box::new(move |id| {
            Ok(Arc::new(LocalBackend::new(root.join(id))?))
        }))
        .named(url))
    }

    /// Binds a namespace to an S3-compatible bucket.
    ///
    /// ```text
    /// AWS    s3://my-bucket/durable?region=eu-central-1
    /// R2     s3://my-bucket/durable?region=auto&endpoint=https://<id>.r2.cloudflarestorage.com
    /// MinIO  s3://my-bucket/durable?endpoint=http://127.0.0.1:9000
    /// ```
    ///
    /// Credentials are deliberately not among the parameters. They come from
    /// the environment or the shared credentials file, because a namespace URL
    /// is the sort of thing that gets logged, put in a config file and pasted
    /// into an issue. See [`Credentials::resolve`](super::Credentials::resolve)
    /// for the order, and for what to do with an SSO profile.
    #[cfg(feature = "durable-s3")]
    fn s3(url: &str) -> Result<Self> {
        use super::backends::{S3Backend, S3Options};

        let (bucket, base_prefix) = s3_location(url)?;
        let query = query_of(url);
        let value = |name: &str| {
            query
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
                .filter(|value| !value.is_empty())
        };
        // `pathStyle` too, because that is what the Go binding's URLs use and
        // the same configuration string should work in both.
        let path_style = value("path_style")
            .or_else(|| value("pathStyle"))
            .map(|raw| raw == "true" || raw == "1");

        let region = value("region");
        let endpoint = value("endpoint");
        Ok(Self::with_backend(Box::new(move |id| {
            let prefix = if base_prefix.is_empty() {
                id.to_string()
            } else {
                format!("{base_prefix}/{id}")
            };
            Ok(Arc::new(S3Backend::new(S3Options {
                bucket: bucket.clone(),
                prefix,
                region: region.clone(),
                endpoint: endpoint.clone(),
                path_style,
                ..S3Options::default()
            })?))
        }))
        .named(url))
    }

    /// The `s3` scheme needs the `durable-s3` feature, which is what carries
    /// the HTTP and TLS stack it signs requests with.
    #[cfg(not(feature = "durable-s3"))]
    fn s3(url: &str) -> Result<Self> {
        Err(err(
            Category::Backend,
            format!(
                "durable: {url:?} needs the `durable-s3` feature, which is off in this build;                  enable it, or pass your own backend to Namespace::with_backend"
            ),
        ))
    }

    /// Binds a namespace to a backend of your own.
    ///
    /// The factory is called once per open with the object id, and must return a
    /// backend scoped to that object's prefix — nothing below this point checks
    /// that one object cannot address another's keys.
    pub fn with_backend(backend_factory: BackendFactory) -> Self {
        Self {
            location: "<custom backend>".to_string(),
            backend_factory,
            engine_factory: Box::new(|| Ok(Box::new(ChdbEngine::new()))),
            owner: None,
            scratch_root: None,
            tuning: Tuning::default(),
        }
    }

    fn named(mut self, location: &str) -> Self {
        self.location = location.to_string();
        self
    }

    /// Uses a different engine for every object this namespace opens.
    ///
    /// The default runs the chDB engine this process linked. A test double or a
    /// connection with extra arguments goes here.
    pub fn with_engine(mut self, engine_factory: EngineFactory) -> Self {
        self.engine_factory = engine_factory;
        self
    }

    /// The default writer name recorded in the lease of every object this
    /// namespace opens. Observability only.
    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    /// The default parent directory for scratch trees.
    pub fn with_scratch_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.scratch_root = Some(root.into());
        self
    }

    /// The default lease and commit parameters for every object this namespace
    /// opens.
    ///
    /// A per-open [`OpenOptions::tuning`] wins, unless it is exactly
    /// [`Tuning::default`] — which is what an [`OpenOptions`] built with
    /// `..Default::default()` carries, and means "whatever the namespace says".
    pub fn with_tuning(mut self, tuning: Tuning) -> Self {
        self.tuning = tuning;
        self
    }

    /// Where this namespace points.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Opens one object, reporting whether it already existed.
    ///
    /// A caller creating a tenant on first use needs to tell "restored" from
    /// "created" without a separate probe, which is what the flag is for. A
    /// writer lease is taken unless [`OpenOptions::read_only`] is set.
    ///
    /// ```no_run
    /// use chdb_rust::durable::{Namespace, OpenOptions};
    /// use chdb_rust::format::OutputFormat;
    ///
    /// let namespace = Namespace::new("file:///var/lib/chdb-durable")?.with_owner("worker-1");
    /// let (object, existed) = namespace.open("tenant-123", OpenOptions {
    ///     database: Some("mem".to_string()),
    ///     ..OpenOptions::default()
    /// })?;
    ///
    /// if !existed {
    ///     object.execute("CREATE TABLE events (id UInt64) ENGINE = MergeTree ORDER BY id")?;
    /// }
    /// let ticket = object.execute("INSERT INTO events VALUES (1)")?;
    /// object.flush_through(ticket)?;   // now it survives losing this machine
    /// let rows = object.query("SELECT count() FROM events", OutputFormat::CSV)?;
    /// object.close()?;
    /// # Ok::<(), chdb_rust::durable::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// An open that fails leaves nothing behind: any lease it took is released,
    /// the engine is closed and the scratch directory is removed. See
    /// [`Category`](super::Category) for what the failures mean; the common ones
    /// are [`Category::NotFound`] for a read-only open of a missing object and
    /// [`Category::LeaseHeld`] when another writer has it.
    pub fn open(&self, id: &str, options: OpenOptions) -> Result<(DurableObject, bool)> {
        validate_object_id(id)?;
        let backend = (self.backend_factory)(id)?;
        let options = OpenOptions {
            owner: options.owner.or_else(|| self.owner.clone()),
            scratch_root: options.scratch_root.or_else(|| self.scratch_root.clone()),
            tuning: if options.tuning == Tuning::default() {
                self.tuning.clone()
            } else {
                options.tuning
            },
            ..options
        };
        open_object(id, backend, &self.engine_factory, options)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn a_local_url_names_a_directory_in_three_spellings() -> Result<()> {
        assert_eq!(local_root("file:///var/lib/x")?, Path::new("/var/lib/x"));
        assert_eq!(local_root("local:/var/lib/x")?, Path::new("/var/lib/x"));
        assert_eq!(local_root("/var/lib/x")?, Path::new("/var/lib/x"));
        Ok(())
    }

    #[test]
    fn a_scheme_this_build_cannot_serve_fails_at_configuration_time() {
        let error = local_root("gs://bucket/prefix").expect_err("no GCS backend here");
        assert_eq!(error.category(), Category::Backend);
        assert!(error.to_string().contains("backend factory"), "{error}");
    }

    #[cfg(feature = "durable-s3")]
    #[test]
    fn an_s3_url_splits_into_a_bucket_and_a_prefix() {
        assert_eq!(
            s3_location("s3://my-bucket/durable/objects?region=eu-central-1").unwrap(),
            ("my-bucket".to_string(), "durable/objects".to_string())
        );
        assert_eq!(
            s3_location("s3://my-bucket").unwrap(),
            ("my-bucket".to_string(), String::new()),
            "a bucket with no prefix is the bucket root"
        );
        assert_eq!(
            s3_location("s3://my-bucket/").unwrap(),
            ("my-bucket".to_string(), String::new())
        );
        assert_eq!(
            s3_location("s3://").unwrap_err().category(),
            Category::Backend
        );
    }

    #[cfg(feature = "durable-s3")]
    #[test]
    fn a_query_value_survives_being_escaped_by_a_url_builder() {
        let query = query_of("s3://b/p?region=auto&endpoint=https%3A%2F%2Fx.example&pathStyle=1");
        assert_eq!(query[0], ("region".to_string(), "auto".to_string()));
        assert_eq!(
            query[1],
            ("endpoint".to_string(), "https://x.example".to_string()),
            "an escaped endpoint has to mean the same as an unescaped one"
        );
        assert_eq!(query[2], ("pathStyle".to_string(), "1".to_string()));

        assert!(query_of("s3://b/p").is_empty());
        // A stray percent is a literal, not a reason to fail a configuration.
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("a%2Fb"), "a/b");
    }

    #[test]
    fn a_relative_root_is_refused() {
        assert!(local_root("file://relative/path").is_err());
        assert!(local_root("chdb-durable").is_err());
    }

    #[test]
    fn an_object_id_cannot_address_another_objects_prefix() {
        let namespace = Namespace::new("/tmp/chdb-durable-test").expect("a local namespace");
        for id in ["", "..", "a/b"] {
            let error = namespace
                .open(id, OpenOptions::default())
                .expect_err("an id that is not one segment");
            assert_eq!(error.category(), Category::Backend, "{id:?}");
        }
    }
}
