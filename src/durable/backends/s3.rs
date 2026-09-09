//! S3-compatible backend — AWS S3, Cloudflare R2, MinIO, and anything else that
//! speaks the same two operations.
//!
//! This is the backend that makes the whole thing mean something. A local
//! directory cannot be a remote authority: when the machine holding it is gone,
//! so is the object. Recovering a database on a *different* machine needs the
//! head, the checkpoints and the WAL to live somewhere neither machine owns.
//!
//! # Conditional writes are the whole contract
//!
//! The protocol needs a real atomic create and a real atomic compare-and-swap;
//! simulating either with a HEAD followed by a PUT is not a weaker version, it
//! is the bug the protocol exists to prevent. S3 provides both as preconditions
//! on `PutObject`:
//!
//! ```text
//! put_bytes_if_absent  ->  PUT with If-None-Match: *
//! replace_if_match     ->  PUT with If-Match: <etag>
//! ```
//!
//! A precondition failure is a compare-and-swap outcome, not a transport error,
//! and it is reported as one. Everything the state machine does with
//! [`PutOutcome::AlreadyExists`] and [`ReplaceOutcome::NotMatched`] depends on
//! that distinction being made here rather than upstream.
//!
//! # No retries here, on purpose
//!
//! The object layer already knows how to settle an uncertain write: the keys it
//! publishes are unique per attempt, so it re-reads and compares a digest, and
//! its head commits re-read and look for their own intent. A retry loop in the
//! backend would sit underneath all of that and turn a request that landed into
//! a precondition failure — recoverable, but only because the layer above never
//! trusts a status code on its own. Leaving the retry to the layer that can
//! prove what happened keeps one deadline and one attempt count for the whole
//! commit, which is what §5.8 asks for.
//!
//! # ETags stay opaque, and carry one assumption worth naming
//!
//! An S3 ETag is quoted, and it is only an MD5 for single-part uploads — not on
//! R2, not for multipart, not necessarily forever. It is stored and handed back
//! exactly as received and never parsed, which is what the contract requires.
//!
//! Being a content hash has a consequence a version counter would not have:
//! writing *byte-identical* content does not advance the ETag, so the token used
//! for that write stays valid afterwards and a second racer holding it could
//! also win. Durable is safe from this because every head write changes the
//! bytes — a lease acquisition moves the generation, a heartbeat moves
//! `expires_at`, and a flush or checkpoint moves `manifest.seq`. That is a real
//! dependency rather than a coincidence, so it is written down here: anything
//! that made a head write idempotent at the byte level would break
//! compare-and-swap on any content-hash-ETag provider.
//!
//! # V1 limits
//!
//! Single `PutObject` only, so an object caps at 5 GiB and a larger checkpoint
//! fails with [`Category::LimitExceeded`] rather than silently truncating.
//! Multipart upload is the fix and is not here yet.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

use ureq::Agent;

use crate::durable::backend::{Backend, PutOutcome, ReplaceOutcome, Tagged};
use crate::durable::digest::Digest;
use crate::durable::errors::{err, Category, Error, Result};
use crate::durable::keys::is_valid_object_key;

use super::sigv4::{sha256_hex, sign, Credentials, Signable};

/// The ceiling for one `PutObject`. Beyond it a checkpoint needs multipart
/// upload.
pub const MAX_SINGLE_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// How an [`S3Backend`] reaches its bucket.
#[derive(Debug, Clone, Default)]
pub struct S3Options {
    /// The bucket. Required.
    pub bucket: String,
    /// The key prefix for this object, without a leading slash. May be empty.
    pub prefix: String,
    /// The region the signature is scoped to. Falls back to `AWS_REGION`, then
    /// `AWS_DEFAULT_REGION`, then `us-east-1` — which is what an S3-compatible
    /// store that ignores regions expects to be told.
    pub region: Option<String>,
    /// Overrides the AWS endpoint, for MinIO, R2, or an S3-compatible gateway.
    /// Must include a scheme.
    pub endpoint: Option<String>,
    /// Puts the bucket in the path rather than the hostname. Defaults to true
    /// when `endpoint` is set, because that is what a local MinIO wants, and
    /// false against AWS.
    pub path_style: Option<bool>,
    /// Credentials, when the environment and the shared credentials file are
    /// not where they live. See [`Credentials::resolve`].
    pub credentials: Option<Credentials>,
    /// How long one request may take. Defaults to five minutes, which a
    /// checkpoint of a large database may need raised.
    pub timeout: Option<Duration>,
}

/// One durable object stored under a bucket prefix.
pub struct S3Backend {
    bucket: String,
    prefix: String,
    region: String,
    endpoint: Option<(String, String, String)>,
    path_style: bool,
    credentials: Credentials,
    agent: Agent,
    describe: String,
}

impl std::fmt::Debug for S3Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Backend")
            .field("location", &self.describe)
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

fn backend_error(message: impl Into<String>) -> Error {
    err(Category::Backend, message)
}

/// Splits an endpoint URL into scheme, host and base path.
fn split_endpoint(endpoint: &str) -> Result<(String, String, String)> {
    let (scheme, rest) = endpoint.split_once("://").ok_or_else(|| {
        backend_error(format!(
            "durable: an S3 endpoint needs a scheme, got {endpoint:?}"
        ))
    })?;
    let (host, path) = match rest.split_once('/') {
        Some((host, path)) => (host, format!("/{}", path.trim_end_matches('/'))),
        None => (rest, String::new()),
    };
    if scheme.is_empty() || host.is_empty() {
        return Err(backend_error(format!(
            "durable: an S3 endpoint needs a scheme and a host, got {endpoint:?}"
        )));
    }
    Ok((
        scheme.to_string(),
        host.to_string(),
        path.trim_end_matches('/').to_string(),
    ))
}

/// The HTTP agent, honouring `AWS_CA_BUNDLE` the way the AWS tools do.
///
/// Reading it matters more than it looks: on a host behind a TLS-inspecting
/// proxy, that variable is the only place the extra root lives, and a client
/// that ignores it fails every request with a certificate error while `aws s3`
/// beside it works.
fn build_agent(timeout: Duration) -> Result<Agent> {
    let config = Agent::config_builder()
        .timeout_global(Some(timeout))
        // 4xx and 5xx are answers here, not errors: a 412 is the
        // compare-and-swap losing, and a 404 is an absent object.
        .http_status_as_error(false);

    let Ok(bundle) = std::env::var("AWS_CA_BUNDLE") else {
        return Ok(config.build().into());
    };
    if bundle.is_empty() {
        return Ok(config.build().into());
    }

    let pem = std::fs::read(&bundle).map_err(|e| {
        Error::wrap(
            Category::Backend,
            e,
            format!("durable: cannot read the AWS_CA_BUNDLE at {bundle}"),
        )
    })?;
    let mut roots = Vec::new();
    for item in ureq::tls::parse_pem(&pem) {
        if let Ok(ureq::tls::PemItem::Certificate(certificate)) = item {
            roots.push(certificate);
        }
    }
    if roots.is_empty() {
        return Err(backend_error(format!(
            "durable: the AWS_CA_BUNDLE at {bundle} holds no certificates"
        )));
    }
    let tls = ureq::tls::TlsConfig::builder()
        .root_certs(ureq::tls::RootCerts::Specific(std::sync::Arc::new(roots)))
        .build();
    Ok(config.tls_config(tls).build().into())
}

impl S3Backend {
    /// Binds a backend to one object's key prefix.
    ///
    /// # Errors
    ///
    /// [`Category::Backend`] when there is no bucket, no resolvable
    /// credentials, or an endpoint that is not a URL.
    pub fn new(options: S3Options) -> Result<Self> {
        if options.bucket.is_empty() {
            return Err(backend_error("durable: the S3 backend needs a bucket"));
        }
        let credentials = Credentials::resolve(options.credentials)?;
        let region = options
            .region
            .filter(|region| !region.is_empty())
            .or_else(|| std::env::var("AWS_REGION").ok().filter(|r| !r.is_empty()))
            .or_else(|| {
                std::env::var("AWS_DEFAULT_REGION")
                    .ok()
                    .filter(|r| !r.is_empty())
            })
            // The signature needs a region whether or not the provider cares
            // which one.
            .unwrap_or_else(|| "us-east-1".to_string());

        let endpoint = match &options.endpoint {
            Some(endpoint) if !endpoint.is_empty() => Some(split_endpoint(endpoint)?),
            _ => None,
        };
        let path_style = options.path_style.unwrap_or(endpoint.is_some());
        let prefix = options.prefix.trim_matches('/').to_string();

        Ok(Self {
            // Never the credentials, and never a presigned anything: this
            // string ends up in error messages and logs.
            describe: format!("s3://{}/{}", options.bucket, prefix),
            bucket: options.bucket,
            prefix,
            region,
            endpoint,
            path_style,
            credentials,
            agent: build_agent(options.timeout.unwrap_or(Duration::from_secs(300)))?,
        })
    }

    /// Prefixes a protocol key, refusing one that is not a plain relative key.
    fn key_for(&self, key: &str) -> Result<String> {
        if !is_valid_object_key(key) {
            return Err(backend_error(format!(
                "durable: refusing to resolve invalid key {key:?}"
            )));
        }
        if self.prefix.is_empty() {
            Ok(key.to_string())
        } else {
            Ok(format!("{}/{key}", self.prefix))
        }
    }

    /// The request URL and the host the signature covers.
    fn url_for(&self, key: &str) -> Result<(String, String, String)> {
        let full = self.key_for(key)?;
        let (scheme, host, path) = match (&self.endpoint, self.path_style) {
            (Some((scheme, host, base)), true) => (
                scheme.clone(),
                host.clone(),
                format!("{base}/{}/{full}", self.bucket),
            ),
            (Some((scheme, host, base)), false) => (
                scheme.clone(),
                format!("{}.{host}", self.bucket),
                format!("{base}/{full}"),
            ),
            (None, true) => (
                "https".to_string(),
                format!("s3.{}.amazonaws.com", self.region),
                format!("/{}/{full}", self.bucket),
            ),
            (None, false) => (
                "https".to_string(),
                format!("{}.s3.{}.amazonaws.com", self.bucket, self.region),
                format!("/{full}"),
            ),
        };
        let encoded = super::sigv4::uri_encode_path(&path);
        Ok((format!("{scheme}://{host}{encoded}"), host, path))
    }

    /// Signs and sends one request whose body is already known.
    ///
    /// The method follows from the body rather than being passed alongside it:
    /// ureq types a builder by whether it carries one, and the two cannot be
    /// mixed up if only one of them can be built.
    fn send(
        &self,
        key: &str,
        headers: &[(String, String)],
        payload_hash: &str,
        body: Body<'_>,
    ) -> Result<Outcome> {
        let method = body.method();
        let (url, host, path) = self.url_for(key)?;
        let signed = sign(
            Signable {
                method,
                host: &host,
                path: &path,
                query: "",
                headers,
                payload_hash,
            },
            &self.credentials,
            &self.region,
            std::time::SystemTime::now(),
        );
        let all = || headers.iter().chain(signed.iter());

        let sent = match body {
            Body::None => {
                let mut request = self.agent.get(&url);
                for (name, value) in all() {
                    request = request.header(name.as_str(), value.as_str());
                }
                request.call()
            }
            Body::Bytes(bytes) => {
                let mut request = self.agent.put(&url);
                for (name, value) in all() {
                    request = request.header(name.as_str(), value.as_str());
                }
                request.send(bytes)
            }
            Body::File(file) => {
                let mut request = self.agent.put(&url);
                for (name, value) in all() {
                    request = request.header(name.as_str(), value.as_str());
                }
                // A File body carries its own length, so the request goes out
                // length-delimited rather than chunked. That is not a nicety:
                // SigV4 header signing of a chunked body is a different signing
                // scheme, and the request would be rejected.
                request.send(file)
            }
        };

        match sent {
            Ok(response) => Ok(Outcome::Responded(response)),
            Err(e) if reached_the_service(&e) => Ok(Outcome::Indeterminate),
            Err(e) => Err(Error::wrap(
                Category::Backend,
                e,
                format!("durable: {method} {key} failed against {}", self.describe),
            )),
        }
    }

    fn assert_within_single_put(&self, key: &str, size: u64) -> Result<()> {
        if size > MAX_SINGLE_PUT_BYTES {
            return Err(err(
                Category::LimitExceeded,
                format!(
                    "durable: {key} is {size} bytes, over the {MAX_SINGLE_PUT_BYTES}-byte ceiling \
                     for a single PutObject; multipart upload is not implemented, so checkpoint \
                     more often or reduce the database"
                ),
            )
            .with_limit(MAX_SINGLE_PUT_BYTES, size));
        }
        Ok(())
    }

    /// Turns an unexpected status into an error carrying the beginning of the
    /// provider's own message.
    ///
    /// S3 answers with an XML document whose `Code` and `Message` are what an
    /// operator needs; the cap is there because an error body is not a place to
    /// trust a length.
    fn status_error(&self, what: &str, mut response: Response) -> Error {
        let status = response.status().as_u16();
        let detail = response
            .body_mut()
            .with_config()
            .limit(2048)
            .read_to_string()
            .unwrap_or_default();
        let detail = detail.trim();
        let suffix = if detail.is_empty() {
            String::new()
        } else {
            format!(": {detail}")
        };
        backend_error(format!(
            "durable: {what} failed against {} (HTTP {status}){suffix}",
            self.describe
        ))
    }
}

type Response = ureq::http::Response<ureq::Body>;

enum Outcome {
    Responded(Response),
    /// The request may or may not have reached the service.
    Indeterminate,
}

enum Body<'a> {
    None,
    Bytes(&'a [u8]),
    File(File),
}

impl Body<'_> {
    fn method(&self) -> &'static str {
        match self {
            Self::None => "GET",
            Self::Bytes(_) | Self::File(_) => "PUT",
        }
    }
}

/// Could a request that failed in transport still have been committed?
///
/// The distinction is the difference between a retry and a `commit_ambiguous`.
/// A timeout or a reset connection may have reached the service; an
/// unresolvable host or a refused connection did not. Guessing "did not" when it
/// did is how a caller ends up publishing twice, so anything genuinely in doubt
/// counts as in doubt — the cases below are the only ones ruled out.
fn reached_the_service(error: &ureq::Error) -> bool {
    match error {
        ureq::Error::HostNotFound | ureq::Error::ConnectionFailed => false,
        ureq::Error::Io(io) => !matches!(
            io.kind(),
            std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
        ),
        // A bad URI, a header the http crate rejects, too many redirects: the
        // request was never sent.
        ureq::Error::BadUri(_)
        | ureq::Error::Http(_)
        | ureq::Error::InvalidProxyUrl
        | ureq::Error::TooManyRedirects
        | ureq::Error::BodyExceedsLimit(_) => false,
        _ => true,
    }
}

/// Does this status mean someone else got there first?
///
/// S3 answers 412 for `If-Match` and for `If-None-Match: *` against an object
/// that already exists; a race between two conditional writes can also surface
/// as 409 `ConditionalRequestConflict`. Both mean the same thing to the
/// protocol.
fn is_precondition_failure(status: u16) -> bool {
    status == 412 || status == 409
}

fn etag_of(response: &Response) -> Option<String> {
    response
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

impl Backend for S3Backend {
    fn describe(&self) -> String {
        self.describe.clone()
    }

    fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.get_bytes_with_etag(key)?.map(|tagged| tagged.data))
    }

    fn get_bytes_with_etag(&self, key: &str) -> Result<Option<Tagged>> {
        let Outcome::Responded(mut response) = self.send(key, &[], &sha256_hex(b""), Body::None)?
        else {
            return Err(backend_error(format!(
                "durable: reading {key} from {} did not complete",
                self.describe
            )));
        };
        let status = response.status().as_u16();
        if status == 404 {
            return Ok(None);
        }
        if status != 200 {
            return Err(self.status_error(&format!("reading {key}"), response));
        }
        // An absent ETag leaves nothing to compare-and-swap against, so it is a
        // hard failure rather than an empty token that silently never matches.
        let etag = etag_of(&response).ok_or_else(|| {
            backend_error(format!(
                "durable: {key} came back from {} without an ETag; this provider cannot support \
                 compare-and-swap",
                self.describe
            ))
        })?;
        let data = response
            .body_mut()
            .with_config()
            // A head is capped by the protocol and a WAL segment by its own
            // limit; both are checked above this layer, and the cap here only
            // stops a runaway response from becoming a runaway allocation.
            .limit(crate::durable::MAX_WAL_SEGMENT_BYTES)
            .read_to_vec()
            .map_err(|e| {
                Error::wrap(
                    Category::Backend,
                    e,
                    format!("durable: reading the body of {key} from {}", self.describe),
                )
            })?;
        Ok(Some(Tagged { data, etag }))
    }

    fn open_reader(&self, key: &str) -> Result<Option<Box<dyn Read + Send>>> {
        let Outcome::Responded(response) = self.send(key, &[], &sha256_hex(b""), Body::None)?
        else {
            return Err(backend_error(format!(
                "durable: opening {key} from {} did not complete",
                self.describe
            )));
        };
        let status = response.status().as_u16();
        if status == 404 {
            return Ok(None);
        }
        if status != 200 {
            return Err(self.status_error(&format!("opening {key}"), response));
        }
        Ok(Some(Box::new(response.into_body().into_reader())))
    }

    fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> Result<PutOutcome> {
        self.assert_within_single_put(key, data.len() as u64)?;
        let headers = vec![("if-none-match".to_string(), "*".to_string())];
        match self.send(key, &headers, &sha256_hex(data), Body::Bytes(data))? {
            Outcome::Indeterminate => Ok(PutOutcome::Ambiguous),
            Outcome::Responded(response) => Ok(self.classify_put(response)),
        }
    }

    fn put_file_if_absent(
        &self,
        key: &str,
        local_path: &Path,
        digest: &Digest,
    ) -> Result<PutOutcome> {
        self.assert_within_single_put(key, digest.size)?;
        let file = File::open(local_path).map_err(|e| {
            Error::wrap(
                Category::Backend,
                e,
                format!("durable: cannot read {}", local_path.display()),
            )
        })?;
        let headers = vec![("if-none-match".to_string(), "*".to_string())];
        // The digest the manifest records is the digest that signs the upload,
        // so a checkpoint archive is never read twice.
        match self.send(key, &headers, &digest.sha256, Body::File(file))? {
            Outcome::Indeterminate => Ok(PutOutcome::Ambiguous),
            Outcome::Responded(response) => Ok(self.classify_put(response)),
        }
    }

    fn replace_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<ReplaceOutcome> {
        self.assert_within_single_put(key, data.len() as u64)?;
        let headers = vec![("if-match".to_string(), etag.to_string())];
        let response = match self.send(key, &headers, &sha256_hex(data), Body::Bytes(data))? {
            Outcome::Indeterminate => return Ok(ReplaceOutcome::Ambiguous),
            Outcome::Responded(response) => response,
        };

        let status = response.status().as_u16();
        if is_precondition_failure(status) {
            return Ok(ReplaceOutcome::NotMatched);
        }
        if status == 404 {
            // The target is gone, so the compare-and-swap did not match.
            // Reporting it as anything else would send the caller looking for
            // its own intent in a head that no longer exists.
            return Ok(ReplaceOutcome::NotMatched);
        }
        if status >= 500 {
            return Ok(ReplaceOutcome::Ambiguous);
        }
        if status != 200 {
            return Err(self.status_error(&format!("replacing {key}"), response));
        }
        match etag_of(&response) {
            Some(etag) => Ok(ReplaceOutcome::Done { etag }),
            // The write landed but the new token is unknown, so the next
            // compare-and-swap would have nothing to present. Re-reading is the
            // honest way out.
            None => Ok(ReplaceOutcome::Ambiguous),
        }
    }
}

impl S3Backend {
    fn classify_put(&self, response: Response) -> PutOutcome {
        let status = response.status().as_u16();
        match status {
            _ if is_precondition_failure(status) => PutOutcome::AlreadyExists,
            200 => PutOutcome::Created,
            // A 5xx, or anything else unexpected: the layer above settles it by
            // re-reading the unique key it just tried to publish.
            _ => PutOutcome::Ambiguous,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn options(bucket: &str) -> S3Options {
        S3Options {
            bucket: bucket.to_string(),
            prefix: "durable/tenant-1".to_string(),
            region: Some("eu-central-1".to_string()),
            credentials: Some(Credentials {
                access_key_id: "AKIDEXAMPLE".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
            }),
            ..S3Options::default()
        }
    }

    #[test]
    fn aws_gets_a_virtual_hosted_url_and_a_gateway_gets_a_path_style_one() -> Result<()> {
        let aws = S3Backend::new(options("my-bucket"))?;
        let (url, host, path) = aws.url_for("head.json")?;
        assert_eq!(host, "my-bucket.s3.eu-central-1.amazonaws.com");
        assert_eq!(path, "/durable/tenant-1/head.json");
        assert_eq!(
            url,
            "https://my-bucket.s3.eu-central-1.amazonaws.com/durable/tenant-1/head.json"
        );

        let minio = S3Backend::new(S3Options {
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            ..options("my-bucket")
        })?;
        let (url, host, _) = minio.url_for("wal/1-1-aa.jsonl")?;
        assert_eq!(
            host, "127.0.0.1:9000",
            "a gateway keeps the bucket in the path"
        );
        assert_eq!(
            url,
            "http://127.0.0.1:9000/my-bucket/durable/tenant-1/wal/1-1-aa.jsonl"
        );
        Ok(())
    }

    #[test]
    fn a_key_out_of_an_untrusted_head_cannot_leave_the_prefix() -> Result<()> {
        let backend = S3Backend::new(options("my-bucket"))?;
        for key in ["../elsewhere", "/absolute", "wal/../../escape", ""] {
            assert_eq!(
                backend.key_for(key).unwrap_err().category(),
                Category::Backend,
                "{key:?}"
            );
        }
        assert_eq!(backend.key_for("head.json")?, "durable/tenant-1/head.json");
        Ok(())
    }

    #[test]
    fn the_description_carries_no_credential() -> Result<()> {
        let backend = S3Backend::new(options("my-bucket"))?;
        let described = format!("{} {:?}", backend.describe(), backend);
        assert!(described.contains("s3://my-bucket/durable/tenant-1"));
        assert!(!described.contains("secret"), "{described}");
        assert!(!described.contains("AKIDEXAMPLE"), "{described}");
        Ok(())
    }

    #[test]
    fn an_object_beyond_one_put_is_a_limit_rather_than_a_truncation() -> Result<()> {
        let backend = S3Backend::new(options("my-bucket"))?;
        let error = backend
            .assert_within_single_put("checkpoints/1-1-aa.tar.gz", MAX_SINGLE_PUT_BYTES + 1)
            .expect_err("over one PutObject");
        assert_eq!(error.category(), Category::LimitExceeded);
        assert_eq!(
            error.limit(),
            Some((MAX_SINGLE_PUT_BYTES, MAX_SINGLE_PUT_BYTES + 1))
        );
        Ok(())
    }

    #[test]
    fn a_precondition_failure_is_a_race_rather_than_an_error() {
        assert!(is_precondition_failure(412), "If-Match / If-None-Match");
        assert!(is_precondition_failure(409), "ConditionalRequestConflict");
        assert!(!is_precondition_failure(404));
        assert!(!is_precondition_failure(200));
        assert!(!is_precondition_failure(500));
    }

    #[test]
    fn only_a_request_that_provably_never_left_is_ruled_out() {
        assert!(!reached_the_service(&ureq::Error::HostNotFound));
        assert!(!reached_the_service(&ureq::Error::ConnectionFailed));
        assert!(!reached_the_service(&ureq::Error::Io(
            std::io::Error::from(std::io::ErrorKind::ConnectionRefused)
        )));
        // A reset or a timeout may well have been received and acted on.
        assert!(reached_the_service(&ureq::Error::Io(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset
        ))));
        assert!(reached_the_service(&ureq::Error::Io(std::io::Error::from(
            std::io::ErrorKind::BrokenPipe
        ))));
    }

    #[test]
    fn an_endpoint_needs_a_scheme_and_a_host() {
        assert!(split_endpoint("127.0.0.1:9000").is_err());
        assert!(split_endpoint("http://").is_err());
        assert_eq!(
            split_endpoint("https://x.r2.cloudflarestorage.com").unwrap(),
            (
                "https".to_string(),
                "x.r2.cloudflarestorage.com".to_string(),
                String::new()
            )
        );
        assert_eq!(
            split_endpoint("http://gateway.example/base/").unwrap(),
            (
                "http".to_string(),
                "gateway.example".to_string(),
                "/base".to_string()
            )
        );
    }
}
