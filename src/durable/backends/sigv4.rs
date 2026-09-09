//! AWS Signature Version 4, for the two requests the S3 backend makes.
//!
//! Signing is here rather than delegated to the AWS SDK on purpose. The durable
//! control plane needs exactly two S3 operations — `GetObject` and `PutObject`,
//! the latter with a precondition — and `aws-sdk-s3` brings an async runtime
//! and several dozen crates with it, for a feature most callers of this crate
//! will never enable. The [`Backend`](crate::durable::Backend) trait is
//! synchronous, so an SDK built on Tokio would have to be driven from a
//! `block_on` inside every call.
//!
//! So the surface is small, and staying small is the point: no multipart, no
//! listing, no chunked signing, no presigning. What is here is the SigV4
//! header-signing form for a request whose payload hash is already known —
//! which it always is, because the protocol computes the SHA-256 of everything
//! it publishes before publishing it (contract §4.5). The digest a manifest
//! records is the digest that signs the upload, so a checkpoint archive is
//! never read twice.
//!
//! There is deliberately no `UNSIGNED-PAYLOAD` constant. Every request signed
//! here knows its payload hash, and leaving the escape hatch out means a future
//! operation cannot reach for it without first thinking about whether the
//! provider accepts it.

use std::fmt::Write as _;

use hmac::{Hmac, Mac};
use sha2::{Digest as _, Sha256};

use crate::durable::errors::{err, Category, Result};

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SERVICE: &str = "s3";

/// What SigV4 needs to sign.
///
/// `session_token` is set only for temporary credentials, which is what SSO,
/// an assumed role and the instance metadata service all hand out.
#[derive(Clone)]
pub struct Credentials {
    /// The access key id.
    pub access_key_id: String,
    /// The secret access key. Never logged, and never part of `describe`.
    pub secret_access_key: String,
    /// The session token for temporary credentials, if there is one.
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    /// Prints the key id and nothing else: this type ends up inside a backend
    /// that a caller may well `{:?}` into a log.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl Credentials {
    fn from_parts(
        id: Option<String>,
        secret: Option<String>,
        token: Option<String>,
    ) -> Option<Self> {
        let (id, secret) = (id?, secret?);
        if id.is_empty() || secret.is_empty() {
            return None;
        }
        Some(Self {
            access_key_id: id,
            secret_access_key: secret,
            session_token: token.filter(|token| !token.is_empty()),
        })
    }

    /// Finds credentials the way the AWS tools do, in the order they do,
    /// stopping at the first complete pair.
    ///
    /// Three sources, and the list stops where a hand-written signer stops
    /// being the right tool:
    ///
    /// 1. given explicitly to the backend;
    /// 2. the standard environment variables;
    /// 3. the shared credentials file, honouring `AWS_PROFILE`.
    ///
    /// Not here: SSO, assumed roles, the EC2 and ECS metadata services, and
    /// anything else needing a refresh loop. Those belong to the AWS SDK. For
    /// an SSO profile, export the temporary credentials the way the CLI does:
    ///
    /// ```sh
    /// eval "$(aws configure export-credentials --profile my-profile --format env)"
    /// ```
    ///
    /// # Errors
    ///
    /// [`Category::Backend`] when nothing supplies a key pair, with a message
    /// naming the three places that were tried.
    pub fn resolve(explicit: Option<Credentials>) -> Result<Self> {
        if let Some(explicit) = explicit {
            return Ok(explicit);
        }
        let from_env = Self::from_parts(
            std::env::var("AWS_ACCESS_KEY_ID").ok(),
            std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
            std::env::var("AWS_SESSION_TOKEN").ok(),
        );
        if let Some(from_env) = from_env {
            return Ok(from_env);
        }
        if let Some(from_file) = Self::from_shared_file() {
            return Ok(from_file);
        }
        Err(err(
            Category::Backend,
            "durable: no S3 credentials found; set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, \
             configure a shared credentials file, or pass credentials to the backend. SSO and \
             instance-role credentials are not resolved here — export them first with \
             `aws configure export-credentials --format env`",
        ))
    }

    /// Reads the requested profile out of the shared credentials file.
    ///
    /// Deliberately a small parser: the INI dialect that file uses has corners
    /// — nested properties, quoted values — that only matter for settings this
    /// signer does not read.
    fn from_shared_file() -> Option<Self> {
        let path = match std::env::var("AWS_SHARED_CREDENTIALS_FILE") {
            Ok(path) if !path.is_empty() => std::path::PathBuf::from(path),
            _ => std::path::PathBuf::from(std::env::var("HOME").ok()?)
                .join(".aws")
                .join("credentials"),
        };
        let body = std::fs::read_to_string(path).ok()?;
        let wanted = std::env::var("AWS_PROFILE").unwrap_or_else(|_| "default".to_string());

        let (mut id, mut secret, mut token) = (None, None, None);
        let mut in_profile = false;
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }
            if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
                // A file written by the SSO tooling prefixes profile names.
                let name = name.trim().strip_prefix("profile ").unwrap_or(name.trim());
                in_profile = name == wanted;
                continue;
            }
            if !in_profile {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let value = value.trim().to_string();
            match key.trim().to_ascii_lowercase().as_str() {
                "aws_access_key_id" => id = Some(value),
                "aws_secret_access_key" => secret = Some(value),
                "aws_session_token" => token = Some(value),
                _ => {}
            }
        }
        Self::from_parts(id, secret, token)
    }
}

/// The lowercase hex SHA-256 of a buffer.
pub(crate) fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = <Hmac<Sha256>>::new_from_slice(key).expect("HMAC takes a key of any length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Derives the date/region/service-scoped signing key.
///
/// `service` is a parameter even though this module only ever signs for S3, so
/// the derivation can be checked against AWS's own published example — which
/// uses a different service. Getting one link of this HMAC chain wrong produces
/// a signature that is simply rejected, with nothing to say which link it was.
fn signing_key(secret: &str, datestamp: &str, region: &str, service: &str) -> Vec<u8> {
    let key = hmac_sha256(format!("AWS4{secret}").as_bytes(), datestamp.as_bytes());
    let key = hmac_sha256(&key, region.as_bytes());
    let key = hmac_sha256(&key, service.as_bytes());
    hmac_sha256(&key, b"aws4_request")
}

/// Percent-encodes a path the way S3 canonicalisation wants: every byte outside
/// the unreserved set is escaped, and `/` is left alone as a separator.
pub(crate) fn uri_encode_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for byte in path.bytes() {
        let unreserved =
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/');
        if unreserved {
            out.push(byte as char);
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// One header of a request being signed.
pub(crate) type Header = (String, String);

/// UTC now, as the two timestamp forms SigV4 uses.
///
/// Passed into [`sign`] rather than read inside it, so a test can sign at a
/// fixed instant and compare against a known-good signature.
pub(crate) fn timestamps(now: std::time::SystemTime) -> (String, String) {
    let secs = now
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (year, month, day, hour, minute, second) = civil_from_unix(secs);
    (
        format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"),
        format!("{year:04}{month:02}{day:02}"),
    )
}

/// Days-from-civil, inverted: the standard algorithm, because `std` has no
/// calendar and a date formatter is not worth a dependency for two strings.
fn civil_from_unix(secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (
        y as u64,
        m as u64,
        d as u64,
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60,
    )
}

/// One request, in the form the canonical request is built from.
pub(crate) struct Signable<'a> {
    /// `GET` or `PUT`.
    pub(crate) method: &'a str,
    /// The `Host` header the signature covers, which for a virtual-hosted
    /// bucket is not the same as the endpoint.
    pub(crate) host: &'a str,
    /// The unencoded path; [`uri_encode_path`] is applied here rather than by
    /// the caller, because the canonical form and the wire form differ.
    pub(crate) path: &'a str,
    /// The canonical query string, empty for every key this protocol uses.
    pub(crate) query: &'a str,
    /// The headers the caller has already set.
    pub(crate) headers: &'a [Header],
    /// The lowercase hex SHA-256 of the body.
    pub(crate) payload_hash: &'a str,
}

/// The headers SigV4 adds to a request, ready to be set on it.
///
/// The ones that matter out of [`Signable::headers`] are signed, and the
/// signature covers `host` whether or not the caller listed it. Signing the
/// preconditions is not required by S3, but they are the headers that decide
/// whether the request mutates anything, so leaving them out of the signature
/// would let something between here and the bucket turn a conditional create
/// into an overwrite.
pub(crate) fn sign(
    request: Signable<'_>,
    credentials: &Credentials,
    region: &str,
    now: std::time::SystemTime,
) -> Vec<Header> {
    let Signable {
        method,
        host,
        path,
        query,
        headers,
        payload_hash,
    } = request;
    let (amz_date, datestamp) = timestamps(now);

    let mut signed: Vec<Header> = vec![("host".to_string(), host.to_string())];
    signed.push(("x-amz-date".to_string(), amz_date.clone()));
    signed.push(("x-amz-content-sha256".to_string(), payload_hash.to_string()));
    if let Some(token) = &credentials.session_token {
        signed.push(("x-amz-security-token".to_string(), token.clone()));
    }
    for (name, value) in headers {
        let lower = name.to_ascii_lowercase();
        let interesting = lower.starts_with("x-amz-")
            || lower == "if-match"
            || lower == "if-none-match"
            || lower == "content-type";
        if interesting && !signed.iter().any(|(existing, _)| *existing == lower) {
            signed.push((lower, value.trim().to_string()));
        }
    }
    signed.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers: String = signed
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let signed_names: Vec<&str> = signed.iter().map(|(name, _)| name.as_str()).collect();
    let signed_headers = signed_names.join(";");

    let canonical_request = format!(
        "{method}\n{}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        uri_encode_path(path)
    );
    let scope = format!("{datestamp}/{region}/{SERVICE}/aws4_request");
    let string_to_sign = format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = hex(&hmac_sha256(
        &signing_key(&credentials.secret_access_key, &datestamp, region, SERVICE),
        string_to_sign.as_bytes(),
    ));

    let authorization = format!(
        "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        credentials.access_key_id
    );

    // Everything the signature covers, plus the signature, minus `host` — which
    // the HTTP client sets from the URL and would reject as a duplicate.
    let mut out: Vec<Header> = signed
        .into_iter()
        .filter(|(name, _)| name != "host")
        .filter(|(name, _)| {
            !headers
                .iter()
                .any(|(set, _)| set.to_ascii_lowercase() == *name)
        })
        .collect();
    out.push(("authorization".to_string(), authorization));
    out
}

#[cfg(test)]
mod test {
    use super::*;

    /// AWS's own published SigV4 test vector, from the "Examples of the
    /// complete Signature Version 4 signing process" documentation.
    ///
    /// It signs for `iam`, not `s3`, which is exactly why `signing_key` takes
    /// the service: a wrong link in the HMAC chain produces a signature that is
    /// rejected with nothing to say which link it was, and this is the only
    /// check that can tell.
    #[test]
    fn the_signing_key_matches_the_published_derivation() {
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex(&key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    #[test]
    fn a_path_is_encoded_for_the_canonical_request_not_for_a_query_string() {
        assert_eq!(uri_encode_path("/wal/1-1-aa.jsonl"), "/wal/1-1-aa.jsonl");
        // A space is %20, never `+`: the `+` form produces a signature that
        // does not match the request S3 received.
        assert_eq!(uri_encode_path("/a b"), "/a%20b");
        assert_eq!(uri_encode_path("/a+b"), "/a%2Bb");
        assert_eq!(uri_encode_path("/tenant~1_2.3"), "/tenant~1_2.3");
        assert_eq!(uri_encode_path("/é"), "/%C3%A9");
    }

    #[test]
    fn timestamps_are_the_two_forms_sigv4_asks_for() {
        // 2015-08-30T12:36:00Z, the instant in the AWS worked example.
        let instant = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_440_938_160);
        assert_eq!(
            timestamps(instant),
            ("20150830T123600Z".to_string(), "20150830".to_string())
        );

        // A leap day, because the civil-from-days arithmetic is the one part of
        // this that a wrong constant would break silently for one day in four
        // years.
        let leap = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_709_209_800);
        assert_eq!(
            timestamps(leap),
            ("20240229T123000Z".to_string(), "20240229".to_string())
        );
    }

    #[test]
    fn the_signature_covers_the_preconditions_that_decide_whether_a_write_mutates() {
        let credentials = Credentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        };
        let headers = vec![("If-None-Match".to_string(), "*".to_string())];
        let signed = sign(
            Signable {
                method: "PUT",
                host: "bucket.s3.eu-central-1.amazonaws.com",
                path: "/prefix/head.json",
                query: "",
                headers: &headers,
                payload_hash: &sha256_hex(b"{}"),
            },
            &credentials,
            "eu-central-1",
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_440_938_160),
        );
        let authorization = signed
            .iter()
            .find(|(name, _)| name == "authorization")
            .map(|(_, value)| value.clone())
            .expect("an Authorization header");

        assert!(
            authorization
                .contains("SignedHeaders=host;if-none-match;x-amz-content-sha256;x-amz-date"),
            "the precondition has to be inside the signature: {authorization}"
        );
        assert!(
            !signed.iter().any(|(name, _)| name == "host"),
            "host comes from the URL; setting it twice is a client error"
        );
        assert!(
            !signed.iter().any(|(name, _)| name == "if-none-match"),
            "a header the caller already set is not returned again"
        );
    }

    #[test]
    fn a_session_token_is_signed_when_there_is_one() {
        let credentials = Credentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: Some("token".to_string()),
        };
        let signed = sign(
            Signable {
                method: "GET",
                host: "bucket.s3.amazonaws.com",
                path: "/head.json",
                query: "",
                headers: &[],
                payload_hash: &sha256_hex(b""),
            },
            &credentials,
            "us-east-1",
            std::time::UNIX_EPOCH,
        );
        assert!(signed
            .iter()
            .any(|(name, value)| name == "x-amz-security-token" && value == "token"));
        let authorization = &signed.last().expect("authorization").1;
        assert!(
            authorization.contains("x-amz-security-token"),
            "{authorization}"
        );
    }

    #[test]
    fn credentials_never_print_themselves() {
        let credentials = Credentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "hunter2".to_string(),
            session_token: Some("hunter3".to_string()),
        };
        let printed = format!("{credentials:?}");
        assert!(
            printed.contains("AKIDEXAMPLE"),
            "the key id is not a secret"
        );
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(!printed.contains("hunter3"), "{printed}");
    }
}
