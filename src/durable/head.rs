//! `head.json`: strict parsing and unknown-field-preserving write-back
//! (contract §4.2, §4.3, §4.5).
//!
//! The single most important thing this module does is *not* rebuild the head
//! from scratch on every write. A writer that constructs a fresh document each
//! time silently deletes every field it does not know about, which turns the
//! whole named-feature mechanism into a lie: a future revision's state would
//! survive exactly until an older writer touched the object. So the parsed raw
//! JSON travels alongside the typed view, and [`serialize_head`] patches the
//! known fields onto a copy of it.
//!
//! The second thing is that "preserve unknown fields" is not "be lenient".
//! Known fields are validated strictly — a wrong type is corrupt, not a
//! best-effort coercion — because a head that does not mean what it says is
//! more dangerous than one that fails to load.
//!
//! The lease and the manifest share one document on purpose: taking the lease,
//! publishing a WAL segment and replacing the base are all one conditional
//! replace, so a cold open costs a couple of round-trips and there is no window
//! in which the lease says one thing and the manifest another.

use serde_json::{Map, Number, Value};

use super::errors::{err, Category, Error, Result};
use super::keys::is_valid_object_key;
use super::types::{
    EngineIdentity, Head, Lease, Manifest, ObjectRef, Protocol, BACKUP_FORMAT_BASELINE,
    MAX_HEAD_BYTES, MAX_SAFE_INTEGER,
};

/// A head as read from the backend: the typed view, its CAS token, and the raw
/// JSON kept so unknown fields survive a write-back.
#[derive(Debug, Clone)]
pub(crate) struct HeadSnapshot {
    pub(crate) head: Head,
    pub(crate) etag: String,
    pub(crate) raw: Map<String, Value>,
}

fn corrupt(message: impl AsRef<str>) -> Error {
    err(
        Category::Corrupt,
        format!("durable: head.json {}", message.as_ref()),
    )
}

/// Reads a field that must be present.
///
/// A plain lookup cannot tell an absent property from an explicit null, and for
/// this document that distinction carries meaning: a released lease is
/// *explicitly* three nulls, while a lease missing those keys is a truncated
/// write. Treating the second as the first hands the object to a new writer on
/// the strength of a corrupt head.
fn required<'a>(object: &'a Map<String, Value>, key: &str, where_: &str) -> Result<&'a Value> {
    object
        .get(key)
        .ok_or_else(|| corrupt(format!("{where_} is required")))
}

fn as_object<'a>(value: &'a Value, where_: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| corrupt(format!("{where_} must be an object")))
}

fn safe_int(value: &Value, where_: &str) -> Result<u64> {
    value
        .as_u64()
        .filter(|n| *n <= MAX_SAFE_INTEGER)
        .ok_or_else(|| corrupt(format!("{where_} must be a non-negative safe integer")))
}

fn non_empty_string(value: &Value, where_: &str) -> Result<String> {
    match value.as_str() {
        Some(text) if !text.is_empty() => Ok(text.to_string()),
        _ => Err(corrupt(format!("{where_} must be a non-empty string"))),
    }
}

fn string_array(value: &Value, where_: &str) -> Result<Vec<String>> {
    let items = value
        .as_array()
        .ok_or_else(|| corrupt(format!("{where_} must be an array of strings")))?;
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| corrupt(format!("{where_} must be an array of strings")))
        })
        .collect()
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn parse_ref(value: &Value, where_: &str) -> Result<ObjectRef> {
    let object = as_object(value, where_)?;

    let key = required(object, "key", &format!("{where_}.key"))?;
    let key = key
        .as_str()
        .filter(|key| is_valid_object_key(key))
        .ok_or_else(|| {
            corrupt(format!(
                "{where_}.key is not a valid relative object key: {key}"
            ))
        })?;

    let size = safe_int(
        required(object, "size", &format!("{where_}.size"))?,
        &format!("{where_}.size"),
    )?;

    let sha256 = required(object, "sha256", &format!("{where_}.sha256"))?;
    let sha256 = sha256
        .as_str()
        .filter(|digest| is_sha256(digest))
        .ok_or_else(|| {
            corrupt(format!(
                "{where_}.sha256 must be 64 lowercase hex characters"
            ))
        })?;

    Ok(ObjectRef {
        key: key.to_string(),
        size,
        sha256: sha256.to_string(),
    })
}

/// Reads the negotiation block.
///
/// A missing block reads as the V1 baseline with no features, so objects
/// written before the block existed stay readable; every other shape is
/// validated strictly. An absent key takes its default, while a key that is
/// *present* takes its value — including an explicit null, which then fails
/// validation. The defaults exist for older documents, not as a way to launder
/// bad data.
fn parse_protocol(value: Option<&Value>) -> Result<Protocol> {
    let Some(value) = value else {
        return Ok(Protocol::default());
    };
    let object = as_object(value, "protocol")?;
    let mut out = Protocol::default();
    if let Some(version) = object.get("version") {
        out.version = safe_int(version, "protocol.version")?;
    }
    if let Some(features) = object.get("reader_features") {
        out.reader_features = string_array(features, "protocol.reader_features")?;
    }
    if let Some(features) = object.get("writer_features") {
        out.writer_features = string_array(features, "protocol.writer_features")?;
    }
    Ok(out)
}

/// Reads the engine identity.
///
/// The block itself has no default — a head that records no engine cannot
/// establish compatibility at all, so an absent one is a refusal rather than a
/// wildcard. The two compatibility fields do have defaults, and both are the
/// conservative reading of an object written before they existed:
///
/// * `backup_format` defaults to the V1 baseline, which is what such an object
///   necessarily used;
/// * `min_reader` defaults to `version`, which reproduces the old exact-match
///   behaviour's lower bound. Defaulting it to something older would
///   retroactively widen an object's audience on the strength of a field its
///   writer never wrote.
fn parse_engine(value: &Value) -> Result<EngineIdentity> {
    let object = as_object(value, "engine")?;
    let version = non_empty_string(
        required(object, "version", "engine.version")?,
        "engine.version",
    )?;
    let name = non_empty_string(required(object, "name", "engine.name")?, "engine.name")?;

    let backup_format = match object.get("backup_format") {
        Some(value) => safe_int(value, "engine.backup_format")?,
        None => BACKUP_FORMAT_BASELINE,
    };
    let min_reader = match object.get("min_reader") {
        Some(value) => non_empty_string(value, "engine.min_reader")?,
        None => version.clone(),
    };

    Ok(EngineIdentity {
        name,
        version,
        backup_format,
        min_reader,
    })
}

/// Reads the writer lease.
///
/// The lease is either fully held or fully released. A partial form — an owner
/// with no expiry, an expiry with no instance — is rejected rather than
/// normalised, because each half implies a different answer to "may I take this
/// over", and guessing is how two writers end up believing they are the one
/// writer.
fn parse_lease(value: &Value) -> Result<Lease> {
    let object = as_object(value, "lease")?;
    let generation = safe_int(
        required(object, "generation", "lease.generation")?,
        "lease.generation",
    )?;
    let owner = required(object, "owner", "lease.owner")?;
    let instance = required(object, "instance", "lease.instance")?;
    let expires_at = required(object, "expires_at", "lease.expires_at")?;

    if owner.is_null() && instance.is_null() && expires_at.is_null() {
        return Ok(Lease::released(generation));
    }

    match (owner.as_str(), instance.as_str(), expires_at.as_f64()) {
        (Some(owner), Some(instance), Some(expires_at)) if expires_at.is_finite() => Ok(Lease {
            generation,
            owner: Some(owner.to_string()),
            instance: Some(instance.to_string()),
            expires_at: Some(expires_at),
        }),
        (_, _, Some(_)) | (_, _, None) => Err(corrupt(
            "lease must be either fully released (owner, instance and expires_at all null) or \
             fully held (owner and instance strings, expires_at a finite number)",
        )),
    }
}

fn parse_manifest(value: &Value) -> Result<Manifest> {
    let object = as_object(value, "manifest")?;
    let db = non_empty_string(required(object, "db", "manifest.db")?, "manifest.db")?;

    // base is the only field the protocol allows to be null; the rest are
    // required and may not be, so an absent or nulled wal is corrupt rather
    // than an empty replay list.
    let base = match required(object, "base", "manifest.base")? {
        Value::Null => None,
        value => Some(parse_ref(value, "manifest.base")?),
    };

    let items = required(object, "wal", "manifest.wal")?
        .as_array()
        .ok_or_else(|| corrupt("manifest.wal must be an array"))?;
    let wal = items
        .iter()
        .enumerate()
        .map(|(index, item)| parse_ref(item, &format!("manifest.wal[{index}]")))
        .collect::<Result<Vec<_>>>()?;

    let seq = safe_int(required(object, "seq", "manifest.seq")?, "manifest.seq")?;

    Ok(Manifest { db, base, wal, seq })
}

/// Validates head bytes strictly and keeps the raw JSON for round-trip.
pub(crate) fn parse_head(data: &[u8]) -> Result<(Head, Map<String, Value>)> {
    let size = data.len() as u64;
    if size > MAX_HEAD_BYTES {
        return Err(err(
            Category::LimitExceeded,
            format!("durable: head.json is {size} bytes, over the V1 limit of {MAX_HEAD_BYTES}"),
        )
        .with_limit(MAX_HEAD_BYTES, size));
    }
    // The protocol says UTF-8. Bytes that are not UTF-8 are corrupt rather than
    // silently repaired: replacing a malformed byte with U+FFFD would let
    // invalid data inside an *unknown* field parse cleanly and then be written
    // back mangled.
    let text = std::str::from_utf8(data).map_err(|e| {
        Error::wrap(
            Category::Corrupt,
            e,
            "durable: head.json is not valid UTF-8",
        )
    })?;

    let raw: Value = serde_json::from_str(text)
        .map_err(|e| Error::wrap(Category::Corrupt, e, "durable: head.json is not valid JSON"))?;
    let raw = raw
        .as_object()
        .ok_or_else(|| corrupt("must be a JSON object"))?
        .clone();

    let head = Head {
        protocol: parse_protocol(raw.get("protocol"))?,
        engine: parse_engine(required(&raw, "engine", "engine")?)?,
        lease: parse_lease(required(&raw, "lease", "lease")?)?,
        manifest: parse_manifest(required(&raw, "manifest", "manifest")?)?,
    };
    Ok((head, raw))
}

/// Patches known fields onto whatever is stored under `key`, so an unrecognised
/// sibling inside `protocol`, `engine`, `lease` or `manifest` survives.
fn merge_into(base: &mut Map<String, Value>, key: &str, known: Map<String, Value>) {
    match base.get_mut(key).and_then(Value::as_object_mut) {
        Some(existing) => {
            for (field, value) in known {
                existing.insert(field, value);
            }
        }
        None => {
            base.insert(key.to_string(), Value::Object(known));
        }
    }
}

fn ref_to_json(reference: &ObjectRef) -> Value {
    Value::Object(Map::from_iter([
        ("key".to_string(), Value::String(reference.key.clone())),
        ("size".to_string(), Value::Number(reference.size.into())),
        (
            "sha256".to_string(),
            Value::String(reference.sha256.clone()),
        ),
    ]))
}

/// Renders `head`, preserving every unrecognised field of `raw`.
///
/// `manifest.base` and `manifest.wal` are replaced wholesale rather than
/// merged: they are this build's own state, and a stale unknown key inside a
/// reference being rewritten would describe bytes that are no longer there.
pub(crate) fn serialize_head(head: &Head, raw: Option<&Map<String, Value>>) -> Result<Vec<u8>> {
    // Refuse to emit a document this parser would reject. Writing one is worse
    // than failing here: head.json is created with a conditional create and V1
    // has no destroy, so an object published with, say, an empty manifest.db is
    // corrupt on every subsequent open and cannot be removed.
    for (field, value) in [
        ("manifest.db", &head.manifest.db),
        ("engine.name", &head.engine.name),
        ("engine.version", &head.engine.version),
        ("engine.min_reader", &head.engine.min_reader),
    ] {
        if value.is_empty() {
            return Err(corrupt(format!("{field} must be a non-empty string")));
        }
    }

    let mut base = raw.cloned().unwrap_or_default();

    merge_into(
        &mut base,
        "protocol",
        Map::from_iter([
            (
                "version".to_string(),
                Value::Number(head.protocol.version.into()),
            ),
            (
                "reader_features".to_string(),
                Value::Array(
                    head.protocol
                        .reader_features
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            ),
            (
                "writer_features".to_string(),
                Value::Array(
                    head.protocol
                        .writer_features
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            ),
        ]),
    );

    merge_into(
        &mut base,
        "engine",
        Map::from_iter([
            ("name".to_string(), Value::String(head.engine.name.clone())),
            (
                "version".to_string(),
                Value::String(head.engine.version.clone()),
            ),
            (
                "backup_format".to_string(),
                Value::Number(head.engine.backup_format.into()),
            ),
            (
                "min_reader".to_string(),
                Value::String(head.engine.min_reader.clone()),
            ),
        ]),
    );

    let expires_at = match head.lease.expires_at {
        Some(seconds) => Value::Number(
            Number::from_f64(seconds).ok_or_else(|| corrupt("lease.expires_at must be finite"))?,
        ),
        None => Value::Null,
    };
    merge_into(
        &mut base,
        "lease",
        Map::from_iter([
            (
                "generation".to_string(),
                Value::Number(head.lease.generation.into()),
            ),
            (
                "owner".to_string(),
                head.lease.owner.clone().map_or(Value::Null, Value::String),
            ),
            (
                "instance".to_string(),
                head.lease
                    .instance
                    .clone()
                    .map_or(Value::Null, Value::String),
            ),
            ("expires_at".to_string(), expires_at),
        ]),
    );

    merge_into(
        &mut base,
        "manifest",
        Map::from_iter([
            ("db".to_string(), Value::String(head.manifest.db.clone())),
            (
                "base".to_string(),
                head.manifest.base.as_ref().map_or(Value::Null, ref_to_json),
            ),
            (
                "wal".to_string(),
                Value::Array(head.manifest.wal.iter().map(ref_to_json).collect()),
            ),
            ("seq".to_string(), Value::Number(head.manifest.seq.into())),
        ]),
    );

    let body = serde_json::to_vec(&Value::Object(base))
        .map_err(|e| Error::wrap(Category::Corrupt, e, "durable: head.json cannot be encoded"))?;
    let size = body.len() as u64;
    if size > MAX_HEAD_BYTES {
        return Err(err(
            Category::LimitExceeded,
            format!(
                "durable: head.json would be {size} bytes, over the V1 limit of {MAX_HEAD_BYTES}; \
                 checkpoint to truncate the WAL list"
            ),
        )
        .with_limit(MAX_HEAD_BYTES, size));
    }
    Ok(body)
}

/// The head a cold object is created with, held by the creating writer.
pub(crate) fn cold_head(db: &str, engine_version: &str, backup_format: u64, lease: Lease) -> Head {
    let mut head = Head::cold(db, engine_version, backup_format);
    head.lease = lease;
    head
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::durable::types::PROTOCOL_VERSION;

    const DOCUMENT: &str = r#"{
      "protocol": {"version": 1, "reader_features": [], "writer_features": []},
      "engine": {"name": "chdb", "version": "26.7.2-rc.2", "backup_format": 1,
                 "min_reader": "26.7.2-rc.2"},
      "lease": {"generation": 3, "owner": "worker-1", "instance": "abc", "expires_at": 1788230400.0},
      "manifest": {
        "db": "mem",
        "base": {"key": "checkpoints/3-8-acde1234.tar.gz", "size": 1048576,
                 "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
        "wal": [{"key": "wal/3-9-acde5678.jsonl", "size": 127,
                 "sha256": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}],
        "seq": 9
      }
    }"#;

    #[test]
    fn the_contracts_example_document_parses_to_what_it_says() -> Result<()> {
        let (head, _) = parse_head(DOCUMENT.as_bytes())?;
        assert_eq!(head.protocol.version, PROTOCOL_VERSION);
        assert_eq!(head.engine.name, "chdb");
        assert_eq!(head.engine.min_reader, "26.7.2-rc.2");
        assert_eq!(head.lease.generation, 3);
        assert_eq!(head.lease.owner.as_deref(), Some("worker-1"));
        assert_eq!(head.manifest.db, "mem");
        assert_eq!(head.manifest.seq, 9);
        assert_eq!(head.manifest.wal.len(), 1);
        assert_eq!(head.manifest.base.unwrap().size, 1048576);
        Ok(())
    }

    #[test]
    fn a_field_this_build_does_not_know_survives_a_write_back() -> Result<()> {
        let document = DOCUMENT.replace(
            r#""seq": 9"#,
            r#""seq": 9, "compaction": {"generation": 4, "policy": "tiered"}"#,
        );
        let document = document.replace(
            r#""protocol": {"#,
            r#""tenancy": {"shard": 7}, "protocol": {"#,
        );

        let (head, raw) = parse_head(document.as_bytes())?;
        let written = serialize_head(&head, Some(&raw))?;
        let round_tripped: Value = serde_json::from_slice(&written).unwrap();

        assert_eq!(round_tripped["tenancy"]["shard"], 7);
        assert_eq!(round_tripped["manifest"]["compaction"]["policy"], "tiered");
        // And the known fields still say what they said.
        assert_eq!(round_tripped["manifest"]["seq"], 9);
        assert_eq!(round_tripped["lease"]["owner"], "worker-1");
        Ok(())
    }

    #[test]
    fn key_order_and_whitespace_are_not_part_of_the_format() -> Result<()> {
        let compact = r#"{"manifest":{"seq":0,"wal":[],"base":null,"db":"mem"},
            "lease":{"expires_at":null,"instance":null,"owner":null,"generation":1},
            "engine":{"min_reader":"26.7.2","backup_format":1,"version":"26.7.2","name":"chdb"},
            "protocol":{"writer_features":[],"reader_features":[],"version":1}}"#;
        let (head, _) = parse_head(compact.as_bytes())?;
        assert_eq!(head.manifest.db, "mem");
        assert!(!head.lease.is_held());
        Ok(())
    }

    #[test]
    fn a_half_released_lease_is_corrupt_rather_than_guessed_at() {
        for lease in [
            r#"{"generation": 1, "owner": "w", "instance": null, "expires_at": null}"#,
            r#"{"generation": 1, "owner": null, "instance": null, "expires_at": 123.0}"#,
            r#"{"generation": 1, "owner": "w", "instance": "i"}"#,
            r#"{"generation": 1, "owner": "w", "instance": "i", "expires_at": "soon"}"#,
        ] {
            let document = DOCUMENT.replace(
                r#""lease": {"generation": 3, "owner": "worker-1", "instance": "abc", "expires_at": 1788230400.0}"#,
                &format!(r#""lease": {lease}"#),
            );
            let error = parse_head(document.as_bytes()).expect_err("a partial lease is corrupt");
            assert_eq!(error.category(), Category::Corrupt, "{lease}");
        }
    }

    #[test]
    fn a_reference_that_could_leave_the_object_is_corrupt() {
        let document = DOCUMENT.replace("checkpoints/3-8-acde1234.tar.gz", "../elsewhere.tar.gz");
        let error = parse_head(document.as_bytes()).expect_err("a traversal is not a key");
        assert_eq!(error.category(), Category::Corrupt);
    }

    #[test]
    fn a_reference_without_its_checksum_is_corrupt() {
        for broken in [
            r#"{"key": "wal/1-1-aaaaaaaa.jsonl", "size": 1}"#,
            r#"{"key": "wal/1-1-aaaaaaaa.jsonl", "sha256": "0123"}"#,
            r#"{"key": "wal/1-1-aaaaaaaa.jsonl", "size": 1, "sha256": "NOTHEX"}"#,
            r#"{"key": "wal/1-1-aaaaaaaa.jsonl", "size": -1, "sha256": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}"#,
        ] {
            let document = DOCUMENT.replace(
                r#"{"key": "wal/3-9-acde5678.jsonl", "size": 127,
                 "sha256": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}"#,
                broken,
            );
            let error = parse_head(document.as_bytes()).expect_err("an unverifiable reference");
            assert_eq!(error.category(), Category::Corrupt, "{broken}");
        }
    }

    #[test]
    fn a_missing_engine_block_cannot_establish_compatibility() {
        let document = DOCUMENT.replace(r#""engine":"#, r#""engine_was":"#);
        let error = parse_head(document.as_bytes()).expect_err("no engine, no gate");
        assert_eq!(error.category(), Category::Corrupt);
    }

    #[test]
    fn an_oversized_head_is_a_limit_rather_than_a_parse_failure() {
        let padding = "x".repeat(MAX_HEAD_BYTES as usize);
        let document = format!(r#"{{"pad": "{padding}", "engine": {{}}}}"#);
        let error = parse_head(document.as_bytes()).expect_err("over the head limit");
        assert_eq!(error.category(), Category::LimitExceeded);
    }

    #[test]
    fn a_document_this_parser_would_reject_is_never_written() {
        let mut head = Head::cold("mem", "26.7.2", BACKUP_FORMAT_BASELINE);
        head.manifest.db = String::new();
        let error = serialize_head(&head, None).expect_err("an empty database name");
        assert_eq!(error.category(), Category::Corrupt);
    }

    #[test]
    fn what_this_build_writes_it_can_read_back() -> Result<()> {
        let head = Head::cold("mem", "26.7.2", BACKUP_FORMAT_BASELINE);
        let (round_tripped, _) = parse_head(&serialize_head(&head, None)?)?;
        assert_eq!(round_tripped, head);
        Ok(())
    }
}
