//! WAL segment encoding and decoding (contract §4.4).
//!
//! The format is deliberately dull: UTF-8 JSONL, one `{"sql": "..."}` object
//! per line, newline-terminated, replayed in manifest order then line order.
//! Being dull is what lets four language bindings agree on it.
//!
//! Two limits are enforced on the write side and tolerated on the read side, as
//! the contract requires: a reader must be able to load anything a conforming
//! writer could have produced, so it refuses only what is over the *frozen*
//! ceiling, never a lower local preference.
//!
//! What this module does not do is make statements deterministic. `now()`,
//! `rand()` and reads of mutable external sources are replayed verbatim and
//! will produce whatever they produce; V1 promises ordered replay of the
//! original statement text and nothing more. Materialising those values before
//! calling `execute` is the caller's job.

use serde_json::{json, Value};

use super::errors::{err, Category, Error, Result};
use super::types::{MAX_SQL_BYTES, MAX_WAL_SEGMENT_BYTES};

/// One statement as a segment holds it, without the terminating newline.
fn encode_line(sql: &str) -> String {
    // serde_json does not escape `<`, `>` or `&`, so SQL containing them is
    // stored as written. A WAL line is the most likely thing an operator ever
    // reads by hand.
    json!({ "sql": sql }).to_string()
}

/// How many bytes a statement occupies in a segment, counting its newline.
///
/// This has to stay exactly consistent with [`encode_segment`]: it is what the
/// object layer budgets against before executing, and a budget that disagrees
/// with the encoder by even one byte per line puts the boundary in the wrong
/// place. A test asserts the two agree.
pub(crate) fn line_bytes(sql: &str) -> u64 {
    encode_line(sql).len() as u64 + 1
}

/// Refuses a statement that could not be written into a conforming segment.
pub(crate) fn assert_statement_within_limit(sql: &str) -> Result<()> {
    let size = sql.len() as u64;
    if size > MAX_SQL_BYTES {
        return Err(err(
            Category::LimitExceeded,
            format!(
                "durable: statement is {size} UTF-8 bytes, over the V1 per-statement limit of \
                 {MAX_SQL_BYTES}"
            ),
        )
        .with_limit(MAX_SQL_BYTES, size));
    }
    Ok(())
}

/// Encodes buffered statements into one segment.
///
/// It refuses an oversized segment rather than splitting: a segment boundary is
/// a commit boundary, so splitting silently would turn one caller-visible flush
/// into two, and a crash between them would commit a prefix the caller was
/// never told about.
pub(crate) fn encode_segment(statements: &[String]) -> Result<Vec<u8>> {
    let mut out = String::new();
    for sql in statements {
        assert_statement_within_limit(sql)?;
        out.push_str(&encode_line(sql));
        out.push('\n');
    }
    let size = out.len() as u64;
    if size > MAX_WAL_SEGMENT_BYTES {
        return Err(err(
            Category::LimitExceeded,
            format!(
                "durable: WAL segment would be {size} bytes, over the V1 limit of \
                 {MAX_WAL_SEGMENT_BYTES}; flush more often"
            ),
        )
        .with_limit(MAX_WAL_SEGMENT_BYTES, size));
    }
    Ok(out.into_bytes())
}

/// Decodes a verified segment into its statements.
///
/// Strict on every count the contract names: exactly one JSON object per line,
/// a string `sql`, and a terminating newline. A tolerant reader here would be
/// the worst kind of bug — it would skip a statement and hand back a database
/// that looks fine and is missing a write.
pub(crate) fn decode_segment(data: &[u8], key: &str) -> Result<Vec<String>> {
    let size = data.len() as u64;
    if size > MAX_WAL_SEGMENT_BYTES {
        return Err(err(
            Category::LimitExceeded,
            format!(
                "durable: WAL segment {key} is {size} bytes, over the V1 limit of \
                 {MAX_WAL_SEGMENT_BYTES}"
            ),
        )
        .with_limit(MAX_WAL_SEGMENT_BYTES, size));
    }
    if data.is_empty() {
        return Ok(Vec::new());
    }
    if data[data.len() - 1] != b'\n' {
        return Err(err(
            Category::Corrupt,
            format!("durable: WAL segment {key} does not end with a newline; it may be truncated"),
        ));
    }
    let text = std::str::from_utf8(&data[..data.len() - 1]).map_err(|e| {
        Error::wrap(
            Category::Corrupt,
            e,
            format!("durable: WAL segment {key} is not valid UTF-8"),
        )
    })?;

    let mut statements = Vec::new();
    for (index, line) in text.split('\n').enumerate() {
        let lineno = index + 1;
        let record: Value = serde_json::from_str(line).map_err(|e| {
            Error::wrap(
                Category::Corrupt,
                e,
                format!("durable: WAL segment {key} line {lineno} is not a JSON object"),
            )
        })?;
        let sql = record
            .as_object()
            .and_then(|record| record.get("sql"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                err(
                    Category::Corrupt,
                    format!("durable: WAL segment {key} line {lineno} has no string \"sql\" field"),
                )
            })?;
        // The per-statement ceiling is part of the frozen format, so it binds
        // the reader too. A conforming writer cannot produce a larger
        // statement, and the segment ceiling alone leaves room for a single
        // statement twice the allowed size.
        assert_statement_within_limit(sql)?;
        statements.push(sql.to_string());
    }
    Ok(statements)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn a_segment_round_trips_through_the_frozen_shape() -> Result<()> {
        let statements = vec![
            "INSERT INTO t VALUES (1)".to_string(),
            "INSERT INTO t VALUES ('a\nb')".to_string(),
            "ALTER TABLE t UPDATE n = 2 WHERE id < 5 AND tag = 'a&b'".to_string(),
        ];
        let segment = encode_segment(&statements)?;

        assert_eq!(segment.last(), Some(&b'\n'));
        assert_eq!(segment.iter().filter(|b| **b == b'\n').count(), 3);
        // Neither the embedded newline nor the ampersand may leak or be escaped
        // into a different spelling.
        let text = String::from_utf8(segment.clone()).unwrap();
        assert!(text.contains(r#"'a\nb'"#), "{text}");
        assert!(text.contains("a&b"), "{text}");

        assert_eq!(
            decode_segment(&segment, "wal/1-1-aaaaaaaa.jsonl")?,
            statements
        );
        Ok(())
    }

    #[test]
    fn the_budget_agrees_with_the_encoder_byte_for_byte() -> Result<()> {
        let statements = vec![
            "INSERT INTO t VALUES (1)".to_string(),
            "INSERT INTO t VALUES ('ünïcøde \" quoted')".to_string(),
        ];
        let budgeted: u64 = statements.iter().map(|sql| line_bytes(sql)).sum();
        assert_eq!(budgeted, encode_segment(&statements)?.len() as u64);
        Ok(())
    }

    #[test]
    fn an_empty_segment_replays_nothing() -> Result<()> {
        assert!(encode_segment(&[])?.is_empty());
        assert!(decode_segment(b"", "wal/1-1-aaaaaaaa.jsonl")?.is_empty());
        Ok(())
    }

    #[test]
    fn a_segment_that_does_not_parse_is_corrupt_rather_than_skipped() {
        for body in [
            &b"{\"sql\":\"SELECT 1\"}"[..], // no trailing newline
            &b"{\"sql\":\"SELECT 1\"}\nnot json\n"[..],
            &b"{\"statement\":\"SELECT 1\"}\n"[..], // wrong field
            &b"{\"sql\":42}\n"[..],                 // wrong type
            &b"[\"SELECT 1\"]\n"[..],               // not an object
        ] {
            let error = decode_segment(body, "wal/1-1-aaaaaaaa.jsonl")
                .expect_err("a segment that does not parse cannot be replayed");
            assert_eq!(error.category(), Category::Corrupt, "{body:?}");
        }
    }

    #[test]
    fn an_oversized_statement_is_refused_on_both_sides() {
        let huge = "x".repeat(MAX_SQL_BYTES as usize + 1);
        let error = assert_statement_within_limit(&huge).expect_err("over the per-statement limit");
        assert_eq!(error.category(), Category::LimitExceeded);
        assert_eq!(error.limit(), Some((MAX_SQL_BYTES, MAX_SQL_BYTES + 1)));

        let error = encode_segment(std::slice::from_ref(&huge)).expect_err("nor may it be encoded");
        assert_eq!(error.category(), Category::LimitExceeded);
    }
}
