//! ClickHouse query parameter values for `{name:Type}` placeholders.
//!
//! Values are encoded as strings for the libchdb C API. The SQL still names the
//! ClickHouse type; this crate does not interpolate values into the query text.
//! After chDB receives the encoded strings, it parses them and substitutes them
//! during planning.
//!
//! ```no_run
//! use chdb_rust::execute_with_params;
//!
//! let result = execute_with_params("SELECT {x:UInt64} AS v", None, [("x", 7_u64)])?;
//! # Ok::<(), chdb_rust::error::Error>(())
//! ```

use std::borrow::Cow;
use std::ffi::c_char;

use crate::error::Result;

/// Builder for mixed-type parameter maps.
///
/// An iterator of `(name, value)` pairs is enough when every value converts to
/// the same [`QueryParam`] type. Use this builder when the types differ, or when
/// you want to assemble the map incrementally. Binding the same name twice keeps
/// the later value.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::connection::Connection;
/// use chdb_rust::format::OutputFormat;
/// use chdb_rust::query_param::QueryParams;
///
/// let conn = Connection::open_in_memory()?;
/// let params = QueryParams::new()
///     .bind("x", 5_u64)
///     .bind("label", "ok");
/// let _ = conn.query_with_params(
///     "SELECT {x:UInt64} AS x, {label:String} AS label",
///     OutputFormat::CSV,
///     params,
/// )?;
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
#[derive(Debug, Default, Clone)]
pub struct QueryParams {
    pairs: Vec<(String, QueryParam)>,
}

impl QueryParams {
    /// An empty parameter map.
    ///
    /// Passing this to a query that still has `{name:Type}` placeholders fails
    /// with a substitution error. A query with no placeholders runs as a plain
    /// query.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a named value and return the map for further binds.
    ///
    /// `name` is the identifier inside `{name:Type}`. A second bind of the same
    /// name replaces the earlier value at query time (last write wins).
    pub fn bind(mut self, name: impl AsRef<str>, value: impl Into<QueryParam>) -> Self {
        self.pairs.push((name.as_ref().to_owned(), value.into()));
        self
    }
}

/// Build a parameter map from an array literal.
///
/// Lets call sites pass `[("x", 41_u64)]` directly wherever a method takes
/// `impl Into<QueryParams>`, without naming the builder.
impl<K, V, const N: usize> From<[(K, V); N]> for QueryParams
where
    K: AsRef<str>,
    V: Into<QueryParam>,
{
    fn from(pairs: [(K, V); N]) -> Self {
        let mut params = Self::new();
        for (name, value) in pairs {
            params = params.bind(name, value);
        }
        params
    }
}

/// Build a parameter map from an owned vector of pairs.
impl<K, V> From<Vec<(K, V)>> for QueryParams
where
    K: AsRef<str>,
    V: Into<QueryParam>,
{
    fn from(pairs: Vec<(K, V)>) -> Self {
        let mut params = Self::new();
        for (name, value) in pairs {
            params = params.bind(name, value);
        }
        params
    }
}

impl IntoIterator for QueryParams {
    type Item = (String, QueryParam);
    type IntoIter = std::vec::IntoIter<(String, QueryParam)>;

    fn into_iter(self) -> Self::IntoIter {
        self.pairs.into_iter()
    }
}

impl<'a> IntoIterator for &'a QueryParams {
    type Item = (&'a str, &'a QueryParam);
    type IntoIter = std::iter::Map<
        std::slice::Iter<'a, (String, QueryParam)>,
        fn(&'a (String, QueryParam)) -> (&'a str, &'a QueryParam),
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.pairs
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }
}

/// A value bound to a `{name:Type}` placeholder in a parameterized query.
///
/// Use [`From`] conversions for scalars, strings, options, and arrays. For pre-formatted
/// ClickHouse literals (tuples, etc.), use [`Self::raw`]. The type in the SQL
/// placeholder is what the engine parses; these variants only choose the string
/// encoding sent over the C API.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryParam {
    /// SQL NULL for `Nullable(...)` placeholders (`\N`).
    Null,
    /// ClickHouse `Bool`, encoded as `true` or `false`.
    Bool(bool),
    /// Signed 64-bit integer. Wider integers should go through [`Self::raw`].
    Int64(i64),
    /// Unsigned 64-bit integer.
    UInt64(u64),
    /// 64-bit floating point.
    Float64(f64),
    /// Raw text for `String`, `Date`, `Identifier`, and similar placeholders.
    Text(String),
    /// Pre-formatted ClickHouse literal (e.g. `(7,'x')`, `[1, 2, 3]`).
    Raw(String),
    /// ClickHouse `Array(...)` value, encoded as `[...]`.
    Array(Vec<QueryParam>),
}

impl QueryParam {
    /// Pass a pre-formatted ClickHouse parameter literal through unchanged.
    ///
    /// The string is sent as-is. It must already be valid for the placeholder
    /// type (`(7,'x')` for a tuple, `[1, 2, 3]` for an array).
    pub fn raw(value: impl Into<String>) -> Self {
        Self::Raw(value.into())
    }

    /// Encode this value as a root-level ClickHouse query parameter string.
    ///
    /// Root strings are unquoted. Nested values inside [`Self::Array`] use
    /// nested rules (quoted strings, `NULL`).
    pub fn encode(&self) -> Result<Cow<'_, str>> {
        Ok(match self {
            Self::Null => Cow::Borrowed("\\N"),
            Self::Bool(true) => Cow::Borrowed("true"),
            Self::Bool(false) => Cow::Borrowed("false"),
            Self::Int64(value) => Cow::Owned(value.to_string()),
            Self::UInt64(value) => Cow::Owned(value.to_string()),
            Self::Float64(value) => Cow::Owned(value.to_string()),
            Self::Text(value) => Self::encode_text(value),
            Self::Raw(value) => Cow::Borrowed(value),
            Self::Array(values) => Cow::Owned(Self::encode_array(values)?),
        })
    }

    /// Escape `\`, tab, and newline so `deserializeTextEscaped` reconstructs `value`.
    fn encode_text(value: &str) -> Cow<'_, str> {
        let mut encoded = None;
        let mut last = 0;

        for (i, ch) in value.char_indices() {
            let esc = match ch {
                '\\' => r"\\",
                '\t' => r"\t",
                '\n' => r"\n",
                _ => continue,
            };
            let buf = encoded.get_or_insert_with(|| String::with_capacity(value.len()));
            buf.push_str(&value[last..i]);
            buf.push_str(esc);
            last = i + ch.len_utf8();
        }

        match encoded {
            None => Cow::Borrowed(value),
            Some(mut buf) => {
                buf.push_str(&value[last..]);
                Cow::Owned(buf)
            }
        }
    }

    fn encode_array(values: &[QueryParam]) -> Result<String> {
        let mut parts = Vec::with_capacity(values.len());
        for value in values {
            parts.push(value.encode_nested()?.into_owned());
        }
        Ok(format!("[{}]", parts.join(", ")))
    }

    /// Encode this value as an element inside an array/tuple/map literal.
    fn encode_nested(&self) -> Result<Cow<'_, str>> {
        Ok(match self {
            Self::Null => Cow::Borrowed("NULL"),
            Self::Bool(true) => Cow::Borrowed("true"),
            Self::Bool(false) => Cow::Borrowed("false"),
            Self::Int64(value) => Cow::Owned(value.to_string()),
            Self::UInt64(value) => Cow::Owned(value.to_string()),
            Self::Float64(value) => Cow::Owned(value.to_string()),
            Self::Text(value) => Cow::Owned(Self::quote_nested_string(value)),
            Self::Raw(value) => Cow::Borrowed(value),
            Self::Array(values) => Cow::Owned(Self::encode_array(values)?),
        })
    }

    /// Quote `value` as an array or tuple element, escaping `\`, tab, newline, and `'`.
    fn quote_nested_string(value: &str) -> String {
        let mut encoded = String::with_capacity(value.len() + 2);
        encoded.push('\'');
        for ch in value.chars() {
            match ch {
                '\\' => encoded.push_str(r"\\"),
                '\t' => encoded.push_str(r"\t"),
                '\n' => encoded.push_str(r"\n"),
                '\'' => encoded.push_str(r"\'"),
                other => encoded.push(other),
            }
        }
        encoded.push('\'');
        encoded
    }
}

/// Name/value buffers and parallel length arrays for `chdb_*_with_params_n`.
///
/// Pointers returned by [`Self::names_ptr`] / [`Self::values_ptr`] and the
/// length arrays from [`Self::name_lens_ptr`] / [`Self::value_lens_ptr`] are
/// valid only while this struct remains alive. Callers must keep `EncodedParams`
/// live across the FFI call that consumes those pointers. The chDB C API is
/// assumed to copy parameter names and values during that call and not retain
/// the pointers.
pub(crate) struct EncodedParams {
    _name_bufs: Vec<Vec<u8>>,
    _value_bufs: Vec<Vec<u8>>,
    name_ptrs: Vec<*const c_char>,
    value_ptrs: Vec<*const c_char>,
    name_lens: Vec<usize>,
    value_lens: Vec<usize>,
}

impl EncodedParams {
    pub(crate) fn encode(params: impl Into<QueryParams>) -> Result<Self> {
        let params = params.into();
        let mut name_bufs = Vec::new();
        let mut value_bufs = Vec::new();

        for (name, param) in params {
            let encoded = param.encode()?;
            name_bufs.push(name.into_bytes());
            value_bufs.push(encoded.as_ref().as_bytes().to_vec());
        }

        let name_lens: Vec<usize> = name_bufs.iter().map(|b| b.len()).collect();
        let value_lens: Vec<usize> = value_bufs.iter().map(|b| b.len()).collect();
        let name_ptrs = name_bufs
            .iter()
            .map(|b| b.as_ptr() as *const c_char)
            .collect();
        let value_ptrs = value_bufs
            .iter()
            .map(|b| b.as_ptr() as *const c_char)
            .collect();

        Ok(Self {
            _name_bufs: name_bufs,
            _value_bufs: value_bufs,
            name_ptrs,
            value_ptrs,
            name_lens,
            value_lens,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.name_ptrs.len()
    }

    /// Pointer to the parallel name C-string array, or null when empty.
    pub(crate) fn names_ptr(&self) -> *const *const c_char {
        if self.name_ptrs.is_empty() {
            std::ptr::null()
        } else {
            self.name_ptrs.as_ptr()
        }
    }

    /// Pointer to the parallel value buffer array, or null when empty.
    pub(crate) fn values_ptr(&self) -> *const *const c_char {
        if self.value_ptrs.is_empty() {
            std::ptr::null()
        } else {
            self.value_ptrs.as_ptr()
        }
    }

    /// Pointer to the parallel name length array, or null when empty.
    pub(crate) fn name_lens_ptr(&self) -> *const usize {
        if self.name_lens.is_empty() {
            std::ptr::null()
        } else {
            self.name_lens.as_ptr()
        }
    }

    /// Pointer to the parallel value length array, or null when empty.
    pub(crate) fn value_lens_ptr(&self) -> *const usize {
        if self.value_lens.is_empty() {
            std::ptr::null()
        } else {
            self.value_lens.as_ptr()
        }
    }
}

impl From<bool> for QueryParam {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i8> for QueryParam {
    fn from(value: i8) -> Self {
        Self::Int64(i64::from(value))
    }
}

impl From<i16> for QueryParam {
    fn from(value: i16) -> Self {
        Self::Int64(i64::from(value))
    }
}

impl From<i32> for QueryParam {
    fn from(value: i32) -> Self {
        Self::Int64(i64::from(value))
    }
}

impl From<i64> for QueryParam {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<u8> for QueryParam {
    fn from(value: u8) -> Self {
        Self::UInt64(u64::from(value))
    }
}

impl From<u16> for QueryParam {
    fn from(value: u16) -> Self {
        Self::UInt64(u64::from(value))
    }
}

impl From<u32> for QueryParam {
    fn from(value: u32) -> Self {
        Self::UInt64(u64::from(value))
    }
}

impl From<u64> for QueryParam {
    fn from(value: u64) -> Self {
        Self::UInt64(value)
    }
}

impl From<f32> for QueryParam {
    fn from(value: f32) -> Self {
        Self::Float64(f64::from(value))
    }
}

impl From<f64> for QueryParam {
    fn from(value: f64) -> Self {
        Self::Float64(value)
    }
}

impl From<String> for QueryParam {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for QueryParam {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<&QueryParam> for QueryParam {
    fn from(value: &QueryParam) -> Self {
        value.clone()
    }
}

impl<T> From<Option<T>> for QueryParam
where
    T: Into<QueryParam>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            None => Self::Null,
            Some(value) => value.into(),
        }
    }
}

impl<T> From<Vec<T>> for QueryParam
where
    T: Into<QueryParam>,
{
    fn from(values: Vec<T>) -> Self {
        Self::Array(values.into_iter().map(Into::into).collect())
    }
}

impl<T> From<&[T]> for QueryParam
where
    T: Into<QueryParam> + Clone,
{
    fn from(values: &[T]) -> Self {
        Self::from(values.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_root_covers_every_variant() -> Result<()> {
        assert_eq!(QueryParam::Null.encode()?, "\\N");
        assert_eq!(QueryParam::Bool(true).encode()?, "true");
        assert_eq!(QueryParam::Bool(false).encode()?, "false");
        assert_eq!(QueryParam::Int64(-42).encode()?, "-42");
        assert_eq!(QueryParam::UInt64(42).encode()?, "42");
        assert_eq!(QueryParam::Float64(1.5).encode()?, "1.5");
        assert_eq!(QueryParam::Text(String::new()).encode()?, "");
        assert_eq!(QueryParam::Text("hello".into()).encode()?, "hello");
        assert_eq!(QueryParam::Text("it's".into()).encode()?, "it's");
        assert_eq!(QueryParam::Text(r"a\b".into()).encode()?, r"a\\b");
        assert_eq!(QueryParam::raw("").encode()?, "");
        assert_eq!(QueryParam::raw("(7,'x')").encode()?, "(7,'x')");
        assert_eq!(QueryParam::raw("[1, 2, 3]").encode()?, "[1, 2, 3]");
        assert_eq!(QueryParam::Array(vec![]).encode()?, "[]");
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Int64(1), QueryParam::Int64(2)]).encode()?,
            "[1, 2]"
        );
        Ok(())
    }

    #[test]
    fn encode_text_escapes_sequences_so_chdb_sees_a_literal() -> Result<()> {
        assert_eq!(QueryParam::Text(r"C:\temp".into()).encode()?, r"C:\\temp");
        assert_eq!(QueryParam::Text(r"\".into()).encode()?, r"\\");
        assert_eq!(QueryParam::Text(r"\\".into()).encode()?, r"\\\\");
        assert_eq!(QueryParam::Text("a\tb".into()).encode()?, r"a\tb");
        assert_eq!(QueryParam::Text("a\nb".into()).encode()?, r"a\nb");
        assert_eq!(QueryParam::Text("it's".into()).encode()?, "it's");
        assert_eq!(QueryParam::Text("a\rb".into()).encode()?, "a\rb");
        assert_eq!(QueryParam::Text("a\u{08}b".into()).encode()?, "a\u{08}b");
        assert_eq!(QueryParam::Text("a\u{0c}b".into()).encode()?, "a\u{0c}b");
        assert_eq!(QueryParam::Text("a\0b".into()).encode()?, "a\0b");
        assert_eq!(QueryParam::Text("hello".into()).encode()?, "hello");
        Ok(())
    }

    #[test]
    fn encode_raw_passes_escape_sequences_through_unchanged() -> Result<()> {
        assert_eq!(QueryParam::raw(r"C:\temp").encode()?, r"C:\temp");
        assert_eq!(QueryParam::raw("a\tb").encode()?, "a\tb");
        assert_eq!(QueryParam::raw(r"it\'s").encode()?, r"it\'s");
        Ok(())
    }

    #[test]
    fn encode_nested_covers_every_variant_via_array() -> Result<()> {
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Null]).encode()?,
            "[NULL]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Bool(true), QueryParam::Bool(false)]).encode()?,
            "[true, false]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Int64(-1), QueryParam::UInt64(2)]).encode()?,
            "[-1, 2]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Float64(1.5)]).encode()?,
            "[1.5]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text(String::new())]).encode()?,
            "['']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text("hello".into())]).encode()?,
            "['hello']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::raw("(7,'x')")]).encode()?,
            "[(7,'x')]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Array(vec![
                QueryParam::Int64(1),
                QueryParam::Int64(2),
            ])])
            .encode()?,
            "[[1, 2]]"
        );
        Ok(())
    }

    #[test]
    fn encode_nested_escapes_quotes_and_backslashes_in_text() -> Result<()> {
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text("'".into())]).encode()?,
            r"['\'']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text(r"\".into())]).encode()?,
            r"['\\']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text("it's".into())]).encode()?,
            r"['it\'s']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text(r"a\b".into())]).encode()?,
            r"['a\\b']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text(r"'\".into())]).encode()?,
            r"['\'\\']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text(r"C:\temp".into())]).encode()?,
            r"['C:\\temp']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text("a\tb".into())]).encode()?,
            r"['a\tb']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text("a\nb".into())]).encode()?,
            r"['a\nb']"
        );
        assert_eq!(
            QueryParam::Array(vec![
                QueryParam::Text("a".into()),
                QueryParam::Text("b".into()),
            ])
            .encode()?,
            "['a', 'b']"
        );
        Ok(())
    }

    #[test]
    fn encode_array_mixed_and_nested_shapes() -> Result<()> {
        assert_eq!(
            QueryParam::Array(vec![
                QueryParam::Int64(1),
                QueryParam::Null,
                QueryParam::Text("x".into()),
                QueryParam::Bool(true),
            ])
            .encode()?,
            "[1, NULL, 'x', true]"
        );
        assert_eq!(
            QueryParam::Array(vec![
                QueryParam::Array(vec![QueryParam::Int64(1), QueryParam::Int64(2)]),
                QueryParam::Array(vec![QueryParam::Int64(3), QueryParam::Int64(4)]),
            ])
            .encode()?,
            "[[1, 2], [3, 4]]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Array(vec![QueryParam::Text(
                "it's".into()
            )])])
            .encode()?,
            r"[['it\'s']]"
        );
        Ok(())
    }

    #[test]
    fn from_option_none_encodes_as_root_null() -> Result<()> {
        let value: Option<i64> = None;
        assert_eq!(QueryParam::from(value).encode()?, "\\N");
        Ok(())
    }

    #[test]
    fn from_vec_and_slice_encode_as_arrays() -> Result<()> {
        assert_eq!(QueryParam::from(Vec::<u64>::new()).encode()?, "[]");
        assert_eq!(QueryParam::from(vec![1_u64, 2, 3]).encode()?, "[1, 2, 3]");
        assert_eq!(QueryParam::from(vec![1_i32, 2, 3]).encode()?, "[1, 2, 3]");
        assert_eq!(
            QueryParam::from([4_i64, 5, 6].as_slice()).encode()?,
            "[4, 5, 6]"
        );
        assert_eq!(
            QueryParam::from(vec!["a".to_owned(), "b".to_owned()]).encode()?,
            "['a', 'b']"
        );
        let values: Vec<Option<i64>> = vec![Some(1), None, Some(3)];
        assert_eq!(QueryParam::from(values).encode()?, "[1, NULL, 3]");
        assert_eq!(
            QueryParam::from(vec![vec![1_u64, 2], vec![3, 4]]).encode()?,
            "[[1, 2], [3, 4]]"
        );
        Ok(())
    }

    #[test]
    fn from_vec_builds_array_variant() {
        assert_eq!(
            QueryParam::from(vec![1_i64, 2]),
            QueryParam::Array(vec![QueryParam::Int64(1), QueryParam::Int64(2)])
        );
    }

    #[test]
    fn query_params_builder_collects_mixed_types() {
        let params = QueryParams::new().bind("x", 5_u64).bind("label", "ok");
        let collected: Vec<_> = params.into_iter().collect();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].0, "x");
        assert_eq!(collected[0].1, QueryParam::UInt64(5));
        assert_eq!(collected[1].0, "label");
        assert_eq!(collected[1].1, QueryParam::Text("ok".into()));
    }

    #[test]
    fn encoded_params_empty_exposes_null_name_and_value_ptrs() -> Result<()> {
        let encoded = EncodedParams::encode(QueryParams::new())?;

        assert_eq!(encoded.len(), 0);
        assert!(encoded.names_ptr().is_null());
        assert!(encoded.values_ptr().is_null());
        assert!(encoded.name_lens_ptr().is_null());
        assert!(encoded.value_lens_ptr().is_null());
        Ok(())
    }

    #[test]
    fn encoded_params_exposes_parallel_name_and_value_ptrs() -> Result<()> {
        let encoded = EncodedParams::encode([
            ("x", QueryParam::from(5_u64)),
            ("label", QueryParam::from("ok")),
        ])?;

        assert_eq!(encoded.len(), 2);
        assert!(!encoded.names_ptr().is_null());
        assert!(!encoded.values_ptr().is_null());
        assert!(!encoded.name_lens_ptr().is_null());
        assert!(!encoded.value_lens_ptr().is_null());

        unsafe {
            let names = encoded.names_ptr();
            let name_lens = encoded.name_lens_ptr();
            let values = encoded.values_ptr();
            let value_lens = encoded.value_lens_ptr();

            assert_eq!(*name_lens, 1);
            assert_eq!(
                std::slice::from_raw_parts(*names as *const u8, *name_lens),
                b"x"
            );
            assert_eq!(*name_lens.add(1), 5);
            assert_eq!(
                std::slice::from_raw_parts(*names.add(1) as *const u8, *name_lens.add(1)),
                b"label"
            );

            assert_eq!(*value_lens, 1);
            assert_eq!(
                std::slice::from_raw_parts(*values as *const u8, *value_lens),
                b"5"
            );
            assert_eq!(*value_lens.add(1), 2);
            assert_eq!(
                std::slice::from_raw_parts(*values.add(1) as *const u8, *value_lens.add(1)),
                b"ok"
            );
        }
        Ok(())
    }
}
