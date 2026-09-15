//! Options for chDB's Arrow paths: the insert-side [`InsertOptions`] (tuning
//! knobs for `INSERT … SELECT` and direct FFI insert) and the read-side
//! [`ArrowOptions`] (type-mapping knobs for Arrow export).

/// Tuning knobs appended as `SETTINGS` on the insert `INSERT … SELECT` query,
/// or passed to direct FFI insert APIs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InsertOptions {
    /// `max_threads` insert setting (default: 4).
    pub max_threads: Option<u32>,
    /// `max_insert_block_size` insert setting.
    pub max_insert_block_size: Option<u64>,
    /// `min_insert_block_size_rows` insert setting.
    pub min_insert_block_size_rows: Option<u64>,
}

impl InsertOptions {
    /// Sensible defaults for bulk Arrow ingest.
    pub fn default_bulk() -> Self {
        Self {
            max_threads: Some(4),
            max_insert_block_size: None,
            min_insert_block_size_rows: None,
        }
    }

    pub(crate) fn settings_clause(&self) -> Option<String> {
        let mut parts = Vec::new();
        if let Some(v) = self.max_threads {
            parts.push(format!("max_threads = {v}"));
        }
        if let Some(v) = self.max_insert_block_size {
            parts.push(format!("max_insert_block_size = {v}"));
        }
        if let Some(v) = self.min_insert_block_size_rows {
            parts.push(format!("min_insert_block_size_rows = {v}"));
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join(", "))
        }
    }
}

/// How the engine maps ClickHouse types to Arrow types on export.
///
/// Distinct from [`InsertOptions`], which tunes the *insert* path — this is the
/// read side, and it wraps a different C struct (`chdb_arrow_options`).
///
/// [`Default`] reproduces the engine's own defaults, so
/// `ArrowOptions::default()` behaves exactly like passing no options at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArrowOptions {
    /// Emit types with no faithful Arrow mapping — JSON/Object, Dynamic,
    /// AggregateFunction — as `Binary` instead of failing. Default `false`,
    /// meaning the engine throws `UNKNOWN_TYPE` for such columns; `true`
    /// degrades them to `Binary`.
    pub unsupported_as_binary: bool,
    /// Emit `LowCardinality(T)` as an Arrow dictionary array instead of
    /// materializing it to `T`. Default `false`.
    ///
    /// The consumer must be able to handle dictionaries whose values are stable
    /// across batches.
    pub low_cardinality_as_dictionary: bool,
    /// Emit `String` as Arrow `Utf8`. Default `true`; `false` emits `Binary`.
    pub string_as_string: bool,
}

impl Default for ArrowOptions {
    fn default() -> Self {
        Self {
            unsupported_as_binary: false,
            low_cardinality_as_dictionary: false,
            string_as_string: true,
        }
    }
}

impl ArrowOptions {
    pub(crate) fn to_c(self) -> crate::bindings::chdb_arrow_options {
        crate::bindings::chdb_arrow_options {
            unsupported_as_binary: i32::from(self.unsupported_as_binary),
            low_cardinality_as_dictionary: i32::from(self.low_cardinality_as_dictionary),
            string_as_string: i32::from(self.string_as_string),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_options_settings_clause() {
        let opts = InsertOptions {
            max_threads: Some(4),
            max_insert_block_size: Some(1_048_576),
            min_insert_block_size_rows: None,
        };
        assert_eq!(
            opts.settings_clause().as_deref(),
            Some("max_threads = 4, max_insert_block_size = 1048576")
        );
        assert!(InsertOptions::default().settings_clause().is_none());
    }
}
