//! Shared options for Arrow bulk insert paths.

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
