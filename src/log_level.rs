//! Log level definitions for chDB.
//!
//! This module defines the log levels that can be used to configure chDB's logging behavior.

/// Log levels for chDB.
///
/// These correspond to the standard log levels used by chDB for controlling
/// the verbosity of log output.
#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    /// Get the string representation of the log level.
    ///
    /// This returns the log level name as expected by chDB.
    ///
    /// # Returns
    ///
    /// Returns the log level name as a static string slice.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "information",
            Self::Warn => "warning",
            Self::Error => "error",
        }
    }
}
