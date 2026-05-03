//! Query argument definitions for chDB.
//!
//! This module provides types for specifying query arguments such as output format,
//! log level, and custom command-line arguments.

use std::borrow::Cow;
use std::fmt::Display;

use crate::format::OutputFormat;
use crate::log_level::LogLevel;

/// Query arguments that can be passed when executing queries.
///
/// `Arg` represents various command-line arguments that can be used to configure
/// query execution. Most commonly, you'll use `OutputFormat` to specify the
/// desired output format.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::arg::Arg;
/// use chdb_rust::format::OutputFormat;
/// use chdb_rust::log_level::LogLevel;
///
/// // Specify output format
/// let args = &[Arg::OutputFormat(OutputFormat::JSONEachRow)];
///
/// // Specify log level
/// let args = &[Arg::LogLevel(LogLevel::Debug)];
///
/// // Use custom arguments
/// let args = &[Arg::Custom("path".into(), Some("/tmp/db".into()))];
/// ```
#[derive(Debug)]
pub enum Arg<'a> {
    /// `--config-file=<value>`
    ///
    /// Can be used to specify a custom configuration
    /// file for the session, allowing one to configure various aspects of the
    /// session's behavior (e.g., SSL settings to use with the `remoteSecure`
    /// function).
    ///
    /// An example SSL configuration file (`chdb_ssl.xml`) might look like this:
    /// ```xml
    /// <clickhouse>
    ///   <openSSL>
    ///     <client>
    ///       <caConfig>path_to_server_ca_cert.pem</caConfig>
    ///     </client>
    ///   </openSSL>
    /// </clickhouse>
    /// ```
    /// where `path_to_server_ca_cert.pem` is the path to the CA certificate
    /// of the remote server.
    ConfigFilePath(Cow<'a, str>),
    /// `--log-level=<value>`
    LogLevel(LogLevel),
    /// `--output-format=<value>`
    OutputFormat(OutputFormat),
    /// --multiquery
    /// Emitted as `-n` (short for `--multiquery`).
    MultiQuery,
    /// Custom argument.
    ///
    /// "--path=/tmp/chdb" translates into one of the following:
    /// 1. Arg::Custom("path".to_string().into(), Some("/tmp/chdb".to_string().into())).
    /// 2. Arg::Custom("path".into(), Some("/tmp/chdb".into())).
    ///
    /// "--multiline" translates into one of the following:
    /// 1. Arg::Custom("multiline".to_string().into(), None).
    /// 2. Arg::Custom("multiline".into(), None).
    ///
    /// Officially supported arguments can be found by running
    /// `clickhouse-client --help` in the terminal.
    Custom(Cow<'a, str>, Option<Cow<'a, str>>),
}

impl<'a> Arg<'a> {
    /// Extract `OutputFormat` from an `Arg` if it is an `OutputFormat` variant.
    ///
    /// This is a helper method used internally to extract output format information
    /// from query arguments.
    pub(crate) fn as_output_format(&self) -> Option<OutputFormat> {
        match self {
            Self::OutputFormat(f) => Some(*f),
            _ => None,
        }
    }
}

impl Display for Arg<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConfigFilePath(v) => write!(f, "--config-file={v}"),
            Self::LogLevel(v) => write!(f, "--log-level={}", v.as_str()),
            Self::OutputFormat(v) => write!(f, "--output-format={}", v.as_str()),
            Self::MultiQuery => write!(f, "-n"),
            Self::Custom(k, v) => match v {
                None => write!(f, "--{}", k.as_ref()),
                Some(v) => write!(f, "--{k}={v}"),
            },
        }
    }
}

/// Extract `OutputFormat` from a slice of `Arg`s.
///
/// This function searches through the provided arguments and returns the first
/// `OutputFormat` found, or the default `TabSeparated` format if none is found.
///
/// # Arguments
///
/// * `args` - Optional slice of query arguments
///
/// # Returns
///
/// Returns the first `OutputFormat` found, or `OutputFormat::TabSeparated` as default.
pub(crate) fn extract_output_format(args: Option<&[Arg]>, default: OutputFormat) -> OutputFormat {
    args.and_then(|args| args.iter().find_map(|a| a.as_output_format()))
        .unwrap_or(default)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_arg_display_config_file_path() {
        assert_eq!(
            Arg::ConfigFilePath(Cow::from("my.xml")).to_string(),
            "--config-file=my.xml"
        );
    }

    #[test]
    fn test_arg_display_log_level() {
        assert_eq!(
            Arg::LogLevel(LogLevel::Trace).to_string(),
            "--log-level=trace"
        );
        assert_eq!(
            Arg::LogLevel(LogLevel::Debug).to_string(),
            "--log-level=debug"
        );
        assert_eq!(
            Arg::LogLevel(LogLevel::Info).to_string(),
            "--log-level=information"
        );
        assert_eq!(
            Arg::LogLevel(LogLevel::Warn).to_string(),
            "--log-level=warning"
        );
        assert_eq!(
            Arg::LogLevel(LogLevel::Error).to_string(),
            "--log-level=error"
        );
    }

    #[test]
    fn test_arg_display_output_format() {
        assert_eq!(
            Arg::OutputFormat(OutputFormat::JSONEachRow).to_string(),
            "--output-format=JSONEachRow"
        );
    }

    #[test]
    fn test_arg_display_multi_query() {
        assert_eq!(Arg::MultiQuery.to_string(), "-n");
    }

    #[test]
    fn test_arg_display_custom_key_only() {
        assert_eq!(
            Arg::Custom("multiline".into(), None).to_string(),
            "--multiline"
        );
    }

    #[test]
    fn test_arg_display_custom_key_value() {
        assert_eq!(
            Arg::Custom("priority".into(), Some("1".into())).to_string(),
            "--priority=1"
        );
    }
}
