use std::borrow::Cow;
use std::ffi::CString;

use crate::error::Error;
use crate::format::OutputFormat;
use crate::log_level::LogLevel;

#[derive(Debug)]
pub enum Arg<'a> {
    /// --config-file=<value>
    ConfigFilePath(Cow<'a, str>),
    /// --log-level=<value>
    LogLevel(LogLevel),
    /// --output-format=<value>
    OutputFormat(OutputFormat),
    /// --multiquery
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
    /// We should tell user where to look for officially supported arguments.
    /// Here is some hint for now: https://github.com/fixcik/chdb-rs/blob/master/OPTIONS.md .
    Custom(Cow<'a, str>, Option<Cow<'a, str>>),
}

impl<'a> Arg<'a> {
    #[allow(dead_code)]
    pub(crate) fn to_cstring(&self) -> Result<CString, Error> {
        Ok(match self {
            Self::ConfigFilePath(v) => CString::new(format!("--config-file={}", v)),
            Self::LogLevel(v) => CString::new(format!("--log-level={}", v.as_str())),
            Self::OutputFormat(v) => CString::new(format!("--output-format={}", v.as_str())),
            Self::MultiQuery => CString::new("-n"),
            Self::Custom(k, v) => match v {
                None => CString::new(k.as_ref()),
                Some(v) => CString::new(format!("--{}={}", k, v)),
            },
        }?)
    }

    /// Extract OutputFormat from an Arg if it is an OutputFormat variant.
    pub(crate) fn as_output_format(&self) -> Option<OutputFormat> {
        match self {
            Self::OutputFormat(f) => Some(*f),
            _ => None,
        }
    }
}

/// Extract OutputFormat from a slice of Args, returns the first one found or default.
pub(crate) fn extract_output_format(args: Option<&[Arg]>) -> OutputFormat {
    args.and_then(|args| args.iter().find_map(|a| a.as_output_format()))
        .unwrap_or(OutputFormat::TabSeparated)
}
