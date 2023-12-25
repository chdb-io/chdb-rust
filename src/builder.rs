use std::{fs, path::PathBuf};

use tracing::error;

use crate::{session::Session, Error};

pub struct SessionBuilder {
    format: String,
    log_level: String,
    data_path: std::path::PathBuf,
    udf_path: std::path::PathBuf,
}

impl SessionBuilder {
    pub fn new() -> Self {
        let mut data_path = std::env::current_dir().unwrap();
        data_path.push("var");

        let mut udf_path = std::env::current_dir().unwrap();
        udf_path.push("udf");

        SessionBuilder {
            format: "CSV".to_owned(),
            log_level: "trace".to_owned(),
            data_path,
            udf_path,
        }
    }

    pub fn format(mut self, format: impl Into<String>) -> Self {
        self.format = format.into();
        self
    }

    pub fn data_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_path = path.into();
        self
    }

    pub fn udf_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_path = path.into();
        self
    }

    pub fn log_level(mut self, level: &str) -> Self {
        self.log_level = match level {
            "trace" => "trace".to_string(),
            "debug" => "debug".to_string(),
            "info" => "information".to_string(),
            "warn" => "warning".to_string(),
            _ => {
                error!("Invalid log level. Setting to info");
                "information".to_string()
            }
        };

        self
    }

    pub fn build(self) -> Result<Session, Error> {
        std::fs::create_dir_all(&self.data_path)?;
        if fs::metadata(&self.data_path)?.permissions().readonly() {
            return Err(Error::InsufficientPermissions);
        }

        let data_path = self.data_path.to_str().ok_or(Error::PathError)?.to_string();

        std::fs::create_dir_all(&self.udf_path)?;
        if fs::metadata(&self.udf_path)?.permissions().readonly() {
            return Err(Error::InsufficientPermissions);
        }

        let udf_path = self.udf_path.to_str().ok_or(Error::PathError)?.to_string();

        Ok(Session {
            format: self.format,
            data_path,
            udf_path,
            log_level: self.log_level,
        })
    }
}
