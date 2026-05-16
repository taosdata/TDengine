use serde::{Deserialize, Serialize};
use std::{num::ParseIntError, path::PathBuf};
use thiserror::Error;

use crate::{ARCHIVE_DIR, ARCHIVE_PREFIX, CACHE_DIR, CACHE_PREFIX};

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingArchiveFailed {
    #[default]
    Rotate,
    Skip,
    Break,
}

impl HandlingArchiveFailed {
    pub fn handle(&self, err: String) -> anyhow::Result<bool> {
        match self {
            HandlingArchiveFailed::Rotate => {
                tracing::trace!("{err}: delete the oldest file and retry");
                Ok(true)
            }
            HandlingArchiveFailed::Skip => {
                tracing::warn!("{err}: skip record");
                Ok(false)
            }
            HandlingArchiveFailed::Break => {
                tracing::error!("{err}: break task");
                anyhow::bail!(err)
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Archive {
    #[serde(
        default = "Archive::default_keep_days",
        skip_serializing_if = "is_default_keep_days"
    )]
    pub keep_days: String,
    #[serde(
        default = "Archive::default_keep_days_value",
        skip_serializing_if = "is_default_keep_days_value"
    )]
    pub keep_days_value: usize,
    #[serde(
        default = "Archive::default_keep_days_unit",
        skip_serializing_if = "is_default_keep_days_unit"
    )]
    pub keep_days_unit: String,
    #[serde(
        default = "Archive::default_max_size",
        skip_serializing_if = "is_default_max_size"
    )]
    pub max_size: String,
    #[serde(
        default = "Archive::default_max_size_value",
        skip_serializing_if = "is_default_max_size_value"
    )]
    pub max_size_value: usize,
    #[serde(
        default = "Archive::default_max_size_unit",
        skip_serializing_if = "is_default_max_size_unit"
    )]
    pub max_size_unit: String,
    #[serde(
        default = "Archive::default_rotate_count",
        skip_serializing_if = "is_default_rotate_count"
    )]
    pub rotate_count: usize,
    #[serde(
        default = "Archive::default_location",
        skip_serializing_if = "is_default_location"
    )]
    pub location: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(
        default = "Archive::default_on_fail",
        skip_serializing_if = "is_default"
    )]
    pub on_fail: HandlingArchiveFailed,
}

impl Default for Archive {
    fn default() -> Self {
        Self {
            keep_days: Self::default_keep_days(),
            keep_days_value: Self::default_keep_days_value(),
            keep_days_unit: Self::default_keep_days_unit(),
            max_size: Self::default_max_size(),
            max_size_value: Self::default_max_size_value(),
            max_size_unit: Self::default_max_size_unit(),
            rotate_count: Self::default_rotate_count(),
            location: Self::default_location(),
            // `prefix` is intentionally None in the default; it is filled in at
            // runtime by `organize_params()`.  Keeping it None here ensures that
            // `is_default(&archive)` works correctly for skip-serialization checks.
            prefix: None,
            on_fail: Self::default_on_fail(),
        }
    }
}

impl Archive {
    pub fn organize_params(
        &mut self,
        task_id: i64,
        job_id: i64,
        def_data_dir: PathBuf,
        is_cache: bool,
    ) -> Result<(), CollateError> {
        self.location = self.location.trim().to_string();
        let dir = std::path::PathBuf::from(&self.location);
        // empty is relative
        if dir.is_relative() {
            let last_dir_name = match (is_cache, self.location.is_empty()) {
                (_, false) => self.location.as_str(),
                (true, true) => CACHE_DIR,
                (false, true) => ARCHIVE_DIR,
            };
            self.location = def_data_dir
                .join("tasks")
                .join(task_id.to_string())
                .join(job_id.to_string())
                .join(last_dir_name)
                .to_string_lossy()
                .to_string();
        }

        if self.prefix.is_none() {
            match is_cache {
                true => self.prefix = Some(CACHE_PREFIX.to_string()),
                false => self.prefix = Some(ARCHIVE_PREFIX.to_string()),
            }
        }
        // check keep_days
        self.keep_days = self.keep_days.trim().to_string();
        if self.keep_days.is_empty() {
            self.keep_days = Self::default_keep_days();
        }
        if self.keep_days != format!("{}{}", self.keep_days_value, self.keep_days_unit) {
            // decompose keep_days
            let re = regex::Regex::new(r"(^\d+)\s*([d|D])$").unwrap();
            let caps = match re.captures(&self.keep_days) {
                Some(caps) => caps,
                None => {
                    return Err(CollateError::KeepDaysFormatIncorrect {
                        input: self.keep_days.clone(),
                    });
                }
            };
            match caps[1].parse::<usize>() {
                Ok(v) => self.keep_days_value = v,
                Err(e) => return Err(CollateError::KeepDaysParseIntError(e)),
            }
            self.keep_days_unit = caps[2].to_lowercase();
        }
        if self.keep_days_value == 0 {
            self.keep_days = Self::default_keep_days();
            self.keep_days_value = Self::default_keep_days_value();
            self.keep_days_unit = Self::default_keep_days_unit();
        }
        // check max_size
        self.max_size = self.max_size.trim().to_string();
        if self.max_size.is_empty() {
            self.max_size = Self::default_max_size();
        }
        if self.max_size != format!("{}{}", self.max_size_value, self.max_size_unit) {
            // decompose max_size
            let re = regex::Regex::new(r"(?i)(^\d+)\s*([GMK]B)$").unwrap();
            let caps = match re.captures(&self.max_size) {
                Some(caps) => caps,
                None => {
                    return Err(CollateError::MaxSizeFormatIncorrect {
                        input: self.max_size.clone(),
                    });
                }
            };
            match caps[1].parse::<usize>() {
                Ok(v) => self.max_size_value = v,
                Err(e) => return Err(CollateError::MaxSizeParseIntError(e)),
            }
            self.max_size_unit = caps[2].to_uppercase();
        }
        if self.max_size_value == 0 {
            self.max_size = Self::default_max_size();
            self.max_size_value = Self::default_max_size_value();
            self.max_size_unit = Self::default_max_size_unit();
        }
        Ok(())
    }
}

impl Archive {
    fn default_keep_days() -> String {
        "30d".to_string()
    }

    fn default_keep_days_value() -> usize {
        30
    }

    fn default_keep_days_unit() -> String {
        "d".to_string()
    }

    fn default_max_size() -> String {
        "1GB".to_string()
    }

    fn default_max_size_value() -> usize {
        1
    }

    fn default_max_size_unit() -> String {
        "GB".to_string()
    }

    fn default_location() -> String {
        "".to_string()
    }

    fn default_rotate_count() -> usize {
        100
    }

    fn default_on_fail() -> HandlingArchiveFailed {
        HandlingArchiveFailed::Rotate
    }
}

fn is_default_keep_days(v: &str) -> bool {
    v == Archive::default_keep_days()
}
fn is_default_keep_days_value(v: &usize) -> bool {
    *v == Archive::default_keep_days_value()
}
fn is_default_keep_days_unit(v: &str) -> bool {
    v == Archive::default_keep_days_unit()
}
fn is_default_max_size(v: &str) -> bool {
    v == Archive::default_max_size()
}
fn is_default_max_size_value(v: &usize) -> bool {
    *v == Archive::default_max_size_value()
}
fn is_default_max_size_unit(v: &str) -> bool {
    v == Archive::default_max_size_unit()
}
fn is_default_rotate_count(v: &usize) -> bool {
    *v == Archive::default_rotate_count()
}
fn is_default_location(v: &str) -> bool {
    v == Archive::default_location()
}
fn is_default<T: Default + PartialEq>(v: &T) -> bool {
    *v == T::default()
}

#[derive(Debug, Error)]
pub enum CollateError {
    #[error(
        "keep_days: {input} format error, support only integer number followed by d or D, e.g. 30d"
    )]
    KeepDaysFormatIncorrect { input: String },
    #[error("keep_days integer parse error, detail error: {0}")]
    KeepDaysParseIntError(ParseIntError),
    #[error(
        "max_size: {input} format error, support only integer number followed by GB/MB/KB, e.g. 1GB"
    )]
    MaxSizeFormatIncorrect { input: String },
    #[error("max_size integer parse error, detail error: {0}")]
    MaxSizeParseIntError(ParseIntError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn organize_params_fills_archive_defaults_and_relative_location() {
        let mut archive = Archive::default();

        archive
            .organize_params(12, 34, PathBuf::from("/var/lib/taosx"), false)
            .unwrap();

        assert_eq!(archive.keep_days, "30d");
        assert_eq!(archive.keep_days_value, 30);
        assert_eq!(archive.keep_days_unit, "d");
        assert_eq!(archive.max_size, "1GB");
        assert_eq!(archive.max_size_value, 1);
        assert_eq!(archive.max_size_unit, "GB");
        assert_eq!(archive.prefix.as_deref(), Some(ARCHIVE_PREFIX));
        assert_eq!(
            archive.location,
            "/var/lib/taosx/tasks/12/34/archived".to_string()
        );
    }

    #[test]
    fn organize_params_uses_cache_defaults_when_requested() {
        let mut archive = Archive {
            location: " custom-cache ".to_string(),
            ..Default::default()
        };

        archive
            .organize_params(1, 2, PathBuf::from("/tmp/data"), true)
            .unwrap();

        assert_eq!(archive.prefix.as_deref(), Some(CACHE_PREFIX));
        assert_eq!(archive.location, "/tmp/data/tasks/1/2/custom-cache");
    }

    #[test]
    fn organize_params_preserves_absolute_location_and_existing_prefix() {
        let mut archive = Archive {
            location: "/absolute/archive".to_string(),
            prefix: Some("custom".to_string()),
            ..Default::default()
        };

        archive
            .organize_params(3, 4, PathBuf::from("/tmp/data"), false)
            .unwrap();

        assert_eq!(archive.location, "/absolute/archive");
        assert_eq!(archive.prefix.as_deref(), Some("custom"));
    }

    #[test]
    fn organize_params_parses_units_case_insensitively() {
        let mut archive = Archive {
            keep_days: " 7D ".to_string(),
            max_size: " 512 mb ".to_string(),
            ..Default::default()
        };

        archive
            .organize_params(1, 1, PathBuf::from("/tmp/data"), false)
            .unwrap();

        assert_eq!(archive.keep_days, "7D");
        assert_eq!(archive.keep_days_value, 7);
        assert_eq!(archive.keep_days_unit, "d");
        assert_eq!(archive.max_size, "512 mb");
        assert_eq!(archive.max_size_value, 512);
        assert_eq!(archive.max_size_unit, "MB");
    }

    #[test]
    fn organize_params_resets_zero_values_to_defaults() {
        let mut archive = Archive {
            keep_days: "0d".to_string(),
            max_size: "0KB".to_string(),
            ..Default::default()
        };

        archive
            .organize_params(1, 1, PathBuf::from("/tmp/data"), false)
            .unwrap();

        assert_eq!(archive.keep_days, Archive::default_keep_days());
        assert_eq!(archive.keep_days_value, Archive::default_keep_days_value());
        assert_eq!(archive.keep_days_unit, Archive::default_keep_days_unit());
        assert_eq!(archive.max_size, Archive::default_max_size());
        assert_eq!(archive.max_size_value, Archive::default_max_size_value());
        assert_eq!(archive.max_size_unit, Archive::default_max_size_unit());
    }

    #[test]
    fn organize_params_rejects_invalid_keep_days_and_max_size() {
        let mut archive = Archive {
            keep_days: "30h".to_string(),
            ..Default::default()
        };

        assert!(matches!(
            archive.organize_params(1, 1, PathBuf::from("/tmp/data"), false),
            Err(CollateError::KeepDaysFormatIncorrect { input }) if input == "30h"
        ));

        let mut archive = Archive {
            max_size: "1TB".to_string(),
            ..Default::default()
        };

        assert!(matches!(
            archive.organize_params(1, 1, PathBuf::from("/tmp/data"), false),
            Err(CollateError::MaxSizeFormatIncorrect { input }) if input == "1TB"
        ));
    }

    #[test]
    fn handling_archive_failed_variants_return_expected_retry_decision() {
        assert!(
            HandlingArchiveFailed::Rotate
                .handle("rotate".to_string())
                .unwrap()
        );
        assert!(
            !HandlingArchiveFailed::Skip
                .handle("skip".to_string())
                .unwrap()
        );
        assert!(
            HandlingArchiveFailed::Break
                .handle("break".to_string())
                .is_err()
        );
    }

    #[test]
    fn default_predicates_match_archive_defaults() {
        assert!(is_default_keep_days("30d"));
        assert!(is_default_keep_days_value(&30));
        assert!(is_default_keep_days_unit("d"));
        assert!(is_default_max_size("1GB"));
        assert!(is_default_max_size_value(&1));
        assert!(is_default_max_size_unit("GB"));
        assert!(is_default_rotate_count(&100));
        assert!(is_default_location(""));
        assert!(is_default(&HandlingArchiveFailed::Rotate));

        assert!(!is_default_keep_days("7d"));
        assert!(!is_default_max_size("512MB"));
        assert!(!is_default(&HandlingArchiveFailed::Break));
    }
}
