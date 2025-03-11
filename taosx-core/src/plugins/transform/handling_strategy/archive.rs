use serde::{Deserialize, Serialize};

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
    "0GB".to_string()
}

fn default_max_size_value() -> usize {
    0
}

fn default_max_size_unit() -> String {
    "GB".to_string()
}

fn default_location() -> String {
    "archived".to_string()
}

fn default_on_fail() -> HandlingArchiveFailed {
    HandlingArchiveFailed::Rotate
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Archive {
    #[serde(default = "default_keep_days")]
    pub keep_days: String,
    #[serde(default = "default_keep_days_value")]
    pub keep_days_value: usize,
    #[serde(default = "default_keep_days_unit")]
    pub keep_days_unit: String,
    #[serde(default = "default_max_size")]
    pub max_size: String,
    #[serde(default = "default_max_size_value")]
    pub max_size_value: usize,
    #[serde(default = "default_max_size_unit")]
    pub max_size_unit: String,
    #[serde(default = "default_location")]
    pub location: String,
    #[serde(default = "default_on_fail")]
    pub on_fail: HandlingArchiveFailed,
}

impl Default for Archive {
    fn default() -> Self {
        Self {
            keep_days: default_keep_days(),
            keep_days_value: default_keep_days_value(),
            keep_days_unit: default_keep_days_unit(),
            max_size: default_max_size(),
            max_size_value: default_max_size_value(),
            max_size_unit: default_max_size_unit(),
            location: default_location(),
            on_fail: default_on_fail(),
        }
    }
}
