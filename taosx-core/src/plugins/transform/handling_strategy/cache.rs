use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingCacheFailed {
    #[default]
    Skip,
    Break,
}

impl HandlingCacheFailed {
    pub fn handle(&self, err: String) -> anyhow::Result<()> {
        match self {
            HandlingCacheFailed::Skip => {
                tracing::warn!("{err}: skip record");
                Ok(())
            }
            HandlingCacheFailed::Break => {
                tracing::error!("{err}: break task");
                anyhow::bail!(err)
            }
        }
    }
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
    "cache".to_string()
}

fn default_on_fail() -> HandlingCacheFailed {
    HandlingCacheFailed::Skip
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Cache {
    #[serde(default = "default_max_size")]
    pub max_size: String,
    #[serde(default = "default_max_size_value")]
    pub max_size_value: usize,
    #[serde(default = "default_max_size_unit")]
    pub max_size_unit: String,
    #[serde(default = "default_location")]
    pub location: String,
    #[serde(default = "default_on_fail")]
    pub on_fail: HandlingCacheFailed,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            max_size: default_max_size(),
            max_size_value: default_max_size_value(),
            max_size_unit: default_max_size_unit(),
            location: default_location(),
            on_fail: default_on_fail(),
        }
    }
}
