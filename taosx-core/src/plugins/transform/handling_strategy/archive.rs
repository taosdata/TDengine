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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Archive {
    #[serde(default)]
    pub keep_days: String,
    #[serde(default)]
    pub keep_days_value: usize,
    #[serde(default)]
    pub keep_days_unit: String,
    #[serde(default)]
    pub max_size: String,
    #[serde(default)]
    pub max_size_value: usize,
    #[serde(default)]
    pub max_size_unit: String,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub on_fail: HandlingArchiveFailed,
}

impl Default for Archive {
    fn default() -> Self {
        Self {
            keep_days: "0d".to_string(),
            keep_days_value: 0,
            keep_days_unit: "d".to_string(),
            max_size: "0GB".to_string(),
            max_size_value: 0,
            max_size_unit: "GB".to_string(),
            location: "archived".to_string(),
            on_fail: HandlingArchiveFailed::default(),
        }
    }
}
