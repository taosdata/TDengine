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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Cache {
    #[serde(default = "default_max_size")]
    pub max_size: String,
    #[serde(default = "default_max_size_value")]
    pub max_size_value: usize,
    #[serde(default)]
    pub max_size_unit: String,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub on_fail: HandlingCacheFailed,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            max_size: "0GB".to_string(),
            max_size_value: 0,
            max_size_unit: "GB".to_string(),
            location: "cache".to_string(),
            on_fail: HandlingCacheFailed::default(),
        }
    }
}
