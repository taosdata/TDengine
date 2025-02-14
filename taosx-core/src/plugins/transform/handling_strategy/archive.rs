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
    pub fn handle(&self, err: String) -> anyhow::Result<()> {
        match self {
            HandlingArchiveFailed::Rotate => {
                // TODO1: Implement delete old files
                Ok(())
            }
            HandlingArchiveFailed::Skip => {
                // TODO1: Implement skip
                Ok(())
            }
            HandlingArchiveFailed::Break => {
                anyhow::bail!(err)
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Archive {
    #[serde(default)]
    pub keep_days: usize,
    #[serde(default)]
    pub max_size: usize,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub on_fail: HandlingArchiveFailed,
}

impl Default for Archive {
    fn default() -> Self {
        Self {
            keep_days: 30,
            max_size: 0,
            location: "archived".to_string(),
            on_fail: HandlingArchiveFailed::default(),
        }
    }
}
