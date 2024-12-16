use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy)]
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
                // TODO: Implement delete old files
                Ok(())
            }
            HandlingArchiveFailed::Skip => {
                // TODO: Implement skip
                Ok(())
            }
            HandlingArchiveFailed::Break => {
                anyhow::bail!(err)
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Archive {
    pub keep_days: usize,
    pub max_size: usize,
    pub location: String,
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
