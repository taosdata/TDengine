use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy)]
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
                // TODO: Implement skip
                Ok(())
            }
            HandlingCacheFailed::Break => {
                anyhow::bail!(err)
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Cache {
    pub max_size: usize,
    pub location: String,
    pub on_fail: HandlingCacheFailed,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            max_size: 0,
            location: "cache".to_string(),
            on_fail: HandlingCacheFailed::default(),
        }
    }
}
