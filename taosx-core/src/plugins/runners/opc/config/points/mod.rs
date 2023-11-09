use serde::{Deserialize, Serialize};


#[derive(Debug, Serialize, Deserialize)]
pub struct PointsConfig {
    pub limit: usize,
    pub regex: Option<String>,
}