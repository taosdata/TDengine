use crate::get_data_dir;
use std::path::PathBuf;
pub struct MetricsStore {
    pub path: PathBuf,
}

impl MetricsStore {
    pub fn new(task_id: &str) -> Self {
        let data_dir = get_data_dir();
        let path = data_dir.join("tasks").join(task_id).join("metrics.json");
        Self { path }
    }

    pub fn clear(&self) -> anyhow::Result<()> {
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }

    pub fn get_string(&self) -> anyhow::Result<String> {
        let content = std::fs::read_to_string(&self.path)?;
        Ok(content)
    }

    pub fn set(&self, metrics: &str) -> anyhow::Result<()> {
        std::fs::write(&self.path, metrics)?;
        Ok(())
    }
}
