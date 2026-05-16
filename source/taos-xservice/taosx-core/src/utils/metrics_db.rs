use anyhow::Context;

use crate::get_data_dir;
use std::path::PathBuf;
pub struct MetricsStore {
    pub path: PathBuf,
}

impl MetricsStore {
    pub async fn new(task_id: i64, job_id: i64) -> Self {
        let data_dir = get_data_dir();
        let path = data_dir
            .join("tasks")
            .join(task_id.to_string())
            .join(job_id.to_string());
        if !path.exists()
            && let Err(err) = tokio::fs::create_dir_all(&path).await
        {
            tracing::error!("failed to create dir {:?}: {}", path, err);
        }
        let path = path.join("metrics.json");
        Self { path }
    }

    pub fn new_blocking(task_id: i64, job_id: i64) -> Self {
        let data_dir = get_data_dir();
        let path = data_dir
            .join("tasks")
            .join(task_id.to_string())
            .join(job_id.to_string());
        if !path.exists()
            && let Err(err) = std::fs::create_dir_all(&path)
        {
            tracing::error!("failed to create dir {:?}: {}", path, err);
        }
        let path = path.join("metrics.json");
        Self { path }
    }

    pub async fn clear(&self) -> anyhow::Result<()> {
        if self.path.exists() {
            tokio::fs::remove_file(&self.path)
                .await
                .context("Remove metrics json file error")?;
        }
        Ok(())
    }

    pub async fn get_string(&self) -> anyhow::Result<String> {
        tokio::fs::read_to_string(&self.path)
            .await
            .context("Get metrics json content error")
    }

    pub async fn set(&self, metrics: &str) -> anyhow::Result<()> {
        tokio::fs::write(&self.path, metrics)
            .await
            .context("Update metrics json file error")?;
        Ok(())
    }

    pub fn set_blocking(&self, metrics: &str) -> anyhow::Result<()> {
        std::fs::write(&self.path, metrics).context("Update metrics json file error")?;
        Ok(())
    }
}
