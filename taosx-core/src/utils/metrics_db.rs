use crate::get_data_dir;
use std::path::PathBuf;
use tracing::{debug, info};

pub struct MetricsDb {
    db: sled::Db,
}

impl MetricsDb {
    fn db_dir(task_id: &str) -> PathBuf {
        let path = get_data_dir();
        path.join("tasks").join(task_id).join("metrics")
    }

    pub fn new(task_id: &str) -> anyhow::Result<Self> {
        let path = Self::db_dir(task_id);
        debug!("metrics db path: {}", path.display());
        let db = sled::open(path)
            .map_err(|err| anyhow::anyhow!("sled open metrics db file failed: {:?}", err))?;
        Ok(Self { db })
    }

    pub fn set(&self, metrics: &str) -> anyhow::Result<()> {
        let key = "metrics";
        self.db.insert(key, metrics)?;
        Ok(())
    }

    pub fn get(&self) -> anyhow::Result<Option<String>> {
        let key = "metrics";
        let result = self.db.get(key)?;
        match result {
            Some(v) => Ok(Some(String::from_utf8(v.to_vec())?)),
            None => Ok(None),
        }
    }

    pub fn clear(&self, task_id: &str) -> anyhow::Result<()> {
        let path = Self::db_dir(task_id);
        // delete db file
        info!("delete metrics db file: {}", path.display());
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
        }
        Ok(())
    }
}

