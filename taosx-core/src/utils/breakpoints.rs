use crate::get_data_dir;
use anyhow::Context;
use flate2::{write::GzEncoder, Compression};
use std::{io::Write, path::PathBuf};
use tracing::{debug, info};

fn breakpoints_db_dir(task_id: &str) -> PathBuf {
    let path = get_data_dir();
    path.join("tasks").join(task_id).join("breakpoints")
}

#[derive(Debug, Clone)]
pub struct BreakpointDb {
    db: std::sync::Arc<sled::Db>,
}

impl BreakpointDb {
    fn new(db: sled::Db) -> Self {
        Self {
            db: std::sync::Arc::new(db),
        }
    }

    pub async fn new_with_task(id: &str) -> anyhow::Result<Self> {
        let path = breakpoints_db_dir(id);
        // create db file
        if !path.exists() {
            tokio::fs::create_dir_all(&path).await?;
        }

        let mut retries = 0;
        let max_retries = 5;
        loop {
            match sled::open(&path) {
                Ok(db) => {
                    return Ok(BreakpointDb::new(db));
                }
                Err(err) => {
                    if retries >= max_retries {
                        return Err(anyhow::anyhow!("sled open db file failed: {:?}", err));
                    }
                    retries += 1;
                    tracing::warn!(
                        "sled open db file failed: {:?}, retrying in 1 second, retries: {}",
                        err,
                        retries
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    pub async fn set(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let db = self.db.clone();
        let key = key.to_string();
        let value = value.as_bytes().to_vec();
        tokio::task::spawn_blocking(move || db.insert(key, value).map(|_| ()))
            .await?
            .context("Breakpoint set error")?;
        Ok(())
    }

    pub async fn batch_set(&self, data: Vec<(String, String)>) -> anyhow::Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut batch = sled::Batch::default();
            for (key, value) in data {
                batch.insert(key.as_str(), value.as_str());
            }
            db.apply_batch(batch)
        })
        .await?
        .context("Breakpoint batch set error")?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        let db = self.db.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let result = db.get(key).context("Breakpoint get error")?;
            match result {
                Some(v) => Some(
                    String::from_utf8(v.to_vec())
                        .map_err(|err| anyhow::anyhow!("Breakpoint value utf8 error: {:?}", err)),
                )
                .transpose(),
                None => Ok(None),
            }
        })
        .await
        .context("Spawn blocking task for breakpoint get")?
    }
}

pub fn breakpoints_set(task_id: &str, sub_task: &str, breakpoints: &str) -> anyhow::Result<()> {
    if task_id == "-1" {
        return Ok(());
    }
    let path = breakpoints_db_dir(task_id);
    debug!(
        "breakpoints db path: {}, breakpoints key: {}, value: {}",
        path.display(),
        sub_task,
        breakpoints
    );
    let db =
        sled::open(path).map_err(|err| anyhow::anyhow!("sled open db file failed: {:?}", err))?;
    db.insert(sub_task, breakpoints)?;
    Ok(())
}

pub fn breakpoints_get(task_id: &str, sub_task: &str) -> anyhow::Result<Option<String>> {
    let path = breakpoints_db_dir(task_id);
    // if path not exist, return None to avoid create db file
    if !path.exists() {
        return Ok(None);
    }
    let db =
        sled::open(path).map_err(|err| anyhow::anyhow!("sled open db file failed: {:?}", err))?;
    let result = db.get(sub_task)?;
    match result {
        Some(v) => Ok(Some(String::from_utf8(v.to_vec())?)),
        None => Ok(None),
    }
}

pub async fn breakpoints_get_async(
    task_id: &str,
    sub_task: &str,
) -> anyhow::Result<Option<String>> {
    let path = breakpoints_db_dir(task_id);
    // if path not exist, return None to avoid create db file
    if !path.exists() {
        return Ok(None);
    }
    let sub_task = sub_task.to_string();
    tokio::task::spawn_blocking(move || {
        let db = sled::open(path)
            .map_err(|err| anyhow::anyhow!("sled open db file failed: {:?}", err))?;
        let result = db.get(sub_task.as_bytes())?;
        match result {
            Some(v) => Ok(Some(String::from_utf8(v.to_vec())?)),
            None => Ok(None),
        }
    })
    .await?
}

pub fn breakpoints_get_all(task_id: &str) -> anyhow::Result<Vec<(String, String)>> {
    let path = breakpoints_db_dir(task_id);
    // if path not exist, return None to avoid create db file
    if !path.exists() {
        return Ok(vec![]);
    }
    let db =
        sled::open(path).map_err(|err| anyhow::anyhow!("sled open db file failed: {:?}", err))?;
    let mut result = vec![];
    for item in db.iter() {
        let (key, value) = item?;
        result.push((
            String::from_utf8(key.to_vec())?,
            String::from_utf8(value.to_vec())?,
        ));
    }
    Ok(result)
}

pub fn export_breakpoints_to_csv(task_id: &str) -> anyhow::Result<Option<String>> {
    let breakpoint_db_path = breakpoints_db_dir(task_id);
    // if path not exist, return None to avoid create db file
    if !breakpoint_db_path.exists() {
        return Ok(None);
    }
    let db = sled::open(&breakpoint_db_path)
        .map_err(|err| anyhow::anyhow!("sled open db file failed: {:?}", err))?;
    let export_file = breakpoint_db_path.with_extension("csv");
    let mut file = std::fs::File::create(export_file)?;
    for item in db.iter() {
        let (key, value) = item?;
        file.write(&key)?;
        file.write(b",")?;
        file.write(&value)?;
        file.write(b"\n")?;
    }
    let relative_path = "tasks/".to_string() + task_id + "/breakpoints.csv";
    Ok(Some(relative_path))
}

pub fn export_breakpoints_to_compressed_csv(task_id: &str) -> anyhow::Result<Option<String>> {
    let breakpoint_db_path = breakpoints_db_dir(task_id);
    // if path not exist, return None to avoid create db file
    if !breakpoint_db_path.exists() {
        return Ok(None);
    }
    let db = sled::open(&breakpoint_db_path)
        .map_err(|err| anyhow::anyhow!("sled open db file failed: {:?}", err))?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    for item in db.iter() {
        let (key, value) = item?;
        encoder.write_all(&key)?;
        encoder.write_all(b",")?;
        encoder.write_all(&value)?;
        encoder.write_all(b"\n")?;
    }
    let compressed_data = encoder.finish()?;
    let export_file = breakpoint_db_path.with_extension("csv.gz");
    let mut file = std::fs::File::create(export_file)?;
    file.write(&compressed_data)?;
    let relative_path = "tasks/".to_string() + task_id + "/breakpoints.csv.gz";
    Ok(Some(relative_path))
}

pub fn breakpoints_remove(task_id: &str, sub_task: &str) -> anyhow::Result<()> {
    let path = breakpoints_db_dir(task_id);
    let db =
        sled::open(path).map_err(|err| anyhow::anyhow!("sled open db file failed: {:?}", err))?;
    db.remove(sub_task)?;
    Ok(())
}

pub fn breakpoints_clear(task_id: &str) -> anyhow::Result<()> {
    let path = breakpoints_db_dir(task_id);
    // delete db file
    info!("delete breakpoints db file: {}", path.display());
    if path.exists() {
        std::fs::remove_dir_all(&path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_breakpoints_db_dir() {
        let task_id = "1";
        // let path = breakpoints_db_dir(task_id);
        // assert_eq!(path, "./data/1/breakpoints");

        // set env
        std::env::set_var("TAOSX_DATA_DIR", "/tmp/data");
        let path = breakpoints_db_dir(task_id);
        assert_eq!(
            "/tmp/data/tasks/1/breakpoints",
            format!("{:}", path.display())
        );
    }

    #[test]
    fn test_breakpoints_remove() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::env::set_var("TAOSX_DATA_DIR", tmp.path());
        let task_id = "1";
        let sub_task = "t0001";
        let breakpoints = "2023-01-01 20:00:00";
        breakpoints_set(task_id, sub_task, breakpoints).unwrap();

        let result = breakpoints_get(task_id, sub_task).unwrap();
        assert_eq!(result.unwrap(), breakpoints);

        breakpoints_remove(task_id, sub_task).unwrap();
        let result = breakpoints_get(task_id, sub_task).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_breakpoints_clear() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::env::set_var("TAOSX_DATA_DIR", tmp.path());
        let task_id = "2";
        let sub_task = "t0001";
        let breakpoints = "2023-01-01 20:00:00";
        breakpoints_set(task_id, sub_task, breakpoints).unwrap();

        let result = breakpoints_get(task_id, sub_task).unwrap();
        assert_eq!(result.unwrap(), breakpoints);

        breakpoints_clear(task_id).unwrap();

        let path = breakpoints_db_dir(task_id);
        assert!(!path.exists());

        let result = breakpoints_get(task_id, sub_task).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_breakpoints_full_routine() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::env::set_var("TAOSX_DATA_DIR", tmp.path());
        let res_not_exist = breakpoints_get("20", "t0001").unwrap();
        assert_eq!(res_not_exist, None);

        let task_id = "1";
        let sub_task = "t0001";
        let breakpoints = "2023-01-01 20:00:00";
        breakpoints_set(task_id, sub_task, breakpoints).unwrap();

        let result = breakpoints_get(task_id, sub_task).unwrap();
        assert_eq!(result.unwrap(), breakpoints);
    }

    #[test]
    fn test_breakpoints_get_all() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::env::set_var("TAOSX_DATA_DIR", tmp.path());
        let task_id = "1";
        let sub_task = "t0001";
        let breakpoints = "2023-01-01 20:00:00";
        breakpoints_set(task_id, sub_task, breakpoints).unwrap();

        let task_id = "2";
        let sub_task = "t0002";
        let breakpoints = "2023-01-01 20:00:00";
        breakpoints_set(task_id, sub_task, breakpoints).unwrap();

        let result = breakpoints_get_all("1").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "t0001");
        assert_eq!(result[0].1, "2023-01-01 20:00:00");

        let result = breakpoints_get_all("2").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "t0002");
        assert_eq!(result[0].1, "2023-01-01 20:00:00");
    }

    #[test]
    fn test_breakpoints_set_multi_thread() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::env::set_var("TAOSX_DATA_DIR", tmp.path());
        use std::thread;
        let mut handles = vec![];
        let n = 10;
        for i in 0..n {
            let task_id = format!("task{}", i);
            let sub_task = format!("sub_task{}", i);
            let breakpoints = format!("breakpoints{}", i);

            let handle = thread::spawn(move || {
                // 调用 breakpoints_set 函数
                match breakpoints_set(&task_id, &sub_task, &breakpoints) {
                    Ok(()) => println!("Thread {} succeeded", i),
                    Err(err) => println!("Thread {} failed: {}", i, err),
                }
            });

            handles.push(handle);
        }

        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }

        // 验证所有数据都写入成功
        for i in 0..n {
            let task_id = format!("task{}", i);
            let sub_task = format!("sub_task{}", i);
            let breakpoints = format!("breakpoints{}", i);

            let result = breakpoints_get(&task_id, &sub_task).unwrap();
            assert_eq!(result.unwrap(), breakpoints);
        }

        // 清理数据
        for i in 0..n {
            let task_id = format!("task{}", i);
            breakpoints_clear(&task_id).unwrap();
        }
    }

    #[tokio::test]
    async fn test_export() {
        std::env::set_var("TAOSX_DATA_DIR", "/var/lib/taos/taosx");
        let task_id = "1000000";
        let breakpoint_db = BreakpointDb::new_with_task(task_id).await.unwrap();
        breakpoint_db
            .set("table1", "2023-01-01 20:00:00")
            .await
            .unwrap();
        breakpoint_db
            .set("table2", "2023-01-01 20:00:00")
            .await
            .unwrap();
        breakpoint_db
            .set("table3", "2023-01-01 20:00:00")
            .await
            .unwrap();
        drop(breakpoint_db);
        let export_file = export_breakpoints_to_csv(task_id).unwrap().unwrap();
        println!("{}", export_file);
    }
}
