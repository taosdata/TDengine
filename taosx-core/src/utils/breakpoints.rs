use crate::get_data_dir;
use anyhow::Context;
use std::path::PathBuf;
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
    use chrono_tz::Brazil;

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
        assert_eq!(path.exists(), false);

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

    #[test]
    fn test_view_breakpoints_db() {
        std::env::set_var("TAOSX_DATA_DIR", "/var/lib/taos/taosx");
        let task_id = "3";
        let all = breakpoints_get_all(task_id).unwrap();
        for (key, value) in all {
            println!("key: {}, value: {}", key, value);
        }
    }
}
