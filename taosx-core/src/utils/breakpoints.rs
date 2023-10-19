use tracing::{debug, info};

fn breakpoints_db_dir(task_id: &str) -> String {
    let current_dir = std::env::current_dir().unwrap();
    let path = std::env::var("TAOSX_DATA_DIR")
        .unwrap_or_else(|_| format!("{}/data", current_dir.to_str().unwrap()));
    format!("{}/{}/breakpoints", path, task_id)
}

pub fn breakpoints_set(task_id: &str, sub_task: &str, breakpoints: &str) -> anyhow::Result<()> {
    let path = breakpoints_db_dir(task_id);
    debug!(
        "breakpoints db path: {}, breakponts key: {}, value: {}",
        path, sub_task, breakpoints
    );
    let db =
        sled::open(path).map_err(|err| anyhow::anyhow!("sled open db file failed: {:?}", err))?;
    db.insert(sub_task, breakpoints)?;
    Ok(())
}

pub fn breakpoints_get(task_id: &str, sub_task: &str) -> anyhow::Result<Option<String>> {
    let path = breakpoints_db_dir(task_id);
    // if path not exist, return None to avoid create db file
    if !std::path::Path::new(&path).exists() {
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

pub fn breakpoints_get_all(task_id: &str) -> anyhow::Result<Vec<(String, String)>> {
    let path = breakpoints_db_dir(task_id);
    // if path not exist, return None to avoid create db file
    if !std::path::Path::new(&path).exists() {
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
    info!("delete breakpoints db file: {}", path);
    if std::path::Path::new(&path).exists() {
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
        assert_eq!(path, "/tmp/data/1/breakpoints");
    }

    #[test]
    fn test_breakpoints_remove() {
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
        let task_id = "2";
        let sub_task = "t0001";
        let breakpoints = "2023-01-01 20:00:00";
        breakpoints_set(task_id, sub_task, breakpoints).unwrap();

        let result = breakpoints_get(task_id, sub_task).unwrap();
        assert_eq!(result.unwrap(), breakpoints);

        breakpoints_clear(task_id).unwrap();

        let dir = breakpoints_db_dir(task_id);
        assert_eq!(std::path::Path::new(&dir).exists(), false);

        let result = breakpoints_get(task_id, sub_task).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_breakpoints_full_routine() {
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
    fn test_breakpoints_set_muti_thread() {
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
}
