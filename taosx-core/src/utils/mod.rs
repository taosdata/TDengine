use std::{path::Path, thread::JoinHandle, io::BufRead};

use futures::TryStreamExt;
use taos::*;

pub mod port_pool;

pub fn stop_thread<T>(handle: JoinHandle<T>) {
    #[cfg(windows)]
    unsafe {
        use std::os::windows::io::IntoRawHandle;
        use winapi::ctypes::c_void as winapi_c_void;
        use winapi::um::processthreadsapi::TerminateThread;

        let raw_handle = handle.into_raw_handle();
        TerminateThread(raw_handle as *mut winapi_c_void, 0);
    }
    #[cfg(unix)]
    unsafe {
        use libc::pthread_kill;
        use std::os::unix::thread::JoinHandleExt;

        let raw_handle = handle.into_pthread_t();
        pthread_kill(raw_handle, 2);
    };
}

// /// Check enterprise edition
// pub async fn is_available_enterprise_edition(taos: &TaosBuilder) -> bool {
//     taos.is_enterprise_edition().await
// }

/// Clear database stables and tables.
pub async fn clear_database(dsn: &Dsn) -> anyhow::Result<()> {
    let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

    let mut stables = taos.query("SHOW STABLES").await?;
    let mut rows = stables.rows();

    while let Some(mut row) = rows.try_next().await? {
        let name = format!("{}", row.next().unwrap().1);
        taos.exec(format!("DROP STABLE {name}")).await?;
    }

    let mut tables = taos.query("SHOW TABLES").await?;
    let mut rows = tables.rows();

    while let Some(mut row) = rows.try_next().await? {
        let name = format!("{}", row.next().unwrap().1);
        taos.exec(format!("DROP TABLE {name}")).await?;
    }

    Ok(())
}

/// read_first: only read first file or first string config when set true
/// append: append all values into a single string (contains line break) when set true
pub fn get_string_content_from_param_value(param_value: &str, read_fisrt: bool, append: bool) -> anyhow::Result<Option<String>> {
    let (files, str_contents): (Vec<String>, Vec<String>) = param_value.split(",")
        .map(|s| s.trim()).filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .partition(|v| v.starts_with("@"));
    let mut result = String::new();
    let mut index = 0;
    let len = if read_fisrt {
        1
    } else {
        if files.len() > str_contents.len() {
            files.len()
        } else {
            str_contents.len()
        }
    };
    for file in files {
        if index >= len {
            break;
        }
        let f = std::fs::File::open(&file[1..]);
        if let Err(err) = f {
            anyhow::bail!("file: {} read error, cause: {}", file, err.to_string());
        } else {
            let buf = std::io::BufReader::new(f.unwrap());
            let file_data = buf.lines().collect_vec().iter().filter_map(|r| r.as_ref().ok()).join("");
            result.push_str(file_data.as_str());
        }
        index += 1;
    }
    if result.is_empty() && append {
        for content in str_contents {
            if index >= len {
                break;
            }
            result.push_str(content.as_str());
            index += 1;
        }
    }
    
    if result.is_empty() {
        Ok(None)
    } else {
        Ok(Some(result))
    }
}


pub fn get_string_content_from_file_path(file_path: &str) -> Option<String> {
    let (files, _str_contents): (Vec<String>, Vec<String>) = file_path.split(",")
        .map(|s| s.trim()).filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .partition(|v| v.starts_with("@"));
        let file = files.get(0);
        if file.is_none() {
            None
        } else {
            let file = file.unwrap();
            let f = std::fs::File::open(&file[1..]);
            if let Err(err) = f {
                log::error!("file: {} read error, cause: {}", file, err.to_string());
                None
            } else {
                let buf = std::io::BufReader::new(f.unwrap());
                let file_data = buf.lines().collect_vec().iter().filter_map(|r| r.as_ref().ok()).join("");
                Some(file_data)
            }
        }
}

pub async fn clear_local(local: &Dsn) -> anyhow::Result<()> {
    if let Some(path) = local.path.as_deref() {
        let path = Path::new(path);
        if path.exists() {
            tokio::fs::remove_dir_all(path).await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clear_database() -> anyhow::Result<()> {
    let dsn = "taos:///";

    let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

    let db = "test_clear_database";
    taos.exec_many([
        format!("drop database if exists {db}"),
        format!("create database {db}"),
        format!("use {db}"),
        format!("create stable stb1 (ts timestamp, v int) tags(t1 int)"),
        format!("create table ctb1 using stb1 tags(1)"),
        format!("create table ctb2 using stb1 tags(2)"),
        format!("create table ntb1 (ts timestamp, v int)"),
        format!("create table ntb2 (ts timestamp, v int)"),
    ])
    .await?;

    use std::str::FromStr;

    clear_database(&Dsn::from_str(&format!("taos:///{db}"))?).await?;

    assert!(taos.query_one::<_, String>("show stables").await?.is_none());
    assert!(taos.query_one::<_, String>("show tables").await?.is_none());

    taos.exec(format!("drop database {db}")).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_string_content_from_param_value() {
    
}
