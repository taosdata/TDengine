use std::{io::BufRead, path::Path, thread::JoinHandle};

use anyhow::bail;
use taos::*;

pub mod breakpoints;
pub mod constants;
pub mod dsn;
pub mod duration;
pub mod files;
pub mod interval;
pub mod license;
pub mod log_cache;
pub mod metrics_db;
pub mod monitor;
pub mod port_pool;
pub mod rhai_syntax_validator;
pub mod sql;
pub mod timeout;
pub mod trace;

pub use duration::parse_duration;

pub fn value_equals(value: &Value, other: &Value) -> bool {
    match (value, other) {
        (Value::Null(l0), Value::Null(r0)) => l0 == r0,
        (Value::Bool(l0), Value::Bool(r0)) => l0 == r0,
        (Value::TinyInt(l0), Value::TinyInt(r0)) => l0 == r0,
        (Value::SmallInt(l0), Value::SmallInt(r0)) => l0 == r0,
        (Value::Int(l0), Value::Int(r0)) => l0 == r0,
        (Value::BigInt(l0), Value::BigInt(r0)) => l0 == r0,
        (Value::Float(l0), Value::Float(r0)) => l0 == r0,
        (Value::Double(l0), Value::Double(r0)) => l0 == r0,
        (Value::VarChar(l0) | Value::NChar(l0), Value::VarChar(r0) | Value::NChar(r0)) => l0 == r0,
        (Value::Timestamp(l0), Value::Timestamp(r0)) => l0 == r0,
        (Value::UTinyInt(l0), Value::UTinyInt(r0)) => l0 == r0,
        (Value::USmallInt(l0), Value::USmallInt(r0)) => l0 == r0,
        (Value::UInt(l0), Value::UInt(r0)) => l0 == r0,
        (Value::UBigInt(l0), Value::UBigInt(r0)) => l0 == r0,
        (Value::Json(l0), Value::Json(r0)) => l0 == r0,
        (Value::VarBinary(l0), Value::VarBinary(r0)) => l0 == r0,
        (Value::Decimal(l0), Value::Decimal(r0)) => l0 == r0,
        (Value::Blob(l0), Value::Blob(r0)) => l0 == r0,
        (Value::MediumBlob(l0), Value::MediumBlob(r0)) => l0 == r0,
        _ => false,
    }
}

pub fn mask_dsn(dsn: &Dsn) -> Dsn {
    let mut dsn = dsn.clone();
    dsn.password.take();
    dsn.username.take();
    dsn.params.clear();
    dsn
}

pub fn try_mask_dsn(dsn: &str) -> Option<String> {
    dsn.parse()
        .ok()
        .map(|dsn| mask_dsn(&dsn))
        .map(|dsn| dsn.to_string())
}

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
pub fn get_string_content_from_param_value(
    param_value: &str,
    read_first: bool,
    append: bool,
) -> anyhow::Result<Option<String>> {
    let (files, str_contents): (Vec<String>, Vec<String>) = param_value
        .split(",")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .partition(|v| v.starts_with("@"));
    let mut result = String::new();
    let mut index = 0;
    let len = if read_first {
        1
    } else if files.len() > str_contents.len() {
        files.len()
    } else {
        str_contents.len()
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
            let file_data = buf
                .lines()
                .collect_vec()
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .join("\n");
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
    let (files, _str_contents): (Vec<String>, Vec<String>) = file_path
        .split(",")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .partition(|v| v.starts_with("@"));
    let file = files.first();
    if file.is_none() {
        None
    } else {
        let file = file.unwrap();
        let f = std::fs::File::open(&file[1..]);
        if let Err(err) = f {
            tracing::error!("file: {} read error, cause: {}", file, err.to_string());
            None
        } else {
            let buf = std::io::BufReader::new(f.unwrap());
            let file_data = buf
                .lines()
                .collect_vec()
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .join("");
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

pub fn get_main_version_from_server_version(version: &String) -> anyhow::Result<(i32, i32, i32)> {
    let mut version_vec = version.splitn(4, ".").collect_vec();
    version_vec.truncate(3);
    let res = version_vec
        .into_iter()
        .map(|x| x.parse::<i32>())
        .collect_tuple();
    match res {
        Some((Ok(a), Ok(b), Ok(c))) => Ok((a, b, c)),
        _ => Err(anyhow::anyhow!("Invalid version string: {}", version)),
    }
}

pub async fn get_server_version(taos: &Taos) -> anyhow::Result<String> {
    let version = taos.server_version().await;
    match version {
        Err(err) => anyhow::bail!(format!("Get TDengine server version error: {err:?}")),
        Ok(version) => Ok(version.to_string()),
    }
}

lazy_static::lazy_static! {
    static ref TABLE_COLUMN_NAME_REGEX: regex::Regex = regex::Regex::new(r"^[a-zA-Z][a-zA-Z0-9_]*$").unwrap();
}

pub fn validate_table_column_name(col_name: &str, name_value: &str) -> anyhow::Result<()> {
    if name_value.len() > 192 {
        bail!(
            "The {}: {} is too long, the max length is 192.",
            col_name,
            name_value
        );
    }

    if name_value.contains(".") {
        bail!(
            "The {}: {} is invalid, it should not contain the character: .",
            col_name,
            name_value
        );
    }

    if name_value.contains("`") {
        bail!(
            "The {}: {} is invalid, it should not contain the character: `",
            col_name,
            name_value
        );
    }

    // if !TABLE_COLUMN_NAME_REGEX.is_match(name) {
    //     bail!(
    //         "The {}: {} is invalid, contains illegal characters.",
    //         name_type,
    //         name
    //     );
    // }
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
        "create stable stb1 (ts timestamp, v int) tags(t1 int)".to_string(),
        "create table ctb1 using stb1 tags(1)".to_string(),
        "create table ctb2 using stb1 tags(2)".to_string(),
        "create table ntb1 (ts timestamp, v int)".to_string(),
        "create table ntb2 (ts timestamp, v int)".to_string(),
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
async fn test_get_main_version_from_server_version() -> anyhow::Result<()> {
    let version = "3.0.5.0";
    assert_eq!(
        (3, 0, 5,),
        get_main_version_from_server_version(&version.to_string())?
    );
    let version = "3.0.5.0.2023061722";
    assert_eq!(
        (3, 0, 5,),
        get_main_version_from_server_version(&version.to_string())?
    );
    let version = "3.0";
    assert_eq!(
        Err("error"),
        get_main_version_from_server_version(&version.to_string()).map_err(|_err| "error")
    );
    let version = "a.b";
    assert_eq!(
        Err("error"),
        get_main_version_from_server_version(&version.to_string()).map_err(|_err| "error")
    );
    let version = "ab";
    assert_eq!(
        Err("error"),
        get_main_version_from_server_version(&version.to_string()).map_err(|_err| "error")
    );
    Ok(())
}
