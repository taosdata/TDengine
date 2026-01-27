use anyhow::bail;
use chrono::{DateTime, FixedOffset, Utc};
use serde::ser::StdError;
use std::path::PathBuf;
use std::time::Duration;
use std::{io::BufRead, path::Path, thread::JoinHandle};
use taos::*;
use tokio_util::sync::CancellationToken;

use taosx_utils::dsn::json_to_dsn;

pub mod breakpoints;
pub mod cert;
pub mod codec;
pub mod constants;
pub mod defer;
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
pub mod table_meta;
pub mod timeout;
pub mod trace;

pub use duration::parse_duration;

use crate::get_data_dir;

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
        (Value::Decimal(l0), Value::Decimal(r0)) | (Value::Decimal64(l0), Value::Decimal64(r0)) => {
            l0 == r0
        }
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

pub fn try_mask_dsn(dsn: &serde_json::Value) -> Option<String> {
    // dsn.parse()
    json_to_dsn(dsn)
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
        taos.exec(format!("DROP STABLE `{name}`")).await?;
    }

    let mut tables = taos.query("SHOW TABLES").await?;
    let mut rows = tables.rows();

    while let Some(mut row) = rows.try_next().await? {
        let name = format!("{}", row.next().unwrap().1);
        taos.exec(format!("DROP TABLE `{name}`")).await?;
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
            anyhow::bail!("file: {} read error, cause: {}", file, err);
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
    file.and_then(|file| {
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
    })
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

pub async fn clear_local_dir(dir: &str) -> anyhow::Result<()> {
    let path = Path::new(dir);
    if path.exists() {
        tokio::fs::remove_dir_all(path).await?;
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

    Ok(())
}

/// use the date to replace the placeholder in the string
///
/// # Examples
///
/// ${Y} -> year 2021
/// ${y} -> year 21
/// ${m} -> month 01
/// ${M} -> month 1
/// ${b} -> month Jan
/// ${B} -> month January
/// ${d} -> day 01
/// ${D} -> day 1
/// ${j} -> day of year 001
/// ${J} -> day of year 1
/// ${F} -> date 2021-01-01
/// ${Ymd} -> date 20210101
/// ${ymd} -> date 210101
/// ${md} -> date 0101
/// ${dm} -> date 0101
/// ${Yj} -> date 2021001
/// ${yj} -> date 21001
///
pub fn replace_date_placeholder(str: String, date: DateTime<FixedOffset>) -> String {
    str.replace("${Y}", date.format("%Y").to_string().as_str())
        .replace("${y}", date.format("%y").to_string().as_str())
        .replace("${m}", date.format("%m").to_string().as_str())
        .replace(
            "${M}",
            date.format("%m").to_string().trim_start_matches("0"),
        )
        .replace("${b}", date.format("%b").to_string().as_str())
        .replace("${B}", date.format("%B").to_string().as_str())
        .replace("${d}", date.format("%d").to_string().as_str())
        .replace(
            "${D}",
            date.format("%d").to_string().trim_start_matches("0"),
        )
        .replace("${j}", date.format("%j").to_string().as_str())
        .replace(
            "${J}",
            date.format("%j").to_string().trim_start_matches("0"),
        )
        .replace("${F}", date.format("%F").to_string().as_str())
        .replace("${Ymd}", date.format("%Y%m%d").to_string().as_str())
        .replace("${ymd}", date.format("%y%m%d").to_string().as_str())
        .replace("${md}", date.format("%m%d").to_string().as_str())
        .replace("${dm}", date.format("%d%m").to_string().as_str())
        .replace("${Yj}", date.format("%Y%j").to_string().as_str())
        .replace("${yj}", date.format("%y%j").to_string().as_str())
}

pub fn parse_keys_in_dsn<T: std::str::FromStr>(
    dsn: &Dsn,
    keys: &[&str],
) -> anyhow::Result<Option<T>>
where
    <T as std::str::FromStr>::Err: std::fmt::Debug,
    <T as std::str::FromStr>::Err: StdError,
    <T as std::str::FromStr>::Err: Send,
    <T as std::str::FromStr>::Err: Sync,
    <T as std::str::FromStr>::Err: 'static,
{
    for key in keys {
        let val = parse_key_in_dsn(dsn, key)?;
        if let Some(val) = val {
            return anyhow::Ok(Some(val));
        }
    }
    Ok(None)
}

pub fn parse_key_in_dsn<T: std::str::FromStr>(dsn: &Dsn, key: &str) -> anyhow::Result<Option<T>>
where
    <T as std::str::FromStr>::Err: std::fmt::Debug,
    <T as std::str::FromStr>::Err: StdError,
    <T as std::str::FromStr>::Err: Send,
    <T as std::str::FromStr>::Err: Sync,
    <T as std::str::FromStr>::Err: 'static,
{
    dsn.get(key)
        .filter(|s| !s.is_empty())
        .map(|val| {
            val.parse::<T>()
                .map_err(|err| anyhow::Error::from(err).context(format!("invalid {key}: {val}")))
        })
        .transpose()
}

pub fn parse_duration_in_dsn(dsn: &Dsn, key: &str) -> anyhow::Result<Option<Duration>> {
    parse_key_in_dsn::<String>(dsn, key)?
        .map(|val| {
            fundu::parse_duration(val.as_str())
                .map_err(|err| anyhow::Error::from(err).context(format!("invalid {key}: {val}")))
        })
        .transpose()
}

pub fn parse_datetime_in_dsn(dsn: &Dsn, key: &str) -> anyhow::Result<Option<DateTime<Utc>>> {
    parse_key_in_dsn::<String>(dsn, key)?
        .map(|val| {
            if val.to_lowercase() == "now" {
                return Ok(Utc::now());
            }
            DateTime::parse_from_rfc3339(val.as_str())
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|err| anyhow::Error::from(err).context(format!("invalid {key}: {val}")))
        })
        .transpose()
}

pub fn parse_local_datetime_in_dsn(
    dsn: &Dsn,
    key: &str,
) -> anyhow::Result<Option<DateTime<chrono::Local>>> {
    parse_key_in_dsn::<String>(dsn, key)?
        .map(|val| {
            if val.to_lowercase() == "now" {
                return Ok(chrono::Local::now());
            }
            DateTime::parse_from_rfc3339(val.as_str())
                .map(|dt| dt.with_timezone(&chrono::Local))
                .map_err(|err| anyhow::Error::from(err).context(format!("invalid {key}: {val}")))
        })
        .transpose()
}

/// 从 DSN 的参数中解析目录路径，返回绝对路径，但不保证路径存在
pub fn parse_dir_in_dsn(dsn: &Dsn, key: Option<&str>) -> anyhow::Result<Option<PathBuf>> {
    let p = match key {
        None => dsn.path.as_ref().filter(|p| !p.is_empty()),
        Some(key) => dsn.get(key).filter(|s| !s.is_empty()),
    };

    p.map(|p| {
        PathBuf::from(p)
            .canonicalize()
            .map_err(|err| anyhow::Error::new(err).context(format!("invalid path: {p}")))
    })
    .transpose()
}

/// 解析 dsn 中的备份目录 local:/<BACKUP_DIR>
pub fn parse_backup_dir(dsn: &Dsn, task_job_id: Option<(i64, i64)>) -> anyhow::Result<PathBuf> {
    let mut dir = match parse_dir_in_dsn(dsn, None)? {
        // dir 为空，使用默认路径: $TAOSX_DATA_DIR/backup
        None => {
            let default_dir = get_data_dir().join("backup");
            // 如果 $TAOSX_DATA_DIR/backup 不存在，则创建
            if !default_dir.exists() {
                std::fs::create_dir_all(&default_dir).map_err(|err| {
                    anyhow::Error::new(err).context(format!(
                        "failed to create backup dir: {}",
                        default_dir.display()
                    ))
                })?;
                tracing::info!("create backup dir: {}", default_dir.display());
            }
            default_dir
        }
        // 用户指定的 dir
        Some(dir) => {
            // 如果 dir 不存在，则报错
            if !dir.exists() {
                bail!("backup dir not exists: {}", dir.display());
            }
            dir
        }
    };

    if let Some((task_id, job_id)) = task_job_id {
        dir = dir.join(task_id.to_string()).join(job_id.to_string());
    }

    Ok(dir)
}

/// 解析 dsn 中的压缩等级参数
pub fn parse_compression_in_dsn(
    dsn: &Dsn,
    keys: &[&str],
) -> anyhow::Result<Option<async_compression::Level>> {
    parse_keys_in_dsn::<String>(dsn, keys)?
        .map(|s| {
            let level = s.to_lowercase();
            match level.as_str() {
                "fastest" => Ok(async_compression::Level::Fastest),
                "best" => Ok(async_compression::Level::Best),
                "default" | "balanced" => Ok(async_compression::Level::Default),
                _ => level
                    .parse::<i32>()
                    .map_err(|err| {
                        anyhow::Error::from(err).context(format!("invalid compression level: {s}"))
                    })
                    .map(async_compression::Level::Precise),
            }
        })
        .transpose()
}

pub fn parse_bytes(size_str: &str) -> anyhow::Result<u64> {
    let size_str = size_str.trim();
    let (number, unit) = size_str.split_at(
        size_str
            .find(|c: char| !c.is_numeric() && c != '.')
            .unwrap_or(size_str.len()),
    );
    let number: f64 = number.trim().parse()?;
    let bytes = match unit.trim().to_uppercase().as_str() {
        "B" => number,
        "KB" => number * 1024.0,
        "MB" => number * 1024.0 * 1024.0,
        "GB" => number * 1024.0 * 1024.0 * 1024.0,
        _ => bail!("invalid unit: {}", size_str),
    };
    Ok(bytes as u64)
}

pub fn contains_uppercase(s: &str) -> bool {
    for c in s.chars() {
        if c.is_uppercase() {
            return true;
        }
    }
    false
}

/// 如果当前时间比 upcoming 早，等待到 upcoming
/// 如果等待期间收到取消信号则立刻返回错误
pub async fn wait_for_upcoming(
    upcoming: Option<DateTime<Utc>>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    if let Some(upcoming) = upcoming {
        let now = Utc::now();
        if now < upcoming {
            let duration = upcoming - now;
            tracing::info!("wait for upcoming: {}", upcoming);
            let dur = duration.to_std().map_err(|err| {
                anyhow::Error::from(err)
                    .context(format!("failed to convert: {:?} to std duration", duration))
            })?;
            tokio::select! {
                _ = tokio::time::sleep(dur) => {},
                _ = cancel.cancelled() => {
                    anyhow::bail!("cancelled while waiting for upcoming");
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Local;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_bytes() {
        assert_eq!(1024, parse_bytes("1KB").unwrap());
        assert_eq!(1024, parse_bytes("1kb").unwrap());
        assert_eq!(1024, parse_bytes("1 KB").unwrap());
        assert_eq!(1024, parse_bytes("1 KB").unwrap());

        assert_eq!(123, parse_bytes("123B").unwrap());
        assert_eq!(456 * 1024, parse_bytes("456KB").unwrap());
        assert_eq!(789 * 1024 * 1024, parse_bytes("789MB").unwrap());
        assert_eq!(
            (1.23 * 1024.0 * 1024.0 * 1024.0) as u64,
            parse_bytes("1.23GB").unwrap()
        );
    }

    #[test]
    fn test_parse_dir_in_dsn() {
        let dsn = "local:./".into_dsn().unwrap();
        let dir = parse_dir_in_dsn(&dsn, None).unwrap().unwrap();
        assert_eq!(PathBuf::from(".").canonicalize().unwrap(), dir);

        let dsn = "local:/".into_dsn().unwrap();
        let dir = parse_dir_in_dsn(&dsn, None).unwrap().unwrap();
        assert_eq!(PathBuf::from("/").canonicalize().unwrap(), dir);

        let dsn = "local://".into_dsn().unwrap();
        let dir = parse_dir_in_dsn(&dsn, None).unwrap();
        assert!(dir.is_none());

        let dsn = "local://?dir=./".into_dsn().unwrap();
        let dir = parse_dir_in_dsn(&dsn, Some("dir")).unwrap().unwrap();
        assert_eq!(PathBuf::from(".").canonicalize().unwrap(), dir);

        let dsn = "local://?dir=/".into_dsn().unwrap();
        let dir = parse_dir_in_dsn(&dsn, Some("dir")).unwrap().unwrap();
        assert_eq!(PathBuf::from("/").canonicalize().unwrap(), dir);

        let dsn = "local://?dir=".into_dsn().unwrap();
        let dir = parse_dir_in_dsn(&dsn, Some("dir")).unwrap();
        assert!(dir.is_none());
    }

    #[test]
    fn test_parse_datetime_in_dsn() {
        let now = Utc::now();
        let dsn = "tmq://?upcoming=now".into_dsn().unwrap();
        let upcoming = parse_datetime_in_dsn(&dsn, "upcoming").unwrap().unwrap();
        assert!(upcoming - now < chrono::Duration::seconds(1));

        let now = Utc::now();
        let dsn = format!("tmq://?upcoming={}", now.to_rfc3339())
            .into_dsn()
            .unwrap();
        let upcoming = parse_datetime_in_dsn(&dsn, "upcoming").unwrap().unwrap();
        assert_eq!(upcoming, now);

        let now = Local::now();
        let dsn = format!("local://?from={}", now.to_rfc3339())
            .into_dsn()
            .unwrap();
        let upcoming = parse_datetime_in_dsn(&dsn, "from").unwrap().unwrap();
        assert_eq!(upcoming, now.with_timezone(&Utc));

        let dsn = "tmq://".into_dsn().unwrap();
        let upcoming = parse_datetime_in_dsn(&dsn, "upcoming").unwrap();
        assert!(upcoming.is_none());

        let dsn = "tmq://?upcoming=".into_dsn().unwrap();
        let upcoming = parse_datetime_in_dsn(&dsn, "upcoming").unwrap();
        assert!(upcoming.is_none());

        let dsn = "tmq://?upcoming=abc".into_dsn().unwrap();
        let upcoming = parse_datetime_in_dsn(&dsn, "upcoming");
        assert!(upcoming.is_err());
        assert_eq!(upcoming.unwrap_err().to_string(), "invalid upcoming: abc");
    }

    #[test]
    fn test_parse_duration_in_dsn() {
        let dsn = "taos://?timeout=1s".into_dsn().unwrap();
        let duration = parse_duration_in_dsn(&dsn, "timeout").unwrap();
        assert_eq!(Some(Duration::from_secs(1)), duration);

        let dsn = "taos://?timeout=".into_dsn().unwrap();
        assert_eq!(None, parse_duration_in_dsn(&dsn, "timeout").unwrap());

        let dsn = "taos://?timeout=abc".into_dsn().unwrap();
        let err = parse_duration_in_dsn(&dsn, "timeout");
        assert!(err.is_err());
        assert_eq!("invalid timeout: abc", err.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_keys_in_dsn() {
        let dsn = "taos://?error_max_retry=3".into_dsn().unwrap();
        let val = parse_keys_in_dsn::<u32>(&dsn, &["error.max.retry", "error_max_retry"]).unwrap();
        assert_eq!(Some(3u32), val);

        let dsn = "taos://?error_max_retry=".into_dsn().unwrap();
        let val = parse_keys_in_dsn::<u32>(&dsn, &["error.max.retry", "error_max_retry"]).unwrap();
        assert_eq!(None, val);

        let dsn = "taos://?error_max_retry=abc".into_dsn().unwrap();
        let err = parse_keys_in_dsn::<u32>(&dsn, &["error.max.retry", "error_max_retry"]);
        assert!(err.is_err());
        assert_eq!("invalid error_max_retry: abc", err.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_key_in_dsn() {
        let dsn = "taos://?error.max.retry=3".into_dsn().unwrap();
        assert_eq!(
            Some(3u32),
            parse_key_in_dsn::<u32>(&dsn, "error.max.retry").unwrap()
        );

        let dsn = "taos://?error.max.retry=".into_dsn().unwrap();
        assert_eq!(
            None,
            parse_key_in_dsn::<u32>(&dsn, "error.max.retry").unwrap()
        );

        let dsn = "taos://?error.max.retry=abc".into_dsn().unwrap();
        let err = parse_key_in_dsn::<u32>(&dsn, "error.max.retry");
        assert!(err.is_err());
        assert_eq!("invalid error.max.retry: abc", err.unwrap_err().to_string());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_clear_database_with_taos() -> anyhow::Result<()> {
        let dsn = "taos:///";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

        let db = "test_clear_database";
        taos.exec_many([
            format!("drop database if exists {db}"),
            format!("create database {db}"),
            format!("use {db}"),
            "create stable `Stb1` (ts timestamp, v int) tags(t1 int)".to_string(),
            "create table `Ctb1` using `Stb1` tags(1)".to_string(),
            "create table `Ctb2` using `Stb1` tags(2)".to_string(),
            "create table `Ntb1` (ts timestamp, v int)".to_string(),
            "create table `Ntb2` (ts timestamp, v int)".to_string(),
        ])
        .await?;

        use std::str::FromStr;

        clear_database(&Dsn::from_str(&format!("{dsn}{db}"))?).await?;

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

    #[test]
    fn test_contains_uppercase() {
        assert!(contains_uppercase("sasl.isEnable"));
        assert!(contains_uppercase("aBc"));
        assert!(contains_uppercase("ABC"));
        assert!(!contains_uppercase("123"));
        assert!(!contains_uppercase("abc"));
        assert!(contains_uppercase("123A"));
    }

    #[tokio::test]
    async fn test_wait_for_upcoming() {
        let cancel = tokio_util::sync::CancellationToken::new();

        let now = Utc::now();
        wait_for_upcoming(Some(now + chrono::Duration::seconds(2)), cancel.clone())
            .await
            .unwrap();
        let current = Utc::now();
        assert_eq!(current.timestamp() - now.timestamp(), 2);

        let now = Utc::now();
        wait_for_upcoming(None, cancel.clone()).await.unwrap();
        let current = Utc::now();
        assert_eq!(current.timestamp() - now.timestamp(), 0);

        let now = Utc::now();
        wait_for_upcoming(Some(now - chrono::Duration::days(1)), cancel.clone())
            .await
            .unwrap();
        let current = Utc::now();
        assert_eq!(current.timestamp() - now.timestamp(), 0);

        let now = Utc::now();
        let cancel_clone = cancel.clone();
        // 在 100ms 后触发取消
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            cancel_clone.cancel();
        });
        // 原计划等待 2s，但应在取消后尽快返回错误
        let res = wait_for_upcoming(Some(now + chrono::Duration::seconds(2)), cancel).await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "cancelled while waiting for upcoming"
        );
    }

    #[test]
    fn test_parse_backup_dir() {
        let dsn = "local:/tmp".into_dsn().unwrap();
        let task_id = Some((123, 1));
        let backup_dir = parse_backup_dir(&dsn, task_id).unwrap();

        let cur_dir = Path::new("/tmp")
            .canonicalize()
            .unwrap()
            .join("123")
            .join("1");
        assert_eq!(backup_dir, cur_dir);
    }

    /// 测试解析备份文件的压缩等级
    /// 测试用例：
    /// 1. compression.level=fastest
    /// 2. compression.level=best
    /// 3. compression.level=default
    /// 4. compression.level=balanced
    /// 5. compression.level=5
    /// 6. compression.level=
    /// 7. compression.level=abc
    /// 8. 不包含 compression.level
    #[test]
    fn test_parse_compression_level() {
        let dsn = "local:/tmp?compression.level=fastest".into_dsn().unwrap();
        let level = parse_compression_in_dsn(&dsn, &["compression.level"])
            .unwrap()
            .unwrap();
        assert_eq!("Fastest", format!("{:?}", level));

        let dsn = "local:/tmp?compression.level=best".into_dsn().unwrap();
        let level = parse_compression_in_dsn(&dsn, &["compression.level"])
            .unwrap()
            .unwrap();
        assert_eq!("Best", format!("{:?}", level));

        let dsn = "local:/tmp?compression.level=default".into_dsn().unwrap();
        let level = parse_compression_in_dsn(&dsn, &["compression.level"])
            .unwrap()
            .unwrap();
        assert_eq!("Default", format!("{:?}", level));

        let dsn = "local:/tmp?compression.level=balanced".into_dsn().unwrap();
        let level = parse_compression_in_dsn(&dsn, &["compression.level"])
            .unwrap()
            .unwrap();
        assert_eq!("Default", format!("{:?}", level));

        let dsn = "local:/tmp?compression.level=5".into_dsn().unwrap();
        let level = parse_compression_in_dsn(&dsn, &["compression.level"])
            .unwrap()
            .unwrap();
        assert_eq!("Precise(5)", format!("{:?}", level));

        let dsn = "local:/tmp".into_dsn().unwrap();
        let level = parse_compression_in_dsn(&dsn, &["compression.level"]).unwrap();
        assert!(level.is_none());

        let dsn = "local:/tmp?compression.level=".into_dsn().unwrap();
        let level = parse_compression_in_dsn(&dsn, &["compression.level"]).unwrap();
        assert!(level.is_none());

        let dsn = "local:/tmp?compression.level=abc".into_dsn().unwrap();
        let level = parse_compression_in_dsn(&dsn, &["compression.level"]);
        assert!(level.is_err());
        assert_eq!(
            "invalid compression level: abc",
            format!("{}", level.err().unwrap())
        );
    }

    #[test]
    fn test_mask_dsn_and_value_equals() {
        let dsn = "taos://user:pass@localhost:6030/db?param=1"
            .into_dsn()
            .unwrap();
        let masked = mask_dsn(&dsn);
        assert!(masked.username.is_none());
        assert!(masked.password.is_none());
        assert!(masked.params.is_empty());
        assert_eq!(masked.driver, dsn.driver);
        assert_eq!(masked.addresses, dsn.addresses);

        assert!(value_equals(&Value::Int(5), &Value::Int(5)));
        assert!(value_equals(
            &Value::VarChar("abc".into()),
            &Value::NChar("abc".into())
        ));
        assert!(!value_equals(&Value::Int(1), &Value::BigInt(1)));
    }

    #[test]
    fn test_get_string_content_from_param_value() -> anyhow::Result<()> {
        let mut tmp = NamedTempFile::new()?;
        writeln!(tmp, "file_line")?;
        let param = format!("@{},inline", tmp.path().display());
        let file_first = get_string_content_from_param_value(&param, false, true)?;
        assert_eq!(Some("file_line".to_string()), file_first);

        let param_inline = "alpha,beta";
        let inline_only = get_string_content_from_param_value(param_inline, true, true)?;
        assert_eq!(Some("alpha".to_string()), inline_only);

        let none = get_string_content_from_param_value("", true, true)?;
        assert!(none.is_none());
        Ok(())
    }

    #[test]
    fn test_replace_date_placeholder_and_validate() {
        let dt = DateTime::parse_from_rfc3339("2021-12-03T12:34:56+00:00").unwrap();
        let replaced = replace_date_placeholder("path/${Y}/${m}/${d}".to_string(), dt);
        assert_eq!("path/2021/12/03", replaced);

        assert!(validate_table_column_name("col", "valid_name").is_ok());
        assert!(validate_table_column_name("col", "has.dot").is_err());
        assert!(validate_table_column_name("col", "has`backtick").is_err());
        let long_name = "a".repeat(193);
        assert!(validate_table_column_name("col", &long_name).is_err());
    }
}
