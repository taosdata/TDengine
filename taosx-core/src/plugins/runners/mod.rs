use file_rotate::compression::Compression;
use file_rotate::suffix::{AppendTimestamp, DateFrom, FileLimit};
use file_rotate::{ContentLimit, FileRotate, TimeFrequency};
use std::path::{Path, PathBuf};

mod config;
pub mod historian;
pub mod influxdb;
pub mod kafka;
pub mod mqtt;
pub mod opc;
pub mod opentsdb;
pub mod pi;

pub const ENV_PLUGINS_HOME: &'static str = "PLUGINS_HOME";
const ENV_PLUGINS_HOME_DEFAULT: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\Program Files\\taos\\plugins"
        } else {
            "/usr/local/taos/plugins"
        }
    }
};

#[inline]
pub fn get_plugins_home_dir() -> PathBuf {
    let env = std::env::var(ENV_PLUGINS_HOME).unwrap_or_else(|_| ENV_PLUGINS_HOME_DEFAULT.to_string());
    Path::new(&env).to_path_buf()
}

#[inline]
pub(crate) fn get_plugin_dir(plugin: &str) -> PathBuf {
    get_plugins_home_dir().join(plugin)
}

pub const ENV_TAOSX_DATA_DIR: &'static str = "TAOSX_DATA_DIR";
const ENV_TAOSX_DATA_DIR_DEFAULT: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\Program Files\\taos\\data"
        } else {
            "/usr/local/taos/data"
        }
    }
};

#[inline]
pub fn get_data_dir() -> String {
    std::env::var(ENV_TAOSX_DATA_DIR).unwrap_or_else(|_| ENV_TAOSX_DATA_DIR_DEFAULT.to_string())
}

pub const ENV_TAOSX_LOGS_HOME: &'static str = "TAOSX_LOGS_HOME";
const ENV_TAOSX_LOGS_HOME_DEFAULT: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\Program Files\\taos\\logs"
        } else {
            "/var/log/taos"
        }
    }
};

#[inline]
pub fn get_logs_home_dir() -> PathBuf {
    let env = std::env::var(ENV_TAOSX_LOGS_HOME).unwrap_or_else(|_| ENV_TAOSX_LOGS_HOME_DEFAULT.to_string());
    Path::new(&env).to_path_buf()
}

#[inline]
pub fn get_log_dir(plugin: &str) -> PathBuf {
    get_logs_home_dir().join(plugin)
}

const ENV_TAOSX_UPLOAD_FILE_HOME: &'static str = "TAOSX_UPLOAD_FILE_HOME";
pub(crate) const ENV_TAOSX_UPLOAD_FILE_HOME_DEFAULT: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\Program Files\\taos\\files"
        } else {
            "/usr/local/taos/files"
        }
    }
};

#[inline]
pub fn get_file_upload_home_dir() -> PathBuf {
    let env = std::env::var(ENV_TAOSX_UPLOAD_FILE_HOME).unwrap_or_else(|_| ENV_TAOSX_UPLOAD_FILE_HOME_DEFAULT.to_string());
    std::path::Path::new(&env).to_path_buf()
}

pub const ENV_TAOSX_LOGS_KEEP_DAYS: &'static str = "TAOSX_LOGS_KEEP_DAYS";

#[inline]
pub fn valid_env_log_keep_days() -> Option<i64> {
    std::env::var(ENV_TAOSX_LOGS_KEEP_DAYS)
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| v > &0)
}

#[inline]
pub fn get_log_keep_days() -> i64 {
    const DEFAULT_LOGS_KEEP_DAYS: i64 = 30;
    if let Some(v) = valid_env_log_keep_days() {
        v
    } else {
        DEFAULT_LOGS_KEEP_DAYS
    }
}

pub fn get_plugins_info() -> Vec<(&'static str, PathBuf, String)> {
    let mut plugins = Vec::new();
    if let Ok(info) = opc::info() {
        plugins.push(info)
    }
    if let Ok(info) = mqtt::info() {
        plugins.push(info)
    }
    if let Ok(info) = influxdb::info() {
        plugins.push(info)
    }
    if let Ok(info) = opentsdb::info() {
        plugins.push(info)
    }
    plugins
}

pub fn log_rotation(log_path: &PathBuf, log_keep_days: i64) -> FileRotate<AppendTimestamp> {
    FileRotate::new(
        &log_path,
        AppendTimestamp::with_format(
            "%Y-%m-%d",
            FileLimit::Age(chrono::Duration::days(log_keep_days)),
            DateFrom::DateYesterday,
        ),
        ContentLimit::Time(TimeFrequency::Daily),
        Compression::None,
        #[cfg(unix)]
        None,
    )
}

#[test]
fn info() {
    let info = get_plugins_info();
    dbg!(info);
}
