use file_rotate::compression::Compression;
use file_rotate::suffix::{AppendTimestamp, DateFrom, FileLimit};
use file_rotate::{ContentLimit, FileRotate, TimeFrequency};
use itertools::Itertools;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use taos::Dsn;

mod config;
pub mod historian;
pub mod influxdb;
pub mod kafka;
pub mod mqtt;
pub mod opc;
pub mod opentsdb;
pub mod pi;

const ENV_PLUGINS_HOME: &'static str = "PLUGINS_HOME";
const ENV_TAOSX_PLUGINS_HOME: &'static str = "TAOSX_PLUGINS_HOME";
const ENV_PLUGINS_HOME_DEFAULT: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\TDengine\\plugins"
        } else {
            "/usr/local/taos/plugins"
        }
    }
};

pub fn set_env_plugins_home_dir(config: Option<String>) {
    // 使用配置、环境变量、默认值
    if let Some(plugins_home_dir) = config {
        std::env::set_var(ENV_PLUGINS_HOME, plugins_home_dir);
    } else {
        let plugins_home_dir = std::env::var(ENV_TAOSX_PLUGINS_HOME);
        match plugins_home_dir {
            Ok(home) => std::env::set_var(ENV_PLUGINS_HOME, home),
            Err(_) => {
                #[cfg(unix)]
                {
                    // 新版本默认路径
                    let default = "/usr/local/taos/plugins";
                    let path = Path::new(default);
                    if path.exists() {
                        std::env::set_var(ENV_PLUGINS_HOME, default);
                    } else {
                        // 兼容旧版本默认路径
                        let default = "/usr/local/taosx/plugins";
                        let path = Path::new(default);
                        if path.exists() {
                            std::env::set_var(ENV_PLUGINS_HOME, default);
                        }
                        // 兼容日志路径
                        let logs_home = std::env::var(ENV_LOGS_HOME).or(std::env::var(ENV_TAOSX_LOGS_HOME));
                        match logs_home {
                            Ok(home) => {
                                std::env::set_var(ENV_LOGS_HOME, home);
                            }
                            Err(_) => {
                                #[cfg(unix)]
                                {
                                    // 优先判断旧版
                                    let default = "/usr/local/taosx/logs";
                                    let path = Path::new(default);
                                    if path.exists() {
                                        std::env::set_var(ENV_LOGS_HOME, default);
                                    } else {
                                        let default = "/var/log/taos/";
                                        std::env::set_var(ENV_LOGS_HOME, default);
                                    }
                                }
                            }
                        }
                        // 兼容数据路径
                        let data_dir = std::env::var(ENV_TAOSX_DATA_DIR).ok();
                        match data_dir {
                            Some(_) => (),
                            None => {
                                #[cfg(unix)]
                                {
                                    // 优先判断旧版
                                    let default = "/usr/local/taosx";
                                    let path = Path::new(default);
                                    if path.exists() {
                                        std::env::set_var(ENV_TAOSX_DATA_DIR, default);
                                    } else {
                                        let default = "/var/lib/taos/taosx";
                                        std::env::set_var(ENV_TAOSX_DATA_DIR, default);
                                    }
                                }
                            }
                        }
                    }
                }
                // windows及未赋值成功时，在取路径时使用默认值
            }
        }
    }
}

#[inline]
pub fn get_plugins_home_dir() -> PathBuf {
    Path::new(&std::env::var(ENV_PLUGINS_HOME).unwrap_or(ENV_PLUGINS_HOME_DEFAULT.to_string())).to_path_buf()
}

#[inline]
pub(crate) fn get_plugin_dir(plugin: &str) -> PathBuf {
    get_plugins_home_dir().join(plugin)
}

const ENV_TAOSX_DATA_DIR: &'static str = "TAOSX_DATA_DIR";
const ENV_TAOSX_DATA_DIR_DEFAULT: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\TDengine\\data\\taosx"
        } else {
            "/var/lib/taos/taosx"
        }
    }
};

pub fn set_env_data_dir(config: Option<String>) {
    if let Some(data_dir) = config {
        std::env::set_var(ENV_TAOSX_DATA_DIR, data_dir);
    } else {
        std::env::set_var(ENV_TAOSX_DATA_DIR, ENV_TAOSX_DATA_DIR_DEFAULT.to_string());
    }
}

#[inline]
pub fn get_data_dir() -> PathBuf {
    Path::new(&std::env::var(ENV_TAOSX_DATA_DIR).unwrap()).to_path_buf()
}

#[inline]
pub fn get_file_upload_home_dir() -> PathBuf {
    get_data_dir().join("files")
}

const ENV_LOGS_HOME: &'static str = "LOGS_HOME";
const ENV_TAOSX_LOGS_HOME: &'static str = "TAOSX_LOGS_HOME";
const ENV_LOGS_HOME_DEFAULT: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\TDengine\\log"
        } else {
            "/var/log/taos"
        }
    }
};

pub fn set_env_log_home_dir(config: Option<String>) {
    if let Some(log_home_dir) = config {
        std::env::set_var(ENV_LOGS_HOME, log_home_dir);
    } else {
        let log_home_dir = std::env::var(ENV_TAOSX_LOGS_HOME).unwrap_or(ENV_LOGS_HOME_DEFAULT.to_string());
        std::env::set_var(ENV_LOGS_HOME, log_home_dir);
    }
}

#[inline]
pub fn get_logs_home_dir() -> PathBuf {
    Path::new(&std::env::var(ENV_LOGS_HOME).unwrap()).to_path_buf()
}

#[inline]
pub fn get_log_dir(plugin: &str) -> PathBuf {
    get_logs_home_dir().join(plugin)
}

const ENV_TAOSX_LOGS_KEEP_DAYS: &'static str = "TAOSX_LOGS_KEEP_DAYS";

pub fn set_env_log_keep_days(config: Option<i64>) {
    if let Some(log_keep_days) = config {
        if log_keep_days > 0 && valid_env_log_keep_days().is_none() {
            std::env::set_var(ENV_TAOSX_LOGS_KEEP_DAYS, log_keep_days.to_string());
        }
    }
}

#[inline]
fn valid_env_log_keep_days() -> Option<i64> {
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

/// get string value from dsn's key
/// line_break: push \n between lines if is true
/// append_line: push append_line between lines if is not None
pub fn get_string_from_param_or_file(
    dsn: &mut Dsn,
    key: &str,
    line_break: bool,
    append_line: Option<&str>,
) -> Result<Option<String>, String> {
    if let Some(value) = dsn.remove(key) {
        let (files, config): (Vec<_>, Vec<_>) = value
            .split(",")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .partition(|v| v.starts_with("@"));
        let mut result = String::new();
        for config_str in config {
            if line_break && !result.is_empty() {
                result.push_str("\n");
            }
            if append_line.is_some() && !result.is_empty() {
                result.push_str(append_line.unwrap());
            }
            result.push_str(&config_str);
        }
        for file in files {
            let f = std::fs::File::open(&file[1..]);
            if f.is_err() {
                return Err("file read error".to_string());
            }
            let buf = std::io::BufReader::new(f.unwrap());
            let file_data = buf.lines().collect_vec();
            file_data
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .for_each(|v| {
                    if line_break && !result.is_empty() {
                        result.push_str("\n");
                    }
                    if append_line.is_some() && !result.is_empty() {
                        result.push_str(append_line.unwrap());
                    }
                    result.push_str(v.as_str());
                });
        }
        Ok(Some(result))
    } else {
        Ok(None)
    }
}

/// get string vector from dsn's key. if value starts with @, read file.
/// the first line in file will be skipped, the rest will be read as a string per line, replace `,` with `::` and push to vector
/// if value not starts with @, the value will split by `,` and push to vector
pub fn get_string_vec_from_param_or_file(dsn: &mut Dsn, key: &str) -> Result<Vec<String>, String> {
    if let Some(nodes) = dsn.remove(key) {
        let (files, mut node_config): (Vec<_>, Vec<_>) = nodes
            .split(",")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .partition(|v| v.starts_with("@"));
        for file in files {
            tracing::info!(
                "current log: {}",
                std::env::current_dir().unwrap().to_str().unwrap()
            );
            let f = std::fs::File::open(&file[1..]);
            if f.is_err() {
                tracing::warn!(
                    "file: {} read error, cause: {}",
                    &file[1..],
                    f.err().unwrap()
                );
                continue;
            }
            let buf = std::io::BufReader::new(f.unwrap());
            let mut file_data = buf.lines().collect_vec();
            // remove header
            if file_data.remove(0).is_err() {
                tracing::warn!("file: {} content length < 1", file);
            }

            node_config.extend(
                file_data
                    .iter()
                    .filter_map(|r| r.as_ref().ok())
                    .map(|s| s.replace(",", "::")),
            );
        }
        if node_config.len() == 0 {
            tracing::warn!("node config is empty");
            // return Err(format!("node config set but is empty: {nodes}"));
        }
        return Ok(node_config);
    }
    return Err("Nodes not set".to_string());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn info() {
        let info = get_plugins_info();
        dbg!(info);
    }

    #[test]
    fn test_get_string_vec_from_param_or_file() {
        let mut dsn = Dsn::from_str("driver://?topics=1,2,3").unwrap();
        let topics = get_string_vec_from_param_or_file(&mut dsn, "topics").unwrap();
        assert_eq!(vec!["1", "2", "3"], topics);

        let mut dsn = Dsn::from_str("driver://?topics=@../tests/mqtt/topics").unwrap();
        let topics = get_string_vec_from_param_or_file(&mut dsn, "topics").unwrap();
        assert_eq!(vec!["a::b::c", "1::2::3"], topics);
    }

    #[test]
    fn test_get_string_from_param_or_file() {
        let mut dsn =
            Dsn::from_str("driver:///?ca=123,456,@../tests/mqtt/ca,@../tests/mqtt/ca").unwrap();
        let result = get_string_from_param_or_file(&mut dsn, "ca", true, None)
            .unwrap()
            .unwrap();
        assert_eq!(
            "123
456
-----BEGIN CERTIFICATE-----
MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV
-----END CERTIFICATE-----",
            result
        );

        let mut dsn =
            Dsn::from_str("driver:///?ca=123,456,@../tests/mqtt/ca,@../tests/mqtt/ca").unwrap();
        let result = get_string_from_param_or_file(&mut dsn, "ca", false, None)
            .unwrap()
            .unwrap();
        assert_eq!("123456-----BEGIN CERTIFICATE-----MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV-----END CERTIFICATE----------BEGIN CERTIFICATE-----MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV-----END CERTIFICATE-----", result);

        let mut dsn =
            Dsn::from_str("driver:///?ca=123,456,@../tests/mqtt/ca,@../tests/mqtt/ca").unwrap();
        let result = get_string_from_param_or_file(&mut dsn, "ca", false, Some(","))
            .unwrap()
            .unwrap();
        assert_eq!("123,456,-----BEGIN CERTIFICATE-----,MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV,-----END CERTIFICATE-----,-----BEGIN CERTIFICATE-----,MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV,-----END CERTIFICATE-----", result);
    }
}
