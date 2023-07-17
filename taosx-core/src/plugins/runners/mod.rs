use std::path::{Path, PathBuf};

pub mod influxdb;
pub mod mqtt;
pub mod opc;
pub mod pi;

const ENV_PLUGINS_HOME: &'static str = "PLUGINS_HOME";
const ENV_PLUGINS_HOME_DEFAULT: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\Program Files\\taosX\\plugins"
        } else {
            "/usr/local/taosx/plugins"
        }
    }
};

#[inline]
pub fn get_plugins_home_dir() -> PathBuf {
    const ENV_TAOSX_PLUGINS_HOME: &'static str = "TAOSX_PLUGINS_HOME";
    let env = std::env::var(ENV_PLUGINS_HOME)
        .or(std::env::var(ENV_TAOSX_PLUGINS_HOME))
        .unwrap_or_else(|_| ENV_PLUGINS_HOME_DEFAULT.to_string());
    Path::new(&env).to_path_buf()
}

#[inline]
pub(crate) fn get_plugin_dir(plugin: &str) -> PathBuf {
    get_plugins_home_dir().join(plugin)
}

const ENV_TAOSX_LOGS_HOME: &'static str = "TAOSX_LOGS_HOME";
const ENV_TAOSX_LOGS_HOME_DEFAULT: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\Program Files\\taosX\\logs"
        } else {
            "/usr/local/taosx/logs"
        }
    }
};

#[inline]
pub fn get_logs_home_dir() -> PathBuf {
    let env = std::env::var(ENV_TAOSX_LOGS_HOME)
        .unwrap_or_else(|_| ENV_TAOSX_LOGS_HOME_DEFAULT.to_string());
    Path::new(&env).to_path_buf()
}

#[inline]
pub fn get_log_dir(plugin: &str) -> PathBuf {
    get_logs_home_dir().join(plugin)
}

#[inline]
pub fn get_log_keep_days() -> i64 {
    const ENV_TAOSX_LOGS_KEEP_DAYS: &'static str = "TAOSX_LOGS_KEEP_DAYS";
    const DEFAULT_LOGS_KEEP_DAYS: i64 = 30;
    std::env::var(ENV_TAOSX_LOGS_KEEP_DAYS)
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(DEFAULT_LOGS_KEEP_DAYS)
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
    plugins
}

#[test]
fn info() {
    let info = get_plugins_info();
    dbg!(info);
}
