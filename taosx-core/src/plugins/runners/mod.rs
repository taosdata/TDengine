use std::path::{Path, PathBuf};

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

#[test]
fn info() {
    let info = get_plugins_info();
    dbg!(info);
}
