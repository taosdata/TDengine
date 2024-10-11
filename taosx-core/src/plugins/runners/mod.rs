use std::borrow::Cow;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::time::Duration;

use file_rotate::compression::Compression;
use file_rotate::suffix::{AppendTimestamp, DateFrom, FileLimit};
use file_rotate::{ContentLimit, FileRotate, TimeFrequency};
use itertools::Itertools;
use rumqttc::tokio_rustls;
use rumqttc::tokio_rustls::rustls;
use rumqttc::tokio_rustls::rustls::client::danger::ServerCertVerifier;
use rumqttc::tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use taos::Dsn;

mod config;
pub mod historian;
pub mod influxdb;
pub mod kafka;
pub mod mongodb;
pub mod mqtt;
pub mod mssql;
pub mod mysql;
pub mod opc;
pub mod opentsdb;
pub mod oracle;
pub mod pi;
pub mod postgres;

pub const ENV_PLUGINS_HOME: &str = "PLUGINS_HOME";
pub const ENV_TAOSX_PLUGINS_HOME: &str = "TAOSX_PLUGINS_HOME";
const ENV_PLUGINS_HOME_DEFAULT: &str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\TDengine\\plugins"
        } else {
            "/usr/local/taos/plugins"
        }
    }
};

pub fn set_env_plugins_home_dir(config: String) {
    // 使用配置、环境变量、默认值
    if !config.trim().is_empty() {
        std::env::set_var(ENV_PLUGINS_HOME, config);
        return;
    }
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
                    let logs_home =
                        std::env::var(ENV_LOGS_HOME).or(std::env::var(ENV_TAOSX_LOGS_HOME));
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

#[inline]
pub fn get_plugins_home_dir() -> PathBuf {
    Path::new(&std::env::var(ENV_PLUGINS_HOME).unwrap_or(ENV_PLUGINS_HOME_DEFAULT.to_string()))
        .to_path_buf()
}

#[inline]
pub(crate) fn get_plugin_dir(plugin: &str) -> PathBuf {
    get_plugins_home_dir().join(plugin)
}

pub const ENV_TAOSX_DATA_DIR: &str = "TAOSX_DATA_DIR";
const ENV_TAOSX_DATA_DIR_DEFAULT: &str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "C:\\TDengine\\data\\taosx"
        } else {
            "/var/lib/taos/taosx"
        }
    }
};

/// set tcp keep alive
pub fn set_tcp_keepalive(stream: &std::net::TcpStream) -> anyhow::Result<()> {
    let sock_ref = socket2::SockRef::from(stream);
    let keep_alive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(10))
        .with_interval(Duration::from_secs(10));
    sock_ref.set_tcp_keepalive(&keep_alive)?;

    Ok(())
}

pub fn set_env_data_dir(config: String) {
    std::env::set_var(ENV_TAOSX_DATA_DIR, config);
}

#[inline]
pub fn get_data_dir() -> PathBuf {
    Path::new(
        std::env::var(ENV_TAOSX_DATA_DIR)
            .map(Cow::Owned)
            .unwrap_or(Cow::Borrowed(ENV_TAOSX_DATA_DIR_DEFAULT))
            .as_ref(),
    )
    .to_path_buf()
}

#[inline]
pub fn get_file_upload_home_dir() -> PathBuf {
    get_data_dir().join("files")
}

pub const ENV_LOGS_HOME: &str = "LOGS_HOME";
pub const ENV_TAOSX_LOGS_HOME: &str = "TAOSX_LOGS_HOME";

pub fn set_env_log_home_dir(config: String) {
    std::env::set_var(ENV_LOGS_HOME, config);
}

#[inline]
pub fn get_logs_home_dir() -> PathBuf {
    Path::new(&std::env::var(ENV_LOGS_HOME).unwrap()).to_path_buf()
}

#[inline]
pub fn get_log_dir(plugin: &str) -> PathBuf {
    get_logs_home_dir().join(plugin)
}

const ENV_TAOSX_LOGS_KEEP_DAYS: &str = "TAOSX_LOGS_KEEP_DAYS";

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
        log_path,
        AppendTimestamp::with_format(
            "%Y-%m-%d",
            FileLimit::Age(chrono::Duration::days(log_keep_days)),
            DateFrom::DateYesterday,
        ),
        ContentLimit::Time(TimeFrequency::Daily),
        Compression::OnRotate(2),
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
                result.push('\n');
            }
            #[allow(clippy::unnecessary_unwrap)]
            if append_line.is_some() && !result.is_empty() {
                result.push_str(append_line.unwrap());
            }
            result.push_str(&config_str);
        }
        for file in files {
            let f = std::fs::canonicalize(&file[1..])
                .map_err(|err| format!("failed to read file: {}, cause: {:?}", &file[1..], err))?;
            match std::fs::File::open(f) {
                Err(err) => {
                    return Err(format!(
                        "failed to read file: {}, cause: {}",
                        &file[1..],
                        err
                    ));
                }
                Ok(f) => {
                    let buf = std::io::BufReader::new(f);
                    let file_data = buf.lines().collect_vec();
                    file_data
                        .iter()
                        .filter_map(|r| r.as_ref().ok())
                        .for_each(|v| {
                            if line_break && !result.is_empty() {
                                result.push('\n');
                            }
                            #[allow(clippy::unnecessary_unwrap)]
                            if append_line.is_some() && !result.is_empty() {
                                result.push_str(append_line.unwrap());
                            }
                            result.push_str(v.as_str());
                        });
                }
            }
        }
        Ok(Some(result))
    } else {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct NoCertificateVerification();

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        use tokio_rustls::rustls::SignatureScheme::*;
        vec![
            RSA_PKCS1_SHA1,
            ECDSA_SHA1_Legacy,
            RSA_PKCS1_SHA256,
            ECDSA_NISTP256_SHA256,
            RSA_PKCS1_SHA384,
            ECDSA_NISTP384_SHA384,
            RSA_PKCS1_SHA512,
            ECDSA_NISTP521_SHA512,
            RSA_PSS_SHA256,
            RSA_PSS_SHA384,
            RSA_PSS_SHA512,
            ED25519,
            ED448,
        ]
    }
}

/// get string vector from dsn's key. if value starts with @, read file.
///
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
        if node_config.is_empty() {
            tracing::warn!("node config is empty");
            // return Err(format!("node config set but is empty: {nodes}"));
        }
        return Ok(node_config);
    }
    Err("Nodes not set".to_string())
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::str::FromStr;
    use std::thread;

    use super::*;

    #[test]
    #[ignore]
    fn test_set_tcp_keepalive() {
        let server = thread::spawn(|| {
            let listener = TcpListener::bind("127.0.0.1:54321").unwrap();

            if let Some(Ok(_stream)) = listener.incoming().next() {
                println!("connection established!");
                thread::sleep(Duration::from_secs(5));
            }
        });

        let stream = std::net::TcpStream::connect("127.0.0.1:54321").unwrap();
        set_tcp_keepalive(&stream).unwrap();

        let sock_ref = socket2::SockRef::from(&stream);
        assert!(sock_ref.keepalive().unwrap());
        #[cfg(not(target_os = "windows"))]
        {
            assert_eq!(10, sock_ref.keepalive_time().unwrap().as_secs());
            assert_eq!(10, sock_ref.keepalive_interval().unwrap().as_secs());
        }

        server.join().unwrap();
    }

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
    #[ignore]
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

        let mut dsn = Dsn::from_str("opc+ua://Win10-2021XIVKQ:53530/OPCUA/SimulationServer?ua.nodes=ns=3;i=1004::ntb1::c0::double,ns=3;i=1008::ntb1::c1::double").unwrap();
        let vec_string = get_string_vec_from_param_or_file(&mut dsn, "ua.nodes").unwrap();
        assert_eq!(
            vec_string,
            vec![
                String::from("ns=3;i=1004::ntb1::c0::double"),
                String::from("ns=3;i=1008::ntb1::c1::double"),
            ]
        );
        let mut dsn = Dsn::from_str("opc+ua://Win10-2021XIVKQ:53530/OPCUA/SimulationServer?ua.nodes=ns=3;i=1004::ntb1::c0::double,ns=3;i=1008::ntb1::c1::double,@/Users/zmlgirl/Downloads/test_opc.csv").unwrap();
        let vec_string = get_string_vec_from_param_or_file(&mut dsn, "ua.nodes").unwrap();
        assert_eq!(
            vec_string,
            vec![
                String::from("ns=3;i=1004::ntb1::c0::double"),
                String::from("ns=3;i=1008::ntb1::c1::double"),
                String::from("ns=2;i=2::ntb2::c1::double"),
                String::from("ns=2;i=3::ntb3::c2::int"),
            ]
        );
    }
}
