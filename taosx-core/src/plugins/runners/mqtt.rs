use std::{
    collections::HashMap,
    io::{BufRead, Write},
    num::ParseIntError,
    path::PathBuf,
    str::ParseBoolError,
    sync::Arc,
    time::Duration,
};

use itertools::Itertools;
use taos::{AsyncTBuilder, Dsn, TaosBuilder};
use tokio_util::sync::CancellationToken;

use crate::{
    plugins::{runners::get_plugin_dir, sink},
    utils::port_pool::PortPool,
    Parser, Transferred,
};

#[derive(Debug, serde::Serialize)]
struct MqttConfig {
    log_level: String,
    remote: String,
    mqtt: MqttConnectConfig,
    topics: HashMap<String, u8>,
}

#[derive(Debug, serde::Serialize)]
struct MqttConnectConfig {
    address: String,
    version: String,
    client_id: String,
    username: String,
    password: String,
    keep_alive: usize,
    clean_session: bool,
    ca: String,
    cert: String,
    cert_key: String,
}

#[derive(Debug, thiserror::Error)]
enum MqttConfigError {
    // #[error("address is required in OPC dsn: {0} like")]
    // MqttAddrIsRequired(Dsn),
    #[error("Parse integer error from {1} while parsing parameter {0}: {2:?}")]
    ParseIntError(&'static str, String, ParseIntError),
    #[error("Parse bool error from {1} while parsing parameter {0}: {2:?}")]
    ParseBoolError(&'static str, String, ParseBoolError),
    #[error("Parse topics error from {1} while parsing parameter {0}")]
    ParseTopicsError(&'static str, String),
    #[error("Database name is required in MQTT dsn: {0}")]
    DatabaseIsRequired(Dsn),
    #[error("Mqtt ca config read error, cause: {0}")]
    CAConfigReadError(String),
    #[error("Mqtt config parse error, cause: {0}")]
    MqttConfigParseError(String),
}

impl MqttConfig {
    fn new(mut dsn: Dsn, ipc_port: u16) -> Result<Self, MqttConfigError> {
        let address = dsn.addresses.first().unwrap();
        let host = if let Some(host) = address.host.clone() {
            host
        } else {
            "127.0.0.1".to_string()
        };
        let port = if let Some(port) = address.port {
            port
        } else {
            1883
        };
        let ca = get_string_from_param_or_file(&mut dsn, "ca", true, None)
            .map_err(|s| MqttConfigError::CAConfigReadError(s))?;
        let cert = get_string_from_param_or_file(&mut dsn, "cert", true, None)
            .map_err(|s| MqttConfigError::CAConfigReadError(s))?;
        let cert_key = get_string_from_param_or_file(&mut dsn, "cert_key", true, None)
            .map_err(|s| MqttConfigError::CAConfigReadError(s))?;
        let address = if ca.is_some() {
            format!("ssl://{host}:{port}")
        } else {
            format!("tcp://{host}:{port}")
        };
        let topics_vec = super::opc::get_string_vec_from_param_or_file(&mut dsn, "topics")
            .map_err(|err| MqttConfigError::ParseTopicsError("topics", err))?;
        let mut topics = HashMap::new();
        for i in 0..topics_vec.len() {
            let pair = topics_vec[i].split("::").collect_vec();
            if pair.len() != 2 {
                return Err(MqttConfigError::MqttConfigParseError(format!(
                    "topic config error: {}",
                    topics_vec[i]
                )));
            }
            let topic = String::from(pair[0]);
            let qos = pair[1]
                .parse::<u8>()
                .map_err(|err| MqttConfigError::ParseIntError("qos", pair[1].to_string(), err))?;
            // let table = String::from(pair[2]);
            // let field = String::from(pair[3]);
            // let value_type = String::from(pair[4]);
            topics.insert(topic, qos);
        }
        Ok(MqttConfig {
            log_level: dsn.remove("log_level").unwrap_or("info".to_string()),
            remote: format!("127.0.0.1:{ipc_port}"),
            mqtt: MqttConnectConfig {
                address,
                version: dsn.remove("version").unwrap_or("3.0".to_string()),
                client_id: dsn.remove("client_id").unwrap_or("".to_string()),
                username: dsn.username.clone().unwrap_or("".to_string()),
                password: dsn.password.clone().unwrap_or("".to_string()),
                keep_alive: dsn
                    .remove("keep_alive")
                    .map(|v| {
                        v.parse::<usize>()
                            .map_err(|err| MqttConfigError::ParseIntError("keep_alive", v, err))
                    })
                    .transpose()?
                    .unwrap_or(60),
                clean_session: dsn
                    .remove("clean_session")
                    .map(|v| {
                        v.parse::<bool>()
                            .map_err(|err| MqttConfigError::ParseBoolError("clean_session", v, err))
                    })
                    .transpose()?
                    .unwrap_or(true),
                ca: ca.unwrap_or("".to_string()),
                cert: cert.unwrap_or("".to_string()),
                cert_key: cert_key.unwrap_or("".to_string()),
            },
            topics,
        })
    }
}

const EXE: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "taosx-mqtt.exe"
        } else {
            "taosx-mqtt"
        }
    }
};

fn mqtt_exe_path() -> PathBuf {
    get_plugin_dir("mqtt").join(EXE)
}
pub fn info() -> Result<(&'static str, PathBuf, String), std::io::Error> {
    let path = mqtt_exe_path();
    let output = std::process::Command::new(&path)
        .arg("--version")
        .output()?;
    Ok((
        "mqtt",
        path,
        String::from_utf8_lossy(&output.stderr).trim().to_string(),
    ))
}
pub async fn mqtt_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    to: Dsn,
    _jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
) -> anyhow::Result<()> {
    println!("# loading plugin: MQTT");
    if to.subject.is_none() {
        Err(MqttConfigError::DatabaseIsRequired(to.clone()))?;
    }
    // let target_pool = TaosBuilder::from_dsn(&to)?.pool()?;
    // let taos = target_pool.get().await?;
    // let target_pool_for_ipc = target_pool.clone();

    let ipc_port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for MQTT connection"))?;

    let config = MqttConfig::new(from, ipc_port)?;

    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();
    log::info!(
        "Using mqtt config file {} \n{}",
        config_path.display(),
        toml
    );
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let ipc = if with_agent.is_none() {
        sink::listen_tcp_socket(
            TaosBuilder::from_dsn(&to)?.pool()?,
            config.remote,
            sender,
            None,
            cancel.clone(),
            with_agent,
            parser.clone(),
            Some("mqtt"),
            transferred,
        )?
    } else {
        sink::listen_tcp_socket_with_agent(
            config.remote,
            sender,
            None,
            cancel.clone(),
            with_agent.unwrap(),
        )?
    };
    let mqtt = mqtt_exe_path();
    let mut command = tokio::process::Command::new(mqtt);
    let mut child = command
        .arg("-c")
        .arg(&config_path)
        .stdout(async_process::Stdio::inherit())
        .stderr(async_process::Stdio::inherit())
        .spawn()
        .map_err(|err| anyhow::format_err!("Cannot spawn mqtt process: {err:?}"))?;
    let port_pool = port_pool.clone();
    tokio::spawn(async move {
        tokio::select! {
            status = child.wait() => {
                let status = status?;
                log::info!("mqtt exit with status {status}");
                if !status.success() {
                    let _ = ipc.send(());
                    anyhow::bail!("mqtt exit with status {status}");
                }
            },
            err = receiver.recv() => {
                log::info!("have received worker thread panicked message, terminate child process");
                if let Some(err) = err {
                    let _ = ipc.send(());
                    anyhow::bail!("mqtt writer error: {err}");
                }
            },
            _ = cancel.cancelled() => {
                log::info!("mqtt task cancelled");
            },
        };
        ipc.send(())?;
        let _ = child.kill().await;
        log::info!("mqtt to taos task done");
        temp_path.close().unwrap();
        port_pool.put(ipc_port);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    })
    .await??;
    Ok(())
}

/// get string value from dsn's key
///
/// line_break: push \n between lines if is true
///
/// append_line: push append_line between lines if is not None
pub(super) fn get_string_from_param_or_file(
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

#[cfg(test)]
mod tests {
    use taos::IntoDsn;

    use super::*;
    use crate::TaskOpts;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_mqtt_parser() {
        std::env::set_var("RUST_LOG", "debug,tokio=warn");
        pretty_env_logger::init();
        let transferred = Arc::new(Transferred::default());
        let metrics = transferred.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(200));
            loop {
                interval.tick().await;
                dbg!(&metrics);
            }
        });
        let opts = TaskOpts {
            transform: vec![],
            from: "mqtt://192.168.0.201:11883?topics=topic-1::1"
                .parse()
                .unwrap(),
            to: "taos:///mqtt".parse().unwrap(),
            parser: Some(
                serde_json::from_str(
                    r#"
                {
                    "parse": { "payload": { "json": [
                        { "name": "pre", "alias": "value" }
                    ], "flatten": false, "keep": true } },
                    "model": {
                        "name": "{topic}-{qos}",
                        "using": "mqtt",
                        "tags": ["topic", "qos"],
                        "columns": ["ts", "value"]
                    }
                }
                "#,
                )
                .unwrap(),
            ),
            jobs: 0,
            compression_level: None,
            force: false,
            cancel: CancellationToken::new(),
            // port_pool: ONCE,
            with_agent: None,
            offsets: Default::default(),
            transferred: Some(transferred),
        };
        opts.run(&PortPool::default()).await.unwrap();
    }

    #[test]
    fn test_mqtt_config() {
        let log_level = "debug".to_string();
        let remote = "127.0.0.1:62307".to_string();
        let address = "tcp://127.0.0.1:1883".to_string();
        let version = String::from("3.0");
        // let client_id = Some("12123".to_string());
        let client_id = "".to_string();
        let username = "mqtt_test".to_string();
        let password = "123456".to_string();
        let keep_alive = 60 as usize;
        let clean_session = true;
        let ca = r#"-----BEGIN CERTIFICATE-----
MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt
-----END CERTIFICATE-----"#
            .to_string();
        let cert = r#"-----BEGIN CERTIFICATE-----
MIIDEzCCAfugAwIBAgIBATANBgkqhkiG9w0BAQsFADA
-----END CERTIFICATE-----"#
            .to_string();
        let cert_key = r#"-----BEGIN CERTIFICATE-----
MIIEpAIBAAKCAQEAzLiGiSwpxkENtjrzS7pNLblTnWe4HUUFwYyUX0H
-----END RSA PRIVATE KEY-----"#
            .to_string();
        let mut topics = HashMap::new();
        topics.insert("topic-1".to_string(), 1);
        let mqtt_config = MqttConfig {
            log_level,
            remote,
            mqtt: MqttConnectConfig {
                address,
                version,
                client_id,
                username,
                password,
                keep_alive,
                clean_session,
                ca,
                cert,
                cert_key,
            },
            topics,
        };
        let toml = toml::to_string(&mqtt_config).unwrap();
        println!("{}", toml);
    }

    #[test]
    fn test_get_string_from_param_or_file() -> anyhow::Result<()> {
        let ca = "-----BEGIN CERTIFICATE-----
MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV
-----END CERTIFICATE-----";
        let mut config_file = tempfile::NamedTempFile::new()?;
        write!(config_file, "{}", ca)?;
        let config_path = config_file.path().to_path_buf();
        let temp_path = config_file.into_temp_path();
        let mut dsn = format!(
            "mqtt:///?ca=123,456,@{},@{}",
            &config_path.display(),
            &config_path.display()
        )
        .into_dsn()?;
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

        let mut dsn = format!(
            "mqtt:///?ca=123,456,@{},@{}",
            &config_path.display(),
            &config_path.display()
        )
        .into_dsn()?;
        let result = get_string_from_param_or_file(&mut dsn, "ca", false, None)
            .unwrap()
            .unwrap();
        assert_eq!("123456-----BEGIN CERTIFICATE-----MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV-----END CERTIFICATE----------BEGIN CERTIFICATE-----MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV-----END CERTIFICATE-----", result);

        let mut dsn = format!(
            "mqtt:///?ca=123,456,@{},@{}",
            &config_path.display(),
            &config_path.display()
        )
        .into_dsn()?;
        let result = get_string_from_param_or_file(&mut dsn, "ca", false, Some(","))
            .unwrap()
            .unwrap();
        assert_eq!("123,456,-----BEGIN CERTIFICATE-----,MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV,-----END CERTIFICATE-----,-----BEGIN CERTIFICATE-----,MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV,-----END CERTIFICATE-----", result);
        temp_path.close()?;
        Ok(())
    }
}
