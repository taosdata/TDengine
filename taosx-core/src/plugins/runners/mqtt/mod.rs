use std::collections::HashMap;
use std::{fs, io::Write, path::PathBuf, sync::Arc};

use anyhow::Context;
use itertools::Itertools;
use taos::Dsn;
use tokio::{io::AsyncBufReadExt, sync::Mutex};
use tokio_util::sync::CancellationToken;
use tracing::Span;

use crate::dsv::DataSourceValidation;
use crate::runners::log_rotation;
use crate::runners::mqtt::config::{MqttConfig, MqttConnectConfig};
use crate::{
    build_ipc, get_log_keep_days, plugins::runners::get_plugin_dir, utils::port_pool::PortPool,
    Parser, Transferred,
};

mod config;

const EXE: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "taosx-mqtt.exe"
        } else {
            "taosx-mqtt"
        }
    }
};

fn mqtt_exe_path() -> anyhow::Result<PathBuf> {
    let path = get_plugin_dir("mqtt").join(EXE);
    if !path.exists() {
        anyhow::bail!(format!("mqtt plugin not found {:?}", path))
    }
    Ok(path)
}

const LOG_FILE: &str = "mqtt.log";

fn log_path() -> PathBuf {
    super::get_log_dir("mqtt")
}

pub fn info() -> anyhow::Result<(&'static str, PathBuf, String)> {
    let path = mqtt_exe_path()?;
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
    span: Span,
) -> anyhow::Result<()> {
    let ipc_port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for MQTT connection"))?;

    let config = MqttConfig::from(&from, Some(ipc_port))?;
    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();
    tracing::info!(
        "Using mqtt config file {} \n{}",
        config_path.display(),
        toml
    );
    let mut ipc_handler = build_ipc(
        &config.remote,
        parser,
        &to,
        Some("mqtt"),
        None,
        &cancel,
        with_agent,
        transferred,
        span,
        None,
    )
    .await?;

    let mqtt = mqtt_exe_path()?;
    let mut command = tokio::process::Command::new(mqtt);

    let mut log_path = log_path();
    fs::create_dir_all(&log_path)?;
    tracing::info!("log path created: {}", &log_path.display());

    log_path.push(LOG_FILE);
    tracing::info!("log file dir: {}", &log_path.display());

    let log_keep_days = get_log_keep_days();
    let mut log_rotation = log_rotation(&log_path, log_keep_days);
    let child = command
        .arg("-c")
        .arg(&config_path)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::piped());

    let mut child = child
        .spawn()
        .map_err(|err| anyhow::format_err!("Cannot spawn mqtt process: {err:?}"))?;

    const ERROR_BUF_SIZE: usize = 2;
    let error_buf = Arc::new(Mutex::new(ringbuf::HeapRb::<String>::new(ERROR_BUF_SIZE)));
    let error_buf_producer = error_buf.clone();
    let stderr = child.stderr.take().expect("Failed to capture stderr");
    tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(stderr);
        let mut line = String::new();
        loop {
            // Read a line from stderr
            let bytes_read = reader.read_line(&mut line).await.unwrap();
            if bytes_read == 0 {
                break; // End of stream, exit the loop
            }
            if line.contains("fatal") {
                use ringbuf::Rb;
                let mut guard = error_buf_producer.lock().await;
                let _ = guard.push_overwrite(line.clone());
            }
            // Write the line to log_rotation
            write!(log_rotation, "{}", line).unwrap();
            line.clear();
        }
        Ok::<(), std::io::Error>(())
    });

    let port_pool = port_pool.clone();
    tokio::spawn(async move {
        macro_rules! safe_exit {
            () => {
                let _ = ipc_handler.close().await;
                temp_path.close().unwrap();
                port_pool.put(ipc_port);
            };
        }
        tokio::select! {
            status = child.wait() => {
                let status = status?;
                tracing::info!("mqtt exit with {status}");
                if !status.success() {
                    use ringbuf::Rb;
                    safe_exit!();
                    let error = error_buf.lock().await.iter().join("");
                    anyhow::bail!("MQTT exit with {}\n{error}", status);
                }
            },
            err = ipc_handler.recv_error() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                if let Some(err) = err {
                    safe_exit!();
                    anyhow::bail!("mqtt writer error: {err}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("mqtt task cancelled");
            },
        }

        let _ = child.kill().await;
        tracing::info!("mqtt to taos task done");
        safe_exit!();
        Ok(())
    }).await??;
    Ok(())
}

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = MqttConnectConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            "mqtt".to_string(),
            format!(
                "invalid mqtt dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            let mqtt_config = MqttConfig {
                log_level: "".to_string(),
                remote: "".to_string(),
                mqtt: c,
                topics: HashMap::new(),
            };
            let valid = validate_mqtt(mqtt_config).await;
            match valid {
                Err(err) => DataSourceValidation::invalid(
                    "mqtt".to_string(),
                    format!(
                        "failed to connect to dsn: {}, cause: {}",
                        dsn.to_string(),
                        err.to_string()
                    ),
                ),
                Ok(v) => v,
            }
        }
    }
}

async fn validate_mqtt(config: MqttConfig) -> anyhow::Result<DataSourceValidation> {
    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;

    // startup the connector
    let mqtt = mqtt_exe_path()?;
    let mut command = tokio::process::Command::new(mqtt.clone());
    let output = command
        .arg("--check")
        .arg("-c")
        .arg(config_file.path())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .with_context(|| format!("failed to execute mqtt: {:?}", mqtt.as_path()))?;

    if output.status.success() {
        let result: serde_json::Value =
            serde_json::from_slice(&output.stdout).with_context(|| {
                format!(
                    "Deserialize mqtt validation result error: {}",
                    String::from_utf8_lossy(&output.stdout)
                )
            })?;
        Ok(DataSourceValidation {
            valid: result["valid"].as_bool().unwrap_or(false),
            support: result["support"].as_bool().unwrap_or(false),
            data_source: "mqtt".to_string(),
            version: result["version"].as_str().map(|s| s.to_string()),
            message: result["message"].as_str().map(|s| s.to_string()),
        })
    } else {
        Ok(DataSourceValidation::invalid(
            "mqtt".to_string(),
            format!(
                "failed to execute mqtt: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::str::FromStr;

    use crate::TaskOpts;

    use super::*;

    #[tokio::test]
    async fn test_invalid() {
        let dsn = Dsn::from_str("mqtt://").unwrap();
        let validation = is_valid(&dsn).await;
        assert_eq!(false, validation.valid);
        assert_eq!(false, validation.support);
        assert_eq!("mqtt", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!(
            "invalid mqtt dsn: mqtt://, cause: host is required",
            validation.message.unwrap()
        );

        let dsn =
            Dsn::from_str("mqtt://127.0.0.1:1833?clean_session=true&keep_alive=60&version=3.0")
                .unwrap();
        let validation = is_valid(&dsn).await;
        assert_eq!(false, validation.valid);
        assert_eq!(false, validation.support);
        assert_eq!("mqtt", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!("failed to connect to dsn: mqtt://127.0.0.1:1833?clean_session=true&keep_alive=60&version=3.0, cause: mqtt plugin not found \"/usr/local/taos/plugins/mqtt/taosx-mqtt\"", validation.message.unwrap());
    }

    #[ignore]
    #[tokio::test]
    async fn test_valid() {
        env::set_var("PLUGINS_HOME", "../plugins");

        let dsn = Dsn::from_str("mqtt://192.168.1.42:1883?version=3.0").unwrap();
        let dsv = is_valid(&dsn).await;
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!("mqtt", dsv.data_source);
        assert_eq!(None, dsv.version);
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_mqtt_parser() {
        std::env::set_var("RUST_LOG", "debug,tokio=warn");
        pretty_env_logger::init();
        let transferred = Arc::new(Transferred::default());
        let _metrics = transferred.clone();
        use std::time::Duration;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(200));
            loop {
                interval.tick().await;
                // dbg!(&metrics);
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
            breakpoints: None,
            offsets: Default::default(),
            transferred: Some(transferred),
            span: tracing::info_span!("test_mqtt"),
            task_id: None,
        };
        opts.run(&PortPool::default()).await.unwrap();
    }
}
