use std::{fs, io::prelude::*, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;

use itertools::Itertools;
use taos::Dsn;
use tokio::{io::AsyncBufReadExt, sync::Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument, Span};

use crate::plugins::mask_dsn;
use crate::runners::influxdb::config::{ConnectionConfig, InfluxdbConfig, INFLUXDB_V1};
use crate::runners::log_rotation;
use crate::validation::DataSourceValidation;
use crate::{
    build_ipc, get_log_keep_days, utils::port_pool::PortPool, Action, DataSet, Transferred,
};

use super::get_plugin_dir;

mod config;

const EXE: &'static str = "taosx-influxdb.jar";
const LOG_FILE: &str = "influxdb.log";

pub fn info() -> anyhow::Result<(&'static str, PathBuf, String)> {
    let path = influxdb_jar_path()?;
    let output = std::process::Command::new("java")
        .arg("-jar")
        .arg(&path)
        .arg("-version")
        .output()?;
    Ok((
        "influxdb",
        path,
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
    ))
}

/// InfluxDB DSN example: "influxdb://127.0.0.1:8086/?token=abc&orgId=def&mode=normal&beginTime=2023-05-01&endTime="
#[instrument(
    skip_all,
    fields(
        x.influxdb.source = % mask_dsn(& from),
        x.influxdb.sink = % mask_dsn(& to),
        x.influxdb.agent = with_agent.as_ref().map(| a | a.0),
    )
)]
pub async fn influxdb_to_taos(
    from: Dsn,
    _actions: Vec<Action>,
    to: Dsn,
    _jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    span: Span,
    task_id: Option<i64>,
) -> anyhow::Result<()> {
    let ipc_port = port_pool
        .get()
        .ok_or(anyhow::anyhow!("No available port for InfluxDB connection"))?;

    // generate config
    let config = InfluxdbConfig::from(&from, ipc_port)?;
    // transform to toml
    let toml = toml::to_string(&config)?;
    // write to a temporary file
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    // get the path of the temporary file
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();
    tracing::info!("Using config file {}", config_path.display());
    // create socket channel
    let mut ipc = build_ipc(
        format!("127.0.0.1:{ipc_port}").as_str(),
        None,
        &to,
        Some("influxdb"),
        None,
        &cancel,
        with_agent,
        transferred,
        span,
        task_id,
    )
    .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut log_path = log_path();

    fs::create_dir_all(&log_path)?;

    tracing::info!("log path created: {}", &log_path.display());

    log_path.push(LOG_FILE);

    tracing::info!("log file dir: {}", &log_path.display());

    let log_keep_days = get_log_keep_days();

    let mut log_rotation = log_rotation(&log_path, log_keep_days);

    // get the version of jdk
    let get_jdk_version = tokio::process::Command::new("java")
        .arg("-version")
        .output()
        .await
        .context("Get JDK version error")?;
    let jdk_version = String::from_utf8(get_jdk_version.stderr.clone())?;

    let mut command = tokio::process::Command::new("java");
    let child;

    let connector_path = influxdb_jar_path()?;
    if jdk_version.contains("build 1.") {
        child = command
            .arg("-jar")
            .arg(&connector_path)
            .arg(&config_path)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped());
    } else {
        child = command
            .arg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .arg("-jar")
            .arg(&connector_path)
            .arg(&config_path)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped());
    }

    let port_pool = port_pool.clone();

    let mut child = child.spawn().context("Start InfluxDB collector error")?;
    const ERROR_BUF_SIZE: usize = 2;
    let error_buf = Arc::new(Mutex::new(ringbuf::HeapRb::<String>::new(ERROR_BUF_SIZE)));
    let error_buf_producer = error_buf.clone();
    let stderr = child.stderr.take().expect("Failed to capture stderr");
    tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(stderr);
        let mut line = String::new();
        loop {
            // Read a line from stderr
            let bytes_read = reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                break; // End of stream, exit the loop
            }
            if line.contains("ERROR") {
                use ringbuf::Rb;
                let mut guard = error_buf_producer.lock().await;
                let _ = guard.push_overwrite(line.clone());
            }
            // Write the line to log_rotation
            write!(log_rotation, "{}", line)?;
            line.clear();
        }
        Ok::<(), std::io::Error>(())
    });
    // waiting until the end
    tracing::info!("waiting for InfluxDB connector");
    tokio::spawn(async move {
        macro_rules! safe_exit {
            () => {
                let _ = ipc.close().await;
                temp_path.close().unwrap();
                port_pool.put(ipc_port);
            };
        }
        tokio::select! {
            // application exit with error code
            status = child.wait() => {
                let status = status?;
                tracing::info!("InfluxDB exit with {}", status);
                if !status.success() {
                    use ringbuf::Rb;
                    safe_exit!();
                    let error = error_buf.lock().await.iter().join("");
                    anyhow::bail!("InfluxDB exit with status {status}: {error}");
                }
            },
            err = ipc.recv_error() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                if let Some(err) = err {
                    // kill child pid before raising error
                    let _ = child.kill().await;
                    safe_exit!();
                    anyhow::bail!("InfluxDB writer error: {err}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("InfluxDB task cancelled");
            }
        }
        // stop the connector
        let _ = child.kill().await;
        tracing::info!("InfluxDB task Done");
        safe_exit!();
        Ok(())
    }.in_current_span()).await??;

    Ok(())
}

pub async fn influxdb_datasets(dsn: Dsn) -> anyhow::Result<Vec<DataSet>> {
    let c = ConnectionConfig::from_dsn(&dsn)?;
    // 连接器路径
    let path = influxdb_jar_path()?;

    // startup the connector
    let mut command = tokio::process::Command::new("java");
    // 查询命令
    let output;
    // 不同版本不同参数
    if INFLUXDB_V1.contains(&c.version.as_str()) {
        // 查询命令
        output = command
            .arg("-jar")
            .arg(&path)
            .arg("-fetch")
            .arg(&c.version)
            .arg(&c.url)
            .arg(&c.username.unwrap())
            .arg(&c.password.unwrap())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .with_context(|| "Start InfluxDB collector error")?;
    } else {
        output = command
            .arg("-jar")
            .arg(&path)
            .arg("-fetch")
            .arg(&c.version)
            .arg(&c.url)
            .arg(&c.token.unwrap())
            .arg(&c.org_id.unwrap())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .with_context(|| "Start InfluxDB collector error")?;
    }

    if output.status.success() {
        let s = String::from_utf8(output.stdout.clone())?;
        if s == "" {
            anyhow::bail!("InfluxDB connector returns OK, but result is nothing");
        }
        let mut vec = Vec::new();
        vec.push(DataSet {
            id: s,
            name: None,
            category: None,
            r#type: None,
            options: None,
            format: None,
        });
        Ok(vec)
    } else {
        match output.status.code() {
            Some(101) => anyhow::bail!("Failed to connect, ip or port error"),
            Some(102) => anyhow::bail!("Unauthorized access"),
            Some(103) => anyhow::bail!("Organization not found"),
            None => anyhow::bail!("InfluxDB connector closed by signal"),
            Some(exit) => {
                anyhow::bail!("Unknown exit code {exit}, maybe failed to connect, ip or port error")
            }
        }
    }
}

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = ConnectionConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            "influxdb".to_string(),
            format!(
                "invalid dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            let result = validate_source_influxdb(c).await;
            match result {
                Err(err) => DataSourceValidation::invalid(
                    "influxdb".to_string(),
                    format!(
                        "failed to connect to dsn: {}, cause: {}",
                        dsn.to_string(),
                        err.to_string()
                    ),
                ),
                Ok(validate) => validate,
            }
        }
    }
}

async fn validate_source_influxdb(
    config: ConnectionConfig,
) -> anyhow::Result<DataSourceValidation> {
    // 连接器路径
    let connector_path = influxdb_jar_path()?;

    // startup the connector
    let mut command = tokio::process::Command::new("java");
    // 查询命令
    let output;
    // 不同版本不同参数
    if INFLUXDB_V1.contains(&config.version.as_str()) {
        // 查询命令
        output = command
            .arg("-jar")
            .arg(&connector_path)
            .arg("-check")
            .arg(config.version)
            .arg(config.url)
            .arg(config.username.unwrap())
            .arg(config.password.unwrap())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .with_context(|| "Start InfluxDB collector error")?;
    } else {
        output = command
            .arg("-jar")
            .arg(&connector_path)
            .arg("-check")
            .arg(config.version)
            .arg(config.url)
            .arg(config.token.unwrap())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .with_context(|| "Start InfluxDB collector error")?;
    }
    if output.status.success() {
        let result: serde_json::Value =
            serde_json::from_slice(&output.stdout).with_context(|| {
                format!(
                    "Deserialize influxdb validation result error: {}",
                    String::from_utf8_lossy(&output.stdout)
                )
            })?;
        // 组装结果
        Ok(DataSourceValidation {
            valid: result["valid"].as_bool().unwrap_or(false),
            support: result["support"].as_bool().unwrap_or(false),
            data_source: String::from("influxdb"),
            version: result["version"].as_str().map(|s| s.to_string()),
            message: result["message"].as_str().map(|s| s.to_string()),
        })
    } else {
        let msg = match output.status.code() {
            Some(1) => String::from("The input parameters are incorrect"),
            Some(3) => String::from("Failed to connect"),
            _ => String::from("Unknown exit code, maybe failed to connect, ip or port error")
        };
        Ok(DataSourceValidation {
            valid: false,
            support: false,
            data_source: String::from("influxdb"),
            version: None,
            message: Some(msg),
        })
    }
}

fn log_path() -> PathBuf {
    super::get_log_dir("influxdb")
}

fn influxdb_jar_path() -> anyhow::Result<PathBuf> {
    let path = get_plugin_dir("influxdb").join(EXE);
    if !path.exists() {
        return Err(anyhow::anyhow!(format!(
            "influxdb plugin not found {:?}",
            path.to_str()
        )));
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use taos::Dsn;

    #[tokio::test]
    async fn test_is_valid() {
        let dsn = Dsn::from_str("influxdb://?version=2.7").unwrap();
        let validation = is_valid(&dsn).await;
        assert_eq!(false, validation.valid);
        assert_eq!(false, validation.support);
        assert_eq!("influxdb", validation.data_source);
        assert!(validation.version.is_none());
        assert_eq!(
            "invalid dsn: influxdb://?version=2.7, cause: orgId is required",
            validation.message.unwrap()
        );

        let dsn =
            Dsn::from_str("influxdb://127.0.0.1:8086?version=2.7&orgId=abc&token=123").unwrap();
        let validation = is_valid(&dsn).await;
        assert_eq!(false, validation.valid);
        assert_eq!(false, validation.support);
        assert_eq!("influxdb", validation.data_source);
        assert!(validation.version.is_none());
        assert!(validation.message.unwrap().contains("plugin not found"));
    }
}
