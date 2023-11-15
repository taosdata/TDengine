use std::{fs, io::prelude::*, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use itertools::Itertools;
use taos::Dsn;
use tokio::{io::AsyncBufReadExt, sync::Mutex};
use tokio_process_terminate::TerminateExt;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span};

use crate::dsv::DataSourceValidation;
use crate::runners::log_rotation;
use crate::runners::opentsdb::config::{ConnectionConfig, OpentsdbConfig};
use crate::{
    build_ipc, get_log_keep_days, utils::port_pool::PortPool, Action, DataSet, Transferred,
};

use super::get_data_dir;
use super::get_plugin_dir;

mod config;

const EXE: &'static str = "taosx-opentsdb.jar";

fn opentsdb_jar_path() -> anyhow::Result<PathBuf> {
    let path = get_plugin_dir("opentsdb").join(EXE);
    if !path.exists() {
        anyhow::bail!(format!("opentsdb plugin not found {:?}", path))
    }
    Ok(path)
}

const LOG_FILE: &str = "opentsdb.log";

fn log_path() -> PathBuf {
    super::get_log_dir("")
}

pub fn info() -> anyhow::Result<(&'static str, PathBuf, String)> {
    let path = opentsdb_jar_path()?;
    let output = std::process::Command::new("java")
        .arg("-jar")
        .arg(&path)
        .arg("-version")
        .output()?;
    Ok((
        "opentsdb",
        path,
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
    ))
}

/// OpentsDB DSN example: "opentsdb://127.0.0.1:4242/?beginTime=2023-05-01&endTime="
pub async fn opentsdb_to_taos(
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
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    let ipc_port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for OpenTSDB connection"))?;
    // generate config
    let config = OpentsdbConfig::from(&from, ipc_port)?;
    // transform to toml
    let toml = toml::to_string(&config)?;
    // write to a temporary file
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    // save the temporary file to task dir
    let path = get_data_dir()
        .join("tasks")
        .join(task_id.unwrap().to_string());
    std::fs::create_dir_all(&path).unwrap();
    let path = path.join(format!(
        "{}-{}-{}.{}",
        task_id.unwrap(),
        "opentsdb",
        chrono::Local::now().format("%Y%m%d%H%M"),
        "toml"
    ));
    let _ = config_file.persist(path.clone());
    tracing::info!("Using config file {}", path.display());

    let exec_span = tracing::info_span!("extern plugin exec", plugin.name = "opentsdb");
    exec_span.follows_from(&span);

    // create socket channel
    let mut ipc_handler = build_ipc(
        &format!("127.0.0.1:{}", ipc_port),
        None,
        &to,
        Some("opentsdb"),
        None,
        &cancel,
        with_agent,
        transferred,
        span,
        task_id,
        notify,
    )
    .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;
    // 连接器路径
    let connector_path = opentsdb_jar_path()?;

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

    if jdk_version.contains("build 1.") {
        child = command
            .arg("-jar")
            .arg(&connector_path)
            .arg(&path)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped());
    } else {
        child = command
            .arg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .arg("-jar")
            .arg(&connector_path)
            .arg(&path)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped());
    }

    let port_pool = port_pool.clone();
    {
        let mut child = child.spawn().context("Start OpenTSDB collector error")?;
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
        tracing::info!("waiting for OpenTSDB connector");
        tokio::spawn(async move {
            let pid = child.id();
            macro_rules! safe_exit {
                () => {
                    // kill child pid before raising error
                    let _ = child.terminate_timeout(Duration::from_secs(2)).await;
                    let _ = ipc_handler.close().await;
                    port_pool.put(ipc_port).await;
                };
            }
            tokio::select! {
                // application exit with error code
                status = child.wait().instrument(tracing::info_span!("process", plugin.pid = pid)) => {
                    let status = status?;
                    tracing::info!("OpenTSDB exit with {}", status);
                    if !status.success() {
                        use ringbuf::Rb;
                        safe_exit!();
                        let error = error_buf.lock().await.iter().join("");
                        anyhow::bail!("OpenTSDB exit with {}\n{error}", status);
                    }
                },
                err = ipc_handler.recv_error() => {
                    tracing::info!("have received worker thread panicked message, terminate child process");
                    if let Some(err) = err {
                        safe_exit!();
                        anyhow::bail!("OpenTSDB writer error: {err}");
                    }
                },
                _ = cancel.cancelled() => {
                    tracing::info!("OpenTSDB task cancelled");
                }
            }
            ;
            tracing::info!("OpenTSDB task Done");
            safe_exit!();
            // wait for completion
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }.instrument(exec_span)).await??;
    }
    Ok(())
}

pub async fn opentsdb_datasets(dsn: Dsn) -> anyhow::Result<Vec<DataSet>> {
    let config = ConnectionConfig::from_dsn(&dsn);
    match config {
        Err(err) => {
            anyhow::bail!(err)
        }
        Ok(c) => {
            // 连接器路径
            let connector_path = opentsdb_jar_path()?;
            // startup the connector
            let mut command = tokio::process::Command::new("java");
            // 查询命令
            let output = command
                .arg("-jar")
                .arg(&connector_path)
                .arg("-fetch")
                .arg(&c.url)
                .kill_on_drop(true)
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::piped())
                .output()
                .await
                .with_context(|| "Start OpenTSDB collector error")?;
            if output.status.success() {
                let s = String::from_utf8(output.stdout.clone())?;
                if s == "" {
                    anyhow::bail!("OpenTSDB connector returns OK, but result is nothing");
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
                    Some(102) => anyhow::bail!("Protocol error"),
                    Some(103) => anyhow::bail!("Params error or service mismatch"),
                    None => anyhow::bail!("OpenTSDB connector closed by signal"),
                    Some(exit) => {
                        anyhow::bail!(
                            "Unknown exit code {exit}, maybe failed to connect, ip or port error"
                        )
                    }
                }
            }
        }
    }
}

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = ConnectionConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            "opentsdb".to_string(),
            format!(
                "invalid dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            let result = validate_source_opentsdb(c).await;
            match result {
                Err(err) => DataSourceValidation::invalid(
                    "opentsdb".to_string(),
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

async fn validate_source_opentsdb(
    config: ConnectionConfig,
) -> anyhow::Result<DataSourceValidation> {
    // 连接器路径
    let connector_path = opentsdb_jar_path()?;
    // startup the connector
    let mut command = tokio::process::Command::new("java");
    // 查询命令
    let output = command
        .arg("-jar")
        .arg(&connector_path)
        .arg("-check")
        .arg(&config.url)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .with_context(|| "Start OpenTSDB collector error")?;
    if output.status.success() {
        let result: serde_json::Value =
            serde_json::from_slice(&output.stdout).with_context(|| {
                format!(
                    "Deserialize opentsdb validation result error: {}",
                    String::from_utf8_lossy(&output.stdout)
                )
            })?;
        // 组装结果
        Ok(DataSourceValidation {
            valid: result["valid"].as_bool().unwrap_or(false),
            support: result["support"].as_bool().unwrap_or(false),
            data_source: String::from("opentsdb"),
            version: result["version"].as_str().map(|s| s.to_string()),
            message: result["message"].as_str().map(|s| s.to_string()),
        })
    } else {
        let msg = match output.status.code() {
            Some(1) => String::from("The input parameters are incorrect"),
            Some(3) => String::from("Failed to connect"),
            _ => String::from("Unknown exit code, maybe failed to connect, ip or port error"),
        };
        Ok(DataSourceValidation {
            valid: false,
            support: false,
            data_source: String::from("opentsdb"),
            version: Some(String::from("")),
            message: Some(msg),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::str::FromStr;
    use taos::Dsn;

    use super::*;

    #[tokio::test]
    async fn test_invalid() {
        let dsn = Dsn::from_str("opentsdb://").unwrap();
        let validation = is_valid(&dsn).await;
        assert_eq!(false, validation.valid);
        assert_eq!(false, validation.support);
        assert_eq!("opentsdb", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!(
            "invalid dsn: opentsdb://, cause: host is required",
            validation.message.unwrap()
        );

        let dsn = Dsn::from_str("opentsdb://127.0.0.1:6060").unwrap();
        let validation = is_valid(&dsn).await;
        assert_eq!(false, validation.valid);
        assert_eq!(false, validation.support);
        assert_eq!("opentsdb", validation.data_source);
        assert_eq!(None, validation.version);
        assert!(validation
            .message
            .unwrap()
            .contains("cause: opentsdb plugin not found"));
    }

    #[ignore]
    #[tokio::test]
    async fn test_valid() {
        env::set_var("PLUGINS_HOME", "../plugins");

        let dsn = Dsn::from_str("opentsdb://192.168.2.12:4242").unwrap();
        let dsv = is_valid(&dsn).await;
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!("opentsdb", dsv.data_source);
        assert_eq!("2.4.0", dsv.version.unwrap());
    }
}
