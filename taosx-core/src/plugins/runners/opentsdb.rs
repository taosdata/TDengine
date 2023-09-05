use std::{fs, io::prelude::*, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use file_rotate::{
    compression::Compression,
    suffix::{AppendTimestamp, DateFrom, FileLimit},
    ContentLimit, FileRotate, TimeFrequency,
};
use itertools::Itertools;
use taos::Dsn;
use tokio::{io::AsyncBufReadExt, sync::Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span};

use crate::{
    build_ipc, get_log_keep_days, utils::port_pool::PortPool, Action, DataSet, Transferred, ValidatedSource,
};

use super::get_plugin_dir;
use std::error::Error;

#[derive(Debug, serde::Serialize)]
struct OpentsdbConfig {
    // the datasource config
    opents: OpentsConfig,
    // the addr for connector to agent
    taosx: TaosxConfig,
    // the task config
    task: TaskConfig,
    // the performance config
    performance: PerformanceConfig,

    // others
    #[serde(skip)]
    #[allow(dead_code)]
    td_database: String,
    #[serde(skip)]
    ipc_stream: String,
}

#[derive(Debug, serde::Serialize)]
struct OpentsConfig {
    #[serde(rename = "url")]
    opents_url: String,
}

#[derive(Debug, serde::Serialize)]
struct TaosxConfig {
    #[serde(rename = "host")]
    taosx_host: String,
    #[serde(rename = "port")]
    taosx_port: u16,
}

#[derive(Debug, serde::Serialize)]
struct TaskConfig {
    #[serde(rename = "mode")]
    task_mode: String,
    #[serde(rename = "metrics")]
    task_metrics: Vec<String>,
    #[serde(rename = "beginTime")]
    task_begin_time: String,
    #[serde(rename = "endTime")]
    task_end_time: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct PerformanceConfig {
    #[serde(rename = "readWindow")]
    performance_read_window: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum OpentsdbError {
    #[error("The access address of OpenTSDB is required: {0}")]
    OpentsUrlIsRequired(Dsn),
    #[error("The data begin time is required: {0}")]
    TaskBeginTimeIsRequired(Dsn),
    #[error("plugin not found: {0}")]
    ExeNotFound(String),
}

impl OpentsdbConfig {
    pub fn new(mut dsn: Dsn, td_database: String, ipc: u16) -> Result<Self, OpentsdbError> {
        debug_assert!(dsn.driver == "opentsdb");
        // the datasource config
        let host = dsn
            .addresses
            .first()
            .and_then(|addr| addr.host.clone())
            .ok_or_else(|| OpentsdbError::OpentsUrlIsRequired(dsn.clone()))?;
        let port = dsn
            .addresses
            .first()
            .and_then(|addr| addr.port.clone())
            .ok_or_else(|| OpentsdbError::OpentsUrlIsRequired(dsn.clone()))?;
        let protocol = dsn.protocol.as_deref().unwrap_or("http");
        let opents_url = format!("{}://{}:{}/", protocol, host, port);

        // the addr for connector to agent
        let taosx_host = String::from("127.0.0.1");
        let taosx_port = ipc;

        // the task config
        let task_mode = dsn.remove("mode").unwrap_or("normal".to_string());
        let task_metrics = dsn
            .remove("metrics")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect_vec();
        let task_begin_time = dsn
            .remove("beginTime")
            .ok_or_else(|| OpentsdbError::TaskBeginTimeIsRequired(dsn.clone()))?;
        let task_end_ime = dsn.remove("endTime");

        // the performance config
        let performance_read_window = dsn.remove("readWindow");

        // agent监听地址
        let ipc_stream = format!("127.0.0.1:{ipc}");

        let opents = OpentsConfig { opents_url };

        let taosx = TaosxConfig {
            taosx_host,
            taosx_port,
        };

        let task = TaskConfig {
            task_mode,
            task_metrics,
            task_begin_time,
            task_end_time: task_end_ime,
        };

        let performance = PerformanceConfig {
            performance_read_window,
        };

        Ok(Self {
            opents,
            taosx,
            task,
            performance,
            td_database,
            ipc_stream,
        })
    }
}

const EXE: &'static str = "taosx-opentsdb.jar";

fn opentsdb_jar_path() -> PathBuf {
    get_plugin_dir("opentsdb").join(EXE)
}

const LOG_FILE: &str = "opentsdb.log";

fn log_path() -> PathBuf {
    super::get_log_dir("opentsdb")
}

pub fn info() -> Result<(&'static str, PathBuf, String), std::io::Error> {
    let path = opentsdb_jar_path();
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
) -> anyhow::Result<()> {
    println!("# loading plugin: OpentsDB");

    let exe_exists = std::path::Path::new(&opentsdb_jar_path()).exists();
    if !exe_exists {
        tracing::error!("plugin not found {}", opentsdb_jar_path().to_str().unwrap());
        Err(OpentsdbError::ExeNotFound(format!(
            "{}",
            opentsdb_jar_path().to_str().unwrap()
        )))?;
    }

    // tdengine
    let td_database = to.subject.clone();
    // let target_pool = <TaosBuilder as taos::AsyncTBuilder>::from_dsn(&to)?.pool()?;
    // let target_pool_for_ipc = target_pool.clone();
    // a random port
    let ipc_port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for OpenTSDB connection"))?;
    // generate config
    let config = OpentsdbConfig::new(from, td_database.unwrap(), ipc_port)?;
    // transform to toml
    let toml = toml::to_string(&config)?;
    // write to a temporary file
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    // get the path of the temporary file
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();
    tracing::info!("Using config file {}", config_path.display());

    let exec_span = tracing::info_span!("extern plugin exec", plugin.name = "opentsdb");
    exec_span.follows_from(&span);
    // create socket channel
    let mut ipc_handler = build_ipc(
        &config.ipc_stream,
        None,
        &to,
        Some("opentsdb"),
        None,
        &cancel,
        with_agent,
        transferred,
        span,
    )
    .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;
    // 连接器路径
    let connector_path = opentsdb_jar_path();

    let mut log_path = log_path();

    fs::create_dir_all(&log_path)?;

    tracing::info!("log path created: {}", &log_path.display());

    log_path.push(LOG_FILE);

    tracing::info!("log file dir: {}", &log_path.display());

    let log_keep_days = get_log_keep_days();

    let mut log_rotation = FileRotate::new(
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
    );

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
            .arg(&config_path)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped());
    } else {
        child = command
            .arg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .arg("-jar")
            .arg(&connector_path)
            .arg(&config_path)
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
            tokio::select! {
                // application exit with error code
                status = child.wait().instrument(tracing::info_span!("process", plugin.pid = pid)) => {
                    let status = status?;
                    tracing::info!("OpenTSDB exit with {}", status);
                    if !status.success() {
                        use ringbuf::Rb;
                        let _ = ipc_handler.close().await?;
                        let error = error_buf.lock().await.iter().join("");
                        anyhow::bail!("OpenTSDB exit with {}\n{error}", status);
                    }
                },
                err = ipc_handler.recv_error() => {
                    tracing::info!("have received worker thread panicked message, terminate child process");
                    if let Some(err) = err {
                        let _ = child.kill().await;
                        let _ = ipc_handler.close().await?;
                        anyhow::bail!("OpenTSDB writer error: {err}");
                    }
                },
                _ = cancel.cancelled() => {
                    tracing::info!("OpenTSDB task cancelled");
                }
            }
            ;
            // stop the connector
            let _ = child.kill().await;
            // send an empty tuple
            ipc_handler.close().await?;
            tracing::info!("OpenTSDB task Done");
            // delete the temporary file
            let _ = temp_path.close();
            // put ipc port back to port pool.
            port_pool.put(ipc_port);
            // wait for completion
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }.instrument(exec_span)).await??;
    }
    Ok(())
}

pub async fn opentsdb_datasets(dsn: Dsn) -> anyhow::Result<Vec<DataSet>> {
    let host = dsn
        .addresses
        .first()
        .and_then(|addr| addr.host.clone())
        .ok_or_else(|| OpentsdbError::OpentsUrlIsRequired(dsn.clone()))?;
    let port = dsn
        .addresses
        .first()
        .and_then(|addr| addr.port.clone())
        .ok_or_else(|| OpentsdbError::OpentsUrlIsRequired(dsn.clone()))?;
    let protocol = dsn.protocol.as_deref().unwrap_or("http");
    let opents_url = format!("{}://{}:{}/", protocol, host, port);
    // 连接器路径
    let connector_path = opentsdb_jar_path();
    // startup the connector
    let mut command = tokio::process::Command::new("java");
    // 查询命令
    let output = command
        .arg("-jar")
        .arg(&connector_path)
        .arg("-fetch")
        .arg(&opents_url)
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
                anyhow::bail!("Unknown exit code {exit}, maybe failed to connect, ip or port error")
            }
        }
    }
}

pub async fn opentsdb_validate(dsn: Dsn) -> anyhow::Result<ValidatedSource> {
    let host = dsn
        .addresses
        .first()
        .and_then(|addr| addr.host.clone())
        .ok_or_else(|| OpentsdbError::OpentsUrlIsRequired(dsn.clone()))?;
    let port = dsn
        .addresses
        .first()
        .and_then(|addr| addr.port.clone())
        .ok_or_else(|| OpentsdbError::OpentsUrlIsRequired(dsn.clone()))?;
    let protocol = dsn.protocol.as_deref().unwrap_or("http");
    let opents_url = format!("{}://{}:{}/api/version", protocol, host, port);
    // http 客户端
    let client = reqwest::Client::new();
    // 发送请求，获取结果
    let mut result = client.get(opents_url).send().await;
    // 请求成功
    if result.is_ok() {
        let response = result.unwrap();
        let mut text = response.text().await.unwrap();
        // 转换为json格式
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        // 组装结果
        Ok(ValidatedSource {
            available: true,
            version: Some(json.get("version").unwrap().to_string()),
            since: Some(String::from("")),
        })
    } else {
        Ok(ValidatedSource {
            available: false,
            version: Some(String::from("")),
            since: Some(result.err().unwrap().source().unwrap().to_string()),
        })
    }
}