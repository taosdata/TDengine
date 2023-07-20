use std::{fs, io::prelude::*, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use file_rotate::{
    compression::Compression,
    suffix::{AppendTimestamp, DateFrom, FileLimit},
    ContentLimit, FileRotate, TimeFrequency,
};
use itertools::Itertools;
use taos::{AsyncTBuilder, Dsn, TaosBuilder};
use tokio::io::AsyncBufReadExt;
use tokio_util::sync::CancellationToken;

use crate::{
    get_log_keep_days, plugins::sink, utils::port_pool::PortPool, Action, DataSet, Transferred,
};

use super::get_plugin_dir;

const INFLUXDB_V1: [&str; 2] = ["1.7", "1.8"];
const INFLUXDB_V2: [&str; 8] = ["2.0", "2.1", "2.2", "2.3", "2.4", "2.5", "2.6", "2.7"];

#[derive(Debug, serde::Serialize)]
struct InfluxdbConfig {
    // the datasource config
    influx: InfluxConfig,
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
struct InfluxConfig {
    #[serde(rename = "url")]
    influx_url: String,
    #[serde(rename = "version")]
    influx_version: String,
    #[serde(rename = "username")]
    influx_username: String,
    #[serde(rename = "password")]
    influx_password: String,
    #[serde(rename = "token")]
    influx_token: String,
    #[serde(rename = "orgId")]
    influx_org_id: String,
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
    #[serde(rename = "bucket")]
    task_bucket: String,
    #[serde(rename = "measurements")]
    task_measurements: Vec<String>,
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
pub enum InfluxdbError {
    #[error("The access address of InfluxDB is required: {0}")]
    InfluxUrlIsRequired(Dsn),
    #[error("The version of InfluxDB is required: {0}")]
    InfluxVersionIsRequired(Dsn),
    #[error("The username is required: {0}")]
    InfluxUsernameIsRequired(Dsn),
    #[error("The password is required: {0}")]
    InfluxPasswordIsRequired(Dsn),
    #[error("The access token is required: {0}")]
    InfluxTokenIsRequired(Dsn),
    #[error("The organization id is required: {0}")]
    InfluxOrgIdIsRequired(Dsn),
    #[error("The data begin time is required: {0}")]
    TaskBeginTimeIsRequired(Dsn),
    #[error("The bucket is required: {0}")]
    TaskBucketIsRequired(Dsn),
    #[error("plugin not found: {0}")]
    ExeNotFound(String),
}

impl InfluxdbConfig {
    pub fn new(mut dsn: Dsn, td_database: String, ipc: u16) -> Result<Self, InfluxdbError> {
        debug_assert!(dsn.driver == "influxdb");
        // the datasource config
        let host = dsn
            .addresses
            .first()
            .and_then(|addr| addr.host.clone())
            .ok_or_else(|| InfluxdbError::InfluxUrlIsRequired(dsn.clone()))?;
        let port = dsn
            .addresses
            .first()
            .and_then(|addr| addr.port.clone())
            .ok_or_else(|| InfluxdbError::InfluxUrlIsRequired(dsn.clone()))?;
        let influx_url = format!("http://{}:{}/", host, port);
        let influx_version = dsn
            .remove("version")
            .ok_or_else(|| InfluxdbError::InfluxVersionIsRequired(dsn.clone()))?;
        // On version 1.x, only username/password mode can be used
        // On version 2.x, only access token mode can be used.
        let influx_org_id = dsn.remove("orgId").unwrap_or("".to_string());
        let influx_username = dsn.remove("username").unwrap_or("".to_string());
        let influx_password = dsn.remove("password").unwrap_or("".to_string());
        let influx_token = dsn.remove("token").unwrap_or("".to_string());
        if INFLUXDB_V1.contains(&influx_version.as_str()) && influx_username == "" {
            return Err(InfluxdbError::InfluxUsernameIsRequired(dsn.clone()));
        } else if INFLUXDB_V1.contains(&influx_version.as_str()) && influx_password == "" {
            return Err(InfluxdbError::InfluxPasswordIsRequired(dsn.clone()));
        } else if INFLUXDB_V2.contains(&influx_version.as_str()) && influx_org_id == "" {
            return Err(InfluxdbError::InfluxOrgIdIsRequired(dsn.clone()));
        } else if INFLUXDB_V2.contains(&influx_version.as_str()) && influx_token == "" {
            return Err(InfluxdbError::InfluxTokenIsRequired(dsn.clone()));
        }

        // the addr for connector to agent
        let taosx_host = String::from("127.0.0.1");
        let taosx_port = ipc;

        // the task config
        let task_mode = dsn.remove("mode").unwrap_or("normal".to_string());
        let task_bucket = dsn
            .remove("bucket")
            .ok_or_else(|| InfluxdbError::TaskBucketIsRequired(dsn.clone()))?;
        let task_measurements = dsn
            .remove("measurements")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect_vec();
        let task_begin_time = dsn
            .remove("beginTime")
            .ok_or_else(|| InfluxdbError::TaskBeginTimeIsRequired(dsn.clone()))?;
        let task_end_ime = dsn.remove("endTime");

        // the performance config
        let performance_read_window = dsn.remove("readWindow");

        // agent监听地址
        let ipc_stream = format!("127.0.0.1:{ipc}");

        let influx = InfluxConfig {
            influx_url,
            influx_version,
            influx_username,
            influx_password,
            influx_token,
            influx_org_id,
        };

        let taosx = TaosxConfig {
            taosx_host,
            taosx_port,
        };

        let task = TaskConfig {
            task_mode,
            task_bucket,
            task_measurements,
            task_begin_time,
            task_end_time: task_end_ime,
        };

        let performance = PerformanceConfig {
            performance_read_window,
        };

        Ok(Self {
            influx,
            taosx,
            task,
            performance,
            td_database,
            ipc_stream,
        })
    }
}

const EXE: &'static str = "taosx-influxdb.jar";

fn influxdb_jar_path() -> PathBuf {
    get_plugin_dir("influxdb").join(EXE)
}

const LOG_FILE: &str = "influxdb.log";

fn log_path() -> PathBuf {
    super::get_log_dir("influxdb")
}

pub fn info() -> Result<(&'static str, PathBuf, String), std::io::Error> {
    let path = influxdb_jar_path();
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
pub async fn influxdb_to_taos(
    from: Dsn,
    _actions: Vec<Action>,
    to: Dsn,
    _jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
) -> anyhow::Result<()> {
    println!("# loading plugin: InfluxDB");

    let exe_exists = std::path::Path::new(&influxdb_jar_path()).exists();
    if !exe_exists {
        log::error!("plugin not found {}", influxdb_jar_path().to_str().unwrap());
        Err(InfluxdbError::ExeNotFound(format!(
            "{}",
            influxdb_jar_path().to_str().unwrap()
        )))?;
    }

    // tdengine
    let td_database = to.subject.clone();
    let target_pool = <TaosBuilder as taos::AsyncTBuilder>::from_dsn(&to)?.pool()?;
    let target_pool_for_ipc = target_pool.clone();
    // a random port
    let ipc_port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for InfluxDB connection"))?;
    // generate config
    let config = InfluxdbConfig::new(from, td_database.unwrap(), ipc_port)?;
    // transform to toml
    let toml = toml::to_string(&config)?;
    // write to a temporary file
    let mut config_file = tempfile::NamedTempFile::new()?;
    dbg!(&config_file);
    write!(config_file, "{}", &toml)?;
    // get the path of the temporary file
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();
    log::info!("Using config file {}", config_path.display());
    // create socket channel
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let ipc = if with_agent.is_none() {
        let builder = TaosBuilder::from_dsn(&to)?;
        #[cfg(not(feature = "disable-enterprise-only-validation"))]
        if !builder.is_enterprise_edition().await? {
            anyhow::bail!(
                "Only enterprise edition is supported. If it's not your case, please contact us."
            )
        }
        sink::listen_tcp_socket(
            target_pool_for_ipc,
            config.ipc_stream,
            sender,
            None,
            cancel.clone(),
            with_agent,
            None,
            Some("influxdb"),
            transferred,
        )?
    } else {
        sink::listen_tcp_socket_with_agent(
            config.ipc_stream,
            sender,
            None,
            cancel.clone(),
            with_agent.unwrap(),
        )?
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    // 连接器路径
    let connector_path = influxdb_jar_path();
    // startup the connector
    let mut command = tokio::process::Command::new("java");

    let mut log_path = log_path();

    fs::create_dir_all(&log_path)?;

    log::info!("log path created: {}", &log_path.display());

    log_path.push(LOG_FILE);

    log::info!("log file dir: {}", &log_path.display());

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

    let child = command
        .arg("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .arg("-jar")
        .arg(&connector_path)
        .arg(&config_path)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::piped());

    let port_pool = port_pool.clone();
    {
        let mut child = child.spawn().context("Start InfluxDB collector error")?;

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
                // Write the line to log_rotation
                write!(log_rotation, "{}", line).unwrap();
                line.clear();
            }
            Ok::<(), std::io::Error>(())
        });
        // waiting until the end
        log::info!("waiting for InfluxDB connector");
        tokio::spawn(async move {
            tokio::select! {
                // application exit with error code
                status = child.wait() => {
                    let status = status?;
                    log::info!("InfluxDB exit with {}", status);
                    if !status.success() {
                        let _ = ipc.send(());
                        anyhow::bail!("InfluxDB exit with {}", status);
                    }
                },
                err = receiver.recv() => {
                    log::info!("have received worker thread panicked message, terminate child process");
                    if let Some(err) = err {
                        let _ = ipc.send(());
                        anyhow::bail!("InfluxDB writer error: {err}");
                    }
                },
                _ = cancel.cancelled() => {
                    log::info!("InfluxDB task cancelled");
                }
            }
            ;
            // send an empty tuple
            ipc.send(())?;
            // stop the connector
            let _ = child.kill().await;
            log::info!("InfluxDB task Done");
            // delete the temporary file
            temp_path.close().unwrap();
            // put ipc port back to port pool.
            port_pool.put(ipc_port);
            // wait for completion
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }).await??;
    }
    Ok(())
}

pub async fn influxdb_datasets(mut dsn: Dsn) -> anyhow::Result<Vec<DataSet>> {
    let host = dsn
        .addresses
        .first()
        .and_then(|addr| addr.host.clone())
        .ok_or_else(|| InfluxdbError::InfluxUrlIsRequired(dsn.clone()))?;
    let port = dsn
        .addresses
        .first()
        .and_then(|addr| addr.port.clone())
        .ok_or_else(|| InfluxdbError::InfluxUrlIsRequired(dsn.clone()))?;
    let influx_url = format!("http://{}:{}/", host, port);
    let influx_version = dsn
        .remove("version")
        .ok_or_else(|| InfluxdbError::InfluxVersionIsRequired(dsn.clone()))?;
    // On version 1.x, only username/password mode can be used
    // On version 2.x, only access token mode can be used.
    let influx_username = dsn.remove("username").unwrap_or("".to_string());
    let influx_password = dsn.remove("password").unwrap_or("".to_string());
    let influx_token = dsn.remove("token").unwrap_or("".to_string());
    if INFLUXDB_V1.contains(&influx_version.as_str()) && influx_username == "" {
        anyhow::bail!("The username is required");
    } else if INFLUXDB_V1.contains(&influx_version.as_str()) && influx_password == "" {
        anyhow::bail!("The password is required");
    } else if INFLUXDB_V2.contains(&influx_version.as_str()) && influx_token == "" {
        anyhow::bail!("The access token is required");
    }
    // 连接器路径
    let connector_path = influxdb_jar_path();
    // startup the connector
    let mut command = tokio::process::Command::new("java");
    // 查询命令
    let output;
    // 不同版本不同参数
    if INFLUXDB_V1.contains(&influx_version.as_str()) {
        // 查询命令
        output = command
            .arg("-jar")
            .arg(&connector_path)
            .arg("-fetch")
            .arg(&influx_version)
            .arg(&influx_url)
            .arg(&influx_username)
            .arg(&influx_password)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .with_context(|| "Start InfluxDB collector error")?;
    } else {
        output = command
            .arg("-jar")
            .arg(&connector_path)
            .arg("-fetch")
            .arg(&influx_version)
            .arg(&influx_url)
            .arg(&influx_token)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .with_context(|| "Start InfluxDB collector error")?;
    }
    let s = String::from_utf8(output.stdout.clone())?;
    dbg!(&s);
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
}
