use std::{io::prelude::*, num::ParseIntError, time::Duration};

use actix_web::web::Json;
use anyhow::Context;
use itertools::Itertools;
use serde_json::{Map, Value};
use taos::{AsyncTBuilder, Dsn, IntoDsn, TaosBuilder};
use tokio_util::sync::CancellationToken;
use tracing::field::debug;

use crate::{
    Action,
    DataSet,
    DataSetsReq, plugins::{service::spawn_rest_service, sink}, utils::{port_pool::PortPool, stop_thread},
};

#[derive(Debug, serde::Serialize)]
struct InfluxdbConfig {
    // the datasource config
    influx: InfluxConfig,
    // the addr for connector to agent
    taosx: TaosxConfig,
    // the task config
    task: TaskConfig,

    // others
    #[serde(skip)]
    td_database: String,
    #[serde(skip)]
    ipc_stream: String,
}

#[derive(Debug, serde::Serialize)]
struct InfluxConfig {
    #[serde(rename = "url")]
    influx_url: String,
    #[serde(rename = "token")]
    influx_token: String,
    #[serde(rename = "orgId")]
    influx_orgId: String,
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
    #[serde(rename = "buckets")]
    task_buckets: Vec<String>,
    #[serde(rename = "beginTime")]
    task_beginTime: String,
    #[serde(rename = "endTime")]
    task_endTime: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum InfluxdbError {
    #[error("The access address of InfluxDB is required: {0}")]
    InfluxUrlIsRequired(Dsn),
    #[error("The access token is required: {0}")]
    InfluxTokenIsRequired(Dsn),
    #[error("The organization id is required: {0}")]
    InfluxOrgIdIsRequired(Dsn),
    #[error("The data begin time is required: {0}")]
    TaskBeginTimeIsRequired(Dsn),
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
        let influx_token = dsn
            .remove("token")
            .ok_or_else(|| InfluxdbError::InfluxTokenIsRequired(dsn.clone()))?;
        let influx_orgId = dsn
            .remove("orgId")
            .ok_or_else(|| InfluxdbError::InfluxOrgIdIsRequired(dsn.clone()))?;

        // the addr for connector to agent
        let taosx_host = String::from("127.0.0.1");
        let taosx_port = ipc;

        // the task config
        let task_mode = dsn.remove("mode").unwrap_or("normal".to_string());
        let task_buckets = dsn
            .remove("buckets")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect_vec();
        let task_beginTime = dsn
            .remove("beginTime")
            .ok_or_else(|| InfluxdbError::TaskBeginTimeIsRequired(dsn.clone()))?;
        let task_endTime = dsn.remove("endTime");

        // agent监听地址
        let ipc_stream = format!("127.0.0.1:{ipc}");

        let influx = InfluxConfig {
            influx_url,
            influx_token,
            influx_orgId,
        };

        let taosx = TaosxConfig {
            taosx_host,
            taosx_port,
        };

        let task = TaskConfig {
            task_mode,
            task_buckets,
            task_beginTime,
            task_endTime,
        };

        Ok(Self {
            influx,
            taosx,
            task,
            td_database,
            ipc_stream,
        })
    }
}

/// InfluxDB DSN example: "influxdb://127.0.0.1:8086/?token=abc&orgId=def&mode=normal&beginTime=2023-05-01&endTime="
pub async fn influxdb_to_taos(
    mut from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
) -> anyhow::Result<()> {
    println!("# loading plugin: InfluxDB");
    // tdengine
    let td_database = to.subject.clone();
    let target_pool = <TaosBuilder as taos::AsyncTBuilder>::from_dsn(to)?.pool()?;
    let target_pool_for_ipc = target_pool.clone();
    // a random port
    let ipc = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for InfluxDB connection"))?;
    // generate config
    let config = InfluxdbConfig::new(from, td_database.unwrap(), ipc)?;
    // transform to toml
    let toml = toml::to_string(&config)?;
    // write to a temporary file
    let mut config_file = tempfile::NamedTempFile::new()?;
    dbg!(&config_file);
    write!(config_file, "{}", &toml)?;
    // get the path of the temporary file
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();
    log::info!("Using config file {} \n{}", config_path.display(), toml);
    // create socket channel
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let ipc = sink::listen_tcp_socket(
        target_pool_for_ipc,
        config.ipc_stream,
        sender,
        None,
        cancel.clone(),
        with_agent,
        None,
    )?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    // 连接器路径
    let connectorPath = if cfg!(target_os = "windows") {
        "C:\\TDengine\\xplugins\\influxdb\\taosx-influxdb.bat"
    } else {
        "/usr/local/taos/xplugins/influxdb/taosx-influxdb.sh"
    };
    // startup the connector
    let mut command = tokio::process::Command::new(connectorPath);
    let child = command
        .arg(&config_path)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());
    {
        let mut child = child.spawn().context("Start InfluxDB collector error")?;
        // waiting until the end
        log::info!("waiting for InfluxDB connector");
        tokio::spawn(async move {
            tokio::select! {
                // application exit with error code
                status = child.wait() => {
                    let status = status?;
                    log::info!("InfluxDB exit with status {}", status);
                    if !status.success() {
                        let _ = ipc.send(());
                        anyhow::bail!("InfluxDB exist with status {}", status);
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
            // wait for completion
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }).await??;
    }
    Ok(())
}
