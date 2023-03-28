use std::{
    collections::HashMap,
    io::prelude::*,
    num::ParseIntError,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

use anyhow::bail;
use anyhow::Context;
use itertools::Itertools;
use taos::{AsyncTBuilder, Dsn, TaosBuilder};
use taosx_ipc::prelude::IpcDataType;

use crate::{
    plugins::{service::spawn_rest_service, sink},
    utils::{port_pool::PortPool, stop_thread},
    Action,
};

#[derive(Debug, serde::Serialize)]
struct PiConfig {
    // system
    #[serde(rename = "PIServerName")]
    server_name: String,
    #[serde(rename = "PISystemName")]
    system_name: String,
    #[serde(rename = "AFDatabaseName")]
    database: String,
    #[serde(rename = "PIDataPipesInstances")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pi_data_pipes_instances: Option<u32>,
    #[serde(rename = "AFDataPipesInstances")]
    #[serde(skip_serializing_if = "Option::is_none")]
    af_data_pipes_instances: Option<u32>,
    // runtime
    #[serde(rename = "MaxWaitLen")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_wait_len: Option<u32>,
    #[serde(rename = "UpdateInterval")]
    #[serde(skip_serializing_if = "Option::is_none")]
    update_interval: Option<u32>,
    #[serde(rename = "MaxBackfillRangeDays")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_backfill_range_days: Option<u32>,

    #[serde(rename = "IPCStream")]
    ipc_stream: String,
    #[serde(rename = "SQLAPI")]
    sql_api: String,
    // data set
    #[serde(rename = "TemplateForPIPoint")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    template_for_pi_point: Vec<String>,
    #[serde(rename = "TemplateForAFElement")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    template_for_af_element: Vec<String>,
    #[serde(rename = "Points")]
    #[serde(skip_serializing_if = "Option::is_none")]
    points: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum PiError {
    #[error("Server is required in PI dsn: {0}")]
    ServerIsRequired(Dsn),
    #[error("Database name is required in PI dsn: {0}")]
    DatabaseIsRequired(Dsn),
    #[error("Parse integer error from {1} while parsing parameter {0}: {2:?}")]
    ParseNumberError(&'static str, String, ParseIntError),
}

impl PiConfig {
    pub fn new(mut dsn: Dsn, ipc: u16, sql: u16) -> Result<Self, PiError> {
        debug_assert!(dsn.driver == "pi");
        let server_name = dsn
            .addresses
            .first()
            .and_then(|addr| addr.host.clone())
            .ok_or_else(|| PiError::ServerIsRequired(dsn.clone()))?;
        let system_name = dsn
            .remove("PISystemName")
            .unwrap_or_else(|| server_name.clone());
        let database = dsn
            .subject
            .clone()
            .ok_or_else(|| PiError::DatabaseIsRequired(dsn.clone()))?;

        macro_rules! parse_int_at {
            ($n:expr) => {
                dsn.remove($n)
                    .map(|v| {
                        v.parse::<u32>()
                            .map_err(|err| PiError::ParseNumberError($n, v, err))
                    })
                    .transpose()?
            };
        }
        let pi_data_pipes_instances = parse_int_at!("PIDataPipesInstances");
        let af_data_pipes_instances = parse_int_at!("AFDataPipesInstances");
        let max_wait_len = parse_int_at!("MaxWaitLen");
        let update_interval = parse_int_at!("UpdateInterval");
        let max_backfill_range_days = parse_int_at!("MaxBackfillRangeDays");

        let template_for_pi_point = dsn
            .remove("TemplateForPIPoint")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect_vec();
        let template_for_af_element = dsn
            .remove("TemplateForAFElement")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect_vec();
        let points = dsn.remove("Points").map(|s| Path::new(&s).to_path_buf());

        let ipc_stream = format!("127.0.0.1:{ipc}");
        let sql_api = format!("http://127.0.0.1:{sql}");

        // dsn.addresses
        Ok(Self {
            server_name,
            system_name,
            database,
            pi_data_pipes_instances,
            af_data_pipes_instances,
            max_wait_len,
            update_interval,
            max_backfill_range_days,
            ipc_stream,
            sql_api,
            template_for_pi_point,
            template_for_af_element,
            points,
        })
    }
}

/// PI DSN example: "pi://WIN-2OA23UM12TN/Met1?PISystemName=other&points=@<file>"
pub async fn pi_to_taos(
    mut from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    jobs: usize,
    port_pool: &PortPool,
) -> anyhow::Result<()> {
    println!("# loading plugin: PI");
    #[cfg(not(target_os = "windows"))]
    {
        anyhow::bail!("PI connector support only windows platform");
    }

    let target_pool = <TaosBuilder as taos::AsyncTBuilder>::from_dsn(to)?.pool()?;

    let taos = target_pool.get().await?;

    let target_pool_for_ipc = target_pool.clone();

    let ipc = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for PI connection"))?;
    let sql = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for PI connection"))?;

    let config = PiConfig::new(from, ipc, sql)?;

    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    log::info!("Using config file {} \n{}", config_path.display(), toml);

    let server = std::thread::spawn(move || spawn_rest_service(target_pool, sql));

    let ipc = std::thread::spawn(move || {
        sink::listen_tcp_socket(target_pool_for_ipc, config.ipc_stream, None)
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = reqwest::Client::new();
    let mut retries = 0;
    loop {
        let resp = client.get(format!("{}/ping", config.sql_api)).send().await;
        if resp.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        if retries > 600 {
            break;
        }
        retries += 1;
    }

    let v = tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new(
            "C:\\Program Files (x86)\\TD PI Connector\\TDPIConnector.Service.exe",
            // "target/debug/examples/pi",
        );
        command
            .arg("-f")
            .arg(&config_path)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .output()
    });

    log::info!("waiting for PI connector");
    tokio::select! {
        output = v => {
            let output = output.context("join error")?.context("PI connector run error")?;
            // log::info!("PI exit with stdout: {}", std::str::from_utf8(&output.stdout).unwrap());
            // log::info!("PI exit with stderr: {}", std::str::from_utf8(&output.stderr).unwrap());
            log::info!("PI exit with status {}", output.status);
        },
        _ = tokio::signal::ctrl_c() => {
            log::info!("Ctrl+C triggered, cancel tasks");
            // panic!();
        }
    };

    stop_thread(ipc);
    stop_thread(server);

    temp_path.close()?;
    // rt.handle();
    // (&unsafe { *Arc::into_raw(rt) }).shutdown_background();
    log::info!("Done");
    // server.abort();
    Ok(())
}
