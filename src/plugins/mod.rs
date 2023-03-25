use std::{
    io::prelude::*,
    num::ParseIntError,
    path::{Path, PathBuf},
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

use async_process::Stdio;
use itertools::Itertools;
use taos::{Dsn, TBuilder, TaosBuilder};

use crate::{plugins::service::spawn_rest_service, utils::port_pool::PortPool, Action};

mod config;
mod service;
mod sink;
mod source;
mod transform;

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
        let sql_api = format!("127.0.0.1:{sql}");

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

    let target_pool = TaosBuilder::from_dsn(to)?.pool()?;

    let taos = target_pool.get_timeout(Duration::from_secs(5))?;

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

    let server = spawn_rest_service(target_pool, 6052).await?;

    let ipc =
        std::thread::spawn(move || sink::listen_tcp_socket(target_pool_for_ipc, config.ipc_stream));

    // let ipc = ;
    // let ipc = tokio::spawn(future);

    let v = tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new(
            "C:\\Program Files (x86)\\TD PI Connector\\TDPIConnector.Service.exe",
            // "target/debug/examples/pi",
        );
        command
            .arg("-f")
            .arg(&config_path)
            // .stdout(Stdio::piped())
            // .stderr(Stdio::piped())
            .output()
    });

    tokio::select! {
        output = v => {
            let output = output??;
            // dbg!(output);
            log::info!("PI exit with status {}", output.status);
            // server.abort();
            panic!();
        },
        _ = server => {
            panic!();
        }
        _ = tokio::signal::ctrl_c() => {
            log::info!("Ctrl+C triggered, cancel tasks");
            // panic!();
        }
    };

    stop_thread(ipc);

    // rt.handle();
    // (&unsafe { *Arc::into_raw(rt) }).shutdown_background();
    temp_path.close()?;
    log::info!("Done");
    // server.abort();
    Ok(())
}

fn stop_thread<T>(handle: JoinHandle<T>) {
    #[cfg(windows)]
    unsafe {
        use std::os::windows::io::IntoRawHandle;
        use winapi::ctypes::c_void as winapi_c_void;
        use winapi::um::processthreadsapi::TerminateThread;

        let raw_handle = handle.into_raw_handle();
        TerminateThread(raw_handle as *mut winapi_c_void, 0);
    }
    #[cfg(unix)]
    unsafe {
        use libc::pthread_kill;
        use std::os::unix::thread::JoinHandleExt;

        let raw_handle = handle.into_pthread_t();
        pthread_kill(raw_handle, 2);
    };
}
