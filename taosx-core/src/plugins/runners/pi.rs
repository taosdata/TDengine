use std::{
    io::prelude::*, num::ParseIntError, path::PathBuf, str::FromStr, sync::Arc, time::Duration,
};

use anyhow::Context;
use chrono::{Local, NaiveDateTime};
use itertools::Itertools;
use serde_json::{Map, Value};
use taos::{AsyncTBuilder, Dsn, IntoDsn, TaosBuilder};
use tokio_util::sync::CancellationToken;
use toml::value::Datetime;

use crate::{
    plugins::{service::spawn_rest_service, sink},
    utils::{port_pool::PortPool, stop_thread},
    Action, DataSet, DataSetsReq, Transferred,
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
    #[serde(rename = "TDDataBase")]
    td_database: String,
    // data set
    #[serde(rename = "TemplateForPIPoint")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    template_for_pi_point: Vec<String>,
    #[serde(rename = "TemplateForAFElement")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    template_for_af_element: Vec<String>,
    #[serde(rename = "PointList")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    point_list: Vec<String>,
    // backfill param
    #[serde(rename = "FromTDengineLastTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    from_tdengine_last_time: Option<bool>,
    #[serde(rename = "ToTDengineFirstTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    to_tdengine_first_time: Option<bool>,
    #[serde(rename = "BackfillStartTime", skip_serializing_if = "Option::is_none")]
    backfill_start_time: Option<Datetime>,
    #[serde(rename = "BackfillEndTime", skip_serializing_if = "Option::is_none")]
    backfill_end_time: Option<Datetime>,
}

#[derive(Debug, thiserror::Error)]
pub enum PiError {
    #[error("Server is required in PI dsn: {0}")]
    ServerIsRequired(Dsn),
    #[error("Database name is required in PI dsn: {0}")]
    DatabaseIsRequired(Dsn),
    #[error("Parse integer error from {1} while parsing parameter {0}: {2:?}")]
    ParseNumberError(&'static str, String, ParseIntError),
    #[error("config value {0} error, the value needs between {1} and {2}")]
    ValueConfigError(&'static str, &'static str, &'static str),
    #[error("parse key {0} value error cause {1}")]
    ParseKeyValueError(&'static str, String),
    #[error("Parse param error from {1} while parsing parameter {0}")]
    ParseError(&'static str, String),
}

impl PiConfig {
    pub fn new(mut dsn: Dsn, td_database: String, ipc: u16, sql: u16) -> Result<Self, PiError> {
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
        if let Some(mwl) = max_wait_len {
            if mwl < 1 || mwl > 10000 {
                return Err(PiError::ValueConfigError("MaxWaitLen", "1", "10000"));
            }
        }
        let update_interval = parse_int_at!("UpdateInterval");
        if let Some(ui) = update_interval {
            if ui < 100 || ui > 60000 {
                return Err(PiError::ValueConfigError("UpdateInterval", "100", "60000"));
            }
        }
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
        let point_list =
            super::mqtt::get_string_from_param_or_file(&mut dsn, "PointList", false, Some(","))
                .map_err(|err| PiError::ParseKeyValueError("PointList", err))?
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect_vec();

        let ipc_stream = format!("127.0.0.1:{ipc}");
        let sql_api = format!("http://127.0.0.1:{sql}");
        let from_tdengine_last_time = if let Some(v) = dsn
            .remove("FromTDengineLastTime")
            .map(|v| {
                v.parse::<bool>()
                    .map_err(|err| PiError::ParseError("FromTDengineLastTime", v))
            })
            .transpose()?
        {
            Some(v)
        } else {
            None
        };
        let to_tdengine_first_time = if let Some(v) = dsn
            .remove("ToTDengineFirstTime")
            .map(|v| {
                v.parse::<bool>()
                    .map_err(|err| PiError::ParseError("ToTDengineFirstTime", v))
            })
            .transpose()?
        {
            Some(v)
        } else {
            None
        };

        let backfill_start_time = if let Some(backfill_start) = dsn.remove("BackfillStartTime") {
            let parsed_time =
                NaiveDateTime::parse_from_str(backfill_start.as_str(), "%Y-%m-%d %H:%M:%S")
                    .map_err(|err| {
                        PiError::ParseError("BackfillStartTime", backfill_start.clone())
                    })?
                    .and_local_timezone(Local)
                    .unwrap();
            let parsed_time = Datetime::from_str(parsed_time.to_rfc3339().as_str())
                .map_err(|err| PiError::ParseError("BackfillStartTime", backfill_start))?;
            Some(parsed_time)
        } else {
            None
        };
        let backfill_end_time = if let Some(backfill_start) = dsn.remove("BackfillEndTime") {
            let parsed_time =
                NaiveDateTime::parse_from_str(backfill_start.as_str(), "%Y-%m-%d %H:%M:%S")
                    .map_err(|err| PiError::ParseError("BackfillEndTime", backfill_start.clone()))?
                    .and_local_timezone(Local)
                    .unwrap();
            let parsed_time = Datetime::from_str(parsed_time.to_rfc3339().as_str())
                .map_err(|err| PiError::ParseError("BackfillEndTime", backfill_start))?;
            Some(parsed_time)
        } else {
            None
        };

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
            td_database,
            template_for_pi_point,
            template_for_af_element,
            point_list,
            from_tdengine_last_time,
            to_tdengine_first_time,
            backfill_start_time,
            backfill_end_time,
        })
    }
}

fn pi_exe_path() -> PathBuf {
    super::get_plugin_dir("pi").join("taosx-pi.exe")
}

fn pi_backfill_exe_path() -> PathBuf {
    super::get_plugin_dir("pi").join("taosx-pi-backfill.exe")
}
/// PI DSN example: "pi://WIN-2OA23UM12TN/Met1?PISystemName=other&points=@<file>"
pub async fn pi_to_taos(
    from: Dsn,
    actions: Vec<Action>,
    to: Dsn,
    jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
) -> anyhow::Result<()> {
    println!("# loading plugin: PI or PIBACKFILL");
    #[cfg(not(target_os = "windows"))]
    {
        anyhow::bail!("PI connector support only windows platform");
    }
    let td_database = to.subject.clone();
    let target_pool = <TaosBuilder as taos::AsyncTBuilder>::from_dsn(to)?.pool()?;

    // let taos = target_pool.get().await?;

    let target_pool_for_ipc = target_pool.clone();

    let ipc_port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for PI connection"))?;
    let sql = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for PI connection"))?;
    let driver = from.driver.clone();
    let config = PiConfig::new(from, td_database.unwrap(), ipc_port, sql)?;

    //toml::ser::ValueSerializer
    let toml = toml::to_string(&config)?;

    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    log::info!("Using config file {} \n{}", config_path.display(), toml);

    let server = std::thread::spawn(move || spawn_rest_service(target_pool, sql));
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let ipc = sink::listen_tcp_socket(
        target_pool_for_ipc,
        config.ipc_stream,
        sender,
        None,
        cancel.clone(),
        with_agent,
        None,
        Some("pi"),
        transferred,
    )?;
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

    let child_command;
    match driver.as_str() {
        "pi" => {
            let mut command = async_process::Command::new(pi_exe_path());
            child_command = command
                .arg("-f")
                .arg(&config_path)
                .stdout(async_process::Stdio::inherit())
                .stderr(async_process::Stdio::inherit())
                .spawn()
                .context("Start PI collector error")?;
        }
        "pibackfill" => {
            let mut command = async_process::Command::new(pi_backfill_exe_path());
            child_command = command
                .arg("-f")
                .arg(&config_path)
                .stdout(async_process::Stdio::inherit())
                .stderr(async_process::Stdio::inherit())
                .spawn()
                .context("Start PI Backfill error")?;
        }
        _ => {
            anyhow::bail!("wrong driver configured");
        }
    }

    let pid = child_command.id();
    log::info!("waiting for PI connector");

    let port_pool = port_pool.clone();
    tokio::spawn(async move {
        tokio::select! {
            output = child_command.output() => {
                let output = output.context("PI connector or PI backfill run error")?;
                log::info!("PI connector or PI backfill exit with status {}", output.status);
                if !output.status.success() {
                    let len = output.stdout.len();
                    let err = if len > 200 {
                        String::from_utf8_lossy(&output.stdout[len - 200..])
                    } else {
                        String::from_utf8_lossy(&output.stdout[..])
                    };

                    let _ = ipc.send(());
                    stop_thread(server);
                    anyhow::bail!("PI error: {}", err);
                }
            },
            _ = tokio::signal::ctrl_c() => {
                log::info!("Ctrl+C triggered, cancel tasks");
                cancel.cancel();
                // panic!();
            },
            err = receiver.recv() => {
                if let Some(err) = err {
                    log::warn!("PI writer error occurred: {err}");
                    let _ = ipc.send(());
                    stop_thread(server);
                    anyhow::bail!("PI writer error: {err}");
                }
            },
            _ = cancel.cancelled() => {
                log::info!("pi task cancelled");
            }
        }
        let _ = ipc.send(());
        stop_thread(server);
        terminate_child_process(pid)?;
        log::info!("pi task Done");
        temp_path.close().unwrap();
        port_pool.put(ipc_port);
        Ok(())
    })
    .await??;
    // stop_thread(ipc);
    // let _ = ipc.send(());
    // stop_thread(server);
    // log::info!("pi task Done");
    // temp_path.close().unwrap();

    Ok(())
}

fn terminate_child_process(id: u32) -> anyhow::Result<()> {
    let mut kill_command = std::process::Command::new("TASKKILL");
    kill_command
        .arg("/F")
        .arg("/PID")
        .arg(id.to_string())
        .spawn()?;
    Ok(())
}

pub async fn pi_datasets(data: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    println!("# loading plugin: PI");
    #[cfg(not(target_os = "windows"))]
    {
        anyhow::bail!("PI connector support only windows platform");
    }

    let config = PiConfig::new(data.from.clone().into_dsn()?, String::new(), 0, 0)?;

    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    log::info!("Using config file {} \n{}", config_path.display(), toml);

    let mut command = async_process::Command::new(pi_exe_path());
    let point_filter = if let Some(pf) = data.pattern.clone() {
        pf
    } else {
        String::from("*")
    };

    let output = command
        .arg("-f")
        .arg(&config_path)
        .arg("-p")
        .arg(point_filter)
        .stdout(async_process::Stdio::piped())
        .stderr(async_process::Stdio::piped())
        .output()
        .await?;
    // .context("Start PI collector error")?;
    log::info!("PI Connector exit with status {}", output.status);

    let json: Value = serde_json::from_slice(&output.stdout)?;
    log::debug!("pi dataset: {}", &json);
    let map = json.as_object().unwrap();
    let mut dataset = Vec::new();
    data.categories.iter().for_each(|category| {
        let result = if category.eq("PointList") {
            map_dataset(map, "pointsName", "PointList")
        } else if category.eq("TemplateForPIPoint") {
            map_dataset(map, "templateName", "TemplateForPIPoint")
        } else {
            map_dataset(map, "templateName", "TemplateForAFElement")
        };
        extend_data_set(&mut dataset, &result, data.offset, data.limit);
    });

    temp_path.close()?;
    Ok(dataset)
}

fn map_dataset(map: &Map<String, Value>, key: &str, category: &str) -> Vec<DataSet> {
    map.get(key)
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|f| DataSet {
            id: String::from(f.as_str().unwrap()),
            name: None,
            category: Some(String::from(category)),
            r#type: None,
            options: None,
            format: None,
        })
        .collect_vec()
}

fn extend_data_set(
    dataset: &mut Vec<DataSet>,
    extended_vec: &Vec<DataSet>,
    offset: usize,
    limit: usize,
) {
    let page_index = offset * limit;
    let len = extended_vec.len();
    if len >= page_index + limit {
        dataset.extend_from_slice(&extended_vec[page_index..page_index + limit]);
    } else if len > page_index && len < page_index + limit {
        dataset.extend_from_slice(&extended_vec[page_index..]);
    }
}
