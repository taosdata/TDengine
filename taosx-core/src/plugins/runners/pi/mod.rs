use std::{
    fs, io::prelude::*, num::ParseIntError, path::PathBuf, sync::Arc, time::Duration,
};

use anyhow::Context;
use file_rotate::{
    compression::Compression,
    ContentLimit,
    FileRotate, suffix::{AppendTimestamp, DateFrom, FileLimit}, TimeFrequency,
};
use itertools::Itertools;
use serde::Deserialize;
use serde_json::{Map, Value};
use taos::{AsyncTBuilder, Dsn, IntoDsn, TaosBuilder};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Span};

use crate::{
    Action, build_ipc,
    DataSet,
    DataSetsReq,
    get_log_keep_days, plugins::service::spawn_rest_service, Transferred, utils::{port_pool::PortPool, stop_thread},
};
use crate::runners::log_rotation;
use crate::runners::pi::config::PiConfig;
use crate::validation::DataSourceValidation;

mod config;

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
    #[error("Parse param error from {1} while parsing parameter {0}: {2}")]
    ParseError(&'static str, String, String),
    #[error("plugin not found: {0}")]
    ExeNotFound(String),
    #[error("pi config error: {0}")]
    ConfigError(String),
    #[error("Pi get data sets error: {0}")]
    GetDataSetError(#[from] anyhow::Error),
}

fn pi_exe_path() -> PathBuf {
    super::get_plugin_dir("pi").join("taosx-pi.exe")
}

fn pi_backfill_exe_path() -> PathBuf {
    super::get_plugin_dir("pi").join("taosx-pi-backfill.exe")
}

const LOG_FILE: &str = "pi.log";

fn log_path() -> PathBuf {
    super::get_log_dir("pi")
}

/// PI DSN example: "pi://WIN-2OA23UM12TN/Met1?PISystemName=other&points=@<file>"
#[allow(unused)]
pub async fn pi_to_taos(
    from: Dsn,
    actions: Vec<Action>,
    to: Dsn,
    jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    span: Span,
) -> anyhow::Result<()> {
    println!("# loading plugin: {}", from.driver);
    #[cfg(not(target_os = "windows"))]
    {
        anyhow::bail!("PI connector support only windows platform");
    }

    let exe_exists = std::path::Path::new(&pi_exe_path()).exists();
    if !exe_exists {
        tracing::error!("plugin not found {}", pi_exe_path().to_str().unwrap());
        Err(PiError::ExeNotFound(format!(
            "{}",
            pi_exe_path().to_str().unwrap()
        )))?;
    }

    let td_database = to.subject.clone();
    let target_pool = <TaosBuilder as taos::AsyncTBuilder>::from_dsn(&to)?.pool()?;

    // let taos = target_pool.get().await?;

    let target_pool_for_ipc = target_pool.clone();

    let ipc_port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for PI connection"))?;
    let sql_port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for PI connection"))?;
    let driver = from.driver.clone();
    let config = PiConfig::new(from, td_database.unwrap(), ipc_port, sql_port, true).await?;

    //toml::ser::ValueSerializer
    let toml = toml::to_string(&config)?;

    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    tracing::info!("Using config file {} \n{}", config_path.display(), toml);

    #[derive(Deserialize, Debug, Default)]
    struct IsValid {
        version: Option<String>,
        avaliable: bool,
        since: Option<String>,
        items: Vec<String>,
    }

    match driver.as_str() {
        "pi" => {
            let mut command = tokio::process::Command::new(pi_exe_path());
            let output = command
                .arg("-c")
                .arg(&config_path)
                .kill_on_drop(true)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .context("Check PI connector error")?
                .wait_with_output()
                .await
                .context("Check PI connector error")?;
            if output.status.success() {
                let stdout = String::from_utf8_lossy(output.stdout.as_slice());
                tracing::info!("PI connector check result: {}", stdout);
                let check = serde_json::from_str::<IsValid>(&stdout).map_err(|err| {
                    anyhow::format_err!(
                        "PI connector check result parse error: {}",
                        err.to_string()
                    )
                })?;
                tracing::debug!("{check:?}");
                if !check.avaliable {
                    anyhow::bail!(
                        "PI connector not available since {}:\n{}",
                        check.since.unwrap_or_default(),
                        check.items.join(","),
                    );
                }
            } else {
                let stderr = String::from_utf8_lossy(output.stderr.as_slice());
                tracing::error!("PI connector check error: {}", stderr);
                anyhow::bail!("Unable to check PI connector configuration");
            }
        }
        "pibackfill" => {
            let mut command = tokio::process::Command::new(pi_backfill_exe_path());
            let output = command
                .arg("-c")
                .arg(&config_path)
                .kill_on_drop(true)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .context("Check PI-Backfill connector error")?
                .wait_with_output()
                .await
                .context("Check PI-Backfill connector error")?;
            if output.status.success() {
                let stdout = String::from_utf8_lossy(output.stdout.as_slice());
                tracing::info!("PI connector check result: {}", stdout);
                let check = serde_json::from_str::<IsValid>(&stdout).map_err(|err| {
                    anyhow::format_err!(
                        "PI-Backfill connector check result parse error: {}",
                        err.to_string()
                    )
                })?;
                tracing::debug!("{check:?}");
                if !check.avaliable {
                    anyhow::bail!(
                        "PI-Backfill connector not available since {}:\n{}",
                        check.since.unwrap_or_default(),
                        check.items.join(","),
                    );
                }
            } else {
                let stderr = String::from_utf8_lossy(output.stderr.as_slice());
                tracing::error!("PI-Backfill connector check error: {}", stderr);
                anyhow::bail!("Unable to check PI connector configuration");
            }
        }
        _ => {
            anyhow::bail!("wrong driver configured");
        }
    }

    let server = std::thread::spawn(move || spawn_rest_service(target_pool, sql_port));

    let mut ipc = build_ipc(
        &config.ipc_stream,
        None,
        &to,
        Some("pi"),
        None,
        &cancel,
        with_agent,
        transferred,
        span,
    )
        .await?;
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

    let mut log_path = log_path();

    fs::create_dir_all(&log_path)?;

    tracing::info!("log path created: {}", &log_path.display());

    log_path.push(LOG_FILE);

    tracing::info!("log file dir: {}", &log_path.display());

    let log_keep_days = get_log_keep_days();

    let mut log_rotation = log_rotation(&log_path, log_keep_days);

    let mut child_command;

    match driver.as_str() {
        "pi" => {
            let mut command = tokio::process::Command::new(pi_exe_path());
            child_command = command
                .arg("-f")
                .arg(&config_path)
                .kill_on_drop(true)
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .context("Start PI collector error")?;
        }
        "pibackfill" => {
            let mut command = tokio::process::Command::new(pi_backfill_exe_path());
            child_command = command
                .arg("-f")
                .arg(&config_path)
                .kill_on_drop(true)
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .context("Start PI Backfill error")?;
        }
        _ => {
            anyhow::bail!("wrong driver configured");
        }
    }

    let stderr = child_command
        .stderr
        .take()
        .expect("Failed to capture stderr");
    tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(stderr);
        let mut line = String::new();
        use tokio::io::AsyncBufReadExt;
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

    let pid = child_command.id().unwrap();
    tracing::info!("waiting for PI connector");

    let port_pool = port_pool.clone();
    tokio::spawn(async move {
        macro_rules! safe_exit {
            () => {
                let _ = ipc.close().await;
                temp_path.close().unwrap();
                port_pool.put(ipc_port);
                stop_thread(server);
                port_pool.put(sql_port);
            };
        }
        tokio::select! {
            status = child_command.wait() => {
                let status = status?;
                tracing::info!("PI connector or PI backfill exit with {}", status);
                if !status.success() {
                    safe_exit!();
                    anyhow::bail!("PI connector or PI backfill exit with {}", status);
                }
            },
            err = ipc.recv_error() => {
                if let Some(err) = err {
                    tracing::warn!("PI writer error occurred: {err}");
                    safe_exit!();
                    anyhow::bail!("PI writer error: {err}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("pi task cancelled");
            }
        }
        let _ = ipc.send(()).await;
        terminate_child_process(pid)?;
        tracing::info!("pi task Done");
        safe_exit!();
        Ok(())
    })
        .await??;
    // stop_thread(ipc);
    // let _ = ipc.send(());
    // stop_thread(server);
    // tracing::info!("pi task Done");
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

#[allow(unused_variables, unreachable_code)]
#[instrument(skip(data), fields(plugin = "pi"))]
pub async fn pi_datasets(data: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    println!("# loading plugin: PI");
    #[cfg(not(target_os = "windows"))]
    {
        anyhow::bail!("PI connector support only windows platform");
    }

    let from_dsn = data.from.clone().into_dsn()?;
    let config = PiConfig::parse_connection(&from_dsn, String::new(), 0, 0)?;

    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    tracing::info!("Using config file {} \n{}", config_path.display(), toml);

    let mut command = tokio::process::Command::new(pi_exe_path());
    let point_filter = if let Some(pf) = data.pattern.clone() {
        pf
    } else {
        String::from("*")
    };

    let mut log_path = log_path();

    fs::create_dir_all(&log_path)?;

    tracing::info!("log path created: {}", &log_path.display());

    log_path.push(LOG_FILE);

    tracing::info!("log file dir: {}", &log_path.display());

    let mut log_rotation = FileRotate::new(
        &log_path,
        AppendTimestamp::with_format(
            "%Y-%m-%d",
            FileLimit::Age(chrono::Duration::weeks(100)),
            DateFrom::DateYesterday,
        ),
        ContentLimit::Time(TimeFrequency::Daily),
        Compression::None,
        #[cfg(unix)]
            None,
    );

    let output = command
        .arg("-f")
        .arg(&config_path)
        .arg("-p")
        .arg(point_filter)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await?;

    writeln!(log_rotation, "{}", String::from_utf8_lossy(&output.stderr))?;
    // .context("Start PI collector error")?;
    tracing::info!("PI Connector exit with status {}", output.status);

    let mut lines = output.stdout.lines();
    let json: Value = lines
        .find_map(|line| {
            let line = line.ok()?;
            if line.is_empty() {
                return None;
            }
            if line.len() < 10 {
                tracing::warn!("invalid json line: {}", &line);
                return None;
            }
            serde_json::from_str(&line).ok()
        })
        .ok_or_else(|| {
            tracing::error!(
                "No valid json data returned from PI connector: {}",
                String::from_utf8_lossy(&output.stdout)
            );
            anyhow::format_err!(
                "No valid json data returned from PI connector: {}",
                String::from_utf8_lossy(&output.stdout)
            )
        })?;
    tracing::debug!("pi dataset: {}", &json);
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

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    dbg!(dsn);
    DataSourceValidation::unknown()
}

#[cfg(test)]
mod tests {}
