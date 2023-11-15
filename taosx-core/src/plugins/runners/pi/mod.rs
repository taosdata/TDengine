use std::{fs, io::prelude::*, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use itertools::Itertools;
use serde::Deserialize;
use serde_json::{Map, Value};
use taos::{AsyncTBuilder, Dsn, IntoDsn, TaosBuilder};
use tokio_process_terminate::TerminateExt;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Span};

use crate::dsv::DataSourceValidation;
use crate::runners::log_rotation;
use crate::runners::pi::config::PiConfig;
use crate::{
    build_ipc, get_log_keep_days, plugins::service::spawn_rest_service, utils::port_pool::PortPool,
    Action, DataSet, DataSetsReq, Transferred,
};

mod config;

fn pi_exe_path() -> anyhow::Result<PathBuf> {
    let path = super::get_plugin_dir("pi").join("taosx-pi.exe");
    if !path.exists() {
        let err_msg = format!("pi plugin not found at: {:?}", path);
        tracing::error!(err_msg);
        return Err(anyhow::anyhow!(err_msg));
    }
    Ok(path)
}

fn pi_backfill_exe_path() -> anyhow::Result<PathBuf> {
    let path = super::get_plugin_dir("pi").join("taosx-pi-backfill.exe");
    if !path.exists() {
        let err_msg = format!("pibackfill plugin not found at: {:?}", path);
        tracing::error!(err_msg);
        return Err(anyhow::anyhow!(err_msg));
    }
    Ok(path)
}

const LOG_FILE: &str = "pi.log";

fn log_path() -> PathBuf {
    super::get_log_dir("")
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
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    println!("# loading plugin: {}", from.driver);
    #[cfg(not(target_os = "windows"))]
    {
        anyhow::bail!("PI connector support only windows platform");
    }
    let td_database = to.subject.clone();
    let target_pool = <TaosBuilder as taos::AsyncTBuilder>::from_dsn(&to)?.pool()?;
    let target_pool_for_ipc = target_pool.clone();

    let ipc_port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for PI connection"))?;
    let sql_port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for PI connection"))?;
    let driver = from.driver.clone();
    let config = PiConfig::new(from, td_database.unwrap(), ipc_port, sql_port, true).await?;

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
            let mut command = tokio::process::Command::new(pi_exe_path()?);
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
            let mut command = tokio::process::Command::new(pi_backfill_exe_path()?);
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

    let server_cancellation_token = CancellationToken::new();
    let server_cancellation_token_cloned = server_cancellation_token.clone();
    let server = std::thread::spawn(move || {
        spawn_rest_service(target_pool, sql_port, server_cancellation_token_cloned)
    });

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
        None,
        notify,
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
            let mut command = tokio::process::Command::new(pi_exe_path()?);
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
            let mut command = tokio::process::Command::new(pi_backfill_exe_path()?);
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
                tokio::spawn(async move {
                    let _ = child_command
                        .terminate_timeout(Duration::from_secs(2))
                        .await;
                    tokio::spawn(async move {
                        tracing::info!("Wait for IPC handlers finished");
                        let _ = ipc.close().await;
                        tracing::info!("All IPC handlers have been finished");
                    });
                    temp_path.close().unwrap();
                    port_pool.put(ipc_port).await;
                    tokio::spawn(async move {
                        tracing::info!("Wait for rest api server finished");
                        server_cancellation_token.cancel();
                        let _ = server.join();
                        tracing::info!("REST api server has been finished");
                    });
                    port_pool.put(sql_port).await;
                });
            };
            (wait) => {
                tokio::spawn(async move {
                    let _ = child_command
                        .terminate_timeout(Duration::from_secs(2))
                        .await;
                    tokio::spawn(async move {
                        tracing::info!("Wait for IPC handlers finished");
                        let _ = ipc.close().await;
                        tracing::info!("All IPC handlers have been finished");
                    });
                    temp_path.close().unwrap();
                    tokio::spawn(async move {
                        tracing::info!("Wait for rest api server finished");
                        port_pool.put(ipc_port).await;
                        server_cancellation_token.cancel();
                        let _ = server.join();
                        tracing::info!("REST api server has been finished");
                        port_pool.put(sql_port).await;
                    });
                });
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
                    safe_exit!(wait);
                    anyhow::bail!("PI writer error: {err}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("pi task cancelled");
                safe_exit!(wait);
            }
        }
        tracing::info!("pi task Done");
        Ok(())
    })
    .await??;

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

    let mut command = tokio::process::Command::new(pi_exe_path()?);
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

    let mut log_rotation = log_rotation(&log_path, 700);

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

#[allow(unused_variables, unreachable_code)]
pub async fn is_pi_valid(dsn: &Dsn) -> DataSourceValidation {
    #[cfg(not(target_os = "windows"))]
    {
        return DataSourceValidation::invalid(
            "pi".to_string(),
            "PI connector support only windows platform".to_string(),
        );
    }
    let config = PiConfig::parse_connection(dsn, String::new(), 0, 0);
    match config {
        Err(err) => DataSourceValidation::invalid(
            "pi".to_string(),
            format!(
                "invalid pi dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            let valid = validate_pi(c).await;
            match valid {
                Err(err) => DataSourceValidation::invalid(
                    "pi".to_string(),
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

async fn validate_pi(config: PiConfig) -> anyhow::Result<DataSourceValidation> {
    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_file = config_file.into_temp_path(); // close the file to avoid file lock error

    // startup the connector
    let pi_exe_path = pi_exe_path()?;
    let mut command = tokio::process::Command::new(pi_exe_path.clone());
    let output = command
        .arg("-c")
        .arg(&config_path)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .with_context(|| format!("failed to execute pi: {:?}", pi_exe_path.as_path()))?;

    let dsv = if output.status.success() {
        let result: serde_json::Value =
            serde_json::from_slice(&output.stdout).with_context(|| {
                format!(
                    "Deserialize validation result error: {}",
                    String::from_utf8_lossy(&output.stdout)
                )
            })?;
        tracing::debug!("pi validation result: {}", &result);
        DataSourceValidation {
            valid: result["valid"]
                .as_bool()
                .or(result["avaliable"].as_bool())
                .unwrap_or(false),
            support: result["support"]
                .as_bool()
                .or(result["avaliable"].as_bool())
                .unwrap_or(false),
            data_source: "pi".to_string(),
            version: result["version"].as_str().map(|s| s.to_string()),
            message: result["message"]
                .as_str()
                .or(result["since"].as_str())
                .map(|s| s.to_string()),
        }
    } else {
        DataSourceValidation::invalid(
            "pi".to_string(),
            format!(
                "failed to execute pi: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        )
    };
    temp_file.close()?;
    Ok(dsv)
}

#[allow(unused_variables, unreachable_code)]
pub async fn is_pi_backfill_valid(dsn: &Dsn) -> DataSourceValidation {
    #[cfg(not(target_os = "windows"))]
    {
        return DataSourceValidation::invalid(
            "pibackfill".to_string(),
            "PI Backfill connector support only windows platform".to_string(),
        );
    }
    let config = PiConfig::parse_connection(dsn, String::new(), 0, 0);
    match config {
        Err(err) => DataSourceValidation::invalid(
            "pibackfill".to_string(),
            format!(
                "invalid pibackfill dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            let valid = validate_pi_backfill(c).await;
            match valid {
                Err(err) => DataSourceValidation::invalid(
                    "pibackfill".to_string(),
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

async fn validate_pi_backfill(config: PiConfig) -> anyhow::Result<DataSourceValidation> {
    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_file = config_file.into_temp_path(); // close the file to avoid file lock error

    // startup the connector
    let pi_backfill_exe_path = pi_backfill_exe_path()?;
    let mut command = tokio::process::Command::new(pi_backfill_exe_path.clone());
    let output = command
        .arg("-c")
        .arg(&config_path)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .with_context(|| format!("failed to execute pi: {:?}", pi_backfill_exe_path.as_path()))?;

    let dsv = if output.status.success() {
        let result: serde_json::Value =
            serde_json::from_slice(&output.stdout).with_context(|| {
                format!(
                    "Deserialize validation result error: {}",
                    String::from_utf8_lossy(&output.stdout)
                )
            })?;
        DataSourceValidation {
            valid: result["valid"]
                .as_bool()
                .or(result["avaliable"].as_bool())
                .unwrap_or(false),
            support: result["support"]
                .as_bool()
                .or(result["avaliable"].as_bool())
                .unwrap_or(false),
            data_source: "pibackfill".to_string(),
            version: result["version"].as_str().map(|s| s.to_string()),
            message: result["message"]
                .as_str()
                .or(result["since"].as_str())
                .map(|s| s.to_string()),
        }
    } else {
        DataSourceValidation::invalid(
            "pibackfill".to_string(),
            format!(
                "failed to execute pibackfill: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        )
    };
    temp_file.close()?;
    Ok(dsv)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use taos::Dsn;

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_is_pi_valid() {
        let dsn = Dsn::from_str("pi://").unwrap();
        let validation = is_pi_valid(&dsn).await;
        assert_eq!(false, validation.valid);
        assert_eq!(false, validation.support);
        assert_eq!("pi", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!(
            "invalid pi dsn: pi://, cause: PIServerName is required",
            validation.message.unwrap()
        );

        let dsn = Dsn::from_str("pi://WIN-2OA23UM12TN/Met1?PISystemName=other").unwrap();
        let validation = is_pi_valid(&dsn).await;
        assert_eq!(false, validation.valid);
        assert_eq!(false, validation.support);
        assert_eq!("pi", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!("failed to connect to dsn: pi://WIN-2OA23UM12TN/Met1?PISystemName=other, cause: pi plugin not found at: \"/usr/local/taos/plugins/pi/taosx-pi.exe\"", validation.message.unwrap());
    }

    #[ignore]
    #[tokio::test]
    async fn test_is_pi_backfill_valid() {
        let dsn = Dsn::from_str("pibackfill://").unwrap();
        let validation = is_pi_backfill_valid(&dsn).await;
        assert_eq!(false, validation.valid);
        assert_eq!(false, validation.support);
        assert_eq!("pibackfill", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!(
            "invalid pibackfill dsn: pibackfill://, cause: PIServerName is required",
            validation.message.unwrap()
        );

        let dsn = Dsn::from_str("pibackfill://WIN-2OA23UM12TN/Met1?PISystemName=other").unwrap();
        let validation = is_pi_backfill_valid(&dsn).await;
        assert_eq!(false, validation.valid);
        assert_eq!(false, validation.support);
        assert_eq!("pibackfill", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!("failed to connect to dsn: pibackfill://WIN-2OA23UM12TN/Met1?PISystemName=other, cause: pibackfill plugin not found at: \"/usr/local/taos/plugins/pi/taosx-pi-backfill.exe\"", validation.message.unwrap());
    }
}
