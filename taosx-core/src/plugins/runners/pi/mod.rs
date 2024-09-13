use std::{fs, io::prelude::*, path::PathBuf, sync::Arc, time::Duration};

use super::get_data_dir;
use crate::dsv::DataSourceValidation;
use crate::runners::log_rotation;
use crate::runners::pi::config::PiConfig;
use crate::sink::lush::LushModelConfig;
use crate::utils::monitor::send_sub_process_info;
use crate::TaskNotify;
use crate::{build_ipc, get_log_keep_days, utils::port_pool::PortPool, Action, Transferred};
use anyhow::Context;
use serde::Deserialize;
use serde_json::Value;
use taos::{AsyncTBuilder, Dsn, TaosBuilder};
use tokio_process_terminate::TerminateExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

pub mod config;
pub mod transform;

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

fn log_path() -> PathBuf {
    super::get_log_dir("")
}

/// PI DSN example: "pi://WIN-2OA23UM12TN/Met1?PISystemName=other&points=@<file>"
#[allow(unused)]
#[instrument(skip_all)]
pub async fn pi_to_taos(
    from: Dsn,
    actions: Vec<Action>,
    to: Dsn,
    jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    tracing::info!("Start {} task", from.driver);
    // #[cfg(not(target_os = "windows"))]
    // {
    //     anyhow::bail!("PI connector support only windows platform");
    // }
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
    let config = PiConfig::new(
        from.clone(),
        td_database.unwrap(),
        ipc_port.get(),
        sql_port.get(),
        task_id,
    )
    .await
    .context("Failed to create PIConfig")?;
    pre_check_config(&config)?;
    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new().context("Failed to create tempfile")?;
    write!(config_file, "{}", &toml).context("Faile to write config file")?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();
    tracing::info!("Using config file {} \n{}", config_path.display(), toml);
    // save the temporary file to task dir
    match task_id {
        Some(task_id) => {
            let path = get_data_dir().join("tasks").join(task_id.to_string());
            std::fs::create_dir_all(&path).unwrap();
            let path = path.join(format!(
                "{}-{}-{}.{}",
                task_id,
                "pi",
                chrono::Local::now().format("%Y%m%d%H%M"),
                "toml"
            ));
            let _ = fs::copy(&config_path, path);
        }
        None => {}
    }

    let lush_model_config: Option<LushModelConfig> = if with_agent.is_none() {
        let config = LushModelConfig::try_from(from.clone())?;
        tracing::info!("Lush model config: {}", serde_json::to_string(&config)?);
        Some(config)
    } else {
        None
    };

    #[derive(Deserialize, Debug, Default)]
    struct IsValid {
        version: Option<String>,
        avaliable: bool,
        since: Option<String>,
        items: Vec<String>,
    }

    match from.driver.as_str() {
        "pi" | "pibackfill" => {
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
        _ => {
            anyhow::bail!("wrong driver configured");
        }
    }

    let mut ipc = build_ipc(
        &config.ipc_stream,
        None,
        &to,
        Some("pi"),
        None,
        lush_model_config,
        &cancel,
        with_agent,
        transferred,
        task_id.clone(),
        notify.clone(),
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut log_path = log_path();
    std::fs::create_dir_all(&log_path)
        .with_context(|| format!("Log path {}", log_path.display()))?;
    log_path.push(format!("pi-{}.log", task_id.unwrap_or(0)));
    tracing::info!("Log file dir: {}", &log_path.display());

    let log_keep_days = get_log_keep_days();

    let mut log_rotation = log_rotation(&log_path, log_keep_days);

    let mut child_command;

    match from.driver.as_str() {
        "pi" | "pibackfill" => {
            let mut command = tokio::process::Command::new(pi_exe_path()?);
            child_command = command
                .arg("-f")
                .arg(&config_path)
                .kill_on_drop(true)
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .context("Start PI collector error")?;
            send_sub_process_info(child_command.id(), task_id, "pi");
        }
        _ => {
            anyhow::bail!("wrong driver configured");
        }
    }
    let stderr = child_command
        .stderr
        .take()
        .expect("Failed to capture stderr");
    let log_task_id = task_id.unwrap_or_default();
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
            write!(log_rotation, "[task:{}]{}", log_task_id, line).unwrap();
            line.clear();
        }
        Ok::<(), std::io::Error>(())
    });
    let pid = child_command.id().unwrap();
    tracing::info!("Waiting for PI connector");
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
                });
            };
            (wait) => {
                tokio::spawn(async move {
                    let mut exit = None;
                    if let Ok(Some(status)) = child_command
                        .terminate_timeout(Duration::from_secs(2))
                        .await
                    {
                        tracing::info!("PI connector exit with {}", status);
                        notify.send_async(TaskNotify::Info(format!(
                            "PI connector exit with {}",
                            status
                        )));
                        exit.replace(status);
                    }
                    tokio::spawn(async move {
                        tracing::info!("Wait for IPC handlers finished");
                        let _ = ipc.close().await;
                        tracing::info!("All IPC handlers have been finished");
                    });
                    let _ = temp_path.close();
                    exit
                })
            };
        }
        tokio::select! {
            status = child_command.wait() => {
                let status = status?;
                tracing::info!("PI connector exit with {}", status);
                if !status.success() {
                    safe_exit!();
                    anyhow::bail!("PI connector exit with {}", status);
                }
            },
            err = ipc.recv_error() => {
                if let Some(err) = err {
                    tracing::warn!("PI writer error occurred: {err}");
                    if let Ok(Some(status)) = safe_exit!(wait).await {
                        if status.success() {
                            return Ok(());
                        }
                    }
                    anyhow::bail!("PI writer error: {err}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("{} task cancelled", from.driver);
                safe_exit!(wait);
            }
        }
        tracing::info!("Exit {} task", from.driver);
        Ok(())
    })
    .await??;
    Ok(())
}

#[instrument(skip_all)]
#[allow(unused_variables, unreachable_code)]
pub async fn query_data_source(from_dsn: Dsn, args: Vec<String>) -> anyhow::Result<String> {
    #[cfg(not(target_os = "windows"))]
    {
        anyhow::bail!("PI connector support only windows platform");
    }
    tracing::info!("Start query datasource using PI connector");
    let config = PiConfig::parse_connection(&from_dsn, String::new(), 0, 0)?;

    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let _temp_path = config_file.into_temp_path();

    tracing::info!("Using config file {} \n{}", config_path.display(), toml);

    let mut command = tokio::process::Command::new(pi_exe_path()?);
    let mut log_path = log_path();
    std::fs::create_dir_all(&log_path)
        .with_context(|| format!("Log path {}", log_path.display()))?;
    log_path.push("pi.log");
    tracing::info!("log file: {}", &log_path.display());

    let mut log_rotation = log_rotation(&log_path, 700);
    let cmd: &mut tokio::process::Command = command
        .arg("-f")
        .arg(&config_path)
        .args(args)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    tracing::info!("{:?}", cmd);
    let output = cmd.output().await?;
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
    let data = format!("{}", &json);
    tracing::info!("Query pi data source done, got data len {}", data.len());
    Ok(data)
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
    tracing::debug!("validate_pi config file: {}", &toml);
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
            namespaces: None,
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
    tracing::debug!("validate_pi_backfill config file: {}", &toml);
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
            namespaces: None,
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

pub const AF_SERVER_CONFIG: &str = "PI Data Archive and Asset Framework (AF) Server";
pub const SINGLE_COLUMN_MODEL: &str = "single-column";
pub const MULTI_COLUMN_MODEL: &str = "multi-column";

pub fn parse_query_datasource_params(dsn: &Dsn) -> (&str, &str, &str) {
    let model = dsn
        .params
        .get("model")
        .map(|s| s.as_str())
        .unwrap_or(SINGLE_COLUMN_MODEL);
    let is_af =
        dsn.params.get("system_configuration").map(|s| s.as_str()) == Some(AF_SERVER_CONFIG);
    let mode = match (model, is_af) {
        (SINGLE_COLUMN_MODEL, false) => "-pp", // PI Archive 模式
        (SINGLE_COLUMN_MODEL, true) => "-px",  // AF 单列模式
        (MULTI_COLUMN_MODEL, true) => "-pt",   // 多列模式
        _ => unreachable!("unsupported model: {}, is_af: {}", model, is_af),
    };

    let filter_point = dsn.params.get("filter_point").map(|s| s.as_str());
    let filter_element = dsn.params.get("filter_element").map(|s| s.as_str());
    let filter_template = dsn.params.get("filter_template").map(|s| s.as_str());
    let (pattern, pattern_type) = if mode.eq("-pp") {
        match filter_point {
            Some(pattern) => (pattern, ""),
            None => ("*", ""),
        }
    } else {
        if let Some(pattern) = filter_element {
            (pattern, "Element")
        } else if let Some(pattern) = filter_template {
            (pattern, "Template")
        } else {
            ("*", "Template")
        }
    };
    (mode, pattern, pattern_type)
}

/// 启动连接器前的参数检查
fn pre_check_config(config: &PiConfig) -> anyhow::Result<()> {
    if config.for_backfill {
        if config.backfill_start_time.is_none() {
            anyhow::bail!("PI backfill start time is required");
        }
        if config.backfill_end_time.is_none() {
            anyhow::bail!("PI backfill end time is required");
        }
        let start_time = config.backfill_start_time.unwrap();
        let end_time = config.backfill_end_time.unwrap();
        let start_time_str = start_time.to_string();
        let end_time_str = end_time.to_string();
        let start_time: chrono::DateTime<chrono::Utc> = start_time_str
            .parse()
            .context(format!("Failed to parse start_time_str {}", start_time_str))?;
        let end_time: chrono::DateTime<chrono::Utc> = end_time_str
            .parse()
            .context(format!("Failed to parse end_time_str {}", end_time_str))?;
        if start_time >= end_time {
            anyhow::bail!(
                "PI backfill start time must be less than end time. start: {}, end: {}",
                start_time,
                end_time
            );
        }
        let now = chrono::Utc::now();
        if end_time > now {
            anyhow::bail!(
                "PI backfill end time must be less than current time. end: {}, now: {}",
                end_time,
                now
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

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
