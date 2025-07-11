use std::{io::prelude::*, path::PathBuf};

use crate::dsv::DataSourceValidation;
use crate::runners::log_rotation;
use crate::runners::pi::config::PiConfig;
use anyhow::Context;
use serde_json::Value;
use taos::Dsn;
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
            format!("invalid pi dsn: {}, cause: {}", dsn, err),
        ),
        Ok(c) => {
            let valid = validate_pi(c).await;
            match valid {
                Err(err) => DataSourceValidation::invalid(
                    "pi".to_string(),
                    format!("failed to connect to dsn: {}, cause: {}", dsn, err),
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
            format!("invalid pibackfill dsn: {}, cause: {}", dsn, err),
        ),
        Ok(c) => {
            let valid = validate_pi_backfill(c).await;
            match valid {
                Err(err) => DataSourceValidation::invalid(
                    "pibackfill".to_string(),
                    format!("failed to connect to dsn: {}, cause: {}", dsn, err),
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
    } else if let Some(pattern) = filter_element {
        (pattern, "Element")
    } else if let Some(pattern) = filter_template {
        (pattern, "Template")
    } else {
        ("*", "Template")
    };
    (mode, pattern, pattern_type)
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
        assert!(!validation.valid);
        assert!(!validation.support);
        assert_eq!("pi", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!(
            "invalid pi dsn: pi://, cause: PIServerName is required",
            validation.message.unwrap()
        );

        let dsn = Dsn::from_str("pi://WIN-2OA23UM12TN/Met1?PISystemName=other").unwrap();
        let validation = is_pi_valid(&dsn).await;
        assert!(!validation.valid);
        assert!(!validation.support);
        assert_eq!("pi", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!("failed to connect to dsn: pi://WIN-2OA23UM12TN/Met1?PISystemName=other, cause: pi plugin not found at: \"/usr/local/taos/plugins/pi/taosx-pi.exe\"", validation.message.unwrap());
    }

    #[ignore]
    #[tokio::test]
    async fn test_is_pi_backfill_valid() {
        let dsn = Dsn::from_str("pibackfill://").unwrap();
        let validation = is_pi_backfill_valid(&dsn).await;
        assert!(!validation.valid);
        assert!(!validation.support);
        assert_eq!("pibackfill", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!(
            "invalid pibackfill dsn: pibackfill://, cause: PIServerName is required",
            validation.message.unwrap()
        );

        let dsn = Dsn::from_str("pibackfill://WIN-2OA23UM12TN/Met1?PISystemName=other").unwrap();
        let validation = is_pi_backfill_valid(&dsn).await;
        assert!(!validation.valid);
        assert!(!validation.support);
        assert_eq!("pibackfill", validation.data_source);
        assert_eq!(None, validation.version);
        assert_eq!("failed to connect to dsn: pibackfill://WIN-2OA23UM12TN/Met1?PISystemName=other, cause: pibackfill plugin not found at: \"/usr/local/taos/plugins/pi/taosx-pi-backfill.exe\"", validation.message.unwrap());
    }
}
