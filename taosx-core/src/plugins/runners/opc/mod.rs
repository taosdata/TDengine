use std::{
    fs,
    io::prelude::*,
    num::ParseIntError,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use anyhow::{bail, Context};
use base64::Engine;
use csv_lib::ReaderBuilder;
use file_rotate::{
    compression::Compression,
    ContentLimit,
    FileRotate, suffix::{AppendTimestamp, DateFrom, FileLimit}, TimeFrequency,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder, Ty};
use tokio::{io::AsyncBufReadExt, sync::Mutex};
pub use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Span};

use taosx_ipc::types::OptionSet;

use crate::{
    Action, build_ipc, DataSet, DataSetsReq, get_log_keep_days, Transferred,
    utils::port_pool::PortPool,
};
use crate::dsv::DataSourceValidation;
use crate::runners::log_rotation;
use crate::runners::opc::config::{ColumnConfig, OPCConfig, TableConfig};

mod config;

#[derive(Debug, thiserror::Error)]
enum OpcError {
    #[error("One of `ua` `da` protocol should be set")]
    ProtocolNotFound(Dsn),
    #[error("Endpoint is required in OPC dsn: {0} like `opc+..://localhost:4840?...`")]
    EndpointIsRequired(Dsn),
    #[error("Database name is required in OPC dsn: {0}")]
    DatabaseIsRequired(Dsn),
    #[error("Username and password are both required for UserName authentication method in {0}")]
    UserPassRequired(Dsn),
    // #[error("config file not found: {0}")]
    // FileNotFound(String),
    #[error("file parse error: {0}")]
    FileParseFound(String),
    // #[error("config file content is empty in {0}")]
    // EmptyConfig(String),
    #[error("node config error {0}")]
    NodeConfig(String),
    #[error("Parse integer error from {1} while parsing parameter {0}: {2:?}")]
    ParseNumberError(&'static str, String, ParseIntError),
    #[error("Parse param error from {1} while parsing parameter {0}")]
    ParseError(&'static str, String),
    #[error("{0} config error: {1}")]
    ConfigError(&'static str, String),
}

pub fn parse_bool_param_from_dsn(dsn: &mut Dsn, key: &str) -> anyhow::Result<Option<bool>> {
    if let Some(key) = dsn.remove(key) {
        match key.as_str() {
            "false" => Ok(Some(false)),
            "" | "true" => Ok(Some(true)),
            _ => anyhow::bail!("should config true or false"),
        }
    } else {
        Ok(None)
    }
}


#[instrument(skip(dsn))]
async fn handle_select_all_points(dsn: &mut Dsn) -> anyhow::Result<()> {
    let child_table_expression = dsn.remove("child_table_expression");
    if child_table_expression.is_none() {
        anyhow::bail!("should config child_table_expression");
    }
    let child_table_expression = child_table_expression.unwrap();
    let table_primary_key = dsn.remove("table_primary_key");
    if table_primary_key.is_none() {
        anyhow::bail!("should config table_primary_key");
    }
    let table_primary_key = table_primary_key.unwrap();
    let data = DataSetsReq {
        from: dsn.to_string(),
        categories: vec![String::from("nodes")],
        via: None,
        offset: 0,
        pattern: Some(String::from(".*")),
        limit: usize::MAX / 2 - 1,
        lang: None,
    };
    let all_points = opc_datasets(&data).await?;
    let point_config = all_points
        .iter()
        .map(|point| {
            let point_id = point.id.clone();
            let tbname =
                generate_tbname_from_pattern(&dsn.driver, &child_table_expression, &point_id);
            // 对于 OPCUA 来说，ns=3;s=Special_\"!§$%&/()=?`´\\+~*'#_-:.;,<>|@^°€µ{[]} 是一个有效的点位 ID 和名称
            // 此时需要借助 CSV 的 delimiter 使用 , 进行分隔
            // 前提是点位需要使用双引号引起来
            // 又引出的问题的是如果点位名称已经包含了双引号该如何处理 -》继续加双引号
            format!("\"{}::{}\"", point_id.replace("\"", "\"\""), tbname)
        })
        .join(",");
    if dsn.driver.as_str() == "opcua" {
        dsn.set("ua.nodes", point_config);
    } else {
        dsn.set("da.tags", point_config);
    }
    let stable_prefix = Some(String::from("opc"));
    let mut column_configs = vec![];

    column_configs.push(ColumnConfig {
        column_name: String::from("value"),
        column_type: None,
        column_alias: Some(String::from("val")),
        is_primary_key: false,
    });
    column_configs.push(ColumnConfig {
        column_name: String::from("quality"),
        column_type: Some(Ty::Int),
        column_alias: None,
        is_primary_key: false,
    });
    let opc_table_config = if table_primary_key == "received_ts" {
        column_configs.push(ColumnConfig {
            column_name: String::from("received_ts"),
            column_type: Some(Ty::Timestamp),
            column_alias: None,
            is_primary_key: true,
        });
        column_configs.push(ColumnConfig {
            column_name: String::from("original_ts"),
            column_type: Some(Ty::Timestamp),
            column_alias: None,
            is_primary_key: false,
        });
        TableConfig {
            stable_prefix,
            column_configs,
            tag_configs: None,
        }
    } else {
        column_configs.push(ColumnConfig {
            column_name: String::from("original_ts"),
            column_type: Some(Ty::Timestamp),
            column_alias: None,
            is_primary_key: true,
        });
        TableConfig {
            stable_prefix,
            column_configs,
            tag_configs: None,
        }
    };
    dsn.set(
        "opc_table_config",
        serde_json::to_string(&opc_table_config)?,
    );
    Ok(())
}

/// TODO should support more complicated pattern
/// a expression like d00{point_id}_{tag1}_{tag2}
/// for now only support <table_prfix>_{ns}_{id}_<table_suffix> for opcua
/// <table_prfix>_{TagName}_<table_suffix> for opcda
fn generate_tbname_from_pattern(ty: &str, tb_name: &str, point_id: &str) -> String {
    let tbname = if ty == "opcua" {
        // ns=13;i=1003
        let mut split = point_id.split(";");
        let ns = if let Some(ns) = split.next() {
            if ns.contains("ns=") {
                let mut ns_split = ns.split("=");
                ns_split.next();
                ns_split.next()
            } else {
                None
            }
        } else {
            None
        };
        let id = if let Some(id) = split.next() {
            if id.contains("i=") {
                let mut id_split = id.split("=");
                id_split.next();
                id_split.next()
            } else {
                None
            }
        } else {
            None
        };
        tb_name
            .replace("{ns}", ns.unwrap_or(""))
            .replace("{id}", id.unwrap_or(""))
    } else {
        let tag_index = point_id.rfind(".");
        let tag_name = if let Some(index) = tag_index {
            // should be Device.DeviceType.TagName pattern
            &point_id[index + 1..]
        } else {
            &point_id
        };
        tb_name.replace("{TagName}", tag_name)
    };
    tbname
}

pub(super) fn get_string_vec_from_param_or_file_for_opc(
    dsn: &mut Dsn,
    key: &str,
) -> Result<Vec<String>, String> {
    if let Some(nodes) = dsn.remove(key) {
        let mut rdr = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(nodes.as_bytes());
        let header = rdr.headers().map_err(|err| err.to_string())?;
        let (files, mut node_config): (Vec<_>, Vec<_>) = header
            .into_iter()
            // .split(",")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .partition(|v| v.starts_with("@"));
        // dbg!(&files, &node_config);
        for file in files {
            tracing::info!(
                "current log: {}",
                std::env::current_dir().unwrap().to_str().unwrap()
            );
            let f = std::fs::File::open(&file[1..]);
            if f.is_err() {
                tracing::warn!(
                    "file: {} read error, cause: {}",
                    &file[1..],
                    f.err().unwrap()
                );
                continue;
                // return Err("file read error".to_string());
            }
            let buf = std::io::BufReader::new(f.unwrap());
            let mut file_data = buf.lines().collect_vec();
            // remove header
            if file_data.remove(0).is_err() {
                tracing::warn!("file: {} content length < 1", file);
            }

            node_config.extend(
                file_data
                    .iter()
                    .filter_map(|r| r.as_ref().ok())
                    .map(|s| s.replace(",", "::")),
            );
        }
        if node_config.len() == 0 {
            tracing::warn!("node config is empty");
            // return Err(format!("node config set but is empty: {nodes}"));
        }
        return Result::Ok(node_config);
    }
    // tracing::warn!("node config is empty");
    return Err("Nodes not set".to_string());
}

const EXE: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "taosx-opc.exe"
        } else {
            "taosx-opc"
        }
    }
};

fn exe_path() -> anyhow::Result<PathBuf> {
    let path = super::get_plugin_dir("opc").join(EXE);
    if !path.exists() {
        return Err(anyhow::anyhow!("opc plugin not found at: {:?}", path));
    }
    Ok(path)
}

const LOG_FILE: &str = "opc.log";

fn log_path() -> PathBuf {
    super::get_log_dir("opc")
}

pub fn info() -> anyhow::Result<(&'static str, PathBuf, String)> {
    let path = exe_path()?;
    let output = std::process::Command::new(&path).arg("version").output()?;
    Ok((
        "opc",
        path,
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
    ))
}

#[instrument(skip_all, fields(task.id = with_agent.as_ref().map(| v | v.0)))]
pub async fn opc_to_taos(
    mut from: Dsn,
    _actions: Vec<Action>,
    to: Dsn,
    _jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    span: Span,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    if to.subject.is_none() {
        Err(OpcError::DatabaseIsRequired(to.clone()))?;
    }
    let ipc_port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for OPC connection"))?;
    let builder: TaosBuilder = TaosBuilder::from_dsn(&to)?;
    let taos = builder.build().await?;

    let select_all_points = parse_bool_param_from_dsn(&mut from, "select_all_points")
        .map_err(|err| OpcError::ConfigError("select_all_points", err.to_string()))?
        .unwrap_or(false);
    if select_all_points {
        handle_select_all_points(&mut from).await?;
    }
    let config = OPCConfig::new(from, ipc_port, OPCConfigMode::Collect, Some(&taos)).await?;
    if config.opc_table_config.is_none() {
        anyhow::bail!("should config opc table config");
    }

    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    tracing::info!("Using opc config file {}", config_path.display());

    let table_config = Some(config.parse_tables_with(&taos).await?);
    let connector = match config.opc_type {
        OpcType::FAKE => None,
        OpcType::OPCDA => Some("opc_da"),
        OpcType::OPCUA => Some("opc_ua"),
    };

    let mut ipc_handler = build_ipc(
        &config.report.remote,
        None,
        &to,
        connector,
        table_config,
        &cancel,
        with_agent,
        transferred,
        span,
        None,
        notify,
    )
        .await?;

    let port_pool = port_pool.clone();
    let mut command = tokio::process::Command::new(exe_path()?);

    let mut log_path = log_path();
    fs::create_dir_all(&log_path)?;

    tracing::info!("log path created: {}", &log_path.display());

    log_path.push(LOG_FILE);

    tracing::info!("log file dir: {}", &log_path.display());

    let log_keep_days = get_log_keep_days();

    let mut log_rotation = log_rotation(&log_path, log_keep_days);

    let child = command
        .arg("collect")
        .arg(format!("--conf={}", &config_path.display()))
        .kill_on_drop(true)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::piped());

    let mut child = child.spawn()?;
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

            if line.contains("panic") {
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

    tokio::spawn(async move {
        macro_rules! safe_exit {
            () => {
                let _ = child.kill().await;
                tokio::spawn(async move {
                    tracing::info!("Wait for IPC handlers finished");
                    let _ = ipc_handler.close().await;
                    tracing::info!("All IPC handlers have been finished");
                });
                let _ = temp_path.close();
                tracing::info!("Release IPC port");
                port_pool.put(ipc_port).await;
            };
        }
        tokio::select! {
            status = child.wait() => {
                let status = status?;
                tracing::info!("OPC exit with {}", status);
                if !status.success() {
                    safe_exit!();
                    use ringbuf::Rb;
                    let error = error_buf.lock().await.iter().join("");
                    anyhow::bail!("OPC exit with {}\n{error}", status);
                } else {
                    safe_exit!();
                    anyhow::bail!("OPC process was killed by signal");
                }
            },
            err = ipc_handler.recv_error() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                if let Some(err) = err {
                    safe_exit!();
                    anyhow::bail!("OPC writer error: {err}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("opc task cancelled");
            },
        }
        tracing::info!("OPC to taos task done");
        safe_exit!();
        Ok(())
    }).await??;

    Ok(())
}

pub async fn opc_datasets(req: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    let from: Dsn = req.from.parse().unwrap();
    if req.categories.is_empty() {
        return Err(anyhow::anyhow!("categories is empty"));
    }

    let mut config = OPCConfig::new(from.clone(), 0, OPCConfigMode::Points, None).await?;
    let points_config = PointsConfig {
        limit: req.limit,
        regex: req.pattern.clone(),
    };
    config.points = Some(points_config);
    let toml =
        toml::to_string(&config).with_context(|| format!("toml to_string error encountered"))?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    tracing::info!("Using opc config file {} \n{}", config_path.display(), toml);

    let mut command = tokio::process::Command::new(exe_path()?);
    let output = command
        .arg("points")
        .arg(format!("--conf={}", &config_path.display()))
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .with_context(|| "Start OPC collector error")?;
    // dbg!(output);
    let mut log_path = log_path();
    log_path.push(LOG_FILE);

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

    write!(log_rotation, "{}", String::from_utf8_lossy(&output.stderr))
        .context("writing logs error")?;

    tracing::info!("OPC exit with status {}", output.status);
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        tracing::error!(
            plugin = "opc",
            module = "datasets",
            stdout = ?bytes::Bytes::from(output.stdout),
            "Get OPC datasets error:\n{}",
            error
        );
        let pattern =
            regex::Regex::new(r#"level=PANIC msg="(?P<msg>.*)" error="(?<error>.*)"#).unwrap();
        let matches = pattern.captures(&error);
        if let Some(matches) = matches {
            bail!("{}: {}", &matches["msg"], &matches["error"]);
        } else {
            bail!("Get OPC datasets error: {}", &error);
        }
    }

    temp_path.close()?;
    // let json = String::from_utf8_lossy(&output.stdout);
    let res: Vec<DataSet> = serde_json::from_slice(&output.stdout)?;
    tracing::debug!(
        "opc datasets : {}",
        serde_json::to_string(&res).unwrap_or("".to_string())
    );
    let (option_set_code_display, option_set_code_desc) = if let Some(lang) = req.lang.clone() {
        match lang.as_str() {
            "zh" => ("编码".to_string(), "点位编码".to_string()),
            _ => ("Code".to_string(), "Point Code".to_string()),
        }
    } else {
        ("Code".to_string(), "Point Code".to_string())
    };
    let options = vec![OptionSet {
        name: "code".to_string(),
        display: option_set_code_display,
        description: Some(option_set_code_desc),
        required: true,
    }];
    let format = Some("{id}::{code}".to_string());
    if let Some(pattern) = req.pattern.as_deref() {
        let regex = regex::Regex::from_str(pattern)?;
        // regex.is_match(text)
        let res = res
            .into_iter()
            .filter(|set| {
                regex.is_match(&set.id)
                    || set
                    .name
                    .as_deref()
                    .map(|s| regex.is_match(s))
                    .unwrap_or(false)
            })
            .map(|mut set| {
                set.category = Some(req.categories[0].clone());
                set.options = Some(options.clone());
                set.format = format.clone();
                set
            })
            .skip(req.offset)
            .take(req.limit)
            .collect_vec();
        Ok(res)
    } else {
        Ok(res
            .into_iter()
            .map(|mut set| {
                set.category = Some(req.categories[0].clone());
                set.options = Some(options.clone());
                set.format = format.clone();
                set
            })
            .skip(req.offset)
            .take(req.limit)
            .collect())
    }
}

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    #[cfg(not(windows))]
    if dsn.driver == "opcda" {
        return DataSourceValidation::invalid(
            "opc".to_string(),
            "opcda only support windows".to_string(),
        );
    }
    let config = OPCConfig::new(dsn.clone(), 0, OPCConfigMode::Points, None).await;
    match config {
        Err(err) => DataSourceValidation::invalid(
            "opc".to_string(),
            format!(
                "invalid opc dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            let valid = validate_opc(c).await;
            match valid {
                Err(err) => DataSourceValidation::invalid(
                    "opc".to_string(),
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

async fn validate_opc(config: OPCConfig) -> anyhow::Result<DataSourceValidation> {
    let toml = toml::to_string(&config)?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;

    // startup the connector
    let opc_exe_path = exe_path()?;
    let mut command = tokio::process::Command::new(opc_exe_path.clone());
    let output = command
        .arg("check")
        .arg("--conf")
        .arg(config_file.path())
        .stdout(std::process::Stdio::inherit())
        // .stderr(std::process::Stdio::piped())
        .output()
        .await
        .with_context(|| format!("failed to execute opc: {:?}", opc_exe_path.as_path()))?;

    if output.status.success() {
        let result: serde_json::Value =
            serde_json::from_slice(&output.stdout).with_context(|| {
                format!(
                    "Deserialize opc validation result error: {}",
                    String::from_utf8_lossy(&output.stdout)
                )
            })?;
        Ok(DataSourceValidation {
            valid: result["valid"].as_bool().unwrap_or(false),
            support: result["support"].as_bool().unwrap_or(false),
            data_source: "opc".to_string(),
            version: result["version"].as_str().map(|s| s.to_string()),
            message: result["message"].as_str().map(|s| s.to_string()),
        })
    } else {
        Ok(DataSourceValidation::invalid(
            "opc".to_string(),
            format!(
                "failed to execute opc: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_opc_ua_valid() {
        env::set_var("PLUGINS_HOME", "../plugins");

        let dsn = Dsn::from_str("opcua://192.168.2.16:53530/OPCUA/SimulationServer").unwrap();
        let dsv = is_valid(&dsn).await;
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!("opc", dsv.data_source);
    }

    #[ignore]
    #[tokio::test]
    async fn test_opc_da_valid() {
        env::set_var("PLUGINS_HOME", "../plugins");

        let dsn = Dsn::from_str("opcda://192.168.2.16").unwrap();
        let dsv = is_valid(&dsn).await;
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!("opc", dsv.data_source);
        assert_eq!("2.4.0", dsv.version.unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_with_agent() -> anyhow::Result<()> {
        env::set_var("RUST_LOG", "debug");
        pretty_env_logger::init();

        let opc = "opc+ua://192.168.0.133:53530/OPCUA/SimulationServer?\
    ua.nodes=ns=10;i=1004::t1::c1::double&connect_timeout=5&request_timeout=5&\
    concurrent=1&batch_size=5&batch_timeout=5&debug=true";
        let target = "taos:///opcua";
        let span = tracing::info_span!("task::spawned", trace_id = tracing::field::Empty);
        opc_to_taos(
            opc.parse().unwrap(),
            vec![],
            target.parse().unwrap(),
            1,
            &PortPool::default(),
            CancellationToken::new(),
            Some((2, "http://127.0.0.1:6051".into(), "".into())),
            None,
            span.clone(),
        )
            .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_with_agent_all_nodes() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "debug");
        // tracing_subscriber::fmt::init();
        let opc = "opcua://192.168.0.34:53530/OPCUA/SimulationServer?connect_timeout=1&request_timeout=1&interval=10&collect_mode=observe&enable=false&keep=10&concurrent=1&batch_size=1&batch_timeout=1&debug=false&select_all_points=true&table_primary_key=original_ts&child_table_expression=meter_{ns}_{id}&&select_all_points=true";
        let target = "taos:///opc";
        let span = tracing::info_span!("task::spawned", trace_id = tracing::field::Empty);
        opc_to_taos(
            opc.parse().unwrap(),
            vec![],
            target.parse().unwrap(),
            1,
            &PortPool::default(),
            CancellationToken::new(),
            Some((2, "http://127.0.0.1:6051".into(), "".into())),
            None,
            span.clone(),
        )
            .await?;
        Ok(())
    }
}
