use super::get_data_dir;
use crate::dsv::DataSourceValidation;
use crate::runners::opc::config::{OPCConfig, PointsMode};
use crate::runners::opc::model::{ModelType, OpcModelConfig};
use crate::runners::opc::point_updater::PointsUpdater;
use crate::runners::{get_logs_home_dir, log_rotation, new_rolling_file_appender};
use crate::sink::persist::PersistConfig;
use crate::utils::dsn::json_to_dsn;
use crate::utils::monitor::send_sub_process_info;
use crate::{build_ipc, utils::port_pool::PortPool, Action, DataSet, DataSetsReq, Transferred};
use anyhow::{bail, Context};
use csv::header::CsvHeader;
use csv::CsvParser;
use csv_async::AsyncReader;
use futures_util::StreamExt;
use itertools::Itertools;
use schema::get_schema_path;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::fs::File;
use std::{io::prelude::*, path::PathBuf, sync::Arc};
use taos::{Dsn, IntoDsn};
use taosx_ipc::prelude::IpcDataType;
use taosx_ipc::types::OptionSet;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::{io::AsyncBufReadExt, sync::Mutex};
use tokio_process_terminate::TerminateExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tracing_subscriber::fmt::MakeWriter;

pub mod config;
pub mod csv;
pub mod model;
mod point_updater;
mod schema;

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum OpcType {
    OPCUA,
    OPCDA,
    FAKE,
}

impl OpcType {
    /// valid dsn driver:
    /// opcua:// -> OPCUA
    /// opcda:// -> OPCDA
    /// fake:// -> FAKE
    /// opc+ua:// -> OPCUA
    /// opc+da:// -> OPCDA
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let fake = dsn.params.contains_key("fake");
        if fake {
            return Ok(Self::FAKE);
        }
        let opc_type = dsn.driver.to_lowercase();
        let protocol = dsn.protocol.clone();
        match opc_type.as_str() {
            "opcua" => Ok(Self::OPCUA),
            "opcda" => Ok(Self::OPCDA),
            "fake" => Ok(Self::FAKE),
            "opc" => match protocol.as_deref() {
                Some("ua") => Ok(Self::OPCUA),
                Some("da") => Ok(Self::OPCDA),
                _ => bail!("unknown opc protocol"),
            },
            _ => bail!("invalid opc type"),
        }
    }

    pub fn as_static_str(&self) -> &'static str {
        match self {
            OpcType::OPCUA => "opcua",
            OpcType::OPCDA => "opcda",
            OpcType::FAKE => "fake",
        }
    }
}

impl TryFrom<&str> for OpcType {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "opcua" => Ok(Self::OPCUA),
            "opcda" => Ok(Self::OPCDA),
            "fake" => Ok(Self::FAKE),
            _ => bail!("invalid opc type: {}", value),
        }
    }
}

impl Display for OpcType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::OPCUA => "opcua",
            Self::OPCDA => "opcda",
            Self::FAKE => "fake",
        };
        write!(f, "{}", s)
    }
}

const EXE: &str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "taosx-opc.exe"
        } else {
            "taosx-opc"
        }
    }
};

/// taosx-opc executable path
fn exe_path() -> anyhow::Result<PathBuf> {
    let path = super::get_plugin_dir("opc").join(EXE);
    if !path.exists() {
        return Err(anyhow::anyhow!("opc plugin not found at: {:?}", path));
    }
    Ok(path)
}

/// taosx-opc version
pub fn info() -> anyhow::Result<(&'static str, PathBuf, String)> {
    let path = exe_path()?;
    let output = std::process::Command::new(&path).arg("version").output()?;
    Ok((
        "opc",
        path,
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
    ))
}

/// OPC dataIn task
#[instrument(skip_all, fields(task.id = with_agent.as_ref().map(| v | v.0)))]

pub async fn opc_to_taos(
    from: Dsn,
    _actions: Vec<Action>,
    to: Dsn,
    _jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    if to.subject.is_none() {
        anyhow::bail!(
            "Database name is required in OPC dsn: {}",
            to.clone().to_string()
        );
    }
    if with_agent.is_some() {
        let task_id = task_id.context("Task id not found for agent runner")?;
        let _ = crate::core_metrics::init_task_metrics(&from, &to, task_id, None).await;
    }
    let ipc_port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for OPC connection"))?;

    tracing::info!("OPC task start, from: {}, to: {}", from, to);

    let certificate = get_temp_file(&from, "certificate");
    let private_key = get_temp_file(&from, "private_key");
    let auth_certificate = get_temp_file(&from, "auth_certificate");
    let auth_private_key = get_temp_file(&from, "auth_private_key");

    let mut config = OPCConfig::from_dsn_collect_mode(&from, ipc_port.get(), task_id).await?;

    config.set_temp_filepath("certificate", certificate.as_ref())?;
    config.set_temp_filepath("private_key", private_key.as_ref())?;
    config.set_temp_filepath("auth_certificate", auth_certificate.as_ref())?;
    config.set_temp_filepath("auth_private_key", auth_private_key.as_ref())?;

    let persist_config = task_id
        .or(with_agent.as_ref().map(|a| a.0))
        .and_then(|tid| {
            config.collect.as_ref().and_then(|c| {
                c.persist_data.as_ref().map(|c| PersistConfig {
                    task_id: tid,
                    record_metrics: true,
                    schemas: get_schema_path(c.dir.clone().unwrap_or_else(|| {
                        get_data_dir()
                            .join("tasks")
                            .join(tid.to_string())
                            .join("persist_queue")
                    })),
                    batch_size: config.report.batch_size.map(|v| v as _),
                    batch_timeout: config
                        .report
                        .batch_timeout
                        .map(|v| std::time::Duration::from_secs(v as u64)),
                    batch_chunk_size: None,
                })
            })
        });

    // let tid = task_id
    //     .or(with_agent.as_ref().map(|a| a.0))
    //     .context("task id not found")?;
    // let persist_config = config.collect.as_ref().and_then(|c| {
    //     c.persist_data.as_ref().map(|c| PersistConfig {
    //         task_id: tid,
    //         record_metrics: true,
    //         schemas: get_schema_path(c.dir.clone().unwrap_or_else(|| {
    //             get_data_dir()
    //                 .join("tasks")
    //                 .join(tid.to_string())
    //                 .join("persist_queue")
    //         })),
    //         batch_size: config.report.batch_size.map(|v| v as _),
    //         batch_timeout: config
    //             .report
    //             .batch_timeout
    //             .map(|v| std::time::Duration::from_secs(v as u64)),
    //         batch_chunk_size: None,
    //     })
    // });

    // create IPC handler
    let connector = match config.opc_type {
        OpcType::OPCUA => Some("opc_ua"),
        OpcType::OPCDA => Some("opc_da"),
        OpcType::FAKE => None,
    };
    let (mut ipc_handler, _) = build_ipc(
        Some(&config.report.remote),
        None,
        &to,
        connector,
        config.get_model_config().cloned(),
        None,
        &cancel,
        with_agent,
        transferred,
        task_id,
        notify,
        persist_config,
    )
    .await?;

    // OPCConfig -> collect.toml
    let config_dir = get_data_dir()
        .join("tasks")
        .join(format!("{}", task_id.unwrap_or(-1)));
    std::fs::create_dir_all(&config_dir).map_err(|err| {
        anyhow::anyhow!(
            "failed to create config dir: {}, cause: {}",
            config_dir.display(),
            err.to_string()
        )
    })?;

    let config_file_path = config_dir.join("collect.toml");
    let mut config_file = File::create(&config_file_path)?;
    let toml = toml::to_string(&config)?;
    write!(config_file, "{}", &toml)?;
    config_file.sync_all()?;
    drop(config_file);

    // execute taosx-opc collect
    tracing::info!(
        "execute: taosx-opc collect, opc config: {}\n{}",
        config_file_path.display(),
        toml
    );
    let mut command = tokio::process::Command::new(exe_path()?);
    let child = command
        .arg("collect")
        .arg("--conf")
        .arg(&config_file_path)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::piped());

    let mut child = child.spawn()?;
    send_sub_process_info(child.id(), task_id, config.opc_type.to_string().as_str());

    // start points updating task
    let pu_cancel_token = CancellationToken::new();
    let token = pu_cancel_token.clone();
    let mut updater = PointsUpdater::try_new(
        from.clone(),
        config.clone(),
        config_file_path.display().to_string(),
        token,
    )?;
    tokio::spawn(async move {
        updater.run().await;
    });

    // create log file: opc.log
    let log_path = get_logs_home_dir();
    let log_file_name = format!("opc-{}", task_id.unwrap_or(0));
    let appender = new_rolling_file_appender(log_path.as_path(), &log_file_name)
        .context("failed to create opc log")?;

    const ERROR_BUF_SIZE: usize = 2;
    let error_buf = Arc::new(Mutex::new(ringbuf::HeapRb::<String>::new(ERROR_BUF_SIZE)));
    let error_buf_producer = error_buf.clone();
    let stderr = child.stderr.take().expect("Failed to capture stderr");

    // let log_rotation_clone = Arc::clone(&log_rotation);
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
            let mut log_rotation = appender.make_writer();
            let _ = log_rotation.write(line.as_bytes())?;
            log_rotation.flush()?;

            line.clear();
        }

        Ok::<(), std::io::Error>(())
    });

    // wait for child process exit
    tokio::spawn(async move {
        macro_rules! safe_exit {
            () => {
                use std::time::Duration;
                let _ = child.terminate_timeout(Duration::from_secs(2)).await;
                tokio::spawn(async move {
                    tracing::info!("Wait for IPC handlers finished");
                    let _ = ipc_handler.close().await;
                    tracing::info!("All IPC handlers have been finished");
                });
                // let _ = temp_path.close();
                certificate.map(|f| f.close());
                private_key.map(|f| f.close());
                auth_certificate.map(|f| f.close());
                auth_private_key.map(|f| f.close());

                tracing::info!("Release IPC port");

                // cancel points updater task
                pu_cancel_token.cancel();
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
                   anyhow:: bail!("OPC process was killed by signal");
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

/// OPC UA: <table_prefix>_{ns}_{id}_<table_suffix>
/// OPC DA: <table_prefix>_{tag_name/TagName}_<table_suffix>
fn generate_tbname_from_pattern(ty: &str, tb_name: &str, point_id: &str) -> String {
    let tbname = match ty {
        "opcua" => {
            // ns=13;i=1003
            // ns=6;s=Scalar_Instructions
            // ns=6;g=00000000-0000-0000-0000-000000009204
            // ns=6;b=CQIABQ==
            if let Some((ns, id)) = point_id.split_once(";") {
                let ns = if ns.contains("ns=") {
                    let (_, ns) = ns.split_once("=").unwrap();
                    ns
                } else {
                    ns
                };
                let id = if let Some((_, id)) = id.split_once('=') {
                    id
                } else {
                    id
                };
                assert!(!id.is_empty(), "id should not be empty: {}", point_id);
                tb_name.replace("{ns}", ns).replace("{id}", id)
            } else {
                assert!(!point_id.is_empty(), "id should not be empty: {}", point_id);
                tb_name.replace("{ns}", "0").replace("{id}", point_id)
            }
        }
        "opcda" => {
            if tb_name.contains("{TagName}") || tb_name.contains("{tag_name}") {
                let tag_index = point_id.rfind(".");
                let tag_name = if let Some(index) = tag_index {
                    // should be Device.DeviceType.TagName pattern
                    &point_id[index + 1..]
                } else {
                    point_id
                };
                let tb_name = tb_name.replace("{TagName}", tag_name);
                tb_name.replace("{tag_name}", tag_name)
            } else if tb_name.contains("{/tag_name}") {
                let tag_index = point_id.rfind("/");
                let tag_name = if let Some(index) = tag_index {
                    // should be Device/DeviceType/TagName pattern
                    &point_id[index + 1..]
                } else {
                    point_id
                };
                tb_name.replace("{/tag_name}", tag_name)
            } else if tb_name.contains("{id}") {
                tb_name.replace("{id}", point_id)
            } else if tb_name.contains("{_id}") {
                tb_name.replace("{_id}", &point_id.replace("/", "_"))
            } else {
                tb_name.to_string()
            }
        }
        _ => tb_name.to_string(),
    };

    tbname.replace(".", "_").replace("`", "_")
}

/// OPC UA: {ns} {id}
/// OPC DA: {tag_name/TagName}
fn generate_tag_value_from_pattern(ty: &str, tb_name: &str, point_id: &str) -> String {
    match ty {
        "opcua" => {
            // ns=13;i=1003
            // ns=6;s=Scalar_Instructions
            // ns=6;g=00000000-0000-0000-0000-000000009204
            // ns=6;b=CQIABQ==
            if let Some((ns, id)) = point_id.split_once(";") {
                let ns = if ns.contains("ns=") {
                    let (_, ns) = ns.split_once("=").unwrap();
                    ns
                } else {
                    ns
                };
                let id = if let Some((_, id)) = id.split_once('=') {
                    id
                } else {
                    id
                };
                assert!(!id.is_empty(), "id should not be empty: {}", point_id);
                tb_name.replace("{ns}", ns).replace("{id}", id)
            } else {
                assert!(!point_id.is_empty(), "id should not be empty: {}", point_id);
                tb_name.replace("{ns}", "0").replace("{id}", point_id)
            }
        }
        "opcda" => {
            if tb_name.contains("{TagName}") || tb_name.contains("{tag_name}") {
                let tag_index = point_id.rfind(".");
                let tag_name = if let Some(index) = tag_index {
                    // should be Device.DeviceType.TagName pattern
                    &point_id[index + 1..]
                } else {
                    point_id
                };
                let tb_name = tb_name.replace("{TagName}", tag_name);
                tb_name.replace("{tag_name}", tag_name)
            } else if tb_name.contains("{/tag_name}") {
                let tag_index = point_id.rfind("/");
                let tag_name = if let Some(index) = tag_index {
                    // should be Device/DeviceType/TagName pattern
                    &point_id[index + 1..]
                } else {
                    point_id
                };
                tb_name.replace("{/tag_name}", tag_name)
            } else if tb_name.contains("{id}") {
                tb_name.replace("{id}", point_id)
            } else if tb_name.contains("{_id}") {
                tb_name.replace("{_id}", &point_id.replace("/", "_"))
            } else {
                tb_name.to_string()
            }
        }
        _ => tb_name.to_string(),
    }
}
fn generate_stable_from_pattern(stable_expr: &str, value_type: &Option<IpcDataType>) -> String {
    let mut stable = stable_expr.to_string();
    if stable_expr.contains(".") {
        stable = stable.replace(".", "_");
    }

    if let Some(t) = value_type {
        stable = match t {
            IpcDataType::VarChar(_len) => stable.replace("{type}", "varchar"),
            IpcDataType::NChar(_len) => stable.replace("{type}", "nchar"),
            _ => stable.replace("{type}", &t.sql_repr().replace(" ", "_")),
        };
    }

    stable
}

/// 解析为文件路径: 如果以@开头，表示文件路径, 返回 None;
/// 否则，认为参数值是文件内容，写入临时文件后，返回 NamedTempFile。
fn get_temp_file(dsn: &Dsn, key: &str) -> Option<NamedTempFile> {
    dsn.get(key).and_then(|v| {
        if v.is_empty() || v.starts_with('@') {
            return None;
        }

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(v.as_bytes()).unwrap();
        Some(file)
    })
}

/// 获取 opc 点位
pub async fn opc_datasets(req: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    let req_clone = req.clone();
    let from = if let Some(from_json) = req_clone.from_json {
        json_to_dsn(&from_json)?
    } else if let Some(from) = req_clone.from {
        from.into_dsn()?
    } else {
        anyhow::bail!("from is required");
    };

    if req.categories.is_empty() {
        anyhow::bail!("categories is empty");
    }

    opc_datasets_impl(from).await
}

async fn opc_datasets_impl(from: Dsn) -> anyhow::Result<Vec<DataSet>> {
    let certificate = get_temp_file(&from, "certificate");
    let private_key = get_temp_file(&from, "private_key");
    let auth_certificate = get_temp_file(&from, "auth_certificate");
    let auth_private_key = get_temp_file(&from, "auth_private_key");

    let points_mode = PointsMode::from_dsn(&from)?;
    let opc_points = match points_mode {
        // 解析 csv 文件中的点位
        PointsMode::ByCsv => {
            let opc_type = OpcType::from_dsn(&from)?;
            let csv_files = OPCConfig::parse_csv_config_files(&from).ok_or(anyhow::anyhow!(
                "csv_config_file not found in dsn: {}",
                from.to_string()
            ))?;

            let parser = CsvParser::try_new(opc_type, csv_files)?;
            let model_config = parser.parse().await?;
            to_opc_dataset_vec(&model_config).await?
        }
        // 通过 taosx-opc points 命令获取点位
        PointsMode::ByCommand => {
            let mut config = OPCConfig::from_dsn_point_mode(&from)?;

            config.set_temp_filepath("certificate", certificate.as_ref())?;
            config.set_temp_filepath("private_key", private_key.as_ref())?;
            config.set_temp_filepath("auth_certificate", auth_certificate.as_ref())?;
            config.set_temp_filepath("auth_private_key", auth_private_key.as_ref())?;

            opc_datasets_by_command(&config).await?
        }
    };

    certificate.map(|f| f.close());
    private_key.map(|f| f.close());
    auth_certificate.map(|f| f.close());
    auth_private_key.map(|f| f.close());

    Ok(opc_points)
}

/// 从 model_config 中提取所有 OPC 点位
async fn to_opc_dataset_vec(model_config: &OpcModelConfig) -> anyhow::Result<Vec<DataSet>> {
    let mut datasets = vec![];
    for (point_id, point_config) in model_config.point_config_map.iter() {
        let point_type = point_config.value_type.as_ref().map(|v| v.to_string());
        let name = point_config.tag_values.as_ref().and_then(|tag_values| {
            tag_values.iter().find_map(|(tag_name, tag_value)| {
                if tag_name == "name" {
                    Some(tag_value.to_string())
                } else {
                    None
                }
            })
        });
        let table_config = model_config.table_config_map.get(point_id).unwrap();
        let display = table_config.enabled.unwrap_or(1).to_string();
        let options = vec![OptionSet {
            name: "enabled".to_string(),
            display,
            description: None,
            required: false,
        }];

        let ds = DataSet {
            id: point_id.clone(),
            name,
            category: None,
            r#type: point_type,
            options: Some(options),
            format: None,
        };
        datasets.push(ds);
    }

    Ok(datasets)
}

/// get opc datasets in csv
/// csv: a file path which start with '@' or an encoded csv string
async fn opc_datasets_by_csv(
    opc_type: OpcType,
    csv: String,
    csv_path: Option<String>,
) -> anyhow::Result<Vec<DataSet>> {
    tracing::info!(
        "read opc points from csv: {}, csv_path: {:?}",
        CsvParser::decoded_csv(&csv)?,
        csv_path
    );
    let mut rdr = CsvParser::open_csv_with_path(csv, csv_path).await?;

    let header = rdr.headers().await?;

    let header = CsvHeader::try_new(opc_type, header)?;
    let point_id_idx = header.id_index();
    let enabled_idx = header.enabled_index();

    let mut datasets = vec![];
    let mut records = rdr.records();
    while let Some(record) = records.next().await {
        let record = record?;
        let point_id = record.get(point_id_idx).ok_or(anyhow::anyhow!(
            "failed to get point id in record: {:?} with index: {}",
            record,
            point_id_idx
        ))?;

        if record.get(enabled_idx).unwrap_or("1") == "0" {
            continue;
        }

        datasets.push(DataSet {
            id: point_id.to_string(),
            name: None,
            category: None,
            r#type: None,
            options: None,
            format: None,
        });
    }

    Ok(datasets)
}

/// 通过执行 taosx-opc points 命令获取 opc 点位
async fn opc_datasets_by_command(config: &OPCConfig) -> anyhow::Result<Vec<DataSet>> {
    let toml =
        toml::to_string(&config).with_context(|| "toml to_string error encountered".to_string())?;
    let mut config_file = NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    tracing::info!(
        "execute: taosx-opc points, opc config: {}\n{}",
        config_path.display(),
        toml
    );

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
    let mut log_path = super::get_log_dir("");
    std::fs::create_dir_all(&log_path)
        .with_context(|| format!("Log path {}", log_path.display()))?;
    log_path.push("opc.log");

    let mut log_rotation = log_rotation(&log_path, 700);

    write!(log_rotation, "{}", String::from_utf8_lossy(&output.stderr))
        .context("writing logs error")?;

    tracing::info!("opc_datasets OPC exit with status {}", output.status);
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        tracing::error!(
            plugin = "opc",
            module = "datasets",
            stdout = ?bytes::Bytes::from(output.stdout),
            "Get OPC datasets error:\n{}",
            error
        );
        let error = filter_opc_log(error.to_string()).await;

        let pattern =
            regex::Regex::new(r#"level=PANIC msg="(?P<msg>.*)" error="(?<error>.*)"#).unwrap();
        let matches = pattern.captures(&error);
        if let Some(matches) = matches {
            anyhow::bail!("{}: {}", &matches["msg"], &matches["error"]);
        } else {
            anyhow::bail!("Get OPC datasets error: {}", &error);
        }
    }
    temp_path.close()?;

    let res: Vec<DataSet> = serde_json::from_slice(&output.stdout)?;
    Ok(res)
}

/// 过滤 opc 错误日志，去掉 info 日志
pub async fn filter_opc_log<S: AsRef<str>>(error_log: S) -> String {
    let mut error = String::new();
    for line in error_log.as_ref().lines() {
        if line.contains(" info ") || line.contains(" trace ") || line.contains(" debug ") {
            continue;
        }
        error.push_str(line);
        error.push('\n');
    }
    error.trim_end().to_string() // remove last '\n'
}

/// 连通性检查
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    #[cfg(not(windows))]
    if dsn.driver == "opcda" {
        return DataSourceValidation::invalid(
            "opc".to_string(),
            "opcda only support windows".to_string(),
        );
    }

    is_valid_impl(dsn)
        .await
        .unwrap_or_else(|err| DataSourceValidation::invalid("opc".to_string(), err.to_string()))
}

async fn is_valid_impl(dsn: &Dsn) -> anyhow::Result<DataSourceValidation> {
    let certificate = get_temp_file(dsn, "certificate");
    let private_key = get_temp_file(dsn, "private_key");
    let auth_certificate = get_temp_file(dsn, "auth_certificate");
    let auth_private_key = get_temp_file(dsn, "auth_private_key");

    let mut config = OPCConfig::from_dsn_check_mode(dsn).await.map_err(|err| {
        anyhow::anyhow!(
            "failed to create opc config from dsn: {}, cause: {}",
            dsn.to_string(),
            err.to_string()
        )
    })?;

    config.set_temp_filepath("certificate", certificate.as_ref())?;
    config.set_temp_filepath("private_key", private_key.as_ref())?;
    config.set_temp_filepath("auth_certificate", auth_certificate.as_ref())?;
    config.set_temp_filepath("auth_private_key", auth_private_key.as_ref())?;

    let toml = toml::to_string(&config)?;
    let mut config_file = NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;

    tracing::info!(
        "execute: taosx-opc check, opc config: {}\n{}",
        config_file.path().display(),
        toml
    );

    // startup the connector
    let opc_exe_path = exe_path()?;
    let mut command = tokio::process::Command::new(opc_exe_path.clone());
    let output = command
        .arg("check")
        .arg("--conf")
        .arg(config_file.path())
        .stdout(std::process::Stdio::inherit())
        .output()
        .await
        .with_context(|| format!("failed to execute: {:?}", opc_exe_path.as_path()))?;

    let result = if output.status.success() {
        let mut result: DataSourceValidation =
            serde_json::from_slice(&output.stdout).map_err(|err| {
                anyhow::anyhow!(
                    "failed to deserialize opc validation result: {}, cause: {}",
                    String::from_utf8_lossy(&output.stdout),
                    err.to_string(),
                )
            })?;
        result.data_source = "opc".to_string();
        result
    } else {
        DataSourceValidation::invalid(
            "opc".to_string(),
            format!(
                "failed to execute opc: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        )
    };

    // clean temporary files
    certificate.map(|f| f.close());
    private_key.map(|f| f.close());
    auth_certificate.map(|f| f.close());
    auth_private_key.map(|f| f.close());

    Ok(result)
}

/// 为 opc 的 csv_config_file 追加一行点位配置
pub async fn append_point_to_csv(from: &Dsn, to: &Dsn, csv_line: String) -> anyhow::Result<()> {
    // 检查新增的 point_id 是否在 CSV 中重复
    check_point_id_duplicated(from, csv_line.clone()).await?;

    // 将新增的点位配置，追加到现有的 CSV 点位配置文件中的第一个
    let parser = CsvParser::from_dsn(from)?;
    let (csv_path, mut csv) = parser.read_to_string().await.map_err(|err| {
        anyhow::anyhow!(
            "failed to read csv file with dsn: {}, cause: {}",
            from,
            err.to_string()
        )
    })?;
    tracing::info!("append line to the csv: {:?}", csv_path);

    // 在 csv 末尾追加一行
    csv = csv.trim_end().to_string();
    csv.push('\n');
    let csv_line = csv_line.lines().skip(1).collect::<Vec<&str>>().join("\n");
    csv.push_str(&csv_line);
    tracing::debug!("append opc point to csv, new point: \n{}", csv);

    // 解析 csv 文件，验证合法性
    let opc_type = OpcType::from_dsn(from)?;
    let model = CsvParser::parse_csv(opc_type, csv.clone()).await?;
    model.validate()?;
    // 如果前端配置了 model_type，则校验 model 是否和 TDengine 的 schema 冲突
    if let Some(model_type) = ModelType::from_dsn(from) {
        model.validate_with_sink(model_type, to).await?;
    }

    // 写入 csv 文件
    match csv_path {
        Some(csv_path) => {
            let mut file = tokio::fs::File::create(csv_path).await?;
            file.write_all(csv.as_bytes()).await?;
        }
        None => {
            unimplemented!("write to csv_config_file in dsn is not supported");
        }
    }

    Ok(())
}

/// 检查新增的 point_id 是否在 CSV 中重复
async fn check_point_id_duplicated(dsn: &Dsn, csv_line: String) -> anyhow::Result<()> {
    let opc_type = OpcType::from_dsn(dsn)?;

    // new point
    let mut rdr = AsyncReader::from_reader(csv_line.as_bytes());
    // new point header
    let headers = rdr.headers().await?;
    let csv_header = CsvHeader::try_new(opc_type, headers)?;
    // new point line
    let mut records = rdr.records();
    let record = records.next().await.unwrap()?;
    let point_id = CsvParser::parse_point_id(&csv_header, &record)?;

    // old points
    let csv_files = OPCConfig::parse_csv_config_files(dsn).ok_or(anyhow::anyhow!(
        "csv_config_file not found in dsn: {}",
        dsn.to_string()
    ))?;
    let parser = CsvParser::try_new(opc_type, csv_files)?;
    let point_ids = parser.parse_all_point_id().await?;

    // check if point_id already exists
    for id in point_ids {
        if id == point_id {
            anyhow::bail!("point id: {} already exists", point_id);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_check_point_id_duplicated() {
        std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());

        // given
        let lines = "point_id\nns=3;i=1008".to_string();
        let dsn =
            Dsn::from_str("opcua:///?csv_config_file=@./tests/opc/opcua-utf8bom.csv").unwrap();
        // when
        let res = check_point_id_duplicated(&dsn, lines).await;
        // then
        assert!(res.is_ok());

        // given
        let lines = "point_id\nns=3;i=1007".to_string();
        // when
        let res = check_point_id_duplicated(&dsn, lines).await;
        // then
        assert!(res.is_err());
        assert_eq!(
            "point id: ns=3;i=1007 already exists",
            res.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_get_temp_file() {
        std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());

        let dsn = Dsn::from_str("opcua://").unwrap();
        let file = get_temp_file(&dsn, "certificate");
        assert!(file.is_none());

        let dsn = Dsn::from_str("opcua://?certificate=").unwrap();
        let file = get_temp_file(&dsn, "certificate");
        assert!(file.is_none());

        let dsn = Dsn::from_str("opcua://?certificate=hello\nworld").unwrap();
        let mut file = get_temp_file(&dsn, "certificate").unwrap();
        let mut content = String::new();
        file.as_file_mut()
            .seek(std::io::SeekFrom::Start(0))
            .unwrap();
        file.read_to_string(&mut content).unwrap();
        assert_eq!(content, "hello\nworld");

        let dsn = Dsn::from_str("opcua://?certificate=@./tests/opc/certificate.crt").unwrap();
        let file = get_temp_file(&dsn, "certificate");
        assert!(file.is_none());
    }

    #[test]
    fn test_generate_tbname_from_pattern() {
        // OPC UA
        assert_eq!(
            generate_tbname_from_pattern("opcua", "t_{ns}_{id}", "ns=13;i=10003"),
            "t_13_10003"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcua", "t_{ns}_{id}", "ns=13;b=GCC"),
            "t_13_GCC"
        );
        assert_eq!(
            generate_tbname_from_pattern(
                "opcua",
                "t_{ns}_{id}",
                "ns=13;g=00000000-0000-0000-0000-000000009204"
            ),
            "t_13_00000000-0000-0000-0000-000000009204"
        );
        assert_eq!(
            generate_tbname_from_pattern(
                "opcua",
                "t_{ns}_{id}",
                r#"ns=3;s=Special_\"!§$%&/()=?`´\\+~*'#_-:.;,<>|@^°€µ{[]}"#
            ),
            r#"t_3_Special_\"!§$%&/()=?_´\\+~*'#_-:_;,<>|@^°€µ{[]}"#
        );

        // OPC DA
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{TagName}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{tag_name}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{/tag_name}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_EDCGQ_MP706AT_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{id}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_/ASSETS/AB/EDCGQ_MP706AT_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t{_id}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_ASSETS_AB_EDCGQ_MP706AT_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{TagName}", "02_LI7059.DACA.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{tag_name}", "02_LI7059.DACA.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{/tag_name}", "02_LI7059.DACA.PV"),
            "t_02_LI7059_DACA_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{id}", "02_LI7059.DACA.PV"),
            "t_02_LI7059_DACA_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{_id}", "02_LI7059.DACA.PV"),
            "t_02_LI7059_DACA_PV"
        );
    }

    #[test]
    fn test_opc_type() {
        let dsn = Dsn::from_str("opcua://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::OPCUA);

        let dsn = Dsn::from_str("opcda://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::OPCDA);

        let dsn = Dsn::from_str("opc+ua://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::OPCUA);

        let dsn = Dsn::from_str("opc+da://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::OPCDA);

        let dsn = Dsn::from_str("fake://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::FAKE);

        let dsn = Dsn::from_str("opc://?fake=true").unwrap();
        let opc_type = OpcType::from_dsn(&dsn).unwrap();
        assert_eq!(opc_type, OpcType::FAKE);

        let dsn = Dsn::from_str("opc://").unwrap();
        let opc_type = OpcType::from_dsn(&dsn);
        assert!(opc_type.is_err());
        assert_eq!("unknown opc protocol", opc_type.unwrap_err().to_string());
    }

    #[tokio::test]
    async fn test_filter_opc_log() {
        let log = r#"
10/28 18:48:50.269658 00071305 info "get max node per read success 10000" id=0 model=points
10/28 18:48:50.269698 00071305 panic "get all points error" error=invalid points regex: error parsing regexp: invalid or unsupported Perl syntax: `(?!` model=points
panic: (*logrus.Entry) 0xc00034aaf0
        "#;
        let expect = r#"
10/28 18:48:50.269698 00071305 panic "get all points error" error=invalid points regex: error parsing regexp: invalid or unsupported Perl syntax: `(?!` model=points
panic: (*logrus.Entry) 0xc00034aaf0"#.to_string();
        let res = filter_opc_log(log).await;
        assert_eq!(res, expect);
    }

    /// # Example
    /// ```shell
    /// OPC_SERVER="192.168.2.16:53530/OPCUA/SimulationServer" PLUGINS_HOME=/Users/yangzy/RustProjects/taosx/plugins LOGS_HOME=/Users/yangzy/taosx/log cargo nextest run -p taosx-core test_opc_datasets_by_command --nocapture --retries 0
    /// ```
    #[tokio::test]
    async fn test_opc_datasets_by_command() {
        if let Ok(opc_server) = std::env::var("OPC_SERVER") {
            let dsn = format!("opcua://{opc_server}?browse_name_pattern=\"数据块_1\".\"Tag\\d+\"",)
                .into_dsn()
                .unwrap();
            let config = OPCConfig::from_dsn_point_mode(&dsn).unwrap();

            let datasets = opc_datasets_by_command(&config).await.unwrap();
            dbg!(&datasets);
        }
    }
}
