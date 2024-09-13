use anyhow::{bail, Context};
use csv_async::AsyncReader;
use futures_util::StreamExt;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::str::FromStr;
use std::{io::prelude::*, path::PathBuf, sync::Arc};
use taos::{Dsn, DsnError};
use taosx_ipc::prelude::IpcDataType;
use taosx_ipc::types::OptionSet;
use tempfile::NamedTempFile;
use tokio::{io::AsyncBufReadExt, sync::Mutex};
use tokio_process_terminate::TerminateExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::dsv::DataSourceValidation;
use crate::runners::log_rotation;
use crate::runners::opc::config::csv::header::CsvHeader;
use crate::runners::opc::config::csv::CsvParser;
use crate::runners::opc::config::model::OpcModelConfig;
use crate::runners::opc::config::{OPCConfig, PointsMode};
use crate::runners::opc::point_updater::PointsUpdater;
use crate::utils::monitor::send_sub_process_info;
use crate::{
    build_ipc, get_log_keep_days, utils::port_pool::PortPool, Action, DataSet, DataSetsReq,
    Transferred,
};

use super::get_data_dir;

pub mod config;
mod point_updater;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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
        let fake = dsn.params.get("fake").is_some();
        if fake {
            return Ok(Self::FAKE);
        }

        let opc_type = dsn.driver.as_str();
        let protocol = dsn.protocol.clone();
        match opc_type {
            "opcua" => Ok(Self::OPCUA),
            "opcda" => Ok(Self::OPCDA),
            "fake" => Ok(Self::FAKE),
            "opc" => match protocol.as_deref() {
                Some("ua") => Ok(Self::OPCUA),
                Some("da") => Ok(Self::OPCDA),
                _ => anyhow::bail!("unknown opc protocol"),
            },
            _ => anyhow::bail!("invalid opc type"),
        }
    }
}

impl FromStr for OpcType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "opcua" => Ok(Self::OPCUA),
            "opcda" => Ok(Self::OPCDA),
            "fake" => Ok(Self::FAKE),
            _ => Err(s.to_string()),
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

const EXE: &'static str = {
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
        bail!(
            "Database name is required in OPC dsn: {}",
            to.clone().to_string()
        );
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

    // create IPC handler
    let connector = match config.opc_type {
        OpcType::OPCUA => Some("opc_ua"),
        OpcType::OPCDA => Some("opc_da"),
        OpcType::FAKE => None,
    };
    let mut ipc_handler = build_ipc(
        &config.report.remote,
        None,
        &to,
        connector,
        config.get_model_config().cloned(),
        None,
        &cancel,
        with_agent,
        transferred,
        task_id.clone(),
        notify,
    )
    .await?;

    // create log file: opc.log
    let mut log_path = super::get_log_dir("");
    std::fs::create_dir_all(&log_path)
        .with_context(|| format!("Log path {}", log_path.display()))?;
    log_path.push(format!("opc-{}.log", task_id.unwrap_or(0)));
    let log_keep_days = get_log_keep_days();
    let mut log_rotation = log_rotation(&log_path, log_keep_days);

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
                    bail!("OPC exit with {}\n{error}", status);
                } else {
                    safe_exit!();
                    bail!("OPC process was killed by signal");
                }
            },
            err = ipc_handler.recv_error() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                if let Some(err) = err {
                    safe_exit!();
                    bail!("OPC writer error: {err}");
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

fn csv_string_record_from_iter<'a, I>(iter: I) -> String
where
    I: IntoIterator<Item = String>,
{
    let record = csv_lib::StringRecord::from_iter(iter);

    let mut writer = Vec::new();
    let mut wtr = csv_lib::Writer::from_writer(&mut writer);
    wtr.write_record(&record).unwrap();
    wtr.flush().unwrap();
    drop(wtr);
    String::from_utf8_lossy(&writer).trim().to_string()
}

/// TODO: should support more complicated pattern
/// a expression like d00{point_id}_{tag1}_{tag2}
/// for now only support <table_prfix>_{ns}_{id}_<table_suffix> for opcua
/// <table_prfix>_{TagName}_<table_suffix> for opcda
fn generate_tbname_from_pattern(ty: &str, tb_name: &str, point_id: &str) -> String {
    let tbname = if ty == "opcua" {
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
    } else {
        let tag_index = point_id.rfind(".");
        let tag_name = if let Some(index) = tag_index {
            // should be Device.DeviceType.TagName pattern
            &point_id[index + 1..]
        } else {
            &point_id
        };
        let tb_name = tb_name.replace("{TagName}", tag_name);
        let tb_name = tb_name.replace("{tag_name}", tag_name);

        tb_name
    };
    tbname.replace(".", "_").replace("`", "_")
}

fn generate_stable_from_pattern(stable_expr: &String, value_type: &Option<IpcDataType>) -> String {
    let mut stable = stable_expr.clone();
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
    dsn.get(key)
        .map(|v| {
            if v.is_empty() || v.starts_with('@') {
                return None;
            }

            let mut file = NamedTempFile::new().unwrap();
            file.write_all(v.as_bytes()).unwrap();
            Some(file)
        })
        .flatten()
}

/// 获取 opc 点位
pub async fn opc_datasets(req: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    let from: Dsn = req.from.parse().map_err(|err: DsnError| {
        anyhow::anyhow!(
            "failed to parse dsn: {}, cause: {}",
            req.from,
            err.to_string()
        )
    })?;

    if req.categories.is_empty() {
        bail!("categories is empty");
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

            let parser = CsvParser::try_new(opc_type.clone(), csv_files)?;
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

    let header = CsvHeader::try_new(opc_type.clone(), header)?;
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

    let res: Vec<DataSet> = serde_json::from_slice(&output.stdout)?;
    Ok(res)
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

/// 从 csv_config_files 中获取 csv 文件的 headers
pub async fn get_csv_headers(dsn: &Dsn) -> anyhow::Result<HashMap<String, CsvHeader>> {
    let opc_type = OpcType::from_dsn(dsn)?;
    let csv_files = OPCConfig::parse_csv_config_files(dsn).ok_or(anyhow::anyhow!(
        "csv_config_file not found in dsn: {}",
        dsn.to_string()
    ))?;
    tracing::debug!("get headers from csv files: {:?}", csv_files);

    // parse header from csv files
    let parser = CsvParser::try_new(opc_type, csv_files)?;
    let headers = parser.get_all_headers().await?;

    Ok(headers)
}

/// 为 opc 的 csv_config_file 追加一行点位配置
pub async fn append_point(dsn: &Dsn, csv_line: String) -> anyhow::Result<()> {
    let opc_type = OpcType::from_dsn(dsn)?;

    // new point
    let csv_line_cloned = csv_line.clone();
    let mut rdr = AsyncReader::from_reader(csv_line_cloned.as_bytes());
    // new point header
    let headers = rdr.headers().await?;
    let csv_header = CsvHeader::try_new(opc_type.clone(), headers)?;
    // new point line
    let mut records = rdr.records();
    let record = records.next().await.unwrap()?;
    let point_id = CsvParser::parse_point_id(&csv_header, &record)?;

    // old points
    let csv_files = OPCConfig::parse_csv_config_files(dsn).ok_or(anyhow::anyhow!(
        "csv_config_file not found in dsn: {}",
        dsn.to_string()
    ))?;
    let parser = CsvParser::try_new(opc_type.clone(), csv_files)?;
    let point_ids = parser.parse_all_point_id().await?;

    // check if point_id already exists
    for id in point_ids {
        if id == point_id {
            bail!("point id: {} already exists", point_id);
        }
    }

    // 将新增的点位配置，追加到现有的 CSV 点位配置文件中
    let csv_files = OPCConfig::parse_csv_config_files(dsn).ok_or(anyhow::anyhow!(
        "csv_config_file not found in dsn: {}",
        dsn.to_string()
    ))?;
    let parser = CsvParser::try_new(opc_type, csv_files)?;
    parser.append_line(csv_line).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_csv_headers() {
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-utf8bom.csv").unwrap();

        let headers = get_csv_headers(&dsn).await.unwrap();

        dbg!(headers);
    }

    #[test]
    fn test_get_temp_file() {
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

        let dsn = Dsn::from_str("opcua://?certificate=@../tests/opc/certificate.crt").unwrap();
        let file = get_temp_file(&dsn, "certificate");
        assert!(file.is_none());
    }

    #[test]
    fn test_tbname_pattern() {
        let cases = [
            ("{ns}_{id}", "ns=13;i=10003", "13_10003"),
            ("{ns}_{id}", "ns=13;b=GCC", "13_GCC"),
            (
                "{ns}_{id}",
                "ns=13;g=00000000-0000-0000-0000-000000009204",
                "13_00000000-0000-0000-0000-000000009204",
            ),
            (
                "{ns}_{id}",
                r#"ns=3;s=Special_\"!§$%&/()=?`´\\+~*'#_-:.;,<>|@^°€µ{[]}"#,
                r#"3_Special_\"!§$%&/()=?_´\\+~*'#_-:_;,<>|@^°€µ{[]}"#,
            ),
        ];
        for (pattern, point_id, expected) in cases.iter() {
            let tbname = generate_tbname_from_pattern("opcua", pattern, point_id);
            assert_eq!(tbname, *expected);
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_opc_ua_valid() {
        unsafe {
            std::env::set_var("PLUGINS_HOME", "../plugins");
        }

        let dsn = Dsn::from_str("opcua://192.168.2.16:53530/OPCUA/SimulationServer").unwrap();
        let dsv = is_valid(&dsn).await;
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!("opc", dsv.data_source);
    }

    #[ignore]
    #[tokio::test]
    async fn test_opc_da_valid() {
        unsafe {
            std::env::set_var("PLUGINS_HOME", "../plugins");
        }

        let dsn = Dsn::from_str("opcda://192.168.2.16").unwrap();
        let dsv = is_valid(&dsn).await;
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!("opc", dsv.data_source);
        assert_eq!("2.4.0", dsv.version.unwrap());
    }

    #[test]
    fn test_csv_string_record() {
        let s = r#"ns=3;s=Special_"!§$%&/()=?`´\+~*'#_-:.;,<>|@^°€µ{[]}::meter_3_Special_"!§$%&/()=?_´\+~*'#_-:_;,<>|@^°€µ{[]}"#;
        let record = csv_lib::StringRecord::from_iter([s]);
        let line = record.iter().join(",");
        dbg!(&line);

        let mut writer = Vec::new();
        let mut wtr = csv_lib::Writer::from_writer(&mut writer);
        wtr.write_record(&record).unwrap();
        wtr.flush().unwrap();
        drop(wtr);
        let line = String::from_utf8(writer).unwrap().trim().to_string();
        dbg!(&line);
    }

    #[ignore]
    #[tokio::test]
    async fn test_opc_datasets_by_command() {
        unsafe {
            std::env::set_var("PLUGINS_HOME", "/Users/yangzy/RustProjects/taosx/plugins");
            std::env::set_var("LOGS_HOME", "/Users/yangzy/taosx/log");
        }

        let dsn = Dsn::from_str("opcua://192.168.2.16:53530/OPCUA/SimulationServer").unwrap();
        let config = OPCConfig::from_dsn_point_mode(&dsn).unwrap();
        let res = opc_datasets_by_command(&config).await.unwrap();

        dbg!(res);
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
}
