use std::fmt::Display;
use std::str::FromStr;
use std::{fs, io::prelude::*, path::PathBuf, sync::Arc};

use anyhow::Context;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::{AsyncTBuilder, Dsn, TaosBuilder, Ty};
use tempfile::NamedTempFile;
use tokio::{io::AsyncBufReadExt, sync::Mutex};
use tokio_process_terminate::TerminateExt;
pub use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Span};

use crate::dsv::DataSourceValidation;
use crate::runners::log_rotation;
use crate::runners::opc::config::model::{ColumnConfig, TableConfig};
use crate::runners::opc::config::OPCConfig;
use crate::utils::monitor::send_sub_process_info;
use crate::{
    build_ipc, get_log_keep_days, utils::port_pool::PortPool, Action, DataSet, DataSetsReq,
    Transferred,
};

use super::get_data_dir;

pub mod config;

const EXE: &'static str = {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
            "taosx-opc.exe"
        } else {
            "taosx-opc"
        }
    }
};
const LOG_FILE: &str = "opc.log";

fn exe_path() -> anyhow::Result<PathBuf> {
    let path = super::get_plugin_dir("opc").join(EXE);
    if !path.exists() {
        return Err(anyhow::anyhow!("opc plugin not found at: {:?}", path));
    }
    Ok(path)
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
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    if to.subject.is_none() {
        anyhow::bail!(
            "Database name is required in OPC dsn: {}",
            to.clone().to_string()
        );
    }
    let ipc_port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for OPC connection"))?;

    let builder: TaosBuilder = TaosBuilder::from_dsn(&to)?;
    let taos = builder.build().await?;

    let select_all_points = OPCConfig::parse_select_all_points(&from)?.unwrap_or(false);
    if select_all_points {
        handle_select_all_points(&mut from).await?;
    }

    // 将文件中的内容写入到临时文件中，然后将文件路径写入到 DSN 中
    let certificate = get_temp_file(&mut from, "certificate");
    let private_file = get_temp_file(&mut from, "private_key");
    let auth_certificate = get_temp_file(&mut from, "auth_certificate");
    let auth_private_key = get_temp_file(&mut from, "auth_private_key");

    let config = OPCConfig::from_dsn_collect_mode(&from, ipc_port, &taos, task_id).await?;

    let toml = toml::to_string(&config)?;
    let mut config_file = NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();
    tracing::info!(
        "opc_to_taos using opc config file {} \n{}",
        config_path.display(),
        toml
    );

    // save the temporary file to task dir
    if let Some(task_id) = task_id {
        let path = get_data_dir().join("tasks").join(task_id.to_string());
        fs::create_dir_all(&path).unwrap();
        let path = path.join(format!(
            "{}-{}-{}.{}",
            task_id,
            "opc",
            chrono::Local::now().format("%Y%m%d%H%M"),
            "toml"
        ));
        let _ = fs::copy(&config_path, path);
    }

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
        &cancel,
        with_agent,
        transferred,
        span,
        task_id.clone(),
        notify,
    )
    .await?;

    let port_pool = port_pool.clone();
    let mut command = tokio::process::Command::new(exe_path()?);

    let mut log_path = super::get_log_dir("");
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

    send_sub_process_info(child.id(), task_id, config.opc_type.to_string().as_str());
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
                use std::time::Duration;
                let _ = child.terminate_timeout(Duration::from_secs(2)).await;
                tokio::spawn(async move {
                    tracing::info!("Wait for IPC handlers finished");
                    let _ = ipc_handler.close().await;
                    tracing::info!("All IPC handlers have been finished");
                });
                let _ = temp_path.close();
                certificate.map(|f| f.close());
                private_file.map(|f| f.close());
                auth_certificate.map(|f| f.close());
                auth_private_key.map(|f| f.close());

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
        pattern: None,
        limit: usize::MAX / 2 - 1,
        lang: None,
    };
    let all_points = opc_datasets(&data).await?;
    // 对于 OPCUA 来说，ns=3;s=Special_\"!§$%&/()=?`´\\+~*'#_-:.;,<>|@^°€µ{[]} 是一个有效的点位 ID 和名称
    // 此时需要借助 CSV 的 delimiter 使用 , 进行分隔
    // 前提是点位需要使用双引号引起来
    // 又引出的问题的是如果点位名称已经包含了双引号该如何处理 -》继续加双引号
    // 使用标准 CSV Writer 来处理。
    let point_config = csv_string_record_from_iter(all_points.iter().map(|point| {
        let point_id = point.id.as_str();
        let tbname = generate_tbname_from_pattern(&dsn.driver, &child_table_expression, point_id);
        format!("{}::{}", point_id, tbname)
    }));
    if dsn.driver.as_str() == "opcua" {
        dsn.set("ua.nodes", point_config);
    } else {
        dsn.set("da.tags", point_config);
    }
    let stable_prefix = Some(String::from("opc"));
    let mut column_configs = vec![];

    column_configs.push(ColumnConfig {
        name: String::from("value"),
        r#type: None,
        alias: Some(String::from("val")),
        transform: None,
        is_primary_key: false,
    });
    column_configs.push(ColumnConfig {
        name: String::from("quality"),
        r#type: Some(Ty::Int),
        alias: None,
        transform: None,
        is_primary_key: false,
    });
    let opc_table_config = if table_primary_key == "received_ts" {
        column_configs.push(ColumnConfig {
            name: String::from("received_ts"),
            r#type: Some(Ty::Timestamp),
            alias: None,
            transform: None,
            is_primary_key: true,
        });
        column_configs.push(ColumnConfig {
            name: String::from("original_ts"),
            r#type: Some(Ty::Timestamp),
            alias: None,
            transform: None,
            is_primary_key: false,
        });
        TableConfig {
            enabled: None,
            stable_prefix,
            column_configs,
            tag_configs: None,
        }
    } else {
        column_configs.push(ColumnConfig {
            name: String::from("original_ts"),
            r#type: Some(Ty::Timestamp),
            alias: None,
            transform: None,
            is_primary_key: true,
        });
        TableConfig {
            enabled: None,
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

/// 解析为文件路径.
/// 1. 如果以@开头，表示文件路径, 直接覆盖会dsn;
/// 2. 否则，认为是文件内容，存储到临时文件后，返回文件句柄，为了使tempfile不被删除，需要返回NamedTempFile.
fn get_temp_file(dsn: &mut Dsn, key: &str) -> Option<NamedTempFile> {
    let file_name = dsn.get(key);
    if file_name.is_none() {
        return None;
    }
    let file_name = file_name.unwrap();

    if file_name.starts_with('@') {
        let file_path = &file_name[1..];
        let f = fs::canonicalize(&PathBuf::from(file_path)).unwrap();
        dsn.set(key, f.to_str().unwrap());
        None
    } else {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(file_name.as_bytes()).unwrap();
        dsn.set(key, file.path().to_str().unwrap().to_string());

        Some(file)
    }
}

pub async fn opc_datasets(req: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    let mut from: Dsn = req.from.parse()?;
    let certificate = get_temp_file(&mut from, "certificate");
    let private_key = get_temp_file(&mut from, "private_key");
    let auth_certificate = get_temp_file(&mut from, "auth_certificate");
    let auth_private_key = get_temp_file(&mut from, "auth_private_key");

    if req.categories.is_empty() {
        anyhow::bail!("categories is empty");
    }

    let config = OPCConfig::from_dsn_point_mode(&from)?;
    let toml =
        toml::to_string(&config).with_context(|| "toml to_string error encountered".to_string())?;
    let mut config_file = tempfile::NamedTempFile::new()?;
    write!(config_file, "{}", &toml)?;
    let config_path = config_file.path().to_path_buf();
    let temp_path = config_file.into_temp_path();

    tracing::info!(
        "opc_datasets Using opc config file {} \n{}",
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
    log_path.push(LOG_FILE);

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
            anyhow::bail!("{}: {}", &matches["msg"], &matches["error"]);
        } else {
            anyhow::bail!("Get OPC datasets error: {}", &error);
        }
    }

    temp_path.close()?;
    certificate.map(|f| f.close());
    private_key.map(|f| f.close());
    auth_certificate.map(|f| f.close());
    auth_private_key.map(|f| f.close());
    let res: Vec<DataSet> = serde_json::from_slice(&output.stdout)?;
    Ok(res)
}

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    #[cfg(not(windows))]
    if dsn.driver == "opcda" {
        return DataSourceValidation::invalid(
            "opc".to_string(),
            "opcda only support windows".to_string(),
        );
    }

    let mut dsn = dsn.clone();
    let certificate = get_temp_file(&mut dsn, "certificate");
    let private_key = get_temp_file(&mut dsn, "private_key");
    let auth_certificate = get_temp_file(&mut dsn, "auth_certificate");
    let auth_private_key = get_temp_file(&mut dsn, "auth_private_key");

    let config = OPCConfig::from_dsn_for_validate(&dsn).await;
    let r = match config {
        Err(err) => DataSourceValidation::invalid(
            "opc".to_string(),
            format!(
                "invalid opc dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            let res = validate_opc(c).await;
            res.unwrap_or_else(|err| {
                DataSourceValidation::invalid(
                    "opc".to_string(),
                    format!(
                        "failed to connect to dsn: {}, cause: {}",
                        dsn.to_string(),
                        err.to_string()
                    ),
                )
            })
        }
    };

    // clean temporary files
    certificate.map(|f| f.close());
    private_key.map(|f| f.close());
    auth_certificate.map(|f| f.close());
    auth_private_key.map(|f| f.close());

    r
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
        let mut result: DataSourceValidation = serde_json::from_slice(&output.stdout)
            .with_context(|| {
                format!(
                    "Deserialize opc validation result error: {}",
                    String::from_utf8_lossy(&output.stdout)
                )
            })?;

        result.data_source = "opc".to_string();
        Ok(result)
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
    use super::*;
    use std::env;

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
}

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

#[cfg(test)]
mod opc_type_tests {
    use super::*;

    #[test]
    fn test_from_dsn() {
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
