use anyhow::Context;
use csv_async::AsyncReader;
use futures_util::StreamExt;
use itertools::Itertools;
use ringbuf::traits::{Consumer, RingBuffer};
use schema::get_schema_path;
use std::fs::File;
use std::{io::prelude::*, sync::Arc};
use taos::Dsn;
use taosx_core::dsv::DataSourceValidation;
use taosx_core::plugins::sink::point::csv::{CsvHeader, CsvParser};
use taosx_core::plugins::sink::point::model::ModelType;
use taosx_core::runners::opc::config::OPCConfig;
use taosx_core::runners::opc::points::{
    OPC_DESCRIPTION, OPC_DISPLAY_NAME, OPC_NODE_CLASS, OPC_PATH, OpcNode,
};
use taosx_core::runners::opc::{OpcType, exe_path};
use taosx_core::runners::{get_data_dir, get_logs_home_dir, new_rolling_file_appender};
use taosx_core::sink::persist::PersistConfig;
use taosx_core::sink::point::csv::parse_csv_config_files;
use taosx_core::sink::point::model::{
    SourceType, generate_tag_value_from_pattern, generate_tbname_from_pattern,
};
use taosx_core::utils::monitor::send_sub_process_info;
use taosx_core::{Action, DataSet, Transferred, build_ipc, utils::port_pool::PortPool};
use taosx_core::{TaskNotify, TaskNotifySender, core_metrics, get_log_dir};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::{io::AsyncBufReadExt, sync::Mutex};
use tokio_process_terminate::TerminateExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tracing_subscriber::fmt::MakeWriter;

use crate::point_updater::PointsUpdater;

pub mod failover;
mod point_updater;
mod schema;

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
    notify: TaskNotifySender,
) -> anyhow::Result<()> {
    if to.subject.is_none() {
        anyhow::bail!("Database name is required in OPC dsn: {}", to.clone());
    }
    if with_agent.is_some() {
        let task_id = task_id.context("Task id not found for agent runner")?;
        let _ = core_metrics::init_task_metrics(&from, &to, task_id, None).await;
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
        notify.clone(),
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
            err
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
    send_sub_process_info(child.id(), task_id, config.opc_type.to_string().as_str()).await;

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
    // clone notify sender for use in stderr reader task
    let notify_for_stderr = notify.clone();
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
                let mut guard = error_buf_producer.lock().await;
                let _ = guard.push_overwrite(line.clone());
            }

            // If OPC process reports a reconnect event, forward it to task notification
            // so it will appear in Explorer as a task activity.
            if line.contains("[RECONNECT]") {
                // best-effort notify, ignore send errors
                let tn = TaskNotify::info(line.trim().to_string());
                let _ = notify_for_stderr.send_async(tn).await;
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

    let source_type = SourceType::try_from(opc_type.as_static_str())?;
    let header = CsvHeader::try_new(source_type, header)?;
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
    let log_dir = get_log_dir("");
    std::fs::create_dir_all(&log_dir).with_context(|| format!("Log path {}", log_dir.display()))?;
    let appender =
        new_rolling_file_appender(log_dir.as_path(), "opc").context("failed to create opc log")?;
    {
        let mut w = appender.make_writer();
        use std::io::Write as _;
        w.write_all(String::from_utf8_lossy(&output.stderr).as_bytes())?;
        w.flush().ok();
    }

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
        .inspect_err(|err| tracing::error!("{err:?}"))
        .unwrap_or_else(|err| DataSourceValidation::invalid("opc".to_string(), err.to_string()))
}

async fn is_valid_impl(dsn: &Dsn) -> anyhow::Result<DataSourceValidation> {
    let certificate = get_temp_file(dsn, "certificate");
    let private_key = get_temp_file(dsn, "private_key");
    let auth_certificate = get_temp_file(dsn, "auth_certificate");
    let auth_private_key = get_temp_file(dsn, "auth_private_key");

    let mut config = OPCConfig::from_dsn_check_mode(dsn)
        .await
        .inspect_err(|err| tracing::error!(dsn=%dsn, "{err:?}"))
        .context("failed to create opc config")?;

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
        .inspect_err(|err| tracing::error!(conf=%config_file.path().display(), "{err:?}"))
        .context("failed to execute taosx-opc check")?;

    let result = if output.status.success() {
        let mut result: DataSourceValidation =
            serde_json::from_slice(&output.stdout).map_err(|err| {
                anyhow::anyhow!(
                    "failed to deserialize opc validation result: {}, cause: {}",
                    String::from_utf8_lossy(&output.stdout),
                    err,
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
        anyhow::anyhow!("failed to read csv file with dsn: {}, cause: {}", from, err)
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
    let source_type = SourceType::try_from(opc_type.as_static_str())?;
    let model = CsvParser::parse_csv(source_type, csv.clone()).await?;
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
    let source_type = SourceType::try_from(dsn)?;

    // new point
    let mut rdr = AsyncReader::from_reader(csv_line.as_bytes());
    // new point header
    let headers = rdr.headers().await?;
    let csv_header = CsvHeader::try_new(source_type, headers)?;
    // new point line
    let mut records = rdr.records();
    let record = records.next().await.unwrap()?;
    let point_id = CsvParser::parse_point_id(&csv_header, &record)?;

    // old points
    let csv_files = parse_csv_config_files(dsn)
        .ok_or(anyhow::anyhow!("csv_config_file not found in dsn: {}", dsn))?;
    let parser = CsvParser::try_new(source_type, csv_files)?;
    let point_ids = parser.parse_all_point_id().await?;

    // check if point_id already exists
    for id in point_ids {
        if id == point_id {
            anyhow::bail!("point id: {} already exists", point_id);
        }
    }

    Ok(())
}

pub async fn to_csv_context_by_mode(
    opc_points_mode: &str,
    datasets: Vec<DataSet>,
) -> anyhow::Result<String> {
    match opc_points_mode {
        "variable" => {
            let filter = |node_class: &str| node_class == "Variable";
            variables_to_csv(datasets, Some(filter)).await
        }
        "object" => {
            let filter = |node_class: &str| node_class == "Object";
            variables_to_csv(datasets, Some(filter)).await
        }
        "all" => variables_to_csv::<fn(&str) -> bool>(datasets, None).await,
        _ => anyhow::bail!("unsupported opc points mode: {}", opc_points_mode),
    }
}

pub async fn variables_to_csv<F>(
    datasets: Vec<DataSet>,
    filter: Option<F>,
) -> anyhow::Result<String>
where
    F: Fn(&str) -> bool,
{
    let mut wtr = csv_async::AsyncWriter::from_writer(vec![]);

    // headers
    let cols = vec![
        "id",
        "name",
        OPC_NODE_CLASS,
        OPC_DISPLAY_NAME,
        OPC_DESCRIPTION,
        OPC_PATH,
    ];
    wtr.write_record(&cols).await?;
    wtr.flush().await?;

    // rows
    for dataset in datasets {
        if let Some(ref filter) = filter {
            let node_class = dataset.r#type.clone().unwrap_or_default();
            if !filter(&node_class) {
                continue;
            }
        }
        let mut row = vec![];
        row.push(dataset.id);
        row.push(dataset.name.unwrap_or_default());
        row.push(dataset.r#type.unwrap_or_default());

        match dataset.options {
            Some(opts) => {
                let display_name = opts
                    .iter()
                    .find(|opt| opt.name == OPC_DISPLAY_NAME)
                    .map(|opt| opt.display.clone())
                    .unwrap_or_default();
                row.push(display_name);

                let description = opts
                    .iter()
                    .find(|opt| opt.name == OPC_DESCRIPTION)
                    .and_then(|opt| opt.description.clone())
                    .unwrap_or_default();
                row.push(description);

                let path = opts
                    .iter()
                    .find(|opt| opt.name == OPC_PATH)
                    .map(|opt| opt.display.clone())
                    .unwrap_or_default();
                row.push(path);
            }
            None => {
                row.push("".to_string());
                row.push("".to_string());
                row.push("".to_string());
            }
        }

        wtr.write_record(&row).await?;
    }

    let data = wtr.into_inner().await?;
    let csv_content = String::from_utf8(data)?;
    Ok(csv_content)
}

pub async fn to_csv_context(opc_type: OpcType, datasets: Vec<DataSet>) -> anyhow::Result<String> {
    let mut csv_content = get_template(opc_type, false);
    for (idx, item) in datasets.iter().enumerate() {
        let row = match opc_type {
            OpcType::OPCUA => ua_template_row(idx + 1, item),
            OpcType::OPCDA => da_template_row(idx + 1, item),
            _ => unimplemented!("template for opc type {:?} is not supported", opc_type),
        };
        csv_content.push_str(&row);
    }
    Ok(csv_content)
}

pub fn get_template(opc_type: OpcType, with_demo: bool) -> String {
    match opc_type {
        OpcType::OPCUA => ua_template(with_demo),
        OpcType::OPCDA => da_template(with_demo),
        _ => unimplemented!("template for opc type {:?} is not supported", opc_type),
    }
}

fn ua_template(with_demo: bool) -> String {
    let mut template = UA_HEADER.iter().join(",");
    template.push('\n');
    if with_demo {
        template.push_str("1,ns=3;i=1010,1,opc_{type},t_{ns}_{id},val,val*1.8+32,double,quality,ts,,qts,,rts,,temperature\n");
        template.push_str("2,ns=3;i=1011,1,opc_{type},t_{ns}_{id},val,val + 10,int,quality,ts,ts+8*3600*1000,qts,qts+8*3600*1000,rts,rts+8*3600*1000,pressure\n");
        template.push_str("3,ns=5;s=abcd,1,opc_{type},t_{ns}_{id},val,,,quality,ts,ts-6*1000,qts,qts-6*1000,rts,rts-6*1000,current\n");
    }
    template
}

fn da_template(with_demo: bool) -> String {
    let mut template = DA_HEADER.iter().join(",");
    template.push('\n');
    if with_demo {
        template.push_str("1,root.parent.temperature,1,opc_{type},t_{tag_name},val,val*1.8+32,float,quality,ts,,qts,,rts,,temperature\n");
        template.push_str("2,root.parent.pressure,1,opc_{type},t_{tag_name},val,val+10,,quality,ts,ts+8*3600*1000,qts,qts+8*3600*1000,rts,rts+8*3600*1000,pressure\n");
        template.push_str("3,root.parent.current,1,opc_{type},t_{tag_name},val,,,quality,ts,ts-6*1000,qts,qts-6*1000,rts,rts-6*1000,current\n");
    }
    template
}

pub const UA_HEADER: [&str; 20] = [
    "No.",
    "point_id",
    "enabled",
    "stable",
    "tbname",
    "value_col",
    "value_transform",
    "type",
    "quality_col",
    "ts_col",
    "ts_transform",
    "request_ts_col",
    "request_ts_transform",
    "received_ts_col",
    "received_ts_transform",
    "tag::VARCHAR(1024)::name",
    "tag::VARCHAR(1024)::BrowseName",
    "tag::VARCHAR(1024)::DisplayName",
    "tag::VARCHAR(1024)::Description",
    "tag::VARCHAR(1024)::Path",
];

pub const UA_ROW: [&str; 20] = [
    "",
    "",
    "",
    "opc_{type}",
    "t_{ns}_{id#/_}",
    "val",
    "",
    "",
    "quality",
    "ts",
    "",
    "qts",
    "",
    "rts",
    "",
    "{id#/.}",
    "{BrowseName}",
    "{DisplayName}",
    "{Description}",
    "{Path}",
];

pub const DA_HEADER: [&str; 16] = [
    "No.",
    "tag_name",
    "enabled",
    "stable",
    "tbname",
    "value_col",
    "value_transform",
    "type",
    "quality_col",
    "ts_col",
    "ts_transform",
    "request_ts_col",
    "request_ts_transform",
    "received_ts_col",
    "received_ts_transform",
    "tag::VARCHAR(200)::name",
];

pub const DA_ROW: [&str; 16] = [
    "",
    "",
    "",
    "opc_{type}",
    "t_{tag_name}",
    "val",
    "",
    "",
    "quality",
    "ts",
    "",
    "qts",
    "",
    "rts",
    "",
    "",
];

pub fn ua_template_row(row_idx: usize, item: &DataSet) -> String {
    let mut cols = vec![];

    let opc_node = OpcNode::try_from(item.clone()).ok();

    for (col_idx, col) in UA_ROW.iter().enumerate() {
        if col_idx == 0 {
            // No.
            cols.push(row_idx.to_string());
        } else if col_idx == 1 {
            // point_id
            let point_id = get_safe_string_for_csv(&item.id);
            cols.push(point_id.clone());
        } else if col_idx == 2 {
            // enabled
            let enabled = get_enabled(item.clone());
            cols.push(enabled.to_string());
        } else if col_idx == 3 {
            let node_type = opc_node.as_ref().and_then(|n| n.node_type.clone());
            let stable = if let Some("Object") = node_type.as_deref() {
                "opc_object"
            } else {
                "opc_{type}"
            };
            cols.push(stable.to_string());
        } else if col_idx == 4 {
            // tbname
            let point_id = get_safe_string_for_csv(&item.id);
            let tbname = generate_tbname_from_pattern(
                SourceType::OPCUA.as_static_str(),
                "t_{ns}_{id#/_}",
                &point_id,
            );
            cols.push(tbname);
        } else if col_idx == 15 {
            // tag::VARCHAR(255)::name
            let point_id = &item.id;
            let name = generate_tag_value_from_pattern(
                SourceType::OPCUA.as_static_str(),
                "{id#/.}",
                point_id,
            );
            cols.push(name);
        } else if col_idx == 16 {
            // tag::VARCHAR(255)::BrowseName
            let browse_name = opc_node
                .as_ref()
                .and_then(|n| n.name.clone())
                .unwrap_or("{BrowseName}".to_string());
            cols.push(browse_name.clone());
        } else if col_idx == 17 {
            // tag::VARCHAR(255)::DisplayName
            let display_name = opc_node
                .as_ref()
                .and_then(|n| n.display_name.clone())
                .unwrap_or("{DisplayName}".to_string());
            cols.push(display_name.clone());
        } else if col_idx == 18 {
            // tag::VARCHAR(255)::Description
            let description = opc_node
                .as_ref()
                .and_then(|n| n.description.clone())
                .unwrap_or("".to_string());
            cols.push(description.clone());
        } else if col_idx == 19 {
            // tag::VARCHAR(255)::Path
            let path = opc_node
                .as_ref()
                .and_then(|n| n.path.clone())
                .unwrap_or("{Path}".to_string());
            cols.push(path.clone());
        } else {
            cols.push(col.to_string());
        }
    }
    format!("{}\n", cols.join(","))
}

pub fn da_template_row(row_idx: usize, item: &DataSet) -> String {
    // 替换 DA_ROW 的前三个字段和最后一个字段
    let mut cols = vec![];
    for (idx, col) in DA_ROW.iter().enumerate() {
        if idx == 0 {
            // No.
            cols.push(row_idx.to_string());
        } else if idx == 1 {
            // tag_name
            cols.push(item.id.clone());
        } else if idx == 2 {
            // enabled
            let enabled = get_enabled(item.clone());
            cols.push(enabled.to_string());
        } else if idx == (DA_ROW.len() - 1) {
            // tag::VARCHAR(255)::name
            cols.push(item.name.clone().unwrap_or("".to_string()));
        } else {
            cols.push(col.to_string());
        }
    }
    format!("{}\n", cols.join(","))
}

fn get_enabled(item: DataSet) -> i8 {
    item.options
        .map(|o| {
            if o.is_empty() {
                return 1;
            }
            o.iter()
                .find(|o| o.name == "enabled")
                .map(|o| {
                    if o.display == "0" {
                        return 0;
                    }
                    1
                })
                .unwrap_or(1)
        })
        .unwrap_or(1)
}

fn get_safe_string_for_csv(s: &str) -> String {
    let mut safe_str = s.to_string();
    if safe_str.contains(",") {
        safe_str = format!("\"{}\"", safe_str.replace("\"", "\"\""));
    }
    safe_str
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use csv_async::StringRecord;

    use super::*;

    #[tokio::test]
    async fn test_check_point_id_duplicated() {
        unsafe {
            std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());
        }

        // given
        let lines = "point_id\nns=3;i=1008".to_string();
        let dsn = Dsn::from_str("opcua:///?csv_config_file=@./tests/opcua-utf8bom.csv").unwrap();
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
    fn test_get_template() {
        let template = get_template(OpcType::OPCUA, true);
        let lines = template.trim().split("\n").collect_vec();
        assert_eq!(lines.len(), 4);

        let header = StringRecord::from(lines[0].split(",").collect_vec());
        let header = CsvHeader::try_new(SourceType::OPCUA, &header).unwrap();
        assert_eq!(header.source_type, SourceType::OPCUA);
        assert_eq!(header.columns.len(), 20);
        assert_eq!(header.get_column("point_id").unwrap().index, 1);
        assert_eq!(header.get_primary_timestamp().unwrap().name, "ts_col");
        assert_eq!(header.get_column("ts_transform").unwrap().index, 10);
        assert_eq!(header.get_column("request_ts_col").unwrap().index, 11);
        assert_eq!(header.get_column("request_ts_transform").unwrap().index, 12);
        assert_eq!(header.get_column("received_ts_col").unwrap().index, 13);
        assert_eq!(
            header.get_column("received_ts_transform").unwrap().index,
            14
        );

        let template = get_template(OpcType::OPCDA, true);
        let lines = template.trim().split("\n").collect_vec();
        assert_eq!(lines.len(), 4);
        let header = StringRecord::from(lines[0].split(",").collect_vec());
        let header = CsvHeader::try_new(SourceType::OPCDA, &header).unwrap();
        assert_eq!(header.source_type, SourceType::OPCDA);
        assert_eq!(header.columns.len(), 16);
        assert_eq!(header.get_column("tag_name").unwrap().index, 1);
        assert_eq!(header.get_primary_timestamp().unwrap().name, "ts_col");
        assert_eq!(header.get_column("ts_transform").unwrap().index, 10);
        assert_eq!(header.get_column("request_ts_col").unwrap().index, 11);
        assert_eq!(header.get_column("request_ts_transform").unwrap().index, 12);
        assert_eq!(header.get_column("received_ts_col").unwrap().index, 13);
        assert_eq!(
            header.get_column("received_ts_transform").unwrap().index,
            14
        );
    }

    #[test]
    fn test_ua_template_row() {
        let item = DataSet {
            id: "ns=3;i=1001".to_string(),
            name: Some("tag1".to_string()),
            category: None,
            r#type: None,
            options: None,
            format: None,
        };
        let row = ua_template_row(1, &item);
        // println!("{}", &row);
        assert_eq!(
            row,
            "1,ns=3;i=1001,1,opc_{type},t_3_1001,val,,,quality,ts,,qts,,rts,,1001,{BrowseName},{DisplayName},,{Path}\n".to_string()
        );
    }

    #[test]
    fn test_da_template_row() {
        let item = DataSet {
            id: "/ASSETS/AB/EDCGQ".to_string(),
            name: Some("tag1".to_string()),
            category: None,
            r#type: None,
            options: None,
            format: None,
        };
        let row = da_template_row(1, &item);
        assert_eq!(
            row,
            "1,/ASSETS/AB/EDCGQ,1,opc_{type},t_{tag_name},val,,,quality,ts,,qts,,rts,,tag1\n"
                .to_string()
        );
    }
}
