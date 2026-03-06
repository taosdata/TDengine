use anyhow::{Context, Ok, bail};
use itertools::Itertools;
use ringbuf::traits::{Consumer, RingBuffer};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use std::{io::Write, path::PathBuf};
use taos::Dsn;
use taosx_core::core_metrics::{CoreMetrics, get_metrics_arc_or, insert_metrics};
use taosx_core::dsv::DataSourceValidation;
use taosx_core::runners::{get_data_dir, get_logs_home_dir, new_rolling_file_appender};
use taosx_core::sink::ipc_metric::IpcMetrics;
use taosx_core::sink::point::model::{PointModelConfig, generate_stable_from_pattern};
use taosx_core::utils::monitor::send_sub_process_info;
use taosx_core::utils::port_pool::PortPool;
use taosx_core::{DataSet, DataSetsReq, TaskNotifySender, Via, build_ipc, utils};
use tempfile::NamedTempFile;
use tokio::io::AsyncBufReadExt;
use tokio::sync::Mutex;
use tokio_process_terminate::TerminateExt;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument};
use tracing_subscriber::fmt::MakeWriter;

use crate::config::PspaceConfig;
use crate::model::PspaceModelConfig;
use crate::nodes::{PSPACE_NODE, PspaceNode};
use crate::points::{PSPACE_POINT, PspacePoint};

use taosx_core::sink::point::model::generate_tbname_from_pattern;

mod config;
pub mod csv;
pub mod model;
pub mod nodes;
pub mod points;

pub const PSPACE_ID: &str = "pspace";
pub const PSPACE_JAR: &str = "taosx-pspace.jar";

/// taosx-pspace.jar path: $PLUGINS_HOME/pspace/taosx-pspace.jar
pub fn pspace_jar_path() -> anyhow::Result<PathBuf> {
    let path = taosx_core::runners::get_plugin_dir("pspace").join(PSPACE_JAR);
    if !path.exists() {
        return Err(anyhow::anyhow!("pspace plugin not found at: {:?}", path));
    }
    Ok(path)
}

/// Connectivity check
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    match is_valid_impl(dsn).await {
        std::result::Result::Ok(validation) => validation,
        Err(err) => {
            tracing::error!("failed to validate pspace, error: {}", err);
            DataSourceValidation::invalid(
                PSPACE_ID.to_string(),
                format!("failed to validate pspace data source: {}", err),
            )
        }
    }
}

pub async fn is_valid_impl(dsn: &Dsn) -> anyhow::Result<DataSourceValidation> {
    let config = PspaceConfig::builder(dsn)
        .context("failed to parse pSpace connection config")?
        .build()?;

    let toml = toml::to_string(&config).context("failed to serialize pspace config to toml")?;
    let mut config_file = NamedTempFile::new().context("failed to create temp file")?;
    write!(config_file, "{}", &toml).context("failed to write toml file")?;

    tracing::info!(
        config_file=%config_file.path().display(),
        "execute pspace check, config:\n{}",
        toml.replace(&config.connection.password, "******")
    );
    // execute "java -jar taosx-pspace.jar -m check -c <config_file>"
    let path = pspace_jar_path()?;
    let mut command = tokio::process::Command::new("java");
    let output = command
        .args(["-jar", &path.to_string_lossy(), "-m", "check", "-c"])
        .arg(config_file.path())
        .stdout(std::process::Stdio::inherit())
        .output()
        .await
        .inspect_err(|err| tracing::error!(config_file=%config_file.path().display(), "{err:?}"))
        .context("failed to execute pspace check")?;

    let result = if output.status.success() {
        // parse JSON result from stdout
        let result: DataSourceValidation = serde_json::from_slice(&output.stdout)
            .inspect_err(|err| {
                tracing::error!(
                    config_file=%config_file.path().display(),
                    exit_code=?output.status.code(),
                    stdout=%String::from_utf8_lossy(&output.stdout),
                    "failed to deserialize pspace check result, err: {err}"
                );
            })
            .context("failed to deserialize pspace check result")?;
        result
    } else {
        // handle exception
        let stderr_str = String::from_utf8_lossy(&output.stderr);
        tracing::error!(
            config_file=%config_file.path().display(),
            exit_code=?output.status.code(),
            "pspace check failed: {}",
            stderr_str
        );
        DataSourceValidation::invalid(
            PSPACE_ID.to_string(),
            format!(
                "failed to execute pspace check: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        )
    };

    Ok(result)
}

/// Registerable datasets lister entry for PSPACE.
pub fn pspace_datasets_lister(from: &Dsn, req: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    list_datasets(from, req)
}

/// query pSpace nodes or data points
pub fn list_datasets(dsn: &Dsn, _req: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    let pspace_mode = utils::parse_key_in_dsn::<String>(dsn, "pspace_mode")?
        .ok_or(anyhow::anyhow!("pspace_mode is required"))?;

    match pspace_mode.as_str() {
        "nodes" => {
            tracing::info!(dsn = %dsn, "list pspace nodes");
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(list_nodes(dsn))
        }
        "points" => {
            tracing::info!(dsn = %dsn, "list pspace points");
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(list_points(dsn))
        }
        other => {
            anyhow::bail!(
                "invalid pspace_mode: {}, expected 'nodes' or 'points'",
                other
            );
        }
    }
}

/// query pSpace nodes
pub async fn list_nodes(dsn: &Dsn) -> anyhow::Result<Vec<DataSet>> {
    let config = PspaceConfig::builder(dsn)?.with_nodes()?.build()?;
    let toml = toml::to_string(&config)?;
    let mut config_file =
        NamedTempFile::new().context("failed to create temp file when listing pspace nodes")?;
    write!(config_file, "{}", &toml)
        .context("failed to write toml file when listing pspace nodes")?;

    tracing::info!(
        config_file=%config_file.path().display(),
        "execute taosx-pspace nodes, config:\n{}",
        toml.replace(&config.connection.password, "******")
    );
    // execute "java -jar taosx-pspace.jar -m nodes -c <config_file>"
    let path = pspace_jar_path()?;
    let mut command = tokio::process::Command::new("java");
    let output = command
        .args(["-jar", &path.to_string_lossy(), "-m", "nodes", "-c"])
        .arg(config_file.path())
        .stdout(std::process::Stdio::inherit())
        .output()
        .await
        .inspect_err(|err| tracing::error!(conf=%config_file.path().display(), "{err:?}"))
        .context("failed to execute taosx-pspace nodes")?;

    if output.status.success() {
        let nodes: Vec<PspaceNode> = serde_json::from_slice(&output.stdout).inspect_err(|err| {
            tracing::error!(
                config_file=%config_file.path().display(),
                exit_code=?output.status.code(),
                stdout=%String::from_utf8_lossy(&output.stdout),
                "failed to deserialize taosx-pspace nodes result, error: {}",err
            )
        })?;

        let datasets: Vec<DataSet> = nodes
            .into_iter()
            .map(|node| {
                let ds: DataSet = node.into();
                ds
            })
            .collect();

        tracing::debug!("execute taosx-pspace nodes, get {} nodes", datasets.len());

        return Ok(datasets);
    }

    let stderr_str = String::from_utf8_lossy(&output.stderr);
    tracing::error!(
        conf=%config_file.path().display(),
        exit_code=?output.status.code(),
        "execute taosx-pspace nodes failed: {}",
        stderr_str
    );
    anyhow::bail!("failed to execute taosx-pspace nodes: {}", stderr_str)
}

/// query pSpace data points
pub async fn list_points(dsn: &Dsn) -> anyhow::Result<Vec<DataSet>> {
    let config = PspaceConfig::builder(dsn)?
        .with_nodes()?
        .with_points()?
        .build()?;
    let toml = toml::to_string(&config)?;
    let mut config_file =
        NamedTempFile::new().context("failed to create temp file when listing pspace points")?;
    write!(config_file, "{}", &toml)
        .context("failed to write toml file when listing pspace points")?;

    tracing::info!(
        "execute taosx-pspace points, config:\n{}",
        toml.replace(&config.connection.password, "******")
    );
    // execute "java -jar taosx-pspace.jar -m points -c <config_file>"
    let path = pspace_jar_path()?;
    let mut command = tokio::process::Command::new("java");
    let output = command
        .args(["-jar", &path.to_string_lossy(), "-m", "points", "-c"])
        .arg(config_file.path())
        .stdout(std::process::Stdio::inherit())
        .output()
        .await
        .inspect_err(|err| tracing::error!(conf=%config_file.path().display(), "{err:?}"))
        .context("failed to execute taosx-pspace points")?;

    if output.status.success() {
        let points: Vec<PspacePoint> =
            serde_json::from_slice(&output.stdout).inspect_err(|err| {
                tracing::error!(
                    config_file=%config_file.path().display(),
                    exit_code=?output.status.code(),
                    stdout=%String::from_utf8_lossy(&output.stdout),
                    "failed to deserialize taosx-pspace points result, error: {}",err
                )
            })?;

        let datasets: Vec<DataSet> = points
            .into_iter()
            .map(|node| {
                let ds: DataSet = node.into();
                ds
            })
            .collect();

        tracing::debug!("execute taosx-pspace points, get {} points", datasets.len());

        return Ok(datasets);
    }

    let stderr_str = String::from_utf8_lossy(&output.stderr);
    tracing::error!(
        conf=%config_file.path().display(),
        exit_code=?output.status.code(),
        "taosx-pspace points failed: {}",
        stderr_str
    );
    anyhow::bail!("failed to execute taosx-pspace points: {}", stderr_str)
}

/// Convert pSpace datasets to point options for frontend.
pub fn to_point_options(datasets: Vec<DataSet>) -> anyhow::Result<serde_json::Value> {
    let mut nodes = vec![];
    let mut points = vec![];
    for ds in datasets {
        match ds.category.as_deref() {
            Some(PSPACE_NODE) => {
                let node = PspaceNode::try_from(ds)?;
                nodes.push(serde_json::to_value(node)?);
            }
            Some(PSPACE_POINT) => {
                let point = PspacePoint::try_from(ds)?;
                points.push(serde_json::to_value(point)?);
            }
            other => {
                anyhow::bail!("invalid dataset category: {:?}", other);
            }
        }
    }

    let options = json!({
        "nodes": serde_json::Value::Array(nodes),
        "points": serde_json::Value::Array(points),
    });

    Ok(options)
}

/// preview pSpace data points list
pub async fn preview_points(datasets: Vec<DataSet>) -> anyhow::Result<String> {
    let mut wtr: csv_async::AsyncWriter<Vec<u8>> = csv_async::AsyncWriter::from_writer(vec![]);

    let cols = vec!["id", "name", "data_type", "long_name", "desc"];
    wtr.write_record(&cols).await?;
    wtr.flush().await?;

    for ds in datasets {
        if let Some(ty) = ds.category.as_deref()
            && ty != PSPACE_POINT
        {
            continue;
        }
        let point =
            PspacePoint::try_from(ds).context("failed to convert DataSet to PspacePoint")?;

        let data_type = point
            .data_type()
            .map(|t| t.sql_repr_display())
            .unwrap_or_default();

        wtr.write_record(&[
            point.id.to_string(),
            point.name,
            data_type,
            point.long_name,
            point.desc.unwrap_or_default(),
        ])
        .await?;
        wtr.flush().await?;
    }

    let data = wtr.into_inner().await?;
    let csv_content = String::from_utf8(data)?;
    Ok(csv_content)
}

/// generate pSpace CSV config file
pub async fn to_csv_context(datasets: Vec<DataSet>) -> anyhow::Result<String> {
    let mut wtr = csv_async::AsyncWriter::from_writer(vec![]);

    // write CSV header
    let cols = vec![
        "No.",
        "point_id",
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
        "tag::VARCHAR(1024)::LongName",
        "tag::VARCHAR(1024)::Description",
    ];
    wtr.write_record(&cols).await?;
    wtr.flush().await?;

    // write each point as a row
    let mut no = 0u64;
    for ds in datasets {
        if let Some(category) = ds.category.as_deref()
            && category != PSPACE_POINT
        {
            continue;
        }

        let point =
            PspacePoint::try_from(ds).context("failed to convert DataSet to PspacePoint")?;

        no += 1;
        let point_id_str = point.id.to_string();
        let point_data_type = point.data_type();
        let stable = generate_stable_from_pattern("pspace_{type}", &point_data_type);

        let tbname = generate_tbname_from_pattern("pspace", "t_{point_id}", &point_id_str);
        let row: Vec<String> = vec![
            no.to_string(),
            point_id_str,
            stable,
            tbname,
            "val".to_string(),
            String::new(),
            point_data_type.map(|dt| dt.sql_repr()).unwrap_or_default(),
            "quality".to_string(),
            "ts".to_string(),
            String::new(),
            "qts".to_string(),
            String::new(),
            "rts".to_string(),
            String::new(),
            point.name,
            point.long_name,
            point.desc.unwrap_or_default(),
        ];

        wtr.write_record(&row).await?;
        wtr.flush().await?;
    }

    let data = wtr.into_inner().await?;
    let csv_content = String::from_utf8(data)?;
    Ok(csv_content)
}

/// pSpace CSV config template
pub fn get_template() -> String {
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/example/template.csv")).to_string()
}

/// pSpace -> TSDB
#[instrument(skip_all, fields(task.id = with_agent.as_ref().map(| v | v.task_id), job.id = with_agent.as_ref().map(| v | v.job_id)))]
pub async fn pspace_to_taos(
    task_job_id: Option<(i64, i64)>,
    from: Dsn,
    to: Dsn,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<Via>,
    notify: TaskNotifySender,
) -> anyhow::Result<()> {
    tracing::info!("pspace_to_taos start");

    // metrics init
    let metrics = get_metrics_arc_or(task_job_id, || {
        let (task_id, job_id) = task_job_id.unwrap_or((-1, -1));
        // task_id is None if taosx run
        Arc::new(CoreMetrics::IPC(IpcMetrics::new(
            format!("taosx_task_{}", PSPACE_ID),
            task_id,
            job_id,
        )))
    });
    if task_job_id.is_none() {
        insert_metrics(-1, -1, metrics.clone());
    }
    tracing::info!("pspace_to_taos metrics initialized");

    // parse task config
    let task_config = PspaceConfig::builder(&from)?
        .with_nodes()?
        .with_points()?
        .with_run()?
        .with_report()?
        .with_advanced_options()?
        .build()
        .context("failed to parse pSpace task config")?;
    tracing::info!("pspace_to_taos task config parsed");

    // parse DSN and build PointModelConfig
    let pspace_model_config =
        PspaceModelConfig::try_from(&from).context("failed to parse pSpace model config")?;
    let model_config: PointModelConfig = pspace_model_config
        .clone()
        .to_point_model_config(&from)
        .await
        .context("failed to convert pSpace model config to PointModelConfig")?;
    tracing::info!("pspace_to_taos PointModelConfig generated");

    // get ipc port
    let ipc_port = port_pool
        .get()
        .await
        .context("No available port for kinghist_to_taos task")?;
    let ipc_port = ipc_port.get();
    let socket = format!("127.0.0.1:{ipc_port}");
    tracing::info!("pspace_to_taos build ipc socket: {}", &socket);

    // build IPC
    let ipc_cancel = cancel.child_token();
    let (mut ipc_handler, _) = build_ipc(
        Some(socket.as_str()),
        None,
        &to,
        Some(PSPACE_ID),
        Some(Arc::new(model_config.clone())),
        None,
        &ipc_cancel,
        with_agent,
        task_job_id,
        notify,
        None,
    )
    .await?;
    tracing::info!("pspace_to_taos ipc handlers created");

    // set report.remote to IPC socket address
    let mut task_config = task_config;
    match task_config.report.as_mut() {
        Some(report) => report.remote = Some(socket.clone()),
        None => {
            task_config.report = Some(config::PspaceReportConfig {
                remote: Some(socket.clone()),
            });
        }
    }

    // set points.point_ids if pspace use csv_config_file mode
    let point_ids: Vec<u64> = model_config
        .point_config_map
        .keys()
        .filter_map(|id_str| id_str.parse::<u64>().ok())
        .collect();
    if matches!(&pspace_model_config, PspaceModelConfig::Csv(_)) && !point_ids.is_empty() {
        match task_config.points.as_mut() {
            Some(points) => points.point_ids = Some(point_ids),
            None => {
                task_config.points = Some(config::PspacePointsConfig {
                    name_filter: None,
                    include_data_type: None,
                    point_ids: Some(point_ids),
                });
            }
        }
    }

    // write config TOML to file
    let (task_id, job_id) = task_job_id.unwrap_or((-1, -1));
    let config_dir = get_data_dir()
        .join("tasks")
        .join(task_id.to_string())
        .join(job_id.to_string());
    std::fs::create_dir_all(&config_dir).map_err(|err| {
        anyhow::anyhow!(
            "failed to create config dir: {}, cause: {}",
            config_dir.display(),
            err
        )
    })?;
    let config_file_path = config_dir.join("collect.toml");
    let toml_str =
        toml::to_string(&task_config).context("failed to serialize pSpace config to TOML")?;
    let mut config_file =
        std::fs::File::create(&config_file_path).context("failed to create pSpace config file")?;
    write!(config_file, "{}", &toml_str)?;
    config_file.sync_all()?;
    drop(config_file);
    tracing::info!("pspace config written to: {}", config_file_path.display());

    // check JDK version
    let get_jdk_version = tokio::process::Command::new("java")
        .arg("-version")
        .output()
        .await
        .context("Get JDK version error, please ensure Java is installed")?;
    let jdk_version = String::from_utf8(get_jdk_version.stderr.clone())?;

    // spawn: java -jar taosx-pspace.jar -m run -c <config_file>
    tracing::info!(
        "execute taosx-pspace run\n{}",
        toml_str.replace(&task_config.connection.password, "******")
    );
    let jar_path = pspace_jar_path()?;
    let mut command = tokio::process::Command::new("java");
    let child = if jdk_version.contains("build 1.") {
        command
            .args(["-jar"])
            .arg(&jar_path)
            .args(["-m", "run", "-c"])
            .arg(&config_file_path)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped())
    } else {
        command
            .arg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .args(["-jar"])
            .arg(&jar_path)
            .args(["-m", "run", "-c"])
            .arg(&config_file_path)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::piped())
    };

    let mut child = child.spawn().context("failed to start pSpace plugin")?;
    tracing::info!("pSpace plugin spawned, pid: {:?}", child.id());
    send_sub_process_info(child.id(), task_job_id, "pspace").await;

    // create rolling log file for subprocess stderr
    let log_path = get_logs_home_dir();
    let log_file_name = format!("pspace-{task_id}-{job_id}");
    let appender = new_rolling_file_appender(log_path.as_path(), &log_file_name)
        .context("failed to create pSpace log file")?;

    // stderr reader: capture errors + write to rolling log
    const ERROR_BUF_SIZE: usize = 2;
    let error_buf = Arc::new(Mutex::new(ringbuf::HeapRb::<String>::new(ERROR_BUF_SIZE)));
    let error_buf_producer = error_buf.clone();
    let stderr = child.stderr.take().expect("Failed to capture stderr");

    tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(stderr);
        let mut line = String::new();
        loop {
            let bytes_read = reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                break;
            }
            if line.contains("ERROR") || line.contains("panic") {
                let mut guard = error_buf_producer.lock().await;
                let _ = guard.push_overwrite(line.clone());
            }
            let mut w = appender.make_writer();
            use std::io::Write as _;
            w.write_all(line.as_bytes())?;
            if let Err(err) = w.flush() {
                eprintln!("failed to flush pSpace log: {err}");
            }
            line.clear();
        }
        std::result::Result::<(), std::io::Error>::Ok(())
    });

    // wait for child / IPC error / cancellation
    tokio::spawn(
        async move {
            macro_rules! safe_exit {
                () => {
                    let _ = child.terminate_timeout(Duration::from_secs(2)).await;
                    tokio::spawn(async move {
                        tracing::info!("Wait for pSpace IPC handlers finished");
                        let _ = ipc_handler.close().await;
                        tracing::info!("All pSpace IPC handlers have been finished");
                    });
                };
            }
            tokio::select! {
                status = child.wait() => {
                    let status = status?;
                    tracing::info!("pSpace plugin exit with {}", status);
                    if !status.success() {
                        safe_exit!();
                        let error = error_buf.lock().await.iter().join("");
                        anyhow::bail!("pSpace plugin exit with {status}\n{error}");
                    }
                },
                err = ipc_handler.recv_error() => {
                    tracing::info!("received IPC worker error, terminate pSpace plugin");
                    if let Some(err) = err {
                        safe_exit!();
                        anyhow::bail!("pSpace IPC writer error: {err}");
                    }
                },
                _ = cancel.cancelled() => {
                    tracing::info!("pSpace task cancelled");
                },
            }
            tracing::info!("pSpace to taos task done");
            safe_exit!();
            std::result::Result::<(), anyhow::Error>::Ok(())
        }
        .in_current_span(),
    )
    .await??;

    std::result::Result::<(), anyhow::Error>::Ok(())
}

/// validate pSpace CSV config file
pub async fn is_csv_valid(dsn: &Dsn) -> anyhow::Result<()> {
    let pspace_model_config =
        PspaceModelConfig::try_from(dsn).context("failed to parse pSpace model config from DSN")?;

    // only validate when point_config_mode=csv_config_file
    if !matches!(&pspace_model_config, PspaceModelConfig::Csv(_)) {
        bail!("only support validate pSpace CSV config when point_config_mode=csv_config_file");
    }

    let model_config = pspace_model_config
        .to_point_model_config(dsn)
        .await
        .context("failed to build PointModelConfig from pSpace CSV")?;

    model_config
        .validate()
        .context("failed to validate pSpace CSV config")?;

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    #[ignore]
    #[test]
    fn test_pspace_jar_path() {
        unsafe {
            let plugins_home = std::env::current_dir().unwrap().join("../../plugins");
            std::env::set_var("PLUGINS_HOME", &plugins_home);
        }

        let path = pspace_jar_path().unwrap();
        println!("pspace jar path: {:?}", path);
    }

    #[test]
    fn test_to_point_options() {
        // 构造节点和数据点的 JSON
        let node_json = r#"{"id":150016,"name":"北京","long_name":"\\北京","is_leaf":false}"#;
        let point_json = r#"{"id":150019,"name":"气温","type":"PS_ANALOG","long_name":"\\北京\\朝阳\\气温","desc":""}"#;

        let node: PspaceNode = serde_json::from_str(node_json).unwrap();
        let point: PspacePoint = serde_json::from_str(point_json).unwrap();

        let ds_node: DataSet = node.into();
        let ds_point: DataSet = point.into();
        let datasets = vec![ds_node, ds_point];

        let options = to_point_options(datasets).unwrap();
        // 验证 nodes
        let nodes = options.get("nodes").unwrap().as_array().unwrap();
        assert_eq!(nodes.len(), 1);
        let n = &nodes[0];
        assert_eq!(n.get("id").unwrap(), 150016);
        assert_eq!(n.get("name").unwrap(), "北京");
        assert_eq!(n.get("long_name").unwrap(), r"\北京");
        assert_eq!(n.get("is_leaf").unwrap(), false);
        // 验证 points
        let points = options.get("points").unwrap().as_array().unwrap();
        assert_eq!(points.len(), 1);
        let p = &points[0];
        assert_eq!(p.get("id").unwrap(), 150019);
        assert_eq!(p.get("name").unwrap(), "气温");
        assert_eq!(p.get("type").unwrap(), "PS_ANALOG");
        assert_eq!(p.get("long_name").unwrap(), r"\北京\朝阳\气温");
        assert_eq!(p.get("desc").unwrap(), "");
        // category 不在 options
        assert!(p.get("category").is_none());
        assert!(n.get("category").is_none());
        // points desc 为 null 的情况
        let point_json2 =
            r#"{"id":150020,"name":"湿度","type":"PS_ANALOG","long_name":"\\北京\\朝阳\\湿度"}"#;
        let point2: PspacePoint = serde_json::from_str(point_json2).unwrap();
        let ds_point2: DataSet = point2.into();
        let options2 = to_point_options(vec![ds_point2]).unwrap();
        let points2 = options2.get("points").unwrap().as_array().unwrap();
        assert_eq!(points2.len(), 1);
        let p2 = &points2[0];
        assert_eq!(p2.get("id").unwrap(), 150020);
        assert_eq!(p2.get("desc").unwrap(), &serde_json::Value::Null);

        // point with data_type (kept in original pSpace format through DataSet round-trip)
        let point_json3 = r#"{"id":150021,"name":"压力","type":"PS_ANALOG","long_name":"\\北京\\朝阳\\压力","desc":"pressure","data_type":"psDataType_Float"}"#;
        let point3: PspacePoint = serde_json::from_str(point_json3).unwrap();
        let ds_point3: DataSet = point3.into();
        let options3 = to_point_options(vec![ds_point3]).unwrap();
        let points3 = options3.get("points").unwrap().as_array().unwrap();
        assert_eq!(points3.len(), 1);
        let p3 = &points3[0];
        assert_eq!(p3.get("id").unwrap(), 150021);
        assert_eq!(p3.get("data_type").unwrap(), "psDataType_Float"); // raw pSpace type preserved
    }
}
